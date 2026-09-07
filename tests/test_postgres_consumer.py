import unittest
from unittest.mock import patch, MagicMock
import json
from datetime import datetime, timezone
from ingestion.consumers.postgres_consumer import PostgresConsumer


class TestPostgresConsumer(unittest.TestCase):
    def setUp(self):
        self.mock_env = {
            "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
            "KAFKA_TOPIC": "github_events",
            "POSTGRES_DB": "testdb",
            "POSTGRES_USER": "testuser",
            "POSTGRES_PASSWORD": "testpass",
            "POSTGRES_HOST": "localhost",
            "POSTGRES_PORT": "5432",
        }
        self.mock_event = {
            "id": "12345",
            "type": "PushEvent",
            "repo": {"id": 98765, "name": "test/repo", "url": "https://api.github.com/repos/test/repo"},
            "actor": {
                "id": 11111,
                "login": "testuser",
                "url": "https://api.github.com/users/testuser",
                "avatar_url": "https://avatars.githubusercontent.com/u/11111",
            },
            "payload": {"ref": "refs/heads/main", "head": "abcdef123", "before": "123456789", "push_id": 987654321},
            "public": True,
            "created_at": "2025-10-20T12:00:00Z",
        }

    def build_consumer(self):
        """Construct a consumer with Kafka and the database stubbed out.

        Consumer is patched where the module bound it -- `from confluent_kafka
        import Consumer` resolves at import time, so patching
        `confluent_kafka.Consumer` misses and a real client is constructed,
        whose background thread then keeps pytest from ever exiting.
        """
        with patch.dict("os.environ", self.mock_env), patch(
            "ingestion.consumers.postgres_consumer.Consumer"
        ) as mock_consumer, patch(
            "ingestion.consumers.postgres_consumer.db.init_schema"
        ) as mock_init_schema:
            consumer = PostgresConsumer()
        consumer.repository = MagicMock()
        return consumer, mock_consumer, mock_init_schema

    def validated(self, status="PASS"):
        """Stub the validator HTTP call, which store_event makes first."""
        response = MagicMock()
        response.json.return_value = {"status": status}
        response.raise_for_status.return_value = None
        return patch("ingestion.consumers.postgres_consumer.requests.post", return_value=response)

    def test_init_db_delegates_to_the_shared_schema(self):
        """DDL lives in db/schema.py, not in the consumer."""
        _, _, mock_init_schema = self.build_consumer()

        mock_init_schema.assert_called_once()

    def test_store_event_buffers_rather_than_writing_immediately(self):
        """Events used to cost one connection and one INSERT each."""
        consumer, _, _ = self.build_consumer()

        with self.validated():
            consumer.store_event(self.mock_event)

        consumer.repository.save_batch.assert_not_called()
        self.assertEqual(len(consumer._pending), 1)

    def test_flush_writes_one_batch(self):
        consumer, _, _ = self.build_consumer()
        consumer.batch_size = 1000

        with self.validated():
            for index in range(5):
                event = dict(self.mock_event, id=str(index))
                consumer.store_event(event)
        consumer.flush()

        consumer.repository.save_batch.assert_called_once()
        self.assertEqual(len(consumer.repository.save_batch.call_args[0][0]), 5)

    def test_batch_size_triggers_a_flush(self):
        consumer, _, _ = self.build_consumer()
        consumer.batch_size = 3

        with self.validated():
            for index in range(3):
                consumer.store_event(dict(self.mock_event, id=str(index)))

        consumer.repository.save_batch.assert_called_once()
        self.assertEqual(consumer._pending, [])

    def test_event_is_mapped_onto_the_flat_columns(self):
        consumer, _, _ = self.build_consumer()

        with self.validated():
            consumer.store_event(self.mock_event)

        columns = consumer._pending[0]["columns"]
        self.assertEqual(columns["event_id"], self.mock_event["id"])
        self.assertEqual(columns["event_type"], self.mock_event["type"])
        self.assertEqual(columns["repo_id"], self.mock_event["repo"]["id"])
        self.assertEqual(columns["repo_name"], self.mock_event["repo"]["name"])
        self.assertEqual(columns["actor_login"], self.mock_event["actor"]["login"])
        self.assertEqual(columns["push_id"], self.mock_event["payload"]["push_id"])

    def test_created_at_is_parsed_as_utc(self):
        """A naive value would be read as server-local time by the database."""
        consumer, _, _ = self.build_consumer()

        with self.validated():
            consumer.store_event(self.mock_event)

        created_at = consumer._pending[0]["columns"]["created_at"]
        self.assertEqual(created_at.tzinfo, timezone.utc)
        self.assertEqual(created_at, datetime(2025, 10, 20, 12, 0, tzinfo=timezone.utc))

    def test_created_at_accepts_fractional_seconds(self):
        """A fixed "%Y-%m-%dT%H:%M:%SZ" raised ValueError and dropped the event."""
        consumer, _, _ = self.build_consumer()
        event = dict(self.mock_event, created_at="2025-10-20T12:00:00.123456Z")

        with self.validated():
            consumer.store_event(event)

        created_at = consumer._pending[0]["columns"]["created_at"]
        self.assertEqual(created_at.tzinfo, timezone.utc)
        self.assertEqual(created_at.microsecond, 123456)

    def test_created_at_accepts_an_explicit_offset(self):
        consumer, _, _ = self.build_consumer()
        event = dict(self.mock_event, created_at="2025-10-20T12:00:00+00:00")

        with self.validated():
            consumer.store_event(event)

        self.assertIsNotNone(consumer._pending[0]["columns"]["created_at"].tzinfo)

    def test_failed_validation_is_not_stored(self):
        consumer, _, _ = self.build_consumer()

        with self.validated(status="FAIL"):
            consumer.store_event(self.mock_event)

        self.assertEqual(consumer._pending, [])

    def test_unreachable_validator_is_fail_closed(self):
        import requests

        consumer, _, _ = self.build_consumer()

        with patch(
            "ingestion.consumers.postgres_consumer.requests.post",
            side_effect=requests.exceptions.ConnectionError("down"),
        ):
            consumer.store_event(self.mock_event)

        self.assertEqual(consumer._pending, [])

    def test_flush_failure_clears_the_buffer_and_raises(self):
        consumer, _, _ = self.build_consumer()
        consumer.repository.save_batch.side_effect = Exception("Database error")

        with self.validated():
            consumer.store_event(self.mock_event)

        with self.assertRaises(Exception):
            consumer.flush()
        # The batch is dropped rather than retried forever on a poison event.
        self.assertEqual(consumer._pending, [])

    def test_start_consuming(self):
        consumer, _, _ = self.build_consumer()
        mock_kafka_consumer = consumer.consumer

        mock_message = MagicMock()
        mock_message.error.return_value = None
        mock_message.value.return_value = json.dumps(self.mock_event).encode()
        mock_kafka_consumer.poll.side_effect = [mock_message, KeyboardInterrupt]

        with patch.dict("os.environ", self.mock_env), patch(
            "ingestion.consumers.postgres_consumer.db.close_pool"
        ):
            consumer.store_event = MagicMock()
            consumer.start_consuming()

        mock_kafka_consumer.subscribe.assert_called_once_with([self.mock_env["KAFKA_TOPIC"]])
        consumer.store_event.assert_called_once()
        self.assertEqual(consumer.store_event.call_args[0][0], self.mock_event)
        mock_kafka_consumer.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
