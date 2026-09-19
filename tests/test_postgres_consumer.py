import unittest
from unittest.mock import patch, MagicMock
import json
import os
import signal
from datetime import datetime, timezone
from ingestion.consumers.postgres_consumer import PostgresConsumer, main
from tests.conftest import MOCK_EVENT, stops_after


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
        self.mock_event = MOCK_EVENT

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

    def validated(self, consumer, status="PASS", failures=()):
        """Stub the validator HTTP call, which store_event makes first.

        The call goes through the consumer's pooled session, so the patch has
        to land there rather than on the module's `requests`.
        """
        response = MagicMock()
        response.json.return_value = {"status": status, "failures": list(failures)}
        response.raise_for_status.return_value = None
        return patch.object(consumer.session, "post", return_value=response)

    def message(self, offset=0):
        """A Kafka message carrying only the coordinates store_event commits."""
        msg = MagicMock()
        msg.topic.return_value = self.mock_env["KAFKA_TOPIC"]
        msg.partition.return_value = 0
        msg.offset.return_value = offset
        return msg

    def test_init_db_delegates_to_the_shared_schema(self):
        """DDL lives in db/schema.py, not in the consumer."""
        _, _, mock_init_schema = self.build_consumer()

        mock_init_schema.assert_called_once()

    def test_store_event_buffers_rather_than_writing_immediately(self):
        """Events used to cost one connection and one INSERT each."""
        consumer, _, _ = self.build_consumer()

        with self.validated(consumer):
            consumer.store_event(self.mock_event, self.message())

        consumer.repository.save_batch.assert_not_called()
        self.assertEqual(len(consumer._pending), 1)

    def test_flush_writes_one_batch(self):
        consumer, _, _ = self.build_consumer()
        consumer.batch_size = 1000

        with self.validated(consumer):
            for index in range(5):
                event = dict(self.mock_event, id=str(index))
                consumer.store_event(event, self.message(index))
        consumer.flush()

        consumer.repository.save_batch.assert_called_once()
        self.assertEqual(len(consumer.repository.save_batch.call_args[0][0]), 5)

    def test_batch_size_triggers_a_flush(self):
        consumer, _, _ = self.build_consumer()
        consumer.batch_size = 3

        with self.validated(consumer):
            for index in range(3):
                consumer.store_event(dict(self.mock_event, id=str(index)), self.message(index))

        consumer.repository.save_batch.assert_called_once()
        self.assertEqual(consumer._pending, [])

    def test_event_is_mapped_onto_the_flat_columns(self):
        consumer, _, _ = self.build_consumer()

        with self.validated(consumer):
            consumer.store_event(self.mock_event, self.message())

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

        with self.validated(consumer):
            consumer.store_event(self.mock_event, self.message())

        created_at = consumer._pending[0]["columns"]["created_at"]
        self.assertEqual(created_at.tzinfo, timezone.utc)
        self.assertEqual(created_at, datetime(2025, 10, 20, 12, 0, tzinfo=timezone.utc))

    def test_created_at_accepts_fractional_seconds(self):
        """A fixed "%Y-%m-%dT%H:%M:%SZ" raised ValueError and dropped the event."""
        consumer, _, _ = self.build_consumer()
        event = dict(self.mock_event, created_at="2025-10-20T12:00:00.123456Z")

        with self.validated(consumer):
            consumer.store_event(event, self.message())

        created_at = consumer._pending[0]["columns"]["created_at"]
        self.assertEqual(created_at.tzinfo, timezone.utc)
        self.assertEqual(created_at.microsecond, 123456)

    def test_created_at_accepts_an_explicit_offset(self):
        consumer, _, _ = self.build_consumer()
        event = dict(self.mock_event, created_at="2025-10-20T12:00:00+00:00")

        with self.validated(consumer):
            consumer.store_event(event, self.message())

        self.assertIsNotNone(consumer._pending[0]["columns"]["created_at"].tzinfo)

    def test_failed_validation_is_not_stored(self):
        consumer, _, _ = self.build_consumer()

        with self.validated(consumer, status="FAIL"):
            consumer.store_event(self.mock_event, self.message())

        self.assertEqual(consumer._pending, [])

    def test_a_duplicate_only_failure_is_stored(self):
        """At-least-once guarantees redeliveries, which always FAIL that check."""
        consumer, _, _ = self.build_consumer()
        failures = [{"check_type": "duplicate", "severity": "FAIL"}]

        with self.validated(consumer, status="FAIL", failures=failures):
            consumer.store_event(self.mock_event, self.message())

        self.assertEqual(len(consumer._pending), 1)

    def test_an_event_missing_optional_fields_is_stored(self):
        """`public`, `payload` and the `*url` fields can all pass validation absent."""
        consumer, _, _ = self.build_consumer()
        event = {
            "id": "999",
            "type": "WatchEvent",
            "repo": {"id": 1, "name": "test/repo"},
            "actor": {"id": 2, "login": "testuser"},
            "created_at": "2025-10-20T12:00:00Z",
        }

        with self.validated(consumer):
            consumer.store_event(event, self.message())

        self.assertEqual(len(consumer._pending), 1)
        columns = consumer._pending[0]["columns"]
        self.assertIsNone(columns["public"])
        self.assertIsNone(columns["payload_ref"])
        self.assertIsNone(columns["repo_url"])
        self.assertIsNone(columns["actor_avatar"])

    def test_unreachable_validator_is_fail_closed(self):
        import requests

        consumer, _, _ = self.build_consumer()

        with patch.object(
            consumer.session, "post", side_effect=requests.exceptions.ConnectionError("down")
        ):
            consumer.store_event(self.mock_event, self.message())

        self.assertEqual(consumer._pending, [])

    def test_flush_commits_only_after_a_successful_write(self):
        consumer, _, _ = self.build_consumer()
        consumer.batch_size = 1000

        with self.validated(consumer):
            consumer.store_event(self.mock_event, self.message(7))
        consumer.consumer.commit.assert_not_called()
        consumer.flush()

        consumer.consumer.commit.assert_called_once()
        committed = consumer.consumer.commit.call_args[1]["offsets"][0]
        self.assertEqual(committed.offset, 8)
        self.assertFalse(consumer.consumer.commit.call_args[1]["asynchronous"])

    def test_flush_failure_retains_the_batch_and_does_not_commit(self):
        """A database outage must cost latency, not events."""
        consumer, _, _ = self.build_consumer()
        consumer.retry_backoff_seconds = 0
        consumer.repository.save_batch.side_effect = Exception("Database error")

        with self.validated(consumer):
            consumer.store_event(self.mock_event, self.message())
        with patch.object(consumer, "_dead_letter", return_value=False):
            consumer.flush()

        self.assertEqual(len(consumer._pending), 1)
        consumer.consumer.commit.assert_not_called()

    def test_an_unwritable_batch_is_dead_lettered_then_committed(self):
        consumer, _, _ = self.build_consumer()
        consumer.retry_backoff_seconds = 0
        consumer.repository.save_batch.side_effect = Exception("Database error")

        with self.validated(consumer):
            consumer.store_event(self.mock_event, self.message())
        with patch("ingestion.consumers.postgres_consumer.Producer") as mock_producer:
            mock_producer.return_value.flush.return_value = 0
            consumer.flush()

        self.assertEqual(consumer.repository.save_batch.call_count, consumer.max_write_attempts)
        mock_producer.return_value.produce.assert_called_once()
        self.assertEqual(mock_producer.return_value.produce.call_args[0][0], "github_events.dlq")
        self.assertEqual(consumer._pending, [])
        consumer.consumer.commit.assert_called_once()

    def test_an_unreachable_validator_leaves_the_offset_uncommitted(self):
        import requests

        consumer, _, _ = self.build_consumer()

        with patch.object(
            consumer.session, "post", side_effect=requests.exceptions.ConnectionError("down")
        ):
            handled = consumer.store_event(self.mock_event, self.message(3))
        consumer.flush()

        self.assertFalse(handled)
        consumer.consumer.commit.assert_not_called()

    def test_start_consuming(self):
        consumer, _, _ = self.build_consumer()
        mock_kafka_consumer = consumer.consumer

        mock_message = MagicMock()
        mock_message.error.return_value = None
        mock_message.value.return_value = json.dumps(self.mock_event).encode()
        mock_kafka_consumer.poll.side_effect = stops_after(consumer, mock_message)

        with patch.dict("os.environ", self.mock_env), patch(
            "ingestion.consumers.postgres_consumer.db.close_pool"
        ):
            consumer.store_event = MagicMock()
            consumer.start_consuming()

        mock_kafka_consumer.subscribe.assert_called_once_with([self.mock_env["KAFKA_TOPIC"]])
        consumer.store_event.assert_called_once()
        self.assertEqual(consumer.store_event.call_args[0][0], self.mock_event)
        mock_kafka_consumer.close.assert_called_once()

    def test_sigterm_stops_consuming_and_flushes_the_buffer(self):
        """The whole pending buffer used to die with the container."""
        for sig in (signal.SIGTERM, signal.SIGINT):
            self.addCleanup(signal.signal, sig, signal.getsignal(sig))
        consumer, _, _ = self.build_consumer()
        consumer.batch_size = 1000

        with self.validated(consumer):
            consumer.store_event(self.mock_event, self.message())
        consumer.consumer.poll.side_effect = lambda *_: os.kill(os.getpid(), signal.SIGTERM)

        with patch("ingestion.consumers.postgres_consumer.PostgresConsumer", return_value=consumer), patch(
            "ingestion.consumers.postgres_consumer.db.close_pool"
        ):
            main()

        self.assertFalse(consumer._running)
        consumer.repository.save_batch.assert_called_once()
        consumer.consumer.commit.assert_called_once()


if __name__ == "__main__":
    unittest.main()
