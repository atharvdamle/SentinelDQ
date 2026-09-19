import unittest
from unittest.mock import patch, MagicMock
import json
import os
import signal
from botocore.exceptions import ClientError
from ingestion.consumers.minio_consumer import MinIOConsumer, main
from tests.conftest import MOCK_EVENT, stops_after


class TestMinIOConsumer(unittest.TestCase):
    def setUp(self):
        # MinIOConsumer builds a Kafka client in __init__. Patch the name the
        # module bound at import -- patching `confluent_kafka.Consumer` misses,
        # so a real client is constructed and its background thread keeps the
        # test process alive forever.
        self.kafka_patcher = patch("ingestion.consumers.minio_consumer.Consumer")
        self.mock_kafka_consumer_class = self.kafka_patcher.start()
        self.addCleanup(self.kafka_patcher.stop)

        self.mock_env = {
            "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
            "KAFKA_TOPIC": "github_events",
            "MINIO_HOST": "localhost",
            "MINIO_API_PORT": "9000",
            "MINIO_SECURE": "False",
            "MINIO_BUCKET": "testbucket",
            "MINIO_ACCESS_KEY": "testaccesskey",
            "MINIO_SECRET_KEY": "testsecretkey",
        }
        self.mock_event = MOCK_EVENT

    @patch("boto3.client")
    def test_init_connection_success(self, mock_boto3_client):
        # Mock S3 client
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        mock_s3.list_buckets.return_value = {"Buckets": []}

        # Initialize consumer
        with patch.dict("os.environ", self.mock_env):
            MinIOConsumer()

        # Verify S3 client was created with correct parameters. Certificate
        # verification is off only because this endpoint is plain http.
        mock_boto3_client.assert_called_once_with(
            "s3",
            endpoint_url=f"http://{self.mock_env['MINIO_HOST']}:{self.mock_env['MINIO_API_PORT']}",
            aws_access_key_id=self.mock_env["MINIO_ACCESS_KEY"],
            aws_secret_access_key=self.mock_env["MINIO_SECRET_KEY"],
            config=unittest.mock.ANY,
            verify=False,
            region_name="us-east-1",
        )

    @patch("boto3.client")
    def test_certificate_verification_follows_minio_secure(self, mock_boto3_client):
        """verify=False was hardcoded, so TLS was unverified even over https."""
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        mock_s3.list_buckets.return_value = {"Buckets": []}

        with patch.dict("os.environ", dict(self.mock_env, MINIO_SECURE="True")):
            MinIOConsumer()

        kwargs = mock_boto3_client.call_args[1]
        self.assertTrue(kwargs["verify"])
        self.assertTrue(kwargs["endpoint_url"].startswith("https://"))

    @patch("boto3.client")
    def test_init_connection_failure(self, mock_boto3_client):
        # Mock connection failure
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        mock_s3.list_buckets.side_effect = Exception("Connection failed")

        # Verify consumer initialization raises exception
        with patch.dict("os.environ", self.mock_env):
            with self.assertRaises(Exception):
                MinIOConsumer()

    @patch("boto3.client")
    def test_init_bucket_exists(self, mock_boto3_client):
        # Mock S3 client with existing bucket
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        mock_s3.head_bucket.return_value = {}

        # Initialize consumer
        with patch.dict("os.environ", self.mock_env):
            MinIOConsumer()

        # Verify bucket creation was not attempted
        mock_s3.create_bucket.assert_not_called()

    @patch("boto3.client")
    def test_init_bucket_creation(self, mock_boto3_client):
        # Mock S3 client with non-existent bucket
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        mock_s3.head_bucket.side_effect = ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "HeadBucket")

        # Initialize consumer
        with patch.dict("os.environ", self.mock_env):
            MinIOConsumer()

        # Verify bucket was created
        mock_s3.create_bucket.assert_called_once_with(Bucket=self.mock_env["MINIO_BUCKET"])

    @patch("boto3.client")
    def test_store_event_success(self, mock_boto3_client):
        # Mock S3 client
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        mock_s3.list_buckets.return_value = {"Buckets": []}

        # Initialize consumer and store event
        with patch.dict("os.environ", self.mock_env):
            consumer = MinIOConsumer()
            consumer.store_event(self.mock_event)

        # The key is derived from the event, not the clock, so a redelivered
        # event overwrites its earlier copy instead of duplicating it.
        mock_s3.put_object.assert_called_once()
        call_kwargs = mock_s3.put_object.call_args[1]
        self.assertEqual(call_kwargs["Bucket"], self.mock_env["MINIO_BUCKET"])
        self.assertEqual(call_kwargs["Key"], "raw/2025-10-20/12345.json")
        self.assertEqual(call_kwargs["ContentType"], "application/json")

    @patch("boto3.client")
    def test_store_event_failure(self, mock_boto3_client):
        # Mock S3 client with error
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        mock_s3.list_buckets.return_value = {"Buckets": []}
        mock_s3.put_object.side_effect = Exception("Storage error")

        # Initialize consumer
        with patch.dict("os.environ", self.mock_env):
            consumer = MinIOConsumer()
            consumer.retry_backoff_seconds = 0

            # Verify store_event retries, then raises
            with self.assertRaises(Exception):
                consumer.store_event(self.mock_event)

        self.assertEqual(mock_s3.put_object.call_count, consumer.max_put_attempts)

    @patch("boto3.client")
    def test_a_failed_put_is_rewound_rather_than_committed(self, mock_boto3_client):
        mock_kafka_consumer = MagicMock()
        self.mock_kafka_consumer_class.return_value = mock_kafka_consumer

        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        mock_s3.list_buckets.return_value = {"Buckets": []}

        mock_message = MagicMock()
        mock_message.error.return_value = None
        mock_message.value.return_value = json.dumps(self.mock_event).encode()
        mock_message.topic.return_value = self.mock_env["KAFKA_TOPIC"]
        mock_message.partition.return_value = 0
        mock_message.offset.return_value = 4
        with patch.dict("os.environ", self.mock_env):
            consumer = MinIOConsumer()
            consumer.store_event = MagicMock(side_effect=Exception("Storage error"))
            mock_kafka_consumer.poll.side_effect = stops_after(consumer, mock_message)
            consumer.start_consuming()

        mock_kafka_consumer.commit.assert_not_called()
        self.assertEqual(mock_kafka_consumer.seek.call_args[0][0].offset, 4)

    @patch("boto3.client")
    def test_start_consuming(self, mock_boto3_client):
        # Mock Kafka consumer
        mock_kafka_consumer = MagicMock()
        self.mock_kafka_consumer_class.return_value = mock_kafka_consumer

        # Mock S3 client
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        mock_s3.list_buckets.return_value = {"Buckets": []}

        # Mock a Kafka message
        mock_message = MagicMock()
        mock_message.error.return_value = None
        mock_message.value.return_value = json.dumps(self.mock_event).encode()

        # Initialize consumer and start consuming
        with patch.dict("os.environ", self.mock_env):
            consumer = MinIOConsumer()
            # store_event returns the stored size, which the loop accumulates.
            consumer.store_event = MagicMock(return_value=1.5)
            # One message, then the graceful stop a signal would request.
            mock_kafka_consumer.poll.side_effect = stops_after(consumer, mock_message)
            consumer.start_consuming()

        # Verify consumer was subscribed to correct topic
        mock_kafka_consumer.subscribe.assert_called_once_with([self.mock_env["KAFKA_TOPIC"]])

        # Verify store_event was called with correct event
        consumer.store_event.assert_called_once_with(self.mock_event)

        # Verify the offset was committed by hand, only after the store
        mock_kafka_consumer.commit.assert_called_once_with(message=mock_message, asynchronous=False)

        # Verify consumer was closed
        mock_kafka_consumer.close.assert_called_once()

    @patch("boto3.client")
    def test_sigterm_stops_consuming_and_closes_the_consumer(self, mock_boto3_client):
        """This loop was a bare `while True` with no clean group leave."""
        for sig in (signal.SIGTERM, signal.SIGINT):
            self.addCleanup(signal.signal, sig, signal.getsignal(sig))
        mock_kafka_consumer = MagicMock()
        self.mock_kafka_consumer_class.return_value = mock_kafka_consumer
        mock_kafka_consumer.poll.side_effect = lambda *_: os.kill(os.getpid(), signal.SIGTERM)

        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        mock_s3.list_buckets.return_value = {"Buckets": []}

        with patch.dict("os.environ", self.mock_env):
            consumer = MinIOConsumer()
            with patch("ingestion.consumers.minio_consumer.MinIOConsumer", return_value=consumer):
                main()

        self.assertFalse(consumer._running)
        mock_kafka_consumer.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
