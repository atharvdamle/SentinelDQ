import unittest
from unittest.mock import patch, MagicMock
import json
from botocore.exceptions import ClientError
from ingestion.consumers.minio_consumer import MinIOConsumer


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
        self.mock_event = {
            "id": "12345",
            "type": "PushEvent",
            "repo": {
                "name": "test/repo",
            },
            "created_at": "2025-10-20T12:00:00Z",
        }

    @patch("boto3.client")
    def test_init_connection_success(self, mock_boto3_client):
        # Mock S3 client
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        mock_s3.list_buckets.return_value = {"Buckets": []}

        # Initialize consumer
        with patch.dict("os.environ", self.mock_env):
            consumer = MinIOConsumer()

        # Verify S3 client was created with correct parameters
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
    def test_init_connection_failure(self, mock_boto3_client):
        # Mock connection failure
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        mock_s3.list_buckets.side_effect = Exception("Connection failed")

        # Verify consumer initialization raises exception
        with patch.dict("os.environ", self.mock_env):
            with self.assertRaises(Exception):
                consumer = MinIOConsumer()

    @patch("boto3.client")
    def test_init_bucket_exists(self, mock_boto3_client):
        # Mock S3 client with existing bucket
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        mock_s3.head_bucket.return_value = {}

        # Initialize consumer
        with patch.dict("os.environ", self.mock_env):
            consumer = MinIOConsumer()

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
            consumer = MinIOConsumer()

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
        mock_kafka_consumer.poll.side_effect = [mock_message, KeyboardInterrupt]

        with patch.dict("os.environ", self.mock_env):
            consumer = MinIOConsumer()
            consumer.store_event = MagicMock(side_effect=Exception("Storage error"))
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

        # Set up consumer to return one message then raise KeyboardInterrupt
        mock_kafka_consumer.poll.side_effect = [mock_message, KeyboardInterrupt]

        # Initialize consumer and start consuming
        with patch.dict("os.environ", self.mock_env):
            consumer = MinIOConsumer()
            consumer.store_event = MagicMock()  # Mock store_event to avoid S3 calls
            consumer.start_consuming()

        # Verify consumer was subscribed to correct topic
        mock_kafka_consumer.subscribe.assert_called_once_with([self.mock_env["KAFKA_TOPIC"]])

        # Verify store_event was called with correct event
        consumer.store_event.assert_called_once_with(self.mock_event)

        # Verify the offset was committed by hand, only after the store
        mock_kafka_consumer.commit.assert_called_once_with(message=mock_message, asynchronous=False)

        # Verify consumer was closed
        mock_kafka_consumer.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
