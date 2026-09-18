import os
import json
import time
import signal
import logging
from datetime import datetime
import boto3
from botocore.client import Config
from confluent_kafka import Consumer, KafkaError, TopicPartition
from dotenv import load_dotenv

# Configure logging with a more detailed format
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s"
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


class MinIOConsumer:
    def __init__(self):
        logger.info("Initializing MinIO Consumer...")

        # Kafka configuration
        kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        logger.info(f"Configuring Kafka consumer with bootstrap servers: {kafka_servers}")
        self.consumer = Consumer(
            {
                "bootstrap.servers": kafka_servers,
                "group.id": "github_events_minio_consumer",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        self.topic = os.getenv("KAFKA_TOPIC")
        logger.info(f"Will consume from Kafka topic: {self.topic}")
        self._running = True
        self.max_put_attempts = 3
        self.retry_backoff_seconds = 1.0

        # MinIO configuration
        self.host = os.getenv("MINIO_HOST", "localhost")
        self.port = os.getenv("MINIO_API_PORT", "9000")
        self.secure = os.getenv("MINIO_SECURE", "False").lower() == "true"
        self.bucket_name = os.getenv("MINIO_BUCKET")

        protocol = "https" if self.secure else "http"
        self.endpoint = f"{protocol}://{self.host}:{self.port}"
        logger.info(f"Configuring MinIO client with endpoint: {self.endpoint} (secure: {self.secure})")

        # Test connection before proceeding
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
            config=Config(signature_version="s3v4"),
            verify=False,  # Disable SSL verification for local development
            region_name="us-east-1",  # MinIO default region
        )
        self.test_minio_connection()

        # Ensure bucket exists
        self.init_bucket()

    def test_minio_connection(self):
        """Test the MinIO connection by trying to list buckets."""
        try:
            logger.info(f"Testing MinIO connection to {self.endpoint}...")
            self.s3_client.list_buckets()
            logger.info("Successfully connected to MinIO")
        except Exception as e:
            logger.error(f"Failed to connect to MinIO: {str(e)}")
            logger.error("Connection details:")
            logger.error(f"  - Endpoint: {self.endpoint}")
            logger.error(f"  - Access Key: {os.getenv('MINIO_ACCESS_KEY')}")
            logger.error(f"  - Bucket: {self.bucket_name}")
            raise

    def init_bucket(self):
        """Initialize the MinIO bucket if it doesn't exist."""
        logger.info(f"Checking if bucket '{self.bucket_name}' exists...")
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"Bucket '{self.bucket_name}' already exists")
        except Exception as e:
            logger.warning(f"Bucket '{self.bucket_name}' not found, creating it now...")
            try:
                self.s3_client.create_bucket(Bucket=self.bucket_name)
                logger.info(f"Successfully created bucket: {self.bucket_name}")
            except Exception as create_error:
                logger.error(f"Failed to create bucket: {create_error}")
                raise

    def object_key(self, event):
        """Build the object key for an event.

        Keyed by event id, and dated by the event's own timestamp rather than
        the ingest clock, so a redelivered event overwrites its earlier copy
        instead of landing beside it under a different path.
        """
        created_at = event.get("created_at", "")
        date_path = created_at[:10] if len(created_at) >= 10 else datetime.utcnow().strftime("%Y-%m-%d")
        return f"raw/{date_path}/{event.get('id', 'unknown')}.json"

    def store_event(self, event):
        """Store event JSON in MinIO, retrying before giving up on the write."""
        # Extract event metadata for logging
        event_id = event.get("id", "unknown")
        event_type = event.get("type", "unknown")
        repo_name = event.get("repo", {}).get("name", "unknown")

        key = self.object_key(event)
        logger.info(f"Preparing to store event - ID: {event_id}, Type: {event_type}, Repo: {repo_name}")

        event_json = json.dumps(event).encode("utf-8")
        size_kb = len(event_json) / 1024
        delay = self.retry_backoff_seconds

        for attempt in range(1, self.max_put_attempts + 1):
            try:
                self.s3_client.put_object(
                    Bucket=self.bucket_name, Key=key, Body=event_json, ContentType="application/json"
                )
                logger.info(
                    f"Successfully stored event in MinIO:\n"
                    f"  - Path: {key}\n"
                    f"  - Size: {size_kb:.2f} KB\n"
                    f"  - Event Type: {event_type}\n"
                    f"  - Event ID: {event_id}\n"
                    f"  - Repository: {repo_name}"
                )
                return
            except Exception as e:
                logger.error(
                    f"Failed to store event in MinIO "
                    f"(attempt {attempt}/{self.max_put_attempts}):\n"
                    f"  - Event ID: {event_id}\n"
                    f"  - Path: {key}\n"
                    f"  - Error: {str(e)}"
                )
                if attempt < self.max_put_attempts:
                    time.sleep(delay)
                    delay *= 2

        raise RuntimeError(f"Could not store event {event_id} at {key} after {self.max_put_attempts} attempts")

    def stop(self):
        """Gracefully stop consuming."""
        self._running = False

    def start_consuming(self):
        """Start consuming messages from Kafka."""
        try:
            self.consumer.subscribe([self.topic])
            logger.info(f"Started consuming from Kafka topic: {self.topic}")

            messages_processed = 0
            total_size_kb = 0
            start_time = datetime.utcnow()

            while self._running:
                msg = self.consumer.poll(1.0)

                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"Reached end of partition for topic: {self.topic}")
                        continue
                    else:
                        logger.error(f"Kafka consumer error: {msg.error()}")
                        continue

                # Log processing metrics every 100 messages
                messages_processed += 1
                if messages_processed % 100 == 0:
                    elapsed_time = (datetime.utcnow() - start_time).total_seconds()
                    avg_msg_per_sec = messages_processed / elapsed_time if elapsed_time > 0 else 0
                    logger.info(
                        f"Processing Statistics:\n"
                        f"  - Messages Processed: {messages_processed}\n"
                        f"  - Total Size: {total_size_kb:.2f} KB\n"
                        f"  - Avg Messages/sec: {avg_msg_per_sec:.2f}\n"
                        f"  - Running Time: {elapsed_time:.1f} seconds"
                    )

                try:
                    event = json.loads(msg.value())
                except Exception as e:
                    logger.error(f"Skipping unreadable message at offset {msg.offset()}: {e}")
                    self.consumer.commit(message=msg, asynchronous=False)
                    continue

                try:
                    self.store_event(event)
                except Exception as e:
                    # Uncommitted and rewound: the event is replayed rather than
                    # skipped past once MinIO comes back.
                    logger.error(f"Rewinding to offset {msg.offset()} on partition {msg.partition()}: {e}")
                    self.consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
                    continue

                self.consumer.commit(message=msg, asynchronous=False)

        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        finally:
            self.consumer.close()
            logger.info("Consumer closed")


def main():
    consumer = MinIOConsumer()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: consumer.stop())
    consumer.start_consuming()


if __name__ == "__main__":
    main()
