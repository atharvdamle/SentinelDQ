import os
import json
import time
import signal
import logging
from datetime import datetime, timezone
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from confluent_kafka import Consumer, KafkaException, TopicPartition
from dotenv import load_dotenv

from ingestion.config import configure_logging, require

configure_logging()
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


class MinIOConsumer:
    def __init__(self):
        # Kafka configuration
        self.consumer = Consumer(
            {
                "bootstrap.servers": require("KAFKA_BOOTSTRAP_SERVERS"),
                "group.id": "github_events_minio_consumer",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        self.topic = require("KAFKA_TOPIC")
        self._running = True
        self.max_put_attempts = 3
        self.retry_backoff_seconds = 1.0

        # MinIO configuration
        self.host = os.getenv("MINIO_HOST", "localhost")
        self.port = os.getenv("MINIO_API_PORT", "9000")
        self.secure = os.getenv("MINIO_SECURE", "False").lower() == "true"
        self.bucket_name = require("MINIO_BUCKET")

        protocol = "https" if self.secure else "http"
        self.endpoint = f"{protocol}://{self.host}:{self.port}"
        logger.debug(f"Configuring MinIO client with endpoint: {self.endpoint} (secure: {self.secure})")

        # Test connection before proceeding
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=require("MINIO_ACCESS_KEY"),
            aws_secret_access_key=require("MINIO_SECRET_KEY"),
            config=Config(signature_version="s3v4"),
            # Only meaningful over TLS, so it tracks MINIO_SECURE rather than
            # staying off once the endpoint is actually https.
            verify=self.secure,
            region_name="us-east-1",  # MinIO default region
        )
        self.test_minio_connection()

        # Ensure bucket exists
        self.init_bucket()

    def test_minio_connection(self):
        """Test the MinIO connection by trying to list buckets."""
        try:
            self.s3_client.list_buckets()
            logger.info(f"Connected to MinIO at {self.endpoint}, bucket '{self.bucket_name}'")
        except Exception as e:
            logger.error(f"Failed to connect to MinIO at {self.endpoint} for bucket '{self.bucket_name}': {e}")
            raise

    def init_bucket(self):
        """Create the MinIO bucket if it is genuinely absent.

        Only a 404 means "missing". A 403 or a network error used to take the
        same branch, so a credentials problem surfaced as a confusing failed
        create instead of the real error.
        """
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.debug(f"Bucket '{self.bucket_name}' already exists")
            return
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") not in ("404", "NoSuchBucket"):
                raise

        logger.info(f"Bucket '{self.bucket_name}' not found, creating it")
        self.s3_client.create_bucket(Bucket=self.bucket_name)

    def object_key(self, event):
        """Build the object key for an event.

        Keyed by event id, and dated by the event's own timestamp rather than
        the ingest clock, so a redelivered event overwrites its earlier copy
        instead of landing beside it under a different path.
        """
        created_at = event.get("created_at") or ""
        date_path = created_at[:10] if len(created_at) >= 10 else datetime.now(timezone.utc).strftime("%Y-%m-%d")
        return f"raw/{date_path}/{event.get('id', 'unknown')}.json"

    def store_event(self, event):
        """Store event JSON in MinIO, retrying before giving up on the write.

        Returns the stored size in KB, which the caller accumulates.
        """
        event_id = event.get("id", "unknown")
        key = self.object_key(event)
        event_json = json.dumps(event).encode("utf-8")
        size_kb = len(event_json) / 1024
        delay = self.retry_backoff_seconds

        for attempt in range(1, self.max_put_attempts + 1):
            try:
                self.s3_client.put_object(
                    Bucket=self.bucket_name, Key=key, Body=event_json, ContentType="application/json"
                )
                logger.debug(f"Stored event {event_id} at {key} ({size_kb:.2f} KB)")
                return size_kb
            except Exception as e:
                logger.error(
                    f"Failed to store event {event_id} at {key} (attempt {attempt}/{self.max_put_attempts}): {e}"
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
            messages_failed = 0
            total_size_kb = 0.0
            start_time = datetime.now(timezone.utc)

            while self._running:
                msg = self.consumer.poll(1.0)

                if msg is None:
                    continue
                if msg.error():
                    if msg.error().fatal():
                        # Retrying costs one log line per poll forever; exit and
                        # let the restart policy rebuild the client.
                        logger.critical(f"Fatal Kafka error, exiting: {msg.error()}")
                        raise KafkaException(msg.error())
                    logger.error(f"Kafka consumer error: {msg.error()}")
                    continue

                try:
                    event = json.loads(msg.value())
                except Exception as e:
                    logger.error(f"Skipping unreadable message at offset {msg.offset()}: {e}")
                    self.consumer.commit(message=msg, asynchronous=False)
                    continue

                if not isinstance(event, dict):
                    # event.get() would raise here, outside the try below.
                    logger.error(f"Skipping non-object message at offset {msg.offset()}: {type(event).__name__}")
                    self.consumer.commit(message=msg, asynchronous=False)
                    continue

                try:
                    total_size_kb += self.store_event(event)
                except Exception as e:
                    # Uncommitted and rewound: the event is replayed rather than
                    # skipped past once MinIO comes back.
                    messages_failed += 1
                    logger.error(f"Rewinding to offset {msg.offset()} on partition {msg.partition()}: {e}")
                    self.consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
                    continue

                self.consumer.commit(message=msg, asynchronous=False)

                messages_processed += 1
                if messages_processed % 100 == 0:
                    elapsed_time = (datetime.now(timezone.utc) - start_time).total_seconds()
                    avg_msg_per_sec = messages_processed / elapsed_time if elapsed_time > 0 else 0
                    logger.info(
                        f"Stored {messages_processed} events ({total_size_kb:.2f} KB) in "
                        f"{elapsed_time:.1f}s at {avg_msg_per_sec:.2f}/s, {messages_failed} put failures"
                    )

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
