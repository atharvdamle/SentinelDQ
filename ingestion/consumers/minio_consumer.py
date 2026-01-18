import os
import json
import logging
import uuid
from datetime import datetime
import boto3
from botocore.client import Config
from confluent_kafka import Consumer, KafkaError
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
            }
        )
        self.topic = os.getenv("KAFKA_TOPIC")
        logger.info(f"Will consume from Kafka topic: {self.topic}")

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

    def store_event(self, event):
        """Store event JSON in MinIO."""
        timestamp = datetime.utcnow()
        date_path = timestamp.strftime("%Y-%m-%d")
        time_path = timestamp.strftime("%H-%M-%S")
        file_uuid = str(uuid.uuid4())

        # Extract event metadata for logging
        event_id = event.get("id", "unknown")
        event_type = event.get("type", "unknown")
        repo_name = event.get("repo", {}).get("name", "unknown")

        key = f"raw/{date_path}/{time_path}-{file_uuid}.json"
        logger.info(f"Preparing to store event - ID: {event_id}, Type: {event_type}, Repo: {repo_name}")

        try:
            # Calculate event size for logging
            event_json = json.dumps(event)
            size_kb = len(event_json.encode("utf-8")) / 1024

            self.s3_client.put_object(
                Bucket=self.bucket_name, Key=key, Body=event_json.encode("utf-8"), ContentType="application/json"
            )
            logger.info(
                f"Successfully stored event in MinIO:\n"
                f"  - Path: {key}\n"
                f"  - Size: {size_kb:.2f} KB\n"
                f"  - Event Type: {event_type}\n"
                f"  - Event ID: {event_id}\n"
                f"  - Repository: {repo_name}"
            )
        except Exception as e:
            logger.error(
                f"Failed to store event in MinIO:\n"
                f"  - Event ID: {event_id}\n"
                f"  - Path: {key}\n"
                f"  - Error: {str(e)}"
            )
            raise

    def start_consuming(self):
        """Start consuming messages from Kafka."""
        try:
            self.consumer.subscribe([self.topic])
            logger.info(f"Started consuming from Kafka topic: {self.topic}")

            messages_processed = 0
            total_size_kb = 0
            start_time = datetime.utcnow()

            while True:
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
                    self.store_event(event)
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    continue

        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        finally:
            self.consumer.close()
            logger.info("Consumer closed")


def main():
    consumer = MinIOConsumer()
    consumer.start_consuming()


if __name__ == "__main__":
    main()
