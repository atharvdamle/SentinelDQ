import os
import json
import logging
import psycopg2
import psycopg2.extras
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


class PostgresConsumer:
    def __init__(self):
        # Kafka configuration
        self.consumer = Consumer({
            'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
            'group.id': 'github_events_postgres_consumer',
            'auto.offset.reset': 'earliest'
        })
        self.topic = os.getenv('KAFKA_TOPIC')
        self._running = True

        # PostgreSQL configuration
        self.db_config = {
            'dbname': os.getenv('POSTGRES_DB', 'postgres'),
            'user': os.getenv('POSTGRES_USER', 'postgres'),
            'password': os.getenv('POSTGRES_PASSWORD', ''),
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': int(os.getenv('POSTGRES_PORT', '5432'))
        }

        # Initialize database
        self.init_db()

    def init_db(self):
        """Initialize the database table."""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS github_events (
            id SERIAL PRIMARY KEY,
            event_id TEXT UNIQUE,
            event_type TEXT,
            repo_id BIGINT,
            repo_name TEXT,
            repo_url TEXT,
            actor_id BIGINT,
            actor_login TEXT,
            actor_url TEXT,
            actor_avatar TEXT,
            payload_ref TEXT,
            payload_head TEXT,
            payload_before TEXT,
            push_id BIGINT,
            public BOOLEAN,
            created_at TIMESTAMP,
            ingestion_ts TIMESTAMP DEFAULT NOW()
        );
        """

        with psycopg2.connect(**self.db_config) as conn:
            with conn.cursor() as cur:
                cur.execute(create_table_query)
        logger.info("Database table initialized")

    def store_event(self, event):
        """Store a single event in PostgreSQL."""
        insert_query = """
        INSERT INTO github_events (
            event_id, event_type, 
            repo_id, repo_name, repo_url,
            actor_id, actor_login, actor_url, actor_avatar,
            payload_ref, payload_head, payload_before, push_id,
            public, created_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (event_id) DO NOTHING
        """

        try:
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        insert_query,
                        (
                            event['id'],
                            event['type'],
                            event['repo']['id'],
                            event['repo']['name'],
                            event['repo']['url'],
                            event['actor']['id'],
                            event['actor']['login'],
                            event['actor']['url'],
                            event['actor']['avatar_url'],
                            event['payload'].get('ref'),
                            event['payload'].get('head'),
                            event['payload'].get('before'),
                            event['payload'].get('push_id'),
                            event['public'],
                            datetime.strptime(
                                event['created_at'], '%Y-%m-%dT%H:%M:%SZ')
                        )
                    )
            logger.info(f"Stored event {event['id']} in PostgreSQL")
        except Exception as e:
            logger.error(f"Error storing event in PostgreSQL: {e}")
            raise

    def stop(self):
        """Gracefully stop consuming."""
        self._running = False

    def start_consuming(self):
        """Start consuming messages from Kafka."""
        try:
            self.consumer.subscribe([self.topic])
            logger.info(f"Started consuming from topic: {self.topic}")

            while self._running:
                msg = self.consumer.poll(1.0)

                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        continue

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
    consumer = PostgresConsumer()
    consumer.start_consuming()


if __name__ == "__main__":
    main()
