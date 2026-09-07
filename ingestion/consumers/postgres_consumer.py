import os
import json
import logging
import requests
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
from datetime import datetime, timezone

import db
from db import EventRepository

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


class PostgresConsumer:
    def __init__(self):
        # Kafka configuration
        self.consumer = Consumer(
            {
                "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
                "group.id": "github_events_postgres_consumer",
                "auto.offset.reset": "earliest",
            }
        )
        self.topic = os.getenv("KAFKA_TOPIC")
        self._running = True

        self.repository = EventRepository()
        # Events are written in batches rather than one connection and one
        # INSERT per message. The flush interval bounds how long a validated
        # event can sit unwritten.
        self.batch_size = int(os.getenv("INGEST_BATCH_SIZE", "100"))
        self.flush_interval_seconds = float(os.getenv("INGEST_FLUSH_INTERVAL", "5"))
        self._pending = []
        self._last_flush = datetime.now(timezone.utc)

        # Validator endpoint (env-configurable)
        self.validator_url = os.getenv("VALIDATOR_URL", "http://validator:8000/validate")
        self.validator_timeout = float(os.getenv("VALIDATOR_TIMEOUT", "0.5"))

        # Initialize database
        self.init_db()

    def init_db(self):
        """Create any missing tables and indexes. The DDL lives in db/schema.py."""
        db.init_schema()

    def store_event(self, event):
        """Store a single event in PostgreSQL."""
        # Validate event with central validator service (fail-closed)
        # Try the primary validator URL, but allow fallbacks to support host-run consumers.
        fallback_env = os.getenv("VALIDATOR_FALLBACKS", "")
        fallback_list = [u.strip() for u in fallback_env.split(",") if u.strip()]
        # sensible defaults: localhost and host.docker.internal (useful on Docker for Windows)
        default_fallbacks = ["http://localhost:8000/validate", "http://host.docker.internal:8000/validate"]
        try_urls = [self.validator_url] + fallback_list + default_fallbacks

        status = None
        last_err = None
        for url in try_urls:
            try:
                resp = requests.post(url, json={"event": event}, timeout=self.validator_timeout)
                resp.raise_for_status()
                v = resp.json()
                status = v.get("status")
                # update validator_url to the working one for future calls
                self.validator_url = url
                break
            except requests.exceptions.RequestException as e:
                last_err = e
                logger.debug(f"Validator call to {url} failed: {e}")
                continue

        if status is None:
            logger.error(f"Validator call failed (fail-closed). Attempts: {try_urls}. Last error: {last_err}")
            # Fail-closed: do not store the event if validator is unavailable
            return

        if status == "FAIL":
            logger.info(f"Event {event.get('id')} failed validation. Skipping insert.")
            return

        self._pending.append(
            {
                "event_id": event["id"],
                "raw": event,
                "validation_status": status,
                "columns": {
                    "event_id": event["id"],
                    "event_type": event["type"],
                    "repo_id": event["repo"]["id"],
                    "repo_name": event["repo"]["name"],
                    "repo_url": event["repo"]["url"],
                    "actor_id": event["actor"]["id"],
                    "actor_login": event["actor"]["login"],
                    "actor_url": event["actor"]["url"],
                    "actor_avatar": event["actor"]["avatar_url"],
                    "payload_ref": event["payload"].get("ref"),
                    "payload_head": event["payload"].get("head"),
                    "payload_before": event["payload"].get("before"),
                    "push_id": event["payload"].get("push_id"),
                    "public": event["public"],
                    "created_at": _parse_created_at(event["created_at"]),
                },
            }
        )

        if len(self._pending) >= self.batch_size:
            self.flush()

    def flush(self):
        """Write pending events. Clears the buffer only on a successful write."""
        self._last_flush = datetime.now(timezone.utc)
        if not self._pending:
            return

        try:
            self.repository.save_batch(self._pending)
        except Exception as e:
            logger.error(f"Error storing {len(self._pending)} events in PostgreSQL: {e}")
            raise
        finally:
            # The batch is dropped either way: on success it is written, and on
            # failure retrying it would stall the consumer on a poison event.
            # Kafka offsets auto-commit on a timer regardless of what happened
            # here, so a failed batch is lost -- see IMPROVEMENTS.md F9, which
            # covers manual offset commits and a dead-letter path.
            self._pending = []

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
                    self._flush_if_due()
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

                self._flush_if_due()

        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        finally:
            try:
                self.flush()
            except Exception as e:
                logger.error(f"Final flush failed: {e}")
            self.consumer.close()
            db.close_pool()
            logger.info("Consumer closed")

    def _flush_if_due(self):
        """Flush once the buffer has been waiting longer than the interval."""
        age = (datetime.now(timezone.utc) - self._last_flush).total_seconds()
        if self._pending and age >= self.flush_interval_seconds:
            try:
                self.flush()
            except Exception:
                # Already logged in flush(); keep consuming rather than dying
                # on one bad batch.
                pass


def _parse_created_at(value):
    """Parse GitHub's event timestamp as UTC.

    GitHub sends whole-second Zulu time, but fromisoformat also accepts
    fractional seconds and explicit offsets -- a fixed "%Y-%m-%dT%H:%M:%SZ"
    raised ValueError on anything else, which dropped the event. test_e2e.py
    has always produced microsecond timestamps for exactly this reason.

    Returning an aware datetime keeps the value correct in the TIMESTAMPTZ
    column regardless of the database's own timezone.
    """
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def main():
    consumer = PostgresConsumer()
    consumer.start_consuming()


if __name__ == "__main__":
    main()
