import os
import json
import time
import signal
import logging
import requests
from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition
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
        # Kafka configuration. Offsets are committed by hand, after the write.
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        self.consumer = Consumer(
            {
                "bootstrap.servers": self.bootstrap_servers,
                "group.id": "github_events_postgres_consumer",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        self.topic = os.getenv("KAFKA_TOPIC")
        self.dlq_topic = f"{self.topic}.dlq"
        self._dlq_producer = None
        self._running = True

        self.repository = EventRepository()
        self.batch_size = int(os.getenv("INGEST_BATCH_SIZE", "100"))
        self.flush_interval_seconds = float(os.getenv("INGEST_FLUSH_INTERVAL", "5"))
        self.max_write_attempts = 3
        self.retry_backoff_seconds = 1.0
        self._pending = []
        self._offsets = {}
        self._last_flush = datetime.now(timezone.utc)

        # Validator endpoint (env-configurable)
        self.validator_url = os.getenv("VALIDATOR_URL", "http://validator:8000/validate")
        self.validator_timeout = float(os.getenv("VALIDATOR_TIMEOUT", "0.5"))

        # Initialize database
        self.init_db()

    def init_db(self):
        """Create any missing tables and indexes. The DDL lives in db/schema.py."""
        db.init_schema()

    def store_event(self, event, msg):
        """Buffer a single event for the next write to PostgreSQL.

        Returns False only when the validator was unreachable, so the offset
        must not advance. A FAIL is skipped unless its sole cause is the
        duplicate check (see _is_duplicate_redelivery).
        """
        fallback_env = os.getenv("VALIDATOR_FALLBACKS", "")
        fallback_list = [u.strip() for u in fallback_env.split(",") if u.strip()]
        default_fallbacks = ["http://localhost:8000/validate", "http://host.docker.internal:8000/validate"]
        try_urls = [self.validator_url] + fallback_list + default_fallbacks

        status = None
        failures = []
        last_err = None
        for url in try_urls:
            try:
                resp = requests.post(url, json={"event": event}, timeout=self.validator_timeout)
                resp.raise_for_status()
                v = resp.json()
                status = v.get("status")
                failures = v.get("failures", [])
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
            return False

        self._offsets[(msg.topic(), msg.partition())] = msg.offset() + 1

        if status == "FAIL" and not _is_duplicate_redelivery(failures):
            logger.info(f"Event {event.get('id')} failed validation. Skipping insert.")
            return True

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

        return True

    def flush(self):
        """Write pending events, then commit the offsets they cover.

        The buffer survives a failed write, so a database outage costs nothing
        but latency. Only a batch that has exhausted its retries and reached
        the dead-letter topic is dropped.
        """
        self._last_flush = datetime.now(timezone.utc)

        if self._pending:
            if not self._write_with_retries() and not self._dead_letter():
                return
            self._pending = []

        self._commit()

    def _write_with_retries(self):
        """Write the buffer, backing off between attempts. True once written."""
        delay = self.retry_backoff_seconds
        for attempt in range(1, self.max_write_attempts + 1):
            try:
                self.repository.save_batch(self._pending)
                return True
            except Exception as e:
                logger.error(
                    f"Error storing {len(self._pending)} events in PostgreSQL "
                    f"(attempt {attempt}/{self.max_write_attempts}): {e}"
                )
                if attempt < self.max_write_attempts:
                    time.sleep(delay)
                    delay *= 2
        return False

    def _dead_letter(self):
        """Publish the unwritable buffer to the DLQ. True once it is safely there."""
        try:
            if self._dlq_producer is None:
                self._dlq_producer = Producer({"bootstrap.servers": self.bootstrap_servers})
            for row in self._pending:
                self._dlq_producer.produce(self.dlq_topic, json.dumps(row["raw"]).encode("utf-8"))
            undelivered = self._dlq_producer.flush(30)
            if undelivered:
                raise RuntimeError(f"{undelivered} messages still queued")
        except Exception as e:
            logger.error(f"Dead-lettering to {self.dlq_topic} failed, retaining {len(self._pending)} events: {e}")
            return False

        logger.error(
            f"Dead-lettered {len(self._pending)} events to {self.dlq_topic} "
            f"after {self.max_write_attempts} failed writes"
        )
        return True

    def _commit(self):
        """Commit the offsets of every message the pipeline has finished with."""
        if not self._offsets:
            return

        offsets = [TopicPartition(topic, partition, offset) for (topic, partition), offset in self._offsets.items()]
        self.consumer.commit(offsets=offsets, asynchronous=False)
        self._offsets = {}

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
                    handled = self.store_event(event, msg)
                except Exception as e:
                    logger.error(f"Error processing message at offset {msg.offset()}: {e}")
                    self._offsets[(msg.topic(), msg.partition())] = msg.offset() + 1
                    continue

                if not handled:
                    self._rewind(msg)

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

    def _rewind(self, msg):
        """Replay this message on the next poll, after pausing for the backoff.

        Buffered events sit at lower offsets, so they stay flushable and
        committable while this one is retried.
        """
        logger.warning(f"Rewinding to offset {msg.offset()} on partition {msg.partition()} to retry")
        self.consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
        time.sleep(self.retry_backoff_seconds)

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


def _is_duplicate_redelivery(failures):
    """True when the only critical failure is the validator's duplicate check.

    Redeliveries always FAIL that check; storing them is a no-op thanks to
    ON CONFLICT (event_id) DO NOTHING.
    """
    critical = [f for f in failures if f.get("severity") == "FAIL"]
    return bool(critical) and all(f.get("check_type") == "duplicate" for f in critical)


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
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: consumer.stop())
    consumer.start_consuming()


if __name__ == "__main__":
    main()
