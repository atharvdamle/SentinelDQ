import os
import json
import time
import signal
import logging
import requests
from confluent_kafka import Producer
from dotenv import load_dotenv
from pythonjsonlogger import jsonlogger

# Configure logging
log_handler = logging.StreamHandler()
log_handler.setFormatter(jsonlogger.JsonFormatter())
logging.basicConfig(level=logging.INFO, handlers=[log_handler])
logger = logging.getLogger(__name__)


class GitHubEventsProducer:
    def __init__(self):
        self.api_url = os.getenv("GITHUB_EVENTS_URL")
        self.poll_interval = int(os.getenv("GITHUB_POLL_INTERVAL_SECONDS", "60"))
        self.headers = {"Accept": "application/vnd.github.v3+json"}
        if github_token := os.getenv("GITHUB_TOKEN"):
            self.headers["Authorization"] = f"Bearer {github_token}"

        self.producer = Producer(
            {"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"), "client.id": "github_events_producer"}
        )
        self.topic = os.getenv("KAFKA_TOPIC")
        self._running = True

    def fetch_events(self):
        """Fetch events from GitHub API."""
        try:
            response = requests.get(self.api_url, headers=self.headers)
            response.raise_for_status()
            events = response.json()
            logger.info(f"Fetched {len(events)} events from GitHub API")
            return events
        except Exception as e:
            logger.error(f"Error fetching events from GitHub API: {e}")
            return []

    def delivery_report(self, err, msg):
        """Callback for Kafka producer delivery reports."""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def produce_events(self):
        """Fetch and produce GitHub events to Kafka."""
        events = self.fetch_events()

        for event in events:
            try:
                self.producer.produce(
                    self.topic, key=str(event["id"]), value=json.dumps(event), callback=self.delivery_report
                )
                self.producer.poll(0)  # Trigger delivery reports
            except Exception as e:
                logger.error(f"Error producing event to Kafka: {e}")

        self.producer.flush()
        logger.info(f"Produced {len(events)} events to Kafka topic {self.topic}")

    def stop(self):
        """Gracefully stop polling."""
        self._running = False

    def run(self):
        """Poll GitHub on a fixed interval until stopped."""
        try:
            while self._running:
                self.produce_events()
                self._sleep_until_next_poll()
        finally:
            self.producer.flush()
            logger.info("Producer stopped")

    def _sleep_until_next_poll(self):
        """Sleep in short slices so a stop signal is noticed promptly."""
        deadline = time.monotonic() + self.poll_interval
        while self._running and time.monotonic() < deadline:
            time.sleep(1)


def main():
    producer = GitHubEventsProducer()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: producer.stop())
    producer.run()


if __name__ == "__main__":
    main()
