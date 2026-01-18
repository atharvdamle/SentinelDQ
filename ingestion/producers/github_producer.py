import os
import json
import time
import logging
import schedule
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


def main():
    producer = GitHubEventsProducer()
    interval = int(os.getenv("GITHUB_EVENTS_FETCH_INTERVAL", 60))

    # Schedule the job
    schedule.every(interval).seconds.do(producer.produce_events)

    # Run immediately once
    producer.produce_events()

    # Keep running
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    main()
