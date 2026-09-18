import os
import json
import time
import signal
import logging
import requests
from collections import OrderedDict
from confluent_kafka import Producer
from dotenv import load_dotenv
from pythonjsonlogger import jsonlogger

# Configure logging
log_handler = logging.StreamHandler()
log_handler.setFormatter(jsonlogger.JsonFormatter())
logging.basicConfig(level=logging.INFO, handlers=[log_handler])
logger = logging.getLogger(__name__)

# The events endpoint exposes 10 pages of 30; there is nothing beyond that.
MAX_PAGES = 10
# Enough id history to outlast many polls, since one poll can add at most 300.
SEEN_IDS_CAPACITY = 5000


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

        # Cursor state. The feed is ordered by id, and ids -- unlike created_at
        # -- are monotonic with that order, so they are the only safe cursor.
        self._etag = None
        self._seen_ids = OrderedDict()
        self.gap_count = 0
        self._next_poll_delay = self.poll_interval

    def fetch_events(self):
        """Fetch events published since the last poll, newest first.

        Pages until one overlaps an already-seen id; MAX_PAGES without an
        overlap means events were missed, and counts a gap.
        """
        self._next_poll_delay = self.poll_interval
        bootstrap = not self._seen_ids

        new_events = []
        collected = set()
        caught_up = False
        interrupted = False
        pages_read = 0
        url = self.api_url

        for page in range(1, MAX_PAGES + 1):
            response = self._get(url, conditional=(page == 1))
            if response is None:
                interrupted = True
                break
            if response.status_code == 304:
                caught_up = True
                break
            if page == 1:
                self._etag = response.headers.get("ETag")

            events = response.json()
            pages_read += 1
            caught_up = not events or any(str(e["id"]) in self._seen_ids for e in events)
            for event in events:
                event_id = str(event["id"])
                if event_id not in self._seen_ids and event_id not in collected:
                    collected.add(event_id)
                    new_events.append(event)

            url = response.links.get("next", {}).get("url")
            # The first poll has no previous poll to be gapped from, so page 1
            # is the whole of it -- paginating there would only burn requests.
            if caught_up or bootstrap or not url:
                break

        # A failed or rate-limited request proves nothing about a gap and is
        # logged on its own, so it must not pollute the counter -- that number
        # is only useful while it means "the poll interval is too long".
        if not caught_up and not bootstrap and not interrupted:
            self.gap_count += 1
            logger.warning(
                f"Feed outran the {self.poll_interval}s poll interval: read {pages_read} pages "
                f"without reaching a known event, so earlier events were missed "
                f"(gap #{self.gap_count}). Shorten the interval."
            )

        logger.info(f"Fetched {len(new_events)} new events from GitHub API")
        return new_events

    def _get(self, url, conditional):
        """GET one page, or None if it failed or the rate limit is exhausted.

        `conditional` sends If-None-Match, so an unchanged feed 304s for free.
        """
        headers = dict(self.headers)
        if conditional and self._etag:
            headers["If-None-Match"] = self._etag

        try:
            response = requests.get(url, headers=headers)
        except Exception as e:
            logger.error(f"Error fetching events from GitHub API: {e}")
            return None

        if poll_interval := response.headers.get("X-Poll-Interval"):
            self._next_poll_delay = max(self.poll_interval, float(poll_interval))

        if response.status_code == 304:
            return response
        if self._is_rate_limited(response):
            self._hold_for_rate_limit(response)
            return None

        try:
            response.raise_for_status()
        except Exception as e:
            logger.error(f"Error fetching events from GitHub API: {e}")
            return None
        return response

    @staticmethod
    def _is_rate_limited(response):
        """True for a 403/429 the API told us to wait out, not a bad token."""
        if response.status_code not in (403, 429):
            return False
        return bool(response.headers.get("Retry-After")) or response.headers.get("X-RateLimit-Remaining") == "0"

    def _hold_for_rate_limit(self, response):
        """Defer the next poll past the rate-limit reset."""
        if retry_after := response.headers.get("Retry-After"):
            wait = float(retry_after)
        elif reset := response.headers.get("X-RateLimit-Reset"):
            wait = float(reset) - time.time()
        else:
            wait = self.poll_interval

        self._next_poll_delay = max(wait, 0) + 1
        logger.warning(f"Rate limited by the GitHub API; pausing polling for {self._next_poll_delay:.0f}s")

    def _mark_seen(self, event_id):
        """Record a published id, evicting the oldest at capacity."""
        self._seen_ids[event_id] = None
        if len(self._seen_ids) > SEEN_IDS_CAPACITY:
            self._seen_ids.popitem(last=False)

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
                # Only now is the event someone else's problem; an id marked
                # seen is never fetched again.
                self._mark_seen(str(event["id"]))
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
        deadline = time.monotonic() + self._next_poll_delay
        while self._running and time.monotonic() < deadline:
            time.sleep(1)


def main():
    producer = GitHubEventsProducer()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: producer.stop())
    producer.run()


if __name__ == "__main__":
    main()
