import unittest
from unittest.mock import patch, MagicMock
import json
import os
import signal
from ingestion.producers.github_producer import REQUEST_TIMEOUT_SECONDS, GitHubEventsProducer, main


class TestGitHubEventsProducer(unittest.TestCase):
    def setUp(self):
        # Patch the name the module bound at import. An idempotent producer
        # acquires its id eagerly, so an unpatched client dials localhost:9092
        # for real on construction.
        self.producer_patcher = patch("ingestion.producers.github_producer.Producer")
        self.producer_patcher.start()
        self.addCleanup(self.producer_patcher.stop)

        self.mock_env = {
            "GITHUB_EVENTS_URL": "https://api.github.com/events",
            "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
            "KAFKA_TOPIC": "github_events",
            "GITHUB_TOKEN": "test_token",
            "GITHUB_POLL_INTERVAL_SECONDS": "60",
        }
        with patch.dict("os.environ", self.mock_env):
            self.producer = GitHubEventsProducer()
        # Requests now go through a pooled session, so patch that, not the
        # module-level requests.get.
        self.producer.session = MagicMock()

    def response(self, status_code=200, events=(), headers=None, links=None):
        """A stubbed GitHub API response for the session to return."""
        mock_response = MagicMock()
        mock_response.status_code = status_code
        mock_response.json.return_value = list(events)
        mock_response.raise_for_status.return_value = None
        mock_response.headers = headers or {}
        mock_response.links = links or {}
        return mock_response

    def test_fetch_events_success(self):
        # Prepare mock response
        mock_events = [{"id": "1", "type": "PushEvent"}, {"id": "2", "type": "PullRequestEvent"}]
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_events
        mock_response.raise_for_status.return_value = None
        mock_response.headers = {}
        self.producer.session.get.return_value = mock_response

        # Test fetch_events method
        events = self.producer.fetch_events()

        # Verify results
        self.assertEqual(events, mock_events)
        # A request without a timeout hangs the poll loop forever.
        self.producer.session.get.assert_called_once_with(
            self.mock_env["GITHUB_EVENTS_URL"],
            headers={
                "Accept": "application/vnd.github.v3+json",
                "Authorization": f"Bearer {self.mock_env['GITHUB_TOKEN']}",
            },
            timeout=REQUEST_TIMEOUT_SECONDS,
        )

    def test_fetch_events_failure(self):
        # Simulate API error
        self.producer.session.get.side_effect = Exception("API Error")

        # Test fetch_events method
        events = self.producer.fetch_events()

        # Verify empty list is returned on error
        self.assertEqual(events, [])

    def test_an_unchanged_feed_publishes_nothing(self):
        """A conditional request that 304s is free and carries no events."""
        self.producer._etag = 'W/"cafebabe"'
        self.producer._seen_ids["1"] = None
        self.producer.session.get.return_value = self.response(status_code=304)

        events = self.producer.fetch_events()

        self.assertEqual(events, [])
        self.assertEqual(self.producer.session.get.call_args[1]["headers"]["If-None-Match"], 'W/"cafebabe"')

    def test_already_seen_ids_are_not_republished(self):
        """Overlapping polls used to refetch ids the validator then FAILed."""
        self.producer._seen_ids["1"] = None
        self.producer.session.get.return_value = self.response(events=[{"id": "1"}, {"id": "2"}])

        events = self.producer.fetch_events()

        self.assertEqual([event["id"] for event in events], ["2"])

    def test_a_rate_limited_response_defers_the_next_poll(self):
        """403 used to return [] and retry blindly a minute later."""
        self.producer.session.get.return_value = self.response(status_code=403, headers={"Retry-After": "120"})

        events = self.producer.fetch_events()

        self.assertEqual(events, [])
        self.assertEqual(self.producer._next_poll_delay, 121)

    def test_sigterm_stops_the_poll_loop_and_flushes(self):
        """SIGTERM never reached the producer, so a poll was always lost."""
        for sig in (signal.SIGTERM, signal.SIGINT):
            self.addCleanup(signal.signal, sig, signal.getsignal(sig))
        mock_kafka = MagicMock()
        self.producer.producer = mock_kafka
        self.producer.fetch_events = MagicMock(side_effect=lambda: os.kill(os.getpid(), signal.SIGTERM) or [])

        with patch("ingestion.producers.github_producer.GitHubEventsProducer", return_value=self.producer):
            main()

        self.assertFalse(self.producer._running)
        mock_kafka.flush.assert_called_with()

    def test_a_failed_delivery_leaves_the_id_unseen(self):
        """An id marked seen at enqueue time would never be refetched."""
        mock_msg = MagicMock()
        mock_msg.key.return_value = b"1"

        self.producer.delivery_report(Exception("Delivery failed"), mock_msg)
        self.assertNotIn("1", self.producer._seen_ids)

        self.producer.delivery_report(None, mock_msg)
        self.assertIn("1", self.producer._seen_ids)

    def test_a_full_queue_is_drained_and_retried(self):
        """BufferError is backpressure; poll() frees the slots again."""
        mock_producer = MagicMock()
        mock_producer.produce.side_effect = [BufferError("queue full"), None]
        self.producer.producer = mock_producer

        self.producer._produce({"id": "1"})

        self.assertEqual(mock_producer.produce.call_count, 2)
        mock_producer.poll.assert_any_call(1)

    def test_produce_events(self):
        # Mock fetch_events to return test data
        test_events = [{"id": "1", "type": "PushEvent"}, {"id": "2", "type": "PullRequestEvent"}]
        self.producer.fetch_events = MagicMock(return_value=test_events)

        # Mock the Kafka producer instance's produce method
        mock_producer = MagicMock()
        self.producer.producer = mock_producer

        # Test produce_events method
        self.producer.produce_events()

        # Verify produce was called for each event
        self.assertEqual(mock_producer.produce.call_count, len(test_events))

        # Verify produce calls had correct arguments
        calls = mock_producer.produce.call_args_list
        for i, call in enumerate(calls):
            args, kwargs = call
            # topic is first positional arg
            self.assertEqual(args[0], self.mock_env["KAFKA_TOPIC"])
            self.assertEqual(kwargs["key"], str(test_events[i]["id"]))
            self.assertEqual(kwargs["value"], json.dumps(test_events[i]))

    def test_delivery_report_success(self):
        # Test successful delivery
        mock_msg = MagicMock()
        mock_msg.topic.return_value = "test_topic"
        mock_msg.partition.return_value = 0

        with self.assertLogs(level="DEBUG") as log:
            self.producer.delivery_report(None, mock_msg)
            self.assertIn("Message delivered", log.output[0])

    def test_delivery_report_failure(self):
        # Test failed delivery
        with self.assertLogs(level="ERROR") as log:
            self.producer.delivery_report(Exception("Delivery failed"), None)
            self.assertIn("Message delivery failed", log.output[0])


if __name__ == "__main__":
    unittest.main()
