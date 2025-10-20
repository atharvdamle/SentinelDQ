import unittest
from unittest.mock import patch, MagicMock
import json
from ingestion.producers.github_producer import GitHubEventsProducer

class TestGitHubEventsProducer(unittest.TestCase):
    def setUp(self):
        self.mock_env = {
            'GITHUB_EVENTS_URL': 'https://api.github.com/events',
            'KAFKA_BOOTSTRAP_SERVERS': 'localhost:9092',
            'KAFKA_TOPIC': 'github_events',
            'GITHUB_TOKEN': 'test_token',
            'GITHUB_POLL_INTERVAL_SECONDS': '60'
        }
        with patch.dict('os.environ', self.mock_env):
            self.producer = GitHubEventsProducer()

    @patch('requests.get')
    def test_fetch_events_success(self, mock_get):
        # Prepare mock response
        mock_events = [
            {'id': '1', 'type': 'PushEvent'},
            {'id': '2', 'type': 'PullRequestEvent'}
        ]
        mock_response = MagicMock()
        mock_response.json.return_value = mock_events
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        # Test fetch_events method
        events = self.producer.fetch_events()
        
        # Verify results
        self.assertEqual(events, mock_events)
        mock_get.assert_called_once_with(
            self.mock_env['GITHUB_EVENTS_URL'],
            headers={
                'Accept': 'application/vnd.github.v3+json',
                'Authorization': f"Bearer {self.mock_env['GITHUB_TOKEN']}"
            }
        )

    @patch('requests.get')
    def test_fetch_events_failure(self, mock_get):
        # Simulate API error
        mock_get.side_effect = Exception('API Error')

        # Test fetch_events method
        events = self.producer.fetch_events()
        
        # Verify empty list is returned on error
        self.assertEqual(events, [])

    @patch('confluent_kafka.Producer.produce')
    def test_produce_events(self, mock_produce):
        # Mock fetch_events to return test data
        test_events = [
            {'id': '1', 'type': 'PushEvent'},
            {'id': '2', 'type': 'PullRequestEvent'}
        ]
        self.producer.fetch_events = MagicMock(return_value=test_events)

        # Test produce_events method
        self.producer.produce_events()

        # Verify produce was called for each event
        self.assertEqual(mock_produce.call_count, len(test_events))
        
        # Verify produce calls had correct arguments
        calls = mock_produce.call_args_list
        for i, call in enumerate(calls):
            args, kwargs = call
            self.assertEqual(kwargs['topic'], self.mock_env['KAFKA_TOPIC'])
            self.assertEqual(kwargs['key'], str(test_events[i]['id']))
            self.assertEqual(kwargs['value'], json.dumps(test_events[i]))

    def test_delivery_report_success(self):
        # Test successful delivery
        mock_msg = MagicMock()
        mock_msg.topic.return_value = 'test_topic'
        mock_msg.partition.return_value = 0

        with self.assertLogs(level='DEBUG') as log:
            self.producer.delivery_report(None, mock_msg)
            self.assertIn('Message delivered', log.output[0])

    def test_delivery_report_failure(self):
        # Test failed delivery
        with self.assertLogs(level='ERROR') as log:
            self.producer.delivery_report(Exception('Delivery failed'), None)
            self.assertIn('Message delivery failed', log.output[0])

if __name__ == '__main__':
    unittest.main()