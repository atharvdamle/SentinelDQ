import unittest
from unittest.mock import patch, MagicMock
import json
from datetime import datetime
from ingestion.consumers.postgres_consumer import PostgresConsumer

class TestPostgresConsumer(unittest.TestCase):
    def setUp(self):
        self.mock_env = {
            'KAFKA_BOOTSTRAP_SERVERS': 'localhost:9092',
            'KAFKA_TOPIC': 'github_events',
            'POSTGRES_DB': 'testdb',
            'POSTGRES_USER': 'testuser',
            'POSTGRES_PASSWORD': 'testpass',
            'POSTGRES_HOST': 'localhost',
            'POSTGRES_PORT': '5432'
        }
        self.mock_event = {
            'id': '12345',
            'type': 'PushEvent',
            'repo': {
                'id': 98765,
                'name': 'test/repo',
                'url': 'https://api.github.com/repos/test/repo'
            },
            'actor': {
                'id': 11111,
                'login': 'testuser',
                'url': 'https://api.github.com/users/testuser',
                'avatar_url': 'https://avatars.githubusercontent.com/u/11111'
            },
            'payload': {
                'ref': 'refs/heads/main',
                'head': 'abcdef123',
                'before': '123456789',
                'push_id': 987654321
            },
            'public': True,
            'created_at': '2025-10-20T12:00:00Z'
        }

    @patch('psycopg2.connect')
    def test_init_db(self, mock_connect):
        # Mock cursor and connection
        mock_cursor = MagicMock()
        mock_connection = MagicMock()
        mock_connection.__enter__.return_value = mock_connection
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        # Initialize consumer
        with patch.dict('os.environ', self.mock_env):
            consumer = PostgresConsumer()

        # Verify table creation query was executed
        mock_cursor.execute.assert_called_once()
        create_table_query = mock_cursor.execute.call_args[0][0]
        self.assertIn('CREATE TABLE IF NOT EXISTS github_events', create_table_query)
        mock_connection.commit.assert_called_once()

    @patch('psycopg2.connect')
    def test_store_event_success(self, mock_connect):
        # Mock cursor and connection
        mock_cursor = MagicMock()
        mock_connection = MagicMock()
        mock_connection.__enter__.return_value = mock_connection
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        # Initialize consumer and store event
        with patch.dict('os.environ', self.mock_env):
            consumer = PostgresConsumer()
            consumer.store_event(self.mock_event)

        # Verify insert query was executed with correct parameters
        mock_cursor.execute.assert_called()
        _, kwargs = mock_cursor.execute.call_args
        args = kwargs.get('parameters', mock_cursor.execute.call_args[0][1])
        
        # Verify all required fields are present in the correct order
        self.assertEqual(args[0], self.mock_event['id'])
        self.assertEqual(args[1], self.mock_event['type'])
        self.assertEqual(args[2], self.mock_event['repo']['id'])
        self.assertEqual(args[3], self.mock_event['repo']['name'])
        self.assertEqual(args[4], self.mock_event['repo']['url'])
        self.assertEqual(args[5], self.mock_event['actor']['id'])
        self.assertEqual(args[6], self.mock_event['actor']['login'])
        
        # Verify commit was called
        mock_connection.commit.assert_called_once()

    @patch('psycopg2.connect')
    def test_store_event_failure(self, mock_connect):
        # Mock database error
        mock_connect.side_effect = Exception('Database error')

        # Initialize consumer
        with patch.dict('os.environ', self.mock_env):
            consumer = PostgresConsumer()
            
            # Verify store_event raises exception
            with self.assertRaises(Exception):
                consumer.store_event(self.mock_event)

    @patch('confluent_kafka.Consumer')
    @patch('psycopg2.connect')
    def test_start_consuming(self, mock_connect, mock_consumer):
        # Mock Kafka consumer
        mock_kafka_consumer = MagicMock()
        mock_consumer.return_value = mock_kafka_consumer

        # Mock a Kafka message
        mock_message = MagicMock()
        mock_message.error.return_value = None
        mock_message.value.return_value = json.dumps(self.mock_event).encode()

        # Set up consumer to return one message then raise KeyboardInterrupt
        mock_kafka_consumer.poll.side_effect = [
            mock_message,
            KeyboardInterrupt
        ]

        # Initialize consumer and start consuming
        with patch.dict('os.environ', self.mock_env):
            consumer = PostgresConsumer()
            consumer.store_event = MagicMock()  # Mock store_event to avoid DB calls
            consumer.start_consuming()

        # Verify consumer was subscribed to correct topic
        mock_kafka_consumer.subscribe.assert_called_once_with([self.mock_env['KAFKA_TOPIC']])
        
        # Verify store_event was called with correct event
        consumer.store_event.assert_called_once()
        args = consumer.store_event.call_args[0]
        self.assertEqual(args[0], self.mock_event)

        # Verify consumer was closed
        mock_kafka_consumer.close.assert_called_once()

if __name__ == '__main__':
    unittest.main()