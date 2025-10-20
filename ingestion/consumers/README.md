# GitHub Events Consumers

This directory contains two Kafka consumers that process GitHub events:

1. PostgreSQL Consumer (`postgres_consumer.py`)
2. MinIO Raw Backup Consumer (`minio_consumer.py`)

## Prerequisites

- Python 3.10+
- Running Kafka broker
- PostgreSQL database
- MinIO server
- `.env` file with proper configuration

## Environment Variables

Make sure your `.env` file contains the following variables:

```env
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=github_events

# PostgreSQL Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=sentineldq
POSTGRES_USER=user
POSTGRES_PASSWORD=password

# MinIO Configuration
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET_NAME=github-events-backup
```

## Running the Consumers

You can run both consumers simultaneously using separate terminal windows.

### Terminal 1 - PostgreSQL Consumer:
```bash
# Activate virtual environment (if not already activated)
source .venv/bin/activate  # On Windows: .\.venv\Scripts\activate

# Run PostgreSQL consumer
python -m consumers.postgres_consumer
```

### Terminal 2 - MinIO Consumer:
```bash
# Activate virtual environment (if not already activated)
source .venv/bin/activate  # On Windows: .\.venv\Scripts\activate

# Run MinIO consumer
python -m consumers.minio_consumer
```

## Data Flow

1. PostgreSQL Consumer:
   - Reads messages from Kafka topic
   - Deserializes JSON payload
   - Stores in `github_events` table with schema:
     - id (serial primary key)
     - event_id (string, unique)
     - event_type (string)
     - repo_name (string)
     - created_at (timestamp)
     - raw_payload (JSONB)
   - Ensures idempotency using unique constraint on event_id

2. MinIO Consumer:
   - Reads messages from same Kafka topic
   - Stores raw JSON in MinIO bucket
   - File path format: raw/YYYY-MM-DD/HH-MM-SS-UUID.json

## Monitoring

Both consumers use Python's logging module and output logs in the following format:
```
YYYY-MM-DD HH:MM:SS,mmm - consumer_name - LEVEL - Message
```

## Stopping the Consumers

To stop either consumer, press Ctrl+C in its terminal window. The consumer will gracefully shut down, closing its connections.
