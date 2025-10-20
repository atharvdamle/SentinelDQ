# SentinelDQ Ingestion Service

This service is responsible for ingesting GitHub events data into the SentinelDQ pipeline. It uses a Kafka-based architecture with specialized consumers for different storage backends.

## Architecture

```
┌──────────────┐    ┌─────────┐    ┌─────────────────┐
│  GitHub API  │───►│  Kafka  │◄───┤ PostgresConsumer│──►PostgreSQL
└──────────────┘    │ (KRaft) │    └─────────────────┘
                    └─────────┘    ┌─────────────────┐
                         ▲        │  MinIOConsumer  │──►MinIO
                         │         └─────────────────┘
                         │
                    ┌─────────┐
                    │Producer │
                    └─────────┘
```

### Components
- **GitHub Events Producer**: Fetches events from GitHub's public API
- **PostgreSQL Consumer**: Stores structured event data for analysis
- **MinIO Consumer**: Creates event backups in S3-compatible storage

## Prerequisites

- Python 3.13+
- Docker and Docker Compose
- GitHub API Token (optional, but recommended)

## Setup

1. Create a Python virtual environment:
```bash
python -m venv .venv
.\.venv\Scripts\Activate.ps1  # On Windows
source .venv/bin/activate     # On Unix
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure environment:
```bash
# Copy the example environment file
cp .env.example .env
# Edit .env with your settings
```

4. Start infrastructure:
```bash
docker-compose up -d
```

This starts:
- Kafka (KRaft mode)
- PostgreSQL
- MinIO

## Running the Pipeline

Use the supervisor script to manage all components:
```bash
python run_pipeline.py
```

This will start:
1. GitHub Events Producer
2. PostgreSQL Consumer
3. MinIO Consumer

Each component has color-coded logging for easy monitoring.

## Infrastructure Details

### Kafka
- Bootstrap Server: localhost:9092
- Topic: github_events
- Mode: KRaft (no Zookeeper required)

### PostgreSQL
- Host: localhost
- Port: 5432
- Database: SentinelDQ_DB
- Credentials: configured in .env

### MinIO
- API Endpoint: localhost:9000
- Console UI: localhost:9001
- Default Credentials: minioadmin/minioadmin
- Bucket: github-events-backup

## Data Flow

1. The GitHub producer polls the GitHub API (configurable interval)
2. Events are published to Kafka topic 'github_events'
3. Two specialized consumers process the events:
   - PostgreSQL Consumer: Stores normalized data for analysis
   - MinIO Consumer: Creates JSON backups with detailed logging

## Project Structure
```
SentinelDQ/
├── ingestion/
│   ├── producers/
│   │   └── github_producer.py    # GitHub Events Producer
│   ├── consumers/
│   │   ├── postgres_consumer.py  # PostgreSQL storage
│   │   └── minio_consumer.py     # S3 backup storage
│   └── requirements.txt          # Python dependencies
├── docker-compose.yml           # Infrastructure setup
├── .env                        # Configuration
└── run_pipeline.py             # Supervisor script
```

## Environment Variables

### Kafka Configuration
- KAFKA_BOOTSTRAP_SERVERS
- KAFKA_TOPIC

### PostgreSQL Configuration
- POSTGRES_HOST
- POSTGRES_PORT
- POSTGRES_DB
- POSTGRES_USER
- POSTGRES_PASSWORD

### MinIO Configuration
- MINIO_HOST
- MINIO_API_PORT
- MINIO_CONSOLE_PORT
- MINIO_ACCESS_KEY
- MINIO_SECRET_KEY
- MINIO_BUCKET
- MINIO_SECURE

### GitHub Configuration
- GITHUB_EVENTS_URL
- GITHUB_POLL_INTERVAL_SECONDS
- GITHUB_TOKEN (optional)

## Monitoring & Maintenance

### MinIO Data Access
1. Web Console:
   - Access at http://localhost:9001
   - Login with credentials from .env

2. MinIO Client (mc):
```powershell
# Configure local endpoint
mc.exe alias set local http://localhost:9000 minioadmin minioadmin

# List buckets
mc.exe ls local

# Browse backup data
mc.exe ls local/github-events-backup/
```

3. Programmatic Access:
   - Uses AWS S3 compatible API
   - Configure with endpoint: http://localhost:9000

### Logs and Monitoring
- All components use structured logging
- Color-coded output in supervisor
- Detailed MinIO operation logging
- Processing statistics every 100 messages

## Troubleshooting

### Common Issues

1. MinIO Connection Issues:
   - Verify ports 9000/9001 are accessible
   - Check MinIO container logs
   - Ensure correct endpoint configuration

2. Kafka Issues:
   - Verify KRaft configuration
   - Check consumer group status
   - Monitor topic partitions

3. PostgreSQL Issues:
   - Verify database exists
   - Check table schema
   - Monitor connection pool

### Debug Commands

```powershell
# Check service status
docker-compose ps

# View logs
docker-compose logs -f

# Test MinIO connection
Test-NetConnection -ComputerName localhost -Port 9000
```
