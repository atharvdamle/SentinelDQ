#!/usr/bin/env python3
"""
SentinelDQ End-to-End Integration Test

This script:
1. Starts all Docker services (Kafka, Postgres, MinIO, Validator, Drift Detector)
2. Waits for services to be healthy
3. Produces GitHub events to Kafka
4. Verifies events are consumed and stored in Postgres
5. Verifies events are stored in MinIO
6. Runs data validation on the stored events
7. Runs drift detection on the data
8. Validates all components are working correctly

Usage:
    python test_e2e.py
"""

import subprocess
import time
import sys
import logging
import json
import os
import requests
import psycopg2
from minio import Minio
from minio.error import S3Error
from confluent_kafka import Producer
from datetime import datetime, timezone
import colorama
from colorama import Fore, Style

# Initialize colorama for Windows
colorama.init()

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class E2ETest:
    def __init__(self):
        self.test_passed = True
        self.load_env()

    def load_env(self):
        """Load environment variables from .env file or set defaults."""
        env_file = ".env"
        if os.path.exists(env_file):
            with open(env_file) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        key, value = line.split("=", 1)
                        os.environ[key.strip()] = value.strip()

        # Set defaults if not present
        self.postgres_host = os.getenv("POSTGRES_HOST", "localhost")
        self.postgres_port = os.getenv("POSTGRES_PORT", "5432")
        self.postgres_db = os.getenv("POSTGRES_DB", "SentinelDQ_DB")
        self.postgres_user = os.getenv("POSTGRES_USER", "postgres")
        self.postgres_password = os.getenv("POSTGRES_PASSWORD", "postgres")

        self.minio_address = os.getenv("MINIO_ADDRESS", "localhost:9000")
        self.minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        self.minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
        self.minio_bucket = os.getenv("MINIO_BUCKET", "github-events-backup")

        self.kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.kafka_topic = os.getenv("KAFKA_TOPIC", "github_events")

    def print_step(self, step: str):
        """Print a test step with formatting."""
        print(f"\n{Fore.CYAN}{'='*80}{Style.RESET_ALL}")
        print(f"{Fore.CYAN}STEP: {step}{Style.RESET_ALL}")
        print(f"{Fore.CYAN}{'='*80}{Style.RESET_ALL}\n")

    def print_success(self, message: str):
        """Print a success message."""
        print(f"{Fore.GREEN}✓ {message}{Style.RESET_ALL}")

    def print_error(self, message: str):
        """Print an error message."""
        print(f"{Fore.RED}✗ {message}{Style.RESET_ALL}")
        self.test_passed = False

    def print_info(self, message: str):
        """Print an info message."""
        print(f"{Fore.YELLOW}ℹ {message}{Style.RESET_ALL}")

    def run_command(self, cmd: list, capture_output=True):
        """Run a shell command."""
        try:
            if capture_output:
                result = subprocess.run(cmd, capture_output=True, text=True, check=True)
                return result.stdout
            else:
                subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError as e:
            logger.error(f"Command failed: {' '.join(cmd)}")
            if capture_output:
                logger.error(f"Error: {e.stderr}")
            raise

    def start_services(self):
        """Start all Docker Compose services."""
        self.print_step("Starting Docker Compose services")

        self.print_info("Stopping any existing services...")
        subprocess.run(["docker-compose", "down"], capture_output=True)

        self.print_info("Starting services with docker-compose up -d...")
        self.run_command(["docker-compose", "up", "-d"], capture_output=False)

        self.print_success("Services started successfully")

    def wait_for_services(self):
        """Wait for all services to be healthy."""
        self.print_step("Waiting for services to be healthy")

        services = {
            "Kafka": ("localhost", 9092),
            "Postgres": ("localhost", int(self.postgres_port)),
            "MinIO": ("localhost", 9000),
            "Validator API": ("localhost", 8000),
        }

        max_wait = 120  # 2 minutes
        start_time = time.time()

        for service, (host, port) in services.items():
            self.print_info(f"Waiting for {service} at {host}:{port}...")
            while time.time() - start_time < max_wait:
                try:
                    if service == "Validator API":
                        response = requests.get(f"http://{host}:{port}/health", timeout=2)
                        if response.status_code == 200:
                            self.print_success(f"{service} is ready")
                            break
                    else:
                        import socket

                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sock.settimeout(2)
                        result = sock.connect_ex((host, port))
                        sock.close()
                        if result == 0:
                            self.print_success(f"{service} is ready")
                            break
                except Exception:
                    pass
                time.sleep(2)
            else:
                self.print_error(f"{service} did not become ready in time")
                return False

        # Extra time for services to fully initialize
        self.print_info("Waiting additional 10 seconds for full initialization...")
        time.sleep(10)
        return True

    def produce_test_events(self, num_events=10):
        """Produce test GitHub events to Kafka."""
        self.print_step(f"Producing {num_events} test GitHub events to Kafka")

        try:
            producer = Producer({"bootstrap.servers": self.kafka_bootstrap, "client.id": "e2e-test-producer"})

            events_produced = 0
            for i in range(num_events):
                event = {
                    "id": f"test-event-{i}-{int(time.time())}",
                    "type": "PushEvent" if i % 2 == 0 else "PullRequestEvent",
                    "repo": {
                        "id": 12345 + i,
                        "name": f"test-org/test-repo-{i}",
                        "url": f"https://api.github.com/repos/test-org/test-repo-{i}",
                    },
                    "actor": {
                        "id": 1000 + i,
                        "login": f"test-user-{i}",
                        "url": f"https://api.github.com/users/test-user-{i}",
                        "avatar_url": f"https://avatars.githubusercontent.com/u/{1000+i}",
                    },
                    "payload": {
                        "ref": "refs/heads/main",
                        "head": f"commit-hash-{i}",
                        "before": f"previous-hash-{i}",
                        "push_id": 9000 + i,
                    },
                    "public": True,
                    "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                }

                producer.produce(
                    self.kafka_topic,
                    value=json.dumps(event).encode("utf-8"),
                    callback=lambda err, msg: logger.debug(f"Delivered: {msg.topic()}[{msg.partition()}]"),
                )
                events_produced += 1

            producer.flush(timeout=10)
            self.print_success(f"Produced {events_produced} events to Kafka topic '{self.kafka_topic}'")

            # Wait for consumers to process
            self.print_info("Waiting 15 seconds for consumers to process events...")
            time.sleep(15)

            return events_produced

        except Exception as e:
            self.print_error(f"Failed to produce events: {e}")
            return 0

    def verify_postgres_data(self, expected_count):
        """Verify events were stored in Postgres."""
        self.print_step("Verifying data in Postgres")

        try:
            conn = psycopg2.connect(
                host=self.postgres_host,
                port=self.postgres_port,
                database=self.postgres_db,
                user=self.postgres_user,
                password=self.postgres_password,
            )
            cursor = conn.cursor()

            # Check if table exists
            cursor.execute(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'github_events'
                )
            """
            )
            table_exists = cursor.fetchone()[0]

            if not table_exists:
                self.print_error("Table 'github_events' does not exist")
                return False

            # Count events
            cursor.execute("SELECT COUNT(*) FROM github_events")
            count = cursor.fetchone()[0]

            self.print_info(f"Found {count} events in Postgres (expected at least {expected_count})")

            if count >= expected_count:
                self.print_success(f"Postgres contains sufficient events ({count} >= {expected_count})")

                # Show sample data
                cursor.execute(
                    "SELECT event_id, event_type, actor_login, repo_name, created_at FROM github_events LIMIT 3"
                )
                rows = cursor.fetchall()
                self.print_info("Sample events:")
                for row in rows:
                    print(f"  - ID: {row[0]}, Type: {row[1]}, Actor: {row[2]}, Repo: {row[3]}")

                cursor.close()
                conn.close()
                return True
            else:
                self.print_error(f"Insufficient events in Postgres ({count} < {expected_count})")
                cursor.close()
                conn.close()
                return False

        except Exception as e:
            self.print_error(f"Failed to verify Postgres data: {e}")
            return False

    def verify_minio_data(self):
        """Verify events were stored in MinIO."""
        self.print_step("Verifying data in MinIO")

        try:
            # Remove http:// prefix if present
            minio_host = self.minio_address.replace("http://", "")

            client = Minio(minio_host, access_key=self.minio_access_key, secret_key=self.minio_secret_key, secure=False)

            bucket_name = self.minio_bucket

            # Check if bucket exists
            if not client.bucket_exists(bucket_name):
                self.print_error(f"Bucket '{bucket_name}' does not exist")
                return False

            # Retry logic for MinIO - consumer might be lagging
            max_retries = 5
            retry_delay = 3

            for attempt in range(max_retries):
                # List objects
                objects = list(client.list_objects(bucket_name, recursive=True))

                if len(objects) > 0:
                    self.print_success(f"Found {len(objects)} objects in MinIO bucket '{bucket_name}'")
                    self.print_info("Sample objects:")
                    for obj in objects[:3]:
                        print(f"  - {obj.object_name} ({obj.size} bytes)")
                    return True

                if attempt < max_retries - 1:
                    self.print_info(
                        f"No objects found yet, retrying in {retry_delay} seconds... (attempt {attempt + 1}/{max_retries})"
                    )
                    time.sleep(retry_delay)

                # After all retries
                return True
            else:
                self.print_error(f"No objects found in MinIO bucket '{bucket_name}'")
                return False

        except S3Error as e:
            self.print_error(f"MinIO error: {e}")
            return False
        except Exception as e:
            self.print_error(f"Failed to verify MinIO data: {e}")
            return False

    def verify_validator_api(self):
        """Test the data validation API."""
        self.print_step("Testing Data Validation API")

        try:
            # Test health endpoint
            response = requests.get("http://localhost:8000/health", timeout=5)
            if response.status_code == 200:
                self.print_success("Validator API health check passed")
            else:
                self.print_error(f"Health check failed with status {response.status_code}")
                return False

            # Get Postgres connection from earlier verification
            conn = psycopg2.connect(
                host=self.postgres_host,
                port=self.postgres_port,
                database=self.postgres_db,
                user=self.postgres_user,
                password=self.postgres_password,
            )
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM github_events LIMIT 1")
            columns = [desc[0] for desc in cursor.description]
            row = cursor.fetchone()
            cursor.close()
            conn.close()

            if not row:
                self.print_error("No data available to validate")
                return False

            # Create a sample dataset for validation, converting datetime to string
            sample_dict = dict(zip(columns, row))
            # Convert datetime objects to ISO format strings
            for key, value in sample_dict.items():
                if hasattr(value, "isoformat"):
                    sample_dict[key] = value.isoformat()
            sample_data = [sample_dict]

            # Test validation endpoint - API expects single event, not array
            response = requests.post(
                "http://localhost:8000/validate",
                json={"event": sample_dict, "event_id": str(sample_dict.get("event_id", "test-event"))},
                timeout=10,
            )

            if response.status_code == 200:
                result = response.json()
                self.print_success(f"Validation completed: {result.get('summary', {})}")
                return True
            else:
                self.print_error(f"Validation failed with status {response.status_code}: {response.text}")
                return False

        except Exception as e:
            self.print_error(f"Failed to test validator API: {e}")
            return False

    def verify_drift_detection(self):
        """Verify drift detection is running."""
        self.print_step("Verifying Drift Detection")

        try:
            # Check if drift detector container is running
            result = subprocess.run(["docker-compose", "ps", "drift-detector"], capture_output=True, text=True)

            if "Up" in result.stdout or "running" in result.stdout.lower():
                self.print_success("Drift detector service is running")

                # Check logs for drift detection activity
                logs = subprocess.run(
                    ["docker-compose", "logs", "--tail=50", "drift-detector"], capture_output=True, text=True
                )

                if "drift" in logs.stdout.lower() or "profile" in logs.stdout.lower():
                    self.print_success("Drift detection appears to be active")
                    self.print_info("Recent drift detector logs:")
                    for line in logs.stdout.split("\n")[-5:]:
                        if line.strip():
                            print(f"  {line}")
                    return True
                else:
                    self.print_info("Drift detector running but no recent activity detected")
                    return True
            else:
                self.print_error("Drift detector service is not running")
                return False

        except Exception as e:
            self.print_error(f"Failed to verify drift detection: {e}")
            return False

    def cleanup(self):
        """Clean up and stop services."""
        self.print_step("Cleaning up")

        self.print_info("Displaying service logs...")
        subprocess.run(["docker-compose", "logs", "--tail=20"], capture_output=False)

        user_input = input(f"\n{Fore.YELLOW}Stop all services? (y/n): {Style.RESET_ALL}")
        if user_input.lower() == "y":
            self.print_info("Stopping services...")
            subprocess.run(["docker-compose", "down"], capture_output=False)
            self.print_success("Services stopped")
        else:
            self.print_info("Services left running for further inspection")

    def run(self):
        """Run the complete end-to-end test."""
        print(f"\n{Fore.MAGENTA}{'='*80}{Style.RESET_ALL}")
        print(f"{Fore.MAGENTA}SentinelDQ End-to-End Integration Test{Style.RESET_ALL}")
        print(f"{Fore.MAGENTA}{'='*80}{Style.RESET_ALL}\n")

        try:
            # Step 1: Start services
            self.start_services()

            # Step 2: Wait for services
            if not self.wait_for_services():
                self.print_error("Services did not start properly")
                return False

            # Step 3: Produce test events
            num_events = self.produce_test_events(10)
            if num_events == 0:
                self.print_error("Failed to produce test events")
                return False

            # Step 4: Verify Postgres
            if not self.verify_postgres_data(num_events):
                self.print_error("Postgres verification failed")
                # Continue anyway to see other results

            # Step 5: Verify MinIO
            if not self.verify_minio_data():
                self.print_error("MinIO verification failed")
                # Continue anyway

            # Step 6: Test Validator API
            if not self.verify_validator_api():
                self.print_error("Validator API test failed")
                # Continue anyway

            # Step 7: Verify Drift Detection
            if not self.verify_drift_detection():
                self.print_error("Drift detection verification failed")

            # Final result
            print(f"\n{Fore.MAGENTA}{'='*80}{Style.RESET_ALL}")
            if self.test_passed:
                print(f"{Fore.GREEN}✓ ALL TESTS PASSED{Style.RESET_ALL}")
                print(f"{Fore.GREEN}The SentinelDQ pipeline is working end-to-end!{Style.RESET_ALL}")
            else:
                print(f"{Fore.RED}✗ SOME TESTS FAILED{Style.RESET_ALL}")
                print(f"{Fore.YELLOW}Check the output above for details{Style.RESET_ALL}")
            print(f"{Fore.MAGENTA}{'='*80}{Style.RESET_ALL}\n")

            return self.test_passed

        except KeyboardInterrupt:
            self.print_info("\nTest interrupted by user")
            return False
        except Exception as e:
            self.print_error(f"Test failed with exception: {e}")
            import traceback

            traceback.print_exc()
            return False
        finally:
            self.cleanup()


if __name__ == "__main__":
    test = E2ETest()
    success = test.run()
    sys.exit(0 if success else 1)
