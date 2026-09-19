FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements and install
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy project
COPY . /app

ENV PYTHONPATH=/app
RUN useradd --create-home --uid 10001 sentinel
USER sentinel

# Default command (can be overridden by docker-compose)
CMD ["python", "-c", "print('SentinelDQ container ready')"]
