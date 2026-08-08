# Multi-stage Dockerfile for PostgreSQL 10.4 to BigQuery Migration Tool
FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install system build dependencies and libpq for PostgreSQL driver
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy and install python dependencies
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy application source code
COPY app/ ./app/
COPY .env.example ./.env.example

# Expose port for FastAPI Web UI
EXPOSE 8000

# Set default entrypoint to run Web UI or CLI
CMD ["python", "-m", "app.cli", "serve"]
