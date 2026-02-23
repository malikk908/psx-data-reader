# syntax=docker/dockerfile:1
FROM python:3.12-slim

# Install tzdata for timezone support
RUN apt-get update \
    && apt-get install -y --no-install-recommends tzdata \
    && rm -rf /var/lib/apt/lists/*

# Set timezone (can be overridden by env on Railway)
ENV TZ=Asia/Karachi

# Ensure Python output is logged immediately in Railway
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install Python dependencies first (better layer caching)
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the project
COPY . .

# Make sure local package imports work (psx under /app/src)
ENV PYTHONPATH=/app/src

# Default command: run the cron job script
# CMD ["python", "-u", "src/psx/mongodb_cron.py"] //will run through railway cron
