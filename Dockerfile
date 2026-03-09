# Use Python 3.10 slim image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python packages
COPY pyproject.toml .
COPY README.md .

# Install Python dependencies with pip
# RUN pip install --no-cache-dir -e

# Copy application code
COPY . .

# Create directories for outputs
RUN mkdir -p /app/output/screenshots /app/output/benchmarks /app/output/results

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Default command - keep container running for interactive use
CMD ["python", "-c", "print('Big Data e-commerce Container Ready!')"]
