#!/bin/bash

# PostgreSQL Data Loading Script
# This script loads the cleaned data into PostgreSQL database

echo "=== PostgreSQL Data Loading ==="

# Check if PostgreSQL is running
if ! docker ps | grep -q "bigdata-postgres"; then
    echo "Starting PostgreSQL container..."
    docker compose up -d postgres
    sleep 10
fi

# Load environment variables
source .env

# Run the Python loading script
echo "Loading data into PostgreSQL..."
python scripts/load_data_psql.py

echo "PostgreSQL data loading completed!"
