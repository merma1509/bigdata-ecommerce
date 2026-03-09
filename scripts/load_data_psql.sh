#!/bin/bash

# PostgreSQL Data Loading Script
# This script loads the cleaned data into PostgreSQL database

echo "=== PostgreSQL Data Loading ==="

# Check if PostgreSQL is running
if ! docker ps | grep -q "bigdata-postgres"; then
    echo "Starting PostgreSQL container..."
    cd docker
    docker-compose up -d postgres
    cd ..
    sleep 10
fi

# Run the Python loading script
echo "Loading data into PostgreSQL..."
python scripts/ingestion/load_data_psql.py

echo "PostgreSQL data loading completed!"
