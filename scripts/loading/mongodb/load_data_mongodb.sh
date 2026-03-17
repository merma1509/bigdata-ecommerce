#!/bin/bash

# MongoDB Data Loading Script
# This script loads the cleaned data into MongoDB database

echo "=== MongoDB Data Loading ==="

# Check if MongoDB is running
if ! docker ps | grep -q "bigdata-mongodb"; then
    echo "Starting MongoDB container..."
    docker compose up -d mongodb
    sleep 10
fi

# Load environment variables
source .env

# Run the Python loading script
echo "Loading data into MongoDB..."
python scripts/load_data_mongodb.py

echo "MongoDB data loading completed!"
