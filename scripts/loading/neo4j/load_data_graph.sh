#!/bin/bash

# Neo4j Data Loading Script
# This script loads the cleaned data into Neo4j graph database

echo "=== Neo4j Data Loading ==="

# Check if Neo4j is running
if ! docker ps | grep -q "bigdata-neo4j"; then
    echo "Starting Neo4j container..."
    docker compose up -d neo4j
    sleep 15
fi

# Load environment variables
source .env

# Run the Python loading script
echo "Loading data into Neo4j..."
python scripts/load_data_graph.py

echo "Neo4j data loading completed!"
