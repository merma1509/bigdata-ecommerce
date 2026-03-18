#!/bin/bash
# Neo4j Optimal Data Loading Script
# Runs the optimal Neo4j data loader

echo "Starting Neo4j Optimal Data Loading..."
echo "======================================"

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is not installed"
    exit 1
fi

# Check if Neo4j is running
if ! pgrep neo4j > /dev/null && ! pgrep memgraph > /dev/null; then
    echo "Warning: Neo4j/Memgraph may not be running. Please start the service."
fi

# Check if data files exist
if [ ! -f "../../data/processed/events_cleaned.csv" ]; then
    echo "Error: Cleaned data files not found. Please run data cleaning first."
    exit 1
fi

# Run the optimal Neo4j loader
python3 scripts/loading/load_data_graph.py

# Check exit status
if [ $? -eq 0 ]; then
    echo "======================================"
    echo "Neo4j optimal loading completed successfully!"
else
    echo "======================================"
    echo "Neo4j optimal loading failed!"
    exit 1
fi
