#!/bin/bash
# PostgreSQL Optimal Data Loading Script
# Runs the optimal PostgreSQL data loader

echo "Starting PostgreSQL Optimal Data Loading..."
echo "======================================"

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is not installed"
    exit 1
fi

# Check if data files exist
if [ ! -f "../../data/processed/events_cleaned.csv" ]; then
    echo "Error: Cleaned data files not found. Please run data cleaning first."
    exit 1
fi

# Run the optimal PostgreSQL loader
python3 scripts/loading/load_data_psql.py

# Check exit status
if [ $? -eq 0 ]; then
    echo "======================================"
    echo "PostgreSQL optimal loading completed successfully!"
else
    echo "======================================"
    echo "PostgreSQL optimal loading failed!"
    exit 1
fi
