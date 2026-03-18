#!/bin/bash
# MongoDB Optimal Data Loading Script
# Runs the optimal MongoDB data loader

echo "Starting MongoDB Optimal Data Loading..."
echo "======================================"

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is not installed"
    exit 1
fi

# Check if MongoDB is running
if ! pgrep mongod > /dev/null; then
    echo "Warning: MongoDB may not be running. Please start MongoDB service."
fi

# Check if data files exist
if [ ! -f "../../data/processed/events_cleaned.csv" ]; then
    echo "Error: Cleaned data files not found. Please run data cleaning first."
    exit 1
fi

# Run the optimal MongoDB loader
python3 scripts/loading/load_data_mongodb.py

# Check exit status
if [ $? -eq 0 ]; then
    echo "======================================"
    echo "MongoDB optimal loading completed successfully!"
else
    echo "======================================"
    echo "MongoDB optimal loading failed!"
    exit 1
fi
