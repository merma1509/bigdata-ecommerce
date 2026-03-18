#!/bin/bash
# Complete Data Pipeline Runner
# Cleans raw data and loads into PostgreSQL, MongoDB, and Neo4j

echo "BIG DATA STORAGE & RETRIEVAL - COMPLETE PIPELINE"
echo "=================================================="
echo "This script will:"
echo "  1. Clean all raw data files"
echo "  2. Load cleaned data into PostgreSQL"
echo "  3. Load cleaned data into MongoDB"
echo "  4. Load cleaned data into Neo4j"
echo "=================================================="

# Check if Python is available 
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is not installed"
    exit 1
fi

# Change to project root directory
cd "$(dirname "$0")/.."

# Run the complete pipeline
python3 scripts/run_complete_pipeline.py

# Check exit status
if [ $? -eq 0 ]; then
    echo ""
    echo "=================================================="
    echo "COMPLETE PIPELINE EXECUTED SUCCESSFULLY!"
    echo "All data cleaned and loaded into all three databases!"
    echo "=================================================="
else
    echo ""
    echo "=================================================="
    echo "PIPELINE FAILED!"
    echo "Check error messages above for details."
    echo "=================================================="
    exit 1
fi
