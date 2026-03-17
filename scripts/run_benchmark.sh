#!/bin/bash
# Shell script to run complete database benchmarking

echo "Database Benchmarking Suite"
echo "==========================="

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "Python3 is required but not installed"
    exit 1
fi

# Check if required packages are installed
echo "🔍 Checking Python packages..."
python3 -c "
import sys
packages = ['psycopg2', 'pymongo', 'neo4j', 'pandas', 'statistics']
missing = []
for pkg in packages:
    try:
        __import__(pkg)
    except ImportError:
        missing.append(pkg)

if missing:
    print(f'Missing packages: {missing}')
    echo 'Please install missing packages with: pip install -r requirements.txt'
    exit 1)
else:
    echo 'All required packages are available'
"

# Check if environment variables are set
echo "🔍 Checking environment configuration..."
if [ ! -f ".env" ]; then
    echo ".env file not found. Using default database configurations..."
    cp .env.example .env
    echo "Default .env file created. Please edit with your credentials."
fi

# Check if database services are running
echo "🔍 Checking database services..."

# PostgreSQL
if command -v psql &> /dev/null; then
    echo "✅ PostgreSQL client available"
    if psql -h localhost -p 5432 -U ecommerce_user -d ecommerce -c "SELECT 1;" &> /dev/null; then
        echo "✅ PostgreSQL connection successful"
    else
        echo "⚠️ PostgreSQL connection failed - please check service"
    fi
else
    echo "⚠️ PostgreSQL client not available"
fi

# MongoDB
if command -v mongo &> /dev/null; then
    echo "✅ MongoDB client available"
    if mongo --eval "db.runCommand('ping')" &> /dev/null; then
        echo "✅ MongoDB connection successful"
    else
        echo "⚠️ MongoDB connection failed - please check service"
    fi
else
    echo "⚠️ MongoDB client not available"
fi

# Neo4j
if command -v cypher-shell &> /dev/null; then
    echo "✅ Neo4j client available"
    if cypher-shell -a bolt://localhost:7687 -u neo4j -p neo4j_pass "RETURN 1;" &> /dev/null; then
        echo "✅ Neo4j connection successful"
    else
        echo "⚠️ Neo4j connection failed - please check service"
    fi
else
    echo "⚠️ Neo4j client not available"
fi

# Check if data files exist
echo "🔍 Checking data files..."
if [ ! -f "data/processed/events_cleaned.csv" ]; then
    echo "⚠️ Processed data files not found. Running data cleaning first..."
    python3 scripts/data/clean_data.py
fi

# Check if analysis queries exist
echo "🔍 Checking analysis queries..."
required_queries=(
    "scripts/analysis/q1/q1.sql"
    "scripts/analysis/q1/q1.js"
    "scripts/analysis/q1/q1.cypher"
    "scripts/analysis/q2/q2.sql"
    "scripts/analysis/q2/q2.js"
    "scripts/analysis/q2/q2.cypher"
    "scripts/analysis/q3/q3.sql"
    "scripts/analysis/q3/q3.js"
    "scripts/analysis/q3/q3.cypher"
)

for query in "${required_queries[@]}"; do
    if [ ! -f "$query" ]; then
        echo "❌ Required query file not found: $query"
        exit 1
    fi
done

echo "✅ All required query files found"

# Run benchmarking
echo "🚀 Starting benchmark tests..."
python3 scripts/benchmark.py

echo "================================="
echo "✅ Benchmarking Complete!"
echo ""
echo "Results saved to:"
echo "  - benchmark_results.json (raw data)"
echo "  - benchmark_report.md (formatted report)"
echo ""
echo "Screenshots should be taken for:"
echo "  - Each query execution"
echo "  - Database connection tests"
echo "  - Performance results"
