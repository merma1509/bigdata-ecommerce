#!/bin/bash
# Shell script to run the hybrid scalable data model implementation

echo "Starting Scalable Hybrid Data Model Implementation..."
echo "=================================================="

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "Python3 is required but not installed"
    exit 1
fi

# Check if required packages are installed
echo "Checking required packages..."
python3 -c "
import sys
packages = ['psycopg2', 'pymongo', 'neo4j', 'pandas']
missing = []
for pkg in packages:
    try:
        __import__(pkg)
    except ImportError:
        missing.append(pkg)

if missing:
    print(f'Missing packages: {missing}')
    print('Installing missing packages...')
    import subprocess
    subprocess.check_call([sys.executable, '-m', 'pip', 'install'] + missing)
    print('Packages installed successfully')
else:
    print('All required packages are installed')
"

# Check if data files exist
echo "Checking data files..."
if [ ! -f "data/processed/events_cleaned.csv" ]; then
    echo "Processed data files not found. Running data cleaning first..."
    python3 scripts/clean_data.py
fi

# Check if environment variables are set
echo "Checking environment configuration..."
if [ ! -f ".env" ]; then
    echo ".env file not found. Using default database configurations..."
    echo "POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=ecommerce
POSTGRES_USER=ecommerce_user
POSTGRES_PASSWORD=ecommerce_pass

MONGO_HOST=localhost
MONGO_PORT=27017
MONGO_DB=ecommerce

NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=neo4j_pass" > .env
    echo "Default .env file created"
fi

# Run the hybrid model implementation
echo "Building scalable hybrid data model..."
python3 scripts/load_data_hybrid.py

echo "=================================================="
echo "Scalable Hybrid Model Implementation Complete!"
echo ""
echo "Hybrid Model Features:"
echo "  • PostgreSQL: Core transactional data (ACID compliance)"
echo "  • MongoDB: Activity data (document flexibility, horizontal scaling)"
echo "  • Neo4j: Graph data (relationship traversal, network analysis)"
echo ""
echo "Advantages of Hybrid Model:"
echo "  • Optimal performance for each query type"
echo "  • Scalability where needed (horizontal for MongoDB, vertical for PostgreSQL)"
echo "  • Rich relationship analysis (Neo4j graph algorithms)"
echo "  • Data integrity (PostgreSQL ACID transactions)"
echo "  • Flexible document storage (MongoDB schema flexibility)"
echo ""
echo "Performance Comparison:"
echo "  • Analytics queries: PostgreSQL (complex joins, aggregations)"
echo "  • User activity tracking: MongoDB (fast document reads, embedded data)"
echo "  • Social network analysis: Neo4j (graph traversal, recommendations)"
echo "  • Recommendation engine: Neo4j (path finding, centrality analysis)"
echo "  • Real-time personalization: MongoDB (document caching, horizontal scaling)"
echo "  • Financial transactions: PostgreSQL (ACID compliance, data integrity)"
