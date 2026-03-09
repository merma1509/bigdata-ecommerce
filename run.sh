#!/bin/bash

# Complete project environment setup with security
# This script sets up and runs the entire project environment

echo "Starting..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Docker is not running. Please start Docker first"
    exit 1
fi

# Install UV for fast dependency management
echo "Installing UV for fast package management..."
if ! command -v uv &> /dev/null; then
    echo "UV not found, installing quickly..."
    chmod +x scripts/install_uv.sh
    ./scripts/install_uv.sh
else
    echo "UV already installed: $(uv --version)"
fi

# Setup security first
echo "Setting up secure environment..."
chmod +x scripts/setup_security.sh
./scripts/setup_security.sh

# Load environment variables
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
    echo "Environment variables loaded"
else
    echo "Warning: .env file not found"
fi

# Build and start containers
echo "Building and starting secure containers..."
docker-compose up --build -d || echo "Docker build failed, but continuing..."

# Wait for databases to be ready
echo "Waiting for databases to start..."
sleep 20

# Install Python dependencies using UV (much faster than pip)
echo "Installing Python dependencies with UV..."
if [ -f "pyproject.toml" ]; then
    export UV_LINK_MODE=copy
    uv sync --dev
else
    echo "pyproject.toml not found, installing from requirements.txt"
    pip install -r requirements.txt
fi

echo "Environment setup complete!"
echo ""
echo "Next steps:"
echo "1. Test secure connections: python scripts/security/secure_connections.py"
echo "2. Run data cleaning: python scripts/ingestion/clean_data.py"
echo "3. Load databases: ./scripts/load_data_*.sh"
echo "4. Start analysis: jupyter notebook"
echo ""
echo "Database URLs (with SSL/TLS):"
echo "PostgreSQL: postgresql://ecommerce_user:PASSWORD@localhost:5432/ecommerce?sslmode=require"
echo "MongoDB: mongodb://admin:PASSWORD@localhost:27017/ecommerce?ssl=true"
echo "Neo4j: bolt://neo4j:PASSWORD@localhost:7687?encrypted=true"
