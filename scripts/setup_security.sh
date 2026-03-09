#!/bin/bash

# Security Setup Script
set -e

echo "Setting up secure environment..."

# Create secrets directory
mkdir -p secrets

# Generate secure passwords
if command -v openssl >/dev/null 2>&1; then
    POSTGRES_PASSWORD=$(openssl rand -base64 32 | tr -d '=')
    MONGO_ROOT_PASSWORD=$(openssl rand -base64 32 | tr -d '=')
    NEO4J_PASSWORD=$(openssl rand -base64 32 | tr -d '=')
else
    POSTGRES_PASSWORD=$(powershell -Command "[System.Security.Cryptography.RandomNumberGenerator]::GetBytes(32) | ForEach-Object {[byte]$_} {[Convert]::ToBase64String($_)}" | Out-String)
    MONGO_ROOT_PASSWORD=$(powershell -Command "[System.Security.Cryptography.RandomNumberGenerator]::GetBytes(32) | ForEach-Object {[byte]$_} {[Convert]::ToBase64String($_)}" | Out-String)
    NEO4J_PASSWORD=$(powershell -Command "[System.Security.Cryptography.RandomNumberGenerator]::GetBytes(32) | ForEach-Object {[byte]$_} {[Convert]::ToBase64String($_)}" | Out-String)
fi

# Create secret files
echo "$POSTGRES_PASSWORD" > secrets/postgres_password
echo "admin" > secrets/mongo_root_user
echo "$MONGO_ROOT_PASSWORD" > secrets/mongo_root_password
echo "neo4j/$NEO4J_PASSWORD" > secrets/neo4j_auth

# Set permissions
chmod 600 secrets/*

# Create .env file
cat > .env << EOF
POSTGRES_PASSWORD="$POSTGRES_PASSWORD"
MONGO_ROOT_PASSWORD="$MONGO_ROOT_PASSWORD"
NEO4J_PASSWORD="$NEO4J_PASSWORD"
POSTGRES_USER=ecommerce_user
MONGO_ROOT_USER=admin
NEO4J_USER=neo4j
POSTGRES_DB=ecommerce
MONGO_DB=ecommerce
NEO4J_DB=ecommerce
EOF

echo "Security setup complete!"
echo "Ready to start secure containers!"
