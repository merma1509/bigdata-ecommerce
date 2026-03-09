# Security-First Database Connection
# This module provides secure database connections using environment variables

import os
import psycopg2
import pymongo
from neo4j import GraphDatabase
from typing import Optional

class SecureDatabaseManager:
    """Secure database connection manager"""
    
    def __init__(self):
        """Initialize with environment variables"""
        self._validate_environment()
    
    def _validate_environment(self):
        """Ensure all required environment variables are set"""
        required_vars = [
            'POSTGRES_PASSWORD',
            'MONGO_ROOT_PASSWORD', 
            'NEO4J_PASSWORD'
        ]
        
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {missing_vars}")
    
    def get_postgres_connection(self, database: str = "ecommerce") -> psycopg2.extensions.connection:
        """Get secure PostgreSQL connection"""
        return psycopg2.connect(
            host="localhost",
            port=5432,
            database=database,
            user=os.getenv("POSTGRES_USER", "ecommerce_user"),
            password=os.getenv("POSTGRES_PASSWORD"),
            sslmode="require"  # Enforce SSL
        )
    
    def get_mongo_connection(self, database: str = "ecommerce") -> pymongo.database.Database:
        """Get secure MongoDB connection"""
        client = pymongo.MongoClient(
            host="localhost",
            port=27017,
            username=os.getenv("MONGO_ROOT_USER", "admin"),
            password=os.getenv("MONGO_ROOT_PASSWORD"),
            authSource="admin",
            ssl=True,  # Enforce SSL
            ssl_cert_reqs='required'
        )
        return client[database]
    
    def get_neo4j_connection(self) -> GraphDatabase.driver:
        """Get secure Neo4j connection"""
        return GraphDatabase.driver(
            uri="bolt://localhost:7687",
            auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD")),
            encrypted=True,  # Enforce encryption
            trust="TRUST_ALL_CERTIFICATES"
        )

# Usage example in data loading scripts
def secure_connection_example():
    """Example of secure database usage"""
    db_manager = SecureDatabaseManager()
    
    # PostgreSQL
    with db_manager.get_postgres_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        print(f"PostgreSQL: {cursor.fetchone()}")
    
    # MongoDB
    mongo_db = db_manager.get_mongo_connection()
    print(f"MongoDB collections: {mongo_db.list_collection_names()}")
    
    # Neo4j
    with db_manager.get_neo4j_connection().session() as session:
        result = session.run("RETURN 'Connected' as status")
        print(f"Neo4j: {result.single()['status']}")

if __name__ == "__main__":
    secure_connection_example()
