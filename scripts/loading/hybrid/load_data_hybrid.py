#!/usr/bin/env python3
"""Custom Scalable Hybrid Data Model Implementation
Combines PostgreSQL, MongoDB, and Neo4j for optimal performance

Assignment: Data Modeling and Storage
Author: E-commerce Data Modeling Team"""

import psycopg2
import pymongo
from neo4j import GraphDatabase
import pandas as pd
import json
import os
from datetime import datetime
from typing import Dict, List, Any

class ScalableEcommerceModel:
    """
    Hybrid multi-database architecture that leverages the strengths of each database type:
    - PostgreSQL: Core transactional data (ACID compliance, complex joins)
    - MongoDB: Activity data (document flexibility, horizontal scaling)
    - Neo4j: Graph data (relationship traversal, network analysis)
    """
    
    def __init__(self):
        """Initialize connections to all three databases"""
        self.postgres_conn = None
        self.mongo_client = None
        self.neo4j_driver = None
        
        # Database configurations
        self.pg_config = {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': os.getenv('POSTGRES_PORT', '5432'),
            'database': os.getenv('POSTGRES_DB', 'ecommerce'),
            'user': os.getenv('POSTGRES_USER', 'ecommerce_user'),
            'password': os.getenv('POSTGRES_PASSWORD', 'ecommerce_pass')
        }
        
        self.mongo_config = {
            'host': os.getenv('MONGO_HOST', 'localhost'),
            'port': int(os.getenv('MONGO_PORT', '27017')),
            'database': os.getenv('MONGO_DB', 'ecommerce')
        }
        
        self.neo4j_config = {
            'uri': os.getenv('NEO4J_URI', 'bolt://localhost:7687'),
            'user': os.getenv('NEO4J_USER', 'neo4j'),
            'password': os.getenv('NEO4J_PASSWORD', 'neo4j_pass')
        }
    
    def connect_all_databases(self):
        """Establish connections to all three databases"""
        try:
            # PostgreSQL connection
            self.postgres_conn = psycopg2.connect(**self.pg_config)
            print("PostgreSQL connected successfully")
            
            # MongoDB connection
            self.mongo_client = pymongo.MongoClient(
                f"mongodb://{self.mongo_config['host']}:{self.mongo_config['port']}/"
            )
            self.mongo_db = self.mongo_client[self.mongo_config['database']]
            print("MongoDB connected successfully")
            
            # Neo4j connection
            self.neo4j_driver = GraphDatabase.driver(
                self.neo4j_config['uri'],
                auth=(self.neo4j_config['user'], self.neo4j_config['password'])
            )
            print("Neo4j connected successfully")
            
        except Exception as e:
            print(f"Database connection error: {e}")
            raise
    
    def create_postgres_core_schema(self):
        """Create PostgreSQL schema for core transactional data"""
        if not self.postgres_conn:
            raise Exception("PostgreSQL not connected")
        
        cursor = self.postgres_conn.cursor()
        
        # Core entities schema
        schema_sql = """
        -- PostgreSQL Core Transactional Data Schema
        
        -- Categories Table
        CREATE TABLE IF NOT EXISTS categories (
            category_id BIGINT PRIMARY KEY,
            category_code VARCHAR(100) UNIQUE NOT NULL,
            category_name VARCHAR(255) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Products Table
        CREATE TABLE IF NOT EXISTS products (
            product_id BIGINT PRIMARY KEY,
            category_id BIGINT REFERENCES categories(category_id),
            brand VARCHAR(100),
            price DECIMAL(10,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Users Table
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            client_id BIGINT UNIQUE NOT NULL,
            user_device_id INTEGER,
            first_purchase_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Campaigns Table
        CREATE TABLE IF NOT EXISTS campaigns (
            campaign_id INTEGER PRIMARY KEY,
            campaign_type VARCHAR(50),
            channel VARCHAR(50),
            topic VARCHAR(255),
            started_at TIMESTAMP,
            finished_at TIMESTAMP,
            subject_features JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Performance Indexes
        CREATE INDEX IF NOT EXISTS idx_products_category_id ON products(category_id);
        CREATE INDEX IF NOT EXISTS idx_products_brand ON products(brand);
        CREATE INDEX IF NOT EXISTS idx_users_client_id ON users(client_id);
        CREATE INDEX IF NOT EXISTS idx_campaigns_type ON campaigns(campaign_type);
        CREATE INDEX IF NOT EXISTS idx_campaigns_channel ON campaigns(channel);
        """
        
        cursor.execute(schema_sql)
        self.postgres_conn.commit()
        print("PostgreSQL core schema created")
    
    def create_mongodb_activity_schema(self):
        """Create MongoDB collections for activity data with schema validation"""
        if not self.mongo_client:
            raise Exception("MongoDB not connected")
        
        # Events collection with schema validation
        events_schema = {
            "$jsonSchema": {
                "bsonType": "object",
                "title": "events",
                "properties": {
                    "_id": {"bsonType": "objectId"},
                    "event_time": {"bsonType": "date"},
                    "event_type": {"bsonType": "string", "enum": ["view", "purchase", "cart_add", "cart_remove"]},
                    "product_id": {"bsonType": "long"},
                    "user_id": {"bsonType": "long"},
                    "user_session": {"bsonType": "string"},
                    "price": {"bsonType": "decimal"}
                },
                "required": ["event_time", "event_type", "product_id", "user_id"]
            }
        }
        
        # Messages collection with embedded engagement
        messages_schema = {
            "$jsonSchema": {
                "bsonType": "object",
                "title": "messages",
                "properties": {
                    "_id": {"bsonType": "objectId"},
                    "message_id": {"bsonType": "string"},
                    "campaign_id": {"bsonType": "int"},
                    "user_id": {"bsonType": "long"},
                    "engagement": {
                        "bsonType": "object",
                        "properties": {
                            "opened": {"bsonType": "bool"},
                            "opened_first_time_at": {"bsonType": "date"},
                            "clicked": {"bsonType": "bool"},
                            "clicked_first_time_at": {"bsonType": "date"}
                        }
                    },
                    "conversion": {
                        "bsonType": "object",
                        "properties": {
                            "purchased": {"bsonType": "bool"},
                            "purchased_at": {"bsonType": "date"},
                            "purchase_amount": {"bsonType": "decimal"}
                        }
                    },
                    "sent_at": {"bsonType": "date"}
                },
                "required": ["message_id", "campaign_id", "user_id", "sent_at"]
            }
        }
        
        # User sessions collection
        sessions_schema = {
            "$jsonSchema": {
                "bsonType": "object",
                "title": "user_sessions",
                "properties": {
                    "_id": {"bsonType": "objectId"},
                    "session_id": {"bsonType": "string"},
                    "user_id": {"bsonType": "long"},
                    "start_time": {"bsonType": "date"},
                    "end_time": {"bsonType": "date"},
                    "device_type": {"bsonType": "string"},
                    "page_views": {"bsonType": "int"},
                    "events_count": {"bsonType": "int"}
                },
                "required": ["session_id", "user_id", "start_time"]
            }
        }
        
        # Create collections with schema validation
        try:
            # Drop existing collections
            self.mongo_db.events.drop()
            self.mongo_db.messages.drop()
            self.mongo_db.user_sessions.drop()
            
            # Create with schema validation
            self.mongo_db.create_collection("events", validator={"$jsonSchema": events_schema["$jsonSchema"]})
            self.mongo_db.create_collection("messages", validator={"$jsonSchema": messages_schema["$jsonSchema"]})
            self.mongo_db.create_collection("user_sessions", validator={"$jsonSchema": sessions_schema["$jsonSchema"]})
            
            # Create indexes
            self.mongo_db.events.create_index([("user_id", 1), ("event_time", -1)])
            self.mongo_db.events.create_index([("product_id", 1)])
            self.mongo_db.messages.create_index([("user_id", 1), ("sent_at", -1)])
            self.mongo_db.messages.create_index([("campaign_id", 1)])
            self.mongo_db.user_sessions.create_index([("user_id", 1), ("start_time", -1)])
            
            print("MongoDB activity schema created")
            
        except Exception as e:
            print(f"MongoDB schema creation warning: {e}")
    
    def create_neo4j_graph_schema(self):
        """Create Neo4j graph schema for relationships and recommendations"""
        if not self.neo4j_driver:
            raise Exception("Neo4j not connected")
        
        with self.neo4j_driver.session() as session:
            # Create uniqueness constraints
            constraints = [
                "CREATE CONSTRAINT user_id_unique IF NOT EXISTS ON (u:User) ASSERT u.user_id IS UNIQUE",
                "CREATE CONSTRAINT product_id_unique IF NOT EXISTS ON (p:Product) ASSERT p.product_id IS UNIQUE",
                "CREATE CONSTRAINT category_id_unique IF NOT EXISTS ON (c:Category) ASSERT c.category_id IS UNIQUE",
                "CREATE CONSTRAINT campaign_id_unique IF NOT EXISTS ON (ca:Campaign) ASSERT ca.campaign_id IS UNIQUE"
            ]
            
            for constraint in constraints:
                session.run(constraint)
            
            # Create indexes for performance
            indexes = [
                "CREATE INDEX user_device_idx IF NOT EXISTS FOR (u:User) ON (u.user_device_id)",
                "CREATE INDEX product_brand_idx IF NOT EXISTS FOR (p:Product) ON (p.brand)",
                "CREATE INDEX product_price_idx IF NOT EXISTS FOR (p:Product) ON (p.price)",
                "CREATE INDEX campaign_type_idx IF NOT EXISTS FOR (ca:Campaign) ON (ca.campaign_type)"
            ]
            
            for index in indexes:
                session.run(index)
            
            print("Neo4j graph schema created")
    
    def load_core_data_to_postgres(self):
        """Load core entity data to PostgreSQL"""
        if not self.postgres_conn:
            raise Exception("PostgreSQL not connected")
        
        cursor = self.postgres_conn.cursor()
        
        try:
            # Load categories
            categories_df = pd.read_csv('data/processed/campaigns_cleaned.csv')
            # Extract unique categories from campaigns data
            unique_categories = categories_df[['campaign_type', 'channel']].drop_duplicates()
            
            for _, row in unique_categories.iterrows():
                cursor.execute("""
                    INSERT INTO categories (category_id, category_code, category_name)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (category_id) DO NOTHING
                """, (hash(row['campaign_type']) % 1000000, f"cat_{row['campaign_type']}", row['campaign_type']))
            
            # Load products (from events data)
            events_df = pd.read_csv('data/processed/events_cleaned.csv')
            unique_products = events_df[['product_id', 'price']].drop_duplicates()
            
            for _, row in unique_products.iterrows():
                cursor.execute("""
                    INSERT INTO products (product_id, price)
                    VALUES (%s, %s)
                    ON CONFLICT (product_id) DO NOTHING
                """, (row['product_id'], row['price']))
            
            # Load users
            friends_df = pd.read_csv('data/processed/friends_cleaned.csv')
            unique_users = pd.concat([friends_df['user_id'], friends_df['friend_id']]).unique()
            
            for user_id in unique_users:
                cursor.execute("""
                    INSERT INTO users (user_id, client_id)
                    VALUES (%s, %s)
                    ON CONFLICT (user_id) DO NOTHING
                """, (user_id, user_id))
            
            # Load campaigns
            campaigns_df = pd.read_csv('data/processed/campaigns_cleaned.csv')
            
            for _, row in campaigns_df.iterrows():
                cursor.execute("""
                    INSERT INTO campaigns (campaign_id, campaign_type, channel, topic, started_at, finished_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (campaign_id) DO NOTHING
                """, (row['id'], row['campaign_type'], row['channel'], row['topic'], 
                      pd.to_datetime(row['started_at']), pd.to_datetime(row['finished_at'])))
            
            self.postgres_conn.commit()
            print("Core data loaded to PostgreSQL")
            
        except Exception as e:
            print(f"Error loading PostgreSQL data: {e}")
            self.postgres_conn.rollback()
            raise
    
    def load_activity_data_to_mongodb(self):
        """Load activity data to MongoDB"""
        if not self.mongo_client:
            raise Exception("MongoDB not connected")
        
        try:
            # Load events
            events_df = pd.read_csv('data/processed/events_cleaned.csv')
            events_records = events_df.to_dict('records')
            
            # Convert to MongoDB format
            mongo_events = []
            for record in events_records:
                mongo_event = {
                    "event_time": pd.to_datetime(record['event_time']),
                    "event_type": record['event_type'],
                    "product_id": int(record['product_id']),
                    "user_id": int(record['user_id']),
                    "user_session": record.get('user_session', ''),
                    "price": float(record['price'])
                }
                mongo_events.append(mongo_event)
            
            # Bulk insert events
            if mongo_events:
                self.mongo_db.events.insert_many(mongo_events)
            
            # Load messages
            messages_df = pd.read_csv('data/processed/messages_cleaned.csv')
            messages_records = messages_df.to_dict('records')
            
            # Convert to MongoDB format with embedded documents
            mongo_messages = []
            for record in messages_records:
                mongo_message = {
                    "message_id": record['message_id'],
                    "campaign_id": int(record['campaign_id']),
                    "user_id": int(record['user_id']),
                    "engagement": {
                        "opened": bool(record['is_opened']),
                        "opened_first_time_at": pd.to_datetime(record['opened_first_time_at']) if record['opened_first_time_at'] else None,
                        "clicked": bool(record['is_clicked']),
                        "clicked_first_time_at": pd.to_datetime(record['clicked_first_time_at']) if record['clicked_first_time_at'] else None
                    },
                    "conversion": {
                        "purchased": bool(record['is_purchased']),
                        "purchased_at": pd.to_datetime(record['purchased_at']) if record['purchased_at'] else None,
                        "purchase_amount": float(record['purchase_amount']) if record['purchase_amount'] else None
                    },
                    "sent_at": pd.to_datetime(record['sent_at'])
                }
                mongo_messages.append(mongo_message)
            
            # Bulk insert messages
            if mongo_messages:
                self.mongo_db.messages.insert_many(mongo_messages)
            
            print("Activity data loaded to MongoDB")
            
        except Exception as e:
            print(f"Error loading MongoDB data: {e}")
            raise
    
    def load_graph_data_to_neo4j(self):
        """Load graph relationships to Neo4j"""
        if not self.neo4j_driver:
            raise Exception("Neo4j not connected")
        
        with self.neo4j_driver.session() as session:
            try:
                # Create user nodes
                friends_df = pd.read_csv('data/processed/friends_cleaned.csv')
                unique_users = pd.concat([friends_df['user_id'], friends_df['friend_id']]).unique()
                
                for user_id in unique_users:
                    session.run("""
                        MERGE (u:User {user_id: $user_id})
                        SET u.created_at = datetime()
                    """, user_id=int(user_id))
                
                # Create product nodes
                events_df = pd.read_csv('data/processed/events_cleaned.csv')
                unique_products = events_df['product_id'].unique()
                
                for product_id in unique_products:
                    session.run("""
                        MERGE (p:Product {product_id: $product_id})
                        SET p.created_at = datetime()
                    """, product_id=int(product_id))
                
                # Create friendship relationships
                for _, row in friends_df.iterrows():
                    session.run("""
                        MATCH (u1:User {user_id: $user_id})
                        MATCH (u2:User {user_id: $friend_id})
                        MERGE (u1)-[:FRIENDS_WITH {created_at: datetime()}]->(u2)
                    """, user_id=int(row['user_id']), friend_id=int(row['friend_id']))
                
                # Create event relationships
                for _, row in events_df.iterrows():
                    session.run("""
                        MATCH (u:User {user_id: $user_id})
                        MATCH (p:Product {product_id: $product_id})
                        MERGE (u)-[:EVENT {
                            event_type: $event_type,
                            event_time: datetime($event_time),
                            user_session: $user_session,
                            price: $price
                        }]->(p)
                    """, user_id=int(row['user_id']), product_id=int(row['product_id']),
                        event_type=row['event_type'], event_time=row['event_time'],
                        user_session=row.get('user_session', ''), price=float(row['price']))
                
                print("Graph data loaded to Neo4j")
                
            except Exception as e:
                print(f"Error loading Neo4j data: {e}")
                raise
    
    def demonstrate_hybrid_queries(self):
        """Demonstrate the power of the hybrid architecture"""
        print("\nHybrid Model Demonstration:")
        
        # PostgreSQL: Complex analytical queries
        if self.postgres_conn:
            cursor = self.postgres_conn.cursor()
            cursor.execute("""
                SELECT campaign_type, COUNT(*) as campaign_count
                FROM campaigns 
                GROUP BY campaign_type
                ORDER BY campaign_count DESC
            """)
            results = cursor.fetchall()
            print("\nPostgreSQL - Campaign Analytics:")
            for row in results:
                print(f"  {row[0]}: {row[1]} campaigns")
        
        # MongoDB: Document queries with embedded data
        if self.mongo_client:
            pipeline = [
                {"$group": {
                    "_id": "$engagement.opened",
                    "count": {"$sum": 1},
                    "avg_purchase_rate": {"$avg": {"$cond": ["$conversion.purchased", 1, 0]}}
                }}
            ]
            results = list(self.mongo_db.messages.aggregate(pipeline))
            print("\nMongoDB - Message Engagement:")
            for result in results:
                status = "Opened" if result["_id"] else "Not Opened"
                print(f"  {status}: {result['count']} messages, {result['avg_purchase_rate']:.2%} purchase rate")
        
        # Neo4j: Graph traversal queries
        if self.neo4j_driver:
            with self.neo4j_driver.session() as session:
                result = session.run("""
                    MATCH (u:User)-[:FRIENDS_WITH]->(friend:User)
                    RETURN u.user_id, count(friend) as friend_count
                    ORDER BY friend_count DESC
                    LIMIT 5
                """)
                print("\nNeo4j - Social Network Analysis:")
                for record in result:
                    print(f"  User {record['u.user_id']}: {record['friend_count']} friends")
    
    def close_connections(self):
        """Close all database connections"""
        if self.postgres_conn:
            self.postgres_conn.close()
        if self.mongo_client:
            self.mongo_client.close()
        if self.neo4j_driver:
            self.neo4j_driver.close()
        print("All database connections closed")
    
    def run_complete_implementation(self):
        """Run the complete hybrid model implementation"""
        print("Starting Scalable Hybrid Data Model Implementation...")
        
        try:
            # Step 1: Connect to all databases
            self.connect_all_databases()
            
            # Step 2: Create schemas
            self.create_postgres_core_schema()
            self.create_mongodb_activity_schema()
            self.create_neo4j_graph_schema()
            
            # Step 3: Load data
            self.load_core_data_to_postgres()
            self.load_activity_data_to_mongodb()
            self.load_graph_data_to_neo4j()
            
            # Step 4: Demonstrate capabilities
            self.demonstrate_hybrid_queries()
            
            print("\nScalable Hybrid Model Implementation Complete!")
            
        except Exception as e:
            print(f"Implementation error: {e}")
            raise
        finally:
            self.close_connections()

def main():
    """Main execution function"""
    model = ScalableEcommerceModel()
    model.run_complete_implementation()

if __name__ == "__main__":
    main()
