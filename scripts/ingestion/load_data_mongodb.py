#!/usr/bin/env python3
"""MongoDB Data Loading Script

This script creates the MongoDB collections and loads the cleaned data
using a document-oriented approach"""

import pymongo
from pymongo import MongoClient
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MongoDBLoader:
    """MongoDB database loader"""
    
    def __init__(self, connection_string: str = "mongodb://admin:password@localhost:27017/", database_name: str = "ecommerce"):
        self.connection_string = connection_string
        self.database_name = database_name
        self.client = None
        self.db = None
        
    def connect(self):
        """Establish database connection"""
        try:
            self.client = MongoClient(self.connection_string)
            self.db = self.client[self.database_name]
            # Test connection
            self.client.admin.command('ping')
            logger.info("Connected to MongoDB database")
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
    
    def disconnect(self):
        """Close database connection"""
        if self.client:
            self.client.close()
            logger.info("Disconnected from MongoDB database")
    
    def create_schema(self):
        """Create collections and indexes"""
        logger.info("Creating MongoDB collections and indexes...")
        
        # Drop existing collections
        self.db.users.drop()
        self.db.products.drop()
        self.db.campaigns.drop()
        self.db.events.drop()
        self.db.messages.drop()
        self.db.friends.drop()
        
        # Create indexes for performance
        # Users collection
        self.db.users.create_index("user_id", unique=True)
        self.db.users.create_index("first_purchase_date")
        
        # Products collection
        self.db.products.create_index("product_id", unique=True)
        self.db.products.create_index("category_id")
        self.db.products.create_index("brand")
        self.db.products.create_index([("category_code", "text")])
        
        # Campaigns collection
        self.db.campaigns.create_index([("campaign_id", 1), ("campaign_type", 1)], unique=True)
        self.db.campaigns.create_index("campaign_type")
        self.db.campaigns.create_index("channel")
        self.db.campaigns.create_index("started_at")
        
        # Events collection
        self.db.events.create_index("user_id")
        self.db.events.create_index("product_id")
        self.db.events.create_index("event_time")
        self.db.events.create_index("event_type")
        self.db.events.create_index([("user_id", 1), ("event_time", -1)])
        
        # Messages collection
        self.db.messages.create_index("campaign_id")
        self.db.messages.create_index("client_id")
        self.db.messages.create_index("sent_at")
        self.db.messages.create_index([("campaign_id", 1), ("is_purchased", 1)])
        
        # Friends collection
        self.db.friends.create_index([("user_id", 1), ("friend_id", 1)], unique=True)
        
        logger.info("MongoDB collections and indexes created successfully")
    
    def load_data(self, data_path: str):
        """Load cleaned data into collections"""
        logger.info("Loading data into MongoDB...")
        
        data_path = Path(data_path)
        
        # Load and transform users data
        logger.info("Loading users data...")
        users_df = pd.read_csv(data_path / "client_first_purchase_date_cleaned.csv")
        users_docs = users_df.to_dict('records')
        if users_docs:
            self.db.users.insert_many(users_docs)
        
        # Load products data (from events)
        logger.info("Loading products data...")
        events_df = pd.read_csv(data_path / "events_cleaned.csv")
        products_df = events_df[['product_id', 'category_id', 'category_code', 'brand', 'price']].drop_duplicates()
        products_docs = products_df.to_dict('records')
        if products_docs:
            self.db.products.insert_many(products_docs)
        
        # Load campaigns data
        logger.info("Loading campaigns data...")
        campaigns_df = pd.read_csv(data_path / "campaigns_cleaned.csv")
        campaigns_docs = campaigns_df.to_dict('records')
        if campaigns_docs:
            self.db.campaigns.insert_many(campaigns_docs)
        
        # Load events data
        logger.info("Loading events data...")
        events_docs = events_df.to_dict('records')
        if events_docs:
            # Insert in batches for better performance
            batch_size = 10000
            for i in range(0, len(events_docs), batch_size):
                batch = events_docs[i:i + batch_size]
                self.db.events.insert_many(batch)
        
        # Load messages data
        logger.info("Loading messages data...")
        messages_df = pd.read_csv(data_path / "messages_cleaned.csv")
        messages_docs = messages_df.to_dict('records')
        if messages_docs:
            # Insert in batches
            batch_size = 10000
            for i in range(0, len(messages_docs), batch_size):
                batch = messages_docs[i:i + batch_size]
                self.db.messages.insert_many(batch)
        
        # Load friends data
        logger.info("Loading friends data...")
        friends_df = pd.read_csv(data_path / "friends_cleaned.csv")
        friends_docs = friends_df.to_dict('records')
        if friends_docs:
            self.db.friends.insert_many(friends_docs)
        
        logger.info("Data loading completed successfully")
    
    def create_aggregated_views(self):
        """Create aggregated collections for analytics"""
        logger.info("Creating aggregated collections...")
        
        # Create user activity summary
        pipeline = [
            {
                "$group": {
                    "_id": "$user_id",
                    "total_events": {"$sum": 1},
                    "unique_products": {"$addToSet": "$product_id"},
                    "event_types": {"$addToSet": "$event_type"},
                    "first_event": {"$min": "$event_time"},
                    "last_event": {"$max": "$event_time"}
                }
            },
            {
                "$project": {
                    "user_id": "$_id",
                    "total_events": 1,
                    "unique_products_count": {"$size": "$unique_products"},
                    "event_types": 1,
                    "first_event": 1,
                    "last_event": 1,
                    "_id": 0
                }
            }
        ]
        
        user_activity = list(self.db.events.aggregate(pipeline))
        if user_activity:
            self.db.user_activity_summary.drop()
            self.db.user_activity_summary.insert_many(user_activity)
            self.db.user_activity_summary.create_index("user_id", unique=True)
        
        # Create campaign performance summary
        pipeline = [
            {
                "$group": {
                    "_id": "$campaign_id",
                    "total_messages": {"$sum": 1},
                    "opened_messages": {"$sum": {"$cond": [{"$eq": ["$is_opened", True]}, 1, 0]}},
                    "clicked_messages": {"$sum": {"$cond": [{"$eq": ["$is_clicked", True]}, 1, 0]}},
                    "purchased_messages": {"$sum": {"$cond": [{"$eq": ["$is_purchased", True]}, 1, 0]}}
                }
            },
            {
                "$project": {
                    "campaign_id": "$_id",
                    "total_messages": 1,
                    "open_rate": {"$divide": ["$opened_messages", "$total_messages"]},
                    "click_rate": {"$divide": ["$clicked_messages", "$total_messages"]},
                    "conversion_rate": {"$divide": ["$purchased_messages", "$total_messages"]},
                    "_id": 0
                }
            }
        ]
        
        campaign_performance = list(self.db.messages.aggregate(pipeline))
        if campaign_performance:
            self.db.campaign_performance_summary.drop()
            self.db.campaign_performance_summary.insert_many(campaign_performance)
            self.db.campaign_performance_summary.create_index("campaign_id", unique=True)
        
        logger.info("Aggregated collections created successfully")
    
    def get_data_summary(self):
        """Get summary statistics of loaded data"""
        logger.info("Getting data summary...")
        
        collections = ['users', 'products', 'campaigns', 'events', 'messages', 'friends']
        summary = {}
        
        for collection in collections:
            count = self.db[collection].count_documents({})
            summary[collection] = count
            
        return summary

def main():
    """Main execution function"""
    # Initialize loader
    loader = MongoDBLoader()
    
    try:
        # Connect to database
        loader.connect()
        
        # Create schema and load data
        loader.create_schema()
        loader.load_data("../data/processed")
        loader.create_aggregated_views()
        
        # Get summary
        summary = loader.get_data_summary()
        print("\n=== MongoDB Data Summary ===")
        for collection, count in summary.items():
            print(f"{collection}: {count:,} records")
        
    except Exception as e:
        logger.error(f"Error during data loading: {e}")
        raise
    finally:
        loader.disconnect()

if __name__ == "__main__":
    main()
