#!/usr/bin/env python3
"""SUPER-FAST MongoDB Data Loader"""

import os
import sys
import pandas as pd
from pymongo import MongoClient
from datetime import datetime
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

def load_env():
    """Load environment variables from .env file"""
    env_file = project_root / '.env'
    if env_file.exists():
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip()

load_env()

class SuperFastMongoLoader:
    def __init__(self):
        self.client = MongoClient(
            host=os.getenv('MONGO_HOST', 'localhost'),
            port=int(os.getenv('MONGO_PORT', '27017')),
            serverSelectionTimeoutMS=5000,
            maxPoolSize=50,  # SUPER HIGH POOL
            retryWrites=False,
            w=1  # ACKNOWLEDGED WRITES FOR COMPATIBILITY
        )
        self.db = self.client[os.getenv('MONGO_DATABASE', 'ecommerce')]
        
    def load_all_data(self):
        """Load all data into MongoDB - SUPER FAST VERSION"""
        print("Starting SUPER-FAST MongoDB Data Loading")
        print("=" * 50)
        
        start_time = datetime.now()
        
        try:
            # Drop existing collections
            print("Creating collections...")
            self.db.categories.drop()
            self.db.products.drop()
            self.db.users.drop()
            self.db.campaigns.drop()
            self.db.events.drop()
            self.db.friends.drop()
            self.db.messages.drop()
            print("  Collections dropped successfully")
            
            # Load categories
            print("Loading categories...")
            events_file = project_root / 'data/processed/events_cleaned.csv'
            categories_df = pd.read_csv(events_file, usecols=['category_id', 'category_code'])
            categories_df = categories_df.drop_duplicates(subset=['category_id'])
            categories_df['category_name'] = categories_df['category_code'].str.split('.').str[-1].fillna('unknown')
            
            categories = []
            for _, row in categories_df.iterrows():
                category = {
                    'category_id': int(row['category_id']),
                    'category_code': str(row['category_code']),
                    'category_name': str(row['category_name']),
                    'created_at': datetime.now()
                }
                categories.append(category)
            
            if categories:
                self.db.categories.insert_many(categories, ordered=False)
                print(f"  Loaded {len(categories)} categories")
            
            # Load products
            print("Loading products...")
            products_df = pd.read_csv(events_file, usecols=['product_id', 'category_id', 'brand', 'price'])
            products_df = products_df.drop_duplicates(subset=['product_id'])
            products_df['brand'] = products_df['brand'].fillna('unknown')
            products_df['price'] = pd.to_numeric(products_df['price'], errors='coerce').fillna(0.0)
            
            products = []
            for _, row in products_df.iterrows():
                product = {
                    'product_id': int(row['product_id']),
                    'category_id': int(row['category_id']),
                    'brand': str(row['brand']),
                    'price': float(row['price']),
                    'created_at': datetime.now()
                }
                products.append(product)
            
            if products:
                self.db.products.insert_many(products, ordered=False)
                print(f"  Loaded {len(products)} products")
            
            # Load users
            print("Loading users...")
            users_file = project_root / 'data/processed/client_first_purchase_date_cleaned.csv'
            users_df = pd.read_csv(users_file)
            
            users = []
            for _, row in users_df.iterrows():
                user = {
                    'user_id': int(row['user_id']),
                    'client_id': int(row['client_id']),
                    'user_device_id': int(row['user_device_id']),
                    'first_purchase_date': str(row['first_purchase_date']) if pd.notna(row['first_purchase_date']) else None,
                    'created_at': datetime.now()
                }
                users.append(user)
            
            if users:
                self.db.users.insert_many(users, ordered=False)
                print(f"  Loaded {len(users)} users")
            
            # Load campaigns
            print("Loading campaigns...")
            campaigns_file = project_root / 'data/processed/campaigns_cleaned.csv'
            campaigns_df = pd.read_csv(campaigns_file)
            
            campaigns = []
            for _, row in campaigns_df.iterrows():
                campaign = {
                    'campaign_id': int(row['id']),
                    'campaign_type': str(row['campaign_type']),
                    'channel': str(row['channel']),
                    'topic': str(row['topic']),
                    'started_at': row['started_at'] if pd.notna(row['started_at']) else None,
                    'finished_at': row['finished_at'] if pd.notna(row['finished_at']) else None,
                    'total_count': int(row['total_count']) if pd.notna(row['total_count']) else 0,
                    'created_at': datetime.now()
                }
                campaigns.append(campaign)
            
            if campaigns:
                self.db.campaigns.insert_many(campaigns, ordered=False)
                print(f"  Loaded {len(campaigns)} campaigns")
            
            # Load events - SUPER CHUNKS
            print("Loading events...")
            events_file = project_root / 'data/processed/events_cleaned.csv'
            events_count = 0
            chunk_size = 500000  # SUPER LARGE CHUNKS
            
            for chunk in pd.read_csv(events_file, chunksize=chunk_size):
                chunk['event_type'] = chunk['event_type'].fillna('')
                chunk['user_session'] = chunk['user_session'].fillna('')
                chunk['price'] = pd.to_numeric(chunk['price'], errors='coerce').fillna(0.0)
                
                events = []
                for _, row in chunk.iterrows():
                    event = {
                        'event_time': row['event_time'] if pd.notna(row['event_time']) else None,
                        'event_type': str(row['event_type']),
                        'user_id': int(row['user_id']),
                        'product_id': int(row['product_id']),
                        'category_id': int(row['category_id']),
                        'price': float(row['price']),
                        'user_session': str(row['user_session']),
                        'created_at': datetime.now()
                    }
                    events.append(event)
                
                if events:
                    self.db.events.insert_many(events, ordered=False)
                    events_count += len(events)
                    print(f"    Processed {events_count} events...")
            
            print(f"  Loaded {events_count} events")
            
            # Load friends - SUPER CHUNKS
            print("Loading friends...")
            friends_file = project_root / 'data/processed/friends_cleaned.csv'
            friends_count = 0
            chunk_size = 500000
            
            for chunk in pd.read_csv(friends_file, chunksize=chunk_size):
                friends = []
                for _, row in chunk.iterrows():
                    friend = {
                        'user_id': int(row['friend1']),
                        'friend_id': int(row['friend2']),
                        'friendship_date': datetime.now(),
                        'created_at': datetime.now()
                    }
                    friends.append(friend)
                
                if friends:
                    self.db.friends.insert_many(friends, ordered=False)
                    friends_count += len(friends)
                    print(f"    Processed {friends_count} friend relationships...")
            
            print(f"  Loaded {friends_count} friend relationships")
            
            # Load messages - SUPER CHUNKS
            print("Loading messages...")
            messages_file = project_root / 'data/processed/messages_cleaned.csv'
            messages_count = 0
            chunk_size = 200000
            
            for chunk in pd.read_csv(messages_file, chunksize=chunk_size):
                chunk['message_id'] = chunk['message_id'].fillna('')
                chunk['message_type'] = chunk['message_type'].fillna('')
                chunk['channel'] = chunk['channel'].fillna('')
                chunk['date'] = pd.to_datetime(chunk['date'], errors='coerce')
                chunk['sent_at'] = pd.to_datetime(chunk['sent_at'], errors='coerce')
                chunk['is_opened'] = chunk['is_opened'].fillna(False).astype(bool)
                chunk['is_clicked'] = chunk['is_clicked'].fillna(False).astype(bool)
                chunk['is_purchased'] = chunk['is_purchased'].fillna(False).astype(bool)
                
                messages = []
                for _, row in chunk.iterrows():
                    message = {
                        'message_id': str(row['message_id']),
                        'user_id': int(row['user_id']),
                        'campaign_id': int(row['campaign_id']) if pd.notna(row['campaign_id']) else None,
                        'message_type': str(row['message_type']),
                        'channel': str(row['channel']),
                        'date': row['date'] if pd.notna(row['date']) else None,
                        'sent_at': row['sent_at'] if pd.notna(row['sent_at']) else None,
                        'is_opened': bool(row['is_opened']),
                        'is_clicked': bool(row['is_clicked']),
                        'is_purchased': bool(row['is_purchased']),
                        'created_at': datetime.now()
                    }
                    messages.append(message)
                
                if messages:
                    self.db.messages.insert_many(messages, ordered=False)
                    messages_count += len(messages)
                    print(f"    Processed {messages_count} messages...")
            
            print(f"  Loaded {messages_count} messages")
            
            self.client.close()
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # Summary
            print("\nSUPER-FAST MongoDB Loading Summary:")
            print(f"  categories: {len(categories):,} documents")
            print(f"  products: {len(products):,} documents")
            print(f"  users: {len(users):,} documents")
            print(f"  campaigns: {len(campaigns):,} documents")
            print(f"  events: {events_count:,} documents")
            print(f"  friends: {friends_count:,} documents")
            print(f"  messages: {messages_count:,} documents")
            print(f"\nTotal loading time: {duration:.2f} seconds")
            
            return True
            
        except Exception as e:
            print(f"Error loading data: {e}")
            return False

def main():
    """Main function"""
    loader = SuperFastMongoLoader()
    success = loader.load_all_data()
    
    if success:
        print("\nSUPER-FAST MongoDB data loading completed successfully!")
    else:
        print("\nSUPER-FAST MongoDB data loading failed!")
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())
