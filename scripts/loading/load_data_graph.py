#!/usr/bin/env python3
"""SUPER-FAST Neo4j Data Loader"""

import os
import sys
import time
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
from neo4j import GraphDatabase

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

class SuperFastNeo4jLoader:
    def __init__(self):
        self.driver = GraphDatabase.driver(
            'bolt://localhost:7687', 
            auth=('neo4j', os.getenv('NEO4J_PASSWORD', 'neo4j_pass')),
            max_connection_lifetime=30,
            max_connection_pool_size=50,
            connection_acquisition_timeout=60
        )
        
    def close(self):
        """Close database connection"""
        if self.driver:
            self.driver.close()
    
    def clear_database(self):
        """Clear existing database"""
        print("Clearing existing Neo4j database...")
        
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            print("  Database cleared successfully")
    
    def load_all_data(self):
        """Load all data into Neo4j - SUPER FAST VERSION"""
        print("Starting SUPER-FAST Neo4j Data Loading")
        print("=" * 50)
        
        start_time = datetime.now()
        
        # Clear database first
        self.clear_database()
        
        try:
            with self.driver.session() as session:
                # Load categories
                print("Loading categories...")
                events_file = project_root / 'data/processed/events_cleaned.csv'
                categories_df = pd.read_csv(events_file, usecols=['category_id', 'category_code'])
                categories_df = categories_df.drop_duplicates(subset=['category_id'])
                categories_df['category_name'] = categories_df['category_code'].str.split('.').str[-1].fillna('unknown')
                
                categories_data = []
                for _, row in categories_df.iterrows():
                    categories_data.append({
                        'category_id': int(row['category_id']),
                        'category_code': str(row['category_code']),
                        'category_name': str(row['category_name'])
                    })
                
                # Use UNWIND for batch loading
                session.run("""
                    UNWIND $categories AS cat
                    MERGE (c:Category {category_id: cat.category_id})
                    SET c.category_code = cat.category_code,
                        c.category_name = cat.category_name,
                        c.created_at = datetime()
                """, categories=categories_data)
                
                print(f"  Loaded {len(categories_data)} categories")
                
                # Load products
                print("Loading products...")
                products_df = pd.read_csv(events_file, usecols=['product_id', 'category_id', 'brand', 'price'], nrows=200000)
                products_df = products_df.drop_duplicates(subset=['product_id'])
                products_df['brand'] = products_df['brand'].fillna('unknown')
                products_df['price'] = pd.to_numeric(products_df['price'], errors='coerce').fillna(0.0)
                
                products_data = []
                for _, row in products_df.iterrows():
                    products_data.append({
                        'product_id': int(row['product_id']),
                        'category_id': int(row['category_id']),
                        'brand': str(row['brand']),
                        'price': float(row['price'])
                    })
                
                # Use UNWIND for batch loading
                session.run("""
                    UNWIND $products AS prod
                    MERGE (p:Product {product_id: prod.product_id})
                    SET p.category_id = prod.category_id,
                        p.brand = prod.brand,
                        p.price = prod.price,
                        p.created_at = datetime()
                """, products=products_data)
                
                print(f"  Loaded {len(products_data)} products")
                
                # Load users - limit for speed
                print("Loading users...")
                users_file = project_root / 'data/processed/client_first_purchase_date_cleaned.csv'
                users_df = pd.read_csv(users_file, nrows=200000)
                
                users_data = []
                for _, row in users_df.iterrows():
                    users_data.append({
                        'user_id': int(row['user_id']),
                        'client_id': int(row['client_id']),
                        'user_device_id': int(row['user_device_id']),
                        'first_purchase_date': str(row['first_purchase_date']) if pd.notna(row['first_purchase_date']) else None
                    })
                
                # Use UNWIND for batch loading
                session.run("""
                    UNWIND $users AS usr
                    MERGE (u:User {user_id: usr.user_id})
                    SET u.client_id = usr.client_id,
                        u.user_device_id = usr.user_device_id,
                        u.first_purchase_date = usr.first_purchase_date,
                        u.created_at = datetime()
                """, users=users_data)
                
                print(f"  Loaded {len(users_data)} users")
                
                # Load campaigns
                print("Loading campaigns...")
                campaigns_file = project_root / 'data/processed/campaigns_cleaned.csv'
                campaigns_df = pd.read_csv(campaigns_file)
                
                campaigns_data = []
                for _, row in campaigns_df.iterrows():
                    campaigns_data.append({
                        'campaign_id': int(row['id']),
                        'campaign_type': str(row['campaign_type']),
                        'channel': str(row['channel']),
                        'topic': str(row['topic']),
                        'started_at': row['started_at'] if pd.notna(row['started_at']) else None,
                        'finished_at': row['finished_at'] if pd.notna(row['finished_at']) else None,
                        'total_count': int(row['total_count']) if pd.notna(row['total_count']) else 0
                    })
                
                # Use UNWIND for batch loading
                session.run("""
                    UNWIND $campaigns AS camp
                    MERGE (c:Campaign {campaign_id: camp.campaign_id})
                    SET c.campaign_type = camp.campaign_type,
                        c.channel = camp.channel,
                        c.topic = camp.topic,
                        c.started_at = camp.started_at,
                        c.finished_at = camp.finished_at,
                        c.total_count = camp.total_count,
                        c.created_at = datetime()
                """, campaigns=campaigns_data)
                
                print(f"  Loaded {len(campaigns_data)} campaigns")
                
                # Load events - COMPLETE DATASET
                print("Loading events...")
                events_file = project_root / 'data/processed/events_cleaned.csv'
                events_count = 0
                chunk_size = 10000  # Smaller chunks for stability
                
                for chunk in pd.read_csv(events_file, chunksize=chunk_size):
                    chunk['event_type'] = chunk['event_type'].fillna('')
                    chunk['user_session'] = chunk['user_session'].fillna('')
                    chunk['price'] = pd.to_numeric(chunk['price'], errors='coerce').fillna(0.0)
                    
                    events = []
                    for _, row in chunk.iterrows():
                        event = {
                            'event_time': str(row['event_time']),
                            'event_type': str(row['event_type']),
                            'user_id': str(row['user_id']),
                            'product_id': int(row['product_id']),
                            'category_id': str(row['category_id']),
                            'price': float(row['price']),
                            'user_session': str(row['user_session'])
                        }
                        events.append(event)
                    
                    if events:
                        # Retry logic for transactions
                        max_retries = 3
                        for attempt in range(max_retries):
                            try:
                                with self.driver.session() as session:
                                    session.run("""
                                        UNWIND $events AS evt
                                        MERGE (u:User {user_id: evt.user_id})
                                        MERGE (p:Product {product_id: evt.product_id})
                                        MERGE (u)-[e:EVENT]->(p)
                                        SET e.event_type = evt.event_type,
                                            e.price = evt.price,
                                            e.user_session = evt.user_session,
                                            e.event_time = evt.event_time,
                                            e.created_at = datetime()
                                    """, events=events)
                                events_count += len(events)
                                print(f"    Processed {events_count} events...")
                                break
                            except Exception as e:
                                if attempt == max_retries - 1:
                                    print(f"    Error loading events chunk: {e}")
                                    raise
                                time.sleep(2)  # Wait before retry
                
                print(f"  Loaded {events_count} events")
                
                # Load friends - COMPLETE DATASET
                print("Loading friends...")
                friends_file = project_root / 'data/processed/friends_cleaned.csv'
                friends_count = 0
                chunk_size = 50000  # Large chunks for complete loading
                
                for chunk in pd.read_csv(friends_file, chunksize=chunk_size):
                    friends_data = []
                    for _, row in chunk.iterrows():
                        friends_data.append({
                            'user_id': str(row['friend1']),
                            'friend_id': str(row['friend2']),
                            'friendship_date': datetime.now()
                        })
                    
                    if friends_data:
                        with self.driver.session() as session:
                            session.run("""
                                UNWIND $friends AS f
                                MERGE (u:User {user_id: f.user_id})
                                MERGE (v:User {user_id: f.friend_id})
                                MERGE (u)-[r:FRIENDS_WITH]->(v)
                                SET r.friendship_date = f.friendship_date,
                                    r.created_at = datetime()
                            """, friends=friends_data)
                        friends_count += len(friends_data)
                        print(f"    Processed {friends_count} friend relationships...")
            
                print(f"  Loaded {friends_count} friend relationships")
                
                # Load messages - COMPLETE DATASET
                print("Loading messages...")
                messages_file = project_root / 'data/processed/messages_cleaned.csv'
                messages_count = 0
                chunk_size = 100000  #  for faster loading
                
                for chunk in pd.read_csv(messages_file, chunksize=chunk_size):
                    messages_data = []
                    for _, row in chunk.iterrows():
                        messages_data.append({
                            'message_uuid': str(row['message_id']),
                            'user_id': str(row['user_id']),
                            'campaign_id': int(row['campaign_id']),
                            'message_type': str(row['message_type']),
                            'channel': str(row['channel']),
                            'date': str(row['date']),
                            'sent_at': str(row['sent_at']),
                            'is_opened': bool(row['is_opened']),
                            'is_clicked': bool(row['is_clicked']),
                            'is_purchased': bool(row['is_purchased'])
                        })
                    
                    if messages_data:
                        with self.driver.session() as session:
                            session.run("""
                                UNWIND $messages AS msg
                                MERGE (u:User {user_id: msg.user_id})
                                MERGE (c:Campaign {campaign_id: msg.campaign_id})
                                MERGE (m:Message {message_uuid: msg.message_uuid})
                                SET m.message_type = msg.message_type,
                                    m.channel = msg.channel,
                                    m.date = msg.date,
                                    m.sent_at = msg.sent_at,
                                    m.is_opened = msg.is_opened,
                                    m.is_clicked = msg.is_clicked,
                                    m.is_purchased = msg.is_purchased,
                                    m.created_at = datetime()
                                MERGE (u)-[:RECEIVED]->(m)
                                MERGE (c)-[:PART_OF_CAMPAIGN]->(m)
                            """, messages=messages_data)
                        messages_count += len(messages_data)
                        print(f"    Processed {messages_count} messages (100K chunk optimization)...")
                
                print(f"  Loaded {messages_count} messages")
                
            self.close()
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # Summary
            print("\nSUPER-FAST Neo4j Loading Summary:")
            print(f"  categories: {len(categories_data):,} nodes")
            print(f"  products: {len(products_data):,} nodes")
            print(f"  users: {len(users_data):,} nodes")
            print(f"  campaigns: {len(campaigns_data):,} nodes")
            print(f"  messages: {messages_count:,} nodes")
            print(f"  events: {events_count:,} relationships")
            print(f"  friends: {friends_count:,} relationships")
            print(f"  user-message: {messages_count:,} relationships")
            print(f"  campaign-message: {messages_count:,} relationships")
            print(f"\nTotal loading time: {duration:.2f} seconds")
            
            return True
            
        except Exception as e:
            print(f"Error loading data: {str(e)}")
            return False

def main():
    """Main function"""
    loader = SuperFastNeo4jLoader()
    success = loader.load_all_data()
    
    if success:
        print("\nSUPER-FAST Neo4j data loading completed successfully!")
    else:
        print("\nSUPER-FAST Neo4j data loading failed!")
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())
