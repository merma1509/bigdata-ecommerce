#!/usr/bin/env python3
"""SUPER-FAST PostgreSQL Data Loader"""

import os
import sys
import pandas as pd
import psycopg2
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

class SuperFastPostgreSQLLoader:
    def __init__(self):
        self.db_params = {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': int(os.getenv('POSTGRES_PORT', '5432')),
            'user': os.getenv('POSTGRES_USER', 'postgres'),
            'password': os.getenv('POSTGRES_PASSWORD', 'postgres'),
            'database': os.getenv('POSTGRES_DB', 'postgres')
        }
        
    def get_connection(self):
        """Get database connection"""
        try:
            conn = psycopg2.connect(**self.db_params)
            conn.autocommit = False
            return conn
        except psycopg2.OperationalError as e:
            print(f"Connection error: {e}")
            return None
    
    def load_all_data(self):
        """Load all data into PostgreSQL - SUPER FAST VERSION"""
        print("Starting SUPER-FAST PostgreSQL Data Loading")
        print("=" * 50)
        
        start_time = datetime.now()
        
        try:
            conn = self.get_connection()
            if not conn:
                return False
                
            with conn.cursor() as cursor:
                # Drop and recreate tables
                print("Creating tables...")
                
                tables = ['user_friends', 'messages', 'campaigns', 'users', 'products', 'categories', 'events']
                for table in tables:
                    cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
                
                # Create optimized tables
                cursor.execute("""
                    CREATE TABLE categories (
                        category_id BIGINT PRIMARY KEY,
                        category_code VARCHAR(255),
                        category_name VARCHAR(255),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    ) WITH (fillfactor=100)
                """)
                
                cursor.execute("""
                    CREATE TABLE products (
                        product_id BIGINT PRIMARY KEY,
                        category_id BIGINT,
                        brand VARCHAR(255),
                        price DECIMAL(10,2),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    ) WITH (fillfactor=100)
                """)
                
                cursor.execute("""
                    CREATE TABLE users (
                        user_id TEXT PRIMARY KEY,
                        client_id TEXT,
                        user_device_id TEXT,
                        first_purchase_date DATE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    ) WITH (fillfactor=100)
                """)
                
                cursor.execute("""
                    CREATE TABLE campaigns (
                        campaign_id BIGINT PRIMARY KEY,
                        campaign_type VARCHAR(100),
                        channel VARCHAR(100),
                        topic VARCHAR(255),
                        started_at TIMESTAMP,
                        finished_at TIMESTAMP,
                        total_count INTEGER,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    ) WITH (fillfactor=100)
                """)
                
                cursor.execute("""
                    CREATE TABLE events (
                        event_id BIGSERIAL PRIMARY KEY,
                        event_time TIMESTAMP,
                        event_type VARCHAR(100),
                        user_id TEXT,
                        product_id BIGINT,
                        category_id TEXT,
                        price DECIMAL(20,2),
                        user_session VARCHAR(255),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    ) WITH (fillfactor=100)
                """)
                
                cursor.execute("""
                    CREATE TABLE user_friends (
                        user_id TEXT,
                        friend_id TEXT,
                        friendship_date DATE DEFAULT CURRENT_DATE,
                        PRIMARY KEY (user_id, friend_id)
                    ) WITH (fillfactor=100)
                """)
                
                cursor.execute("""
                    CREATE TABLE messages (
                        message_id BIGSERIAL PRIMARY KEY,
                        message_uuid VARCHAR(255),
                        user_id TEXT,
                        campaign_id BIGINT,
                        message_type VARCHAR(100),
                        channel VARCHAR(100),
                        date DATE,
                        sent_at TIMESTAMP,
                        is_opened BOOLEAN DEFAULT FALSE,
                        is_clicked BOOLEAN DEFAULT FALSE,
                        is_purchased BOOLEAN DEFAULT FALSE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    ) WITH (fillfactor=100)
                """)
                
                conn.commit()
                print("  Tables created successfully")
                
                # SUPER FAST LOADING WITH LARGE CHUNKS
                print("Loading categories...")
                events_file = project_root / 'data/processed/events_cleaned.csv'
                categories_df = pd.read_csv(events_file, usecols=['category_id', 'category_code'])
                categories_df = categories_df.drop_duplicates(subset=['category_id'])
                categories_df['category_name'] = categories_df['category_code'].str.split('.').str[-1].fillna('unknown')
                
                categories_data = [tuple(x) for x in categories_df.itertuples(index=False, name=None)]
                cursor.executemany("""
                    INSERT INTO categories (category_id, category_code, category_name)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (category_id) DO NOTHING
                """, categories_data)
                categories_count = cursor.rowcount
                conn.commit()
                print(f"  Loaded {categories_count} categories")
                
                # Load products
                print("Loading products...")
                products_df = pd.read_csv(events_file, usecols=['product_id', 'category_id', 'brand', 'price'])
                products_df = products_df.drop_duplicates(subset=['product_id'])
                products_df['brand'] = products_df['brand'].fillna('unknown')
                products_df['price'] = pd.to_numeric(products_df['price'], errors='coerce').fillna(0.0)
                
                products_data = [tuple(x) for x in products_df.itertuples(index=False, name=None)]
                cursor.executemany("""
                    INSERT INTO products (product_id, category_id, brand, price)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (product_id) DO NOTHING
                """, products_data)
                products_count = cursor.rowcount
                conn.commit()
                print(f"  Loaded {products_count} products")
                
                # Load users
                print("Loading users...")
                users_file = project_root / 'data/processed/client_first_purchase_date_cleaned.csv'
                users_df = pd.read_csv(users_file)
                
                users_data = [tuple(x) for x in users_df.itertuples(index=False, name=None)]
                # Convert large IDs to string to avoid integer overflow and handle date properly
                users_data = [(str(u[0]) if isinstance(u[0], (int, float)) else u[0], str(u[1]) if isinstance(u[1], (int, float)) else u[1], str(u[2]) if isinstance(u[2], (int, float)) else u[2], pd.to_datetime(u[3], errors='coerce') if u[3] and pd.notna(u[3]) else None) for u in users_data]
                cursor.executemany("""
                    INSERT INTO users (user_id, client_id, user_device_id, first_purchase_date)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (user_id) DO NOTHING
                """, users_data)
                users_count = cursor.rowcount
                conn.commit()
                print(f"  Loaded {users_count} users")
                
                # Load campaigns
                print("Loading campaigns...")
                campaigns_file = project_root / 'data/processed/campaigns_cleaned.csv'
                campaigns_df = pd.read_csv(campaigns_file)
                
                campaigns_data = []
                for _, row in campaigns_df.iterrows():
                    campaign = (
                        int(row['id']),
                        str(row['campaign_type']),
                        str(row['channel']),
                        str(row['topic']),
                        row['started_at'] if pd.notna(row['started_at']) else None,
                        row['finished_at'] if pd.notna(row['finished_at']) else None,
                        float(row['total_count']) if pd.notna(row['total_count']) else 0.0
                    )
                    campaigns_data.append(campaign)
                cursor.executemany("""
                    INSERT INTO campaigns (campaign_id, campaign_type, channel, topic, started_at, finished_at, total_count)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (campaign_id) DO NOTHING
                """, campaigns_data)
                campaigns_count = cursor.rowcount
                conn.commit()
                print(f"  Loaded {campaigns_count} campaigns")
                
                # Load events - SUPER CHUNKS
                print("Loading events...")
                events_file = project_root / 'data/processed/events_cleaned.csv'
                events_count = 0
                chunk_size = 200000  # SUPER LARGE CHUNKS
                
                for chunk in pd.read_csv(events_file, chunksize=chunk_size):
                    # Select only the columns we need
                    chunk = chunk[['event_time', 'event_type', 'product_id', 'category_id', 'price', 'user_id', 'user_session']].copy()
                    chunk['event_type'] = chunk['event_type'].fillna('')
                    chunk['user_session'] = chunk['user_session'].fillna('')
                    chunk['price'] = pd.to_numeric(chunk['price'], errors='coerce').fillna(0.0)
                    
                    events_data = [tuple(x) for x in chunk.itertuples(index=False, name=None)]
                    # Convert large user_id to string to avoid integer overflow
                    events_data = [(e[0], e[1], str(e[2]) if isinstance(e[2], (int, float)) else e[2], int(e[3]) if isinstance(e[3], (int, float)) else e[3], str(e[4]) if isinstance(e[4], (int, float)) else e[4], e[5], e[6]) for e in events_data]
                    cursor.executemany("""
                        INSERT INTO events (event_time, event_type, user_id, product_id, category_id, price, user_session)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (event_time, user_id, product_id, event_type) DO NOTHING
                    """, events_data)
                    events_count += cursor.rowcount
                    conn.commit()
                    print(f"    Processed {events_count} events...")
                
                print(f"  Loaded {events_count} events")
                
                # Load friends - SUPER CHUNKS
                print("Loading friends...")
                friends_file = project_root / 'data/processed/friends_cleaned.csv'
                friends_count = 0
                chunk_size = 200000
                
                for chunk in pd.read_csv(friends_file, chunksize=chunk_size):
                    friends_data = [tuple(x) for x in chunk.itertuples(index=False, name=None)]
                    # Convert large user_id to string to avoid integer overflow
                    friends_data = [(str(f[0]) if isinstance(f[0], (int, float)) else f[0], str(f[1]) if isinstance(f[1], (int, float)) else f[1]) for f in friends_data]
                    cursor.executemany("""
                        INSERT INTO user_friends (user_id, friend_id)
                        VALUES (%s, %s)
                        ON CONFLICT (user_id, friend_id) DO NOTHING
                    """, friends_data)
                    friends_count += cursor.rowcount
                    conn.commit()
                    print(f"    Processed {friends_count} friend relationships...")
                
                print(f"  Loaded {friends_count} friend relationships")
                
                # Load messages - SUPER CHUNKS
                print("Loading messages...")
                messages_file = project_root / 'data/processed/messages_cleaned.csv'
                messages_count = 0
                chunk_size = 100000
                
                for chunk in pd.read_csv(messages_file, chunksize=chunk_size):
                    # Select only the columns we need
                    chunk = chunk[['message_id', 'campaign_id', 'message_type', 'channel', 'date', 'sent_at', 'is_opened', 'is_clicked', 'is_purchased', 'user_id']].copy()
                    chunk['message_id'] = chunk['message_id'].fillna('')
                    chunk['message_type'] = chunk['message_type'].fillna('')
                    chunk['channel'] = chunk['channel'].fillna('')
                    chunk['date'] = pd.to_datetime(chunk['date'], errors='coerce')
                    chunk['sent_at'] = pd.to_datetime(chunk['sent_at'], errors='coerce')
                    chunk['is_opened'] = chunk['is_opened'].fillna(False).astype(bool)
                    chunk['is_clicked'] = chunk['is_clicked'].fillna(False).astype(bool)
                    chunk['is_purchased'] = chunk['is_purchased'].fillna(False).astype(bool)
                    
                    messages_data = [tuple(x) for x in chunk.itertuples(index=False, name=None)]
                    # Convert large user_id to string to avoid integer overflow
                    messages_data = [(m[0], str(m[9]) if isinstance(m[9], (int, float)) else m[9], m[1], m[2], m[3], m[4], m[5], m[6], m[7], m[8]) for m in messages_data]
                    cursor.executemany("""
                        INSERT INTO messages (message_uuid, user_id, campaign_id, message_type, channel, date, sent_at, is_opened, is_clicked, is_purchased)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (message_uuid) DO NOTHING
                    """, messages_data)
                    messages_count += cursor.rowcount
                    conn.commit()
                    print(f"    Processed {messages_count} messages...")
                
                print(f"  Loaded {messages_count} messages")
                
            conn.close()
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # Summary
            print("\nSUPER-FAST PostgreSQL Loading Summary:")
            print(f"  categories: {categories_count:,} records")
            print(f"  products: {products_count:,} records")
            print(f"  users: {users_count:,} records")
            print(f"  campaigns: {campaigns_count:,} records")
            print(f"  events: {events_count:,} records")
            print(f"  friends: {friends_count:,} records")
            print(f"  messages: {messages_count:,} records")
            print(f"\nTotal loading time: {duration:.2f} seconds")
            
            return True
            
        except Exception as e:
            print(f"Error loading data: {e}")
            return False

def main():
    """Main function"""
    loader = SuperFastPostgreSQLLoader()
    success = loader.load_all_data()
    
    if success:
        print("\nSUPER-FAST PostgreSQL data loading completed successfully!")
    else:
        print("\nSUPER-FAST PostgreSQL data loading failed!")
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())
