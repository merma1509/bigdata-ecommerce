#!/usr/bin/env python3
"""PostgreSQL Data Loading Script
This script creates the PostgreSQL schema and loads the cleaned datainto the appropriate tables"""

import psycopg2
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PostgreSQLLoader:
    """PostgreSQL database loader"""
    
    def __init__(self, dbname: str, user: str, password: str, host: str = "localhost", port: str = "5432"):
        self.connection_params = {
            'dbname': dbname,
            'user': user,
            'password': password,
            'host': host,
            'port': port
        }
        self.conn = None
        
    def connect(self):
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(**self.connection_params)
            self.conn.autocommit = False
            logger.info("Connected to PostgreSQL database")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise
    
    def disconnect(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Disconnected from PostgreSQL database")
    
    def create_schema(self):
        """Create database schema"""
        logger.info("Creating PostgreSQL schema...")
        
        with self.conn.cursor() as cur:
            # Drop existing tables if they exist
            cur.execute("DROP TABLE IF EXISTS events CASCADE;")
            cur.execute("DROP TABLE IF EXISTS messages CASCADE;")
            cur.execute("DROP TABLE IF EXISTS friends CASCADE;")
            cur.execute("DROP TABLE IF EXISTS campaigns CASCADE;")
            cur.execute("DROP TABLE IF EXISTS products CASCADE;")
            cur.execute("DROP TABLE IF EXISTS users CASCADE;")
            
            # Create users table
            cur.execute("""
                CREATE TABLE users (
                    user_id BIGINT PRIMARY KEY,
                    first_purchase_date DATE
                );
            """)
            
            # Create products table
            cur.execute("""
                CREATE TABLE products (
                    product_id BIGINT PRIMARY KEY,
                    category_id BIGINT,
                    category_code TEXT,
                    brand TEXT,
                    price DECIMAL(10,2)
                );
            """)
            
            # Create campaigns table
            cur.execute("""
                CREATE TABLE campaigns (
                    campaign_id BIGINT,
                    campaign_type VARCHAR(20),
                    channel VARCHAR(20),
                    topic TEXT,
                    started_at TIMESTAMP,
                    finished_at TIMESTAMP,
                    total_count INTEGER,
                    ab_test BOOLEAN,
                    warmup_mode BOOLEAN,
                    hour_limit INTEGER,
                    subject_length DECIMAL(5,1),
                    subject_with_personalization BOOLEAN,
                    subject_with_deadline BOOLEAN,
                    subject_with_emoji BOOLEAN,
                    subject_with_bonuses BOOLEAN,
                    subject_with_discount BOOLEAN,
                    subject_with_saleout BOOLEAN,
                    is_test BOOLEAN,
                    position INTEGER,
                    PRIMARY KEY (campaign_id, campaign_type)
                );
            """)
            
            # Create events table
            cur.execute("""
                CREATE TABLE events (
                    event_id BIGSERIAL PRIMARY KEY,
                    event_time TIMESTAMP,
                    event_type VARCHAR(20),
                    product_id BIGINT REFERENCES products(product_id),
                    category_id BIGINT,
                    category_code TEXT,
                    brand TEXT,
                    price DECIMAL(10,2),
                    user_id BIGINT REFERENCES users(user_id),
                    user_session UUID
                );
            """)
            
            # Create messages table
            cur.execute("""
                CREATE TABLE messages (
                    message_id BIGSERIAL PRIMARY KEY,
                    campaign_id BIGINT,
                    message_type VARCHAR(20),
                    channel VARCHAR(20),
                    client_id VARCHAR(50),
                    email_provider VARCHAR(50),
                    platform VARCHAR(20),
                    stream VARCHAR(20),
                    date DATE,
                    sent_at TIMESTAMP,
                    is_opened BOOLEAN,
                    opened_first_time_at TIMESTAMP,
                    opened_last_time_at TIMESTAMP,
                    is_clicked BOOLEAN,
                    clicked_first_time_at TIMESTAMP,
                    clicked_last_time_at TIMESTAMP,
                    is_unsubscribed BOOLEAN,
                    unsubscribed_at TIMESTAMP,
                    is_hard_bounced BOOLEAN,
                    hard_bounced_at TIMESTAMP,
                    is_soft_bounced BOOLEAN,
                    soft_bounced_at TIMESTAMP,
                    is_complained BOOLEAN,
                    complained_at TIMESTAMP,
                    is_blocked BOOLEAN,
                    blocked_at TIMESTAMP,
                    is_purchased BOOLEAN,
                    purchased_at TIMESTAMP
                );
            """)
            
            # Create friends table
            cur.execute("""
                CREATE TABLE friends (
                    user_id BIGINT,
                    friend_id BIGINT,
                    PRIMARY KEY (user_id, friend_id),
                    FOREIGN KEY (user_id) REFERENCES users(user_id),
                    FOREIGN KEY (friend_id) REFERENCES users(user_id)
                );
            """)
            
            self.conn.commit()
            logger.info("PostgreSQL schema created successfully")
    
    def create_indexes(self):
        """Create performance indexes"""
        logger.info("Creating indexes...")
        
        with self.conn.cursor() as cur:
            # Events table indexes
            cur.execute("CREATE INDEX idx_events_user_id ON events(user_id);")
            cur.execute("CREATE INDEX idx_events_product_id ON events(product_id);")
            cur.execute("CREATE INDEX idx_events_event_time ON events(event_time);")
            cur.execute("CREATE INDEX idx_events_event_type ON events(event_type);")
            
            # Messages table indexes
            cur.execute("CREATE INDEX idx_messages_campaign_id ON messages(campaign_id);")
            cur.execute("CREATE INDEX idx_messages_client_id ON messages(client_id);")
            cur.execute("CREATE INDEX idx_messages_sent_at ON messages(sent_at);")
            
            # Products table indexes
            cur.execute("CREATE INDEX idx_products_category_id ON products(category_id);")
            cur.execute("CREATE INDEX idx_products_brand ON products(brand);")
            cur.execute("CREATE INDEX idx_products_category_code ON products USING gin(to_tsvector('english', category_code));")
            
            self.conn.commit()
            logger.info("Indexes created successfully")
    
    def load_data(self, data_path: str):
        """Load cleaned data into tables"""
        logger.info("Loading data into PostgreSQL...")
        
        data_path = Path(data_path)
        
        # Load users data
        logger.info("Loading users data...")
        users_df = pd.read_csv(data_path / "client_first_purchase_date_cleaned.csv")
        with self.conn.cursor() as cur:
            for _, row in users_df.iterrows():
                cur.execute("""
                    INSERT INTO users (user_id, first_purchase_date) 
                    VALUES (%s, %s) 
                    ON CONFLICT (user_id) DO NOTHING
                """, (row['user_id'], row['first_purchase_date']))
        
        # Load products data (from events)
        logger.info("Loading products data...")
        events_df = pd.read_csv(data_path / "events_cleaned.csv")
        products_df = events_df[['product_id', 'category_id', 'category_code', 'brand', 'price']].drop_duplicates()
        with self.conn.cursor() as cur:
            for _, row in products_df.iterrows():
                cur.execute("""
                    INSERT INTO products (product_id, category_id, category_code, brand, price) 
                    VALUES (%s, %s, %s, %s, %s) 
                    ON CONFLICT (product_id) DO NOTHING
                """, (row['product_id'], row['category_id'], row['category_code'], 
                      row['brand'], row['price']))
        
        # Load campaigns data
        logger.info("Loading campaigns data...")
        campaigns_df = pd.read_csv(data_path / "campaigns_cleaned.csv")
        with self.conn.cursor() as cur:
            for _, row in campaigns_df.iterrows():
                cur.execute("""
                    INSERT INTO campaigns (
                        campaign_id, campaign_type, channel, topic, started_at, finished_at,
                        total_count, ab_test, warmup_mode, hour_limit, subject_length,
                        subject_with_personalization, subject_with_deadline, subject_with_emoji,
                        subject_with_bonuses, subject_with_discount, subject_with_saleout,
                        is_test, position
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (campaign_id, campaign_type) DO NOTHING
                """, tuple(row))
        
        # Load events data
        logger.info("Loading events data...")
        with self.conn.cursor() as cur:
            for _, row in events_df.iterrows():
                cur.execute("""
                    INSERT INTO events (
                        event_time, event_type, product_id, category_id, category_code,
                        brand, price, user_id, user_session
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (row['event_time'], row['event_type'], row['product_id'], 
                      row['category_id'], row['category_code'], row['brand'], 
                      row['price'], row['user_id'], row['user_session']))
        
        # Load messages data
        logger.info("Loading messages data...")
        messages_df = pd.read_csv(data_path / "messages_cleaned.csv")
        with self.conn.cursor() as cur:
            for _, row in messages_df.iterrows():
                cur.execute("""
                    INSERT INTO messages (
                        campaign_id, message_type, channel, client_id, email_provider,
                        platform, stream, date, sent_at, is_opened, opened_first_time_at,
                        opened_last_time_at, is_clicked, clicked_first_time_at,
                        clicked_last_time_at, is_unsubscribed, unsubscribed_at,
                        is_hard_bounced, hard_bounced_at, is_soft_bounced,
                        soft_bounced_at, is_complained, complained_at, is_blocked,
                        blocked_at, is_purchased, purchased_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, tuple(row))
        
        # Load friends data
        logger.info("Loading friends data...")
        friends_df = pd.read_csv(data_path / "friends_cleaned.csv")
        with self.conn.cursor() as cur:
            for _, row in friends_df.iterrows():
                cur.execute("""
                    INSERT INTO friends (user_id, friend_id) 
                    VALUES (%s, %s) 
                    ON CONFLICT (user_id, friend_id) DO NOTHING
                """, (row['user_id'], row['friend_id']))
        
        self.conn.commit()
        logger.info("Data loading completed successfully")
    
    def get_data_summary(self):
        """Get summary statistics of loaded data"""
        logger.info("Getting data summary...")
        
        with self.conn.cursor() as cur:
            tables = ['users', 'products', 'campaigns', 'events', 'messages', 'friends']
            summary = {}
            
            for table in tables:
                cur.execute(f"SELECT COUNT(*) FROM {table};")
                count = cur.fetchone()[0]
                summary[table] = count
                
        return summary

def main():
    """Main execution function"""
    # Database connection parameters
    db_params = {
        'dbname': 'ecommerce',
        'user': 'postgres',
        'password': 'password',
        'host': 'localhost',
        'port': '5432'
    }
    
    # Initialize loader
    loader = PostgreSQLLoader(**db_params)
    
    try:
        # Connect to database
        loader.connect()
        
        # Create schema and load data
        loader.create_schema()
        loader.create_indexes()
        loader.load_data("../data/processed")
        
        # Get summary
        summary = loader.get_data_summary()
        print("\n=== PostgreSQL Data Summary ===")
        for table, count in summary.items():
            print(f"{table}: {count:,} records")
        
    except Exception as e:
        logger.error(f"Error during data loading: {e}")
        raise
    finally:
        loader.disconnect()

if __name__ == "__main__":
    main()
