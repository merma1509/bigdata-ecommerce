#!/usr/bin/env python3
"""
PostgreSQL Data Model Implementation and Data Loading
Creates optimized relational schema and loads cleaned data
"""

import psycopg2
import pandas as pd
import os
from datetime import datetime
import sys

# Database connection parameters
DB_PARAMS = {
    'host': 'localhost',
    'port': 5432,
    'database': 'ecommerce',
    'user': 'ecommerce_user',
    'password': None  # Will be loaded from environment
}

def get_db_connection():
    """Get database connection"""
    # Load password from environment
    import os
    DB_PARAMS['password'] = os.getenv('POSTGRES_PASSWORD')
    
    if not DB_PARAMS['password']:
        raise ValueError("POSTGRES_PASSWORD environment variable not set")
    
    return psycopg2.connect(**DB_PARAMS)

def create_tables(conn):
    """Create optimized PostgreSQL tables"""
    print("🏗️ Creating PostgreSQL tables...")
    
    cursor = conn.cursor()
    
    # Drop existing tables
    cursor.execute("DROP TABLE IF EXISTS message_events CASCADE")
    cursor.execute("DROP TABLE IF EXISTS campaign_performance CASCADE")
    cursor.execute("DROP TABLE IF EXISTS client_purchases CASCADE")
    cursor.execute("DROP TABLE IF EXISTS user_friends CASCADE")
    cursor.execute("DROP TABLE IF EXISTS user_events CASCADE")
    cursor.execute("DROP TABLE IF EXISTS campaigns CASCADE")
    cursor.execute("DROP TABLE IF EXISTS users CASCADE")
    cursor.execute("DROP TABLE IF EXISTS products CASCADE")
    cursor.execute("DROP TABLE IF EXISTS categories CASCADE")
    
    # Categories table
    cursor.execute("""
        CREATE TABLE categories (
            category_id BIGINT PRIMARY KEY,
            category_code VARCHAR(255),
            category_name VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Products table
    cursor.execute("""
        CREATE TABLE products (
            product_id BIGINT PRIMARY KEY,
            category_id BIGINT REFERENCES categories(category_id),
            brand VARCHAR(255),
            price DECIMAL(10,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Users table
    cursor.execute("""
        CREATE TABLE users (
            user_id BIGINT PRIMARY KEY,
            client_id BIGINT UNIQUE,
            user_device_id INTEGER,
            first_purchase_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Campaigns table
    cursor.execute("""
        CREATE TABLE campaigns (
            id INTEGER PRIMARY KEY,
            campaign_type VARCHAR(50),
            channel VARCHAR(50),
            topic VARCHAR(255),
            started_at TIMESTAMP,
            finished_at TIMESTAMP,
            total_count BIGINT,
            ab_test VARCHAR(100),
            warmup_mode VARCHAR(100),
            hour_limit DECIMAL(5,2),
            subject_length INTEGER,
            subject_with_personalization BOOLEAN DEFAULT FALSE,
            subject_with_deadline BOOLEAN DEFAULT FALSE,
            subject_with_emoji BOOLEAN DEFAULT FALSE,
            subject_with_bonuses BOOLEAN DEFAULT FALSE,
            subject_with_discount BOOLEAN DEFAULT FALSE,
            subject_with_saleout BOOLEAN DEFAULT FALSE,
            is_test BOOLEAN DEFAULT FALSE,
            position INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # User events table
    cursor.execute("""
        CREATE TABLE user_events (
            event_id BIGSERIAL PRIMARY KEY,
            event_time TIMESTAMP,
            event_type VARCHAR(50),
            product_id BIGINT REFERENCES products(product_id),
            category_id BIGINT REFERENCES categories(category_id),
            user_id BIGINT REFERENCES users(user_id),
            user_session VARCHAR(255),
            price DECIMAL(10,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # User friends table
    cursor.execute("""
        CREATE TABLE user_friends (
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT REFERENCES users(user_id),
            friend_id BIGINT REFERENCES users(user_id),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, friend_id),
            CHECK(user_id != friend_id)
        )
    """)
    
    # Client purchases table
    cursor.execute("""
        CREATE TABLE client_purchases (
            id BIGSERIAL PRIMARY KEY,
            client_id BIGINT REFERENCES users(client_id),
            first_purchase_date DATE,
            user_id BIGINT REFERENCES users(user_id),
            user_device_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Campaign performance table
    cursor.execute("""
        CREATE TABLE campaign_performance (
            id BIGSERIAL PRIMARY KEY,
            campaign_id INTEGER REFERENCES campaigns(id),
            total_sent BIGINT,
            total_opened BIGINT,
            total_clicked BIGINT,
            total_purchased BIGINT,
            total_unsubscribed BIGINT,
            open_rate DECIMAL(5,4),
            click_rate DECIMAL(5,4),
            purchase_rate DECIMAL(5,4),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Message events table
    cursor.execute("""
        CREATE TABLE message_events (
            id BIGSERIAL PRIMARY KEY,
            message_id VARCHAR(255) UNIQUE,
            campaign_id INTEGER REFERENCES campaigns(id),
            message_type VARCHAR(50),
            client_id BIGINT REFERENCES users(client_id),
            channel VARCHAR(50),
            category VARCHAR(255),
            platform VARCHAR(100),
            email_provider VARCHAR(100),
            stream VARCHAR(50),
            date DATE,
            sent_at TIMESTAMP,
            is_opened BOOLEAN DEFAULT FALSE,
            opened_first_time_at TIMESTAMP,
            opened_last_time_at TIMESTAMP,
            is_clicked BOOLEAN DEFAULT FALSE,
            clicked_first_time_at TIMESTAMP,
            clicked_last_time_at TIMESTAMP,
            is_unsubscribed BOOLEAN DEFAULT FALSE,
            unsubscribed_at TIMESTAMP,
            is_hard_bounced BOOLEAN DEFAULT FALSE,
            hard_bounced_at TIMESTAMP,
            is_soft_bounced BOOLEAN DEFAULT FALSE,
            soft_bounced_at TIMESTAMP,
            is_complained BOOLEAN DEFAULT FALSE,
            complained_at TIMESTAMP,
            is_blocked BOOLEAN DEFAULT FALSE,
            blocked_at TIMESTAMP,
            is_purchased BOOLEAN DEFAULT FALSE,
            purchased_at TIMESTAMP,
            user_device_id INTEGER,
            user_id BIGINT REFERENCES users(user_id),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Create indexes for performance
    indexes = [
        "CREATE INDEX idx_user_events_user_id ON user_events(user_id)",
        "CREATE INDEX idx_user_events_product_id ON user_events(product_id)",
        "CREATE INDEX idx_user_events_event_time ON user_events(event_time)",
        "CREATE INDEX idx_user_events_event_type ON user_events(event_type)",
        "CREATE INDEX idx_message_events_campaign_id ON message_events(campaign_id)",
        "CREATE INDEX idx_message_events_client_id ON message_events(client_id)",
        "CREATE INDEX idx_message_events_sent_at ON message_events(sent_at)",
        "CREATE INDEX idx_campaigns_started_at ON campaigns(started_at)",
        "CREATE INDEX idx_users_client_id ON users(client_id)",
        "CREATE INDEX idx_products_category_id ON products(category_id)",
        "CREATE INDEX idx_user_friends_user_id ON user_friends(user_id)",
        "CREATE INDEX idx_user_friends_friend_id ON user_friends(friend_id)"
    ]
    
    for index_sql in indexes:
        cursor.execute(index_sql)
    
    conn.commit()
    print("✅ PostgreSQL tables created successfully")

def load_categories(conn):
    """Load categories data"""
    print("📥 Loading categories...")
    
    cursor = conn.cursor()
    
    # Extract unique categories from events data
    events_df = pd.read_csv('data/processed/events_cleaned.csv')
    categories_df = events_df[['category_id', 'category_code']].drop_duplicates()
    
    # Extract category names from category_code
    categories_df['category_name'] = categories_df['category_code'].str.split('.').str[-1]
    categories_df['category_name'] = categories_df['category_name'].fillna('')
    
    # Insert data
    for _, row in categories_df.iterrows():
        cursor.execute("""
            INSERT INTO categories (category_id, category_code, category_name)
            VALUES (%s, %s, %s)
            ON CONFLICT (category_id) DO UPDATE SET
                category_code = EXCLUDED.category_code,
                category_name = EXCLUDED.category_name,
                updated_at = CURRENT_TIMESTAMP
        """, (row['category_id'], row['category_code'], row['category_name']))
    
    conn.commit()
    print(f"✅ Loaded {len(categories_df)} categories")

def load_products(conn):
    """Load products data"""
    print("📥 Loading products...")
    
    cursor = conn.cursor()
    
    events_df = pd.read_csv('data/processed/events_cleaned.csv')
    products_df = events_df[['product_id', 'category_id', 'brand', 'price']].drop_duplicates()
    
    for _, row in products_df.iterrows():
        cursor.execute("""
            INSERT INTO products (product_id, category_id, brand, price)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (product_id) DO UPDATE SET
                category_id = EXCLUDED.category_id,
                brand = EXCLUDED.brand,
                price = EXCLUDED.price,
                updated_at = CURRENT_TIMESTAMP
        """, (row['product_id'], row['category_id'], row['brand'], row['price']))
    
    conn.commit()
    print(f"✅ Loaded {len(products_df)} products")

def load_users(conn):
    """Load users data"""
    print("📥 Loading users...")
    
    cursor = conn.cursor()
    
    # Load from client purchase data
    purchase_df = pd.read_csv('data/processed/client_purchase_cleaned.csv')
    
    for _, row in purchase_df.iterrows():
        cursor.execute("""
            INSERT INTO users (user_id, client_id, user_device_id, first_purchase_date)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE SET
                client_id = EXCLUDED.client_id,
                user_device_id = EXCLUDED.user_device_id,
                first_purchase_date = EXCLUDED.first_purchase_date,
                updated_at = CURRENT_TIMESTAMP
        """, (row['user_id'], row['client_id'], row['user_device_id'], row['first_purchase_date']))
    
    conn.commit()
    print(f"✅ Loaded {len(purchase_df)} users")

def load_campaigns(conn):
    """Load campaigns data"""
    print("📥 Loading campaigns...")
    
    cursor = conn.cursor()
    
    campaigns_df = pd.read_csv('data/processed/campaigns_cleaned.csv')
    
    for _, row in campaigns_df.iterrows():
        cursor.execute("""
            INSERT INTO campaigns (
                id, campaign_type, channel, topic, started_at, finished_at,
                total_count, ab_test, warmup_mode, hour_limit, subject_length,
                subject_with_personalization, subject_with_deadline, subject_with_emoji,
                subject_with_bonuses, subject_with_discount, subject_with_saleout,
                is_test, position
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                campaign_type = EXCLUDED.campaign_type,
                channel = EXCLUDED.channel,
                topic = EXCLUDED.topic,
                started_at = EXCLUDED.started_at,
                finished_at = EXCLUDED.finished_at,
                total_count = EXCLUDED.total_count,
                ab_test = EXCLUDED.ab_test,
                warmup_mode = EXCLUDED.warmup_mode,
                hour_limit = EXCLUDED.hour_limit,
                subject_length = EXCLUDED.subject_length,
                subject_with_personalization = EXCLUDED.subject_with_personalization,
                subject_with_deadline = EXCLUDED.subject_with_deadline,
                subject_with_emoji = EXCLUDED.subject_with_emoji,
                subject_with_bonuses = EXCLUDED.subject_with_bonuses,
                subject_with_discount = EXCLUDED.subject_with_discount,
                subject_with_saleout = EXCLUDED.subject_with_saleout,
                is_test = EXCLUDED.is_test,
                position = EXCLUDED.position,
                updated_at = CURRENT_TIMESTAMP
        """, tuple(row))
    
    conn.commit()
    print(f"✅ Loaded {len(campaigns_df)} campaigns")

def load_user_events(conn):
    """Load user events data"""
    print("📥 Loading user events...")
    
    cursor = conn.cursor()
    
    events_df = pd.read_csv('data/processed/events_cleaned.csv')
    
    # Insert in batches for better performance
    batch_size = 1000
    for i in range(0, len(events_df), batch_size):
        batch = events_df.iloc[i:i+batch_size]
        
        for _, row in batch.iterrows():
            cursor.execute("""
                INSERT INTO user_events (
                    event_time, event_type, product_id, category_id,
                    user_id, user_session, price
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (row['event_time'], row['event_type'], row['product_id'],
                  row['category_id'], row['user_id'], row['user_session'], row['price']))
        
        conn.commit()
        if i % 10000 == 0:
            print(f"  Processed {i:,} events...")
    
    conn.commit()
    print(f"✅ Loaded {len(events_df)} user events")

def load_friends(conn):
    """Load user friends data"""
    print("📥 Loading user friends...")
    
    cursor = conn.cursor()
    
    friends_df = pd.read_csv('data/processed/friends_cleaned.csv')
    
    for _, row in friends_df.iterrows():
        cursor.execute("""
            INSERT INTO user_friends (user_id, friend_id)
            VALUES (%s, %s)
            ON CONFLICT (user_id, friend_id) DO NOTHING
        """, (row['friend1'], row['friend2']))
    
    conn.commit()
    print(f"✅ Loaded {len(friends_df)} friend relationships")

def load_messages(conn):
    """Load message events data"""
    print("📥 Loading message events...")
    
    cursor = conn.cursor()
    
    messages_df = pd.read_csv('data/processed/messages_cleaned.csv')
    
    # Insert in batches for better performance
    batch_size = 1000
    for i in range(0, len(messages_df), batch_size):
        batch = messages_df.iloc[i:i+batch_size]
        
        for _, row in batch.iterrows():
            cursor.execute("""
                INSERT INTO message_events (
                    message_id, campaign_id, message_type, client_id, channel,
                    category, platform, email_provider, stream, date, sent_at,
                    is_opened, opened_first_time_at, opened_last_time_at,
                    is_clicked, clicked_first_time_at, clicked_last_time_at,
                    is_unsubscribed, unsubscribed_at, is_hard_bounced, hard_bounced_at,
                    is_soft_bounced, soft_bounced_at, is_complained, complained_at,
                    is_blocked, blocked_at, is_purchased, purchased_at,
                    user_device_id, user_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (message_id) DO UPDATE SET
                    campaign_id = EXCLUDED.campaign_id,
                    message_type = EXCLUDED.message_type,
                    client_id = EXCLUDED.client_id,
                    channel = EXCLUDED.channel,
                    category = EXCLUDED.category,
                    platform = EXCLUDED.platform,
                    email_provider = EXCLUDED.email_provider,
                    stream = EXCLUDED.stream,
                    date = EXCLUDED.date,
                    sent_at = EXCLUDED.sent_at,
                    is_opened = EXCLUDED.is_opened,
                    opened_first_time_at = EXCLUDED.opened_first_time_at,
                    opened_last_time_at = EXCLUDED.opened_last_time_at,
                    is_clicked = EXCLUDED.is_clicked,
                    clicked_first_time_at = EXCLUDED.clicked_first_time_at,
                    clicked_last_time_at = EXCLUDED.clicked_last_time_at,
                    is_unsubscribed = EXCLUDED.is_unsubscribed,
                    unsubscribed_at = EXCLUDED.unsubscribed_at,
                    is_hard_bounced = EXCLUDED.is_hard_bounced,
                    hard_bounced_at = EXCLUDED.hard_bounced_at,
                    is_soft_bounced = EXCLUDED.is_soft_bounced,
                    soft_bounced_at = EXCLUDED.soft_bounced_at,
                    is_complained = EXCLUDED.is_complained,
                    complained_at = EXCLUDED.complained_at,
                    is_blocked = EXCLUDED.is_blocked,
                    blocked_at = EXCLUDED.blocked_at,
                    is_purchased = EXCLUDED.is_purchased,
                    purchased_at = EXCLUDED.purchased_at,
                    user_device_id = EXCLUDED.user_device_id,
                    user_id = EXCLUDED.user_id,
                    updated_at = CURRENT_TIMESTAMP
            """, tuple(row))
        
        conn.commit()
        if i % 10000 == 0:
            print(f"  Processed {i:,} messages...")
    
    conn.commit()
    print(f"✅ Loaded {len(messages_df)} message events")

def main():
    """Main function to create tables and load data"""
    print("🚀 Starting PostgreSQL data loading...")
    
    try:
        # Connect to database
        conn = get_db_connection()
        print("✅ Connected to PostgreSQL database")
        
        # Create tables
        create_tables(conn)
        
        # Load data in order of dependencies
        load_categories(conn)
        load_products(conn)
        load_users(conn)
        load_campaigns(conn)
        load_user_events(conn)
        load_friends(conn)
        load_messages(conn)
        
        conn.close()
        
        print("\n🎉 PostgreSQL data loading completed successfully!")
        print("📊 All data loaded into optimized relational schema")
        
    except Exception as e:
        print(f"❌ Error during PostgreSQL data loading: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
