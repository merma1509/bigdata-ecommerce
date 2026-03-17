#!/usr/bin/env python3
"""
MongoDB Data Model Implementation and Data Loading
Creates optimized document schema and loads cleaned data
"""

import pymongo
import pandas as pd
import os
from datetime import datetime
import sys
from bson import ObjectId

# Database connection parameters
DB_PARAMS = {
    'host': 'localhost',
    'port': 27017,
    'database': 'ecommerce',
    'username': 'admin',
    'password': None  # Will be loaded from environment
}

def get_db_connection():
    """Get MongoDB database connection"""
    # Load password from environment
    DB_PARAMS['password'] = os.getenv('MONGO_ROOT_PASSWORD')
    
    if not DB_PARAMS['password']:
        raise ValueError("MONGO_ROOT_PASSWORD environment variable not set")
    
    client = pymongo.MongoClient(
        host=DB_PARAMS['host'],
        port=DB_PARAMS['port'],
        username=DB_PARAMS['username'],
        password=DB_PARAMS['password'],
        authSource="admin"
    )
    
    return client[DB_PARAMS['database']]

def create_collections_and_indexes(db):
    """Create MongoDB collections and indexes"""
    print("🏗️ Creating MongoDB collections and indexes...")
    
    # Drop existing collections
    db.categories.drop()
    db.products.drop()
    db.users.drop()
    db.campaigns.drop()
    db.events.drop()
    db.friends.drop()
    db.messages.drop()
    
    # Create collections with validation schemas
    db.create_collection('categories', validator={
        '$jsonSchema': {
            'bsonType': 'object',
            'required': ['category_id', 'category_code'],
            'properties': {
                'category_id': {'bsonType': 'long'},
                'category_code': {'bsonType': 'string'},
                'category_name': {'bsonType': 'string'},
                'created_at': {'bsonType': 'date'},
                'updated_at': {'bsonType': 'date'}
            }
        }
    })
    
    db.create_collection('products', validator={
        '$jsonSchema': {
            'bsonType': 'object',
            'required': ['product_id', 'category_id'],
            'properties': {
                'product_id': {'bsonType': 'long'},
                'category_id': {'bsonType': 'long'},
                'brand': {'bsonType': 'string'},
                'price': {'bsonType': 'decimal'},
                'created_at': {'bsonType': 'date'},
                'updated_at': {'bsonType': 'date'}
            }
        }
    })
    
    db.create_collection('users', validator={
        '$jsonSchema': {
            'bsonType': 'object',
            'required': ['user_id'],
            'properties': {
                'user_id': {'bsonType': 'long'},
                'client_id': {'bsonType': 'long'},
                'user_device_id': {'bsonType': 'int'},
                'first_purchase_date': {'bsonType': 'date'},
                'created_at': {'bsonType': 'date'},
                'updated_at': {'bsonType': 'date'},
                'friends': {'bsonType': 'array'},
                'events': {'bsonType': 'array'},
                'messages': {'bsonType': 'array'}
            }
        }
    })
    
    db.create_collection('campaigns', validator={
        '$jsonSchema': {
            'bsonType': 'object',
            'required': ['id', 'campaign_type', 'channel'],
            'properties': {
                'id': {'bsonType': 'int'},
                'campaign_type': {'bsonType': 'string'},
                'channel': {'bsonType': 'string'},
                'topic': {'bsonType': 'string'},
                'started_at': {'bsonType': 'date'},
                'finished_at': {'bsonType': 'date'},
                'total_count': {'bsonType': 'long'},
                'ab_test': {'bsonType': 'string'},
                'warmup_mode': {'bsonType': 'string'},
                'hour_limit': {'bsonType': 'decimal'},
                'subject_length': {'bsonType': 'int'},
                'subject_features': {
                    'bsonType': 'object',
                    'properties': {
                        'personalization': {'bsonType': 'bool'},
                        'deadline': {'bsonType': 'bool'},
                        'emoji': {'bsonType': 'bool'},
                        'bonuses': {'bsonType': 'bool'},
                        'discount': {'bsonType': 'bool'},
                        'saleout': {'bsonType': 'bool'}
                    }
                },
                'is_test': {'bsonType': 'bool'},
                'position': {'bsonType': 'int'},
                'created_at': {'bsonType': 'date'},
                'updated_at': {'bsonType': 'date'},
                'performance': {
                    'bsonType': 'object',
                    'properties': {
                        'total_sent': {'bsonType': 'long'},
                        'total_opened': {'bsonType': 'long'},
                        'total_clicked': {'bsonType': 'long'},
                        'total_purchased': {'bsonType': 'long'},
                        'open_rate': {'bsonType': 'decimal'},
                        'click_rate': {'bsonType': 'decimal'},
                        'purchase_rate': {'bsonType': 'decimal'}
                    }
                }
            }
        }
    })
    
    db.create_collection('events', validator={
        '$jsonSchema': {
            'bsonType': 'object',
            'required': ['event_time', 'event_type', 'user_id'],
            'properties': {
                'event_time': {'bsonType': 'date'},
                'event_type': {'bsonType': 'string'},
                'product_id': {'bsonType': 'long'},
                'category_id': {'bsonType': 'long'},
                'user_id': {'bsonType': 'long'},
                'user_session': {'bsonType': 'string'},
                'price': {'bsonType': 'decimal'},
                'created_at': {'bsonType': 'date'}
            }
        }
    })
    
    db.create_collection('friends', validator={
        '$jsonSchema': {
            'bsonType': 'object',
            'required': ['user_id', 'friend_id'],
            'properties': {
                'user_id': {'bsonType': 'long'},
                'friend_id': {'bsonType': 'long'},
                'created_at': {'bsonType': 'date'}
            }
        }
    })
    
    db.create_collection('messages', validator={
        '$jsonSchema': {
            'bsonType': 'object',
            'required': ['message_id', 'campaign_id', 'client_id'],
            'properties': {
                'message_id': {'bsonType': 'string'},
                'campaign_id': {'bsonType': 'int'},
                'message_type': {'bsonType': 'string'},
                'client_id': {'bsonType': 'long'},
                'channel': {'bsonType': 'string'},
                'category': {'bsonType': 'string'},
                'platform': {'bsonType': 'string'},
                'email_provider': {'bsonType': 'string'},
                'stream': {'bsonType': 'string'},
                'date': {'bsonType': 'date'},
                'sent_at': {'bsonType': 'date'},
                'engagement': {
                    'bsonType': 'object',
                    'properties': {
                        'opened': {'bsonType': 'bool'},
                        'opened_first_time_at': {'bsonType': 'date'},
                        'opened_last_time_at': {'bsonType': 'date'},
                        'clicked': {'bsonType': 'bool'},
                        'clicked_first_time_at': {'bsonType': 'date'},
                        'clicked_last_time_at': {'bsonType': 'date'}
                    }
                },
                'status': {
                    'bsonType': 'object',
                    'properties': {
                        'unsubscribed': {'bsonType': 'bool'},
                        'unsubscribed_at': {'bsonType': 'date'},
                        'hard_bounced': {'bsonType': 'bool'},
                        'hard_bounced_at': {'bsonType': 'date'},
                        'soft_bounced': {'bsonType': 'bool'},
                        'soft_bounced_at': {'bsonType': 'date'},
                        'complained': {'bsonType': 'bool'},
                        'complained_at': {'bsonType': 'date'},
                        'blocked': {'bsonType': 'bool'},
                        'blocked_at': {'bsonType': 'date'}
                    }
                },
                'conversion': {
                    'bsonType': 'object',
                    'properties': {
                        'purchased': {'bsonType': 'bool'},
                        'purchased_at': {'bsonType': 'date'}
                    }
                },
                'user_device_id': {'bsonType': 'int'},
                'user_id': {'bsonType': 'long'},
                'created_at': {'bsonType': 'date'},
                'updated_at': {'bsonType': 'date'}
            }
        }
    })
    
    # Create indexes for performance
    indexes = [
        # Categories indexes
        ("categories", "category_id", 1),
        ("categories", "category_code", 1),
        
        # Products indexes
        ("products", "product_id", 1),
        ("products", "category_id", 1),
        ("products", "brand", 1),
        ("products", "price", 1),
        
        # Users indexes
        ("users", "user_id", 1),
        ("users", "client_id", 1),
        
        # Campaigns indexes
        ("campaigns", "id", 1),
        ("campaigns", "campaign_type", 1),
        ("campaigns", "channel", 1),
        ("campaigns", "started_at", 1),
        
        # Events indexes
        ("events", "user_id", 1),
        ("events", "product_id", 1),
        ("events", "category_id", 1),
        ("events", "event_time", 1),
        ("events", "event_type", 1),
        ("events", "user_session", 1),
        
        # Friends indexes
        ("friends", "user_id", 1),
        ("friends", "friend_id", 1),
        
        # Messages indexes
        ("messages", "message_id", 1),
        ("messages", "campaign_id", 1),
        ("messages", "client_id", 1),
        ("messages", "sent_at", 1),
        ("messages", "channel", 1),
        ("messages", "engagement.opened", 1),
        ("messages", "engagement.clicked", 1),
        ("messages", "conversion.purchased", 1)
    ]
    
    for collection, field, direction in indexes:
        db[collection].create_index([(field, direction)])
    
    print("✅ MongoDB collections and indexes created successfully")

def load_categories(db):
    """Load categories data"""
    print("📥 Loading categories...")
    
    events_df = pd.read_csv('data/processed/events_cleaned.csv')
    categories_df = events_df[['category_id', 'category_code']].drop_duplicates()
    
    # Extract category names from category_code
    categories_df['category_name'] = categories_df['category_code'].str.split('.').str[-1]
    categories_df['category_name'] = categories_df['category_name'].fillna('')
    
    # Convert to list of documents
    categories = []
    for _, row in categories_df.iterrows():
        categories.append({
            'category_id': int(row['category_id']),
            'category_code': row['category_code'],
            'category_name': row['category_name'],
            'created_at': datetime.utcnow(),
            'updated_at': datetime.utcnow()
        })
    
    # Insert in bulk
    if categories:
        db.categories.insert_many(categories)
    
    print(f"✅ Loaded {len(categories)} categories")

def load_products(db):
    """Load products data"""
    print("📥 Loading products...")
    
    events_df = pd.read_csv('data/processed/events_cleaned.csv')
    products_df = events_df[['product_id', 'category_id', 'brand', 'price']].drop_duplicates()
    
    # Convert to list of documents
    products = []
    for _, row in products_df.iterrows():
        products.append({
            'product_id': int(row['product_id']),
            'category_id': int(row['category_id']),
            'brand': row['brand'],
            'price': float(row['price']),
            'created_at': datetime.utcnow(),
            'updated_at': datetime.utcnow()
        })
    
    # Insert in bulk
    if products:
        db.products.insert_many(products)
    
    print(f"✅ Loaded {len(products)} products")

def load_users(db):
    """Load users data"""
    print("📥 Loading users...")
    
    purchase_df = pd.read_csv('data/processed/client_purchase_cleaned.csv')
    
    # Convert to list of documents
    users = []
    for _, row in purchase_df.iterrows():
        users.append({
            'user_id': int(row['user_id']),
            'client_id': int(row['client_id']),
            'user_device_id': int(row['user_device_id']),
            'first_purchase_date': pd.to_datetime(row['first_purchase_date']).to_pydatetime(),
            'created_at': datetime.utcnow(),
            'updated_at': datetime.utcnow(),
            'friends': [],
            'events': [],
            'messages': []
        })
    
    # Insert in bulk
    if users:
        db.users.insert_many(users)
    
    print(f"✅ Loaded {len(users)} users")

def load_campaigns(db):
    """Load campaigns data"""
    print("📥 Loading campaigns...")
    
    campaigns_df = pd.read_csv('data/processed/campaigns_cleaned.csv')
    
    # Convert to list of documents
    campaigns = []
    for _, row in campaigns_df.iterrows():
        campaigns.append({
            'id': int(row['id']),
            'campaign_type': row['campaign_type'],
            'channel': row['channel'],
            'topic': row['topic'],
            'started_at': pd.to_datetime(row['started_at']).to_pydatetime(),
            'finished_at': pd.to_datetime(row['finished_at']).to_pydatetime(),
            'total_count': int(row['total_count']),
            'ab_test': row['ab_test'],
            'warmup_mode': row['warmup_mode'],
            'hour_limit': float(row['hour_limit']),
            'subject_length': int(row['subject_length']),
            'subject_features': {
                'personalization': bool(row['subject_with_personalization']),
                'deadline': bool(row['subject_with_deadline']),
                'emoji': bool(row['subject_with_emoji']),
                'bonuses': bool(row['subject_with_bonuses']),
                'discount': bool(row['subject_with_discount']),
                'saleout': bool(row['subject_with_saleout'])
            },
            'is_test': bool(row['is_test']),
            'position': int(row['position']) if pd.notna(row['position']) else None,
            'created_at': datetime.utcnow(),
            'updated_at': datetime.utcnow(),
            'performance': {}
        })
    
    # Insert in bulk
    if campaigns:
        db.campaigns.insert_many(campaigns)
    
    print(f"✅ Loaded {len(campaigns)} campaigns")

def load_events(db):
    """Load events data"""
    print("📥 Loading events...")
    
    events_df = pd.read_csv('data/processed/events_cleaned.csv')
    
    # Convert to list of documents
    events = []
    for _, row in events_df.iterrows():
        events.append({
            'event_time': pd.to_datetime(row['event_time']).to_pydatetime(),
            'event_type': row['event_type'],
            'product_id': int(row['product_id']),
            'category_id': int(row['category_id']),
            'user_id': int(row['user_id']),
            'user_session': row['user_session'],
            'price': float(row['price']),
            'created_at': datetime.utcnow()
        })
    
    # Insert in batches for better performance
    batch_size = 1000
    for i in range(0, len(events), batch_size):
        batch = events[i:i+batch_size]
        db.events.insert_many(batch)
        
        if i % 10000 == 0:
            print(f"  Processed {i:,} events...")
    
    print(f"✅ Loaded {len(events)} events")

def load_friends(db):
    """Load friends data"""
    print("📥 Loading friends...")
    
    friends_df = pd.read_csv('data/processed/friends_cleaned.csv')
    
    # Convert to list of documents
    friends = []
    for _, row in friends_df.iterrows():
        friends.append({
            'user_id': int(row['friend1']),
            'friend_id': int(row['friend2']),
            'created_at': datetime.utcnow()
        })
    
    # Insert in bulk
    if friends:
        db.friends.insert_many(friends)
    
    print(f"✅ Loaded {len(friends)} friend relationships")

def load_messages(db):
    """Load messages data"""
    print("📥 Loading messages...")
    
    messages_df = pd.read_csv('data/processed/messages_cleaned.csv')
    
    # Convert to list of documents
    messages = []
    for _, row in messages_df.iterrows():
        messages.append({
            'message_id': row['message_id'],
            'campaign_id': int(row['campaign_id']),
            'message_type': row['message_type'],
            'client_id': int(row['client_id']),
            'channel': row['channel'],
            'category': row['category'],
            'platform': row['platform'],
            'email_provider': row['email_provider'],
            'stream': row['stream'],
            'date': pd.to_datetime(row['date']).to_pydatetime(),
            'sent_at': pd.to_datetime(row['sent_at']).to_pydatetime(),
            'engagement': {
                'opened': bool(row['is_opened']),
                'opened_first_time_at': pd.to_datetime(row['opened_first_time_at']).to_pydatetime() if pd.notna(row['opened_first_time_at']) else None,
                'opened_last_time_at': pd.to_datetime(row['opened_last_time_at']).to_pydatetime() if pd.notna(row['opened_last_time_at']) else None,
                'clicked': bool(row['is_clicked']),
                'clicked_first_time_at': pd.to_datetime(row['clicked_first_time_at']).to_pydatetime() if pd.notna(row['clicked_first_time_at']) else None,
                'clicked_last_time_at': pd.to_datetime(row['clicked_last_time_at']).to_pydatetime() if pd.notna(row['clicked_last_time_at']) else None
            },
            'status': {
                'unsubscribed': bool(row['is_unsubscribed']),
                'unsubscribed_at': pd.to_datetime(row['unsubscribed_at']).to_pydatetime() if pd.notna(row['unsubscribed_at']) else None,
                'hard_bounced': bool(row['is_hard_bounced']),
                'hard_bounced_at': pd.to_datetime(row['hard_bounced_at']).to_pydatetime() if pd.notna(row['hard_bounced_at']) else None,
                'soft_bounced': bool(row['is_soft_bounced']),
                'soft_bounced_at': pd.to_datetime(row['soft_bounced_at']).to_pydatetime() if pd.notna(row['soft_bounced_at']) else None,
                'complained': bool(row['is_complained']),
                'complained_at': pd.to_datetime(row['complained_at']).to_pydatetime() if pd.notna(row['complained_at']) else None,
                'blocked': bool(row['is_blocked']),
                'blocked_at': pd.to_datetime(row['blocked_at']).to_pydatetime() if pd.notna(row['blocked_at']) else None
            },
            'conversion': {
                'purchased': bool(row['is_purchased']),
                'purchased_at': pd.to_datetime(row['purchased_at']).to_pydatetime() if pd.notna(row['purchased_at']) else None
            },
            'user_device_id': int(row['user_device_id']),
            'user_id': int(row['user_id']),
            'created_at': datetime.utcnow(),
            'updated_at': datetime.utcnow()
        })
    
    # Insert in batches for better performance
    batch_size = 1000
    for i in range(0, len(messages), batch_size):
        batch = messages[i:i+batch_size]
        db.messages.insert_many(batch)
        
        if i % 10000 == 0:
            print(f"  Processed {i:,} messages...")
    
    print(f"✅ Loaded {len(messages)} messages")

def main():
    """Main function to create collections and load data"""
    print("🚀 Starting MongoDB data loading...")
    
    try:
        # Connect to database
        db = get_db_connection()
        print("✅ Connected to MongoDB database")
        
        # Create collections and indexes
        create_collections_and_indexes(db)
        
        # Load data in order of dependencies
        load_categories(db)
        load_products(db)
        load_users(db)
        load_campaigns(db)
        load_events(db)
        load_friends(db)
        load_messages(db)
        
        print("\n🎉 MongoDB data loading completed successfully!")
        print("📊 All data loaded into optimized document schema")
        
    except Exception as e:
        print(f"❌ Error during MongoDB data loading: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
