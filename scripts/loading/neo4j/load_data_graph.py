#!/usr/bin/env python3
"""
Neo4j Data Model Implementation and Data Loading
Creates optimized graph schema and loads cleaned data
"""

from neo4j import GraphDatabase
import pandas as pd
import os
from datetime import datetime
import sys

# Database connection parameters
DB_PARAMS = {
    'uri': 'bolt://localhost:7687',
    'user': 'neo4j',
    'password': None  # Will be loaded from environment
}

def get_db_connection():
    """Get Neo4j database connection"""
    # Load password from environment
    DB_PARAMS['password'] = os.getenv('NEO4J_PASSWORD')
    
    if not DB_PARAMS['password']:
        raise ValueError("NEO4J_PASSWORD environment variable not set")
    
    return GraphDatabase.driver(DB_PARAMS['uri'], auth=(DB_PARAMS['user'], DB_PARAMS['password']))

def create_constraints_and_indexes(driver):
    """Create Neo4j constraints and indexes"""
    print("🏗️ Creating Neo4j constraints and indexes...")
    
    with driver.session() as session:
        # Drop existing constraints and indexes
        constraints = [
            "DROP CONSTRAINT user_id_unique IF EXISTS",
            "DROP CONSTRAINT product_id_unique IF EXISTS", 
            "DROP CONSTRAINT category_id_unique IF EXISTS",
            "DROP CONSTRAINT campaign_id_unique IF EXISTS",
            "DROP CONSTRAINT message_id_unique IF EXISTS"
        ]
        
        for constraint in constraints:
            try:
                session.run(constraint)
            except:
                pass
        
        # Create uniqueness constraints
        constraints = [
            "CREATE CONSTRAINT user_id_unique IF NOT EXISTS FOR (u:User) REQUIRE u.user_id IS UNIQUE",
            "CREATE CONSTRAINT product_id_unique IF NOT EXISTS FOR (p:Product) REQUIRE p.product_id IS UNIQUE",
            "CREATE CONSTRAINT category_id_unique IF NOT EXISTS FOR (c:Category) REQUIRE c.category_id IS UNIQUE",
            "CREATE CONSTRAINT campaign_id_unique IF NOT EXISTS FOR (cam:Campaign) REQUIRE cam.campaign_id IS UNIQUE",
            "CREATE CONSTRAINT message_id_unique IF NOT EXISTS FOR (m:Message) REQUIRE m.message_id IS UNIQUE"
        ]
        
        for constraint in constraints:
            session.run(constraint)
        
        # Create indexes for performance
        indexes = [
            "CREATE INDEX user_client_id IF NOT EXISTS FOR (u:User) ON (u.client_id)",
            "CREATE INDEX product_category_id IF NOT EXISTS FOR (p:Product) ON (p.category_id)",
            "CREATE INDEX product_brand IF NOT EXISTS FOR (p:Product) ON (p.brand)",
            "CREATE INDEX product_price IF NOT EXISTS FOR (p:Product) ON (p.price)",
            "CREATE INDEX campaign_type IF NOT EXISTS FOR (cam:Campaign) ON (cam.campaign_type)",
            "CREATE INDEX campaign_channel IF NOT EXISTS FOR (cam:Campaign) ON (cam.channel)",
            "CREATE INDEX event_time IF NOT EXISTS FOR ()-[e:EVENT]->() ON (e.event_time)",
            "CREATE INDEX event_type IF NOT EXISTS FOR ()-[e:EVENT]->() ON (e.event_type)",
            "CREATE INDEX message_sent_at IF NOT EXISTS FOR (m:Message) ON (m.sent_at)",
            "CREATE INDEX message_channel IF NOT EXISTS FOR (m:Message) ON (m.channel)"
        ]
        
        for index in indexes:
            session.run(index)
    
    print("✅ Neo4j constraints and indexes created successfully")

def load_categories(driver):
    """Load category nodes"""
    print("📥 Loading categories...")
    
    events_df = pd.read_csv('data/processed/events_cleaned.csv')
    categories_df = events_df[['category_id', 'category_code']].drop_duplicates()
    
    # Extract category names from category_code
    categories_df['category_name'] = categories_df['category_code'].str.split('.').str[-1]
    categories_df['category_name'] = categories_df['category_name'].fillna('')
    
    with driver.session() as session:
        for _, row in categories_df.iterrows():
            session.run("""
                MERGE (c:Category {category_id: $category_id})
                SET c.category_code = $category_code,
                    c.category_name = $category_name,
                    c.created_at = datetime(),
                    c.updated_at = datetime()
            """, 
            category_id=int(row['category_id']),
            category_code=row['category_code'],
            category_name=row['category_name'])
    
    print(f"✅ Loaded {len(categories_df)} categories")

def load_products(driver):
    """Load product nodes and relationships"""
    print("📥 Loading products...")
    
    events_df = pd.read_csv('data/processed/events_cleaned.csv')
    products_df = events_df[['product_id', 'category_id', 'brand', 'price']].drop_duplicates()
    
    with driver.session() as session:
        for _, row in products_df.iterrows():
            # Create product node
            session.run("""
                MERGE (p:Product {product_id: $product_id})
                SET p.brand = $brand,
                    p.price = $price,
                    p.created_at = datetime(),
                    p.updated_at = datetime()
            """, 
            product_id=int(row['product_id']),
            brand=row['brand'],
            price=float(row['price']))
            
            # Create relationship with category
            session.run("""
                MATCH (p:Product {product_id: $product_id})
                MATCH (c:Category {category_id: $category_id})
                MERGE (p)-[:BELONGS_TO]->(c)
            """, 
            product_id=int(row['product_id']),
            category_id=int(row['category_id']))
    
    print(f"✅ Loaded {len(products_df)} products")

def load_users(driver):
    """Load user nodes"""
    print("📥 Loading users...")
    
    purchase_df = pd.read_csv('data/processed/client_purchase_cleaned.csv')
    
    with driver.session() as session:
        for _, row in purchase_df.iterrows():
            session.run("""
                MERGE (u:User {user_id: $user_id})
                SET u.client_id = $client_id,
                    u.user_device_id = $user_device_id,
                    u.first_purchase_date = date($first_purchase_date),
                    u.created_at = datetime(),
                    u.updated_at = datetime()
            """, 
            user_id=int(row['user_id']),
            client_id=int(row['client_id']),
            user_device_id=int(row['user_device_id']),
            first_purchase_date=row['first_purchase_date'])
    
    print(f"✅ Loaded {len(purchase_df)} users")

def load_campaigns(driver):
    """Load campaign nodes"""
    print("📥 Loading campaigns...")
    
    campaigns_df = pd.read_csv('data/processed/campaigns_cleaned.csv')
    
    with driver.session() as session:
        for _, row in campaigns_df.iterrows():
            session.run("""
                MERGE (cam:Campaign {campaign_id: $campaign_id})
                SET cam.campaign_type = $campaign_type,
                    cam.channel = $channel,
                    cam.topic = $topic,
                    cam.started_at = datetime($started_at),
                    cam.finished_at = datetime($finished_at),
                    cam.total_count = $total_count,
                    cam.ab_test = $ab_test,
                    cam.warmup_mode = $warmup_mode,
                    cam.hour_limit = $hour_limit,
                    cam.subject_length = $subject_length,
                    cam.subject_with_personalization = $subject_with_personalization,
                    cam.subject_with_deadline = $subject_with_deadline,
                    cam.subject_with_emoji = $subject_with_emoji,
                    cam.subject_with_bonuses = $subject_with_bonuses,
                    cam.subject_with_discount = $subject_with_discount,
                    cam.subject_with_saleout = $subject_with_saleout,
                    cam.is_test = $is_test,
                    cam.position = $position,
                    cam.created_at = datetime(),
                    cam.updated_at = datetime()
            """, 
            campaign_id=int(row['id']),
            campaign_type=row['campaign_type'],
            channel=row['channel'],
            topic=row['topic'],
            started_at=row['started_at'],
            finished_at=row['finished_at'],
            total_count=int(row['total_count']),
            ab_test=row['ab_test'],
            warmup_mode=row['warmup_mode'],
            hour_limit=float(row['hour_limit']),
            subject_length=int(row['subject_length']),
            subject_with_personalization=bool(row['subject_with_personalization']),
            subject_with_deadline=bool(row['subject_with_deadline']),
            subject_with_emoji=bool(row['subject_with_emoji']),
            subject_with_bonuses=bool(row['subject_with_bonuses']),
            subject_with_discount=bool(row['subject_with_discount']),
            subject_with_saleout=bool(row['subject_with_saleout']),
            is_test=bool(row['is_test']),
            position=int(row['position']) if pd.notna(row['position']) else None)
    
    print(f"✅ Loaded {len(campaigns_df)} campaigns")

def load_events(driver):
    """Load event relationships"""
    print("📥 Loading events...")
    
    events_df = pd.read_csv('data/processed/events_cleaned.csv')
    
    with driver.session() as session:
        batch_size = 1000
        for i in range(0, len(events_df), batch_size):
            batch = events_df.iloc[i:i+batch_size]
            
            for _, row in batch.iterrows():
                # Create event relationship between user and product
                session.run("""
                    MATCH (u:User {user_id: $user_id})
                    MATCH (p:Product {product_id: $product_id})
                    MERGE (u)-[e:EVENT]->(p)
                    SET e.event_time = datetime($event_time),
                        e.event_type = $event_type,
                        e.user_session = $user_session,
                        e.price = $price,
                        e.created_at = datetime()
                """, 
                user_id=int(row['user_id']),
                product_id=int(row['product_id']),
                event_time=row['event_time'],
                event_type=row['event_type'],
                user_session=row['user_session'],
                price=float(row['price']))
            
            if i % 10000 == 0:
                print(f"  Processed {i:,} events...")
    
    print(f"✅ Loaded {len(events_df)} event relationships")

def load_friends(driver):
    """Load friendship relationships"""
    print("📥 Loading friendships...")
    
    friends_df = pd.read_csv('data/processed/friends_cleaned.csv')
    
    with driver.session() as session:
        for _, row in friends_df.iterrows():
            # Create bidirectional friendship relationship
            session.run("""
                MATCH (u1:User {user_id: $user_id1})
                MATCH (u2:User {user_id: $user_id2})
                MERGE (u1)-[:FRIENDS_WITH]->(u2)
                SET relationship.created_at = datetime()
            """, 
            user_id1=int(row['friend1']),
            user_id2=int(row['friend2']))
    
    print(f"✅ Loaded {len(friends_df)} friendship relationships")

def load_messages(driver):
    """Load message nodes and relationships"""
    print("📥 Loading messages...")
    
    messages_df = pd.read_csv('data/processed/messages_cleaned.csv')
    
    with driver.session() as session:
        batch_size = 1000
        for i in range(0, len(messages_df), batch_size):
            batch = messages_df.iloc[i:i+batch_size]
            
            for _, row in batch.iterrows():
                # Create message node
                session.run("""
                    MERGE (m:Message {message_id: $message_id})
                    SET m.message_type = $message_type,
                        m.channel = $channel,
                        m.category = $category,
                        m.platform = $platform,
                        m.email_provider = $email_provider,
                        m.stream = $stream,
                        m.date = date($date),
                        m.sent_at = datetime($sent_at),
                        m.is_opened = $is_opened,
                        m.opened_first_time_at = CASE WHEN $opened_first_time_at IS NOT NULL THEN datetime($opened_first_time_at) ELSE NULL END,
                        m.opened_last_time_at = CASE WHEN $opened_last_time_at IS NOT NULL THEN datetime($opened_last_time_at) ELSE NULL END,
                        m.is_clicked = $is_clicked,
                        m.clicked_first_time_at = CASE WHEN $clicked_first_time_at IS NOT NULL THEN datetime($clicked_first_time_at) ELSE NULL END,
                        m.clicked_last_time_at = CASE WHEN $clicked_last_time_at IS NOT NULL THEN datetime($clicked_last_time_at) ELSE NULL END,
                        m.is_unsubscribed = $is_unsubscribed,
                        m.unsubscribed_at = CASE WHEN $unsubscribed_at IS NOT NULL THEN datetime($unsubscribed_at) ELSE NULL END,
                        m.is_hard_bounced = $is_hard_bounced,
                        m.hard_bounced_at = CASE WHEN $hard_bounced_at IS NOT NULL THEN datetime($hard_bounced_at) ELSE NULL END,
                        m.is_soft_bounced = $is_soft_bounced,
                        m.soft_bounced_at = CASE WHEN $soft_bounced_at IS NOT NULL THEN datetime($soft_bounced_at) ELSE NULL END,
                        m.is_complained = $is_complained,
                        m.complained_at = CASE WHEN $complained_at IS NOT NULL THEN datetime($complained_at) ELSE NULL END,
                        m.is_blocked = $is_blocked,
                        m.blocked_at = CASE WHEN $blocked_at IS NOT NULL THEN datetime($blocked_at) ELSE NULL END,
                        m.is_purchased = $is_purchased,
                        m.purchased_at = CASE WHEN $purchased_at IS NOT NULL THEN datetime($purchased_at) ELSE NULL END,
                        m.user_device_id = $user_device_id,
                        m.created_at = datetime(),
                        m.updated_at = datetime()
                """, 
                message_id=row['message_id'],
                message_type=row['message_type'],
                channel=row['channel'],
                category=row['category'],
                platform=row['platform'],
                email_provider=row['email_provider'],
                stream=row['stream'],
                date=row['date'],
                sent_at=row['sent_at'],
                is_opened=bool(row['is_opened']),
                opened_first_time_at=row['opened_first_time_at'] if pd.notna(row['opened_first_time_at']) else None,
                opened_last_time_at=row['opened_last_time_at'] if pd.notna(row['opened_last_time_at']) else None,
                is_clicked=bool(row['is_clicked']),
                clicked_first_time_at=row['clicked_first_time_at'] if pd.notna(row['clicked_first_time_at']) else None,
                clicked_last_time_at=row['clicked_last_time_at'] if pd.notna(row['clicked_last_time_at']) else None,
                is_unsubscribed=bool(row['is_unsubscribed']),
                unsubscribed_at=row['unsubscribed_at'] if pd.notna(row['unsubscribed_at']) else None,
                is_hard_bounced=bool(row['is_hard_bounced']),
                hard_bounced_at=row['hard_bounced_at'] if pd.notna(row['hard_bounced_at']) else None,
                is_soft_bounced=bool(row['is_soft_bounced']),
                soft_bounced_at=row['soft_bounced_at'] if pd.notna(row['soft_bounced_at']) else None,
                is_complained=bool(row['is_complained']),
                complained_at=row['complained_at'] if pd.notna(row['complained_at']) else None,
                is_blocked=bool(row['is_blocked']),
                blocked_at=row['blocked_at'] if pd.notna(row['blocked_at']) else None,
                is_purchased=bool(row['is_purchased']),
                purchased_at=row['purchased_at'] if pd.notna(row['purchased_at']) else None,
                user_device_id=int(row['user_device_id']))
                
                # Create relationship with campaign
                session.run("""
                    MATCH (m:Message {message_id: $message_id})
                    MATCH (cam:Campaign {campaign_id: $campaign_id})
                    MERGE (m)-[:PART_OF_CAMPAIGN]->(cam)
                """, 
                message_id=row['message_id'],
                campaign_id=int(row['campaign_id']))
                
                # Create relationship with user
                if pd.notna(row['user_id']):
                    session.run("""
                        MATCH (m:Message {message_id: $message_id})
                        MATCH (u:User {user_id: $user_id})
                        MERGE (u)-[:RECEIVED]->(m)
                        SET relationship.received_at = datetime($sent_at)
                    """, 
                    message_id=row['message_id'],
                    user_id=int(row['user_id']),
                    sent_at=row['sent_at'])
            
            if i % 10000 == 0:
                print(f"  Processed {i:,} messages...")
    
    print(f"✅ Loaded {len(messages_df)} messages")

def main():
    """Main function to create schema and load data"""
    print("🚀 Starting Neo4j data loading...")
    
    try:
        # Connect to database
        driver = get_db_connection()
        print("✅ Connected to Neo4j database")
        
        # Create constraints and indexes
        create_constraints_and_indexes(driver)
        
        # Load data in order of dependencies
        load_categories(driver)
        load_products(driver)
        load_users(driver)
        load_campaigns(driver)
        load_events(driver)
        load_friends(driver)
        load_messages(driver)
        
        driver.close()
        
        print("\n🎉 Neo4j data loading completed successfully!")
        print("📊 All data loaded into optimized graph schema")
        
    except Exception as e:
        print(f"❌ Error during Neo4j data loading: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
