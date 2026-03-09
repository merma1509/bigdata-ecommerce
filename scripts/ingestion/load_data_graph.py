#!/usr/bin/env python3
"""Neo4j Graph Database Loading Script

This script creates the graph schema and loads the cleaned data
into Neo4j using a relationship-oriented approach"""

from neo4j import GraphDatabase
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Neo4jLoader:
    """Neo4j graph database loader"""
    
    def __init__(self, uri: str = "bolt://localhost:7687", user: str = "neo4j", password: str = "password"):
        self.uri = uri
        self.user = user
        self.password = password
        self.driver = None
        
    def connect(self):
        """Establish database connection"""
        try:
            self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
            # Test connection
            with self.driver.session() as session:
                session.run("RETURN 1")
            logger.info("Connected to Neo4j database")
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j: {e}")
            raise
    
    def disconnect(self):
        """Close database connection"""
        if self.driver:
            self.driver.close()
            logger.info("Disconnected from Neo4j database")
    
    def clear_database(self):
        """Clear all existing data"""
        logger.info("Clearing existing database...")
        
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            logger.info("Database cleared successfully")
    
    def create_constraints_and_indexes(self):
        """Create constraints and indexes for performance"""
        logger.info("Creating constraints and indexes...")
        
        constraints = [
            "CREATE CONSTRAINT user_id_unique IF NOT EXISTS FOR (u:User) REQUIRE u.user_id IS UNIQUE",
            "CREATE CONSTRAINT product_id_unique IF NOT EXISTS FOR (p:Product) REQUIRE p.product_id IS UNIQUE",
            "CREATE CONSTRAINT campaign_id_unique IF NOT EXISTS FOR (c:Campaign) REQUIRE (c.campaign_id, c.campaign_type) IS NODE KEY",
            "CREATE CONSTRAINT message_id_unique IF NOT EXISTS FOR (m:Message) REQUIRE m.message_id IS UNIQUE"
        ]
        
        indexes = [
            "CREATE INDEX user_first_purchase_idx IF NOT EXISTS FOR (u:User) ON (u.first_purchase_date)",
            "CREATE INDEX product_category_idx IF NOT EXISTS FOR (p:Product) ON (p.category_id)",
            "CREATE INDEX product_brand_idx IF NOT EXISTS FOR (p:Product) ON (p.brand)",
            "CREATE INDEX campaign_type_idx IF NOT EXISTS FOR (c:Campaign) ON (c.campaign_type)",
            "CREATE INDEX campaign_channel_idx IF NOT EXISTS FOR (c:Campaign) ON (c.channel)",
            "CREATE INDEX event_time_idx IF NOT EXISTS FOR ()-[e:EVENT]-() ON (e.event_time)",
            "CREATE INDEX message_sent_at_idx IF NOT EXISTS FOR ()-[m:RECEIVED]-() ON (m.sent_at)"
        ]
        
        with self.driver.session() as session:
            for constraint in constraints:
                try:
                    session.run(constraint)
                except Exception as e:
                    logger.warning(f"Constraint creation failed: {e}")
            
            for index in indexes:
                try:
                    session.run(index)
                except Exception as e:
                    logger.warning(f"Index creation failed: {e}")
        
        logger.info("Constraints and indexes created successfully")
    
    def load_users(self, data_path: str):
        """Load users as nodes"""
        logger.info("Loading users...")
        
        users_df = pd.read_csv(Path(data_path) / "client_first_purchase_date_cleaned.csv")
        
        with self.driver.session() as session:
            for _, row in users_df.iterrows():
                session.run("""
                    MERGE (u:User {user_id: $user_id})
                    SET u.first_purchase_date = $first_purchase_date
                """, user_id=row['user_id'], first_purchase_date=row['first_purchase_date'])
        
        logger.info(f"Loaded {len(users_df)} users")
    
    def load_products(self, data_path: str):
        """Load products as nodes"""
        logger.info("Loading products...")
        
        events_df = pd.read_csv(Path(data_path) / "events_cleaned.csv")
        products_df = events_df[['product_id', 'category_id', 'category_code', 'brand', 'price']].drop_duplicates()
        
        with self.driver.session() as session:
            for _, row in products_df.iterrows():
                session.run("""
                    MERGE (p:Product {product_id: $product_id})
                    SET p.category_id = $category_id,
                        p.category_code = $category_code,
                        p.brand = $brand,
                        p.price = $price
                """, product_id=row['product_id'], 
                   category_id=row['category_id'],
                   category_code=row['category_code'],
                   brand=row['brand'],
                   price=row['price'])
        
        logger.info(f"Loaded {len(products_df)} products")
    
    def load_campaigns(self, data_path: str):
        """Load campaigns as nodes"""
        logger.info("Loading campaigns...")
        
        campaigns_df = pd.read_csv(Path(data_path) / "campaigns_cleaned.csv")
        
        with self.driver.session() as session:
            for _, row in campaigns_df.iterrows():
                session.run("""
                    MERGE (c:Campaign {campaign_id: $campaign_id, campaign_type: $campaign_type})
                    SET c.channel = $channel,
                        c.topic = $topic,
                        c.started_at = $started_at,
                        c.finished_at = $finished_at,
                        c.total_count = $total_count,
                        c.ab_test = $ab_test,
                        c.warmup_mode = $warmup_mode,
                        c.hour_limit = $hour_limit,
                        c.subject_length = $subject_length,
                        c.subject_with_personalization = $subject_with_personalization,
                        c.subject_with_deadline = $subject_with_deadline,
                        c.subject_with_emoji = $subject_with_emoji,
                        c.subject_with_bonuses = $subject_with_bonuses,
                        c.subject_with_discount = $subject_with_discount,
                        c.subject_with_saleout = $subject_with_saleout,
                        c.is_test = $is_test,
                        c.position = $position
                """, **row.to_dict())
        
        logger.info(f"Loaded {len(campaigns_df)} campaigns")
    
    def load_events_and_relationships(self, data_path: str):
        """Load events as relationships between users and products"""
        logger.info("Loading events as relationships...")
        
        events_df = pd.read_csv(Path(data_path) / "events_cleaned.csv")
        
        with self.driver.session() as session:
            for _, row in events_df.iterrows():
                # Create EVENT relationship
                session.run("""
                    MATCH (u:User {user_id: $user_id})
                    MATCH (p:Product {product_id: $product_id})
                    MERGE (u)-[e:EVENT]->(p)
                    SET e.event_time = $event_time,
                        e.event_type = $event_type,
                        e.category_id = $category_id,
                        e.category_code = $category_code,
                        e.brand = $brand,
                        e.price = $price,
                        e.user_session = $user_session
                """, user_id=row['user_id'],
                   product_id=row['product_id'],
                   event_time=row['event_time'],
                   event_type=row['event_type'],
                   category_id=row['category_id'],
                   category_code=row['category_code'],
                   brand=row['brand'],
                   price=row['price'],
                   user_session=row['user_session'])
        
        logger.info(f"Loaded {len(events_df)} event relationships")
    
    def load_messages_and_relationships(self, data_path: str):
        """Load messages as nodes with relationships"""
        logger.info("Loading messages and relationships...")
        
        messages_df = pd.read_csv(Path(data_path) / "messages_cleaned.csv")
        
        with self.driver.session() as session:
            message_id = 0
            for _, row in messages_df.iterrows():
                message_id += 1
                
                # Create message node
                session.run("""
                    CREATE (m:Message {
                        message_id: $message_id,
                        campaign_id: $campaign_id,
                        message_type: $message_type,
                        channel: $channel,
                        client_id: $client_id,
                        email_provider: $email_provider,
                        platform: $platform,
                        stream: $stream,
                        date: $date,
                        sent_at: $sent_at,
                        is_opened: $is_opened,
                        opened_first_time_at: $opened_first_time_at,
                        opened_last_time_at: $opened_last_time_at,
                        is_clicked: $is_clicked,
                        clicked_first_time_at: $clicked_first_time_at,
                        clicked_last_time_at: $clicked_last_time_at,
                        is_unsubscribed: $is_unsubscribed,
                        unsubscribed_at: $unsubscribed_at,
                        is_hard_bounced: $is_hard_bounced,
                        hard_bounced_at: $hard_bounced_at,
                        is_soft_bounced: $is_soft_bounced,
                        soft_bounced_at: $soft_bounced_at,
                        is_complained: $is_complained,
                        complained_at: $complained_at,
                        is_blocked: $is_blocked,
                        blocked_at: $blocked_at,
                        is_purchased: $is_purchased,
                        purchased_at: $purchased_at
                    })
                """, message_id=message_id, **row.to_dict())
                
                # Create relationship to campaign
                session.run("""
                    MATCH (m:Message {message_id: $message_id})
                    MATCH (c:Campaign {campaign_id: $campaign_id, campaign_type: $message_type})
                    MERGE (m)-[:PART_OF]->(c)
                """, message_id=message_id, 
                   campaign_id=row['campaign_id'],
                   message_type=row['message_type'])
                
                # Extract user_id from client_id for relationship
                # client_id = 151591562 + user_id + user_device_id
                try:
                    client_id_str = str(row['client_id'])
                    if client_id_str.startswith('151591562'):
                        # Extract user_id 
                        remaining = client_id_str[9:]           # Remove '151591562' prefix
                        user_id = int(remaining.split('_')[0])  # Get the part before underscore
                        
                        session.run("""
                            MATCH (m:Message {message_id: $message_id})
                            MATCH (u:User {user_id: $user_id})
                            MERGE (u)-[:RECEIVED {
                                sent_at: $sent_at,
                                is_opened: $is_opened,
                                is_clicked: $is_clicked,
                                is_purchased: $is_purchased
                            }]->(m)
                        """, message_id=message_id,
                           user_id=user_id,
                           sent_at=row['sent_at'],
                           is_opened=row['is_opened'],
                           is_clicked=row['is_clicked'],
                           is_purchased=row['is_purchased'])
                except:
                    # Skip if client_id parsing fails
                    pass
        
        logger.info(f"Loaded {len(messages_df)} messages and relationships")
    
    def load_friendships(self, data_path: str):
        """Load friendship relationships"""
        logger.info("Loading friendships...")
        
        friends_df = pd.read_csv(Path(data_path) / "friends_cleaned.csv")
        
        with self.driver.session() as session:
            for _, row in friends_df.iterrows():
                session.run("""
                    MATCH (u1:User {user_id: $user_id})
                    MATCH (u2:User {user_id: $friend_id})
                    MERGE (u1)-[:FRIEND]->(u2)
                """, user_id=row['user_id'], friend_id=row['friend_id'])
        
        logger.info(f"Loaded {len(friends_df)} friendship relationships")
    
    def get_data_summary(self):
        """Get summary statistics of loaded data"""
        logger.info("Getting data summary...")
        
        with self.driver.session() as session:
            # Count nodes
            node_counts = session.run("""
                MATCH (n)
                RETURN labels(n) as label, count(n) as count
                ORDER BY count DESC
            """).data()
            
            # Count relationships
            rel_counts = session.run("""
                MATCH ()-[r]->()
                RETURN type(r) as type, count(r) as count
                ORDER BY count DESC
            """).data()
            
            return {
                'nodes': node_counts,
                'relationships': rel_counts
            }

def main():
    """Main execution function"""
    # Initialize loader
    loader = Neo4jLoader()
    
    try:
        # Connect to database
        loader.connect()
        
        # Clear and setup database
        loader.clear_database()
        loader.create_constraints_and_indexes()
        
        # Load all data
        loader.load_users("../data/processed")
        loader.load_products("../data/processed")
        loader.load_campaigns("../data/processed")
        loader.load_events_and_relationships("../data/processed")
        loader.load_messages_and_relationships("../data/processed")
        loader.load_friendships("../data/processed")
        
        # Get summary
        summary = loader.get_data_summary()
        print("\n=== Neo4j Data Summary ===")
        print("Nodes:")
        for node in summary['nodes']:
            print(f"  {node['label'][0]}: {node['count']:,}")
        print("\nRelationships:")
        for rel in summary['relationships']:
            print(f"  {rel['type']}: {rel['count']:,}")
        
    except Exception as e:
        logger.error(f"Error during data loading: {e}")
        raise
    finally:
        loader.disconnect()

if __name__ == "__main__":
    main()
