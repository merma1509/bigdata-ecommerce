#!/usr/bin/env python3
"""
Run Neo4j Analysis Queries - Clean and Professional
"""

from neo4j import GraphDatabase
import os
from pathlib import Path

# Load environment variables
def load_env():
    env_file = Path('../../.env')
    if env_file.exists():
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip()

load_env()

def run_neo4j_analysis():
    """Run Neo4j analysis queries"""
    print("Running Neo4j Analysis Queries")
    print("=" * 50)
    
    try:
        driver = GraphDatabase.driver(
            'bolt://localhost:7687', 
            auth=('neo4j', os.getenv('NEO4J_PASSWORD', 'neo4j_pass'))
        )
        
        session = driver.session()
        
        # Query 1: Campaign Effectiveness
        print("\nCampaign Effectiveness Analysis:")
        print("-" * 40)
        
        q1_query = """
        MATCH (campaign:Campaign)
        WHERE campaign.started_at <= datetime() <= campaign.finished_at
        WITH campaign
        MATCH (campaign)-[:PART_OF_CAMPAIGN]->(message:Message)
        WITH campaign, message
        MATCH (message)<-[:RECEIVED]-(user:User)
        WITH campaign, message, user
        OPTIONAL MATCH (user)-[:EVENT {event_type: 'purchase'}]->(product:Product)
        WHERE user.user_id IS NOT NULL
        WITH campaign, user, COUNT(product) as purchase_count
        WITH campaign, 
            COUNT(DISTINCT user) as engaged_users,
            COUNT(DISTINCT CASE WHEN purchase_count > 0 THEN user END) as purchasing_users, 
            SUM(purchase_count) as total_purchases
        WITH campaign, engaged_users, purchasing_users, total_purchases, 
            CASE 
                WHEN engaged_users > 0 THEN purchasing_users * 100.0 / engaged_users 
                ELSE 0 
            END as conversion_rate
        RETURN 
            campaign.campaign_id,
            campaign.campaign_type,
            campaign.channel,
            engaged_users,
            purchasing_users,
            total_purchases,
            conversion_rate
        ORDER BY conversion_rate DESC, engaged_users DESC
        LIMIT 10
        """
        
        result = session.run(q1_query)
        for record in result:
            print(f"  Campaign {record['campaign.campaign_id']} ({record['campaign.campaign_type']}):")
            print(f"    Channel: {record['campaign.channel']}")
            print(f"    Engaged Users: {record['engaged_users']}")
            print(f"    Purchasing Users: {record['purchasing_users']}")
            print(f"    Total Purchases: {record['total_purchases']}")
            print(f"    Conversion Rate: {record['conversion_rate']:.2f}%")
            print()
        
        # Query 2: Product Analytics
        print("\nProduct Analytics:")
        print("-" * 40)
        
        q2_query = """
        MATCH (product:Product)
        OPTIONAL MATCH (product)<-[:BELONGS_TO]-(category:Category)
        OPTIONAL MATCH (product)<-[e:EVENT]-(user:User)
        WHERE e.event_type = 'purchase'
        WITH product, category, COUNT(DISTINCT user) as unique_buyers, COUNT(e) as total_purchases
        OPTIONAL MATCH (product)<-[e2:EVENT]-(user2:User)
        WHERE e2.event_type = 'view'
        WITH product, category, unique_buyers, total_purchases, COUNT(DISTINCT user2) as unique_viewers
        RETURN 
            product.product_id,
            product.brand,
            product.price,
            category.category_name,
            unique_viewers,
            unique_buyers,
            total_purchases,
            CASE 
                WHEN unique_viewers > 0 THEN unique_buyers * 100.0 / unique_viewers 
                ELSE 0 
            END as purchase_conversion_rate
        ORDER BY total_purchases DESC
        LIMIT 10
        """
        
        result = session.run(q2_query)
        for record in result:
            print(f"  Product {record['product.product_id']} ({record['product.brand']}):")
            print(f"    Category: {record['category.category_name']}")
            print(f"    Price: ${record['product.price']:.2f}")
            print(f"    Viewers: {record['unique_viewers']}")
            print(f"    Buyers: {record['unique_buyers']}")
            print(f"    Purchases: {record['total_purchases']}")
            print(f"    Purchase Rate: {record['purchase_conversion_rate']:.2f}%")
            print()
        
        # Query 3: Social Network Analysis
        print("\nSocial Network Analysis:")
        print("-" * 40)
        
        q3_query = """
        MATCH (user:User)-[:FRIENDS_WITH]-(friend:User)
        WITH user, COUNT(friend) as friend_count
        OPTIONAL MATCH (user)-[:EVENT {event_type: 'purchase'}]->(product:Product)
        WITH user, friend_count, COUNT(product) as purchase_count
        OPTIONAL MATCH (user)-[:EVENT {event_type: 'view'}]->(viewed_product:Product)
        WITH user, friend_count, purchase_count, COUNT(DISTINCT viewed_product) as viewed_count
        RETURN 
            user.user_id,
            friend_count,
            purchase_count,
            viewed_count,
            CASE 
                WHEN viewed_count > 0 THEN purchase_count * 1.0 / viewed_count 
                ELSE 0 
            END as purchase_rate
        ORDER BY friend_count DESC
        LIMIT 10
        """
        
        result = session.run(q3_query)
        for record in result:
            print(f"  User {record['user.user_id']}:")
            print(f"    Friends: {record['friend_count']}")
            print(f"    Purchases: {record['purchase_count']}")
            print(f"    Products Viewed: {record['viewed_count']}")
            print(f"    Purchase Rate: {record['purchase_rate']:.3f}")
            print()
        
        session.close()
        driver.close()
        
        print("Neo4j analysis completed successfully!")
        
    except Exception as e:
        print(f"Error running Neo4j analysis: {e}")

if __name__ == "__main__":
    run_neo4j_analysis()
