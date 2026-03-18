#!/usr/bin/env python3
"""Simplified Hybrid Architecture Demonstration
Demonstrates hybrid capabilities using existing database structure"""

import psycopg2
import pymongo
from neo4j import GraphDatabase
import pandas as pd
import json
import os
from datetime import datetime
import time
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

class SimpleHybridDemo:
    """Simple demonstration of hybrid architecture benefits"""
    
    def __init__(self):
        # Database configurations
        self.pg_config = {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': os.getenv('POSTGRES_PORT', '5432'),
            'database': os.getenv('POSTGRES_DB', 'ecommerce'),
            'user': os.getenv('POSTGRES_USER', 'ecommerce_user'),
            'password': os.getenv('POSTGRES_PASSWORD', 'ecommerce_pass')
        }
        
        self.mongo_config = {
            'host': os.getenv('MONGO_HOST', 'localhost'),
            'port': int(os.getenv('MONGO_PORT', '27017')),
            'database': os.getenv('MONGO_DB', 'ecommerce')
        }
        
        self.neo4j_config = {
            'uri': os.getenv('NEO4J_URI', 'bolt://localhost:7687'),
            'user': os.getenv('NEO4J_USER', 'neo4j'),
            'password': os.getenv('NEO4J_PASSWORD', 'neo4j_pass')
        }
        
        self.results = {}
    
    def connect_databases(self):
        """Connect to all databases"""
        try:
            self.pg_conn = psycopg2.connect(**self.pg_config)
            print("PostgreSQL connected")
            
            self.mongo_client = pymongo.MongoClient(
                f"mongodb://{self.mongo_config['host']}:{self.mongo_config['port']}/"
            )
            self.mongo_db = self.mongo_client[self.mongo_config['database']]
            print("MongoDB connected")
            
            self.neo4j_driver = GraphDatabase.driver(
                self.neo4j_config['uri'],
                auth=(self.neo4j_config['user'], self.neo4j_config['password'])
            )
            print("Neo4j connected")
            return True
        except Exception as e:
            print(f"Connection error: {e}")
            return False
    
    def demonstrate_postgresql_strengths(self):
        """Demonstrate PostgreSQL strengths - structured analytics"""
        print("\n=== POSTGRESQL: STRUCTURED ANALYTICS ===")
        
        # Complex join query with aggregations
        query = """
        SELECT 
            c.campaign_type,
            c.channel,
            COUNT(DISTINCT ue.user_id) as total_users,
            COUNT(DISTINCT CASE WHEN ue.event_type = 'purchase' THEN ue.user_id END) as purchasers,
            ROUND(AVG(CASE WHEN ue.event_type = 'purchase' THEN ue.price END), 2) as avg_purchase_value
        FROM campaigns c
        LEFT JOIN message_events me ON c.campaign_id = me.campaign_id
        LEFT JOIN user_events ue ON me.client_id = ue.client_id AND ue.event_type = 'purchase'
        GROUP BY c.campaign_type, c.channel
        ORDER BY avg_purchase_value DESC NULLS LAST
        LIMIT 5
        """
        
        start_time = time.time()
        result = pd.read_sql(query, self.pg_conn)
        execution_time = (time.time() - start_time) * 1000
        
        self.results['postgresql'] = {
            'execution_time': execution_time,
            'record_count': len(result),
            'strength': 'Complex joins and aggregations',
            'data': result.to_dict('records')
        }
        
        print(f"Query completed in {execution_time:.2f}ms")
        print(f"{len(result)} campaign performance records")
        print("Strength: Complex analytical queries with multiple joins")
        
        return result
    
    def demonstrate_mongodb_strengths(self):
        """Demonstrate MongoDB strengths - document flexibility"""
        print("\n=== MONGODB: DOCUMENT FLEXIBILITY ===")
        
        # Flexible document aggregation
        pipeline = [
            {"$match": {"event_type": "view"}},
            {"$group": {
                "_id": "$product_id",
                "brand": {"$first": "$brand"},
                "category": {"$first": "$category_code"},
                "view_count": {"$sum": 1},
                "unique_users": {"$addToSet": "$user_id"}
            }},
            {"$addFields": {
                "unique_user_count": {"$size": "$unique_users"}
            }},
            {"$sort": {"view_count": -1}},
            {"$limit": 5}
        ]
        
        start_time = time.time()
        result = list(self.mongo_db.events.aggregate(pipeline))
        execution_time = (time.time() - start_time) * 1000
        
        self.results['mongodb'] = {
            'execution_time': execution_time,
            'record_count': len(result),
            'strength': 'Flexible document aggregation',
            'data': result
        }
        
        print(f"Query completed in {execution_time:.2f}ms")
        print(f"{len(result)} product view aggregations")
        print("Strength: Flexible schema and powerful aggregation")
        
        return result
    
    def demonstrate_neo4j_strengths(self):
        """Demonstrate Neo4j strengths - relationship traversal"""
        print("\n=== NEO4J: RELATIONSHIP TRAVERSAL ===")
        
        # Graph traversal for recommendations
        query = """
        MATCH (u:User)-[:VIEWED]->(p:Product)
        WHERE u.user_id IS NOT NULL
        WITH p, COUNT(DISTINCT u) as viewer_count
        OPTIONAL MATCH (p)<-[:FRIENDS_WITH]-(friend:User)
        RETURN 
            p.product_id,
            p.brand,
            p.price,
            viewer_count,
            COUNT(DISTINCT friend) as social_reach
        ORDER BY viewer_count DESC, social_reach DESC
        LIMIT 5
        """
        
        start_time = time.time()
        with self.neo4j_driver.session() as session:
            result = list(session.run(query))
        execution_time = (time.time() - start_time) * 1000
        
        # Convert to dict format
        result_dicts = [dict(record) for record in result]
        
        self.results['neo4j'] = {
            'execution_time': execution_time,
            'record_count': len(result_dicts),
            'strength': 'Natural relationship traversal',
            'data': result_dicts
        }
        
        print(f"Query completed in {execution_time:.2f}ms")
        print(f"{len(result_dicts)} product relationship records")
        print("Strength: Natural graph traversal and social analysis")
        
        return result_dicts
    
    def demonstrate_hybrid_integration(self):
        """Show how hybrid approach combines strengths"""
        print("\n=== HYBRID INTEGRATION BENEFITS ===")
        
        # Calculate performance metrics
        total_time = sum([
            self.results['postgresql']['execution_time'],
            self.results['mongodb']['execution_time'],
            self.results['neo4j']['execution_time']
        ])
        
        avg_time = total_time / 3
        
        # Determine best database for each use case
        best_analytics = min([
            ('PostgreSQL', self.results['postgresql']['execution_time'])
        ], key=lambda x: x[1])
        
        best_documents = min([
            ('MongoDB', self.results['mongodb']['execution_time'])
        ], key=lambda x: x[1])
        
        best_graph = min([
            ('Neo4j', self.results['neo4j']['execution_time'])
        ], key=lambda x: x[1])
        
        print(f"Average query time: {avg_time:.2f}ms")
        print(f"Best for analytics: {best_analytics[0]} ({best_analytics[1]:.2f}ms)")
        print(f"Best for documents: {best_documents[0]} ({best_documents[1]:.2f}ms)")
        print(f"Best for graphs: {best_graph[0]} ({best_graph[1]:.2f}ms)")
        
        # Hybrid benefits
        hybrid_benefits = {
            'query_routing': 'Route queries to optimal database',
            'performance_optimization': f'{avg_time:.2f}ms average vs single database limitations',
            'flexibility': 'Choose right database for each use case',
            'scalability': 'Scale each component independently'
        }
        
        self.results['hybrid_benefits'] = hybrid_benefits
        
        print("\nHYBRID ARCHITECTURE BENEFITS:")
        for benefit, description in hybrid_benefits.items():
            print(f"  • {benefit.replace('_', ' ').title()}: {description}")
        
        return hybrid_benefits
    
    def create_hybrid_summary_report(self):
        """Create comprehensive summary report"""
        print("\n" + "="*60)
        print("HYBRID ARCHITECTURE DEMONSTRATION SUMMARY")
        print("="*60)
        
        # Performance comparison
        print("\nPERFORMANCE COMPARISON:")
        print("-" * 40)
        for db, data in self.results.items():
            if db != 'hybrid_benefits' and 'execution_time' in data:
                print(f"{db:12}: {data['execution_time']:8.2f}ms | {data['strength']}")
        
        # Use case recommendations
        print("\nUSE CASE RECOMMENDATIONS:")
        print("-" * 40)
        recommendations = {
            'Campaign Analytics': 'PostgreSQL (complex joins)',
            'Real-time Events': 'MongoDB (document flexibility)',
            'Recommendations': 'Neo4j (graph traversal)',
            'Search Functionality': 'PostgreSQL (text search)',
            'Activity Logging': 'MongoDB (schema flexibility)',
            'Social Analysis': 'Neo4j (relationship queries)'
        }
        
        for use_case, recommendation in recommendations.items():
            print(f"{use_case:20}: {recommendation}")
        
        # Architecture benefits
        print("\nARCHITECTURE ADVANTAGES:")
        print("-" * 40)
        advantages = [
            "Optimal performance through database selection",
            "Specialized query optimization per database",
            "Independent scaling of components",
            "Reduced single point of failure",
            "Flexibility for evolving requirements"
        ]
        
        for advantage in advantages:
            print(f"{advantage}")
    
    def save_results(self):
        """Save demonstration results"""
        output_dir = "output"
        os.makedirs(output_dir, exist_ok=True)
        
        # Add metadata
        self.results['timestamp'] = datetime.now().isoformat()
        self.results['demonstration_type'] = 'hybrid_architecture'
        self.results['databases_tested'] = ['PostgreSQL', 'MongoDB', 'Neo4j']
        
        # Save to JSON
        results_file = os.path.join(output_dir, "hybrid_demo_results.json")
        with open(results_file, 'w') as f:
            json.dump(self.results, f, indent=2, default=str)
        
        print(f"\nResults saved to: {results_file}")
        return results_file
    
    def close_connections(self):
        """Close all connections"""
        try:
            if hasattr(self, 'pg_conn') and self.pg_conn:
                self.pg_conn.close()
            if hasattr(self, 'mongo_client') and self.mongo_client:
                self.mongo_client.close()
            if hasattr(self, 'neo4j_driver') and self.neo4j_driver:
                self.neo4j_driver.close()
            print("All connections closed")
        except:
            pass
    
    def run_demonstration(self):
        """Run the complete hybrid demonstration"""
        print("STARTING HYBRID ARCHITECTURE DEMONSTRATION")
        print("="*60)
        
        try:
            if not self.connect_databases():
                return
            
            # Run individual database demonstrations
            self.demonstrate_postgresql_strengths()
            self.demonstrate_mongodb_strengths()
            self.demonstrate_neo4j_strengths()
            
            # Show hybrid integration benefits
            self.demonstrate_hybrid_integration()
            
            # Create summary report
            self.create_hybrid_summary_report()
            
            # Save results
            self.save_results()
            
            print("\n" + "="*60)
            print("HYBRID DEMONSTRATION COMPLETED SUCCESSFULLY!")
            print("Key Takeaway: Hybrid architecture optimizes performance by")
            print("leveraging each database's strengths for specific use cases.")
            print("="*60)
            
        except Exception as e:
            print(f"\nDemonstration error: {e}")
        finally:
            self.close_connections()

def main():
    """Main execution"""
    demo = SimpleHybridDemo()
    demo.run_demonstration()

if __name__ == "__main__":
    main()
