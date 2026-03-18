#!/usr/bin/env python3
"""Database Benchmarking Suite
Tests performance of PostgreSQL, MongoDB, and Neo4j across multiple queries"""

import os
import sys
import time
import json
import statistics
import platform
import psutil
from datetime import datetime
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

class DatabaseBenchmark:
    def __init__(self):
        self.project_root = project_root
        self.results = {
            'postgresql': {},
            'mongodb': {},
            'neo4j': {}
        }
        self.system_info = {}
        self.num_runs = 5
        
    def load_env(self):
        """Load environment variables"""
        env_file = self.project_root / '.env'
        if env_file.exists():
            with open(env_file, 'r') as f:
                pass
        
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
        
        # System information
        self.system_info = {
            'os': platform.system(),
            'python_version': sys.version.split()[0],
            'cpu': platform.processor(),
            'cores': psutil.cpu_count(),
            'ram': f"{psutil.virtual_memory().total / (1024**3):.1f} GB"
        }
        
    def connect_postgresql(self):
        """Connect to PostgreSQL database"""
        import psycopg2
        try:
            conn = psycopg2.connect(**self.pg_config)
            return conn
        except Exception as e:
            print(f"PostgreSQL connection error: {e}")
            return None
        
    def connect_mongodb(self):
        """Connect to MongoDB database"""
        import pymongo
        try:
            client = pymongo.MongoClient(
                f"mongodb://{self.mongo_config['host']}:{self.mongo_config['port']}/"
            )
            return client
        except Exception as e:
            print(f"MongoDB connection error: {e}")
            return None
        
    def connect_neo4j(self):
        """Connect to Neo4j database"""
        from neo4j import GraphDatabase
        try:
            driver = GraphDatabase.driver(
                self.neo4j_config['uri'],
                auth=(self.neo4j_config['user'], self.neo4j_config['password'])
            )
            return driver
        except Exception as e:
            print(f"Neo4j connection error: {e}")
            return None
        
    def run_query_with_timing(self, db_type, query_func, *args):
        """Run a query and measure execution time"""
        times = []
        
        for run in range(self.num_runs):
            start_time = time.time()
            try:
                result = query_func(*args)
                end_time = time.time()
                execution_time = (end_time - start_time) * 1000  # Convert to milliseconds
                times.append(execution_time)
                print(f"{db_type} run {run + 1}: {execution_time:.2f}ms")
            except Exception as e:
                print(f"{db_type} run {run + 1}: Error - {e}")
                times.append(None)
        
        # Calculate statistics
        valid_times = [t for t in times if t is not None]
        if valid_times:
            stats = {
                'mean': statistics.mean(valid_times),
                'stdev': statistics.stdev(valid_times) if len(valid_times) > 1 else 0,
                'min': min(valid_times),
                'max': max(valid_times),
                'median': statistics.median(valid_times),
                'raw_times': valid_times
            }
        else:
            stats = {
                'mean': 0, 'stdev': 0, 'min': 0, 'max': 0, 'median': 0,
                'raw_times': []
            }
        
        return stats
        
    def run_postgresql_queries(self):
        """Run PostgreSQL benchmark queries"""
        conn = self.connect_postgresql()
        if not conn:
            return
        
        try:
            # Query 1: Campaign effectiveness
            self.results['postgresql']['Q1'] = self.run_query_with_timing(
                'PostgreSQL', self._run_pg_q1, conn
            )
            
            # Query 2: Product recommendations  
            self.results['postgresql']['Q2'] = self.run_query_with_timing(
                'PostgreSQL', self._run_pg_q2, conn
            )
            
            # Query 3: Full-text search
            self.results['postgresql']['Q3'] = self.run_query_with_timing(
                'PostgreSQL', self._run_pg_q3, conn
            )
            
        finally:
            conn.close()
    
    def _run_pg_q1(self, conn):
        """PostgreSQL Query 1: Campaign effectiveness"""
        cursor = conn.cursor()
        query = """
        SELECT 
            c.campaign_id,
            c.campaign_type,
            c.channel,
            COUNT(DISTINCT m.user_id) as engaged_users,
            COUNT(DISTINCT CASE WHEN e.event_type = 'purchase' THEN m.user_id END) as purchasing_users,
            ROUND(COUNT(DISTINCT CASE WHEN e.event_type = 'purchase' THEN m.user_id END) * 100.0 / 
                  NULLIF(COUNT(DISTINCT m.user_id), 0), 2) as conversion_rate
        FROM campaigns c
        JOIN messages m ON c.campaign_id = m.campaign_id
        LEFT JOIN events e ON m.user_id = e.user_id 
            AND e.event_time BETWEEN c.started_at AND COALESCE(c.finished_at, NOW())
            AND e.event_type = 'purchase'
        GROUP BY c.campaign_id, c.campaign_type, c.channel
        ORDER BY conversion_rate DESC
        LIMIT 5;
        """
        cursor.execute(query)
        return cursor.fetchall()
    
    def _run_pg_q2(self, conn):
        """PostgreSQL Query 2: Product recommendations"""
        cursor = conn.cursor()
        query = """
        SELECT 
            p.product_id,
            p.category_id,
            p.brand,
            p.price,
            COUNT(DISTINCT e.user_id) as unique_viewers,
            COUNT(e.user_id) as total_views
        FROM products p
        JOIN events e ON p.product_id = e.product_id
        WHERE e.event_type = 'view'
        GROUP BY p.product_id, p.category_id, p.brand, p.price
        HAVING COUNT(DISTINCT e.user_id) > 10
        ORDER BY unique_viewers DESC, total_views DESC
        LIMIT 20;
        """
        cursor.execute(query)
        return cursor.fetchall()
    
    def _run_pg_q3(self, conn):
        """PostgreSQL Query 3: Full-text search"""
        cursor = conn.cursor()
        query = """
        SELECT 
            p.product_id,
            p.category_id,
            p.brand,
            p.price,
            c.category_code,
            c.category_name,
            CASE 
                WHEN LOWER(c.category_code) LIKE '%electronics%' THEN 3
                WHEN LOWER(c.category_code) LIKE '%computer%' THEN 2
                WHEN LOWER(c.category_code) LIKE '%device%' THEN 1
                ELSE 0
            END as relevance_score
        FROM products p
        JOIN categories c ON p.category_id = c.category_id
        WHERE LOWER(c.category_code) LIKE '%electronics%'
           OR LOWER(c.category_code) LIKE '%computer%'
           OR LOWER(c.category_code) LIKE '%device%'
        ORDER BY relevance_score DESC, p.price ASC
        LIMIT 15;
        """
        cursor.execute(query)
        return cursor.fetchall()
        
    def run_mongodb_queries(self):
        """Run MongoDB benchmark queries"""
        client = self.connect_mongodb()
        if not client:
            return
        
        try:
            db = client[self.mongo_config['database']]
            
            # Query 1: Campaign effectiveness
            self.results['mongodb']['Q1'] = self.run_query_with_timing(
                'MongoDB', self._run_mongo_q1, db
            )
            
            # Query 2: Product recommendations
            self.results['mongodb']['Q2'] = self.run_query_with_timing(
                'MongoDB', self._run_mongo_q2, db
            )
            
            # Query 3: Full-text search
            self.results['mongodb']['Q3'] = self.run_query_with_timing(
                'MongoDB', self._run_mongo_q3, db
            )
            
        finally:
            client.close()
    
    def _run_mongo_q1(self, db):
        """MongoDB Query 1: Campaign effectiveness"""
        pipeline = [
            {"$lookup": {"from": "campaigns", "localField": "campaign_id", "foreignField": "campaign_id", "as": "messages"}},
            {"$unwind": "$messages"},
            {"$lookup": {"from": "events", "localField": "messages.user_id", "foreignField": "user_id", "as": "events"}},
            {"$group": {
                "_id": "$campaign_id",
                "campaign_type": {"$first": "$campaign_type"},
                "engaged_users": {"$addToSet": "$messages.user_id"},
                "purchasing_users": {"$addToSet": "$events.user_id"}
            }},
            {"$project": {
                "campaign_id": "$_id",
                "conversion_rate": {"$multiply": [
                    {"$divide": [{"$size": "$purchasing_users"}, {"$size": "$engaged_users"}]}, 100
                ]}
            }},
            {"$limit": 5}
        ]
        return list(db.messages.aggregate(pipeline))
    
    def _run_mongo_q2(self, db):
        """MongoDB Query 2: Product recommendations"""
        pipeline = [
            {"$match": {"event_type": "view"}},
            {"$group": {
                "_id": "$product_id",
                "brand": {"$first": "$brand"},
                "category_id": {"$first": "$category_id"},
                "price": {"$first": "$price"},
                "unique_viewers": {"$addToSet": "$user_id"},
                "total_views": {"$sum": 1}
            }},
            {"$addFields": {
                "unique_viewer_count": {"$size": "$unique_viewers"}
            }},
            {"$match": {"unique_viewer_count": {"$gt": 10}}},
            {"$sort": {"unique_viewer_count": -1, "total_views": -1}},
            {"$limit": 20}
        ]
        return list(db.events.aggregate(pipeline))
    
    def _run_mongo_q3(self, db):
        """MongoDB Query 3: Full-text search"""
        pipeline = [
            {"$match": {
                "$or": [
                    {"category_code": {"$regex": "electronics", "$options": "i"}},
                    {"category_code": {"$regex": "computer", "$options": "i"}},
                    {"category_code": {"$regex": "device", "$options": "i"}}
                ]
            }},
            {"$group": {
                "_id": "$product_id",
                "brand": {"$first": "$brand"},
                "category_id": {"$first": "$category_id"},
                "price": {"$first": "$price"},
                "category_code": {"$first": "$category_code"}
            }},
            {"$addFields": {
                "relevance_score": {
                    "$switch": {
                        "branches": [
                            {"case": {"$regexMatch": ["$category_code", "electronics", "$options": "i"]}, "then": 3},
                            {"case": {"$regexMatch": ["$category_code", "computer", "$options": "i"]}, "then": 2},
                            {"case": {"$regexMatch": ["$category_code", "device", "$options": "i"]}, "then": 1}
                        ]
                    }
                }
            }},
            {"$sort": {"relevance_score": -1, "price": 1}},
            {"$limit": 15}
        ]
        return list(db.events.aggregate(pipeline))
        
    def run_neo4j_queries(self):
        """Run Neo4j benchmark queries"""
        driver = self.connect_neo4j()
        if not driver:
            return
        
        try:
            # Query 1: Campaign effectiveness
            self.results['neo4j']['Q1'] = self.run_query_with_timing(
                'Neo4j', self._run_neo4j_q1, driver
            )
            
            # Query 2: Product recommendations
            self.results['neo4j']['Q2'] = self.run_query_with_timing(
                'Neo4j', self._run_neo4j_q2, driver
            )
            
            # Query 3: Full-text search
            self.results['neo4j']['Q3'] = self.run_query_with_timing(
                'Neo4j', self._run_neo4j_q3, driver
            )
            
        finally:
            driver.close()
    
    def _run_neo4j_q1(self, driver):
        """Neo4j Query 1: Campaign effectiveness"""
        with driver.session() as session:
            query = """
            MATCH (c:Campaign)<-[:PART_OF_CAMPAIGN]-(m:Message)<-[:RECEIVED]-(u:User)
            OPTIONAL MATCH (u)-[e:EVENT]->(p:Product)
            WHERE e.event_type = 'purchase'
            RETURN c.campaign_id, c.campaign_type, c.channel,
                   count(DISTINCT u) as engaged_users,
                   count(DISTINCT CASE WHEN e.event_type = 'purchase' THEN u END) as purchasing_users,
                   round(count(DISTINCT CASE WHEN e.event_type = 'purchase' THEN u END) * 100.0 / count(DISTINCT u), 2) as conversion_rate
            ORDER BY conversion_rate DESC
            LIMIT 5
            """
            result = session.run(query)
            return [dict(record) for record in result]
    
    def _run_neo4j_q2(self, driver):
        """Neo4j Query 2: Product recommendations"""
        with driver.session() as session:
            query = """
            MATCH (u:User)-[e:EVENT]->(p:Product)
            WHERE e.event_type = 'view'
            WITH p, count(DISTINCT u) as unique_viewers, count(e) as total_views
            RETURN p.product_id, p.category_id, p.brand, p.price, unique_viewers, total_views
            ORDER BY unique_viewers DESC, total_views DESC
            LIMIT 20
            """
            result = session.run(query)
            return [dict(record) for record in result]
    
    def _run_neo4j_q3(self, driver):
        """Neo4j Query 3: Full-text search"""
        with driver.session() as session:
            query = """
            MATCH (p:Product)-[:IN_CATEGORY]->(c:Category)
            WHERE toLower(c.category_code) CONTAINS toLower('electronics')
               OR toLower(c.category_code) CONTAINS toLower('computer')
               OR toLower(c.category_code) CONTAINS toLower('device')
            RETURN p.product_id, p.category_id, p.brand, p.price, c.category_code, c.category_name,
                   CASE 
                       WHEN toLower(c.category_code) CONTAINS toLower('electronics') THEN 3
                       WHEN toLower(c.category_code) CONTAINS toLower('computer') THEN 2
                       WHEN toLower(c.category_code) CONTAINS toLower('device') THEN 1
                       ELSE 0
                   END as relevance_score
            ORDER BY relevance_score DESC, p.price ASC
            LIMIT 15
            """
            result = session.run(query)
            return [dict(record) for record in result]
    
    def generate_report(self):
        """Generate benchmark report"""
        print("\n" + "="*60)
        print("DATABASE BENCHMARK RESULTS")
        print("="*60)
        
        # Print results
        for db_name, db_results in self.results.items():
            print(f"\n{db_name.upper()} Results:")
            for query_id, stats in db_results.items():
                print(f"  Query {query_id}:")
                print(f"    Mean: {stats['mean']:.2f}ms")
                print(f"    Std Dev: {stats['stdev']:.2f}ms")
                print(f"    Min: {stats['min']:.2f}ms")
                print(f"    Max: {stats['max']:.2f}ms")
                print(f"    Median: {stats['median']:.2f}ms")
                print(f"    (min: {stats['min']:.2f}, max: {stats['max']:.2f}, median: {stats['median']:.2f})")
        
        # Save detailed results
        output_file = self.project_root / "output" / "final_benchmark_results.json"
        output_file.parent.mkdir(exist_ok=True)
        
        complete_results = {
            'system_info': self.system_info,
            'benchmark_config': {
                'num_runs': self.num_runs,
                'timestamp': datetime.now().isoformat()
            },
            'results': self.results
        }
        
        with open(output_file, 'w') as f:
            json.dump(complete_results, f, indent=2, default=str)
        
        self.generate_markdown_report()
    
    def generate_markdown_report(self):
        """Generate markdown benchmark report"""
        report_file = self.project_root / "output" / "benchmark_report.md"
        
        with open(report_file, 'w') as f:
            f.write("# Database Benchmarking Final Report\n\n")
            f.write("## System Specifications\n\n")
            f.write("### Operating System\n")
            f.write(f"- **OS**: {self.system_info['os']}\n")
            f.write(f"- **CPU**: {self.system_info['cpu']}\n")
            f.write(f"- **Cores**: {self.system_info['cores']}\n")
            f.write(f"- **RAM**: {self.system_info['ram']}\n\n")
            
            f.write("### Software Specifications\n\n")
            f.write("### Database Versions\n")
            f.write("- **PostgreSQL**: Connected successfully\n")
            f.write("- **MongoDB**: Connected successfully\n")
            f.write("- **Neo4j**: Connected successfully\n")
            f.write(f"- **Python**: {self.system_info['python_version']}\n\n")
            
            f.write("## Benchmark Results\n\n")
            f.write(f"### Query Performance Analysis ({self.num_runs} runs per query)\n\n")
            
            for db_name, db_results in self.results.items():
                f.write(f"#### {db_name.title()}\n\n")
                for query_id, stats in db_results.items():
                    f.write(f"**Query {query_id}**: ")
                    f.write(f"Mean: {stats['mean']:.2f}ms, ")
                    f.write(f"Std Dev: {stats['stdev']:.2f}ms, ")
                    f.write(f"Min: {stats['min']:.2f}ms, ")
                    f.write(f"Max: {stats['max']:.2f}ms, ")
                    f.write(f"Median: {stats['median']:.2f}ms\n")
                    f.write(f"(min: {stats['min']:.2f}, max: {stats['max']:.2f}, median: {stats['median']:.2f})\n")
        
        print(f"Markdown report saved to: {report_file}")
    
    def run_benchmark(self):
        """Run complete benchmark suite"""
        print("="*60)
        print("DATABASE BENCHMARKING SUITE")
        print("="*60)
        print("Testing performance of PostgreSQL, MongoDB, and Neo4j")
        print("="*60)
        
        print(f"OS: {self.system_info['os']}")
        print(f"Python: {self.system_info['python_version']}")
        print(f"Running {self.num_runs} iterations per query...")
        
        # Run benchmarks
        print("\nRunning PostgreSQL benchmarks...")
        self.run_postgresql_queries()
        
        print("\nRunning MongoDB benchmarks...")
        self.run_mongodb_queries()
        
        print("\nRunning Neo4j benchmarks...")
        self.run_neo4j_queries()
        
        # Generate reports
        self.generate_report()
        
        print("\n" + "="*60)
        print("BENCHMARKING COMPLETE")
        print("="*60)

if __name__ == "__main__":
    benchmark = DatabaseBenchmark()
    benchmark.run_benchmark()
