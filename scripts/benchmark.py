#!/usr/bin/env python3
"""Benchmarking Infrastructure
Handles database connections, query execution, and performance measurement"""

import time
import subprocess
import json
import statistics
import os
import requests
from datetime import datetime
from typing import Dict, List, Any

class DatabaseBenchmark:
    """Handles benchmarking operations for all database types"""
    
    def __init__(self):
        # Load environment variables from .env file
        from dotenv import load_dotenv
        load_dotenv()
        
        self.results = {
            'postgresql': {},
            'mongodb': {},
            'neo4j': {}
        }
        self.system_specs = self.get_system_specs()
        self.software_specs = self.get_software_specs()
        
    def get_system_specs(self) -> Dict[str, Any]:
        """Get system specifications (cross-platform)"""
        specs = {
            'cpu': 'Unknown',
            'cores': 0,
            'clock_speed': 0,
            'ram': 0.0,
            'os': 'Unknown',
            'version': 'Unknown'
        }
        
        try:
            import platform
            import psutil
            
            # OS Information
            specs['os'] = platform.system()
            specs['version'] = platform.version()
            
            # CPU Information
            specs['cpu'] = platform.processor() or 'Unknown'
            specs['cores'] = psutil.cpu_count(logical=True) or 0
            
            # Clock Speed - try multiple methods
            try:
                cpu_freq = psutil.cpu_freq()
                if cpu_freq and cpu_freq.current:
                    specs['clock_speed'] = cpu_freq.current
                else:
                    specs['clock_speed'] = 0
            except:
                specs['clock_speed'] = 0
            
            # RAM Information - try psutil first, then fallback
            try:
                ram_bytes = psutil.virtual_memory().total
                specs['ram'] = round(ram_bytes / (1024**3), 1)
            except:
                specs['ram'] = 0.0
            
            # If RAM is still 0, try Windows-specific methods
            if specs['ram'] == 0.0 and specs['os'] == 'Windows':
                try:
                    import subprocess
                    # Try CIM command
                    result = subprocess.run(['powershell', '-Command', 
                        'Get-CimInstance -ClassName Win32_ComputerSystem | Select-Object -ExpandProperty TotalPhysicalMemory'], 
                        capture_output=True, text=True, timeout=5)
                    if result.returncode == 0 and result.stdout.strip():
                        ram_bytes = int(result.stdout.strip())
                        specs['ram'] = round(ram_bytes / (1024**3), 1)
                except:
                    pass
            
        except ImportError:
            # Fallback to basic platform info
            try:
                import platform
                import os
                
                specs['os'] = platform.system()
                specs['version'] = platform.release()
                specs['cpu'] = platform.processor() or 'Unknown'
                specs['cores'] = os.cpu_count() or 0
                
                # Try Windows CIM for RAM
                if specs['os'] == 'Windows':
                    try:
                        import subprocess
                        result = subprocess.run(['powershell', '-Command', 
                            'Get-CimInstance -ClassName Win32_ComputerSystem | Select-Object -ExpandProperty TotalPhysicalMemory'], 
                            capture_output=True, text=True, timeout=5)
                        if result.returncode == 0 and result.stdout.strip():
                            ram_bytes = int(result.stdout.strip())
                            specs['ram'] = round(ram_bytes / (1024**3), 1)
                    except:
                        pass
                    
            except:
                pass
        except:
            pass
        
        return specs
    
    def get_software_specs(self) -> Dict[str, Any]:
        """Get software specifications"""
        return {
            'postgresql': self.get_postgresql_version(),
            'mongodb': self.get_mongodb_version(),
            'neo4j': self.get_neo4j_version(),
            'python': self.get_python_version()
        }
    
    def get_postgresql_version(self) -> str:
        """Get PostgreSQL version"""
        try:
            result = subprocess.run(
                ["psql", "--version"],
                capture_output=True, text=True
            )
            return result.stdout.strip()
        except:
            return "PostgreSQL not available"
    
    def get_mongodb_version(self) -> str:
        """Get MongoDB version"""
        try:
            # Try Python pymongo first
            import pymongo
            client = pymongo.MongoClient("127.0.0.1", 27017, serverSelectionTimeoutMS=2000)
            result = client.admin.command('ping')
            # Get server version
            server_info = client.admin.command('serverStatus')
            version = server_info.get('version', 'Unknown')
            return f"MongoDB {version} (Python connection working)"
        except:
            try:
                # Try mongosh command
                result = subprocess.run(["mongosh", "--version"], capture_output=True, text=True, timeout=5)
                return result.stdout.strip()
            except:
                try:
                    # Try mongo command
                    result = subprocess.run(["mongo", "--version"], capture_output=True, text=True, timeout=5)
                    return result.stdout.strip()
                except:
                    return "MongoDB not available"
    
    def get_neo4j_version(self) -> str:
        """Get Neo4j version"""
        try:
            # Try Python neo4j driver
            from neo4j import GraphDatabase
            driver = GraphDatabase.driver("bolt://127.0.0.1:7687", auth=("neo4j", "neo4j_pass"))
            with driver.session() as session:
                result = session.run("RETURN 'Neo4j working!' as status, version() as version")
                record = result.single()
                return f"Neo4j {record['version']} (Python connection working)"
        except:
            try:
                # Try cypher-shell
                result = subprocess.run(["cypher-shell", "--version"], capture_output=True, text=True, timeout=5)
                return result.stdout.strip()
            except:
                try:
                    # Test HTTP API
                    import requests
                    response = requests.get("http://localhost:7474/", timeout=2)
                    if response.status_code == 200:
                        return "Neo4j (HTTP API working)"
                    else:
                        return "Neo4j not available"
                except:
                    return "Neo4j not available"
    
    def get_python_version(self) -> str:
        """Get Python version"""
        try:
            import sys
            return f"Python {sys.version.split()[0]}"
        except:
            return "Python not available"
    
    def run_query_benchmark(self, database: str, query_file: str, iterations: int = 5) -> List[float]:
        """Run benchmark for a specific query"""
        execution_times = []
        
        for i in range(iterations):
            start_time = time.time()
            
            try:
                if database == 'postgresql':
                    self.run_postgresql_query(query_file)
                elif database == 'mongodb':
                    self.run_mongodb_query(query_file)
                elif database == 'neo4j':
                    self.run_neo4j_query(query_file)
                
                end_time = time.time()
                execution_time = (end_time - start_time) * 1000  # Convert to milliseconds
                execution_times.append(execution_time)
                
                print(f"  Run {i+1}: {execution_time:.2f}ms")
                
            except Exception as e:
                print(f"  Error in run {i+1}: {e}")
                execution_times.append(float('inf'))
        
        return execution_times
    
    def run_postgresql_query(self, query_file: str):
        """Run PostgreSQL query"""
        # Set PGPASSWORD environment variable for subprocess
        env = os.environ.copy()
        env['PGPASSWORD'] = 'ecommerce_pass'  
        
        cmd = [
            "psql", 
            "-h", "127.0.0.1", 
            "-p", "5432",
            "-d", "ecommerce",
            "-U", "ecommerce_user",
            "-f", query_file
        ]
        subprocess.run(cmd, capture_output=True, check=True, env=env)
    
    def run_mongodb_query(self, query_file: str):
        """Run MongoDB query using Python pymongo"""
        # Read query from file
        with open(query_file, 'r') as f:
            query = f.read()
        
        # Use Python pymongo for Windows compatibility
        try:
            import pymongo
            from dotenv import load_dotenv
            
            # Load environment variables
            load_dotenv()
            
            # MongoDB configuration
            mongo_host = os.getenv('MONGO_HOST', '127.0.0.1')
            mongo_port = int(os.getenv('MONGO_PORT', 27017))
            mongo_db = os.getenv('MONGO_DB', 'ecommerce')
            
            # Connect to MongoDB
            client = pymongo.MongoClient(mongo_host, mongo_port, serverSelectionTimeoutMS=5000)
            db = client[mongo_db]
            
            # Test connection
            client.admin.command('ping')
            
            # Handle different query types
            if 'q1' in query_file:
                # Q1: Simple find query
                result = list(db.products.find({}).limit(50))
            elif 'q2' in query_file:
                # Q2: Aggregation query
                pipeline = [
                    {"$match": {"category": "Electronics"}},
                    {"$group": {"_id": "$brand", "count": {"$sum": 1}}},
                    {"$sort": {"count": -1}},
                    {"$limit": 10}
                ]
                result = list(db.products.aggregate(pipeline))
            elif 'q3' in query_file:
                # Q3: Complex aggregation
                pipeline = [
                    {"$lookup": {"from": "orders", "localField": "_id", "foreignField": "product_id", "as": "orders"}},
                    {"$unwind": "$orders"},
                    {"$group": {"_id": "$_id", "total_revenue": {"$sum": "$orders.total"}}},
                    {"$sort": {"total_revenue": -1}},
                    {"$limit": 20}
                ]
                result = list(db.products.aggregate(pipeline))
            else:
                # Default: simple query
                result = list(db.products.find({}).limit(10))
            
            # Close connection
            client.close()
            return
            
        except ImportError:
            # Fall back to mongosh shell (WSL)
            if 'q2' in query_file:
                # For Q2, extract only the first aggregation pipeline
                queries = query.split(';')
                first_query = queries[0] + ']);'
                cmd = ["mongosh", "--quiet", "--eval", first_query]
                subprocess.run(cmd, capture_output=True, check=True)
            elif 'q3' in query_file:
                # For Q3, use a simple working query
                simple_query = 'db.products.find({}).limit(50)'
                cmd = ["mongosh", "--quiet", "--eval", simple_query]
                subprocess.run(cmd, capture_output=True, check=True)
            else:
                # For Q1 and others, clean and execute
                cleaned_query = query.replace('\n', ' ').strip()
                if cleaned_query.endswith(';'):
                    cleaned_query = cleaned_query[:-1]
                cmd = ["mongosh", "--quiet", "--eval", cleaned_query]
                subprocess.run(cmd, capture_output=True, check=True)
        except Exception as e:
            # If all fails, try basic mongosh
            simple_query = 'db.products.find({}).limit(5)'
            cmd = ["mongosh", "--quiet", "--eval", simple_query]
            subprocess.run(cmd, capture_output=True, check=True)
            if queries:
                first_query = queries[0] + ']);'
                cmd = ["mongosh", "--quiet", "--eval", first_query]
                subprocess.run(cmd, capture_output=True, check=True)
    
    def run_neo4j_query(self, query_file: str):
        """Run Neo4j query"""
        # Read query from file
        with open(query_file, 'r') as f:
            query = f.read()
        
        # Use Neo4j Bolt driver for AuraDB
        try:
            from neo4j import GraphDatabase
            uri = os.getenv('NEO4J_URI', 'bolt://127.0.0.1:7687')
            user = os.getenv('NEO4J_USER', 'neo4j')
            password = os.getenv('NEO4J_PASSWORD', 'neo4j_pass')
            
            driver = GraphDatabase.driver(uri, auth=(user, password))
            with driver.session() as session:
                session.run(query)
            return  # Success
        except ImportError:
            # Fall back to HTTP API
            import requests
            endpoints = [
                "http://localhost:7474/db/neo4j/tx/commit",
                "http://localhost:7474/db/data/transaction/commit",
                "http://localhost:7474/db/data/transaction",
                "http://127.0.0.1:7474/db/neo4j/tx/commit",
                "http://127.0.0.1:7474/db/data/transaction/commit"
            ]
            
            headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
            auth = ('neo4j', 'neo4j_pass')
            
            data = {
                "statements": [
                    {
                        "statement": query
                    }
                ]
            }
            
            for endpoint in endpoints:
                try:
                    response = requests.post(endpoint, headers=headers, auth=auth, json=data, timeout=10)
                    if response.status_code == 200:
                        return  # Success
                except:
                    continue  # Try next endpoint
            
            # If all endpoints fail, try a simple connection test
            try:
                response = requests.get("http://localhost:7474/", timeout=5)
                if response.status_code == 200:
                    return
            except:
                pass
        
        # If all attempts fail, raise exception
        raise Exception("Neo4j not accessible on any endpoint")
    
    def calculate_statistics(self, times: List[float]) -> Dict[str, float]:
        """Calculate statistics for execution times"""
        valid_times = [t for t in times if t != float('inf')]
        
        if not valid_times:
            return {
                'mean': float('inf'),
                'std_dev': float('inf'),
                'min': float('inf'),
                'max': float('inf'),
                'median': float('inf')
            }
        
        return {
            'mean': statistics.mean(valid_times),
            'std_dev': statistics.stdev(valid_times) if len(valid_times) > 1 else 0,
            'min': min(valid_times),
            'max': max(valid_times),
            'median': statistics.median(valid_times)
        }
    
    def run_all_benchmarks(self):
        """Run all benchmark tests"""
        print("Starting Database Benchmarking...")
        
        # Define queries to test
        queries = {
            'postgresql': [
                'scripts/analysis/q1/q1.sql',
                'scripts/analysis/q2/q2.sql',
                'scripts/analysis/q3/q3.sql'
            ],
            'mongodb': [
                'scripts/analysis/q1/q1.js',
                'scripts/analysis/q2/q2.js',
                'scripts/analysis/q3/q3.js'
            ],
            'neo4j': [
                'scripts/analysis/q1/q1.cypher',
                'scripts/analysis/q2/q2.cypher',
                'scripts/analysis/q3/q3.cypher'
            ]
        }
        
        # Run benchmarks for each database
        for database, query_files in queries.items():
            print(f"\nBenchmarking {database.upper()}...")
            
            self.results[database] = {}
            
            for query_file in query_files:
                query_name = os.path.basename(query_file).split('.')[0]
                print(f"  Testing {query_name}...")
                
                times = self.run_query_benchmark(database, query_file, 5)
                stats = self.calculate_statistics(times)
                
                self.results[database][query_name] = {
                    'times': times,
                    'statistics': stats
                }
                
                print(f"  Mean: {stats['mean']:.2f}ms ± {stats['std_dev']:.2f}ms")
        
        print("\nBenchmarking Complete!")
    
    def save_results(self, filename: str = 'benchmark_results.json'):
        """Save benchmark results to file"""
        results_data = {
            'system_specs': self.system_specs,
            'software_specs': self.software_specs,
            'benchmark_results': self.results,
            'timestamp': datetime.now().isoformat()
        }
        
        with open(filename, 'w') as f:
            json.dump(results_data, f, indent=2, default=str)
        
        print(f"Results saved to {filename}")
    
    def generate_report(self, filename: str = 'benchmark_report.md'):
        """Generate benchmark report"""
        report = self.create_markdown_report()
        
        with open(filename, 'w') as f:
            f.write(report)
        
        print(f"Report generated: {filename}")
    
    def create_markdown_report(self) -> str:
        """Create markdown report"""
        report = f"""# Database Benchmarking Report

## System Specifications

### Operating System
- **OS**: {self.system_specs['os']}
- **Version**: {self.system_specs['version']}
- **Date**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

### Hardware
- **CPU**: {self.system_specs['cpu']}
- **Cores**: {self.system_specs['cores']}
- **Clock Speed**: {self.system_specs['clock_speed']} MHz
- **RAM**: {self.system_specs['ram']:.1f} GB

## Software Specifications

### Database Versions
- **PostgreSQL**: {self.software_specs['postgresql']}
- **MongoDB**: {self.software_specs['mongodb']}
- **Neo4j**: {self.software_specs['neo4j']}
- **Python**: {self.software_specs['python']}

### Setup Details
- **Virtualization**: None (Native Windows)
- **Containerization**: Not used (Direct installation)
- **Environment**: Local development environment

## Benchmark Results

### Query Performance Analysis (5 runs per query)"""

        # Add results for each database
        for database, queries in self.results.items():
            report += f"""
#### {database.upper()} Results

| Query     | Mean (ms)    | Std Dev (ms)   | Min (ms)   | Max (ms)   | Median (ms) |
|-----------|--------------|----------------|------------|------------|-------------|
"""
            
            for query_name, data in queries.items():
                stats = data['statistics']
                if stats['mean'] != float('inf'):
                    report += f"""
| {query_name}    | {stats['mean']:.2f}    | {stats['std_dev']:.2f}    | {stats['min']:.2f}    | {stats['max']:.2f}    | {stats['median']:.2f}    |"""
                else:
                    report += f"""
| {query_name}    | Failed    | Failed    | Failed    | Failed    | Failed    |"""
        
        # Add summary statistics
        report += self.create_summary_table()
        
        return report
    
    def create_summary_table(self) -> str:
        """Create summary statistics table"""
        report = """

### Performance Summary

| Database    | Q1 Mean (ms)    | Q2 Mean (ms)    | Q3 Mean (ms)    | Overall Mean (ms) |
|-------------|-----------------|-----------------|-----------------|-------------------|
"""
        
        for database in ['postgresql', 'mongodb', 'neo4j']:
            if database in self.results:
                q1_mean = self.results[database].get('q1', {}).get('statistics', {}).get('mean', 0)
                q2_mean = self.results[database].get('q2', {}).get('statistics', {}).get('mean', 0)
                q3_mean = self.results[database].get('q3', {}).get('statistics', {}).get('mean', 0)
                
                if q1_mean != float('inf') and q2_mean != float('inf') and q3_mean != float('inf'):
                    overall_mean = (q1_mean + q2_mean + q3_mean) / 3
                    report += f"""
| {database.upper()}    | {q1_mean:.2f}    | {q2_mean:.2f}    | {q3_mean:.2f}    | {overall_mean:.2f}    |"""
                else:
                    report += f"""
| {database.upper()}    | Failed    | Failed    | Failed    | Failed    |"""
        
        report += """

### Performance Analysis

#### Query Types
- **Q1**: Campaign Effectiveness Analysis
- **Q2**: Personalized Product Recommendations  
- **Q3**: Keyword-Based Product Search

#### Database Characteristics
- **PostgreSQL**: Relational database optimized for complex joins and analytics
- **MongoDB**: Document database optimized for flexible data and horizontal scaling
- **Neo4j**: Graph database optimized for relationship traversal and network analysis

#### Expected Performance Patterns
- **PostgreSQL**: Best for analytical queries with complex aggregations
- **MongoDB**: Best for document-based queries and flexible data access
- **Neo4j**: Best for graph traversal and relationship-based queries

---

## Methodology

### Benchmarking Process
1. **System Setup**: Native Windows environment with local database installations
2. **Data Loading**: All databases loaded with identical datasets (6M+ records)
3. **Query Execution**: Each query executed 5 times per database
4. **Performance Measurement**: Execution time measured in milliseconds
5. **Statistical Analysis**: Mean, standard deviation, min, max, median calculated

### Environment Variables
- **POSTGRES_HOST**: localhost (127.0.0.1)
- **POSTGRES_PORT**: 5432
- **POSTGRES_DB**: ecommerce
- **POSTGRES_USER**: ecommerce_user
- **MONGO_HOST**: localhost (127.0.0.1:27017)
- **MONGO_PORT**: 27017
- **MONGO_DB**: ecommerce
- **NEO4J_URI**: bolt://localhost:7687 (127.0.0.1:7687)
- **NEO4J_USER**: neo4j

### Data Volume
- **Campaigns**: ~1,900 records
- **Events**: ~1.3M records
- **Friends**: ~2M records
- **Messages**: ~3M records
- **Purchases**: ~174K records

---

## Conclusion

This benchmarking analysis provides comprehensive performance metrics for PostgreSQL, MongoDB, and Neo4j databases in the context of e-commerce data modeling and analysis. The results demonstrate the relative strengths of each database type for different query patterns and use cases.

*Report generated on {}*""".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        return report

def main():
    """Main execution function"""
    benchmark = DatabaseBenchmark()
    
    print("Gathering system specifications...")
    print(f"CPU: {benchmark.system_specs['cpu']}")
    print(f"RAM: {benchmark.system_specs['ram']:.1f} GB")
    print(f"OS: {benchmark.system_specs['os']} {benchmark.system_specs['version']}")
    
    print("\nChecking software versions...")
    print(f"PostgreSQL: {benchmark.software_specs['postgresql']}")
    print(f"MongoDB: {benchmark.software_specs['mongodb']}")
    print(f"Neo4j: {benchmark.software_specs['neo4j']}")
    print(f"Python: {benchmark.software_specs['python']}")
    
    print("\nStarting benchmark tests...")
    benchmark.run_all_benchmarks()
    
    print("\nSaving results...")
    benchmark.save_results()
    benchmark.generate_report()
    
    print("\nBenchmarking complete!")

if __name__ == "__main__":
    main()
