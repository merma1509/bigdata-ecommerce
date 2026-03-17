#!/usr/bin/env python3
"""
Updated Benchmarking Infrastructure for Docker-based E-commerce Data Modeling Project
Handles database connections, query execution, and performance measurement with Docker
"""

import time
import subprocess
import json
import statistics
import os
import sys
from datetime import datetime
from typing import Dict, List, Any

class DockerDatabaseBenchmark:
    """Handles benchmarking operations for Docker-based databases"""
    
    def __init__(self):
        self.results = {
            'postgresql': {},
            'mongodb': {},
            'neo4j': {}
        }
        self.system_specs = self.get_system_specs()
        self.software_specs = self.get_software_specs()
        self.docker_specs = self.get_docker_specs()
        
        # Load Docker environment variables
        self.load_docker_env()
        
    def load_docker_env(self):
        """Load Docker environment variables"""
        try:
            with open('.env.docker', 'r') as f:
                for line in f:
                    if line.strip() and not line.startswith('#'):
                        key, value = line.strip().split('=', 1)
                        os.environ[key] = value
        except FileNotFoundError:
            print("⚠️ .env.docker file not found, using defaults")
    
    def get_system_specs(self) -> Dict[str, Any]:
        """Get system specifications"""
        try:
            # Get system info using PowerShell
            cpu_info = subprocess.run(
                ["powershell", "Get-WmiObject -Class Win32_Processor | Select-Object Name, NumberOfCores, MaxClockSpeed | ConvertTo-Json"],
                capture_output=True, text=True
            )
            
            memory_info = subprocess.run(
                ["powershell", "Get-WmiObject -Class Win32_ComputerSystem | Select-Object TotalPhysicalMemory | ConvertTo-Json"],
                capture_output=True, text=True
            )
            
            os_info = subprocess.run(
                ["powershell", "Get-ComputerInfo | Select-Object WindowsProductName, WindowsVersion | ConvertTo-Json"],
                capture_output=True, text=True
            )
            
            return {
                'cpu': json.loads(cpu_info.stdout.strip()),
                'memory': json.loads(memory_info.stdout.strip()),
                'os': json.loads(os_info.stdout.strip()),
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            print(f"Error getting system specs: {e}")
            return {
                'cpu': {'Name': 'Unknown', 'NumberOfCores': 0, 'MaxClockSpeed': 0},
                'memory': {'TotalPhysicalMemory': 0},
                'os': {'WindowsProductName': 'Unknown', 'WindowsVersion': 'Unknown'},
                'timestamp': datetime.now().isoformat()
            }
    
    def get_docker_specs(self) -> Dict[str, Any]:
        """Get Docker specifications"""
        try:
            docker_info = subprocess.run(
                ["docker", "version", "--format", "json"],
                capture_output=True, text=True
            )
            
            docker_compose_info = subprocess.run(
                ["docker-compose", "version"],
                capture_output=True, text=True
            )
            
            return {
                'docker_version': json.loads(docker_info.stdout.strip()),
                'docker_compose_version': docker_compose_info.stdout.strip(),
                'containers': self.get_container_info(),
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            print(f"Error getting Docker specs: {e}")
            return {
                'docker_version': {'Version': 'Unknown'},
                'docker_compose_version': 'Unknown',
                'containers': {},
                'timestamp': datetime.now().isoformat()
            }
    
    def get_container_info(self) -> Dict[str, Any]:
        """Get running container information"""
        containers = {}
        try:
            result = subprocess.run(
                ["docker-compose", "ps", "--format", "json"],
                capture_output=True, text=True
            )
            
            if result.stdout.strip():
                container_list = json.loads(result.stdout.strip())
                for container in container_list:
                    containers[container['Service']] = {
                        'name': container['Name'],
                        'status': container['State'],
                        'ports': container['Publishers']
                    }
        except Exception as e:
            print(f"Error getting container info: {e}")
        
        return containers
    
    def get_software_specs(self) -> Dict[str, Any]:
        """Get software specifications (Docker-based)"""
        return {
            'postgresql': self.get_docker_postgresql_version(),
            'mongodb': self.get_docker_mongodb_version(),
            'neo4j': self.get_docker_neo4j_version(),
            'python': self.get_python_version(),
            'docker': self.get_docker_version(),
            'timestamp': datetime.now().isoformat()
        }
    
    def get_docker_postgresql_version(self) -> str:
        """Get PostgreSQL version from Docker container"""
        try:
            result = subprocess.run(
                ["docker-compose", "exec", "-T", "postgres", "psql", "--version"],
                capture_output=True, text=True
            )
            return result.stdout.strip()
        except:
            return "PostgreSQL container not running"
    
    def get_docker_mongodb_version(self) -> str:
        """Get MongoDB version from Docker container"""
        try:
            result = subprocess.run(
                ["docker-compose", "exec", "-T", "mongodb", "mongosh", "--eval", "db.version()"],
                capture_output=True, text=True
            )
            return f"MongoDB {result.stdout.strip()}"
        except:
            return "MongoDB container not running"
    
    def get_docker_neo4j_version(self) -> str:
        """Get Neo4j version from Docker container"""
        try:
            result = subprocess.run(
                ["docker-compose", "exec", "-T", "neo4j", "cypher-shell", "-u", "neo4j", "-p", "neo4j_pass", "CALL dbms.components() YIELD name, versions RETURN name, versions[0] as version"],
                capture_output=True, text=True
            )
            return f"Neo4j {result.stdout.strip()}"
        except:
            return "Neo4j container not running"
    
    def get_docker_version(self) -> str:
        """Get Docker version"""
        try:
            result = subprocess.run(
                ["docker", "--version"],
                capture_output=True, text=True
            )
            return result.stdout.strip()
        except:
            return "Docker not available"
    
    def get_python_version(self) -> str:
        """Get Python version"""
        try:
            result = subprocess.run(
                ["python", "--version"],
                capture_output=True, text=True
            )
            return result.stdout.strip()
        except:
            return "Python not available"
    
    def check_docker_services(self) -> bool:
        """Check if all Docker services are running"""
        try:
            result = subprocess.run(
                ["docker-compose", "ps"],
                capture_output=True, text=True
            )
            
            services = ['postgres', 'mongodb', 'neo4j']
            for service in services:
                if service not in result.stdout:
                    print(f"❌ {service} service not found")
                    return False
                if "Up" not in result.stdout.split(service)[1].split('\n')[0]:
                    print(f"❌ {service} service not running")
                    return False
            
            print("✅ All Docker services are running")
            return True
        except Exception as e:
            print(f"❌ Error checking Docker services: {e}")
            return False
    
    def run_query_benchmark(self, database: str, query_file: str, iterations: int = 5) -> List[float]:
        """Run benchmark for a specific query"""
        execution_times = []
        
        for i in range(iterations):
            start_time = time.time()
            
            try:
                if database == 'postgresql':
                    self.run_docker_postgresql_query(query_file)
                elif database == 'mongodb':
                    self.run_docker_mongodb_query(query_file)
                elif database == 'neo4j':
                    self.run_docker_neo4j_query(query_file)
                
                end_time = time.time()
                execution_time = (end_time - start_time) * 1000  # Convert to milliseconds
                execution_times.append(execution_time)
                
                print(f"  Run {i+1}: {execution_time:.2f}ms")
                
            except Exception as e:
                print(f"  Error in run {i+1}: {e}")
                execution_times.append(float('inf'))
        
        return execution_times
    
    def run_docker_postgresql_query(self, query_file: str):
        """Run PostgreSQL query in Docker container"""
        cmd = [
            "docker-compose", "exec", "-T", "postgres", 
            "psql", "-U", "ecommerce_user", "-d", "ecommerce", "-f", query_file
        ]
        subprocess.run(cmd, capture_output=True, check=True)
    
    def run_docker_mongodb_query(self, query_file: str):
        """Run MongoDB query in Docker container"""
        cmd = [
            "docker-compose", "exec", "-T", "mongodb", 
            "mongosh", "--quiet", "--file", query_file
        ]
        subprocess.run(cmd, capture_output=True, check=True)
    
    def run_docker_neo4j_query(self, query_file: str):
        """Run Neo4j query in Docker container"""
        cmd = [
            "docker-compose", "exec", "-T", "neo4j", 
            "cypher-shell", "-u", "neo4j", "-p", "neo4j_pass", "-f", query_file
        ]
        subprocess.run(cmd, capture_output=True, check=True)
    
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
        print("🚀 Starting Docker Database Benchmarking...")
        
        # Check if Docker services are running
        if not self.check_docker_services():
            print("❌ Docker services are not running. Please start them first:")
            print("   docker-compose up -d")
            return
        
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
            print(f"\n📊 Benchmarking {database.upper()}...")
            
            self.results[database] = {}
            
            for query_file in query_files:
                query_name = os.path.basename(query_file).split('.')[0]
                print(f"  🎯 Testing {query_name}...")
                
                times = self.run_query_benchmark(database, query_file, 5)
                stats = self.calculate_statistics(times)
                
                self.results[database][query_name] = {
                    'times': times,
                    'statistics': stats
                }
                
                print(f"    Mean: {stats['mean']:.2f}ms ± {stats['std_dev']:.2f}ms")
        
        print("\n✅ Docker Benchmarking Complete!")
    
    def save_results(self, filename: str = 'docker_benchmark_results.json'):
        """Save benchmark results to file"""
        results_data = {
            'system_specs': self.system_specs,
            'docker_specs': self.docker_specs,
            'software_specs': self.software_specs,
            'benchmark_results': self.results,
            'timestamp': datetime.now().isoformat()
        }
        
        with open(filename, 'w') as f:
            json.dump(results_data, f, indent=2, default=str)
        
        print(f"📄 Docker results saved to {filename}")
    
    def generate_report(self, filename: str = 'docker_benchmark_report.md'):
        """Generate Docker benchmark report"""
        report = self.create_docker_markdown_report()
        
        with open(filename, 'w') as f:
            f.write(report)
        
        print(f"📄 Docker report generated: {filename}")
    
    def create_docker_markdown_report(self) -> str:
        """Create Docker-specific markdown report"""
        report = f"""# **Docker Database Benchmarking Report**

## **🖥️ System Specifications**

### **Operating System**
- **OS**: {self.system_specs['os']['WindowsProductName']}
- **Version**: {self.system_specs['os']['WindowsVersion']}
- **Date**: {self.system_specs['timestamp']}

### **Hardware**
- **CPU**: {self.system_specs['cpu']['Name']}
- **Cores**: {self.system_specs['cpu']['NumberOfCores']}
- **Clock Speed**: {self.system_specs['cpu']['MaxClockSpeed']} MHz
- **RAM**: {self.system_specs['memory']['TotalPhysicalMemory'] / (1024**3):.1f} GB

## **🐳 Docker Specifications**

### **Docker Environment**
- **Docker Version**: {self.docker_specs['docker_version'].get('Version', 'Unknown')}
- **Docker Compose Version**: {self.docker_specs['docker_compose_version']}
- **Setup Date**: {self.docker_specs['timestamp']}

### **Container Configuration**
- **PostgreSQL**: postgres:16 (Port: 5432)
- **MongoDB**: mongo:7.0 (Port: 27017)
- **Neo4j**: neo4j:5.15-community (Ports: 7474, 7687)

## **🔧 Software Specifications**

### **Database Versions (Docker-based)**
- **PostgreSQL**: {self.software_specs['postgresql']}
- **MongoDB**: {self.software_specs['mongodb']}
- **Neo4j**: {self.software_specs['neo4j']}
- **Python**: {self.software_specs['python']}

### **Setup Details**
- **Virtualization**: Docker containers
- **Containerization**: Docker Compose orchestration
- **Environment**: Isolated database environment
- **Network**: Docker bridge network

## **📊 Benchmark Results**

### **Query Performance Analysis (5 runs per query)**"""

        # Add results for each database
        for database, queries in self.results.items():
            report += f"""
#### **{database.upper()} Results**

| Query | Mean (ms) | Std Dev (ms) | Min (ms) | Max (ms) | Median (ms) |
|-------|-----------|-------------|----------|----------|-------------|"""
            
            for query_name, data in queries.items():
                stats = data['statistics']
                if stats['mean'] != float('inf'):
                    report += f"""
| {query_name} | {stats['mean']:.2f} | {stats['std_dev']:.2f} | {stats['min']:.2f} | {stats['max']:.2f} | {stats['median']:.2f} |"""
                else:
                    report += f"""
| {query_name} | Failed | Failed | Failed | Failed | Failed |"""
        
        # Add Docker-specific analysis
        report += f"""

### **🐳 Docker Performance Analysis**

#### **Container Advantages**
- **Consistent Environment**: All databases in identical containers
- **Resource Isolation**: Clean separation of database processes
- **Easy Reproducibility**: Same setup across different machines
- **Portability**: Easy to share and deploy

#### **Performance Considerations**
- **Container Overhead**: Minimal performance impact from containers
- **Resource Limits**: Docker memory/CPU constraints
- **Network Latency**: Container-to-host communication
- **Storage Performance**: Docker volume performance

## **📈 Performance Summary**

| Database | Q1 Mean (ms) | Q2 Mean (ms) | Q3 Mean (ms) | Overall Mean (ms) |
|----------|---------------|---------------|---------------|------------------|"""
        
        for database in ['postgresql', 'mongodb', 'neo4j']:
            if database in self.results:
                q1_mean = self.results[database].get('q1', {}).get('statistics', {}).get('mean', 0)
                q2_mean = self.results[database].get('q2', {}).get('statistics', {}).get('mean', 0)
                q3_mean = self.results[database].get('q3', {}).get('statistics', {}).get('mean', 0)
                
                if q1_mean != float('inf') and q2_mean != float('inf') and q3_mean != float('inf'):
                    overall_mean = (q1_mean + q2_mean + q3_mean) / 3
                    report += f"""
| {database.upper()} | {q1_mean:.2f} | {q2_mean:.2f} | {q3_mean:.2f} | {overall_mean:.2f} |"""
                else:
                    report += f"""
| {database.upper()} | Failed | Failed | Failed | Failed |"""
        
        report += """

## **📋 Docker Methodology**

### **Container Setup**
1. **Docker Compose**: Multi-container orchestration
2. **Volume Mapping**: Data persistence and script access
3. **Network Configuration**: Bridge network for inter-container communication
4. **Health Checks**: Automated service readiness verification

### **Benchmarking Process**
1. **Container Startup**: All databases started simultaneously
2. **Service Verification**: Health checks confirm database readiness
3. **Query Execution**: 5 iterations per query per database
4. **Performance Measurement**: Execution time measured in milliseconds
5. **Statistical Analysis**: Mean, standard deviation, and percentiles

### **Data Volume**
- **Campaigns**: ~1,900 records
- **Events**: ~1.3M records
- **Friends**: ~2M records
- **Messages**: ~3M records
- **Purchases**: ~174K records

---

## **🎉 Docker Benchmarking Conclusion**

This Docker-based benchmarking approach provides consistent, reproducible database performance testing across PostgreSQL, MongoDB, and Neo4j. The containerized environment ensures identical testing conditions while maintaining the flexibility to test different database configurations.

*Report generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*
"""
        return report

def main():
    """Main execution function"""
    benchmark = DockerDatabaseBenchmark()
    
    print("🔍 Gathering system specifications...")
    print(f"CPU: {benchmark.system_specs['cpu']['Name']}")
    print(f"RAM: {benchmark.system_specs['memory']['TotalPhysicalMemory'] / (1024**3):.1f} GB")
    print(f"OS: {benchmark.system_specs['os']['WindowsProductName']}")
    
    print("\n🐳 Checking Docker environment...")
    print(f"Docker: {benchmark.software_specs['docker']}")
    print(f"PostgreSQL: {benchmark.software_specs['postgresql']}")
    print(f"MongoDB: {benchmark.software_specs['mongodb']}")
    print(f"Neo4j: {benchmark.software_specs['neo4j']}")
    print(f"Python: {benchmark.software_specs['python']}")
    
    print("\n🚀 Starting Docker benchmark tests...")
    benchmark.run_all_benchmarks()
    
    print("\n📄 Saving results...")
    benchmark.save_results()
    benchmark.generate_report()
    
    print("\n✅ Docker Benchmarking complete!")

if __name__ == "__main__":
    main()
