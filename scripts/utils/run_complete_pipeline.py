#!/usr/bin/env python3
"""Complete Data Pipeline Orchestrator
Cleans raw data and loads into PostgreSQL, MongoDB, and Neo4j"""

import os
import sys
import subprocess
import time
from pathlib import Path
from datetime import datetime

class DataPipelineOrchestrator:
    def __init__(self):
        self.project_root = Path(__file__).parent.parent
        self.scripts_dir = self.project_root / "scripts"
        self.data_dir = self.project_root / "data"
        self.raw_dir = self.data_dir / "raw"
        self.processed_dir = self.data_dir / "processed"
        
        # Ensure processed directory exists
        self.processed_dir.mkdir(exist_ok=True)
        
        print("=" * 80)
        print("BIG DATA STORAGE & RETRIEVAL")
        print("=" * 80)
        print(f"Project Root: {self.project_root}")
        print(f"Raw Data Dir: {self.raw_dir}")
        print(f"Processed Data Dir: {self.processed_dir}")
        print("=" * 80)
    
    def check_prerequisites(self):
        """Check if all prerequisites are met"""
        print("\nCHECKING PREREQUISITES...")
        
        # Check if raw data exists
        required_files = [
            "campaigns.csv",
            "events.csv", 
            "friends.csv",
            "messages.csv",
            "client_first_purchase_date.csv"
        ]
        
        missing_files = []
        for file in required_files:
            file_path = self.raw_dir / file
            if not file_path.exists():
                missing_files.append(str(file_path))
        
        if missing_files:
            print(f"MISSING RAW DATA FILES:")
            for file in missing_files:
                print(f"   - {file}")
            print("\nPlease ensure all raw CSV files are in the data/raw/ directory.")
            return False
        
        print("All raw data files found!")
        
        # Check if Python is available
        try:
            result = subprocess.run([sys.executable, "--version"], capture_output=True, text=True)
            print(f"Python: {result.stdout.strip()}")
        except Exception as e:
            print(f"Python not available: {e}")
            return False
        
        # Check if required Python packages are available
        required_packages = ['pandas', 'psycopg2', 'pymongo', 'neo4j']
        missing_packages = []
        
        for package in required_packages:
            try:
                # Special handling for psycopg2 vs psycopg2-binary
                if package == 'psycopg2':
                    __import__('psycopg2')  # Try psycopg2 first
                else:
                    __import__(package)
                print(f"{package} available")
            except ImportError:
                # For psycopg2, try psycopg2-binary as fallback
                if package == 'psycopg2':
                    try:
                        __import__('psycopg2_binary')
                        print(f"{package} available (via psycopg2-binary)")
                    except ImportError:
                        missing_packages.append(package)
                        print(f"{package} NOT available")
                else:
                    missing_packages.append(package)
                    print(f"{package} NOT available")
        
        if missing_packages:
            print(f"\nMISSING PACKAGES: {', '.join(missing_packages)}")
            print("Install with: pip install " + " ".join(missing_packages))
            return False
        
        return True
    
    def run_data_cleaning(self):
        """Run data cleaning script"""
        print("\nSTEP 1: DATA CLEANING")
        print("-" * 50)
        
        clean_script = self.scripts_dir / "data" / "clean_data.py"
        
        if not clean_script.exists():
            print(f"Data cleaning script not found: {clean_script}")
            return False
        
        try:
            start_time = time.time()
            print(f"Running: {clean_script}")
            
            # Change to project root directory for relative paths
            result = subprocess.run(
                [sys.executable, str(clean_script)],
                cwd=self.project_root,
                capture_output=True,
                text=True
            )
            
            if result.returncode != 0:
                print(f"Data cleaning failed!")
                print(f"Error: {result.stderr}")
                return False
            
            end_time = time.time()
            duration = end_time - start_time
            
            print("Data cleaning completed successfully!")
            print(f" Duration: {duration:.2f} seconds")
            print(f"Cleaned data saved to: {self.processed_dir}")
            
            # List cleaned files
            cleaned_files = list(self.processed_dir.glob("*_cleaned.csv"))
            print(f"Generated {len(cleaned_files)} cleaned files:")
            for file in cleaned_files:
                size = file.stat().st_size / 1024 / 1024  # MB
                print(f"   - {file.name} ({size:.1f} MB)")
            
            return True
            
        except Exception as e:
            print(f"Error running data cleaning: {e}")
            return False
    
    def run_database_loading(self, db_name, script_name):
        """Run database loading script"""
        print(f"\nSTEP {self.db_step_counter}: LOADING {db_name.upper()}")
        print("-" * 50)
        
        load_script = self.scripts_dir / "loading" / script_name
        
        if not load_script.exists():
            print(f"{db_name} loading script not found: {load_script}")
            return False
        
        try:
            start_time = time.time()
            print(f"Running: {load_script}")
            
            # Change to project root directory for relative paths
            result = subprocess.run(
                [sys.executable, str(load_script)],
                cwd=self.project_root,
                capture_output=True,
                text=True
            )
            
            if result.returncode != 0:
                print(f"{db_name} loading failed!")
                print(f"Error: {result.stderr}")
                return False
            
            end_time = time.time()
            duration = end_time - start_time
            
            print(f"{db_name} loading completed successfully!")
            print(f"Duration: {duration:.2f} seconds")
            
            # Show output summary if available
            if result.stdout:
                print("Loading Summary:")
                # Extract summary lines from output
                lines = result.stdout.split('\n')
                summary_lines = [line for line in lines if 'records' in line.lower() or 'documents' in line.lower() or 'nodes' in line.lower()]
                for line in summary_lines[-10:]:  # Show last 10 summary lines
                    if line.strip():
                        print(f"   {line.strip()}")
            
            return True
            
        except Exception as e:
            print(f"Error running {db_name} loading: {e}")
            return False
    
    def run_all_database_loading(self):
        """Run all database loading scripts"""
        print("\nSTEP 2-4: DATABASE LOADING")
        print("=" * 50)
        
        self.db_step_counter = 2
        
        databases = [
            ("PostgreSQL", "load_data_psql.py"),
            ("MongoDB", "load_data_mongodb.py"), 
            ("Neo4j", "load_data_graph.py")
        ]
        
        results = {}
        for db_name, script_name in databases:
            print(f"\nLoading {db_name}...")
            results[db_name] = self.run_database_loading(db_name, script_name)
            self.db_step_counter += 1
            
            # Brief pause between databases
            time.sleep(2)
        
        return results
    
    def generate_final_report(self, results):
        """Generate final pipeline report"""
        print("\n" + "=" * 80)
        print("FINAL PIPELINE REPORT")
        print("=" * 80)
        
        # Data cleaning status
        print("DATA CLEANING:")
        print("   COMPLETED - All datasets cleaned and optimized")
        
        # Database loading status
        print("\nDATABASE LOADING:")
        for db_name, success in results.items():
            status = "COMPLETED" if success else "FAILED"
            print(f"   {db_name:<12} {status}")
        
        # Summary statistics
        print("\nSUMMARY STATISTICS:")
        try:
            cleaned_files = list(self.processed_dir.glob("*_cleaned.csv"))
            total_size = sum(f.stat().st_size for f in cleaned_files) / 1024 / 1024  # MB
            print(f"   Processed Files: {len(cleaned_files)}")
            print(f"   Total Size: {total_size:.1f} MB")
            
            # Count successful databases
            successful = sum(1 for success in results.values() if success)
            print(f"   Databases Loaded: {successful}/3")
            
        except Exception as e:
            print(f"   Could not calculate statistics: {e}")
        
        # Next steps
        print("\nNEXT STEPS:")
        print("   1. Verify data in each database using provided query tools")
        print("   2. Run performance benchmarks: cd scripts && ./run_benchmark.sh")
        print("   3. Review data models in OPTIMAL_DATA_MODELS_REPORT.md")
        print("   4. Check IMPLEMENTATION_SUMMARY.md for deployment guide")
        
        # Overall status
        all_success = all(results.values())
        if all_success:
            print("\nPIPELINE COMPLETED SUCCESSFULLY!")
            print("   All data cleaned and loaded into PostgreSQL, MongoDB, and Neo4j")
        else:
            print("\nPIPELINE COMPLETED WITH WARNINGS!")
            print("   Some database loading failed. Check error messages above.")
        
        print("=" * 80)
        
        return all_success
    
    def run_complete_pipeline(self):
        """Run the complete data pipeline"""
        start_time = time.time()
        
        try:
            # Step 1: Check prerequisites
            if not self.check_prerequisites():
                print("\nPIPELINE ABORTED: Prerequisites not met!")
                return False
            
            # Step 2: Run data cleaning
            if not self.run_data_cleaning():
                print("\nPIPELINE ABORTED: Data cleaning failed!")
                return False
            
            # Step 3-5: Run database loading
            results = self.run_all_database_loading()
            
            # Step 6: Generate final report
            end_time = time.time()
            total_duration = end_time - start_time
            
            success = self.generate_final_report(results)
            
            print(f"\nTOTAL PIPELINE DURATION: {total_duration:.2f} seconds ({total_duration/60:.1f} minutes)")
            
            return success
            
        except KeyboardInterrupt:
            print("\n\nPIPELINE INTERRUPTED BY USER!")
            return False
        except Exception as e:
            print(f"\nPIPELINE FAILED WITH ERROR: {e}")
            return False

def main():
    """Main function"""
    orchestrator = DataPipelineOrchestrator()
    
    print("Starting Complete Data Pipeline...")
    print("This will:")
    print("  1. Clean all raw data files")
    print("  2. Load cleaned data into PostgreSQL")
    print("  3. Load cleaned data into MongoDB") 
    print("  4. Load cleaned data into Neo4j")
    print("\nPress Ctrl+C to interrupt...")
    
    # Wait a moment for user to read
    time.sleep(3)
    
    # Run the complete pipeline
    success = orchestrator.run_complete_pipeline()
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
