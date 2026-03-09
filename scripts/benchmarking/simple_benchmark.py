#!/usr/bin/env python3
"""
Simple benchmarking utility for Python 3.10 compatibility
Replaces hyperfine which has version conflicts
"""

import time
import statistics
import subprocess
import sys
from typing import List, Dict, Any

class SimpleBenchmark:
    """Simple benchmarking tool for Python 3.10"""
    
    def __init__(self):
        self.results = []
    
    def run_command(self, command: str, warmup_runs: int = 1, benchmark_runs: int = 5) -> Dict[str, Any]:
        """
        Run a command and benchmark it
        
        Args:
            command: Command to run
            warmup_runs: Number of warmup runs
            benchmark_runs: Number of benchmark runs
            
        Returns:
            Dictionary with benchmark results
        """
        print(f"Benchmarking: {command}")
        print(f"Warmup runs: {warmup_runs}, Benchmark runs: {benchmark_runs}")
        
        # Warmup runs
        for i in range(warmup_runs):
            try:
                subprocess.run(command, shell=True, capture_output=True, check=True)
            except subprocess.CalledProcessError as e:
                print(f"Warmup run {i+1} failed: {e}")
                return {"error": str(e)}
        
        # Benchmark runs
        times = []
        for i in range(benchmark_runs):
            try:
                start_time = time.perf_counter()
                result = subprocess.run(command, shell=True, capture_output=True, text=True, check=True)
                end_time = time.perf_counter()
                execution_time = end_time - start_time
                times.append(execution_time)
                print(f"Run {i+1}: {execution_time:.4f}s")
            except subprocess.CalledProcessError as e:
                print(f"Benchmark run {i+1} failed: {e}")
                return {"error": str(e)}
        
        # Calculate statistics
        if times:
            avg_time = statistics.mean(times)
            std_dev = statistics.stdev(times) if len(times) > 1 else 0
            min_time = min(times)
            max_time = max(times)
            
            results = {
                "command": command,
                "runs": benchmark_runs,
                "times": times,
                "average": avg_time,
                "std_dev": std_dev,
                "min": min_time,
                "max": max_time,
                "unit": "seconds"
            }
            
            print(f"\nResults:")
            print(f"Average: {avg_time:.4f}s ± {std_dev:.4f}s")
            print(f"Min: {min_time:.4f}s")
            print(f"Max: {max_time:.4f}s")
            
            self.results.append(results)
            return results
        else:
            return {"error": "No successful runs"}
    
    def compare_commands(self, commands: List[str], warmup_runs: int = 1, benchmark_runs: int = 5) -> Dict[str, Any]:
        """
        Compare multiple commands
        
        Args:
            commands: List of commands to compare
            warmup_runs: Number of warmup runs
            benchmark_runs: Number of benchmark runs
            
        Returns:
            Dictionary with comparison results
        """
        print(f"\nComparing {len(commands)} commands...")
        
        comparison_results = {}
        for command in commands:
            result = self.run_command(command, warmup_runs, benchmark_runs)
            if "error" not in result:
                comparison_results[command] = result["average"]
        
        if comparison_results:
            # Sort by performance
            sorted_results = sorted(comparison_results.items(), key=lambda x: x[1])
            
            print(f"\nComparison Results (sorted by performance):")
            for i, (cmd, avg_time) in enumerate(sorted_results, 1):
                print(f"{i}. {cmd}: {avg_time:.4f}s")
            
            return {
                "comparison": sorted_results,
                "fastest": sorted_results[0][0],
                "slowest": sorted_results[-1][0],
                "speedup": sorted_results[-1][1] / sorted_results[0][1] if len(sorted_results) > 1 else 1.0
            }
        else:
            return {"error": "No successful benchmarks"}
    
    def save_results(self, filename: str = "benchmark_results.json"):
        """Save benchmark results to JSON file"""
        import json
        
        with open(filename, 'w') as f:
            json.dump(self.results, f, indent=2)
        print(f"Results saved to {filename}")

if __name__ == "__main__":
    benchmark = SimpleBenchmark()
    
    # Example usage
    if len(sys.argv) > 1:
        command = " ".join(sys.argv[1:])
        benchmark.run_command(command)
    else:
        print("Usage: python simple_benchmark.py '<command>'")
        print("Example: python simple_benchmark.py 'python scripts/load_data_psql.py'")
