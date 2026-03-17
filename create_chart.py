#!/usr/bin/env python3
"""Create performance chart for benchmarking results"""

import matplotlib.pyplot as plt
import json
import numpy as np

def create_performance_chart():
    """Create performance chart similar to reference paper"""
    
    # Load results
    with open('benchmark_results.json', 'r') as f:
        data = json.load(f)
    
    # Extract data
    databases = []
    q1_means = []
    q2_means = []
    q3_means = []
    q1_std = []
    q2_std = []
    q3_std = []
    
    for db_name, db_data in data['benchmark_results'].items():
        databases.append(db_name.upper())
        
        q1_stats = db_data['q1']['statistics']
        q2_stats = db_data['q2']['statistics']
        q3_stats = db_data['q3']['statistics']
        
        q1_means.append(q1_stats['mean'])
        q2_means.append(q2_stats['mean'])
        q3_means.append(q3_stats['mean'])
        
        q1_std.append(q1_stats['std_dev'])
        q2_std.append(q2_stats['std_dev'])
        q3_std.append(q3_stats['std_dev'])
    
    # Create chart
    fig, ax = plt.subplots(figsize=(12, 8))
    
    x = np.arange(len(databases))
    width = 0.25
    
    # Bars with error bars
    bars1 = ax.bar(x - width, q1_means, width, yerr=q1_std, label='Q1 - Campaign Analysis', capsize=5, alpha=0.8)
    bars2 = ax.bar(x, q2_means, width, yerr=q2_std, label='Q2 - Product Recommendations', capsize=5, alpha=0.8)
    bars3 = ax.bar(x + width, q3_means, width, yerr=q3_std, label='Q3 - Keyword Search', capsize=5, alpha=0.8)
    
    # Labels and formatting
    ax.set_xlabel('Database Systems', fontsize=12, fontweight='bold')
    ax.set_ylabel('Execution Time (ms)', fontsize=12, fontweight='bold')
    ax.set_title('Database Query Performance Comparison', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(databases)
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    # Add value labels on bars
    def add_value_labels(bars):
        for bar in bars:
            height = bar.get_height()
            ax.annotate(f'{height:.1f}',
                        xy=(bar.get_x() + bar.get_width() / 2, height),
                        xytext=(0, 3),
                        textcoords="offset points",
                        ha='center', va='bottom', fontsize=8)
    
    add_value_labels(bars1)
    add_value_labels(bars2)
    add_value_labels(bars3)
    
    plt.tight_layout()
    plt.savefig('performance_chart.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    print("Performance chart saved as 'performance_chart.png'")

if __name__ == "__main__":
    create_performance_chart()
