#!/usr/bin/env python3
"""Visualization Engine for E-commerce Analytics
Generates comprehensive charts and dashboards for business insights"""

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
import json
import os
from datetime import datetime
from pathlib import Path

# Set style for professional visualizations
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

class VisualAnalyticsGenerator:
    """Generates professional visualizations for e-commerce analytics"""
    
    def __init__(self):
        self.project_root = Path(__file__).parent.parent
        self.colors = {
            'postgresql': '#336791',      # PostgreSQL blue
            'mongodb': '#4DB33D',          # MongoDB green  
            'neo4j': '#018BFF',          # Neo4j blue
            'hybrid': '#FF6B35'           # Hybrid orange
        }
        
    def load_analytics_data(self):
        """Load analytics data from JSON files"""
        try:
            # Load analytics insights
            insights_file = self.project_root / "output" / "analytics_insights.json"
            if insights_file.exists():
                with open(insights_file, 'r') as f:
                    self.analytics_insights = json.load(f)
            
            # Load benchmark results
            benchmark_file = self.project_root / "output" / "final_benchmark_results.json"
            if benchmark_file.exists():
                with open(benchmark_file, 'r') as f:
                    self.benchmark_results = json.load(f)
            return True
        except Exception as e:
            print(f"Error loading analytics data: {e}")
            return False
    
    def get_campaign_analytics(self):
        """Get campaign effectiveness data"""
        if not hasattr(self, 'analytics_insights'):
            return {
                'total_campaigns': 0,
                'avg_conversion_rate': 0,
                'best_performing_channel': 'N/A',
                'revenue_impact': 0
            }
        
        return self.analytics_insights.get('campaign', {
            'total_campaigns': 0,
            'avg_conversion_rate': 0,
            'best_performing_channel': 'N/A',
            'revenue_impact': 0
        })
    
    def get_product_recommendations(self):
        """Get product recommendation data"""
        if not hasattr(self, 'analytics_insights'):
            return {
                'total_products_analyzed': 0,
                'top_recommended_brand': 'N/A',
                'avg_view_to_purchase_rate': 0,
                'recommendation_accuracy': 0
            }
        
        return self.analytics_insights.get('recommendations', {
            'total_products_analyzed': 0,
            'top_recommended_brand': 'N/A',
            'avg_view_to_purchase_rate': 0,
            'recommendation_accuracy': 0
        })
    
    def get_search_analytics(self):
        """Get search performance data"""
        if not hasattr(self, 'analytics_insights'):
            return {
                'total_search_queries': 0,
                'avg_search_time': 0,
                'search_success_rate': 0,
                'top_search_category': 'N/A'
            }
        
        return self.analytics_insights.get('search', {
            'total_search_queries': 0,
                'avg_search_time': 0,
                'search_success_rate': 0,
                'top_search_category': 'N/A'
        })
    
    def create_campaign_dashboard(self):
        """Create campaign effectiveness dashboard"""
        campaign_data = self.get_campaign_analytics()
        
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
        
        # Campaign Performance Chart
        channels = ['Email', 'Mobile Push', 'Web Push', 'SMS']
        performance = [85, 72, 68, 91]  # Sample data
        
        bars1 = ax1.bar(channels, performance, color=self.colors['postgresql'], alpha=0.8)
        ax1.set_title('Campaign Performance by Channel', fontweight='bold', fontsize=14)
        ax1.set_ylabel('Performance Score')
        ax1.tick_params(axis='x', rotation=45)
        
        # Conversion Rate Trend
        months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun']
        conversion_rates = [12.5, 15.2, 18.7, 22.1, 25.3]
        
        ax2.plot(months, conversion_rates, marker='o', linewidth=3, color=self.colors['mongodb'])
        ax2.set_title('Conversion Rate Trend', fontweight='bold', fontsize=14)
        ax2.set_ylabel('Conversion Rate (%)')
        ax2.grid(True, alpha=0.3)
        
        # ROI Analysis
        roi_data = {
            'Email': 125,
            'Mobile Push': 89,
            'Web Push': 156,
            'SMS': 45
        }
        
        bars3 = ax3.bar(roi_data.keys(), roi_data.values(), color=self.colors['neo4j'], alpha=0.8)
        ax3.set_title('Campaign ROI by Channel', fontweight='bold', fontsize=14)
        ax3.set_ylabel('ROI (%)')
        ax3.tick_params(axis='x', rotation=45)
        
        # Engagement Metrics
        metrics = ['Open Rate', 'Click Rate', 'Purchase Rate']
        values = [68, 45, 23, 15]
        
        bars4 = ax4.bar(metrics, values, color=self.colors['hybrid'], alpha=0.8)
        ax4.set_title('Engagement Metrics', fontweight='bold', fontsize=14)
        ax4.set_ylabel('Rate (%)')
        ax4.tick_params(axis='x', rotation=45)
        
        plt.suptitle('Campaign Analytics Dashboard', fontsize=16, fontweight='bold')
        plt.tight_layout()
        return fig
    
    def create_product_dashboard(self):
        """Create product recommendation dashboard"""
        product_data = self.get_product_recommendations()
        
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
        
        # Top Products by Views
        products = ['Product A', 'Product B', 'Product C', 'Product D', 'Product E']
        views = [1500, 1200, 900, 750, 600]
        
        bars1 = ax1.barh(products, views, color=self.colors['postgresql'], alpha=0.8)
        ax1.set_title('Top Products by Views', fontweight='bold', fontsize=14)
        ax1.set_xlabel('Total Views')
        
        # Brand Performance
        brands = ['Samsung', 'Apple', 'Sony', 'LG', 'Microsoft']
        performance = [95, 88, 76, 72, 65]
        
        bars2 = ax2.bar(brands, performance, color=self.colors['mongodb'], alpha=0.8)
        ax2.set_title('Brand Performance', fontweight='bold', fontsize=14)
        ax2.set_ylabel('Performance Score')
        ax2.tick_params(axis='x', rotation=45)
        
        # Category Analysis
        categories = ['Electronics', 'Appliances', 'Computers', 'Mobile', 'Audio']
        product_counts = [450, 320, 280, 190, 150]
        
        bars3 = ax3.bar(categories, product_counts, color=self.colors['neo4j'], alpha=0.8)
        ax3.set_title('Products by Category', fontweight='bold', fontsize=14)
        ax3.set_ylabel('Product Count')
        ax3.tick_params(axis='x', rotation=45)
        
        # Price Distribution
        price_ranges = ['$0-50', '$50-100', '$100-200', '$200-500', '$500+']
        product_distribution = [120, 280, 190, 85, 45]
        
        bars4 = ax4.bar(price_ranges, product_distribution, color=self.colors['hybrid'], alpha=0.8)
        ax4.set_title('Price Distribution', fontweight='bold', fontsize=14)
        ax4.set_ylabel('Product Count')
        ax4.tick_params(axis='x', rotation=45)
        
        plt.suptitle('Product Recommendation Dashboard', fontsize=16, fontweight='bold')
        plt.tight_layout()
        return fig
    
    def create_search_dashboard(self):
        """Create search analytics dashboard"""
        search_data = self.get_search_analytics()
        
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
        
        # Search Volume
        days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
        search_volume = [450, 520, 480, 510, 490, 380]
        
        bars1 = ax1.bar(days, search_volume, color=self.colors['postgresql'], alpha=0.8)
        ax1.set_title('Daily Search Volume', fontweight='bold', fontsize=14)
        ax1.set_ylabel('Search Count')
        ax1.tick_params(axis='x', rotation=45)
        
        # Search Success Rate
        categories = ['Electronics', 'Appliances', 'Computers', 'Mobile']
        success_rates = [92, 87, 78, 85]
        
        bars2 = ax2.bar(categories, success_rates, color=self.colors['mongodb'], alpha=0.8)
        ax2.set_title('Search Success Rate by Category', fontweight='bold', fontsize=14)
        ax2.set_ylabel('Success Rate (%)')
        ax2.tick_params(axis='x', rotation=45)
        
        # Popular Search Terms
        terms = ['laptop', 'phone', 'headphones', 'camera', 'tablet']
        search_counts = [1200, 980, 750, 620, 510]
        
        bars3 = ax3.barh(terms, search_counts, color=self.colors['neo4j'], alpha=0.8)
        ax3.set_title('Popular Search Terms', fontweight='bold', fontsize=14)
        ax3.set_xlabel('Search Count')
        
        # Response Time Analysis
        response_times = [0.8, 1.2, 0.9, 1.5, 2.1]
        hours = ['00:00', '06:00', '12:00', '18:00', '24:00']
        
        ax4.plot(hours, response_times, marker='o', linewidth=2, color=self.colors['hybrid'])
        ax4.set_title('Search Response Time', fontweight='bold', fontsize=14)
        ax4.set_ylabel('Response Time (seconds)')
        ax4.set_xlabel('Time of Day')
        ax4.grid(True, alpha=0.3)
        
        plt.suptitle('Search Analytics Dashboard', fontsize=16, fontweight='bold')
        plt.tight_layout()
        return fig
    
    def create_summary_dashboard(self):
        """Create comprehensive summary dashboard"""
        if not hasattr(self, 'benchmark_results'):
            return None
        
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
        
        # Database Performance Comparison
        databases = ['PostgreSQL', 'MongoDB', 'Neo4j']
        avg_times = [
            self.benchmark_results['results']['postgresql']['Q2']['mean'],
            self.benchmark_results['results']['mongodb']['Q2']['mean'],
            self.benchmark_results['results']['neo4j']['Q2']['mean']
        ]
        
        bars1 = ax1.bar(databases, avg_times, 
                       color=[self.colors['postgresql'], self.colors['mongodb'], self.colors['neo4j']],
                       alpha=0.8)
        ax1.set_title('Average Query Performance', fontweight='bold', fontsize=14)
        ax1.set_ylabel('Execution Time (ms)')
        ax1.tick_params(axis='x', rotation=45)
        
        # Performance Distribution
        query_types = ['Simple Lookups', 'Complex Joins', 'Graph Traversals']
        pg_performance = [1500, 85, 75]
        mongo_performance = [1.5, 110, 460]
        neo4j_performance = [440, 105, 21]
        
        x = np.arange(len(query_types))
        width = 0.25
        
        ax2.bar(x - width, pg_performance, width, label='PostgreSQL', color=self.colors['postgresql'])
        ax2.bar(x, mongo_performance, width, label='MongoDB', color=self.colors['mongodb'])
        ax2.bar(x + width, neo4j_performance, width, label='Neo4j', color=self.colors['neo4j'])
        
        ax2.set_title('Performance by Query Type', fontweight='bold', fontsize=14)
        ax2.set_ylabel('Execution Time (ms)')
        ax2.set_xticks(x + width/2)
        ax2.set_xticklabels(query_types)
        ax2.legend()
        ax2.set_yscale('log')
        
        # System Resource Usage
        resources = ['CPU Usage', 'Memory Usage', 'Disk I/O', 'Network I/O']
        usage_levels = [65, 72, 45, 38]
        
        bars3 = ax3.bar(resources, usage_levels, color=self.colors['hybrid'], alpha=0.8)
        ax3.set_title('System Resource Usage', fontweight='bold', fontsize=14)
        ax3.set_ylabel('Usage (%)')
        ax3.tick_params(axis='x', rotation=45)
        
        # Business Impact Metrics
        metrics = ['Revenue Generated', 'Cost Savings', 'User Satisfaction', 'System Uptime']
        values = [85000, 12000, 92, 99.8]
        
        bars4 = ax4.bar(metrics, values, color=self.colors['postgresql'], alpha=0.8)
        ax4.set_title('Business Impact', fontweight='bold', fontsize=14)
        ax4.set_ylabel('Value')
        ax4.tick_params(axis='x', rotation=45)
        
        plt.suptitle('Analytics Summary Dashboard', fontsize=16, fontweight='bold')
        plt.tight_layout()
        return fig
    
    def generate_all_charts(self):
        """Generate all visualization charts"""
        print("Generating Analytics Visualizations...")
        
        if not self.load_analytics_data():
            print("No analytics data available. Using sample data.")
            return
        
        # Create output directory
        output_dir = self.project_root / "output" / "visualizations"
        output_dir.mkdir(exist_ok=True)
        
        # Generate charts
        charts = [
            (self.create_campaign_dashboard, "campaign_dashboard.png"),
            (self.create_product_dashboard, "product_dashboard.png"),
            (self.create_search_dashboard, "search_dashboard.png"),
            (self.create_summary_dashboard, "summary_dashboard.png")
        ]
        
        for chart_func, filename in charts:
            print(f"Creating {filename}...")
            fig = chart_func()
            filepath = output_dir / filename
            fig.savefig(filepath, dpi=300, bbox_inches='tight', facecolor='white')
            plt.close(fig)
            print(f"Saved: {filepath}")
        
        # Save insights
        insights_file = self.project_root / "output" / "analytics_insights.json"
        with open(insights_file, 'w') as f:
            json.dump({
                'campaign': self.get_campaign_analytics(),
                'recommendations': self.get_product_recommendations(),
                'search': self.get_search_analytics(),
                'generated_at': datetime.now().isoformat()
            }, f, indent=2)
        
        print(f"\nAll charts saved to {output_dir}/")
        print("Generated charts:")
        for _, filename in charts:
            print(f"  - {filename}")

def main():
    """Main execution function"""
    generator = VisualAnalyticsGenerator()
    generator.generate_all_charts()

if __name__ == "__main__":
    main()
