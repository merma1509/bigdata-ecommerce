#!/usr/bin/env python3
"""Data Cleaning Script

This script cleans and preprocesses the raw CSV data files:
- events.csv
- campaigns.csv  
- messages.csv
- friends.csv
- client_first_purchase_date.csv

It handles missing values, data type conversions, and prepares
the data for loading into different database systems"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataCleaner:
    """Data cleaning and preprocessing utilities"""
    
    def __init__(self, raw_data_path: str, processed_data_path: str):
        self.raw_data_path = Path(raw_data_path)
        self.processed_data_path = Path(processed_data_path)
        self.processed_data_path.mkdir(parents=True, exist_ok=True)
        
    def clean_events_data(self):
        """Clean events.csv data"""
        logger.info("Cleaning events data...")
        
        # Load events data
        df = pd.read_csv(self.raw_data_path / "events.csv")
        
        # TODO: Implement data cleaning logic
        # - Handle missing values
        # - Convert data types
        # - Parse timestamps
        # - Remove duplicates
        
        logger.info(f"Events data shape: {df.shape}")
        
        # Save cleaned data
        output_path = self.processed_data_path / "events_cleaned.csv"
        df.to_csv(output_path, index=False)
        logger.info(f"Saved cleaned events data to {output_path}")
        
        return df
    
    def clean_campaigns_data(self):
        """Clean campaigns.csv data"""
        logger.info("Cleaning campaigns data...")
        
        df = pd.read_csv(self.raw_data_path / "campaigns.csv")
        
        # TODO: Implement data cleaning logic
        # - Handle missing values
        # - Convert data types  
        # - Parse timestamps
        # - Clean boolean fields
        
        logger.info(f"Campaigns data shape: {df.shape}")
        
        output_path = self.processed_data_path / "campaigns_cleaned.csv"
        df.to_csv(output_path, index=False)
        logger.info(f"Saved cleaned campaigns data to {output_path}")
        
        return df
    
    def clean_messages_data(self):
        """Clean messages.csv data"""
        logger.info("Cleaning messages data...")
        
        df = pd.read_csv(self.raw_data_path / "messages.csv")
        
        # TODO: Implement data cleaning logic
        # - Handle missing values
        # - Convert data types
        # - Parse timestamps
        # - Clean boolean fields
        
        logger.info(f"Messages data shape: {df.shape}")
        
        output_path = self.processed_data_path / "messages_cleaned.csv"
        df.to_csv(output_path, index=False)
        logger.info(f"Saved cleaned messages data to {output_path}")
        
        return df
    
    def clean_friends_data(self):
        """Clean friends.csv data"""
        logger.info("Cleaning friends data...")
        
        df = pd.read_csv(self.raw_data_path / "friends.csv")
        
        # TODO: Implement data cleaning logic
        # - Handle missing values
        # - Remove duplicates
        # - Ensure mutual friendships
        
        logger.info(f"Friends data shape: {df.shape}")
        
        output_path = self.processed_data_path / "friends_cleaned.csv"
        df.to_csv(output_path, index=False)
        logger.info(f"Saved cleaned friends data to {output_path}")
        
        return df
    
    def clean_client_purchase_data(self):
        """Clean client_first_purchase_date.csv data"""
        logger.info("Cleaning client first purchase data...")
        
        df = pd.read_csv(self.raw_data_path / "client_first_purchase_date.csv")
        
        # TODO: Implement data cleaning logic
        # - Handle missing values
        # - Parse dates
        # - Extract user_id from client_id
        
        logger.info(f"Client purchase data shape: {df.shape}")
        
        output_path = self.processed_data_path / "client_first_purchase_date_cleaned.csv"
        df.to_csv(output_path, index=False)
        logger.info(f"Saved cleaned client purchase data to {output_path}")
        
        return df
    
    def clean_all_data(self):
        """Clean all datasets"""
        logger.info("Starting data cleaning process...")
        
        # Clean all datasets
        events_df = self.clean_events_data()
        campaigns_df = self.clean_campaigns_data()
        messages_df = self.clean_messages_data()
        friends_df = self.clean_friends_data()
        client_df = self.clean_client_purchase_data()
        
        logger.info("Data cleaning completed successfully!")
        
        return {
            'events': events_df,
            'campaigns': campaigns_df,
            'messages': messages_df,
            'friends': friends_df,
            'client_purchase': client_df
        }

def main():
    """Main execution function"""
    # Define paths
    raw_data_path = "../data/raw"
    processed_data_path = "../data/processed"
    
    # Initialize cleaner
    cleaner = DataCleaner(raw_data_path, processed_data_path)
    
    # Clean all data
    cleaned_data = cleaner.clean_all_data()
    
    # Print summary statistics
    print("\n=== Data Cleaning Summary ===")
    for name, df in cleaned_data.items():
        print(f"{name}: {df.shape[0]:,} rows, {df.shape[1]} columns")
    
    print("\nData cleaning completed!")

if __name__ == "__main__":
    main()
