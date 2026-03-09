-- Initialize PostgreSQL database
-- Create database extensions and basic setup

-- Enable UUID extension for generating unique IDs
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create the main database if it doesn't exist
-- This will be handled by POSTGRES_DB environment variable

-- Set timezone to UTC for consistency
SET timezone = 'UTC';

-- Create indexes for better query performance
-- These will be created after data loading
