-- PostgreSQL Database Schema for E-commerce Data
-- Optimized for performance and scalability

-- Drop existing tables and recreate
DROP TABLE IF EXISTS messages CASCADE;
DROP TABLE IF EXISTS user_friends CASCADE;
DROP TABLE IF EXISTS events CASCADE;
DROP TABLE IF EXISTS campaigns CASCADE;
DROP TABLE IF EXISTS users CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS categories CASCADE;

-- Categories table
CREATE TABLE categories (
    category_id BIGINT PRIMARY KEY,
    category_code VARCHAR(255) NOT NULL,
    category_name VARCHAR(255),
    category_level INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) WITH (fillfactor=100);

-- Products table with brand relationship
CREATE TABLE products (
    product_id BIGINT PRIMARY KEY,
    category_id BIGINT,
    brand VARCHAR(255),
    price DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) WITH (fillfactor=100);

-- Users table with proper client relationship
CREATE TABLE users (
    user_id TEXT PRIMARY KEY,
    client_id TEXT,
    user_device_id TEXT,
    first_purchase_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) WITH (fillfactor=100);

-- Campaigns table
CREATE TABLE campaigns (
    campaign_id BIGINT PRIMARY KEY,
    campaign_type VARCHAR(100),
    channel VARCHAR(100),
    topic VARCHAR(255),
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    total_count BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) WITH (fillfactor=100);

-- Events table with optimized indexing for time-series data
CREATE TABLE events (
    event_id BIGSERIAL PRIMARY KEY,
    event_time TIMESTAMP NOT NULL,
    event_type VARCHAR(100),
    user_id TEXT NOT NULL,
    product_id BIGINT NOT NULL,
    category_id TEXT,
    price DECIMAL(20,2),
    user_session VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (event_time, user_id, product_id, event_type)
) WITH (fillfactor=100);

-- User Friends relationship table
CREATE TABLE user_friends (
    user_id TEXT NOT NULL,
    friend_id TEXT NOT NULL,
    friendship_date DATE DEFAULT CURRENT_DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, friend_id)
) WITH (fillfactor=100);

-- Messages table for campaign interactions
CREATE TABLE messages (
    message_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    message_uuid TEXT UNIQUE NOT NULL,
    user_id TEXT NOT NULL,
    campaign_id BIGINT NOT NULL,
    message_type VARCHAR(100),
    channel VARCHAR(100),
    date DATE,
    sent_at TIMESTAMP,
    is_opened BOOLEAN DEFAULT FALSE,
    is_clicked BOOLEAN DEFAULT FALSE,
    is_purchased BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) WITH (fillfactor=100);

-- Create indexes for optimal performance

-- Categories indexes
CREATE INDEX idx_categories_code ON categories(category_code);
CREATE INDEX idx_categories_level ON categories(category_level);

-- Products indexes
CREATE INDEX idx_products_category ON products(category_id);
CREATE INDEX idx_products_brand ON products(brand);
CREATE INDEX idx_products_price ON products(price);

-- Users indexes
CREATE INDEX idx_users_client ON users(client_id);
CREATE INDEX idx_users_device ON users(user_device_id);
CREATE INDEX idx_users_purchase_date ON users(first_purchase_date);

-- Campaigns indexes
CREATE INDEX idx_campaigns_type ON campaigns(campaign_type);
CREATE INDEX idx_campaigns_channel ON campaigns(channel);
CREATE INDEX idx_campaigns_dates ON campaigns(started_at, finished_at);

-- Events indexes (time-series optimized)
CREATE INDEX idx_events_time ON events(event_time DESC);
CREATE INDEX idx_events_user_time ON events(user_id, event_time DESC);
CREATE INDEX idx_events_product_time ON events(product_id, event_time DESC);
CREATE INDEX idx_events_type_date ON events(event_type, DATE(event_time));

-- User Friends indexes
CREATE INDEX idx_user_friends_user ON user_friends(user_id);
CREATE INDEX idx_user_friends_friend ON user_friends(friend_id);
CREATE INDEX idx_user_friends_date ON user_friends(friendship_date);

-- Messages indexes
CREATE INDEX idx_messages_user ON messages(user_id);
CREATE INDEX idx_messages_campaign ON messages(campaign_id);
CREATE INDEX idx_messages_date ON messages(sent_at DESC);
CREATE INDEX idx_messages_type ON messages(message_type);

-- Create foreign key constraints for data integrity
ALTER TABLE products ADD CONSTRAINT fk_products_category 
    FOREIGN KEY (category_id) REFERENCES categories(category_id);

ALTER TABLE events ADD CONSTRAINT fk_events_user 
    FOREIGN KEY (user_id) REFERENCES users(user_id);

ALTER TABLE events ADD CONSTRAINT fk_events_product 
    FOREIGN KEY (product_id) REFERENCES products(product_id);

ALTER TABLE events ADD CONSTRAINT fk_events_category 
    FOREIGN KEY (category_id) REFERENCES categories(category_id);

ALTER TABLE user_friends ADD CONSTRAINT fk_user_friends_user 
    FOREIGN KEY (user_id) REFERENCES users(user_id);

ALTER TABLE user_friends ADD CONSTRAINT fk_user_friends_friend 
    FOREIGN KEY (friend_id) REFERENCES users(user_id);

ALTER TABLE messages ADD CONSTRAINT fk_messages_user 
    FOREIGN KEY (user_id) REFERENCES users(user_id);

ALTER TABLE messages ADD CONSTRAINT fk_messages_campaign 
    FOREIGN KEY (campaign_id) REFERENCES campaigns(campaign_id);

-- Grant permissions to the application user
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO ecommerce_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO ecommerce_user;

-- Analyze tables for optimal query planning
ANALYZE categories;
ANALYZE products;
ANALYZE users;
ANALYZE campaigns;
ANALYZE events;
ANALYZE user_friends;
ANALYZE messages;
