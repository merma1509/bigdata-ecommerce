-- E-commerce PostgreSQL Schema

-- Categories table
CREATE TABLE categories (
    category_id BIGINT PRIMARY KEY,
    category_code VARCHAR(255),
    category_name VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Products table with inline foreign key
CREATE TABLE products (
    product_id BIGINT PRIMARY KEY,
    category_id BIGINT,
    brand VARCHAR(255),
    price DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (category_id) REFERENCES categories(category_id)
);

-- Users table
CREATE TABLE users (
    user_id BIGINT PRIMARY KEY,
    user_device_id VARCHAR(255),
    client_id VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Campaigns table
CREATE TABLE campaigns (
    id INTEGER PRIMARY KEY,
    campaign_type VARCHAR(50),
    channel VARCHAR(50),
    topic VARCHAR(255),
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- User events table with inline foreign keys
CREATE TABLE user_events (
    event_id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    product_id BIGINT,
    event_type VARCHAR(50) NOT NULL,
    price DECIMAL(10,2),
    user_session VARCHAR(255),
    event_time TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- User friends table with inline foreign keys
CREATE TABLE user_friends (
    friendship_id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    friend_id BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY (friend_id) REFERENCES users(user_id)
);

-- Message events table with inline foreign keys
CREATE TABLE message_events (
    message_id BIGINT PRIMARY KEY,
    campaign_id INTEGER,
    user_id BIGINT NOT NULL,
    category VARCHAR(255),
    platform VARCHAR(255),
    is_opened BOOLEAN DEFAULT FALSE,
    is_clicked BOOLEAN DEFAULT FALSE,
    is_purchased BOOLEAN DEFAULT FALSE,
    sent_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (campaign_id) REFERENCES campaigns(id),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

-- Campaign performance table with inline foreign key
CREATE TABLE campaign_performance (
    performance_id BIGINT PRIMARY KEY,
    campaign_id INTEGER NOT NULL,
    sent_count INTEGER DEFAULT 0,
    opened_count INTEGER DEFAULT 0,
    clicked_count INTEGER DEFAULT 0,
    purchased_count INTEGER DEFAULT 0,
    open_rate DECIMAL(5,4) DEFAULT 0.0000,
    click_rate DECIMAL(5,4) DEFAULT 0.0000,
    purchase_rate DECIMAL(5,4) DEFAULT 0.0000,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (campaign_id) REFERENCES campaigns(id)
);

-- Create indexes for performance optimization
CREATE INDEX idx_categories_category_code ON categories(category_code);
CREATE INDEX idx_products_category_id ON products(category_id);
CREATE INDEX idx_products_brand ON products(brand);
CREATE INDEX idx_products_price ON products(price);
CREATE INDEX idx_users_client_id ON users(client_id);
CREATE INDEX idx_users_device_id ON users(user_device_id);
CREATE INDEX idx_campaigns_type ON campaigns(campaign_type);
CREATE INDEX idx_campaigns_channel ON campaigns(channel);
CREATE INDEX idx_user_events_user_id ON user_events(user_id);
CREATE INDEX idx_user_events_product_id ON user_events(product_id);
CREATE INDEX idx_user_events_type ON user_events(event_type);
CREATE INDEX idx_user_events_time ON user_events(event_time);
CREATE INDEX idx_user_friends_user_id ON user_friends(user_id);
CREATE INDEX idx_user_friends_friend_id ON user_friends(friend_id);
CREATE INDEX idx_user_friends_unique ON user_friends(user_id, friend_id);
CREATE INDEX idx_message_events_campaign_id ON message_events(campaign_id);
CREATE INDEX idx_message_events_user_id ON message_events(user_id);
CREATE INDEX idx_message_events_opened ON message_events(is_opened);
CREATE INDEX idx_message_events_clicked ON message_events(is_clicked);
CREATE INDEX idx_campaign_performance_campaign_id ON campaign_performance(campaign_id);
