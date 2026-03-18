// MongoDB Shell Script for E-commerce Data Loading
// Aligned with ER diagrams and optimized for performance

// Switch to ecommerce database
db = db.getSiblingDB('ecommerce');

// Clear existing collections
db.categories.drop();
db.products.drop();
db.users.drop();
db.campaigns.drop();
db.events.drop();
db.user_friends.drop();
db.messages.drop();

print("Cleared existing collections...");

// Create categories collection with hierarchical structure
db.categories.insertMany([
    // Categories will be loaded from CSV via Python driver
    // This script focuses on optimization and indexing
]);

// Create indexes for categories
db.categories.createIndex({category_id: 1}, {unique: true});
db.categories.createIndex({category_code: 1}, {unique: true});
db.categories.createIndex({category_level: 1});
db.categories.createIndex({"parent_category": 1}); // For hierarchy

print("Created categories collection with hierarchical indexes...");

// Create products collection with brand relationship
db.products.insertMany([
    // Products will be loaded from CSV via Python driver
]);

// Create indexes for products
db.products.createIndex({product_id: 1}, {unique: true});
db.products.createIndex({category_id: 1});
db.products.createIndex({brand: 1});
db.products.createIndex({price: 1});
db.products.createIndex({"category_hierarchy.level_1": 1, "category_hierarchy.level_2": 1});

print("Created products collection with brand indexes...");

// Create users collection with client relationship
db.users.insertMany([
    // Users will be loaded from CSV via Python driver
]);

// Create indexes for users
db.users.createIndex({user_id: 1}, {unique: true});
db.users.createIndex({client_id: 1});
db.users.createIndex({user_device_id: 1});
db.users.createIndex({first_purchase_date: 1});
db.users.createIndex({"client_id": 1, "user_id": 1}); // User-client relationship

print("Created users collection with client relationship indexes...");

// Create campaigns collection
db.campaigns.insertMany([
    // Campaigns will be loaded from CSV via Python driver
]);

// Create indexes for campaigns
db.campaigns.createIndex({campaign_id: 1}, {unique: true});
db.campaigns.createIndex({campaign_type: 1});
db.campaigns.createIndex({channel: 1});
db.campaigns.createIndex({started_at: 1});
db.campaigns.createIndex({finished_at: 1});

print("Created campaigns collection with indexes...");

// Create events collection with time-series optimized indexes
db.events.insertMany([
    // Events will be loaded from CSV via Python driver
]);

// Create indexes for events (time-series optimization)
db.events.createIndex({event_time: -1});
db.events.createIndex({user_id: 1, event_time: -1});
db.events.createIndex({product_id: 1, event_time: -1});
db.events.createIndex({event_date: -1, event_type: 1});
db.events.createIndex({"user_id": 1, "event_type": 1}); // User-event relationship

print("Created events collection with time-series indexes...");

// Create user_friends collection for social network
db.user_friends.insertMany([
    // User friends will be loaded from CSV via Python driver
]);

// Create indexes for user_friends
db.user_friends.createIndex({user_id: 1});
db.user_friends.createIndex({friend_id: 1});
db.user_friends.createIndex({friendship_date: 1});
db.user_friends.createIndex({user_id: 1, friend_id: 1}, {unique: true}); // Prevent duplicates

print("Created user_friends collection with social network indexes...");

// Create messages collection with campaign relationships
db.messages.insertMany([
    // Messages will be loaded from CSV via Python driver
]);

// Create indexes for messages
db.messages.createIndex({message_id: 1}, {unique: true});
db.messages.createIndex({user_id: 1});
db.messages.createIndex({campaign_id: 1, sent_at: -1});
db.messages.createIndex({sent_at: -1});
db.messages.createIndex({message_type: 1});
db.messages.createIndex({"user_id": 1, "campaign_id": 1}); // User-campaign relationship
db.messages.createIndex({"is_opened": 1, "is_clicked": 1}); // Analytics indexes

print("Created messages collection with campaign relationship indexes...");

// Create aggregation pipeline indexes for analytics
db.events.createIndex({event_type: 1, event_date: 1});
db.messages.createIndex({campaign_id: 1, sent_at: -1});

print("Created aggregation pipeline indexes...");

// Enable sharding for large collections
// sh.shardCollection("events", {event_time: 1});

// Set read preferences for analytics
// db.getMongo().setReadPref("secondaryPreferred");

print("MongoDB schema setup completed!");
print("Collections created with optimized indexes for performance.");
print("Aligned with ER diagram relationships and hierarchies.");
print("Ready for data loading via Python driver...");

// Verify collections
print("\nCollection Statistics:");
print("Categories: " + db.categories.countDocuments() + " documents");
print("Products: " + db.products.countDocuments() + " documents");
print("Users: " + db.users.countDocuments() + " documents");
print("Campaigns: " + db.campaigns.countDocuments() + " documents");
print("Events: " + db.events.countDocuments() + " documents");
print("User Friends: " + db.user_friends.countDocuments() + " documents");
print("Messages: " + db.messages.countDocuments() + " documents");
