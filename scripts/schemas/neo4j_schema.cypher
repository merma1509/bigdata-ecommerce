// Neo4j Schema for E-commerce Data
// Create constraints for uniqueness

// User nodes
CREATE CONSTRAINT user_id_unique FOR (u:User) REQUIRE u.user_id IS UNIQUE;
CREATE CONSTRAINT client_id_unique FOR (u:User) REQUIRE u.client_id IS UNIQUE;

// Product nodes
CREATE CONSTRAINT product_id_unique FOR (p:Product) REQUIRE p.product_id IS UNIQUE;

// Category nodes
CREATE CONSTRAINT category_id_unique FOR (c:Category) REQUIRE c.category_id IS UNIQUE;

// Campaign nodes
CREATE CONSTRAINT campaign_id_unique FOR (ca:Campaign) REQUIRE ca.campaign_id IS UNIQUE;

// Message nodes
CREATE CONSTRAINT message_id_unique FOR (m:Message) REQUIRE m.message_id IS UNIQUE;

// Session nodes
CREATE CONSTRAINT session_id_unique FOR (s:Session) REQUIRE s.session_id IS UNIQUE;

// Create indexes for performance
CREATE INDEX idx_user_device_id FOR (u:User) ON (u.user_device_id);
CREATE INDEX idx_product_brand FOR (p:Product) ON (p.brand);
CREATE INDEX idx_product_price FOR (p:Product) ON (p.price);
CREATE INDEX idx_category_code FOR (c:Category) ON (c.category_code);
CREATE INDEX idx_campaign_type FOR (ca:Campaign) ON (ca.campaign_type);
CREATE INDEX idx_campaign_channel FOR (ca:Campaign) ON (ca.channel);
CREATE INDEX idx_message_opened FOR (m:Message) ON (m.is_opened);
CREATE INDEX idx_message_clicked FOR (m:Message) ON (m.is_clicked);
CREATE INDEX idx_event_time FOR ()-[e:EVENT]-() ON (e.event_time);
CREATE INDEX idx_event_type FOR ()-[e:EVENT]-() ON (e.event_type);

// Sample data structure comments:

// User nodes: (:User {user_id: 123, client_id: "151591562123device", user_device_id: "abc123", created_at: datetime()})

// Product nodes: (:Product {product_id: 456, brand: "Apple", price: 999.99, created_at: datetime()})

// Category nodes: (:Category {category_id: 789, category_code: "electronics.smartphone", category_name: "Smartphones"})

// Campaign nodes: (:Campaign {campaign_id: 1, campaign_type: "bulk", channel: "email", topic: "Summer Sale"})

// Message nodes: (:Message {message_id: 1001, is_opened: true, is_clicked: false, sent_at: datetime()})

// Session nodes: (:Session {session_id: "sess_123", user_id: 123, start_time: datetime()})

// Relationships:
// (:User)-[:EVENT {event_type: "view", price: 999.99, event_time: datetime()}]->(:Product)
// (:User)-[:FRIENDS_WITH {created_at: datetime()}]->(:User)
// (:Product)-[:BELONGS_TO]->(:Category)
// (:User)-[:RECEIVED {is_opened: true}]->(:Message)
// (:Message)-[:PART_OF_CAMPAIGN]->(:Campaign)
// (:User)-[:HAS_SESSION]->(:Session)
