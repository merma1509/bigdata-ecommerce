// Personalized Product Recommendations - Fixed Q2
MATCH (user:User {user_id: 'user123'})
OPTIONAL MATCH (user)-[:EVENT {event_type: 'purchase'}]->(purchased:Product)
WITH user, COLLECT(purchased) as user_purchases

// Find similar users based on purchase patterns
MATCH (similar_user:User)-[:EVENT {event_type: 'purchase'}]->(product:Product)
WHERE similar_user <> user AND product IN user_purchases
WITH user, user_purchases, similar_user, COUNT(product) as common_products
ORDER BY common_products DESC
LIMIT 50

// Recommend products purchased by similar users but not by current user
MATCH (similar_user)-[:EVENT {event_type: 'purchase'}]->(recommended_product:Product)
WHERE NOT (user)-[:EVENT {event_type: 'purchase'}]->(recommended_product)
AND NOT recommended_product IN user_purchases
WITH user, recommended_product, similar_user, COUNT(DISTINCT similar_user) as recommendation_strength
OPTIONAL MATCH (recommended_product)-[:BELONGS_TO]->(category:Category)
RETURN 
    recommended_product.product_id,
    recommended_product.brand,
    recommended_product.price,
    category.category_name,
    category.category_code,
    recommendation_strength,
    COLLECT(DISTINCT similar_user.user_id)[0..4] as recommended_by_users
ORDER BY recommendation_strength DESC, recommended_product.price DESC
LIMIT 20;
