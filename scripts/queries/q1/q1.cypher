// Campaign Effectiveness Analysis - Query 1 Only
MATCH (campaign:Campaign)
WHERE campaign.started_at <= datetime() <= campaign.finished_at
WITH campaign
MATCH (campaign)-[:PART_OF_CAMPAIGN]->(message:Message)
WITH campaign, message
MATCH (message)<-[:RECEIVED]-(user:User)
WITH campaign, message, user
OPTIONAL MATCH (user)-[:EVENT {event_type: 'purchase'}]->(product:Product)
WHERE user.user_id IS NOT NULL
WITH campaign, user, COUNT(product) as purchase_count
WITH campaign, 
    COUNT(DISTINCT user) as engaged_users,
    COUNT(DISTINCT CASE WHEN purchase_count > 0 THEN user END) as purchasing_users,
    SUM(purchase_count) as total_purchases
WITH campaign, engaged_users, purchasing_users, total_purchases,
    CASE 
        WHEN engaged_users > 0 THEN purchasing_users * 100.0 / engaged_users 
        ELSE 0 
    END as conversion_rate
RETURN 
    campaign.campaign_id,
    campaign.campaign_type,
    campaign.channel,
    engaged_users,
    purchasing_users,
    total_purchases,
    conversion_rate
ORDER BY conversion_rate DESC, engaged_users DESC
LIMIT 20;
