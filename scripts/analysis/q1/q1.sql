-- Campaign Effectiveness Analysis
-- Analyzes if campaigns attracted customers to purchase products
-- Leverages social network information for campaign optimization

-- Query 1: Campaign Conversion Analysis
SELECT 
    c.campaign_id,
    c.campaign_type,
    c.channel,
    c.topic,
    COUNT(DISTINCT ue.user_id) as engaged_users,
    COUNT(DISTINCT CASE WHEN e.event_type = 'purchase' THEN ue.user_id END) as purchasing_users,
    COUNT(DISTINCT CASE WHEN me.is_opened THEN ue.user_id END) as opened_users,
    COUNT(DISTINCT CASE WHEN me.is_clicked THEN ue.user_id END) as clicked_users,
    ROUND(
        COUNT(DISTINCT CASE WHEN e.event_type = 'purchase' THEN ue.user_id END) * 100.0 / 
        NULLIF(COUNT(DISTINCT ue.user_id), 0), 2
    ) as conversion_rate,
    ROUND(
        COUNT(DISTINCT CASE WHEN me.is_opened THEN ue.user_id END) * 100.0 / 
        NULLIF(COUNT(DISTINCT ue.user_id), 0), 2
    ) as open_rate,
    ROUND(
        COUNT(DISTINCT CASE WHEN me.is_clicked THEN ue.user_id END) * 100.0 / 
        NULLIF(COUNT(DISTINCT ue.user_id), 0), 2
    ) as click_rate,
    AVG(CASE WHEN e.event_type = 'purchase' THEN e.price END) as avg_purchase_value
FROM campaigns c
JOIN message_events me ON c.campaign_id = me.campaign_id
JOIN users ue ON me.client_id = ue.client_id
LEFT JOIN user_events e ON ue.user_id = e.user_id 
    AND e.event_time BETWEEN c.started_at AND c.finished_at
    AND e.event_type = 'purchase'
GROUP BY c.campaign_id, c.campaign_type, c.channel, c.topic
ORDER BY conversion_rate DESC, engaged_users DESC;

-- Query 2: Campaign Social Influence Analysis
WITH campaign_engagement AS (
    SELECT 
        c.campaign_id,
        c.campaign_type,
        me.user_id,
        me.is_opened,
        me.is_clicked,
        CASE WHEN e.event_type = 'purchase' THEN 1 ELSE 0 END as made_purchase
    FROM campaigns c
    JOIN message_events me ON c.campaign_id = me.campaign_id
    LEFT JOIN user_events e ON me.user_id = e.user_id 
        AND e.event_time BETWEEN c.started_at AND c.finished_at
        AND e.event_type = 'purchase'
)
SELECT 
    ce.campaign_id,
    ce.campaign_type,
    COUNT(DISTINCT ce.user_id) as directly_engaged,
    COUNT(DISTINCT CASE WHEN ce.made_purchase = 1 THEN ce.user_id END) as direct_purchases,
    COUNT(DISTINCT f.friend_id) as friends_of_engaged,
    COUNT(DISTINCT CASE WHEN f_p.event_type = 'purchase' THEN f.friend_id END) as friend_purchases,
    ROUND(
        COUNT(DISTINCT CASE WHEN f_p.event_type = 'purchase' THEN f.friend_id END) * 100.0 / 
        NULLIF(COUNT(DISTINCT f.friend_id), 0), 2
    ) as friend_influence_rate
FROM campaign_engagement ce
JOIN user_friends f ON ce.user_id = f.user_id
LEFT JOIN user_events f_p ON f.friend_id = f_p.user_id 
    AND f_p.event_time BETWEEN (SELECT MIN(c.started_at) FROM campaigns WHERE campaign_id = ce.campaign_id)
    AND (SELECT MAX(c.finished_at) FROM campaigns WHERE campaign_id = ce.campaign_id)
    AND f_p.event_type = 'purchase'
GROUP BY ce.campaign_id, ce.campaign_type
ORDER BY friend_influence_rate DESC, direct_purchases DESC;
