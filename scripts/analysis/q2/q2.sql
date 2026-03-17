-- Personalized Product Recommendations
-- Finds top personalized products for homepage display based on purchase history

-- Query 1: Top Products Analysis
WITH product_popularity AS (
    SELECT 
        p.product_id,
        p.brand,
        p.price,
        c.category_name,
        c.category_code,
        COUNT(DISTINCT ue.user_id) as unique_purchasers,
        COUNT(ue.event_id) as total_purchases,
        AVG(ue.price) as avg_purchase_price
    FROM products p
    JOIN categories c ON p.category_id = c.category_id
    LEFT JOIN user_events ue ON p.product_id = ue.product_id
        AND ue.event_type = 'purchase'
    GROUP BY p.product_id, p.brand, p.price, c.category_name, c.category_code
),
popularity_score AS (
    SELECT 
        pp.*,
        (pp.unique_purchasers * 0.7 + pp.total_purchases * 0.3) as popularity_score
    FROM product_popularity pp
)
SELECT 
    product_id,
    brand,
    price,
    category_name,
    category_code,
    unique_purchasers,
    total_purchases,
    avg_purchase_price,
    popularity_score
FROM popularity_score
ORDER BY popularity_score DESC, avg_purchase_price DESC
LIMIT 50;

-- Query 2: Category-Based Recommendations
WITH user_categories AS (
    SELECT DISTINCT
        ue.user_id,
        c.category_id,
        c.category_name,
        COUNT(DISTINCT ue.product_id) as products_in_category
    FROM user_events ue
    JOIN products p ON ue.product_id = p.product_id
    JOIN categories c ON p.category_id = c.category_id
    WHERE ue.event_type = 'purchase'
    GROUP BY ue.user_id, c.category_id, c.category_name
),
top_categories AS (
    SELECT 
        uc.user_id,
        uc.category_id,
        uc.category_name,
        ROW_NUMBER() OVER (PARTITION BY uc.user_id ORDER BY uc.products_in_category DESC) as category_rank
    FROM user_categories uc
)
SELECT 
    product_id,
    brand,
    price,
    category_name,
    category_code
FROM top_categories tc
JOIN products p ON tc.category_id = p.category_id
WHERE tc.category_rank <= 3
ORDER BY tc.user_id, tc.category_rank, p.price DESC;
