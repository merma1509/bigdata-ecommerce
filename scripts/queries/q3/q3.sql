-- Keyword-Based Product Search
-- Retrieves products by keywords from category_code using full-text search

-- Create full-text search index (run once)
-- CREATE INDEX IF NOT EXISTS idx_product_fulltext ON products USING gin(to_tsvector('english', brand || ' ' || category_name || ' ' || category_code));

-- Query 1: Full-text Product Search
WITH search_terms AS (
    SELECT unnest(string_to_array(lower($keywords))) as term
),
product_search AS (
    SELECT 
        p.product_id,
        p.brand,
        p.price,
        c.category_name,
        c.category_code,
        -- Text relevance scoring
        CASE 
            WHEN to_tsvector('english', p.brand || ' ' || c.category_name || ' ' || c.category_code) @@ to_tsquery('english', st.term) THEN 3
            WHEN lower(p.brand) LIKE '%' || st.term || '%' THEN 2
            WHEN lower(c.category_name) LIKE '%' || st.term || '%' THEN 2
            WHEN lower(c.category_code) LIKE '%' || st.term || '%' THEN 2
            ELSE 0
        END as relevance_score
    FROM products p
    JOIN categories c ON p.category_id = c.category_id
    CROSS JOIN search_terms st
WHERE 
        to_tsvector('english', p.brand || ' ' || c.category_name || ' ' || c.category_code) @@ to_tsquery('english', st.term) OR
        lower(p.brand) LIKE '%' || st.term || '%' OR
        lower(c.category_name) LIKE '%' || st.term || '%' OR
        lower(c.category_code) LIKE '%' || st.term || '%'
)
SELECT 
    product_id,
    brand,
    price,
    category_name,
    category_code,
    MAX(relevance_score) as max_relevance_score,
    COUNT(*) as term_matches
FROM product_search
GROUP BY product_id, brand, price, category_name, category_code
HAVING MAX(relevance_score) > 0
ORDER BY max_relevance_score DESC, term_matches DESC, price ASC
LIMIT 50;

-- Query 2: Category-Based Keyword Search
WITH search_terms AS (
    SELECT unnest(string_to_array(lower($keywords))) as term
),
category_matches AS (
    SELECT 
        c.category_id,
        c.category_name,
        c.category_code,
        -- Category relevance scoring
        CASE 
            WHEN to_tsvector('english', c.category_name || ' ' || c.category_code) @@ to_tsquery('english', st.term) THEN 3
            WHEN lower(c.category_name) LIKE '%' || st.term || '%' THEN 2
            WHEN lower(c.category_code) LIKE '%' || st.term || '%' THEN 2
            ELSE 0
        END as relevance_score
    FROM categories c
    CROSS JOIN search_terms st
WHERE 
        to_tsvector('english', c.category_name || ' ' || c.category_code) @@ to_tsquery('english', st.term) OR
        lower(c.category_name) LIKE '%' || st.term || '%' OR
        lower(c.category_code) LIKE '%' || st.term || '%'
)
SELECT 
    cm.category_id,
    cm.category_name,
    cm.category_code,
    MAX(cm.relevance_score) as max_relevance_score
FROM category_matches cm
GROUP BY cm.category_id, cm.category_name, cm.category_code
HAVING MAX(cm.relevance_score) > 0
ORDER BY max_relevance_score DESC
LIMIT 20;
