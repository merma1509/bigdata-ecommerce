// Keyword-Based Product Search - Query 1 Only
MATCH (p:Product)
WHERE 
    toLower(p.brand) CONTAINS toLower('electronics') OR
    toLower(p.category_name) CONTAINS toLower('electronics') OR
    toLower(p.category_code) CONTAINS toLower('electronics')
OPTIONAL MATCH (p)-[:BELONGS_TO]->(c:Category)
RETURN 
    p.product_id,
    p.brand,
    p.price,
    p.category_name,
    c.category_code,
    // Relevance scoring
    CASE 
        WHEN toLower(p.brand) CONTAINS toLower('electronics') THEN 3
        WHEN toLower(p.category_name) CONTAINS toLower('electronics') THEN 2
        WHEN toLower(p.category_code) CONTAINS toLower('electronics') THEN 2
        ELSE 0
    END as relevance_score
ORDER BY relevance_score DESC, p.price ASC
LIMIT 50;
