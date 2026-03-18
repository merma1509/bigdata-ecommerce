// Keyword-Based Product Search
// Retrieves products by keywords from category_code using full-text search

// Create text search index (run once)
// db.products.createIndex({ 
//     "brand": "text", 
//     "category_name": "text", 
//     "category_code": "text" 
// });

// Query 1: Full-text Product Search
var keywords = ["electronics", "smartphone", "laptop"]; // Pass as parameter

db.products.find({
    $or: [
        { $text: { $search: keywords } },
        { brand: { $regex: new RegExp(keywords.join("|"), "i") } },
        { category_name: { $regex: new RegExp(keywords.join("|"), "i") } },
        { category_code: { $regex: new RegExp(keywords.join("|"), "i") } }
    ]
}).project({
    product_id: 1,
    brand: 1,
    price: 1,
    category_name: 1,
    category_code: 1,
    // Calculate relevance score
    relevance_score: {
        $add: [
            { $cond: [{ $gte: [{ $meta: "textScore" }, 0] }, 3, 0] },
            { $cond: [{ $regexMatch: [{ $input: "$brand" }, new RegExp(keywords.join("|"))] }, 2, 0] },
            { $cond: [{ $regexMatch: [{ $input: "$category_name" }, new RegExp(keywords.join("|"))] }, 2, 0] },
            { $cond: [{ $regexMatch: [{ $input: "$category_code" }, new RegExp(keywords.join("|"))] }, 2, 0] }
        ]
    }
}).sort({ 
    relevance_score: -1, 
    price: 1 
}).limit(50);

// Query 2: Category-Based Keyword Search
db.categories.aggregate([
    {
        $match: {
            $or: [
                { $text: { $search: keywords } },
                { category_name: { $regex: new RegExp(keywords.join("|"), "i") } },
                { category_code: { $regex: new RegExp(keywords.join("|"), "i") } }
            ]
        }
    },
    {
        $addFields: {
            relevance_score: {
                $add: [
                    { $cond: [{ $gte: [{ $meta: "textScore" }, 0] }, 3, 0] },
                    { $cond: [{ $regexMatch: [{ $input: "$category_name" }, new RegExp(keywords.join("|"))] }, 2, 0] },
                    { $cond: [{ $regexMatch: [{ $input: "$category_code" }, new RegExp(keywords.join("|"))] }, 2, 0] }
                ]
            }
        }
    }
    },
    {
        $sort: { relevance_score: -1 }
    },
    { $limit: 20 }
]);

// Query 3: Products from Matching Categories
var matching_categories = db.categories.aggregate([
    {
        $match: {
            $or: [
                { $text: { $search: keywords } },
                { category_name: { $regex: new RegExp(keywords.join("|"), "i") } },
                { category_code: { $regex: new RegExp(keywords.join("|"), "i") } }
            ]
        }
    },
    { $limit: 20 }
]);

// Get products from matching categories
var categoryIds = matching_categories.map(function(c) { return c._id; });
db.products.find({
    category_id: { $in: categoryIds }
}).sort({ price: 1 }).limit(50);
