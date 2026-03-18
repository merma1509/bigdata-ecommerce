// Personalized Product Recommendations
// Finds top personalized products for homepage display based on purchase history

// Query 1: Top Products with User History
db.events.aggregate([
    {
        $match: { "event_type": "purchase" }
    },
    {
        $group: {
            _id: "$product_id",
            unique_purchasers: { $addToSet: "$user_id" },
            total_purchases: { $sum: 1 },
            avg_price: { $avg: "$price" }
        }
    },
    {
        $addFields: {
            purchaser_count: { $size: "$unique_purchasers" }
        }
    },
    {
        $lookup: {
            from: "products",
            localField: "_id",
            foreignField: "product_id",
            as: "product_info"
        }
    },
    { $unwind: "$product_info" },
    {
        $lookup: {
            from: "categories",
            localField: "product_info.category_id",
            foreignField: "category_id",
            as: "category_info"
        }
    },
    { $unwind: "$category_info" },
    {
        $addFields: {
            brand: "$product_info.brand",
            price: "$product_info.price",
            category_name: "$category_info.category_name",
            category_code: "$category_info.category_code",
            popularity_score: {
                $add: [
                    { $multiply: ["$purchaser_count", 0.7] },
                    { $multiply: ["$total_purchases", 0.3] }
                ]
            }
        }
    },
    {
        $project: {
            product_id: 1,
            brand: 1,
            price: 1,
            category_name: 1,
            category_code: 1,
            unique_purchasers: 1,
            total_purchases: 1,
            avg_price: 1,
            popularity_score: 1
        }
    },
    { $sort: { popularity_score: -1, avg_price: -1 } },
    { $limit: 50 }
]);

// Query 2: User Category Preferences
db.events.aggregate([
    {
        $match: { "event_type": "purchase" }
    },
    {
        $group: {
            _id: {
                user_id: "$user_id",
                product_id: "$product_id"
            },
            purchase_count: { $sum: 1 }
        }
    },
    {
        $group: {
            _id: "$_id.user_id",
            products: { $addToSet: "$_id.product_id" }
        }
    },
    {
        $addFields: {
            product_count: { $size: "$products" }
        }
    },
    {
        $lookup: {
            from: "products",
            localField: "products",
            foreignField: "product_id",
            as: "product_details"
        }
    },
    { $unwind: "$product_details" },
    {
        $lookup: {
            from: "categories",
            localField: "product_details.category_id",
            foreignField: "category_id",
            as: "category_details"
        }
    },
    { $unwind: "$category_details" },
    {
        $group: {
            _id: {
                user_id: "$user_id",
                category_id: "$product_details.category_id"
            },
            category_name: { $first: "$category_details.category_name" },
            products_in_category: { $sum: 1 }
        }
    },
    {
        $group: {
            _id: "$_id.user_id",
            categories: { $push: {
                category_id: "$category_id",
                category_name: "$category_name",
                products_in_category: "$products_in_category"
            }}
        }
    },
    { $unwind: "$categories" },
    {
        $sort: { "categories.products_in_category": -1 } },
    {
        $group: {
            _id: "$_id.user_id",
            top_categories: { $slice: ["$categories", 3] }
        }
    },
    { $unwind: "$top_categories" },
    {
        $project: {
            user_id: 1,
            category_id: "$top_categories.category_id",
            category_name: "$top_categories.category_name",
            rank: { $literal: "top_category" }
        }
    }]);
