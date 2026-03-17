// Campaign Effectiveness Analysis
// Analyzes if campaigns attracted customers to purchase products
// Leverages social network information for campaign optimization

// Query 1: Campaign Conversion Analysis
db.messages.aggregate([
    {
        $match: {
            "sent_at": { $exists: true }
        }
    },
    {
        $group: {
            _id: "$campaign_id",
            total_sent: { $sum: 1 },
            unique_users: { $addToSet: "$user_id" }
        }
    },
    {
        $addFields: {
            engaged_users: { $size: "$unique_users" }
        }
    },
    {
        $lookup: {
            from: "events",
            localField: "_id",
            foreignField: "user_id",
            as: "user_events"
        }
    },
    { $unwind: "$user_events" },
    {
        $match: {
            "user_events.event_type": "purchase",
            "user_events.event_time": { $gte: "$$started_at", $lte: "$$finished_at" }
        }
    },
    {
        $group: {
            _id: {
                campaign_id: "$_id",
                user_id: "$user_id"
            },
            purchase_events: { $sum: 1 },
            purchase_value: { $sum: "$user_events.price" }
        }
    },
    {
        $group: {
            _id: "$_id.campaign_id",
            purchasing_users: { $sum: 1 },
            total_purchase_value: { $sum: "$purchase_value" }
        }
    },
    {
        $lookup: {
            from: "campaigns",
            localField: "_id",
            foreignField: "campaign_id",
            as: "campaign_info"
        }
    },
    { $unwind: "$campaign_info" },
    {
        $addFields: {
            campaign_type: "$campaign_info.campaign_type",
            channel: "$campaign_info.channel",
            conversion_rate: {
                $multiply: [
                    { $divide: ["$purchasing_users", "$engaged_users"] },
                    100
                ]
            },
            avg_purchase_value: { $divide: ["$total_purchase_value", "$purchasing_users"] }
        }
    },
    {
        $project: {
            campaign_id: 1,
            campaign_type: 1,
            channel: 1,
            engaged_users: 1,
            purchasing_users: 1,
            conversion_rate: 1,
            total_purchase_value: 1,
            avg_purchase_value: 1
        }
    },
    { $sort: { conversion_rate: -1, engaged_users: -1 } }
]);

// Query 2: Campaign Social Influence Analysis
db.messages.aggregate([
    {
        $match: {
            "campaign_id": { $exists: true }
        }
    },
    {
        $group: {
            _id: "$campaign_id",
            campaign_users: { $addToSet: "$user_id" }
        }
    },
    {
        $addFields: {
            engaged_count: { $size: "$campaign_users" }
        }
    },
    {
        $lookup: {
            from: "friends",
            localField: "campaign_users",
            foreignField: "user_id",
            as: "user_friends"
        }
    },
    { $unwind: "$user_friends" },
    {
        $lookup: {
            from: "events",
            localField: "user_friends.friend_id",
            foreignField: "user_id",
            as: "friend_events"
        }
    },
    { $unwind: "$friend_events" },
    {
        $match: {
            "friend_events.event_type": "purchase"
        }
    },
    {
        $group: {
            _id: {
                campaign_id: "$_id",
                original_user: "$user_id"
            },
            friend_purchases: { $sum: 1 },
            friend_purchase_value: { $sum: "$friend_events.price" }
        }
    },
    {
        $group: {
            _id: "$_id.campaign_id",
            original_users: { $addToSet: "$original_user" },
            total_friend_purchases: { $sum: "$friend_purchases" },
            total_friend_purchase_value: { $sum: "$friend_purchase_value" }
        }
    },
    {
        $addFields: {
            friends_of_engaged: { $size: "$original_users" },
            friend_influence_rate: {
                $multiply: [
                    { $divide: ["$total_friend_purchases", "$friends_of_engaged"] },
                    100
                ]
            }
        }
    },
    {
        $lookup: {
            from: "campaigns",
            localField: "_id",
            foreignField: "campaign_id",
            as: "campaign_info"
        }
    },
    { $unwind: "$campaign_info" },
    {
        $addFields: {
            campaign_type: "$campaign_info.campaign_type",
            channel: "$campaign_info.channel"
        }
    },
    {
        $project: {
            campaign_id: 1,
            campaign_type: 1,
            channel: 1,
            engaged_users: 1,
            friends_of_engaged: 1,
            total_friend_purchases: 1,
            friend_influence_rate: 1
        }
    },
    { $sort: { friend_influence_rate: -1, engaged_users: -1 } }
]);
