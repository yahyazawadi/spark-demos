/**
 * =========================================================================================
 *                  MONGODB & MONGOSH MASTER PRACTICE (COMPREHENSIVE)
 * =========================================================================================
 * Designed for GitHub Repository practice & study.
 * Covers every single concept from the handwritten lecture notes (All 7 Pages of MongoDB (2).pdf)
 * plus production NoSQL patterns based on `orders` and `customers` collections.
 *
 * Concepts Covered:
 *  - Page 1: Database & Collection management (show dbs, db, use, dropDatabase, createCollection,
 *            show collections), insertOne, insertMany, find(), pretty().
 *  - Page 2: find({filter}), sort (asc 1, desc -1), count(), limit(), forEach(), findOne(),
 *            Field Projections ({field: 1}), full doc update, {upsert: true}, $set, $inc.
 *  - Page 3: $rename, remove() / delete, sub-documents update, $elemMatch for arrays,
 *            text index (createIndex({title: "text"})) & $text $search, comparison operators ($gt, $lt, $gte, $lte, $in).
 *  - Pages 4-7: Complete Aggregation Pipeline:
 *            $sort, $group (_id, $sum, $avg, $min, $max, $push, $addToSet, $first, $last),
 *            $match, $project ($multiply, custom fields), $unwind (flattening arrays),
 *            $lookup (JOIN across collections), $addFields, $limit, $skip, $count,
 *            $facet (multi-pipeline analytics in a single query).
 *
 * Structure per question:
 *   // Q<n>: <Question prompt>
 *   [4 Blank Lines for self-study]
 *   <Clean, idiomatic mongosh code solution>
 * =========================================================================================
 */

// =========================================================================================
// PART 1: DATABASE & COLLECTION MANAGEMENT (Page 1)
// =========================================================================================

// Q1: Show all databases currently existing on the MongoDB server (Page 1: show dbs).




show dbs;


// Q2: Display the current active database name you are connected to (Page 1: db).




db;


// Q3: Switch to or create a new database named "retail_store" (Page 1: use acme).




use retail_store;


// Q4: Explicitly create a new collection named "orders" with options or default settings (Page 1: db.createCollection).




db.createCollection("orders");


// Q5: Display all collections existing within the current active database (Page 1: show collections).




show collections;


// Q6: Drop the entire current database to completely clean up resources (Page 1: db.dropDatabase()).




db.dropDatabase();


// =========================================================================================
// PART 2: INSERTION OPERATIONS (Page 1)
// =========================================================================================

// Q7: Insert a single order document into "orders" containing order_id, customer (sub-document with id, name, city), category, product_name, unit_price, quantity, total_amount, order_status, and tags (array) (Page 1: db.posts.insert / insertOne).




db.orders.insertOne({
  order_id: 1001,
  customer: { id: 264, name: "Sara Khalil", city: "Cairo" },
  category: "Clothing",
  product_name: "Denim Jeans Classic",
  unit_price: 38.4,
  quantity: 1,
  total_amount: 38.4,
  order_status: "COMPLETED",
  order_date: ISODate("2026-07-13T09:37:00Z"),
  tags: ["sale", "budget"],
  rating: 2.7
});


// Q8: Insert multiple documents at once into "orders" using a single batch command (Page 1: db.posts.insertMany([...])).




db.orders.insertMany([
  {
    order_id: 1002,
    customer: { id: 160, name: "Bilal Khalil", city: "Amman" },
    category: "Furniture",
    product_name: "Monitor Arm Dual",
    unit_price: 58.95,
    quantity: 1,
    total_amount: 58.95,
    order_status: "CANCELLED",
    order_date: ISODate("2025-10-12T20:55:00Z"),
    tags: ["bestseller"],
    rating: 4.2
  },
  {
    order_id: 1003,
    customer: { id: 188, name: "Fatima Nasser", city: "Hebron" },
    category: "Office Supplies",
    product_name: "Whiteboard Magnetic 90x60",
    unit_price: 40.5,
    quantity: 1,
    total_amount: 40.5,
    order_status: "COMPLETED",
    order_date: ISODate("2026-01-03T21:22:00Z"),
    tags: ["premium", "sale", "organic"],
    rating: 3.8
  }
]);


// =========================================================================================
// PART 3: QUERYING, FILTERING, PROJECTIONS & ITERATION (Pages 1 & 2)
// =========================================================================================

// Q9: Retrieve all documents from "orders" and format the output in readable JSON (Page 1: db.posts.find().pretty()).




db.orders.find().pretty();


// Q10: Find all orders belonging to the category "Clothing" (Page 2: db.posts.find({category: "news"})).




db.orders.find({ category: "Clothing" });


// Q11: Retrieve only the single first matching order located in "Nablus" (Page 2: db.posts.findOne({category: "news"})).




db.orders.findOne({ "customer.city": "Nablus" });


// Q12: Count the total number of orders that have the status "COMPLETED" (Page 2: db.posts.find({category: "news"}).count()).




db.orders.find({ order_status: "COMPLETED" }).count();


// Q13: Retrieve all orders sorted by total_amount in ascending order, then descending order (Page 2: db.posts.sort({title: 1}) / sort({title: -1})).




// Ascending order:
db.orders.find().sort({ total_amount: 1 });

// Descending order:
db.orders.find().sort({ total_amount: -1 });


// Q14: Fetch only the first 5 highest total_amount orders, skipping the first 2 (Page 2: limit(2) & Page 6: skip).




db.orders.find().sort({ total_amount: -1 }).skip(2).limit(5);


// Q15: Project only the product_name and customer.city fields (and omit _id) for all orders (Page 2: db.posts.find({}, {title: 1, author: 1})).




db.orders.find({}, { _id: 0, product_name: 1, "customer.city": 1 });


// Q16: Iterate over all orders in category "Books" and print a formatted console message for each: "Order #<id>: <product_name> - $<total_amount>" (Page 2: forEach(function(doc){...})).




db.orders.find({ category: "Books" }).forEach(function(doc) {
  print("Order #" + doc.order_id + ": " + doc.product_name + " - $" + doc.total_amount);
});


// =========================================================================================
// PART 4: DOCUMENT UPDATES & MODIFIERS (Pages 2 & 3)
// =========================================================================================

// Q17: Update an order with order_id 1001 by completely replacing its document content, and enable upsert so it creates the doc if not found (Page 2: db.posts.update({...}, {...}, {upsert: true})).




db.orders.updateOne(
  { order_id: 1001 },
  {
    $set: {
      order_id: 1001,
      customer: { id: 264, name: "Sara Khalil", city: "Cairo" },
      product_name: "Denim Jeans Premium Replaced",
      total_amount: 45.0,
      order_status: "COMPLETED",
      updated_at: new Date()
    }
  },
  { upsert: true }
);


// Q18: Update only specific fields (order_status to "DELIVERED" and rating to 5.0) without altering any other fields (Page 2: $set).




db.orders.updateOne(
  { order_id: 1003 },
  { $set: { order_status: "DELIVERED", rating: 5.0 } }
);


// Q19: Increment the quantity of order_id 1002 by 2 and increase its total_amount by 117.90 using $inc (Page 2: $inc: {likes: 3}).




db.orders.updateOne(
  { order_id: 1002 },
  { $inc: { quantity: 2, total_amount: 117.90 } }
);


// Q20: Rename the field "rating" to "customer_score" across all documents (Page 3: $rename: {likes: "views"}).




db.orders.updateMany(
  {},
  { $rename: { "rating": "customer_score" } }
);


// Q21: Update a nested sub-document field: change customer.city of order_id 1001 to "Alexandria" (Page 3: sub-doc update).




db.orders.updateOne(
  { order_id: 1001 },
  { $set: { "customer.city": "Alexandria" } }
);


// Q22: Delete/Remove all orders that have order_status equal to "CANCELLED" (Page 3: db.posts.remove({title: ...}) / deleteMany).




db.orders.deleteMany({ order_status: "CANCELLED" });


// =========================================================================================
// PART 5: ARRAYS, ELEMMATCH & TEXT INDEXES (Page 3)
// =========================================================================================

// Q23: Add a new comment object {user: "Ahmad", text: "Great product", score: 5} to a "comments" array inside order_id 1003 using $push (Page 3: comments: [{...}]).




db.orders.updateOne(
  { order_id: 1003 },
  {
    $push: {
      comments: { user: "Ahmad", text: "Great product", score: 5 }
    }
  }
);


// Q24: Query orders where the "comments" array has an element matching BOTH user "Ahmad" and score >= 4 using $elemMatch (Page 3: db.posts.find({comments: {$elemMatch: {...}}})).




db.orders.find({
  comments: {
    $elemMatch: { user: "Ahmad", score: { $gte: 4 } }
  }
});


// Q25: Create a Text Index on product_name (Page 3: db.posts.createIndex({title: "text"})).




db.orders.createIndex({ product_name: "text" });


// Q26: Perform a full-text search to find all documents whose product_name contains "Desk" or "Keyboard" (Page 3: db.posts.find({$text: {$search: "..."}})).




db.orders.find({
  $text: { $search: "Desk Keyboard" }
});


// Q27: Query orders with numerical comparison: unit_price greater than 100 ($gt) and unit_price less than or equal to 500 ($lte) (Page 3: $gt, $lt).




db.orders.find({
  unit_price: { $gt: 100, $lte: 500 }
});


// =========================================================================================
// PART 6: AGGREGATION PIPELINE - GROUPING & ACCUMULATORS (Page 4)
// =========================================================================================

// Q28: Group orders by customer.name and calculate: total revenue ($sum total_amount), average rating ($avg), minimum price ($min), and maximum price ($max) (Page 4: $group, $sum, $avg, $min, $max).




db.orders.aggregate([
  {
    $group: {
      _id: "$customer.name",
      totalRevenue: { $sum: "$total_amount" },
      avgRating: { $avg: "$customer_score" },
      minPrice: { $min: "$unit_price" },
      maxPrice: { $max: "$unit_price" }
    }
  }
]);


// Q29: Group orders by customer.city and collect all product names into an array ($push), and collect unique categories without duplicates ($addToSet) (Page 4: $push, $addToSet).




db.orders.aggregate([
  {
    $group: {
      _id: "$customer.city",
      allProducts: { $push: "$product_name" },
      uniqueCategories: { $addToSet: "$category" }
    }
  }
]);


// Q30: Sort orders by order_date ascending, then group by customer.id and retrieve their first order product and last order product (Page 4: $sort followed by $first and $last).




db.orders.aggregate([
  { $sort: { order_date: 1 } },
  {
    $group: {
      _id: "$customer.id",
      firstOrderProduct: { $first: "$product_name" },
      lastOrderProduct: { $last: "$product_name" },
      totalOrders: { $sum: 1 }
    }
  }
]);


// =========================================================================================
// PART 7: AGGREGATION PIPELINE - MATCH, PROJECT, UNWIND & LOOKUP (Pages 5 & 6)
// =========================================================================================

// Q31: Use $match as the first aggregation stage to filter only orders with category "Electronics" and total_amount > 100 (Page 5: $match equivalent to SQL WHERE).




db.orders.aggregate([
  {
    $match: {
      category: "Electronics",
      total_amount: { $gt: 100 }
    }
  }
]);


// Q32: Project fields using $project: include product_name, rename total_amount to revenue, and create a calculated field "discounted_amount" by multiplying unit_price by 0.9 (Page 5: $project with $multiply).




db.orders.aggregate([
  {
    $project: {
      _id: 0,
      product_name: 1,
      revenue: "$total_amount",
      discounted_amount: { $multiply: ["$unit_price", 0.9] }
    }
  }
]);


// Q33: Deconstruct/flatten the "tags" array so that each tag produces its own separate document using $unwind (Page 5: $unwind: "$tags").




db.orders.aggregate([
  { $unwind: "$tags" },
  {
    $project: {
      order_id: 1,
      product_name: 1,
      tag: "$tags"
    }
  }
]);


// Q34: Count the frequency of each individual tag across all orders by combining $unwind and $group.




db.orders.aggregate([
  { $unwind: "$tags" },
  {
    $group: {
      _id: "$tags",
      tagFrequency: { $sum: 1 }
    }
  },
  { $sort: { tagFrequency: -1 } }
]);


// Q35: Perform a $lookup (Left Outer JOIN) between "orders" and a separate "customers" collection where orders.customer.id matches customers.customer_id, storing matched customer details in "customer_info" (Page 5-6: $lookup {from, localField, foreignField, as}).




db.orders.aggregate([
  {
    $lookup: {
      from: "customers",
      localField: "customer.id",
      foreignField: "customer_id",
      as: "customer_info"
    }
  }
]);


// Q36: Add a calculated field "total_tax" equal to 16% of total_amount without removing existing fields using $addFields (Page 6: $addFields).




db.orders.aggregate([
  {
    $addFields: {
      total_tax: { $multiply: ["$total_amount", 0.16] }
    }
  }
]);


// Q37: Filter orders where customer_score >= 4.0, sort by total_amount descending, skip the first 3 results, and limit to 5 (Page 6: $skip & $limit).




db.orders.aggregate([
  { $match: { customer_score: { $gte: 4.0 } } },
  { $sort: { total_amount: -1 } },
  { $skip: 3 },
  { $limit: 5 }
]);


// Q38: Count the total number of orders in category "Sports & Outdoors" using $count inside an aggregation pipeline (Page 6: $count: "total_sports_orders").




db.orders.aggregate([
  { $match: { category: "Sports & Outdoors" } },
  { $count: "total_sports_orders" }
]);


// Q39: Multi-faceted analytics: Use $facet to run multiple independent pipelines in a single query: (1) top 3 highest-spending orders, (2) top 3 most recent orders, (3) category count summary (Pages 6 & 7: $facet).




db.orders.aggregate([
  {
    $facet: {
      "topHighestOrders": [
        { $sort: { total_amount: -1 } },
        { $limit: 3 },
        { $project: { order_id: 1, product_name: 1, total_amount: 1 } }
      ],
      "recentOrders": [
        { $sort: { order_date: -1 } },
        { $limit: 3 },
        { $project: { order_id: 1, product_name: 1, order_date: 1 } }
      ],
      "categoryCounts": [
        { $group: { _id: "$category", count: { $sum: 1 } } },
        { $sort: { count: -1 } }
      ]
    }
  }
]);


// =========================================================================================
// PART 8: ADVANCED NOSQL REAL-WORLD PATTERNS
// =========================================================================================

// Q40: Conditional Field Projection ($cond): Add a field "order_tier" where orders with total_amount >= 300 are tagged "VIP", otherwise "STANDARD".




db.orders.aggregate([
  {
    $project: {
      order_id: 1,
      total_amount: 1,
      order_tier: {
        $cond: {
          if: { $gte: ["$total_amount", 300] },
          then: "VIP",
          else: "STANDARD"
        }
      }
    }
  }
]);


// Q41: Date Aggregation: Extract year and month from order_date and calculate monthly total revenue.




db.orders.aggregate([
  {
    $group: {
      _id: {
        year: { $year: "$order_date" },
        month: { $month: "$order_date" }
      },
      monthlyRevenue: { $sum: "$total_amount" },
      ordersCount: { $sum: 1 }
    }
  },
  { $sort: { "_id.year": 1, "_id.month": 1 } }
]);


// Q42: String Manipulation ($concat & $toUpper): Create a formatted "display_label" concatenating uppercase category and product name (e.g. "[ELECTRONICS] Laptop Pro 15").




db.orders.aggregate([
  {
    $project: {
      order_id: 1,
      display_label: {
        $concat: ["[", { $toUpper: "$category" }, "] ", "$product_name"]
      }
    }
  }
]);


// Q43: Array Filtering ($filter): Filter the "tags" array to keep only tags that equal "bestseller" or "premium".




db.orders.aggregate([
  {
    $project: {
      order_id: 1,
      filtered_tags: {
        $filter: {
          input: "$tags",
          as: "t",
          cond: { $in: ["$$t", ["bestseller", "premium"]] }
        }
      }
    }
  }
]);


// Q44: Pivot-like Grouping ($group with $cond): Calculate the total count of COMPLETED vs CANCELLED orders in each city.




db.orders.aggregate([
  {
    $group: {
      _id: "$customer.city",
      completedCount: {
        $sum: { $cond: [{ $eq: ["$order_status", "COMPLETED"] }, 1, 0] }
      },
      cancelledCount: {
        $sum: { $cond: [{ $eq: ["$order_status", "CANCELLED"] }, 1, 0] }
      },
      totalOrders: { $sum: 1 }
    }
  },
  { $sort: { totalOrders: -1 } }
]);


// Q45: Export Aggregation Result to a new collection using $out stage.




db.orders.aggregate([
  { $match: { order_status: "COMPLETED" } },
  {
    $group: {
      _id: "$customer.city",
      totalRevenue: { $sum: "$total_amount" },
      completedOrders: { $sum: 1 }
    }
  },
  { $out: "city_sales_summary" }
]);
