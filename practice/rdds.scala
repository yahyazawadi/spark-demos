package practice

import org.apache.spark.{SparkConf, SparkContext, HashPartitioner}

object RDDsPractice {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Ultimate 48 RDD Practice On Manual & Sales Dataset")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Dataset Path
    val filePath = "data/practice_dataset/sales_and_orders_1000.csv"

    // Schema reference:
    // cols(0):  order_id (Int)
    // cols(1):  customer_id (Int)
    // cols(2):  customer_name (String)
    // cols(3):  city (String)
    // cols(4):  category (String)
    // cols(5):  product_name (String)
    // cols(6):  unit_price (Double)
    // cols(7):  quantity (Int)
    // cols(8):  total_amount (Double)
    // cols(9):  order_status (String: COMPLETED, CANCELLED, PENDING, PROCESSING, RETURNED)
    // cols(10): order_date (Timestamp: YYYY-MM-DD HH:MM:SS)
    // cols(11): tags (Semicolon-delimited: sale;budget;premium)
    // cols(12): rating (Double: 1.0 - 5.0)

    val rawRdd = sc.textFile(filePath)
    val header = rawRdd.first()
    val dataRdd = rawRdd.filter(row => row != header)




    // =========================================================================
    // PART 0: DIRECT NOTEBOOK / MANUAL PROBLEMS (sc.parallelize & Core RDD Rules)
    // =========================================================================




    // =========================================================================
    // QUESTION 1: Creating RDD from In-Memory Collection (sc.parallelize)
    // From Manual Page 1: Convert a Scala List("Java", "Scala", "R") into an RDD.
    // Filter elements containing "R" and print the result.
    // =========================================================================




    // SOLUTION 1:
    val listRdd = sc.parallelize(List("Java", "Scala", "R"))
    val resultRdd = listRdd.filter(x => x.contains("R"))
    println("Q1 Elements containing 'R': " + resultRdd.collect().mkString(", "))




    // =========================================================================
    // QUESTION 2: Mathematical In-Memory Map & Collect with mkString
    // From Manual Page 1 & 4: Create an RDD from List(1, 2, 3, 4, 5).
    // Square each number (x => x * x) and print formatted as a comma-separated string using mkString(", ").
    // =========================================================================




    // SOLUTION 2:
    val numbersRdd = sc.parallelize(List(1, 2, 3, 4, 5))
    val squaredRdd = numbersRdd.map(x => x * x)
    println("Q2 Squared Numbers: " + squaredRdd.collect().mkString(", "))




    // =========================================================================
    // QUESTION 3: In-Memory Evens Filter
    // From Manual Page 1: Filter List(1, 2, 3, 4, 5) to keep only even numbers (x % 2 == 0).
    // =========================================================================




    // SOLUTION 3:
    val evensRdd = numbersRdd.filter(x => x % 2 == 0)
    println("Q3 Even Numbers: " + evensRdd.collect().mkString(", "))




    // =========================================================================
    // QUESTION 4: In-Memory Sentence FlatMap Tokenizer
    // From Manual Page 1: Create an RDD from List("hello world", "big data").
    // Split each line by space (" ") to produce an RDD of individual words and collect.
    // =========================================================================




    // SOLUTION 4:
    val linesRdd = sc.parallelize(List("hello world", "big data"))
    val wordsRdd = linesRdd.flatMap(line => line.split(" "))
    println("Q4 Words: " + wordsRdd.collect().mkString(" | "))




    // =========================================================================
    // QUESTION 5: In-Memory GroupBy (Even vs Odd & First Character)
    // From Manual Page 2:
    // (a) Group List(1, 2, 3, 4, 5) into two groups based on (x % 2 == 0).
    // (b) Group List("apple", "banana", "avocado", "blue", "apricot") by their first character.
    // =========================================================================




    // SOLUTION 5:
    val groupedEvenOdd = numbersRdd.groupBy(x => x % 2 == 0)
    groupedEvenOdd.collect().foreach { case (isEven, nums) =>
      val label = if (isEven) "even" else "odd"
      println(s"Q5 Group [$label]: ${nums.mkString(", ")}")
    }

    val fruitWords = sc.parallelize(List("apple", "banana", "avocado", "blue", "apricot"))
    val groupedByChar = fruitWords.groupBy(word => word.charAt(0))
    groupedByChar.collect().foreach { case (char, list) =>
      println(s"Q5 Initial '$char': ${list.mkString(", ")}")
    }




    // =========================================================================
    // QUESTION 6: General Action 'reduce' on Numbers & Strings
    // From Manual Page 3:
    // (a) Use reduce to compute the sum of numbers from 1 to 100 (should equal 5050).
    // (b) Use reduce to concatenate List("Hamed", "Abdelhaq") with a space.
    // =========================================================================




    // SOLUTION 6:
    val sum1To100 = sc.parallelize(1 to 100).reduce(_ + _)
    println(s"Q6 (a) Sum 1 to 100: $sum1To100")

    val fullName = sc.parallelize(List("Hamed", "Abdelhaq")).reduce((x1, x2) => x1 + " " + x2)
    println(s"Q6 (b) Name concatenation: $fullName")




    // =========================================================================
    // QUESTION 7: Basic Actions - first(), take(n), count() & Lazy Evaluation
    // From Manual Page 2 & 3:
    // On sc.parallelize(1 to 10), demonstrate first(), take(5), and count().
    // Note: Spark builds a DAG (Directed Acyclic Graph) for all Transformations lazily.
    // No physical computation occurs until an Action (count, collect, take, etc.) is called.
    // =========================================================================




    // SOLUTION 7:
    val tenRdd = sc.parallelize(1 to 10)
    println("Q7 first(): " + tenRdd.first())
    println("Q7 take(5): " + tenRdd.take(5).mkString(", "))
    println("Q7 count(): " + tenRdd.count())




    // =========================================================================
    // QUESTION 8: In-Memory Key-Value Operations (reduceByKey, sortByKey, join)
    // From Manual Page 2:
    // Given rdd = List(("A", 1), ("B", 1), ("A", 2)), reduceByKey to get ("A", 3), ("B", 1).
    // Sort by key, and join with rdd2 = List(("A", 100), ("B", 200)).
    // =========================================================================




    // SOLUTION 8:
    val pairRdd1 = sc.parallelize(List(("A", 1), ("B", 1), ("A", 2)))
    val reducedPairs = pairRdd1.reduceByKey(_ + _)
    println("Q8 reduced: " + reducedPairs.collect().mkString(", "))

    val sortedPairs = pairRdd1.sortByKey()
    println("Q8 sorted: " + sortedPairs.collect().mkString(", "))

    val pairRdd2 = sc.parallelize(List(("A", 100), ("B", 200)))
    val joinedPairs = reducedPairs.join(pairRdd2)
    println("Q8 joined: " + joinedPairs.collect().mkString(", "))




    // =========================================================================
    // PART 1: 40 PROGRESSIVE DATASET CHALLENGES (ALL ORIGINAL QUESTIONS PRESERVED)
    // =========================================================================




    // =========================================================================
    // QUESTION 9: Total Data Records
    // Count the total number of order records in dataRdd excluding the header.
    // =========================================================================




    // SOLUTION 9:
    val q9 = dataRdd.count()
    println(s"Q9 Total Records: $q9")




    // =========================================================================
    // QUESTION 10: Inspecting Top Rows
    // Print the first record and take the first 3 rows to inspect column alignment.
    // =========================================================================




    // SOLUTION 10:
    val q10First = dataRdd.first()
    val q10Sample = dataRdd.take(3)
    println(s"Q10 First: $q10First")
    q10Sample.foreach(r => println(s"Q10 Sample: $r"))




    // =========================================================================
    // QUESTION 11: Simple Predicate Filter
    // Find how many orders were placed in 'Ramallah'.
    // =========================================================================




    // SOLUTION 11:
    val q11 = dataRdd.filter(l => l.split(",")(3).trim.equalsIgnoreCase("Ramallah")).count()
    println(s"Q11 Ramallah Orders Count: $q11")




    // =========================================================================
    // QUESTION 12: Compound Boolean Filtering
    // Count how many orders in 'Nablus' have status 'COMPLETED' and total_amount > 200.0.
    // =========================================================================




    // SOLUTION 12:
    val q12 = dataRdd.filter { l =>
      val c = l.split(",")
      c(3).trim.equalsIgnoreCase("Nablus") && c(9).trim == "COMPLETED" && c(8).toDouble > 200.0
    }.count()
    println(s"Q12 Filtered Count: $q12")




    // =========================================================================
    // QUESTION 13: Distinct Extraction
    // Extract a list of all distinct product categories available in the dataset.
    // =========================================================================




    // SOLUTION 13:
    val q13 = dataRdd.map(l => l.split(",")(4).trim).distinct().collect()
    println(s"Q13 Categories (${q13.length}): ${q13.mkString(", ")}")




    // =========================================================================
    // QUESTION 14: Basic Projection to Tuples
    // Transform each row into a 3-tuple (order_id, customer_name, total_amount) and print 5.
    // =========================================================================




    // SOLUTION 14:
    val q14 = dataRdd.map { l =>
      val c = l.split(",")
      (c(0).toInt, c(2).trim, c(8).toDouble)
    }.take(5)
    q14.foreach(println)




    // =========================================================================
    // QUESTION 15: Mathematical Transformation on Columns
    // Calculate effective price per unit after purchase (total_amount / quantity).
    // Return (order_id, product_name, effective_unit_price) for the first 5 records.
    // =========================================================================




    // SOLUTION 15:
    val q15 = dataRdd.map { l =>
      val c = l.split(",")
      val effectivePrice = c(8).toDouble / c(7).toInt
      (c(0).toInt, c(5).trim, effectivePrice)
    }.take(5)
    q15.foreach { case (id, prod, price) => println(f"Order $id: $prod%-25s Effective Unit: $$$price%.2f") }




    // =========================================================================
    // QUESTION 16: Date String Slicing
    // Extract just the Order Year from order_date and count distinct years present.
    // =========================================================================




    // SOLUTION 16:
    val q16 = dataRdd.map(l => l.split(",")(10).trim.substring(0, 4)).distinct().collect()
    println(s"Q16 Distinct Years: ${q16.mkString(", ")}")




    // =========================================================================
    // QUESTION 17: Name Tokenization via FlatMap
    // Extract customer first names and last names into a flat stream of names,
    // and count how many total words exist in the customer_name column.
    // =========================================================================




    // SOLUTION 17:
    val q17 = dataRdd.flatMap(l => l.split(",")(2).trim.split(" ")).count()
    println(s"Q17 Total Name Tokens: $q17")




    // =========================================================================
    // QUESTION 18: Tag Extraction and Frequency (WordCount)
    // The 'tags' column has semicolon-separated values (e.g. sale;budget;premium).
    // Extract every tag, clean it, and compute the total occurrence of each tag.
    // =========================================================================




    // SOLUTION 18:
    val q18 = dataRdd.flatMap(_.split(",")(11).split(";"))
      .map(t => (t.trim.toLowerCase, 1))
      .filter(_._1.nonEmpty)
      .reduceByKey(_ + _)
    q18.collect().foreach { case (tag, count) => println(s"Tag [$tag]: $count") }




    // =========================================================================
    // QUESTION 19: Orders Count per Status
    // Group and count the number of orders in each order_status using map + reduceByKey.
    // =========================================================================




    // SOLUTION 19:
    val q19 = dataRdd.map(l => (l.split(",")(9).trim, 1)).reduceByKey(_ + _)
    q19.collect().foreach { case (status, cnt) => println(s"Status $status%-12s: $cnt orders") }




    // =========================================================================
    // QUESTION 20: Total Revenue per Category
    // Compute the sum of total_amount grouped by category using reduceByKey.
    // =========================================================================




    // SOLUTION 20:
    val q20 = dataRdd.map(l => (l.split(",")(4).trim, l.split(",")(8).toDouble)).reduceByKey(_ + _)
    q20.collect().foreach { case (cat, rev) => println(f"Category $cat%-18s: $$$rev%.2f") }




    // =========================================================================
    // QUESTION 21: Total Items Sold per Product
    // Calculate total quantity sold for each product_name, and take 5 products.
    // =========================================================================




    // SOLUTION 21:
    val q21 = dataRdd.map(l => (l.split(",")(5).trim, l.split(",")(7).toInt)).reduceByKey(_ + _)
    q21.take(5).foreach { case (prod, qty) => println(s"Product: $prod%-28s Sold: $qty units") }




    // =========================================================================
    // QUESTION 22: Revenue Per Order Formatted as String (Direct Manual Page 4)
    // From Manual: Map to (order_id, total_amount), reduceByKey, and format as string "id,amount".
    // =========================================================================




    // SOLUTION 22:
    val q22 = dataRdd.map { l =>
      val c = l.split(",")
      (c(0).toInt, c(8).toDouble)
    }.reduceByKey(_ + _)
      .map(o => o._1 + "," + o._2)
    q22.take(5).foreach(println)




    // =========================================================================
    // QUESTION 23: Average Items per Product Formatted as String (Direct Manual Page 4)
    // From Manual: Map to (product, (qty, 1)), reduceByKey, mapValues(sum/count), format "prod,avg".
    // =========================================================================




    // SOLUTION 23:
    val q23 = dataRdd.map { l =>
      val c = l.split(",")
      (c(5).trim, (c(7).toDouble, 1))
    }.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues(x => x._1 / x._2)
      .map(p => p._1 + "," + p._2)
    q23.take(5).foreach(println)




    // =========================================================================
    // QUESTION 24: Filtering Key-Value Pairs
    // Keep only categories whose total revenue exceeds $15,000 using filter on pair RDD.
    // =========================================================================




    // SOLUTION 24:
    val q24 = q20.filter { case (_, totalRev) => totalRev > 15000.0 }
    q24.collect().foreach { case (cat, rev) => println(f"High-Rev Category: $cat%-18s $$$rev%.2f") }




    // =========================================================================
    // QUESTION 25: GroupBy Transformation on Dataset
    // Group customer names by their city using groupBy, and display city with distinct customer count.
    // =========================================================================




    // SOLUTION 25:
    val q25 = dataRdd.map(l => (l.split(",")(3).trim, l.split(",")(2).trim))
      .groupBy(_._1)
      .mapValues(iter => iter.map(_._2).toSet.size)
    q25.collect().foreach { case (city, uniqueCusts) => println(s"City: $city%-12s Unique Customers: $uniqueCusts") }




    // =========================================================================
    // QUESTION 26: Average Order Value per City (MapValues Pattern)
    // Compute the average order total_amount for each city using the (sum, count) pattern.
    // =========================================================================




    // SOLUTION 26:
    val q26 = dataRdd.map(l => (l.split(",")(3).trim, (l.split(",")(8).toDouble, 1)))
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .mapValues { case (sum, count) => sum / count }
    q26.collect().foreach { case (city, avg) => println(f"City $city%-12s Avg Order: $$$avg%.2f") }




    // =========================================================================
    // QUESTION 27: Average Rating per Product Category
    // Compute average customer rating for each category.
    // =========================================================================




    // SOLUTION 27:
    val q27 = dataRdd.map(l => (l.split(",")(4).trim, (l.split(",")(12).toDouble, 1)))
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .mapValues(p => p._1 / p._2)
    q27.collect().foreach { case (cat, avgRating) => println(f"Category $cat%-18s Avg Rating: $avgRating%.2f / 5.0") }




    // =========================================================================
    // QUESTION 28: Multi-Field Customer Order Profile
    // For each customer_id, aggregate in one step: (total_spent, total_quantity, order_count).
    // =========================================================================




    // SOLUTION 28:
    val q28 = dataRdd.map { l =>
      val c = l.split(",")
      (c(1).toInt, (c(8).toDouble, c(7).toInt, 1))
    }.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))
    q28.take(5).foreach { case (id, (spent, qty, count)) =>
      println(f"Customer $id%4d: Total=$$$spent%7.2f, Items=$qty%3d, Orders=$count%2d")
    }




    // =========================================================================
    // QUESTION 29: Sorting Keys Ascending and Descending
    // Find total items ordered per city, sorted from lowest to highest quantity.
    // =========================================================================




    // SOLUTION 29:
    val q29 = dataRdd.map(l => (l.split(",")(3).trim, l.split(",")(7).toInt))
      .reduceByKey(_ + _)
      .map(_.swap)
      .sortByKey(ascending = true)
      .map(_.swap)
    q29.collect().foreach { case (city, qty) => println(s"City $city%-12s Total Items: $qty") }




    // =========================================================================
    // QUESTION 30: Top 5 Highest Spending Customers
    // Find top 5 customers with the highest cumulative spending in COMPLETED orders.
    // =========================================================================




    // SOLUTION 30:
    val q30 = dataRdd.filter(_.split(",")(9).trim == "COMPLETED")
      .map(l => (l.split(",")(1).toInt, l.split(",")(8).toDouble))
      .reduceByKey(_ + _)
      .map(_.swap)
      .sortByKey(ascending = false)
      .map(_.swap)
      .take(5)
    q30.foreach { case (id, spent) => println(f"Top Customer $id: $$$spent%.2f") }




    // =========================================================================
    // QUESTION 31: Monthly Revenue Trend
    // Extract Year-Month (e.g. '2025-06') from order_date and find total revenue per month,
    // sorted chronologically by Year-Month.
    // =========================================================================




    // SOLUTION 31:
    val q31 = dataRdd.map { l =>
      val c = l.split(",")
      (c(10).trim.substring(0, 7), c(8).toDouble)
    }.reduceByKey(_ + _).sortByKey()
    q31.collect().foreach { case (ym, rev) => println(f"Month $ym: $$$rev%.2f") }




    // =========================================================================
    // QUESTION 32: Min and Max Price per Category using ReduceByKey
    // Find (min_unit_price, max_unit_price) for each product category using reduceByKey.
    // =========================================================================




    // SOLUTION 32:
    val q32 = dataRdd.map(l => (l.split(",")(4).trim, (l.split(",")(6).toDouble, l.split(",")(6).toDouble)))
      .reduceByKey((a, b) => (math.min(a._1, b._1), math.max(a._2, b._2)))
    q32.collect().foreach { case (cat, (minP, maxP)) => println(f"Cat $cat%-18s: Min=$$$minP%.2f, Max=$$$maxP%.2f") }




    // =========================================================================
    // QUESTION 33: aggregateByKey for Count and Sum
    // Use aggregateByKey (zeroValue, seqOp, combOp) to calculate (total_sales, count) per city.
    // =========================================================================




    // SOLUTION 33:
    val q33 = dataRdd.map(l => (l.split(",")(3).trim, l.split(",")(8).toDouble))
      .aggregateByKey((0.0, 0))(
        (acc, value) => (acc._1 + value, acc._2 + 1),
        (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
      )
    q33.collect().foreach { case (city, (sum, count)) => println(f"City $city%-12s: Sum=$$$sum%8.2f (Orders: $count)") }




    // =========================================================================
    // QUESTION 34: Comprehensive Statistics via aggregateByKey
    // Compute in ONE pass per category: (min_price, max_price, sum_price, count).
    // =========================================================================




    // SOLUTION 34:
    val q34 = dataRdd.map(l => (l.split(",")(4).trim, l.split(",")(6).toDouble))
      .aggregateByKey((Double.MaxValue, Double.MinValue, 0.0, 0))(
        (acc, p) => (math.min(acc._1, p), math.max(acc._2, p), acc._3 + p, acc._4 + 1),
        (a1, a2) => (math.min(a1._1, a2._1), math.max(a1._2, a2._2), a1._3 + a2._3, a1._4 + a2._4)
      )
    q34.collect().foreach { case (cat, (minP, maxP, sumP, count)) =>
      println(f"Category: $cat%-18s Min: $$$minP%6.2f Max: $$$maxP%6.2f Avg: $$$${sumP/count}%6.2f")
    }




    // =========================================================================
    // QUESTION 35: combineByKey for Custom Rating Averages
    // Implement combineByKey (createCombiner, mergeValue, mergeCombiners) to find avg rating per city.
    // =========================================================================




    // SOLUTION 35:
    val q35 = dataRdd.map(l => (l.split(",")(3).trim, l.split(",")(12).toDouble))
      .combineByKey(
        (rating: Double) => (rating, 1),
        (acc: (Double, Int), r: Double) => (acc._1 + r, acc._2 + 1),
        (acc1: (Double, Int), acc2: (Double, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
      ).mapValues { case (sumR, cnt) => sumR / cnt }
    q35.collect().foreach { case (city, avgR) => println(f"City $city%-12s Avg Rating: $avgR%.2f") }




    // =========================================================================
    // QUESTION 36: Distinct Customers Set per Category using aggregateByKey
    // For each product category, collect all unique customer_ids into a Scala Set and count them.
    // =========================================================================




    // SOLUTION 36:
    val q36 = dataRdd.map(l => (l.split(",")(4).trim, l.split(",")(1).toInt))
      .aggregateByKey(Set.empty[Int])(
        (setAcc, custId) => setAcc + custId,
        (s1, s2) => s1 ++ s2
      ).mapValues(_.size)
    q36.collect().foreach { case (cat, count) => println(s"Category $cat%-18s has $count distinct customers") }




    // =========================================================================
    // QUESTION 37: Inner Join with Customer Tier Lookup RDD
    // Create a customer tiers RDD: (customerId, "VIP" if id <= 180 else "REGULAR").
    // Join with order amounts to calculate total revenue per tier.
    // =========================================================================




    // SOLUTION 37:
    val custIds = dataRdd.map(l => l.split(",")(1).toInt).distinct()
    val tiersRdd = custIds.map(id => (id, if (id <= 180) "VIP" else "REGULAR"))
    val ordersAmtRdd = dataRdd.map(l => (l.split(",")(1).toInt, l.split(",")(8).toDouble))
    val q37 = ordersAmtRdd.join(tiersRdd)
      .map { case (_, (amt, tier)) => (tier, amt) }
      .reduceByKey(_ + _)
    q37.collect().foreach { case (tier, total) => println(f"Tier $tier%-8s Total Revenue: $$$total%.2f") }




    // =========================================================================
    // QUESTION 38: LeftOuterJoin for Order Cancellation Percentage
    // For each city, compute total orders and cancelled orders, perform a leftOuterJoin,
    // and compute cancellation percentage.
    // =========================================================================




    // SOLUTION 38:
    val totalOrdersByCity = dataRdd.map(l => (l.split(",")(3).trim, 1)).reduceByKey(_ + _)
    val cancelledOrdersByCity = dataRdd.filter(_.split(",")(9).trim == "CANCELLED").map(l => (l.split(",")(3).trim, 1)).reduceByKey(_ + _)
    val q38 = totalOrdersByCity.leftOuterJoin(cancelledOrdersByCity).mapValues {
      case (total, Some(cancelled)) => (cancelled.toDouble / total) * 100.0
      case (total, None) => 0.0
    }
    q38.collect().foreach { case (city, rate) => println(f"City $city%-12s Cancel Rate: $rate%5.2f%%") }




    // =========================================================================
    // QUESTION 39: Customer Retention & Churn via CoGroup
    // CoGroup completed orders in 2025 with 2026. Find all customer IDs that placed orders
    // in 2025 but placed 0 orders in 2026 (Churned Customers).
    // =========================================================================




    // SOLUTION 39:
    val completedOnly = dataRdd.filter(_.split(",")(9).trim == "COMPLETED")
    val cust2025 = completedOnly.filter(_.split(",")(10).trim.startsWith("2025")).map(l => (l.split(",")(1).toInt, l.split(",")(0).toInt))
    val cust2026 = completedOnly.filter(_.split(",")(10).trim.startsWith("2026")).map(l => (l.split(",")(1).toInt, l.split(",")(0).toInt))
    val q39 = cust2025.cogroup(cust2026).filter { case (_, (orders25, orders26)) => orders25.nonEmpty && orders26.isEmpty }.keys
    println(s"Q39 Churned Customers Count: ${q39.count()}")




    // =========================================================================
    // QUESTION 40: Full Outer Join for Cross-City Metric Comparison
    // Compute total sales in 2025 vs 2026 per city using fullOuterJoin, handling None as 0.0.
    // =========================================================================




    // SOLUTION 40:
    val city2025 = completedOnly.filter(_.split(",")(10).trim.startsWith("2025")).map(l => (l.split(",")(3).trim, l.split(",")(8).toDouble)).reduceByKey(_ + _)
    val city2026 = completedOnly.filter(_.split(",")(10).trim.startsWith("2026")).map(l => (l.split(",")(3).trim, l.split(",")(8).toDouble)).reduceByKey(_ + _)
    val q40 = city2025.fullOuterJoin(city2026).mapValues {
      case (opt25, opt26) => (opt25.getOrElse(0.0), opt26.getOrElse(0.0))
    }
    q40.collect().foreach { case (city, (s25, s26)) => println(f"City $city%-12s Sales 2025: $$$s25%8.2f | Sales 2026: $$$s26%8.2f") }




    // =========================================================================
    // QUESTION 41: Market Basket Tag Co-Occurrence Pairs
    // For every order having >= 2 distinct tags, generate all unique unordered 2-tag pairs
    // and find the top 5 most frequently co-occurring tag combinations.
    // =========================================================================




    // SOLUTION 41:
    val q41 = dataRdd.flatMap { line =>
      val tags = line.split(",")(11).split(";").map(_.trim.toLowerCase).filter(_.nonEmpty).distinct.sorted
      for {
        i <- 0 until tags.length
        j <- (i + 1) until tags.length
      } yield ((tags(i), tags(j)), 1)
    }.reduceByKey(_ + _).map(_.swap).sortByKey(ascending = false).take(5)
    q41.foreach { case (count, pair) => println(s"Tag Pair (${pair._1}, ${pair._2}) -> $count orders") }




    // =========================================================================
    // QUESTION 42: Bounded In-Memory Sorting (Top 2 Products per Category without GroupByKey OOM)
    // Find the top 2 highest grossing products in each category using reduceByKey and bounded sorting.
    // =========================================================================




    // SOLUTION 42:
    val q42 = dataRdd.map { l =>
      val c = l.split(",")
      ((c(4).trim, c(5).trim), c(8).toDouble)
    }.reduceByKey(_ + _)
      .map { case ((cat, prod), rev) => (cat, List((prod, rev))) }
      .reduceByKey((listA, listB) => (listA ++ listB).sortBy(-_._2).take(2))
    q42.collect().foreach { case (cat, top2) =>
      println(s"=== Category: $cat ===")
      top2.foreach { case (prod, rev) => println(f"  Top Product: $prod%-28s $$$rev%7.2f") }
    }




    // =========================================================================
    // QUESTION 43: High-Value Customer Identification with Threshold Filtering
    // Identify customers who have spent more than $1,500 across at least 3 orders.
    // Output: (customer_id, total_spent, order_count).
    // =========================================================================




    // SOLUTION 43:
    val q43 = dataRdd.filter(_.split(",")(9).trim == "COMPLETED")
      .map(l => (l.split(",")(1).toInt, (l.split(",")(8).toDouble, 1)))
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .filter { case (_, (spent, count)) => spent > 1500.0 && count >= 3 }
    println(s"Total High-Value Customers meeting criteria: ${q43.count()}")




    // =========================================================================
    // QUESTION 44: Category Revenue Contribution Percentage via Broadcast Join
    // Compute total revenue per category, broadcast it, and calculate what percentage
    // each individual completed order contributes to its category total.
    // =========================================================================




    // SOLUTION 44:
    val catRevenueMap = dataRdd.filter(_.split(",")(9).trim == "COMPLETED")
      .map(l => (l.split(",")(4).trim, l.split(",")(8).toDouble))
      .reduceByKey(_ + _).collectAsMap()
    val bCastTotals = sc.broadcast(catRevenueMap)
    val q44 = dataRdd.filter(_.split(",")(9).trim == "COMPLETED").map { line =>
      val cols = line.split(",")
      val cat = cols(4).trim
      val amount = cols(8).toDouble
      val totalForCat = bCastTotals.value.getOrElse(cat, 1.0)
      (cols(0).toInt, cat, amount, (amount / totalForCat) * 100.0)
    }.take(5)
    q44.foreach { case (oid, cat, amt, pct) => println(f"Order $oid%5d ($cat%-15s) $$$amt%7.2f -> $pct%5.2f%% of Cat") }




    // =========================================================================
    // QUESTION 45: Moving Running Total Simulation via Accumulator
    // Use a Spark LongAccumulator to count how many orders have total_amount >= 500.0
    // while executing a map transformation without doing a separate action.
    // =========================================================================




    // SOLUTION 45:
    val highValAcc = sc.longAccumulator("HighValueOrdersCount")
    val processedOrders = dataRdd.map { line =>
      val amt = line.split(",")(8).toDouble
      if (amt >= 500.0) highValAcc.add(1)
      amt
    }
    processedOrders.count() // Trigger action
    println(s"Q45 Accumulator High Value Orders (>=$$500): ${highValAcc.value}")




    // =========================================================================
    // QUESTION 46: Secondary Sort via Composite Key (City Ascending, Total Amount Descending)
    // Sort all records first by City in alphabetical order, and within each city,
    // by total_amount in descending order. Print the top 6 records.
    // =========================================================================




    // SOLUTION 46:
    implicit val cityAndAmountOrdering: Ordering[(String, Double)] = new Ordering[(String, Double)] {
      override def compare(x: (String, Double), y: (String, Double)): Int = {
        val cityCmp = x._1.compareTo(y._1)
        if (cityCmp != 0) cityCmp else y._2.compareTo(x._2)
      }
    }
    val q46 = dataRdd.map { l =>
      val c = l.split(",")
      ((c(3).trim, c(8).toDouble), s"Order ${c(0)} by ${c(2)}")
    }.sortByKey().take(6)
    q46.foreach { case ((city, amt), details) => println(f"City: $city%-12s Amount: $$$amt%7.2f ($details)") }




    // =========================================================================
    // QUESTION 47: Repartition and Sort Within Partitions with Custom Partitioner
    // Partition the dataset by Category into 4 partitions using HashPartitioner and ensure
    // data within each partition is sorted by unit_price descending. Take 1 from each partition.
    // =========================================================================




    // SOLUTION 47:
    implicit val priceDescOrdering: Ordering[Double] = Ordering[Double].reverse
    val q47 = dataRdd.map { l =>
      val c = l.split(",")
      (c(4).trim, (c(6).toDouble, c(5).trim))
    }.repartitionAndSortWithinPartitions(new HashPartitioner(4))
      .mapPartitions(iter => iter.take(1))
    q47.collect().foreach(println)




    // =========================================================================
    // QUESTION 48: In-Memory Partition-Level Aggregation with mapPartitions
    // Using mapPartitions, compute in-memory summary per partition:
    // (partition_records_count, partition_total_sales, partition_avg_rating).
    // Save high-rated completed orders (rating >= 4.5) to a text file directory.
    // =========================================================================




    // SOLUTION 48:
    val q48 = dataRdd.mapPartitionsWithIndex { (idx, iter) =>
      var count = 0
      var totalSales = 0.0
      var totalRating = 0.0
      while (iter.hasNext) {
        val cols = iter.next().split(",")
        count += 1
        totalSales += cols(8).toDouble
        totalRating += cols(12).toDouble
      }
      val avgRating = if (count > 0) totalRating / count else 0.0
      Iterator.single((idx, count, totalSales, avgRating))
    }
    q48.collect().foreach { case (p, c, s, r) => println(f"Partition $p: Count=$c%3d, Sales=$$$s%9.2f, Avg Rating=$r%.2f") }

    // Save as text file (Direct Manual Action)
    val highRatedOrders = dataRdd.filter { l =>
      val c = l.split(",")
      c(9).trim == "COMPLETED" && c(12).toDouble >= 4.5
    }.map(l => l.split(",")(0) + "\t" + l.split(",")(5) + "\t" + l.split(",")(12))
    
    println(s"High rated completed orders count: ${highRatedOrders.count()}")
    // highRatedOrders.saveAsTextFile("practice/high_rated_orders")




    sc.stop()
  }
}
