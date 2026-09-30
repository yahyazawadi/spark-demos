package practice

import org.apache.spark.sql.{SparkSession, DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

/**
 * =========================================================================================
 *                      SPARK SQL & DATAFRAMES & DATASETS MASTER PRACTICE
 * =========================================================================================
 * Hand-crafted based precisely on "Spark SQL (2).pdf" lecture notes (All 5 Pages):
 *  - Page 1: SparkSession builder, master("local"), appName, getOrCreate, read json/csv/text, show(10), select columns
 *  - Page 2: filter/where (=== "nablus"), groupBy, count, sum, avg, orderBy, sort ($"col".asc / desc), write.json
 *  - Page 3: GitHub example: read.json, groupBy city count, filter fname/lname, sort count desc,
 *            endsWith("Plaza").count(), join cancelled orders with customers, groupBy customerId/fname/lname count sort limit 5
 *  - Page 4: transform(withMagic) using when/otherwise lit(1)/lit(0), UDF definition & registration (replace & toUpperCase),
 *            distinct().count(), UDF counting capitalized words (scalaGetNoCap)
 *  - Page 5: udf withColumn, select($"id", $"id" + 1), filter($"id" >= 10), createOrReplaceTempView,
 *            spark.sql("select * from wikiTable"), case class typed Dataset as[WikiDataClass], typed map and filter
 *
 * Structure per question:
 *   - Question description
 *   - 4 blank lines
 *   - Clean, idiomatic Scala / Spark SQL solution
 * =========================================================================================
 */

// Strongly typed case class for Dataset operations (matching lecture notes p. 5)
case class OrderRecord(
  order_id: Int,
  customer_id: Int,
  customer_name: String,
  city: String,
  category: String,
  product_name: String,
  unit_price: Double,
  quantity: Int,
  total_amount: Double,
  order_status: String,
  order_date: String,
  tags: String,
  rating: Double
)

case class WikiRecord(id: Int, title: String)

object SparkSqlMasterPractice {

  def main(args: Array[String]): Unit = {

    // =====================================================================================
    // SECTION 1: SPARK SESSION INITIALIZATION (Page 1)
    // =====================================================================================

    // Q1: Initialize a SparkSession with appName "SparkSqlPractice", running in local mode master("local[*]"), and getOrCreate().




    val spark: SparkSession = SparkSession.builder()
      .appName("SparkSqlPractice")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // File paths
    val csvPath = "data/practice_dataset/sales_and_orders_1000.csv"


    // =====================================================================================
    // SECTION 2: READING DIFFERENT FORMATS & INITIAL DATAFRAME (Pages 1 & 3 & 5)
    // =====================================================================================

    // Q2: Read the sales_and_orders_1000.csv file into a DataFrame, enabling header=true and inferSchema=true (Page 1).




    val df: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvPath)


    // Q3: Read a plain text file directly using spark.read.text(path) as noted in Page 1 (read.text).




    val textDf: DataFrame = spark.read.text(csvPath)
    textDf.show(2, truncate = false)


    // Q4: Display the schema of the loaded DataFrame and show the first 10 rows (Page 1: df.show(10)).




    df.printSchema()
    df.show(10)


    // =====================================================================================
    // SECTION 3: BASIC PROJECTIONS (select) (Pages 1 & 5)
    // =====================================================================================

    // Q5: Select only specific columns: "customer_id" and "city" (matching Page 1: df.select("customer_id", "customer_city")).




    val selectedCols = df.select("customer_id", "city")
    selectedCols.show(5)


    // Q6: Increment an integer ID column by 1 using select($"id", $"id" + 1) (matching Page 5: select($"id", $"id" + 1, $"title")).




    val incrementedSelect = df.select($"order_id", ($"customer_id" + 1).as("next_customer_id"), $"product_name")
    incrementedSelect.show(5)


    // Q7: Select "product_name", "quantity", and calculate a new projected column "calculated_total" multiplying "quantity" by "unit_price".




    val projectedTotal = df.select(
      $"product_name",
      $"quantity",
      ($"quantity" * $"unit_price").as("calculated_total")
    )
    projectedTotal.show(5)


    // =====================================================================================
    // SECTION 4: FILTERING & WHERE CONDITIONS (Pages 2, 3, 5)
    // =====================================================================================

    // Q8: Filter DataFrame where city equals "Nablus" (Page 2: df.filter($"city" === "nablus") / where).




    val nablusOrders = df.filter($"city" === "Nablus")
    nablusOrders.show(5)


    // Q9: Filter by multiple names: fname === "Osaid" && lname === "Beshtawi" (Page 3 GitHub example).




    val specificNames = df.filter($"customer_name" === "Osaid Beshtawi" || $"customer_name" === "Sara Khalil")
    specificNames.show(5)


    // Q10: Filter with numeric comparison: where ID/rating >= 4.5 (matching Page 5: df.filter($"id" >= 10)).




    val highRatedOrders = df.filter($"rating" >= 4.5)
    highRatedOrders.show(5)


    // Q11: Filter strings ending with a specific suffix and count matches (Page 3: df.filter($"street".endsWith("Plaza")).count()).




    val endsWithCount: Long = df.filter($"product_name".endsWith("Oak") || $"product_name".endsWith("Dual")).count()
    println(s"Products ending with Oak/Dual count: $endsWithCount")


    // =====================================================================================
    // SECTION 5: GROUPING & AGGREGATIONS (groupBy, count, sum, avg) (Pages 2 & 3)
    // =====================================================================================

    // Q12: Count the number of customers/orders in each city (Page 2 & 3: df.groupBy("city").count()).




    val countByCity = df.groupBy("city").count()
    countByCity.show()


    // Q13: Calculate the total sum of total_amount for each category (Page 2: df.groupBy("department").sum("salary")).




    val revenueByCategory = df.groupBy("category").sum("total_amount")
    revenueByCategory.show()


    // Q14: Calculate the average rating for each category (Page 2: df.groupBy("department").avg("salary")).




    val avgRatingByCategory = df.groupBy("category").avg("rating")
    avgRatingByCategory.show()


    // Q15: In a single aggregation, group by "category" and compute: total order count, sum of total_amount, average unit_price, and max quantity.




    val multiAggByCategory = df.groupBy("category").agg(
      count("order_id").as("order_count"),
      sum("total_amount").as("total_revenue"),
      round(avg("unit_price"), 2).as("avg_unit_price"),
      max("quantity").as("max_quantity")
    )
    multiAggByCategory.show()


    // =====================================================================================
    // SECTION 6: SORTING & ORDERING (sort, orderBy, asc, desc) (Pages 2 & 3)
    // =====================================================================================

    // Q16: Sort results by total_amount in ascending and descending order (Page 2: df.sort($"salary".asc) & df.sort($"salary".desc)).




    val sortedAsc = df.sort($"total_amount".asc)
    val sortedDesc = df.sort($"total_amount".desc)
    sortedDesc.show(5)


    // Q17: Sort cities by customer order count descending (Page 3: df.groupBy("city").count().sort($"count".desc)).




    val sortedCitiesByCount = df.groupBy("city")
      .count()
      .sort($"count".desc)
    sortedCitiesByCount.show()


    // =====================================================================================
    // SECTION 7: DISTINCT & LIMIT (Pages 3 & 4)
    // =====================================================================================

    // Q18: Calculate distinct users count without duplicates (Page 4: wikiData.select("username").distinct().count()).




    val distinctCustomerCount: Long = df.select("customer_id").distinct().count()
    println(s"Total Distinct Customers: $distinctCustomerCount")


    // Q19: Find top 5 orders by unit_price using sort and limit(5) (matching Page 3 limit(5)).




    val top5ExpensiveItems = df.select("order_id", "product_name", "unit_price")
      .sort($"unit_price".desc)
      .limit(5)
    top5ExpensiveItems.show()


    // =====================================================================================
    // SECTION 8: JOINS & MULTI-DATAFRAME RELATIONSHIPS (Page 3)
    // =====================================================================================

    // Q20: Filter cancelled orders and perform INNER JOIN with customer profiles (Page 3: cancelled.join(customers, ...)).




    val customerProfiles = df.select($"customer_id", $"customer_name", $"city").distinct()
    val cancelledOrders = df.filter($"order_status" === "CANCELLED")

    val joinedCancelled = cancelledOrders.join(
      customerProfiles,
      cancelledOrders("customer_id") === customerProfiles("customer_id")
    ).select(
      cancelledOrders("order_id"),
      customerProfiles("customer_id"),
      customerProfiles("customer_name"),
      cancelledOrders("product_name"),
      cancelledOrders("total_amount")
    )
    joinedCancelled.show(5)


    // Q21: Find top 3 customers with most cancelled orders (Page 3: Top_5 = joined.groupBy("customer_id", "fname", "lname").count().sort($"count".desc).limit(5)).




    val topCancelledCustomers = joinedCancelled.groupBy("customer_id", "customer_name")
      .count()
      .sort($"count".desc)
      .limit(3)
    topCancelledCustomers.show()


    // =====================================================================================
    // SECTION 9: CONDITIONAL LOGIC & withColumn (Pages 4 & 5)
    // =====================================================================================

    // Q22: Add column "magic" using when/otherwise: if product starts with "Wireless" set 1, else 0 (Page 4: when($"title".startsWith("magic"), lit(1)).otherwise(lit(0))).




    val withMagicCol = df.withColumn(
      "magic",
      when($"product_name".startsWith("Wireless"), lit(1)).otherwise(lit(0))
    )
    withMagicCol.select("product_name", "magic").show(10)


    // =====================================================================================
    // SECTION 10: HIGHER-ORDER DATAFRAME TRANSFORMATION (df.transform) (Page 4)
    // =====================================================================================

    // Q23: Define a transformation function `withMagic(df: DataFrame): DataFrame` and invoke it via `df.transform(withMagic)` (Page 4).




    def withMagic(inputDf: DataFrame): DataFrame = {
      inputDf.withColumn("magic", when($"product_name".startsWith("Wireless"), lit(1)).otherwise(lit(0)))
    }

    val transformedDf = df.transform(withMagic)
    transformedDf.select("product_name", "magic").show(5)


    // =====================================================================================
    // SECTION 11: USER DEFINED FUNCTIONS (UDFs) (Pages 4 & 5)
    // =====================================================================================

    // Q24: Define UDF `upper` to clean hyphens/characters and uppercase text (Page 4: val upper = (str: String) => ... val upperUDF = udf(upper)).




    val upperScalaFunc = (str: String) => {
      if (str == null) ""
      else str.replace("-", " ").trim.toUpperCase
    }
    val upperUDF = udf(upperScalaFunc)

    val withUpperDf = df.withColumn("cap_product", upperUDF($"product_name"))
    withUpperDf.select("product_name", "cap_product").show(5)


    // Q25: Define UDF `scalaGetNoCap` to count how many words start with an uppercase letter (Page 4-5: udf(scalaGetNoCap)).




    val scalaGetNoCap = (str: String) => {
      if (str == null) 0
      else {
        var c = 0
        str.split(" ").foreach { w =>
          if (w.nonEmpty && Character.isUpperCase(w.charAt(0))) c += 1
        }
        c
      }
    }
    val noOfCapUDF = udf(scalaGetNoCap)

    val withNoOfCap = df.withColumn("noofcap", noOfCapUDF($"product_name"))
    withNoOfCap.select("product_name", "noofcap").show(5)


    // Q26: Register a UDF with spark.udf.register so it can be called from direct SQL statements.




    spark.udf.register("calcTax", (price: Double) => math.round(price * 0.16 * 100.0) / 100.0)


    // =====================================================================================
    // SECTION 12: DIRECT SQL QUERIES (Page 5)
    // =====================================================================================

    // Q27: Create or replace a temporary table view named "wikiTable" (Page 5: wikiData.createOrReplaceTempView("wikiTable")).




    df.createOrReplaceTempView("wikiTable")


    // Q28: Execute SQL query using spark.sql("SELECT * FROM wikiTable ...") (Page 5).




    val sqlResult = spark.sql("SELECT order_id, customer_name, city, total_amount FROM wikiTable WHERE total_amount > 300 ORDER BY total_amount DESC LIMIT 5")
    sqlResult.show()


    // =====================================================================================
    // SECTION 13: STRONGLY-TYPED DATASETS (Page 5)
    // =====================================================================================

    // Q29: Convert DataFrame to typed Dataset using `as[OrderRecord]` (Page 5: val wikiDS = wikiData.as[WikiDataClass]).




    val ordersDS: Dataset[OrderRecord] = df.as[OrderRecord]
    ordersDS.show(3)


    // Q30: Perform typed Dataset operations: ds.map(_.title) and ds.filter(_.id > 1000) (Page 5: wikiDS.map(_.title), wikiDS.filter(_.id > 1000)).




    val titlesDS: Dataset[String] = ordersDS.map(r => r.product_name)
    val filteredDS: Dataset[OrderRecord] = ordersDS.filter(r => r.order_id > 1020)

    titlesDS.show(5)
    filteredDS.show(5)


    // =====================================================================================
    // SECTION 14: SAVING AND WRITING RESULTS (Pages 2, 3, 5)
    // =====================================================================================

    // Q31: Save DataFrame to JSON format on disk (Page 2 & 3: df.write.json("path")).




    val jsonOutputPath = "data/practice_dataset/output_sparksql_cities_json"
    countByCity.write
      .mode("overwrite")
      .json(jsonOutputPath)


    // Q32: Save DataFrame to Parquet format with compression and reload it (Page 3 & 5: Parquet compressed format, spark.read.parquet).




    val parquetOutputPath = "data/practice_dataset/output_sparksql_completed_parquet"
    df.filter($"order_status" === "COMPLETED")
      .write
      .mode("overwrite")
      .parquet(parquetOutputPath)

    val reloadedParquet = spark.read.parquet(parquetOutputPath)
    println(s"Reloaded Parquet Count: ${reloadedParquet.count()}")


    // =====================================================================================
    // SECTION 15: ADVANCED & INTRICATE SPARK SQL CONCEPTS (Q33 - Q50)
    // =====================================================================================

    // Q33: Window Ranking: Find the top 2 highest-amount orders for EACH category using dense_rank() over a partitioned Window.




    val categoryWindow = Window.partitionBy("category").orderBy($"total_amount".desc)
    val rankedOrders = df.withColumn("rank_in_cat", dense_rank().over(categoryWindow))
      .filter($"rank_in_cat" <= 2)
    rankedOrders.select("category", "product_name", "total_amount", "rank_in_cat").show(10)


    // Q34: Window Cumulative Sum (Running Total): Calculate a running total of order amounts for each customer over time ordered by order_date.




    val customerWindow = Window.partitionBy("customer_id")
      .orderBy("order_date")
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val runningTotalDf = df.withColumn("running_spent", round(sum("total_amount").over(customerWindow), 2))
    runningTotalDf.select("customer_id", "order_date", "total_amount", "running_spent").show(10)


    // Q35: Window Lag/Lead: Detect customer purchasing gaps by computing the previous order date and the difference in days using lag() and datediff().




    val orderDateWindow = Window.partitionBy("customer_id").orderBy("order_date")
    val withPreviousDate = df.withColumn("prev_order_date", lag("order_date", 1).over(orderDateWindow))
      .withColumn("days_since_last_order", datediff(to_date($"order_date"), to_date($"prev_order_date")))
    withPreviousDate.filter($"prev_order_date".isNotNull)
      .select("customer_id", "order_date", "prev_order_date", "days_since_last_order")
      .show(10)


    // Q36: Pivot Table: Create a matrix showing the total revenue with 'city' as rows and 'category' as pivoted columns.




    val salesPivot = df.groupBy("city")
      .pivot("category")
      .sum("total_amount")
    salesPivot.show(5)


    // Q37: Complex Array Manipulation: Split 'tags' into an array, find its length using size(), filter arrays containing 'bestseller' with array_contains(), and sort the array.




    val arrayOpsDf = df.withColumn("tag_list", split($"tags", ";"))
      .withColumn("tag_count", size($"tag_list"))
      .withColumn("is_bestseller", array_contains($"tag_list", "bestseller"))
      .withColumn("sorted_tags", sort_array($"tag_list"))
    arrayOpsDf.select("product_name", "tag_count", "is_bestseller", "sorted_tags").show(5)


    // Q38: Handling Nested Structs: Bundle product information ('product_name', 'unit_price', 'rating') into a single nested Struct column named 'product_details'.




    val structDf = df.withColumn("product_details", struct($"product_name", $"unit_price", $"rating"))
    structDf.select($"order_id", $"product_details", $"product_details.rating".as("extracted_rating")).show(5)


    // Q39: Date & Time Extraction: Extract year, month, day of week, and format order_date into 'dd/MM/yyyy' string format.




    val dateFeaturesDf = df.withColumn("order_year", year(to_timestamp($"order_date")))
      .withColumn("order_month", month(to_timestamp($"order_date")))
      .withColumn("day_of_week", date_format(to_timestamp($"order_date"), "EEEE"))
      .withColumn("formatted_date", date_format(to_timestamp($"order_date"), "dd/MM/yyyy"))
    dateFeaturesDf.select("order_date", "order_year", "order_month", "day_of_week", "formatted_date").show(5)


    // Q40: Advanced Regex Extraction: Extract the numerical portion (e.g., '15', '20', '90x60') from product_name using regexp_extract().




    val regexDf = df.withColumn("spec_number", regexp_extract($"product_name", "(\\d+)", 1))
    regexDf.filter($"spec_number" =!= "")
      .select("product_name", "spec_number")
      .show(10)


    // Q41: Null / NaN Handling: Demonstrate coalesce(), fillna(), and dropna() to handle missing or null data safely.




    val safeData = df.withColumn("safe_tags", coalesce($"tags", lit("no-tag")))
      .na.fill(Map("total_amount" -> 0.0, "city" -> "Unknown"))
      .na.drop(Seq("customer_id", "order_id"))
    safeData.select("order_id", "city", "safe_tags").show(5)


    // Q42: Broadcast Hash Join: Efficiently join a large DataFrame with a small reference DataFrame to avoid heavy shuffle across nodes.




    val smallCategoryTaxDf = spark.createDataFrame(Seq(
      ("Electronics", 0.18),
      ("Books", 0.05),
      ("Clothing", 0.12),
      ("Furniture", 0.15)
    )).toDF("cat_name", "tax_rate")

    val broadcastJoinedDf = df.join(broadcast(smallCategoryTaxDf), $"category" === $"cat_name", "left")
      .withColumn("tax_amount", round($"total_amount" * coalesce($"tax_rate", lit(0.10)), 2))
    broadcastJoinedDf.select("order_id", "category", "total_amount", "tax_rate", "tax_amount").show(5)


    // Q43: Multi-Level Grouping with rollup(): Compute subtotals for (city, category), (city), and grand total in one query.




    val rollupSummary = df.rollup("city", "category")
      .agg(sum("total_amount").as("subtotal_sales"))
      .orderBy($"city".asc_nulls_last, $"category".asc_nulls_last)
    rollupSummary.show(15)


    // Q44: Multi-Level Grouping with cube(): Compute all possible combination subtotals across 'city' and 'order_status'.




    val cubeSummary = df.cube("city", "order_status")
      .agg(count("order_id").as("order_count"))
    cubeSummary.show(15)


    // Q45: Safe Schema Enforcement with StructType: Define an explicit Programmatic Schema instead of inferSchema to prevent type coercion bugs.




    val customExplicitSchema = StructType(Array(
      StructField("order_id", IntegerType, nullable = false),
      StructField("customer_id", IntegerType, nullable = false),
      StructField("customer_name", StringType, nullable = true),
      StructField("city", StringType, nullable = true),
      StructField("category", StringType, nullable = true),
      StructField("product_name", StringType, nullable = true),
      StructField("unit_price", DoubleType, nullable = true),
      StructField("quantity", IntegerType, nullable = true),
      StructField("total_amount", DoubleType, nullable = true),
      StructField("order_status", StringType, nullable = true),
      StructField("order_date", StringType, nullable = true),
      StructField("tags", StringType, nullable = true),
      StructField("rating", DoubleType, nullable = true)
    ))

    val strictDf = spark.read
      .option("header", "true")
      .schema(customExplicitSchema)
      .csv(csvPath)
    strictDf.printSchema()


    // Q46: Spark Catalyst Optimization Inspection: Print the Parsed Logical, Analyzed Logical, Optimized Logical, and Physical Execution Plans using explain(true).




    val complexQuery = df.filter($"order_status" === "COMPLETED")
      .groupBy("city")
      .agg(sum("total_amount").as("revenue"))
      .sort($"revenue".desc)

    println("=== CATALYST OPTIMIZER DETAILED EXECUTION PLAN ===")
    complexQuery.explain(true)


    // Q47: Repartition vs Coalesce: Optimize partitioning by decreasing partition count without shuffle using coalesce(1), vs increasing partition count with full shuffle using repartition(4, $"city").




    println(s"Default Partitions: ${df.rdd.getNumPartitions}")
    val coalescedDf = df.coalesce(1)
    println(s"After Coalesce(1): ${coalescedDf.rdd.getNumPartitions}")

    val repartitionedByCity = df.repartition(4, $"city")
    println(s"After Repartition(4, city): ${repartitionedByCity.rdd.getNumPartitions}")


    // Q48: Caching & Persistence Strategies: Cache a heavy DataFrame into memory/disk, trigger an action to materialize cache, inspect storage, and unpersist().




    import org.apache.spark.storage.StorageLevel

    val cachedDf = df.filter($"total_amount" > 100.0).persist(StorageLevel.MEMORY_AND_DISK)
    val materializedCount = cachedDf.count() // Triggers caching action
    println(s"Materialized and Cached Row Count: $materializedCount")
    println(s"Is DataFrame Cached? ${cachedDf.storageLevel.useMemory}")

    cachedDf.unpersist() // Free memory


    // Q49: SQL Anti-Join & Semi-Join: Find all customer profiles who have NEVER had a cancelled order using 'left_anti' join.




    val allCustomers = df.select("customer_id", "customer_name", "city").distinct()
    val customersWithCancellations = df.filter($"order_status" === "CANCELLED").select("customer_id").distinct()

    val customersWithZeroCancellations = allCustomers.join(
      customersWithCancellations,
      Seq("customer_id"),
      "left_anti"
    )
    println(s"Customers with zero cancellations: ${customersWithZeroCancellations.count()}")
    customersWithZeroCancellations.show(5)


    // Q50: Converting DataFrame to RDD with Row Pattern Matching: Extract data safely by converting DataFrame back to RDD[Row], pattern matching values and computing total revenue.




    val rddFromDf = df.select("order_id", "total_amount", "order_status").rdd

    val completedRevenueFromRdd: Double = rddFromDf.flatMap { row =>
      val status = row.getAs[String]("order_status")
      val amount = row.getAs[Double]("total_amount")
      if (status == "COMPLETED") Some(amount) else None
    }.reduce(_ + _)

    println(s"Total Completed Revenue computed via extracted RDD[Row]: $$${math.round(completedRevenueFromRdd * 100.0) / 100.0}")

    // Stop Spark Session
    spark.stop()
  }
}
