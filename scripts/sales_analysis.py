from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, DateType, StringType, StructField
from pyspark.sql.functions import month, year, quarter, count, countDistinct

# Initialize Spark session
spark = SparkSession.builder.appName("Sales Data Analysis").getOrCreate()

# Define schema for sales data
schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_date", DateType(), True),
    StructField("location", StringType(), True),
    StructField("source_order", StringType(), True)
])

# Load sales data
sale_df = spark.read.format("csv").option("header", "true").schema(schema).load("data/sales_csv.txt")

# Extract date parts
sale_df = sale_df.withColumn("order_year", year("order_date")) \
                 .withColumn("order_month", month("order_date")) \
                 .withColumn("order_quarter", quarter("order_date"))

# Load menu data
menu_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("price", StringType(), True)
])
menu_df = spark.read.format("csv").option("header", "true").schema(menu_schema).load("data/menu_csv.txt")

# Join dataframes
sales_joined = sale_df.join(menu_df, "product_id")

# Total spend per customer
total_amount_spent = sales_joined.groupBy("customer_id").agg({"price": "sum"}).orderBy("customer_id")

# Total spend per food item
total_spent_by_item = sales_joined.groupBy("product_name").agg({"price": "sum"}).orderBy("product_name")

# Sales by month, year, quarter
monthly_sales = sales_joined.groupBy("order_month").agg({"price": "sum"}).orderBy("order_month")
yearly_sales = sales_joined.groupBy("order_year").agg({"price": "sum"}).orderBy("order_year")
quarterly_sales = sales_joined.groupBy("order_quarter").agg({"price": "sum"}).orderBy("order_quarter")

# Most ordered items (Top 5 & Top 1)
most_ordered_items = sales_joined.groupBy("product_name").agg(count("product_id").alias("product_count")) \
                                 .orderBy("product_count", ascending=False)
top_5_items = most_ordered_items.limit(5)
top_1_item = most_ordered_items.limit(1)

# Unique restaurant visits per customer
restaurant_visits = sale_df.filter(sale_df.source_order == "Restaurant") \
                           .groupBy("customer_id") \
                           .agg(countDistinct("order_date").alias("unique_visits"))

# Sales by location and source
sales_by_location = sales_joined.groupBy("location").agg({"price": "sum"})
sales_by_source = sales_joined.groupBy("source_order").agg({"price": "sum"})

# Stop Spark session
spark.stop()
