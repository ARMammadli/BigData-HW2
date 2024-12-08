from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, count, desc
import time

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Car Sales Analysis using Spark DataFrames") \
    .master("local[*]") \
    .getOrCreate()

# Load the dataset into a DataFrame
data_path = "/data/data.csv"
df = spark.read.option("header", "true").csv(data_path)

# -------------------------
# Task 1: Select - Extract specific columns with filtering
print("1. Select:")
start_time = time.time()
selected_df = df.select("Car Make", "Sale Price", "Date") \
                .filter(col("Sale Price").isNotNull()) \
                .filter(col("Car Make") == "Toyota")
selected_df.show(10, truncate=False)  # Display first 10 rows
print(f"Time taken for select operation: {time.time() - start_time:.2f} seconds")
print("-" * 50)

# -------------------------
# Task 2: GroupBy - Average sale price by car make
print("2. GroupBy:")
start_time = time.time()
grouped_df = df.filter(col("Sale Price").isNotNull()) \
               .groupBy("Car Make") \
               .agg(
                   count("*").alias("Total Sales"),
                   avg(col("Sale Price").cast("double")).alias("Average Sale Price")
               )
grouped_df.show(10, truncate=False)  # Display first 10 rows
print(f"Time taken for groupBy operation: {time.time() - start_time:.2f} seconds")
print("-" * 50)

# -------------------------
# Task 3: OrderBy - Sort car makes by total revenue
print("3. OrderBy:")
start_time = time.time()
revenue_df = df.filter(col("Sale Price").isNotNull()) \
               .groupBy("Car Make") \
               .agg(sum(col("Sale Price").cast("double")).alias("Total Revenue")) \
               .orderBy(desc("Total Revenue"))
revenue_df.show(10, truncate=False)  # Display top 10 car makes by revenue
print(f"Time taken for orderBy operation: {time.time() - start_time:.2f} seconds")
print("-" * 50)

# Stop Spark Session
spark.stop()