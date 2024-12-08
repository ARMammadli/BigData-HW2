from pyspark.sql import SparkSession
from pyspark.sql.functions import year, col
import time

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Car Sales Analysis using Spark SQL") \
    .master("local[*]") \
    .getOrCreate()

# Load the dataset into a DataFrame
data_path = "/data/data.csv"
df = spark.read.option("header", "true").csv(data_path)

# Register DataFrame as a Temporary View
df.createOrReplaceTempView("car_sales")

# -------------------------
# Task 1: Aggregation - Calculate total sales for each car make
print("1. Aggregation:")
aggregation_query = """
    SELECT `Car Make`, COUNT(*) AS Total_Sales, SUM(CAST(`Sale Price` AS DOUBLE)) AS Total_Revenue
    FROM car_sales
    GROUP BY `Car Make`
    ORDER BY Total_Revenue DESC
"""
start_time = time.time()
aggregation_result = spark.sql(aggregation_query)
aggregation_result.show(truncate=False)  # Show top 10 results
print(f"Time taken for aggregation: {time.time() - start_time:.2f} seconds")
print("-" * 50)

# -------------------------
# Task 2: Filtering - Filter cars sold in the year 2022
print("2. Filtering:")
df_with_year = df.withColumn("Sold_Year", year(col("Date")))  # Extract Year from Date
df_with_year.createOrReplaceTempView("car_sales_with_year")

filtering_query = """
    SELECT *
    FROM car_sales_with_year
    WHERE Sold_Year = 2022
    LIMIT 10
"""
start_time = time.time()
filtering_result = spark.sql(filtering_query)
filtering_result.show(truncate=False)  # show only 10 rec
print(f"Time taken for filtering: {time.time() - start_time:.2f} seconds")
print(f"Total rows for 2022: {filtering_result.count()}")
print("-" * 50)

# -------------------------
# Task 3: Sorting - Find top 3 car makes sold in 2022
print("3. Sorting:")
sorting_query = """
    SELECT `Car Make`, COUNT(*) AS Total_Sales
    FROM car_sales_with_year
    WHERE Sold_Year = 2022
    GROUP BY `Car Make`
    ORDER BY Total_Sales DESC
    LIMIT 3
"""
start_time = time.time()
sorting_result = spark.sql(sorting_query)
sorting_result.show(truncate=False)
print(f"Time taken for sorting: {time.time() - start_time:.2f} seconds")
print("-" * 50)

# Stop Spark Session
spark.stop()