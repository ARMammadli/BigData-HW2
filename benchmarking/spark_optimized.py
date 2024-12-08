import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Benchmark Problem with DataFrames - Optimized with Cache and Parallelism") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.default.parallelism", "200") \
    .getOrCreate()

# Load the dataset into a DataFrame
data_path = "/data/data.csv"  # Adjust path if necessary
df = spark.read.option("header", "true").csv(data_path)

# Cache the DataFrame for reuse
df.cache()

# Start timing
start_time = time.time()

# Perform all operations to retrieve the final result
filtered_df = df.filter(
    (col("Date").startswith("2022")) & (col("Sale Price").cast("double") > 20000)
).cache()  # Cache filtered data

aggregated_df = filtered_df.groupBy("Car Make").agg(
    sum(col("Sale Price").cast("double")).alias("Total Sales"),
    avg(col("Sale Price").cast("double")).alias("Average Sale Price")
).cache()  # Cache aggregated data

# Calculate global total sales
global_total_sales = aggregated_df.select(sum("Total Sales").alias("Global Total")).first()["Global Total"]

# Add Percentage Contribution Column
result_df = aggregated_df.withColumn(
    "Percentage Contribution",
    (col("Total Sales") / global_total_sales) * 100
).orderBy(col("Total Sales").desc())

# Trigger action and display results
result_df.show(10, truncate=False)

# End timing
end_time = time.time()

# Calculate and display execution time
execution_time = end_time - start_time
print(f"Execution Time: {execution_time:.2f} seconds")

# Stop Spark Session
spark.stop()