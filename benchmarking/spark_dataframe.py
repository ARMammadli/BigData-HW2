from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg
from pyspark.sql.window import Window
import time

spark = SparkSession.builder \
    .appName("Benchmark Problem with DataFrames - Optimized") \
    .master("local[*]") \
    .getOrCreate()

data_path = "/data/data.csv"
df = spark.read.option("header", "true").csv(data_path)

start_time = time.time()

partition_window = Window.partitionBy("Car Make")

aggregated_df = df.filter(
    (col("Date").startswith("2022")) & (col("Sale Price").cast("double") > 20000)
).groupBy("Car Make").agg(
    sum(col("Sale Price").cast("double")).alias("Total Sales"),
    avg(col("Sale Price").cast("double")).alias("Average Sale Price")
)

total_sales = aggregated_df.agg(sum("Total Sales").alias("Global Total")).collect()[0]["Global Total"]

result_df = aggregated_df.withColumn(
    "Percentage Contribution",
    (col("Total Sales") / total_sales) * 100
).orderBy(col("Total Sales").desc())

result_df.show(10, truncate=False)

end_time = time.time()

execution_time = end_time - start_time
print(f"Execution Time: {execution_time:.2f} seconds")

spark.stop()