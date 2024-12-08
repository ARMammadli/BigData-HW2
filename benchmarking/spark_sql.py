from pyspark.sql import SparkSession
import time

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Benchmark Problem with SQL") \
    .master("local[*]") \
    .getOrCreate()

# Load data into DataFrame
data_path = "/data/data.csv"  # Adjust the path to your dataset
df = spark.read.option("header", "true").csv(data_path)

# Register DataFrame as a temporary SQL view
df.createOrReplaceTempView("car_sales")

# SQL query to perform all tasks in one step
query = """
    WITH filtered_sales AS (
        SELECT *
        FROM car_sales
        WHERE YEAR(TO_DATE(Date)) = 2022 AND CAST(`Sale Price` AS DOUBLE) > 20000
    ),
    aggregated_sales AS (
        SELECT `Car Make`, 
               SUM(CAST(`Sale Price` AS DOUBLE)) AS Total_Sales, 
               AVG(CAST(`Sale Price` AS DOUBLE)) AS Average_Sale_Price
        FROM filtered_sales
        GROUP BY `Car Make`
    )
    SELECT `Car Make`, 
           Total_Sales, 
           Average_Sale_Price, 
           (Total_Sales / SUM(Total_Sales) OVER()) * 100 AS Percentage_Contribution
    FROM aggregated_sales
    ORDER BY Total_Sales DESC
"""

# Start the timer
start_time = time.time()

# Execute the query
result_df = spark.sql(query)

# Display the final result
result_df.show()

# End the timer
end_time = time.time()

# Calculate and print the execution time
execution_time = end_time - start_time
print(f"Execution Time: {execution_time:.2f} seconds")

# Stop Spark Session
spark.stop()