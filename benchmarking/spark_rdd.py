from pyspark import SparkContext, SparkConf
import time

# Initialize Spark Context
conf = SparkConf().setAppName("Benchmarking with RDD").setMaster("local[*]")
sc = SparkContext(conf=conf)

# Load the dataset into an RDD
data_path = "/data/data.csv"
raw_rdd = sc.textFile(data_path)

# Extract header
header = raw_rdd.first()
rdd = raw_rdd.filter(lambda line: line != header).map(lambda line: line.split(','))

# Define column indices based on pandas processing
DATE_IDX = 0         # Index for "Date"
CAR_MAKE_IDX = 3     # Index for "Car Make"
SALE_PRICE_IDX = 6   # Index for "Sale Price"

# Start benchmarking
start_time = time.time()

# Filter rows for 2022 and Sale Price > 20,000
filtered_rdd = rdd.filter(
    lambda row: row[DATE_IDX].startswith("2022") and
                row[SALE_PRICE_IDX].replace('.', '', 1).isdigit() and
                float(row[SALE_PRICE_IDX]) > 20000
)

# Map to (Car Make, Sale Price)
car_make_rdd = filtered_rdd.map(lambda row: (row[CAR_MAKE_IDX], float(row[SALE_PRICE_IDX])))

# Aggregate Total Sales and Average Sale Price
aggregated_rdd = car_make_rdd.aggregateByKey(
    (0, 0),  # (Sum of Sale Prices, Count)
    lambda acc, value: (acc[0] + value, acc[1] + 1),  # Local aggregation
    lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])  # Global aggregation
)

# Compute total revenue for percentage contribution
total_revenue = aggregated_rdd.map(lambda x: x[1][0]).sum()

# Compute final RDD with Total Sales, Average Sale Price, and Percentage Contribution
final_rdd = aggregated_rdd.mapValues(lambda v: (v[0], v[0] / v[1], (v[0] / total_revenue) * 100))

# Sort by Total Sales in descending order
sorted_rdd = final_rdd.sortBy(lambda x: -x[1][0])

# Collect final results
final_result = sorted_rdd.collect()

# Stop benchmarking
end_time = time.time()

# Stop Spark Context
sc.stop()

print("Final Results:")
print("Car Make | Total Sales | Average Sale Price | Percentage Contribution")
for car_make, (total_sales, avg_price, pct_contribution) in final_result:
    print(f"{car_make}: ${total_sales:,.2f} | ${avg_price:,.2f} | {pct_contribution:.2f}%")

# Calculate and display execution time
execution_time = end_time - start_time
print(f"Execution Time: {execution_time:.2f} seconds")
