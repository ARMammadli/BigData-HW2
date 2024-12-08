from pyspark import SparkContext, SparkConf
from datetime import datetime
import time

# Initialize Spark Context
conf = SparkConf().setAppName("Car Sales Analysis").setMaster("local[*]")  # Use local mode for standalone execution
sc = SparkContext(conf=conf)

# Load the dataset into an RDD
data_path = "/data/data.csv"
raw_rdd = sc.textFile(data_path)

# Extract header
header = raw_rdd.first()
rdd = raw_rdd.filter(lambda line: line != header).map(lambda line: line.split(','))

# -------------------------
# Task 1: Create a "Year" column (use map)
def extract_year(row):
    try:
        date_obj = datetime.strptime(row[0], "%Y-%m-%d")  # Date column (index 0)
        row.append(str(date_obj.year))  # Add "Year" as the last column
    except Exception as e:
        row.append("Unknown")  # Handle parsing errors
    return row

start_time = time.time()
rdd_with_year = rdd.map(extract_year)
print("1. Map:")
print(f"Time taken to create Year column: {time.time() - start_time:.2f} seconds")
print("-" * 50)

# -------------------------
# Task 2: Filter data for 2022 year (use filter)
YEAR_IDX = len(rdd_with_year.first()) - 1  # Index for "Year"
filtered_rdd = rdd_with_year.filter(lambda row: row[YEAR_IDX] == "2022")

start_time = time.time()
filtered_count = filtered_rdd.count()
print("2. Filter:")
print(f"Time taken to filter data for 2022: {time.time() - start_time:.2f} seconds")
print(f"Total rows for 2022: {filtered_count}")
print("-" * 50)

# -------------------------
# Task 3: Calculate total sales for 2022 (use reduce)
SALE_PRICE_IDX = 6  # Index for "Sale Price"
sales_rdd = filtered_rdd.filter(lambda row: row[SALE_PRICE_IDX].replace('.', '', 1).isdigit()) \
                        .map(lambda row: float(row[SALE_PRICE_IDX]))

start_time = time.time()
total_sales = sales_rdd.reduce(lambda a, b: a + b)  # Sum sale prices
print("3. Reduce:")
print(f"Time taken to calculate total sales: {time.time() - start_time:.2f} seconds")
print(f"Total sales for 2022: ${total_sales:,.2f}")
print("-" * 50)

# -------------------------
# Task 4: Count total transactions for 2022 (use count)
start_time = time.time()
transaction_count = filtered_rdd.count()
print("4. Count:")
print(f"Time taken to count transactions: {time.time() - start_time:.2f} seconds")
print(f"Total transactions for 2022: {transaction_count}")
print("-" * 50)

# -------------------------
# Task 5: Find top 3 car makes sold in 2022 (use groupBy)
CAR_MAKE_IDX = 3  # Index for "Car Make"
car_make_rdd = filtered_rdd.map(lambda row: (row[CAR_MAKE_IDX], 1))  # (Car Make, 1)
grouped_rdd = car_make_rdd.reduceByKey(lambda a, b: a + b)  # (Car Make, count)

start_time = time.time()
top_makes = grouped_rdd.takeOrdered(3, key=lambda x: -x[1])  # Get top 3 car makes by count
print("5. Groupby:")
print(f"Time taken to find top 3 car makes: {time.time() - start_time:.2f} seconds")
print("Top 3 car makes sold in 2022:")
for make, count in top_makes:
    print(f"{make}: {count}")
print("-" * 50)

# Stop Spark Context
sc.stop()