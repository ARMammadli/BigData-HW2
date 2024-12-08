import pandas as pd
from datetime import datetime
import time

# Load the dataset
data_path = "../data/data.csv"  # Path to the dataset
df = pd.read_csv(data_path)

# -------------------------
# Task 1: Extract Sold_Year from Date column
start_time = time.time()
try:
    df['Sold_Year'] = pd.to_datetime(df['Date'], errors='coerce').dt.year.fillna("Unknown").astype(str)
    print("1. Map:")
    print(f"Time taken to extract Sold_Year: {time.time() - start_time:.2f} seconds")
except Exception as e:
    print(f"Error while extracting Sold_Year: {e}")
print("-" * 50)

# -------------------------
# Task 2: Filter data for 2022 year
start_time = time.time()
cars_sold_in_2022 = df[df['Sold_Year'] == "2022"]
filtered_count = len(cars_sold_in_2022)
print("2. Filter:")
print(f"Time taken for filter operation: {time.time() - start_time:.2f} seconds")
print(f"Cars sold in 2022: {filtered_count}")
print("-" * 50)

# -------------------------
# Task 3: Calculate total sales for 2022
start_time = time.time()
try:
    total_sale_price = pd.to_numeric(cars_sold_in_2022['Sale Price'], errors='coerce').sum()
    print("3. Reduce:")
    print(f"Time taken to calculate total sales: {time.time() - start_time:.2f} seconds")
    print(f"Total sales for 2022: ${total_sale_price:,.2f}")
except Exception as e:
    print(f"Error while calculating total sale price: {e}")
print("-" * 50)

# -------------------------
# Task 4: Count total transactions for 2022
start_time = time.time()
transaction_count = len(cars_sold_in_2022)
print("4. Count:")
print(f"Time taken to count transactions: {time.time() - start_time:.2f} seconds")
print(f"Total transactions for 2022: {transaction_count}")
print("-" * 50)

# -------------------------
# Task 5: Find top 3 car makes sold in 2022
start_time = time.time()
car_make_counts = cars_sold_in_2022['Car Make'].value_counts()
top_makes = car_make_counts.head(3)
print("5. Groupby:")
print(f"Time taken to find top 3 car makes: {time.time() - start_time:.2f} seconds")
print("Top 3 car makes sold in 2022:")
for make, count in top_makes.items():
    print(f"{make}: {count}")
print("-" * 50)