import time
import pandas as pd

# Load data
data_path = "/data/data.csv"  # Adjust path if necessary
df = pd.read_csv(data_path)

# Start timing
start_time = time.time()

# Perform all operations in a streamlined manner
filtered_df = df[(df["Date"].str.startswith("2022")) & (df["Sale Price"] > 20000)]

aggregated_df = filtered_df.groupby("Car Make", as_index=False).agg(
    Total_Sales=("Sale Price", "sum"),
    Average_Sale_Price=("Sale Price", "mean")
)

aggregated_df["Percentage_Contribution"] = (
    aggregated_df["Total_Sales"] / aggregated_df["Total_Sales"].sum() * 100
)

# Sort by Total Sales
final_df = aggregated_df.sort_values("Total_Sales", ascending=False)

# End timing
end_time = time.time()

# Display final result
print(final_df)

# Calculate and display execution time
execution_time = end_time - start_time
print(f"Execution Time: {execution_time:.2f} seconds")