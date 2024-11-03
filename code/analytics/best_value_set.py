import os
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
import db_dtypes

# Constants for BigQuery Project
PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID")
DATASET_ID = "pokemon_data"
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.pokemon_prices"

# Initialize the BigQuery client
client = bigquery.Client()

# Load the sets from the CSV file
pack_set_df = pd.read_csv("data/pack_set_dictionary.csv")
sets = pack_set_df["set"].tolist()
today_date = datetime.now().date().isoformat()

# Format the SQL query
set_list_sql = "', '".join(sets)  # Join sets with quotes and commas
sql_query = f"""
SELECT *
FROM `{TABLE_ID}`
WHERE scrape_date = '{today_date}'
  AND source IN ('{set_list_sql}');
"""

# Execute the query and convert the result to a DataFrame
query_job = client.query(sql_query)
result_df = query_job.to_dataframe()

# Ensure 'source' column is named 'set' in result_df if necessary
result_df = result_df.rename(columns={"source": "set"})

# Remove the dollar sign and convert 'Market Price' to numeric
result_df["Market Price"] = result_df["Market Price"].replace('[\$,]', '', regex=True).astype(float)

result_df["Market Price"] = pd.to_numeric(result_df["Market Price"], errors='coerce')


# Group by 'Rarity' and 'set', and calculate the mean for 'Market Price'
grouped_df = result_df.groupby(["set", "Rarity"])["Market Price"].mean().reset_index()

print(grouped_df.head())

# Load the pull rates data
pull_rates_df = pd.read_csv("data/pull_rates.csv")

print(pull_rates_df.head())

# Ensure "Probability" is numeric, handling any conversion errors
pull_rates_df["Probability"] = pd.to_numeric(pull_rates_df["Probability"], errors='coerce')

# Perform the inner join on 'set' column
merged_df = pd.merge(grouped_df, pull_rates_df, on="set", how="left")

print(merged_df.head())

# Calculate the 'value' column by multiplying 'Probability' with 'Market Price'
merged_df["value"] = merged_df["Probability"] * merged_df["Market Price"]

# Group by 'set' and sum the 'value'
set_value_sum = merged_df.groupby("set")["value"].sum().reset_index()

set_value_sum.to_csv("data/set_pull_values.csv", index=False)
print("CSV file written to data/set_pull_values.csv")


