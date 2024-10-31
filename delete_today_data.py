# delete_today_data.py
import os
from datetime import datetime
from google.cloud import bigquery

def delete_today_data():
    client = bigquery.Client()
    table_id = f'{os.getenv("BIGQUERY_PROJECT_ID")}.pokemon_data.pokemon_prices'
    today_date = datetime.now().date().isoformat()
    delete_query = f'DELETE FROM `{table_id}` WHERE scrape_date = "{today_date}"'
    client.query(delete_query).result()
    print(f'Data with date {today_date} deleted from BigQuery.')

if __name__ == "__main__":
    delete_today_data()
