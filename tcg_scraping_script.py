import os
import pandas as pd
import asyncio
from datetime import datetime
from playwright.async_api import async_playwright
from google.cloud import bigquery
from google.oauth2 import service_account
import json

# Constants for BigQuery
PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID")
DATASET_ID = "pokemon_data"
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.pokemon_prices"

from google.cloud import bigquery

def get_bigquery_client():
    # Uses GOOGLE_APPLICATION_CREDENTIALS path set in the workflow
    return bigquery.Client()



async def scrape_single_table(url, browser, retries=3):
    for attempt in range(retries):
        page = await browser.new_page()
        try:
            # Load the URL and wait for it to be ready
            await page.goto(url, timeout=180000)
            await page.wait_for_load_state("networkidle")
            await page.wait_for_selector("table", timeout=180000)

            # Verify that all rows have loaded by checking row count stability
            previous_row_count = 0
            stable_checks = 0
            max_stable_checks = 3  # Number of consecutive checks to confirm stability
            while stable_checks < max_stable_checks:
                rows = await page.query_selector_all("table tr")
                current_row_count = len(rows)
                if current_row_count == previous_row_count:
                    stable_checks += 1
                else:
                    stable_checks = 0  # Reset stability check if row count changes
                previous_row_count = current_row_count
                await asyncio.sleep(2)  # Wait a bit before the next check

            if current_row_count == 0:
                print(f"No rows found in table for {url} on attempt {attempt + 1}. Retrying...")
                await page.close()
                continue

            # Extract headers and data
            table_data = []
            target_columns = ["Product Name", "Printing", "Condition", "Rarity", "Number", "Market Price"]
            headers = [await cell.inner_text() for cell in await rows[0].query_selector_all("th")]
            print(f"Table headers for {url}: {headers}")

            indices = [headers.index(col) for col in target_columns if col in headers]
            for row in rows[1:]:
                cells = await row.query_selector_all("td")
                row_data = [await cells[i].inner_text() for i in indices if i < len(cells)]
                table_data.append(row_data)

            # Create DataFrame and add metadata
            df = pd.DataFrame(table_data, columns=[headers[i] for i in indices])
            df["source"] = url.split('/')[-1]
            df["scrape_date"] = datetime.now().date()

            print(f"Data scraped successfully from {url} - {len(df)} rows")
            await page.close()
            return df

        except Exception as e:
            print(f"Failed to scrape {url} on attempt {attempt + 1} due to error: {e}")
            await page.close()

        await asyncio.sleep(5)  # Wait before retrying

    print(f"Failed to scrape complete data from {url} after {retries} attempts.")
    return pd.DataFrame()  # Return empty if incomplete after retries

def delete_today_data():
    client = get_bigquery_client()
    table_id = f"{os.getenv('BIGQUERY_PROJECT_ID')}.pokemon_data.pokemon_prices"
    
    # Construct the query to delete rows with today's date
    today_date = datetime.now().date().isoformat()
    delete_query = f"""
        DELETE FROM `{table_id}`
        WHERE scrape_date = '{today_date}'
    """
    print(f"Deleting data from BigQuery with today's date ({today_date})...")
    query_job = client.query(delete_query)
    query_job.result()  # Wait for the delete job to complete
    print(f"Data with today's date ({today_date}) has been deleted from BigQuery.")

async def scrape_and_store_data(urls):
    all_data = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)

        for url in urls:
            print(f"Scraping {url}...")
            df = await scrape_single_table(url, browser)
            if not df.empty:
                all_data.append(df)

        await browser.close()

    # Concatenate all DataFrames after all URLs have been scraped
    if all_data:
        combined_data = pd.concat(all_data, ignore_index=True)
        print(f"Total rows scraped across all tables: {len(combined_data)}")
        upload_to_bigquery(combined_data)

def upload_to_bigquery(df):
    df['Market Price'] = df['Market Price'].astype(str)
    df['scrape_date'] = pd.to_datetime(df['scrape_date'], errors='coerce')
    df = df.dropna(subset=['scrape_date'])

    df = df.astype({
        'Product Name': 'string',
        'Printing': 'string',
        'Condition': 'string',
        'Rarity': 'string',
        'Number': 'string',
        'Market Price': 'string',
        'source': 'string',
        'scrape_date': 'datetime64[ns]'
    })

    client = get_bigquery_client()
    table_id = f"{os.getenv('BIGQUERY_PROJECT_ID')}.pokemon_data.pokemon_prices"
    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
    print("Uploading to BigQuery...")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"Uploaded {len(df)} rows to {table_id}.")

import argparse

def read_urls(filename):
    with open(filename, "r") as f:
        base_url = "https://www.tcgplayer.com/categories/trading-and-collectible-card-games/pokemon/price-guides/"
        urls = [base_url + line.strip() for line in f.readlines()]
    print(f"Loaded {len(urls)} URLs from {filename}")
    return urls


def main():
    parser = argparse.ArgumentParser(description="Run TCG scraping script with specific URL subset.")
    parser.add_argument("--urls-file", type=str, required=True, help="The file containing URLs to scrape")
    args = parser.parse_args()

    urls = read_urls(args.urls_file)
    asyncio.run(scrape_and_store_data(urls))

if __name__ == "__main__":
    main()
