"""
Script Name: tcg_card_scraping.py
Description:
    This script scrapes Pokémon card price data from specified URLs on the TCGPlayer website.
    The script loads URL extensions and expected row counts from a CSV file ("set_dictionary.csv"),
    navigates to each page, and verifies the row count of data tables before scraping.
    
    If the row count is as expected, the data is scraped and then uploaded to Google BigQuery
    in the 'pokemon_prices' table. Prior to scraping, the script deletes any records from BigQuery
    with today's date to avoid duplicating data.

Components:
    - delete_today_data: Removes today's data from BigQuery.
    - scrape_table_data: Navigates to and scrapes data from a single URL.
    - scrape_and_store_data: Main function coordinating deletion, scraping, and upload.
    - upload_to_bigquery: Uploads scraped data to BigQuery.

Environment Variables:
    - BIGQUERY_PROJECT_ID: Google Cloud Project ID for BigQuery access.

Dependencies:
    - pandas
    - asyncio
    - playwright.async_api (for web scraping)
    - google.cloud.bigquery (for BigQuery integration)
    
"""

# Modules
import os
import pandas as pd
import asyncio
from datetime import datetime
from playwright.async_api import async_playwright
from google.cloud import bigquery

# Constants for BigQuery Project
PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID")
DATASET_ID = "pokemon_data"
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.pokemon_prices"

# Function: Delete today's data from BigQuery
def delete_today_data():
    """
    Deletes records in the BigQuery table that match today's date.
    Ensures that duplicate data is not stored if the script is run multiple times in a day.
    """
    client = bigquery.Client()
    today_date = datetime.now().date().isoformat()
    delete_query = f"""
        DELETE FROM `{TABLE_ID}`
        WHERE scrape_date = '{today_date}'
    """
    print(f"Deleting data from BigQuery with today's date ({today_date})...")
    client.query(delete_query).result()
    print(f"Data with today's date ({today_date}) has been deleted from BigQuery.")

# Function: Scrape data from a single URL
async def scrape_table_data(url, browser, expected_rows):
    """
    Navigates to a specified URL and scrapes Pokémon card price data if the row count matches `expected_rows`.
    Will refresh the page up to 3 times if the row count is incorrect.

    Args:
        url (str): URL to scrape data from.
        browser (Browser): Playwright browser instance.
        expected_rows (int): Expected row count for data validation.

    Returns:
        pd.DataFrame: DataFrame containing scraped data or empty DataFrame if unsuccessful.
    """
    max_retries = 3
    for attempt in range(max_retries):
        page = await browser.new_page()
        try:
            await page.goto(url, timeout=180000)
            await page.wait_for_load_state("networkidle")

            # Select rows from the primary table on the page
            rows = page.locator("table tr")
            row_count = await rows.count()
            print(f"Attempt {attempt + 1}: Found {row_count} rows in the table for {url}")

            # If row count matches the expected, proceed to scrape
            if row_count == expected_rows + 1:
                data = []
                headers = [await cell.inner_text() for cell in await rows.nth(0).locator("th").all()]
                
                # Scrape data row by row
                for i in range(1, row_count):
                    cells = await rows.nth(i).locator("td").all()
                    row_data = [await cell.inner_text() for cell in cells]
                    data.append(row_data)

                df = pd.DataFrame(data, columns=headers)
                df["source"] = url.split('/')[-1]
                df["scrape_date"] = datetime.now().date()

                print(f"Scraped data successfully from {url} - {len(df)} rows")
                await page.close()
                return df
            else:
                print(f"Row count {row_count} does not match expected {expected_rows} for {url}. Retrying...")

        except Exception as e:
            print(f"Error on {url}, attempt {attempt + 1}: {e}")

        finally:
            await page.close()
            await asyncio.sleep(5)  # Delay before retry

    print(f"Failed to scrape complete data from {url} after {max_retries} attempts.")
    return pd.DataFrame()

# Main Function: Orchestrate the scraping and uploading process
async def scrape_and_store_data():
    """
    Deletes current day's data in BigQuery, scrapes data from URLs in set_dictionary.csv,
    and uploads the results to BigQuery.
    """
    # Step 1: Delete today's data
    delete_today_data()

    # Step 2: Load URLs and expected row counts
    set_df = pd.read_csv("set_dictionary.csv")
    all_data = []

    # Step 3: Initialize Playwright browser
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)

        for _, row in set_df.iterrows():
            set_extension = row['set']
            expected_rows = row['cards']
            url = f"https://www.tcgplayer.com/categories/trading-and-collectible-card-games/pokemon/price-guides/{set_extension}"
            print(f"Scraping {url} with expected rows: {expected_rows}")

            # Scrape data from the URL
            df = await scrape_table_data(url, browser, expected_rows)
            if not df.empty:
                all_data.append(df)

        await browser.close()

    # Step 4: Combine and upload scraped data
    if all_data:
        combined_data = pd.concat(all_data, ignore_index=True)
        print(f"Total rows scraped across all tables: {len(combined_data)}")
        upload_to_bigquery(combined_data)

# Function: Upload data to BigQuery
def upload_to_bigquery(df):
    """
    Uploads the given DataFrame to the BigQuery table.

    Args:
        df (pd.DataFrame): DataFrame containing the scraped data to upload.
    """
    # Convert columns to appropriate data types for BigQuery
    df = df.astype({
        'Product Name': 'string',
        'Market Price': 'string',
        'source': 'string',
        'scrape_date': 'datetime64[ns]'
    })

    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
    
    print("Uploading to BigQuery...")
    client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config).result()
    print(f"Uploaded {len(df)} rows to {TABLE_ID}.")

# Entry point: Run the asynchronous main function
if __name__ == "__main__":
    asyncio.run(scrape_and_store_data())
