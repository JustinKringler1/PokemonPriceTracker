import os
import pandas as pd
import asyncio
from datetime import datetime
from playwright.async_api import async_playwright
from google.cloud import bigquery

# Constants for BigQuery
PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID")
DATASET_ID = "pokemon_data"
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.pokemon_prices"
RETRY_LIMIT = 3  # Number of retries for each URL if row count does not match

# Load set dictionary
set_dict = pd.read_csv("set_dictionary.csv").set_index("set").to_dict()["cards"]

async def scrape_single_table(url, browser, expected_rows):
    retries = 0
    page = await browser.new_page()
    
    while retries < RETRY_LIMIT:
        try:
            # Load the page and wait for it to be ready
            await page.goto(url, timeout=180000)
            await page.wait_for_load_state("networkidle")
            await page.wait_for_selector("table", timeout=180000)

            # Extract rows
            rows = await page.query_selector_all("table tr")
            current_row_count = len(rows)

            if current_row_count >= expected_rows:
                # Scrape if the row count matches or exceeds expectations
                table_data = []
                headers = [await cell.inner_text() for cell in await rows[0].query_selector_all("th")]
                target_columns = ["Product Name", "Printing", "Condition", "Rarity", "Number", "Market Price"]
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
            else:
                print(f"Row count {current_row_count} less than expected {expected_rows}. Retrying ({retries+1}/{RETRY_LIMIT})...")
                retries += 1
                await asyncio.sleep(5)  # Wait before retrying
                await page.reload()
        except Exception as e:
            print(f"Failed attempt {retries + 1} for {url}: {e}")
            retries += 1
            await page.reload()

    print(f"Failed to scrape complete data from {url} after {RETRY_LIMIT} attempts.")
    await page.close()
    return pd.DataFrame()  # Return empty DataFrame if unsuccessful

def delete_today_data():
    client = bigquery.Client()
    table_id = f"{PROJECT_ID}.{DATASET_ID}.pokemon_prices"
    today_date = datetime.now().date().isoformat()
    delete_query = f"DELETE FROM `{table_id}` WHERE scrape_date = '{today_date}'"
    print(f"Deleting today's data ({today_date}) from BigQuery...")
    client.query(delete_query).result()
    print("Data deletion complete.")

async def scrape_and_store_data():
    delete_today_data()  # Delete data with today's date before starting

    # Set up Playwright browser
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        all_data = []
        
        for set_name, expected_rows in set_dict.items():
            url = f"https://www.tcgplayer.com/categories/trading-and-collectible-card-games/pokemon/price-guides/{set_name}"
            print(f"Scraping {url} with expected rows: {expected_rows}")
            df = await scrape_single_table(url, browser, expected_rows)
            if not df.empty:
                all_data.append(df)

        await browser.close()

    # Combine and upload all data
    if all_data:
        combined_data = pd.concat(all_data, ignore_index=True)
        print(f"Total rows scraped across all tables: {len(combined_data)}")
        upload_to_bigquery(combined_data)

def upload_to_bigquery(df):
    client = bigquery.Client()
    table_id = f"{PROJECT_ID}.{DATASET_ID}.pokemon_prices"
    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
    df['Market Price'] = df['Market Price'].astype(str)
    print("Uploading to BigQuery...")
    client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
    print(f"Uploaded {len(df)} rows to {table_id}.")

# Main function
if __name__ == "__main__":
    asyncio.run(scrape_and_store_data())
