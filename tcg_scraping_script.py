import os
import pandas as pd
import asyncio
from datetime import datetime
from playwright.async_api import async_playwright
from google.cloud import bigquery

# Ensure artifacts directory exists for debugging
os.makedirs("artifacts", exist_ok=True)

# Constants for BigQuery
PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID")
DATASET_ID = "pokemon_data"
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.pokemon_prices"

# Semaphore removed as we no longer need concurrency
async def scrape_single_table(url, browser):
    page = await browser.new_page()
    try:
        # Load the URL
        await page.goto(url, timeout=180000)
        await page.wait_for_load_state("networkidle")

        # Wait for the table to be visible
        try:
            await page.wait_for_selector("table", timeout=180000)
            await asyncio.sleep(10)  # Wait an additional 10 seconds to ensure rows are loaded
        except Exception as e:
            print(f"Table not found on {url}. Saving debug files.")
            await page.screenshot(path=f"artifacts/{url.split('/')[-1]}_screenshot_failed.png")
            html_content = await page.content()
            with open(f"artifacts/{url.split('/')[-1]}_content_failed.html", "w") as file:
                file.write(html_content)
            return pd.DataFrame()  # Return an empty DataFrame

        # Scrape table rows
        rows = await page.query_selector_all("table tr")
        if not rows:
            print(f"No rows found in table for {url}. Saving debug files.")
            await page.screenshot(path=f"artifacts/{url.split('/')[-1]}_screenshot_no_rows.png")
            html_content = await page.content()
            with open(f"artifacts/{url.split('/')[-1]}_content_no_rows.html", "w") as file:
                file.write(html_content)
            return pd.DataFrame()  # Return an empty DataFrame

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
        return df

    finally:
        await page.close()

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

        # Delete today's records from BigQuery to avoid duplicates
        delete_today_records()

        # Upload to BigQuery
        upload_to_bigquery(combined_data)

def delete_today_records():
    client = bigquery.Client.from_service_account_json("bigquery-key.json")
    today_date = datetime.now().date().isoformat()
    query = f"DELETE FROM `{TABLE_ID}` WHERE scrape_date = '{today_date}'"

    try:
        client.query(query).result()
        print(f"Existing records for today's date ({today_date}) deleted successfully.")
    except Exception as e:
        print(f"Error deleting existing records for today's date ({today_date}): {e}")

def upload_to_bigquery(df):
    # Keep 'Market Price' as a string type
    df['Market Price'] = df['Market Price'].astype(str)

    # Convert 'scrape_date' to datetime for compatibility with BigQuery's DATE type
    df['scrape_date'] = pd.to_datetime(df['scrape_date'], errors='coerce')

    # Drop rows where 'scrape_date' is NaT after conversion, as it is required
    df = df.dropna(subset=['scrape_date'])

    # Enforce schema types explicitly
    df = df.astype({
        'Product Name': 'string',
        'Printing': 'string',
        'Condition': 'string',
        'Rarity': 'string',
        'Number': 'string',
        'Market Price': 'string',  # Keep as string
        'source': 'string',
        'scrape_date': 'datetime64[ns]'
    })

    # Log DataFrame info to check types and non-null counts
    print(df.info())
    print(df.head())  # Show first few rows

    # Initialize BigQuery client
    client = bigquery.Client.from_service_account_json("bigquery-key.json")
    table_id = f"{os.getenv('BIGQUERY_PROJECT_ID')}.pokemon_data.pokemon_prices"

    # Define job configuration
    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)

    # Upload data to BigQuery
    print("Uploading to BigQuery...")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Wait for the job to complete
    print(f"Uploaded {len(df)} rows to {table_id}.")

def read_urls(filename="sets.txt"):
    with open(filename, "r") as f:
        base_url = "https://www.tcgplayer.com/categories/trading-and-collectible-card-games/pokemon/price-guides/"
        urls = [base_url + line.strip() for line in f.readlines()]
    print(f"Loaded {len(urls)} URLs from {filename}")
    return urls

def main():
    urls = read_urls("sets.txt")
    asyncio.run(scrape_and_store_data(urls))

if __name__ == "__main__":
    main()
