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

async def scrape_single_table(url, browser, retries=3):
    for attempt in range(retries):
        page = await browser.new_page()
        try:
            # Load the URL and wait for it to be ready
            await page.goto(url, timeout=180000)
            await page.wait_for_load_state("networkidle")
            await page.wait_for_selector("table", timeout=180000)
            await asyncio.sleep(10)  # Extra wait time for rows to load

            # Scrape table rows
            rows = await page.query_selector_all("table tr")
            if not rows:
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

            # Sort by "Number" and check for completeness
            if "Number" in df.columns:
                try:
                    # Split the "Number" column by "/" to get "Card Number" and "Set Size" for validation only
                    temp_split = df["Number"].str.split("/", n=1, expand=True)
                    df["Card Number"] = temp_split[0]  # Card Number part
                    df["Set Size"] = pd.to_numeric(temp_split[1], errors="coerce")
                    max_card_count = int(df["Set Size"].max())  # Take the maximum "Set Size" found
                    if len(df) >= max_card_count - 3:
                        print(f"Data scraped successfully from {url} - {len(df)} rows")
                        return df
                    else:
                        print(f"Incomplete data for {url}. Expected {max_card_count}, got {len(df)} rows.")
                except Exception as e:
                    print(f"Error parsing 'Number' column for {url}: {e}")

            # Close page and retry scraping if incomplete
            await page.close()
            print(f"Retrying scrape for {url}, attempt {attempt + 2}...")

        except Exception as e:
            print(f"Failed to scrape {url} on attempt {attempt + 1} due to error: {e}")
            await page.close()

        await asyncio.sleep(5)  # Wait before retrying

    print(f"Failed to scrape complete data from {url} after {retries} attempts.")
    return pd.DataFrame()  # Return empty if incomplete after retries


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

    client = bigquery.Client.from_service_account_json("bigquery-key.json")
    table_id = f"{os.getenv('BIGQUERY_PROJECT_ID')}.pokemon_data.pokemon_prices"
    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
    print("Uploading to BigQuery...")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
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
