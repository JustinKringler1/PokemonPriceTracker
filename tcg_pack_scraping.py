import os
import pandas as pd
import asyncio
from datetime import datetime
from playwright.async_api import async_playwright
from google.cloud import bigquery

# Constants for BigQuery
PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID")
DATASET_ID = "pokemon_data"
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.pokemon_packs"

def delete_today_data():
    client = bigquery.Client()
    today_date = datetime.now().date().isoformat()
    delete_query = f"""
        DELETE FROM `{TABLE_ID}`
        WHERE scrape_date = '{today_date}'
    """
    print(f"Deleting data from BigQuery with today's date ({today_date})...")
    client.query(delete_query).result()
    print(f"Data with today's date ({today_date}) has been deleted from BigQuery.")

async def scrape_sealed_products_table(url, browser, retries=3):
    for attempt in range(retries):
        page = await browser.new_page()
        try:
            # Load the URL and navigate to Sealed Products tab
            await page.goto(url, timeout=180000)
            await page.wait_for_load_state("networkidle")

            sealed_tab = await page.query_selector("a:has-text('Sealed Products')")
            if sealed_tab:
                await sealed_tab.click()
                await page.wait_for_load_state("networkidle")
                await asyncio.sleep(2)  # Wait for the page to stabilize

            # Check for row count stability
            previous_row_count = 0
            stable_checks = 0
            max_stable_checks = 3
            while stable_checks < max_stable_checks:
                rows = await page.query_selector_all("table tr")
                current_row_count = len(rows)
                if current_row_count == previous_row_count:
                    stable_checks += 1
                else:
                    stable_checks = 0
                previous_row_count = current_row_count
                await asyncio.sleep(2)

            if current_row_count == 0:
                print(f"No rows found in sealed products table for {url}. Refreshing...")
                await page.close()
                continue

            # Scrape and filter data
            table_data = []
            target_columns = ["Product Name", "Market Price"]
            headers = [await cell.inner_text() for cell in await rows[0].query_selector_all("th")]
            indices = [headers.index(col) for col in target_columns if col in headers]
            for row in rows[1:]:
                cells = await row.query_selector_all("td")
                row_data = [await cells[i].inner_text() for i in indices if i < len(cells)]
                table_data.append(row_data)

            # Create DataFrame and filter by "Booster Pack"
            df = pd.DataFrame(table_data, columns=[headers[i] for i in indices])
            booster_df = df[df["Product Name"].str.contains("Booster Pack", case=False, na=False)]
            
            # Ensure `scrape_date` column is included
            booster_df["source"] = url.split('/')[-1]
            booster_df["scrape_date"] = datetime.now().date()

            if not booster_df.empty:
                print(f"Filtered data successfully for {url} - {len(booster_df)} rows")
                await page.close()
                return booster_df
            else:
                print(f"No 'Booster Pack' entries found for {url}. Refreshing...")

        except Exception as e:
            print(f"Error on {url}: {e}")

        await page.close()
        await asyncio.sleep(5)  # Wait before retrying

    print(f"Failed to scrape complete data from Sealed Products for {url} after {retries} attempts.")
    
    # Ensure an empty DataFrame has both `source` and `scrape_date` columns
    return pd.DataFrame(columns=["Product Name", "Market Price", "source", "scrape_date"])



async def scrape_and_store_data():
    delete_today_data()

    # Load URLs from CSV
    sets_df = pd.read_csv("pack_set_dictionary.csv")
    all_data = []
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)

        for _, row in sets_df.iterrows():
            set_extension = row['set']
            url = f"https://www.tcgplayer.com/categories/trading-and-collectible-card-games/pokemon/price-guides/{set_extension}"
            print(f"Scraping {url}")

            df = await scrape_sealed_products_table(url, browser)
            if not df.empty:
                all_data.append(df)

        await browser.close()

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
        'Market Price': 'string',
        'source': 'string',
        'scrape_date': 'datetime64[ns]'
    })

    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
    print("Uploading to BigQuery...")
    client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config).result()
    print(f"Uploaded {len(df)} rows to {TABLE_ID}.")

if __name__ == "__main__":
    asyncio.run(scrape_and_store_data())
