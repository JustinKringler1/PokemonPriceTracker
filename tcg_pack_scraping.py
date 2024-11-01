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
            # Load the URL and wait for the page to load
            await page.goto(url, timeout=180000)
            await page.wait_for_load_state("networkidle")

            # Attempt to find and click the "Sealed Products" tab
            sealed_tab = page.locator("a", has_text="Sealed Products")
            if await sealed_tab.count() > 0:
                await sealed_tab.click()
                await page.wait_for_load_state("networkidle")
                await asyncio.sleep(5)  # Allow extra time for content to load

            # Check if the table structure matches the expected two columns
            rows = await page.query_selector_all("table tr")
            if rows:
                first_row_cells = await rows[0].query_selector_all("th, td")
                if len(first_row_cells) != 2:
                    print(f"Table structure mismatch (expected 2 columns) for {url}. Retrying...")
                    await page.close()
                    continue  # Retry if the structure does not match

            # Scrape data from the table if structure is verified
            table_data = []
            headers = [await cell.inner_text() for cell in first_row_cells]
            print(f"Confirmed table headers for {url} (Sealed Products): {headers}")

            for row in rows[1:]:  # Skip header row
                cells = await row.query_selector_all("td")
                row_data = [await cell.inner_text() for cell in cells]
                table_data.append(row_data)

            # Convert to DataFrame and filter for "Booster Pack"
            df = pd.DataFrame(table_data, columns=headers)
            df = df[df["Product Name"].str.contains("Booster Pack", case=False, na=False)]
            df["source"] = url.split('/')[-1]
            df["scrape_date"] = datetime.now().date()

            if not df.empty:
                print(f"Filtered data successfully for {url} - {len(df)} rows")
                await page.close()
                return df
            else:
                print(f"No 'Booster Pack' entries found for {url}. Refreshing...")

        except Exception as e:
            print(f"Error on {url}, attempt {attempt + 1}: {e}")
            await page.close()

        await asyncio.sleep(5)  # Retry delay

    print(f"Failed to scrape complete data from Sealed Products tab for {url} after {retries} attempts.")
    return pd.DataFrame()



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