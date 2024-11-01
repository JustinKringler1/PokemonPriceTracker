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
            # Load the main URL and wait for the page to fully load
            await page.goto(url, timeout=180000)
            await page.wait_for_load_state("networkidle")

            # Attempt to click the "Sealed Products" tab with multiple tries
            for click_attempt in range(3):
                try:
                    # Locate and click the "Sealed Products" tab
                    sealed_tab = await page.query_selector("a:has-text('Sealed Products')")
                    if sealed_tab:
                        await sealed_tab.click()
                        await page.wait_for_load_state("networkidle")
                        await asyncio.sleep(3)  # Allow content to load

                        # Verify the content in the table to ensure we're on the right tab
                        rows = await page.query_selector_all("table tr")
                        if rows:
                            # Additional check: Confirm absence of specific keywords that only appear in the first tab
                            has_incorrect_content = any("Code" in await row.inner_text() for row in rows)
                            if not has_incorrect_content:
                                break  # Exit click retry if "Code" isn't found in the rows
                        else:
                            print(f"Table rows not loaded correctly on tab, retrying tab click for {url}")
                except Exception as e:
                    print(f"Error accessing Sealed Products tab on {url}: {e}")
                    await asyncio.sleep(2)  # Short wait before retrying

            else:
                print(f"Sealed Products tab could not be accessed for {url}. Skipping.")
                await page.close()
                continue

            # Extract headers and data from the correct table
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
            booster_df["source"] = url.split('/')[-1]
            booster_df["scrape_date"] = datetime.now().date()

            if not booster_df.empty:
                print(f"Filtered data successfully for {url} - {len(booster_df)} rows")
                await page.close()
                return booster_df
            else:
                print(f"No 'Booster Pack' entries found for {url}. Refreshing and retrying...")

        except Exception as e:
            print(f"Encountered an error on {url} during attempt {attempt + 1}: {e}")
            await page.close()

        await asyncio.sleep(5)  # Wait before retrying

    print(f"Failed to scrape complete data from Sealed Products tab for {url} after {retries} attempts.")
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
