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
        table_data = []  # Initialize table data list outside the loop
        try:
            await page.goto(url, timeout=180000)
            await page.wait_for_load_state("networkidle")

            # Find tabs with .martech-text-capitalize and exclude "Singles"
            tabs = await page.query_selector_all(".martech-text-capitalize")
            for tab in tabs:
                tab_text = await tab.inner_text()
                if "Singles" not in tab_text:  # Ensure it is not the Singles tab
                    await tab.click()
                    await page.wait_for_timeout(3000)  # Wait for the tab to load
                    break
            
            # Locate the table with XPath and fetch rows
            rows = page.locator("xpath=//*[contains(concat(' ', @class, ' '), ' table ')]//tr")
            row_count = await rows.count()
            print(f"Found {row_count} rows in the table for {url}")

            # Check if table structure is correct with 5 columns
            if row_count > 0:
                header_cells = await rows.nth(0).locator("td, th").all_inner_texts()
                if len(header_cells) >= 5:  # Verify correct table structure
                    for i in range(1, row_count):  # Skip the header row
                        cells = await rows.nth(i).locator("td").all_inner_texts()
                        if len(cells) >= 5:  # Ensure there are at least 5 columns
                            product_name = cells[2]  # 2nd column for Product Name
                            market_price = cells[3]  # 3rd column for Market Price
                            table_data.append([product_name, market_price])

                    # Create DataFrame
                    df = pd.DataFrame(table_data, columns=["Product Name", "Market Price"])
                    #df = df[df["Product Name"].str.contains(r"(?i)booster\s*pack", regex=True, na=False)]
                    df["source"] = url.split('/')[-1]
                    df["scrape_date"] = datetime.now().date()

                    if not df.empty:
                        print(f"Filtered data successfully for {url} - {len(df)} rows")
                        await page.close()
                        return df
                    else:
                        print(f"No 'Booster Pack' entries found for {url}. Retrying...")

                else:
                    print(f"Table structure mismatch (expected 5 columns) for {url}. Retrying...")

        except Exception as e:
            print(f"Error on {url}, attempt {attempt + 1}: {e}")
        finally:
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
