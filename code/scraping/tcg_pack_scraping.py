"""
Script Name: tcg_pack_scraping.py
Description:
    This script scrapes Pokémon booster pack price data from the TCGPlayer website.
    It reads URL extensions from a CSV file ("data/pack_set_dictionary.csv") and navigates to each URL,
    filtering rows in the sealed products table for items containing 'Booster Pack' in the 'Product Name'.
    Once the relevant data is collected, it is uploaded to Google BigQuery, with today's records deleted first
    to avoid duplication.

    The script includes functionality to retry page loads or table searches if expected data is not found initially.
    
Main Functions:
    - delete_today_data: Deletes today's entries in BigQuery to avoid duplicates.
    - scrape_sealed_products_table: Navigates to the sealed products table, verifies structure,
      and extracts 'Product Name' and 'Market Price' data from rows containing 'Booster Pack'.
    - scrape_and_store_data: Orchestrates deletion, scraping, and data upload.
    - upload_to_bigquery: Loads the extracted data into BigQuery.

Environment Variables:
    - BIGQUERY_PROJECT_ID: Google Cloud Project ID used to access BigQuery.
    
Dependencies:
    - pandas
    - asyncio
    - playwright.async_api
    - google.cloud.bigquery
    
Usage:
    Ensure environment variables and dependencies are set before running this script in a Python 3.8+ environment.
"""
# Modules
import os
import pandas as pd
import asyncio
from datetime import datetime
from playwright.async_api import async_playwright
from google.cloud import bigquery

# Constants for BigQuery configuration
PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID")
DATASET_ID = "pokemon_data"
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.pokemon_packs"


def delete_today_data():
    """
    Deletes records with today's date from the BigQuery table to prevent duplicate data entries.
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


async def scrape_sealed_products_table(url, browser, retries=3):
    """
    Navigates to the specified URL, attempts to locate and click the 'Sealed Products' tab (excluding 'Singles'),
    then extracts rows with 'Booster Pack' from the table and fetches 'Product Name' and 'Market Price' columns.
    
    Args:
        url (str): URL to scrape data from.
        browser (Browser): Playwright browser instance.
        retries (int): Number of retry attempts if page loading fails.
    
    Returns:
        pd.DataFrame: DataFrame containing scraped product names and market prices, or empty if no data.
    """
    for attempt in range(retries):
        page = await browser.new_page()
        try:
            await page.goto(url, timeout=180000)
            await page.wait_for_load_state("networkidle")
            
            # Select the non-Singles tab
            tabs = await page.query_selector_all(".martech-text-capitalize")
            for tab in tabs:
                if "Singles" not in await tab.inner_text():
                    await tab.click()
                    await page.wait_for_timeout(3000)
                    break

            # Locate and extract rows from the table
            rows = page.locator("xpath=//*[contains(@class, 'table')]//tr")
            row_count = await rows.count()
            print(f"Found {row_count} rows in the table for {url}")

            table_data = []
            for i in range(1, row_count):  # Skipping header row
                cells = await rows.nth(i).locator("td").all()
                if len(cells) >= 3:  # Ensuring 'Product Name' and 'Market Price' columns are present
                    product_name = await cells[1].inner_text()
                    market_price = await cells[2].inner_text()
                    table_data.append([product_name, market_price])

            # Filter and create DataFrame
            df = pd.DataFrame(table_data, columns=["Product Name", "Market Price"])
            df = df[df["Product Name"].str.contains(r"(?i)booster\s*pack", regex=True, na=False)]
            df["source"] = url.split('/')[-1]
            df["scrape_date"] = datetime.now().date()

            if not df.empty:
                print(f"Filtered data successfully for {url} - {len(df)} rows")
                await page.close()
                return df
            else:
                print(f"No 'Booster Pack' entries found for {url}. Retrying...")

        except Exception as e:
            print(f"Error on {url}, attempt {attempt + 1}: {e}")
        finally:
            await page.close()
            await asyncio.sleep(5)

    print(f"Failed to scrape complete data from Sealed Products tab for {url} after {retries} attempts.")
    return pd.DataFrame()


async def scrape_and_store_data():
    """
    Main function to manage the workflow of deleting today's data, scraping, and uploading the scraped data.
    """
    delete_today_data()
    
    sets_df = pd.read_csv("data/pack_set_dictionary.csv")
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
    """
    Uploads the provided DataFrame to the specified BigQuery table.
    
    Args:
        df (pd.DataFrame): DataFrame containing the scraped data.
    """
    df['Market Price'] = df['Market Price'].astype(str)
    df['scrape_date'] = pd.to_datetime(df['scrape_date'], errors='coerce')
    df.dropna(subset=['scrape_date'], inplace=True)
    
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
