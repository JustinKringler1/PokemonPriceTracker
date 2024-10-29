import pandas as pd
from google.oauth2 import service_account
from playwright.async_api import async_playwright
import os
import asyncio
from datetime import datetime

# Set up Google Cloud BigQuery credentials
credentials = service_account.Credentials.from_service_account_info({
    "type": "service_account",
    "project_id": os.getenv("BIGQUERY_PROJECT_ID"),
    "private_key_id": os.getenv("BIGQUERY_PRIVATE_KEY_ID"),
    "private_key": os.getenv("BIGQUERY_PRIVATE_KEY").replace("\\n", "\n"),
    "client_email": os.getenv("BIGQUERY_CLIENT_EMAIL"),
    "client_id": os.getenv("BIGQUERY_CLIENT_ID"),
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": os.getenv("BIGQUERY_CLIENT_CERT_URL")
})

# Function to upload data to BigQuery
def upload_to_bigquery(df, table_id):
    df.to_gbq(
        table_id, 
        project_id=os.getenv("BIGQUERY_PROJECT_ID"), 
        if_exists="append", 
        credentials=credentials
    )

# Scraping function
async def scrape_single_table(url, browser):
    page = await browser.new_page()
    await page.goto(url)
    
    try:
        await page.wait_for_selector("table", timeout=90000)
        rows = await page.query_selector_all("table tr")
        
        if rows:
            table_data = []
            target_columns = ["Product Name", "Printing", "Condition", "Rarity", "Number", "Market Price"]
            headers = [await cell.inner_text() for cell in await rows[0].query_selector_all("th")]

            indices = [headers.index(col) for col in target_columns if col in headers]
            for row in rows[1:]:
                cells = await row.query_selector_all("td")
                row_data = [await cells[i].inner_text() for i in indices]
                table_data.append(row_data)
            
            df = pd.DataFrame(table_data, columns=target_columns)
            df["source"] = url.split('/')[-1]
            df["scrape_date"] = datetime.now().date()
            print(f"Data scraped successfully from {url}")
            return df
        else:
            print(f"No rows loaded for {url}.")
            return pd.DataFrame()

    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return pd.DataFrame()

    finally:
        await page.close()

# Main function for scraping and uploading data
async def scrape_and_store_data(urls, table_id):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        tasks = [scrape_single_table(url, browser) for url in urls]
        results = await asyncio.gather(*tasks)
        combined_data = pd.concat([df for df in results if not df.empty], ignore_index=True)
        
        if not combined_data.empty:
            upload_to_bigquery(combined_data, table_id)
            print(f"Data uploaded to BigQuery table {table_id}")
        else:
            print("No new data scraped.")
        
        await browser.close()

# Main entry point
def main():
    urls = ["https://www.tcgplayer.com/..."]
    table_id = "pokemon_data.pokemon_prices"  # Dataset.table name in BigQuery
    asyncio.run(scrape_and_store_data(urls, table_id))

if __name__ == "__main__":
    main()
