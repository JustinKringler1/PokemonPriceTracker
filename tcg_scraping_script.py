import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery
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

# Initialize BigQuery client
bq_client = bigquery.Client(credentials=credentials, project=os.getenv("BIGQUERY_PROJECT_ID"))

# Define concurrency limit
CONCURRENT_REQUESTS = 10
semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)

# Function to delete existing data for the current day
def delete_existing_data(table_id, date_column="scrape_date"):
    today = datetime.now().date()
    query = f"""
        DELETE FROM `{table_id}`
        WHERE DATE({date_column}) = "{today}"
    """
    query_job = bq_client.query(query)
    query_job.result()  # Waits for the job to complete
    print(f"Deleted existing data for {today} in table {table_id}")

# Function to upload data to BigQuery
def upload_to_bigquery(df, table_id):
    df.to_gbq(
        table_id, 
        project_id=os.getenv("BIGQUERY_PROJECT_ID"), 
        if_exists="append", 
        credentials=credentials
    )
    print(f"Data uploaded to BigQuery table {table_id}")

# Read URLs from text file
def read_urls(file_path="urls.txt"):
    try:
        with open(file_path, "r") as f:
            urls = [line.strip() for line in f if line.strip()]
        return urls
    except FileNotFoundError:
        print(f"Error: The file {file_path} was not found.")
        return []

# Scraping function with retry mechanism and semaphore for concurrency control
async def scrape_single_table(url, browser, retries=3):
    async with semaphore:
        for attempt in range(retries):
            try:
                page = await browser.new_page()
                await page.goto(url)
                await page.wait_for_selector("table", timeout=90000)  # 90 seconds timeout
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
                print(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < retries - 1:
                    print("Retrying...")
            finally:
                await page.close()

        print(f"Failed to scrape {url} after {retries} attempts.")
        return pd.DataFrame()

# Main function for scraping and uploading data
async def scrape_and_store_data(table_id):
    # Remove any existing data for the current day once at the start
    delete_existing_data(table_id)

    # Load URLs from the text file
    urls = read_urls("urls.txt")
    if not urls:
        print("No URLs found in urls.txt.")
        return
          
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        tasks = [scrape_single_table(url, browser) for url in urls]
        results = await asyncio.gather(*tasks)
        
        # Combine and upload data
        data_to_upload = [df for df in results if not df.empty]
        if data_to_upload:
            combined_data = pd.concat(data_to_upload, ignore_index=True)
            upload_to_bigquery(combined_data, table_id)
        else:
            print("No new data scraped.")

        await browser.close()

# Main entry point
def main():
    table_id = "pokemon_data.pokemon_prices"  # BigQuery dataset.table name
    asyncio.run(scrape_and_store_data(table_id))

if __name__ == "__main__":
    main()
