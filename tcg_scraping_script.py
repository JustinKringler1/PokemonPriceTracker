import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery
from playwright.async_api import async_playwright, Page
import os
import asyncio
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Base URL for TCGPlayer
BASE_URL = "https://www.tcgplayer.com/categories/trading-and-collectible-card-games/pokemon/price-guides/"

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

# Define concurrency limit and batch size
CONCURRENT_REQUESTS = 5
BATCH_SIZE = 1000

# Read URL suffixes from sets.txt file and construct full URLs
def read_sets(file_path="sets.txt"):
    try:
        with open(file_path, "r") as f:
            urls = [BASE_URL + line.strip() for line in f if line.strip()]
            logger.debug(f"Total URLs loaded from {file_path}: {len(urls)}")
            logger.debug(f"Sample URLs: {urls[:5]}")
        return urls
    except FileNotFoundError:
        logger.error(f"Error: The file {file_path} was not found.")
        return []

# Delete existing data for the current day in BigQuery
def delete_existing_data(table_id, date_column="scrape_date"):
    today = datetime.now().date()
    query = f"""
        DELETE FROM `{table_id}`
        WHERE DATE({date_column}) = "{today}"
    """
    query_job = bq_client.query(query)
    try:
        query_job.result()  # Wait for job completion
        logger.debug(f"Deleted existing data for {today} in table {table_id}")
    except Exception as e:
        logger.error(f"Failed to delete existing data in table {table_id}: {e}")

# Function to upload data to BigQuery in batches with error handling
def upload_to_bigquery(df, table_id):
    for start in range(0, len(df), BATCH_SIZE):
        batch = df.iloc[start:start + BATCH_SIZE]
        logger.debug(f"Uploading batch {start} to {start + BATCH_SIZE}")
        try:
            batch.to_gbq(
                table_id,
                project_id=os.getenv("BIGQUERY_PROJECT_ID"),
                if_exists="append",
                credentials=credentials
            )
            logger.debug(f"Batch {start} to {start + BATCH_SIZE} uploaded successfully.")
        except Exception as e:
            logger.error(f"Error uploading batch {start} to {start + BATCH_SIZE}: {e}")

# Scraping function with retry mechanism and semaphore for concurrency control
async def scrape_single_table(url, browser, retries=3):
    async with semaphore:
        for attempt in range(retries):
            try:
                logger.debug(f"Attempting to scrape {url} (Attempt {attempt + 1})")
                page = await browser.new_page()

                # Attempt to load the URL with a high timeout and wait for the network to be idle
                response = await page.goto(url, timeout=180000)
                await page.wait_for_load_state("networkidle")

                # Wait for the table to be visible with an increased timeout
                try:
                    await page.wait_for_selector("table", timeout=180000)
                except Exception as e:
                    logger.error(f"Table not found on {url}. Error: {e}")
                    await page.screenshot(path=f"{url.split('/')[-1]}_screenshot.png")
                    html_content = await page.content()
                    with open(f"{url.split('/')[-1]}_content.html", "w") as file:
                        file.write(html_content)
                    continue

                # Scroll down the page slightly, sometimes needed for JavaScript to load content
                await page.evaluate("window.scrollBy(0, document.body.scrollHeight / 2)")
                await asyncio.sleep(5)

                # Query table rows
                rows = await page.query_selector_all("table tr")
                if not rows:
                    logger.warning(f"No rows found in table for {url}. Saving screenshot and HTML for inspection.")
                    await page.screenshot(path=f"{url.split('/')[-1]}_screenshot.png")
                    html_content = await page.content()
                    with open(f"{url.split('/')[-1]}_content.html", "w") as file:
                        file.write(html_content)
                    return pd.DataFrame()

                # Extract table headers and data
                table_data = []
                target_columns = ["Product Name", "Printing", "Condition", "Rarity", "Number", "Market Price"]
                headers = [await cell.inner_text() for cell in await rows[0].query_selector_all("th")]
                logger.debug(f"Table headers for {url}: {headers}")

                indices = [headers.index(col) for col in target_columns if col in headers]
                for row in rows[1:]:
                    cells = await row.query_selector_all("td")
                    row_data = [await cells[i].inner_text() for i in indices if i < len(cells)]
                    table_data.append(row_data)

                # Create DataFrame and add metadata columns
                df = pd.DataFrame(table_data, columns=[headers[i] for i in indices])
                df["source"] = url.split('/')[-1]
                df["scrape_date"] = datetime.now().date()
                logger.debug(f"Data scraped successfully from {url} - {len(df)} rows")
                return df

            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < retries - 1:
                    logger.debug("Retrying...")

            finally:
                if page:
                    await page.close()

        logger.warning(f"Failed to scrape {url} after {retries} attempts.")
        return pd.DataFrame()

# Sequentially process each batch of URLs
async def process_batches(urls, browser, table_id):
    for i in range(0, len(urls), CONCURRENT_REQUESTS):
        batch_urls = urls[i:i + CONCURRENT_REQUESTS]
        logger.debug(f"Processing batch {i // CONCURRENT_REQUESTS + 1} of URLs")
        
        # Gather the batch results
        tasks = [scrape_single_table(url, browser) for url in batch_urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Collect data from successful scrapes
        data_to_upload = []
        for result in results:
            if isinstance(result, pd.DataFrame) and not result.empty:
                logger.debug(f"Adding {len(result)} rows to upload for batch {i // CONCURRENT_REQUESTS + 1}")
                data_to_upload.append(result)
            else:
                logger.warning("Result is empty or errored")

        if data_to_upload:
            combined_data = pd.concat(data_to_upload, ignore_index=True)
            logger.debug("Total rows in combined_data before upload: " + str(len(combined_data)))
            upload_to_bigquery(combined_data, table_id)
        else:
            logger.warning(f"No data to upload for batch {i // CONCURRENT_REQUESTS + 1}")

# Main function for scraping and uploading data
async def scrape_and_store_data(table_id):
    urls = read_sets("sets.txt")
    if not urls:
        logger.error("No URLs found in sets.txt.")
        return

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        await process_batches(urls, browser, table_id)
        await browser.close()

# Main entry point
def main():
    table_id = "pokemon_data.pokemon_prices"
    delete_existing_data(table_id)  # Delete only once before scraping begins
    asyncio.run(scrape_and_store_data(table_id))

if __name__ == "__main__":
    main()
