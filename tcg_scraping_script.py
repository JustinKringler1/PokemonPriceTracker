import os
import pandas as pd
import asyncio
from playwright.async_api import async_playwright
from datetime import datetime
from google.cloud import bigquery

# New function to delete rows from BigQuery where scrape_date matches today's date
def delete_existing_data(client, table_id):
    """Delete rows from BigQuery table where scrape_date matches today's date."""
    today_date = datetime.today().strftime('%Y-%m-%d')
    query = f"""
        DELETE FROM `{table_id}`
        WHERE scrape_date = '{today_date}'
    """
    query_job = client.query(query)  # Run the delete query
    query_job.result()  # Wait for the job to complete
    print(f"Deleted rows from {table_id} where scrape_date = {today_date}.")

async def scrape_page(page, url):
    await page.goto(url)
    await page.wait_for_selector("table")
    rows = await page.query_selector_all("table tbody tr")

    data = []
    for row in rows:
        cells = await row.query_selector_all("td")
        row_data = []
        for cell in cells:
            text = await cell.inner_text()
            row_data.append(text)
        data.append(row_data)

    df = pd.DataFrame(data)
    df.columns = ["Select all table rows", "Image", "Product Name", "Printing", 
                  "Condition", "Rarity", "Number", "Market Price", "Add to Cart"]

    df = df.drop(["Select all table rows", "Image", "Add to Cart"], axis=1)
    return df

async def scrape_and_store_data(urls):
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()
        
        combined_data = []
        for url in urls:
            df = await scrape_page(page, url)
            df["source"] = url.split("/")[-1]
            combined_data.append(df)
        
        await browser.close()

    combined_data = pd.concat(combined_data, ignore_index=True)

    # Add scrape date as today's date in the correct format
    combined_data['scrape_date'] = datetime.today().strftime('%Y-%m-%d')

    # Upload to BigQuery
    upload_to_bigquery(combined_data)

def upload_to_bigquery(df):
    # Keep 'Market Price' as a string type to retain dollar sign
    df['Market Price'] = df['Market Price'].astype(str)

    # Ensure 'scrape_date' is in date format
    df['scrape_date'] = pd.to_datetime(df['scrape_date'], errors='coerce').dt.date

    # Drop rows where 'scrape_date' is NaT after conversion
    df = df.dropna(subset=['scrape_date'])

    # Set schema types explicitly
    df = df.astype({
        'Product Name': 'string',
        'Printing': 'string',
        'Condition': 'string',
        'Rarity': 'string',
        'Number': 'string',
        'Market Price': 'string',  # Keep as string to retain dollar sign
        'source': 'string',
        'scrape_date': 'object'  # Keep as date
    })

    # Initialize BigQuery client and table ID
    client = bigquery.Client.from_service_account_json("bigquery-key.json")
    table_id = f"{os.getenv('BIGQUERY_PROJECT_ID')}.pokemon_data.pokemon_prices"

    # Delete existing data for today's date to prevent duplicates
    delete_existing_data(client, table_id)

    # Define job configuration
    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)

    # Upload data to BigQuery
    print("Uploading to BigQuery...")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Wait for the job to complete
    print(f"Uploaded {len(df)} rows to {table_id}.")

def load_urls_from_file(filename):
    with open(filename, "r") as f:
        urls = [line.strip() for line in f if line.strip()]
    return urls

def main():
    urls = load_urls_from_file('sets.txt')
    asyncio.run(scrape_and_store_data(urls))

if __name__ == "__main__":
    main()
