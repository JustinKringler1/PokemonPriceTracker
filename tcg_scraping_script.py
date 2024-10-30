import asyncio
import datetime
import pandas as pd
from playwright.async_api import async_playwright
from google.cloud import bigquery

# Constants
TABLE_ID = "pokemon_data.pokemon_prices"
DATE_FORMAT = "%Y-%m-%d"

def get_urls_from_file(filename="sets.txt"):
    with open(filename, "r") as file:
        base_url = "https://www.tcgplayer.com/categories/trading-and-collectible-card-games/pokemon/price-guides/"
        return [base_url + line.strip() for line in file if line.strip()]

async def scrape_page(page, url):
    await page.goto(url)
    table = await page.query_selector("table")
    headers = await table.query_selector_all("th")
    header_names = [await header.inner_text() for header in headers]

    rows = await table.query_selector_all("tbody tr")
    data = []
    for row in rows:
        cells = await row.query_selector_all("td")
        row_data = [await cell.inner_text() for cell in cells]
        data.append(row_data)

    df = pd.DataFrame(data, columns=header_names)
    df["Market Price"] = df["Market Price"].astype(str)
    df["source"] = url.split("/")[-1]
    df["scrape_date"] = datetime.datetime.today().strftime(DATE_FORMAT)
    return df

async def scrape_and_store_data(urls):
    combined_data = []
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()

        for url in urls:
            try:
                df = await scrape_page(page, url)
                combined_data.append(df)
            except Exception as e:
                print(f"Failed to scrape {url}: {e}")

        await browser.close()

    if combined_data:
        combined_df = pd.concat(combined_data, ignore_index=True)
        combined_df["scrape_date"] = pd.to_datetime(combined_df["scrape_date"], format=DATE_FORMAT).dt.date
        await delete_today_records()
        upload_to_bigquery(combined_df)

async def delete_today_records():
    client = bigquery.Client()
    today_date = datetime.datetime.today().strftime(DATE_FORMAT)
    query = f"DELETE FROM `{TABLE_ID}` WHERE scrape_date = '{today_date}'"
    
    try:
        client.query(query).result()
        print("Existing records for today deleted successfully.")
    except Exception as e:
        print(f"Error deleting existing records: {e}")

def upload_to_bigquery(df):
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=[
            bigquery.SchemaField("Product Name", "STRING"),
            bigquery.SchemaField("Printing", "STRING"),
            bigquery.SchemaField("Condition", "STRING"),
            bigquery.SchemaField("Rarity", "STRING"),
            bigquery.SchemaField("Number", "STRING"),
            bigquery.SchemaField("Market Price", "STRING"),
            bigquery.SchemaField("source", "STRING"),
            bigquery.SchemaField("scrape_date", "DATE"),
        ],
    )

    job = client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config)
    job.result()
    print("Data uploaded to BigQuery successfully.")

def main():
    urls = get_urls_from_file("sets.txt")
    asyncio.run(scrape_and_store_data(urls))

if __name__ == "__main__":
    main()
