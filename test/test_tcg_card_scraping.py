"""
Script Name: test_tcg_card_scraping.py
Description:
    This script scrapes Pokémon card price data from specified URLs on the TCGPlayer website.
    The script loads URL extensions and expected row counts from a CSV file ("data/card_set_dictionary.csv"),
    navigates to each page, and verifies the row count of data tables before scraping.
    
    This version takes only the first 10 rows from the CSV file for testing and writes the scraped data 
    to a CSV file named "test_card_prices.csv" in the "test" folder instead of uploading to Google BigQuery.

Components:
    - scrape_table_data: Navigates to and scrapes data from a single URL.
    - scrape_and_store_data: Main function coordinating deletion, scraping, and saving to CSV.

Dependencies:
    - pandas
    - asyncio
    - playwright.async_api (for web scraping)

Usage:
    Run the script with Python 3.8+ in an environment where required packages are installed.
"""

# Modules
import os
import pandas as pd
import asyncio
from datetime import datetime
from playwright.async_api import async_playwright

# Define Constants
OUTPUT_FOLDER = "test"
OUTPUT_FILE = "test_card_prices.csv"

# Create output folder if it doesn't exist
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# Function: Scrape data from a single URL
async def scrape_table_data(url, browser, expected_rows):
    """
    Navigates to a specified URL and scrapes Pokémon card price data if the row count matches `expected_rows`.
    Will refresh the page up to 3 times if the row count is incorrect.

    Args:
        url (str): URL to scrape data from.
        browser (Browser): Playwright browser instance.
        expected_rows (int): Expected row count for data validation.

    Returns:
        pd.DataFrame: DataFrame containing scraped data or empty DataFrame if unsuccessful.
    """
    max_retries = 3
    for attempt in range(max_retries):
        page = await browser.new_page()
        try:
            await page.goto(url, timeout=180000)
            await page.wait_for_load_state("networkidle")

            # Select rows from the primary table on the page
            rows = page.locator("table tr")
            row_count = await rows.count()
            print(f"Attempt {attempt + 1}: Found {row_count} rows in the table for {url}")

            # If row count matches the expected, proceed to scrape
            if row_count == expected_rows + 1:
                data = []
                headers = [await cell.inner_text() for cell in await rows.nth(0).locator("th").all()]
                
                # Scrape data row by row
                for i in range(1, row_count):
                    cells = await rows.nth(i).locator("td").all()
                    row_data = [await cell.inner_text() for cell in cells]
                    data.append(row_data)

                df = pd.DataFrame(data, columns=headers)
                df["source"] = url.split('/')[-1]
                df["scrape_date"] = datetime.now().date()

                print(f"Scraped data successfully from {url} - {len(df)} rows")
                await page.close()
                return df
            else:
                print(f"Row count {row_count} does not match expected {expected_rows} for {url}. Retrying...")

        except Exception as e:
            print(f"Error on {url}, attempt {attempt + 1}: {e}")

        finally:
            await page.close()
            await asyncio.sleep(5)  # Delay before retry

    print(f"Failed to scrape complete data from {url} after {max_retries} attempts.")
    return pd.DataFrame()

# Main Function: Orchestrate the scraping and saving process
async def scrape_and_store_data():
    """
    Scrapes data from the first 10 URLs in card_set_dictionary.csv and saves the results to a CSV file 
    named "test_card_prices.csv" in the test folder.
    """
    # Step 1: Load URLs and expected row counts (first 10 rows)
    set_df = pd.read_csv("data/card_set_dictionary.csv").head(10)
    all_data = []

    # Step 2: Initialize Playwright browser
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)

        for _, row in set_df.iterrows():
            set_extension = row['set']
            expected_rows = row['cards']
            url = f"https://www.tcgplayer.com/categories/trading-and-collectible-card-games/pokemon/price-guides/{set_extension}"
            print(f"Scraping {url} with expected rows: {expected_rows}")

            # Scrape data from the URL
            df = await scrape_table_data(url, browser, expected_rows)
            if not df.empty:
                all_data.append(df)

        await browser.close()

    # Step 3: Combine and save scraped data to CSV
    if all_data:
        combined_data = pd.concat(all_data, ignore_index=True)
        output_path = os.path.join(OUTPUT_FOLDER, OUTPUT_FILE)
        combined_data.to_csv(output_path, index=False)
        print(f"Scraped data saved to {output_path}")

# Entry point: Run the asynchronous main function
if __name__ == "__main__":
    asyncio.run(scrape_and_store_data())
