import pandas as pd
import asyncio
from datetime import datetime
from pathlib import Path
from playwright.async_api import async_playwright

# Read URLs from text file
def read_urls(file_path="urls.txt"):
    with open(file_path, "r") as f:
        return [line.strip() for line in f if line.strip()]

# Define a function to scrape a single table from a given URL
async def scrape_single_table(url, browser):
    page = await browser.new_page()
    await page.goto(url)
    
    try:
        await page.wait_for_selector("table", timeout=90000)  # 90 seconds timeout
        rows = await page.query_selector_all("table tr")
        
        if rows:
            table_data = []
            target_columns = ["Product Name", "Printing", "Condition", "Rarity", "Number", "Market Price"]
            headers = [await cell.inner_text() for cell in await rows[0].query_selector_all("th")]

            # Find indices of target columns
            indices = [headers.index(col) for col in target_columns if col in headers]

            for row in rows[1:]:  # Skip header row
                cells = await row.query_selector_all("td")
                row_data = [await cells[i].inner_text() for i in indices]  # Extract only target columns
                table_data.append(row_data)
            
            # Generate DataFrame with selected columns
            df = pd.DataFrame(table_data, columns=target_columns)
            df["source"] = url.split('/')[-1]  # Add source column
            df["scrape_date"] = datetime.now().strftime("%Y-%m-%d")  # Add scrape date column
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

# Define the function to scrape multiple tables concurrently
async def scrape_multiple_tables_concurrently(urls):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        
        # Load existing data if available
        combined_file = Path("combined_pokemon_prices.csv")
        if combined_file.exists():
            existing_data = pd.read_csv(combined_file)
            
            # Ensure columns are present in existing data
            if "scrape_date" not in existing_data.columns:
                existing_data["scrape_date"] = None
            if "source" not in existing_data.columns:
                existing_data["source"] = None
        else:
            existing_data = pd.DataFrame()

        # Run scraping tasks concurrently and collect results
        tasks = [scrape_single_table(url, browser) for url in urls]
        new_data = await asyncio.gather(*tasks)
        new_data = pd.concat([df for df in new_data if not df.empty], ignore_index=True)
        
        if not new_data.empty:
            # Filter out existing data for today's date and same source
            today = datetime.now().strftime("%Y-%m-%d")
            if not existing_data.empty:
                existing_data = existing_data[
                    ~((existing_data["scrape_date"] == today) & (existing_data["source"].isin(new_data["source"])))
                ]
            
            # Combine new data with existing data
            combined_data = pd.concat([existing_data, new_data], ignore_index=True)
            combined_data.to_csv("combined_pokemon_prices.csv", index=False)
            print("Data saved to combined_pokemon_prices.csv")
        else:
            print("No new data scraped.")

        await browser.close()

# Main entry point
def main():
    urls = read_urls("urls.txt")  # Load URLs from text file
    if urls:
        asyncio.run(scrape_multiple_tables_concurrently(urls))
    else:
        print("No URLs found in urls.txt.")

if __name__ == "__main__":
    main()
