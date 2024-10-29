import pandas as pd
import asyncio
from playwright.async_api import async_playwright

# Define a function to scrape a single table from a given URL
async def scrape_single_table(url, browser):
    page = await browser.new_page()
    await page.goto(url)
    
    # Try different strategies to wait for table content
    try:
        await page.wait_for_selector("table", timeout=90000)  # Increase timeout to 90 seconds
        rows = await page.query_selector_all("table tr")
        
        # Wait until at least some rows are loaded in the table
        if len(rows) < 2:  # Adjust if more rows are expected
            print(f"Waiting for rows to load fully on {url}...")
            await page.wait_for_timeout(5000)  # Wait an additional 5 seconds
            rows = await page.query_selector_all("table tr")
        
        # Collect data if rows are found
        if rows:
            table_data = []
            for row in rows:
                cells = await row.query_selector_all("td, th")
                row_data = [await cell.inner_text() for cell in cells]
                table_data.append(row_data)
            
            # Generate DataFrame with source column
            df = pd.DataFrame(table_data[1:], columns=table_data[0])
            df["source"] = url.split('/')[-1]  # Source column based on URL suffix
            print(f"Data scraped successfully from {url}")
            return df
        else:
            print(f"No rows loaded for {url}. Page content may not be loading as expected.")
            return pd.DataFrame()

    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return pd.DataFrame()

    finally:
        await page.close()

# Define the function to scrape multiple tables concurrently
async def scrape_multiple_tables_concurrently(urls):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)  # Change to False to debug visually
        
        # Run scraping tasks concurrently and collect results
        tasks = [scrape_single_table(url, browser) for url in urls]
        all_data = await asyncio.gather(*tasks)
        
        # Combine all non-empty dataframes into a single CSV file
        non_empty_data = [df for df in all_data if not df.empty]
        
        if non_empty_data:
            combined_df = pd.concat(non_empty_data, ignore_index=True)
            combined_df.to_csv("combined_pokemon_prices.csv", index=False)
            print("Data saved to combined_pokemon_prices.csv")
        else:
            print("No data to save. All tables were empty or not found.")
        
        await browser.close()

# Main entry point
def main():
    urls = [
        "https://www.tcgplayer.com/categories/trading-and-collectible-card-games/pokemon/price-guides/sv-scarlet-and-violet-151",
        # Add more URLs here
    ]
    
    # Run the async function
    asyncio.run(scrape_multiple_tables_concurrently(urls))

if __name__ == "__main__":
    main()
