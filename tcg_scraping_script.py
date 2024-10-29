# Required imports
import pandas as pd
import nest_asyncio
import asyncio
from playwright.async_api import async_playwright

# Enable nested event loops in Colab
nest_asyncio.apply()

# Define a function to scrape a single table from a given URL
async def scrape_single_table(url, browser):
    page = await browser.new_page()
    await page.goto(url)
    
    # Increase timeout and wait until the table has loaded rows
    try:
        await page.wait_for_selector("table", timeout=60000)  # 60 seconds timeout
        rows = await page.query_selector_all("table tr")
        
        # Collect data if rows are found
        table_data = []
        for row in rows:
            cells = await row.query_selector_all("td, th")
            row_data = [await cell.inner_text() for cell in cells]
            table_data.append(row_data)
        
        # Generate DataFrame with source column
        if table_data:
            df = pd.DataFrame(table_data[1:], columns=table_data[0])
            df["source"] = url.split('/')[-1]  # Source column based on URL suffix
            return df
        else:
            print(f"No data found in the table for {url}")
            return pd.DataFrame()  # Return empty DataFrame if no data

    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return pd.DataFrame()

    finally:
        await page.close()

# Define the function to scrape multiple tables concurrently
async def scrape_multiple_tables_concurrently(urls):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        
        # Run scraping tasks concurrently and collect results
        tasks = [scrape_single_table(url, browser) for url in urls]
        all_data = await asyncio.gather(*tasks)
        
        # Combine all non-empty dataframes into a single CSV file
        combined_df = pd.concat([df for df in all_data if not df.empty], ignore_index=True)
        combined_df.to_csv("combined_pokemon_prices.csv", index=False)
        print("Data saved to combined_pokemon_prices.csv")
        
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
