from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import pandas as pd
import time

BASE_URL = "https://www.tcgplayer.com/categories/trading-and-collectible-card-games/pokemon/price-guides/"

def load_sets(filename="sets.txt"):
    with open(filename, 'r') as file:
        sets = file.read().splitlines()
    return [BASE_URL + set_name for set_name in sets]

def get_row_count(url, max_retries=3, delay=3):
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")  # Run in headless mode for GitHub Actions
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    with webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options) as driver:
        driver.get(url)
        
        for attempt in range(max_retries):
            time.sleep(delay)  # Allow JavaScript to load
            try:
                rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
                row_count = len(rows)

                if row_count >= 5:  # Replace with your row count expectation
                    return row_count
                else:
                    print(f"Attempt {attempt + 1}: Incomplete data, retrying...")
            except Exception as e:
                print(f"Error on attempt {attempt + 1}: {e}")
        
        return row_count if 'row_count' in locals() else 0

def main():
    urls = load_sets()
    all_counts = []

    for _ in range(5):
        for url in urls:
            try:
                row_count = get_row_count(url)
                all_counts.append({'url': url, 'row_count': row_count})
            except Exception as e:
                print(f"Failed to scrape {url}: {e}")
            time.sleep(2)
    df = pd.DataFrame(all_counts)
    df.to_csv('row_counts.csv', index=False)

if __name__ == "__main__":
    main()
