from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import os
import csv
import re

# Chrome options for headless mode
options = Options()
options.add_argument("--headless")
options.add_argument("--window-size=1920,1080")
options.add_argument("--disable-gpu")

# Initialize the WebDriver
driver = webdriver.Chrome(options=options)
wait = WebDriverWait(driver, 10)

try:
    # Navigate to the page
    driver.get("https://www.tcgplayer.com/categories/trading-and-collectible-card-games/pokemon/price-guides")
    
    # Locate all dropdown containers
    dropdowns = wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, "tcg-input-autocomplete__combobox-container")))
    
    # Select the second dropdown by index
    second_dropdown = dropdowns[1]  # Index 1 for the second dropdown
    dropdown_button = second_dropdown.find_element(By.CLASS_NAME, "tcg-input-autocomplete__dropdown-toggle-button")
    driver.execute_script("arguments[0].scrollIntoView();", dropdown_button)
    dropdown_button.click()
    
    # Wait for dropdown items to load
    dropdown_items = wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, "tcg-base-dropdown__item-content")))
    
    # Extract and clean the dropdown text
    sets = []
    for item in dropdown_items:
        text = item.text.lower()
        text = re.sub(r'\band\b', 'and', text)
        text = re.sub(r'[^\w\s]', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
        text = text.replace(' ', '-')
        sets.append(text)
    
        # Remove empty rows right before writing to the CSV
    sets = [s for s in sets if s]  # Filter out empty rows

    # Read the old file if it exists
    output_file = "data/sets.csv"
    if os.path.exists(output_file):
        with open(output_file, "r", newline="") as f:
            reader = csv.reader(f)
            old_sets = list(reader)
    else:
        old_sets = []
    

    # Write only if new data has more rows
    if len(sets) >= len(old_sets)-2:
        os.makedirs("data", exist_ok=True)
        with open(output_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["set"])  # Add a header row
            writer.writerows([[s] for s in sets])
        print("New sets written to file.")
    else:
        print("New data has fewer rows. Keeping the existing file.")

finally:
    driver.quit()
