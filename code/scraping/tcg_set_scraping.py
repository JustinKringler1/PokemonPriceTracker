import time
import csv
import re
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options


# Function to apply regex transformations
def transform_text(text):
    text = text.lower()  # Convert to lowercase
    text = re.sub(r'\b&\b', 'and', text)  # Replace '&' with 'and'
    text = re.sub(r'[^\w\s]', '', text)  # Remove all punctuation
    text = re.sub(r'\s+', ' ', text.strip())  # Strip spaces and reduce to single spaces
    return text.replace(' ', '-')  # Replace spaces with dashes


# Function to count rows in an existing CSV file
def count_csv_rows(file_path):
    if not os.path.exists(file_path):
        return 0
    with open(file_path, mode="r", encoding="utf-8") as file:
        return sum(1 for _ in file) - 1  # Subtract 1 for the header row


# URL of the page
url = "https://www.tcgplayer.com/categories/trading-and-collectible-card-games/pokemon/price-guides"

# Chrome options for running in headless mode (necessary for GitHub Actions)
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

# Initialize the WebDriver
service = Service()
driver = webdriver.Chrome(service=service, options=chrome_options)

# Define file paths
csv_file = "data/sets.csv"
temp_csv_file = "data/sets_temp.csv"

try:
    # Open the URL
    driver.get(url)

    # Wait for the page to load and locate the dropdown by its index
    wait = WebDriverWait(driver, 10)
    dropdown_container = wait.until(
        EC.presence_of_all_elements_located(
            (By.CLASS_NAME, "tcg-input-autocomplete__combobox-container")
        )
    )[1]  # Use the second dropdown (index 1)

    # Click the dropdown to open it
    dropdown_button = dropdown_container.find_element(By.TAG_NAME, "button")
    dropdown_button.click()
    print("Dropdown clicked!")

    # Wait for the dropdown options to appear
    options = wait.until(
        EC.presence_of_all_elements_located((By.CLASS_NAME, "tcg-base-dropdown__item-content"))
    )

    # Extract and transform text from each dropdown option
    option_texts = [transform_text(option.text.strip()) for option in options if option.text.strip()]
    print(f"Transformed options: {option_texts}")

    # Write the transformed options to a temporary CSV file
    with open(temp_csv_file, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["set"])  # Write header
        for option in option_texts:
            writer.writerow([option])

    print(f"Data written to temporary file {temp_csv_file}.")

    # Compare row counts
    current_row_count = count_csv_rows(csv_file)
    new_row_count = count_csv_rows(temp_csv_file)

    print(f"Current row count: {current_row_count}")
    print(f"New row count: {new_row_count}")

    # Overwrite the original CSV file if the new file has the same or more rows
    if new_row_count >= current_row_count:
        os.replace(temp_csv_file, csv_file)
        print(f"File {csv_file} overwritten with new data.")
    else:
        os.remove(temp_csv_file)
        print("New file has fewer rows. Original file retained.")

finally:
    # Close the browser
    driver.quit()
