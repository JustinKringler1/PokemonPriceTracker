# Pokémon Card Price Scraper

This repository contains scripts to scrape Pokémon card pricing data from TCGPlayer.com and store it in Google BigQuery. The setup uses GitHub Actions for automated scraping of both individual cards and sealed products (e.g., booster packs) at scheduled intervals.

## Table of Contents

1. [Overview](#overview)
2. [Requirements](#requirements)
3. [Setup](#setup)
4. [Usage](#usage)
5. [Folder Structure](#folder-structure)
6. [Contributing](#contributing)
7. [License](#license)

## Overview

This repository features:
- Automated scraping for Pokémon card prices, fetching both individual card prices and sealed product prices.
- BigQuery integration for data storage, allowing analytics and visualization.
- GitHub Actions workflows for regular, automated scraping.

## Requirements

- **Python**: Version 3.8 or higher
- **Google Cloud Platform**: Set up with a BigQuery project and service account
- **GitHub Secrets**: For securely storing credentials and project configurations

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/yourusername/yourrepo.git
cd yourrepo
```

### 2. BigQuery Setup
- Set up a BigQuery project on GCP and create a dataset named pokemon_data.
- Add two tables in the dataset:
   - pokemon_prices for individual card prices
   - pokemon_packs for sealed product prices
- Generate a BigQuery service account key and download the JSON file.

### 3. GitHub Secrets Configuration
Add the following secrets to your GitHub repository for secure access:

- BIGQUERY_PROJECT_ID: Your BigQuery Project ID
- BIGQUERY_CREDENTIALS_JSON: Paste the contents of your BigQuery service account JSON key

### 4. Dependencies
All dependencies are listed in requirements.txt. To install locally:

```bash
pip install -r requirements.txt
```

## Usage
### Automated Scraping with GitHub Actions
- Workflow Files:
   - card_scraping.yml: For individual card price scraping.
   - pack_scraping.yml: For sealed product price scraping.

## Folder Structure
tcg_scraping_script.py: Script to scrape individual card prices.
tcg_pack_scraping.py: Script to scrape sealed product prices.
.github/workflows/scraping.yml: Workflow for scraping individual card prices.
.github/workflows/pack_scraping.yml: Workflow for scraping sealed product prices.
requirements.txt: Contains the list of required packages.

## Contributing
Contributions are welcome. For significant changes, please open an issue first to discuss your ideas.

## License
Distributed under the MIT License.
