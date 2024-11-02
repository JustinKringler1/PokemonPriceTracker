# TCGPlayer Pokémon Pack Price Scraper

## Overview
The TCGPlayer Pokémon Pack Price Scraper is an automated data scraping tool built to collect price data for Pokémon booster packs from the TCGPlayer website. Using Playwright and Google BigQuery, the scraper navigates specific URLs to extract product and market price details, which it then stores in BigQuery. The project enables continuous monitoring and updates on TCGPlayer prices.

## Table of Contents
1. [Features](#features)
2. [Getting Started](#getting-started)
   - [Prerequisites](#prerequisites)
   - [Installation](#installation)
   - [Configuration](#configuration)
3. [Usage](#usage)
4. [Repository Structure](#repository-structure)
5. [Data Workflow](#data-workflow)
6. [Scheduling with GitHub Actions](#scheduling-with-github-actions)
7. [Contributing](#contributing)
8. [License](#license)

---

## Features
- **Automated Web Scraping**: Extracts Pokémon booster pack data from TCGPlayer in a scheduled workflow.
- **Google BigQuery Integration**: Efficiently stores and manages scraped data in a centralized database.
- **Retry Mechanism**: Ensures reliable data capture with configurable retry attempts.
- **Scheduled Updates**: Automates scraping via GitHub Actions to keep data up-to-date.
- **Customizable Filters**: Filters entries to only include rows containing “Booster Pack” in the product name.

## Getting Started

### Prerequisites
- **Python 3.8+**: The scraper requires Python 3.8 or newer.
- **Google Cloud SDK**: Set up and authenticate Google BigQuery access.
- **GitHub Repository Secrets**:
    - `BIGQUERY_PROJECT_ID`: Your Google Cloud project ID.
    - `BIGQUERY_CREDENTIALS_JSON`: JSON credentials for Google Cloud authentication.

### Installation
Clone the repository:
```bash
git clone https://github.com/yourusername/tcgplayer-pokemon-pack-scraper.git
cd tcgplayer-pokemon-pack-scraper


