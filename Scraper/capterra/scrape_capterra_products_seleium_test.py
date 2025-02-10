#!/usr/bin/env python3
import re
import time
import undetected_chromedriver as uc
import random
import json
import csv
from bs4 import BeautifulSoup
from datetime import datetime
import os

# --- Configuration: User Agents (for logging only) ---
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:115.0) Gecko/20100101 Firefox/115.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    # Add more user agents if available.
]

# --- Files for Heartbeat and Progress ---
HEARTBEAT_FILE = "heartbeat_products.txt"
PROGRESS_FILE = "progress_products.txt"  # Progress file for product scraper

def update_heartbeat(category_index):
    """
    Writes a heartbeat file with the current UTC timestamp and the current category index.
    This file is used by the watchdog to monitor progress.
    """
    with open(HEARTBEAT_FILE, "w") as f:
        heartbeat_text = f"{datetime.utcnow().isoformat()} - Category Index: {category_index}\n"
        print("Updating heartbeat:", heartbeat_text.strip())
        f.write(heartbeat_text)

def update_progress(category_index):
    """
    Update the progress file with the current category index.
    """
    with open(PROGRESS_FILE, "w") as f:
        f.write(str(category_index))

def get_start_index():
    """
    Return the next category index to process based on the progress file.
    If no progress file exists, start at 0.
    """
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, "r") as f:
                index = int(f.read().strip())
                return index + 1  # resume from the next category
        except Exception as e:
            print("Error reading progress file:", e)
    return 0

def get_driver():
    """
    Returns an undetected (stealth) Selenium WebDriver instance.
    """
    options = uc.ChromeOptions()
    options.headless = True  # Run headless if you don't need to see the browser window.
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--lang=en-US")
    # Use a randomized User-Agent.
    options.add_argument("--user-agent=" + random.choice(USER_AGENTS))
    # Additional options to reduce detection.
    options.add_argument("--disable-blink-features=AutomationControlled")
    
    driver = uc.Chrome(options=options)
    return driver

def limited_get(url, driver, retries=3):
    """
    Loads a URL using Selenium’s driver. If the page source contains
    indicators of CAPTCHA or bot detection, it retries with an exponential backoff.
    
    Returns the page source on success, or None if all retries fail.
    """
    for attempt in range(1, retries + 1):
        try:
            print(f"Requesting: {url} (attempt {attempt})")
            driver.get(url)
            # Allow time for the page to load.
            time.sleep(random.uniform(2, 4))
            page_source = driver.page_source
            # Check for CAPTCHA/anti-bot phrases.
            if "captcha" in page_source.lower() or "i am not a robot" in page_source.lower():
                print(f"CAPTCHA detected on page {url}.")
                delay = attempt * 5
                print(f"Waiting for {delay} seconds before retrying...")
                time.sleep(delay)
                continue
            return page_source
        except Exception as e:
            print(f"Error loading {url} on attempt {attempt}: {e}")
            delay = attempt * 5
            print(f"Waiting for {delay} seconds before next attempt...")
            time.sleep(delay)
    print(f"Failed to load {url} after {retries} attempts.")
    return None

def get_max_pages(soup):
    """
    Given a BeautifulSoup object of a category page, extract the maximum page number
    from the pagination (<ul class="pagination">). Returns 1 if no pagination is found.
    """
    pagination = soup.find("ul", class_="pagination")
    if not pagination:
        return 1

    page_numbers = []
    for a in pagination.find_all("a", href=True):
        match = re.search(r"page=(\d+)", a["href"])
        if match:
            page_numbers.append(int(match.group(1)))
    return max(page_numbers) if page_numbers else 1

def get_product_links_from_page(url, driver):
    """
    Loads a page using Selenium and extracts all product review links.
    Returns a list of absolute URLs.
    """
    page_source = limited_get(url, driver)
    if not page_source:
        print(f"Failed to load page {url}.")
        return []
    
    # Check for CAPTCHA or anti-bot messages.
    if "captcha" in page_source.lower() or "i am not a robot" in page_source.lower():
        print(f"CAPTCHA detected on page {url}. Skipping this page.")
        return []
    
    soup = BeautifulSoup(page_source, "html.parser")
    product_links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("/reviews/"):
            absolute_url = "https://www.capterra.ch" + href
            product_links.append(absolute_url)
    
    # Throttle further requests.
    delay = random.uniform(5, 10)
    print(f"Sleeping for {delay:.2f} seconds to throttle requests.")
    time.sleep(delay)
    
    return list(set(product_links))  # Remove duplicates

def get_category_product_links(category_link, driver):
    """
    Given a category link for a Capterra category page, paginate through all pages
    and collect all product review links.
    """
    print(f"Fetching category page: {category_link}")
    page_source = limited_get(category_link, driver)
    if not page_source:
        raise Exception(f"Error fetching category page: {category_link}")
    
    soup = BeautifulSoup(page_source, "html.parser")
    max_pages = get_max_pages(soup)
    print(f"Found maximum pages: {max_pages}")
    
    all_product_links = []
    for page in range(1, max_pages + 1):
        page_url = category_link if page == 1 else f"{category_link}?page={page}"
        print(f"Processing page {page}: {page_url}")
        page_links = get_product_links_from_page(page_url, driver)
        print(f"Found {len(page_links)} product links on page {page}")
        all_product_links.extend(page_links)
    return list(set(all_product_links))

def scrape_all_categories_products():
    with open('capterra_categories.json', 'r') as f:
        categories = json.load(f)
    
    if not categories:
        print("No categories found in capterra_categories.json")
        return {}
    
    results = {}
    driver = get_driver()
    start_index = get_start_index()
    print(f"Resuming from category index: {start_index}")
    
    # Open the CSV once in append mode
    with open('capterra_products.csv', 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Write header only if starting fresh
        if start_index == 0:
            writer.writerow(["Category", "Product Link"])
        
        for idx, cat in enumerate(categories[start_index : start_index + 1], start=start_index):#CHANGE BACK FOR PDOUCTION
            # The rest of your code remains the same...
            update_heartbeat(idx)
            cat_text = cat.get("text")
            cat_href = cat.get("href")
            if cat_href.startswith("/"):
                cat_href = "https://www.capterra.ch" + cat_href
            
            print(f"\nScraping category: {cat_text}\nURL: {cat_href}")
            try:
                product_links = get_category_product_links(cat_href, driver)
                results[cat_text] = product_links
                print(f"Found {len(product_links)} product links for '{cat_text}'")
                
                # Write these links immediately to the CSV
                for link in product_links:
                    writer.writerow([cat_text, link])
                csvfile.flush()  # ensure data is written to disk
                
            except Exception as e:
                print(f"Error scraping category '{cat_text}': {e}")
            
            update_progress(idx)
    driver.quit()
    return results
    

if __name__ == "__main__":
    all_products = scrape_all_categories_products()
    
    print("\n--- All Categories and Their Product Links ---")
    for category, links in all_products.items():
        print(f"\nCategory: {category}")
        print(f"Total Products: {len(links)}")
        for link in links:
            print(f"  {link}")