
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
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/109.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_3_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.5615.49 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPad; CPU OS 15_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 12; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.5563.64 Mobile Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_0_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:112.0) Gecko/20100101 Firefox/112.0",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.5615.49 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1"
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
    options = uc.ChromeOptions()
    options.headless = True  # We are on a server, so headless mode is required.
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--lang=en-US")
    options.add_argument("--user-agent=" + random.choice(USER_AGENTS))
    options.add_argument("--disable-blink-features=AutomationControlled")
    # Additional flags that are useful on Ubuntu servers:
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1280,720")
    
    # Explicitly set the binary location.
    # For Chromium:
    options.binary_location = "/usr/bin/chromium-browser"
    # If you're using Google Chrome, you might use:
    # options.binary_location = "/usr/bin/google-chrome"

    
    driver = uc.Chrome(version_main=133, options=options)
    time.sleep(20)
    return driver

def limited_get(url, driver, max_wait=36000):
    """
    Lädt eine URL mit Selenium. Bei Erkennung von Rate-Limiting (z. B. "Too Many Requests" oder "429")
    wird ein exponentieller Backoff durchgeführt – maximal wird bis zu max_wait Sekunden (10 Stunden) gewartet.
    
    Gibt den Seitenquelltext bei Erfolg zurück oder None, wenn nach max_wait Sekunden immer noch kein Erfolg erzielt wurde.
    """
    attempt = 1
    total_wait = 0
    while True:
        try:
            print(f"Requesting: {url} (attempt {attempt})")
            driver.get(url)
            # Kurze Wartezeit, damit die Seite laden kann
            time.sleep(random.uniform(2, 4))
            page_source = driver.page_source

            # Prüfen, ob Rate-Limiting (HTTP 429 oder "too many requests") vorliegt
            if "too many requests" in page_source.lower() or "429" in page_source:
                # Exponentieller Backoff: Basisverzögerung 60 Sekunden multipliziert mit 2^(attempt-1)
                delay = min(60 * (2 ** (attempt - 1)), max_wait)
                total_wait += delay
                if total_wait >= max_wait:
                    print(f"Maximale Wartezeit von {max_wait} Sekunden erreicht. Abbruch bei {url}.")
                    break
                print(f"Rate limit detected on {url}. Waiting for {delay} seconds before retrying...")
                time.sleep(delay)
                attempt += 1
                continue

            # Prüfen auf CAPTCHA
            if "captcha" in page_source.lower() or "i am not a robot" in page_source.lower():
                delay = attempt * 5
                total_wait += delay
                if total_wait >= max_wait:
                    print(f"Maximale Wartezeit von {max_wait} Sekunden erreicht beim CAPTCHA. Abbruch bei {url}.")
                    break
                print(f"CAPTCHA detected on {url}. Waiting for {delay} seconds before retrying...")
                time.sleep(delay)
                attempt += 1
                continue

            # Seite erfolgreich geladen, Rückgabe des Quelltexts
            return page_source

        except Exception as e:
            delay = attempt * 5
            total_wait += delay
            if total_wait >= max_wait:
                print(f"Maximale Wartezeit von {max_wait} Sekunden erreicht bei Exception. Abbruch bei {url}.")
                break
            print(f"Error loading {url} on attempt {attempt}: {e}. Waiting for {delay} seconds before next attempt...")
            time.sleep(delay)
            attempt += 1

    print(f"Failed to load {url} after waiting a total of {total_wait} seconds.")
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
    Now, it will return links that either:
      - Start with "/reviews/", or 
      - Contain the fragment "#reviews" (e.g. product pages with reviews)
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
    
    # Iterate over all anchor tags with an href.
    for a in soup.find_all("a", href=True):
        href = a["href"]
        # If the href starts with "/reviews/" OR contains "#reviews"
        if href.startswith("/reviews/") or "#reviews" in href:
            # If the URL is relative, build the absolute URL.
            if not href.startswith("http"):
                absolute_url = "https://www.capterra.ch" + href
            else:
                absolute_url = href
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
        
        # Loop over each category from start_index onward.
        for idx, cat in enumerate(categories[start_index:], start=start_index):
            cat_text = cat.get("text")
            cat_href = cat.get("href")
            if cat_href.startswith("/"):
                cat_href = "https://www.capterra.ch" + cat_href
            
            print(f"\nScraping category: {cat_text}\nURL: {cat_href}")
            
            # Initialize retry variables.
            success = False
            retry_count = 0
            base_delay = 5    # initial delay in seconds
            max_delay = 60    # maximum delay in seconds
            
            # Retry loop: keep retrying the current category until it succeeds.
            while not success:
                # Update heartbeat so that watchdog sees a fresh timestamp
                update_heartbeat(idx)
                try:
                    product_links = get_category_product_links(cat_href, driver)
                    results[cat_text] = product_links
                    print(f"Found {len(product_links)} product links for '{cat_text}'")
                    
                    # Write these links immediately to the CSV
                    for link in product_links:
                        writer.writerow([cat_text, link])
                    csvfile.flush()  # ensure data is written to disk
                    
                    success = True  # mark as successful, break out of retry loop
                except Exception as e:
                    print(f"Error scraping category '{cat_text}': {e}")
                    # Calculate exponential backoff delay:
                    retry_delay = min(max_delay, base_delay * (2 ** retry_count))
                    print(f"Retrying category '{cat_text}' after {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_count += 1
            
            # Once successful, update progress for this category.
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
