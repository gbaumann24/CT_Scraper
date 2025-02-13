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
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

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

# --- Proxy List (Beispiel, anpassen!)
PROXIES = [
]

# --- Files for Heartbeat and Progress ---
HEARTBEAT_FILE = "heartbeat_products.txt"
PROGRESS_FILE = "progress_products.txt"  # Progress file for product scraper

def update_heartbeat(category_index):
    with open(HEARTBEAT_FILE, "w") as f:
        heartbeat_text = f"{datetime.utcnow().isoformat()} - Category Index: {category_index}\n"
        print("Updating heartbeat:", heartbeat_text.strip())
        f.write(heartbeat_text)

def update_progress(category_index):
    with open(PROGRESS_FILE, "w") as f:
        f.write(str(category_index))

def get_start_index():
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, "r") as f:
                index = int(f.read().strip())
                return index + 1  # resume from the next category
        except Exception as e:
            print("Error reading progress file:", e)
    return 0

def simulate_human_interaction(driver):
    """
    Simuliert menschliches Scrollen, um Lazy Loading auszulösen.
    """
    scroll_pause = random.uniform(0.5, 1.5)
    last_height = driver.execute_script("return document.body.scrollHeight")
    for _ in range(random.randint(3, 6)):
        driver.execute_script("window.scrollBy(0, {});".format(random.randint(100, 300)))
        time.sleep(scroll_pause)
    # Scroll ans Ende, um sicherzustellen, dass alle Elemente geladen werden.
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(random.uniform(2, 4))

def get_driver():
    options = uc.ChromeOptions()
    options.headless = True  # Headless-Modus für Server
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--lang=en-US")
    options.add_argument("--window-size=1280,720")
    options.add_argument("--incognito")
    options.add_argument("--disable-blink-features=AutomationControlled")
    
    # Zusätzliche experimentelle Optionen, um Header zu variieren
    accept_languages = random.choice(["en-US,en;q=0.9", "de-DE,de;q=0.9", "fr-FR,fr;q=0.9"])
    options.add_experimental_option("prefs", {"intl.accept_languages": accept_languages})
    
    # User-Agent rotieren
    options.add_argument("--user-agent=" + random.choice(USER_AGENTS))
    
    # Proxy rotieren, falls vorhanden
    if PROXIES:
        proxy = random.choice(PROXIES)
        print("Using proxy:", proxy)
        options.add_argument(f"--proxy-server={proxy}")
    
    # Setze Browser-Binary (hier Chromium, ggf. anpassen)
    options.binary_location = "/usr/bin/chromium-browser"
    
    driver = uc.Chrome(version_main=133, options=options, browser_executable_path=options.binary_location)
    # Wartezeit zur Initialisierung
    time.sleep(20)
    return driver

def limited_get(url, driver, max_wait=36000):
    """
    Lädt eine URL mit Selenium. Bei Rate-Limiting oder CAPTCHA wird ein exponentieller Backoff (bis max_wait Sekunden) durchgeführt.
    """
    attempt = 1
    total_wait = 0
    while True:
        try:
            print(f"Requesting: {url} (attempt {attempt})")
            # Lösche vorher evtl. alte Cookies, um frische Sessions zu simulieren.
            driver.delete_all_cookies()
            driver.get(url)
            simulate_human_interaction(driver)
            time.sleep(random.uniform(2, 4))
            page_source = driver.page_source

            # Prüfe auf Rate-Limiting
            if "too many requests" in page_source.lower() or "429" in page_source:
                delay = min(60 * (2 ** (attempt - 1)), max_wait)
                total_wait += delay
                if total_wait >= max_wait:
                    print(f"Maximale Wartezeit von {max_wait} Sekunden erreicht. Abbruch bei {url}.")
                    break
                print(f"Rate limit detected on {url}. Waiting for {delay} seconds before retrying...")
                time.sleep(delay)
                attempt += 1
                continue

            # Prüfe auf CAPTCHA
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
    page_source = limited_get(url, driver)
    if not page_source:
        print(f"Failed to load page {url}.")
        return []
    if "captcha" in page_source.lower() or "i am not a robot" in page_source.lower():
        print(f"CAPTCHA detected on page {url}. Skipping this page.")
        return []
    soup = BeautifulSoup(page_source, "html.parser")
    product_links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("/reviews/") or "#reviews" in href:
            if not href.startswith("http"):
                absolute_url = "https://www.capterra.ch" + href
            else:
                absolute_url = href
            product_links.append(absolute_url)
    delay = random.uniform(5, 10)
    print(f"Sleeping for {delay:.2f} seconds to throttle requests.")
    time.sleep(delay)
    return list(set(product_links))

def get_category_product_links(category_link, driver):
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
    with open('capterra_products.csv', 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        if start_index == 0:
            writer.writerow(["Category", "Product Link"])
        for idx, cat in enumerate(categories[start_index:], start=start_index):
            update_heartbeat(idx)
            cat_text = cat.get("text")
            cat_href = cat.get("href")
            if cat_href.startswith("/"):
                cat_href = "https://www.capterra.ch" + cat_href
            print(f"\nScraping category: {cat_text}\nURL: {cat_href}")
            success = False
            retry_count = 0
            base_delay = 5
            max_delay = 60
            while not success:
                update_heartbeat(idx)
                try:
                    product_links = get_category_product_links(cat_href, driver)
                    results[cat_text] = product_links
                    print(f"Found {len(product_links)} product links for '{cat_text}'")
                    for link in product_links:
                        writer.writerow([cat_text, link])
                    csvfile.flush()
                    success = True
                except Exception as e:
                    print(f"Error scraping category '{cat_text}': {e}")
                    retry_delay = min(max_delay, base_delay * (2 ** retry_count))
                    print(f"Retrying category '{cat_text}' after {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_count += 1
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