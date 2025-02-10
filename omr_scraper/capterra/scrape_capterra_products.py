import requests
from bs4 import BeautifulSoup
import re
import time
import random
import json

# --- Configuration: User Agents (no proxies) ---
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:115.0) Gecko/20100101 Firefox/115.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    # Add more user agents if available.
]

def get_session():
    """
    Returns a requests Session with randomized headers.
    """
    session = requests.Session()
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/",
        "Cache-Control": "max-age=0, private, must-revalidate",
        "Connection": "keep-alive",
        "Accept-Encoding": "gzip, deflate, br"
    }
    session.headers.update(headers)
    return session

def limited_get(url, session, retries=3):
    """
    Fetches a URL with a specified number of retries. If the response is either
    429 (Too Many Requests) or 403 (Forbidden), the function will wait (with an
    exponential backoff strategy) and try again.

    Args:
        url (str): The URL to fetch.
        session (requests.Session): The session to use.
        retries (int): Number of retry attempts.

    Returns:
        The requests.Response object on success, or None if all retries fail.
    """
    for attempt in range(1, retries + 1):
        try:
            print(f"Requesting: {url} (attempt {attempt})")
            response = session.get(url, timeout=10)
            # If server blocks the request, wait and retry.
            if response.status_code in (429, 403):
                print(f"Received status code {response.status_code} for {url}.")
                delay = attempt * 5  # Delay increases with each attempt.
                print(f"Waiting for {delay} seconds before retrying...")
                time.sleep(delay)
                continue
            response.raise_for_status()  # Raise an exception for 4xx/5xx responses.
            return response
        except requests.RequestException as e:
            print(f"Error fetching {url} on attempt {attempt}: {e}")
            delay = attempt * 5
            print(f"Waiting for {delay} seconds before next attempt...")
            time.sleep(delay)
    print(f"Failed to fetch {url} after {retries} attempts.")
    return None

def get_max_pages(soup):
    """
    Given a BeautifulSoup object of a category page, extract the maximum page number
    from the pagination (<ul class="pagination">). It looks for links like '?page=3'.
    
    Returns:
        The maximum page number as an integer. Returns 1 if no pagination is found.
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

def get_product_links_from_page(url, session):
    """
    Fetch a page using the given session and extract all product review links.
    Product links are identified as <a> tags whose href starts with "/reviews/".

    Returns:
        A list of absolute URLs for the product review pages.
    """
    response = limited_get(url, session)
    if not response:
        print(f"Failed to fetch page {url}.")
        return []
    
    # Check for CAPTCHA or anti-bot messages.
    content_lower = response.text.lower()
    if "captcha" in content_lower or "i am not a robot" in content_lower:
        print(f"CAPTCHA detected on page {url}. Skipping this page.")
        return []
    
    soup = BeautifulSoup(response.content, "html.parser")
    product_links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("/reviews/"):
            absolute_url = "https://www.capterra.ch" + href
            product_links.append(absolute_url)
    
    # Throttle further requests by sleeping for a random delay between 5 and 10 seconds.
    delay = random.uniform(5, 10)
    print(f"Sleeping for {delay:.2f} seconds to throttle requests.")
    time.sleep(delay)
    
    return list(set(product_links))  # Remove duplicates

def get_category_product_links(category_link):
    """
    Given a category link (absolute URL) for a Capterra category page,
    this function paginates through all pages and collects all product review links.
    
    Pagination:
      - The first page is the base link.
      - Subsequent pages are accessed by appending "?page=2", "?page=3", etc.
      - The maximum number of pages is determined from the <ul class="pagination"> element.
    
    Returns:
        A list of product review URLs for the given category.
    """
    session = get_session()
    
    print(f"Fetching category page: {category_link}")
    response = limited_get(category_link, session)
    if not response:
        raise Exception(f"Error fetching category page: {category_link}")
    
    soup = BeautifulSoup(response.content, "html.parser")
    max_pages = get_max_pages(soup)
    print(f"Found maximum pages: {max_pages}")
    
    all_product_links = []
    for page in range(1, max_pages + 1):
        page_url = category_link if page == 1 else f"{category_link}?page={page}"
        print(f"Processing page {page}: {page_url}")
        page_links = get_product_links_from_page(page_url, session)
        print(f"Found {len(page_links)} product links on page {page}")
        all_product_links.extend(page_links)
    
    return list(set(all_product_links))

def scrape_all_categories_products():
    """
    Scrapes all product review links from every category listed on Capterra.
    
    Steps:
      1. Use get_capterra_category_links() to fetch all category links.
      2. For each category, convert the relative href to an absolute URL if necessary.
      3. Scrape all product links (paginating as needed).
    
    Returns:
        A dictionary mapping category names to lists of product review URLs.
    """
    # Get all category links from your JSON file.
    with open('capterra_categories.json', 'r') as f:
        categories = json.load(f)
    results = {}
    
    for cat in categories:
        cat_text = cat.get("text")
        cat_href = cat.get("href")
        # Convert relative URL to absolute if needed.
        if cat_href.startswith("/"):
            cat_href = "https://www.capterra.ch" + cat_href
        
        print(f"\nScraping category: {cat_text}\nURL: {cat_href}")
        try:
            product_links = get_category_product_links(cat_href)
            results[cat_text] = product_links
            print(f"Found {len(product_links)} product links for '{cat_text}'")
        except Exception as e:
            print(f"Error scraping category '{cat_text}': {e}")
    
    return results

if __name__ == "__main__":
    all_products = scrape_all_categories_products()
    
    print("\n--- All Categories and Their Product Links ---")
    for category, links in all_products.items():
        print(f"\nCategory: {category}")
        print(f"Total Products: {len(links)}")
        for link in links:
            print(f"  {link}")