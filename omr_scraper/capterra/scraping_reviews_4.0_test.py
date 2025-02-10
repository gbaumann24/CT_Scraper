#!/usr/bin/env python3
import csv
import requests
from bs4 import BeautifulSoup
import time
import random
from urllib.parse import urljoin
from tqdm import tqdm
import sys
import os
from datetime import datetime

# Files used for tracking progress and heartbeat.
HEARTBEAT_FILE = "heartbeat_reviews.txt"
PROGRESS_FILE = "progress_reviews.txt"

def update_heartbeat(product_index):
    """Update the heartbeat file with the current UTC timestamp and product index."""
    with open(HEARTBEAT_FILE, "w") as f:
        f.write(f"{datetime.utcnow().isoformat()} - Product Index: {product_index}\n")

def update_progress(product_index):
    """Update the progress file with the current product index."""
    with open(PROGRESS_FILE, "w") as f:
        f.write(str(product_index))

def get_start_index():
    """Return the next product index to process based on the progress file (or 0 if not found)."""
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, "r") as f:
                index = int(f.read().strip())
                return index + 1  # start from the next product
        except Exception as e:
            print("Error reading progress file:", e)
    return 0

def fetch_page(url, retries=3):
    """
    Fetch the content of a page using requests with a randomized User-Agent.
    Retries a few times (with increasing delays) in case of errors.
    """
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:115.0) Gecko/20100101 Firefox/115.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    ]
    headers = {
        "User-Agent": random.choice(user_agents),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/",
        "Cache-Control": "max-age=0, private, must-revalidate",
        "Connection": "keep-alive",
    }
    for attempt in range(1, retries+1):
        try:
            print(f"Fetching URL: {url} (Attempt {attempt})")
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                return response.text
            else:
                print(f"Non-200 status code: {response.status_code} for URL: {url}")
        except Exception as e:
            print(f"Error fetching {url} on attempt {attempt}: {e}")
        delay = attempt * 5  # exponential-like delay
        print(f"Waiting for {delay} seconds before retrying...")
        time.sleep(delay)
    return None

def extract_reviews(page_content):
    """
    Given the HTML content of a product page, extract the reviews.
    Each review is assumed to be a direct child of the <div id="reviews"> element.
    Extracts:
      - Name from an element with classes "h5 fw-bold mb-2"
      - Raw Role from an element with classes "text-ash mb-2"
      - Raw Industry & Employee from the first child of the info container,
        and Use Duration from the second child (with the fixed prefix removed)
      - Rating from the reviews' rating element.
    """
    soup = BeautifulSoup(page_content, "html.parser")
    review_container = soup.find("div", id="reviews")
    if not review_container:
        print("No review container found on the page.")
        return []
    
    reviews = []
    for review in review_container.find_all(recursive=False):
        review_data = {}
        
        # Extract Name.
        name_el = review.find(class_="h5 fw-bold mb-2")
        review_data["Name"] = name_el.get_text(strip=True) if name_el else ""
        
        # Extract raw Role.
        role_el = review.find(class_="text-ash mb-2")
        review_data["Raw Role"] = role_el.get_text(strip=True) if role_el else ""
        
        # Extract raw Industry & Employee and Use Duration.
        info_container = review.find("div", class_="col-12 col-md-6 col-lg-12 pt-3 pt-md-0 pt-lg-3 text-ash")
        raw_industry_emp = ""
        use_duration = ""
        if info_container:
            children = [child for child in info_container.find_all(recursive=False) if child.name]
            if len(children) >= 1:
                raw_industry_emp = children[0].get_text(strip=True)
            if len(children) >= 2:
                use_duration = children[1].get_text(strip=True).replace("Verwendete die Software für:", "").strip()
        review_data["Raw Industry & Employee"] = raw_industry_emp
        review_data["Use Duration"] = use_duration
        
        # Extract Rating.
        rating = ""
        stars_wrapper = review.find("span", class_="stars-wrapper")
        if stars_wrapper:
            # For this scenario, the rating is contained in the direct following sibling with class "ms-1".
            rating_span = stars_wrapper.find_next_sibling("span", class_="ms-1")
            if rating_span:
                rating = rating_span.get_text(strip=True)
            else:
                print("DEBUG: No sibling with class 'ms-1' found after stars-wrapper.")
        review_data["Rating"] = rating

        # Extract Advantages (first large text)
       # Get all paragraph elements from the review block.
        review_paragraphs = review.find_all("p")

        # Initialize default values.
        comment = ""
        advantages = ""
        disadvantages = ""

        # Loop through paragraphs to look for labels.
        for i, p in enumerate(review_paragraphs):
            # Use a separator to preserve spacing between inline elements.
            p_text = p.get_text(separator=" ", strip=True)
            
            # Look for the comment section.
            if "Kommentare:" in p_text:
                # Remove the label "Kommentare:" and any extra whitespace.
                comment = p_text.replace("Kommentare:", "").strip()
            
            # Look for advantages. We assume that the paragraph immediately following a label
            # containing "Vorteile:" holds the advantages text.
            elif "Vorteile:" in p_text:
                if i + 1 < len(review_paragraphs):
                    advantages = review_paragraphs[i + 1].get_text(separator=" ", strip=True)
            
            # Look for disadvantages in the same way.
            elif "Nachteile:" in p_text:
                if i + 1 < len(review_paragraphs):
                    disadvantages = review_paragraphs[i + 1].get_text(separator=" ", strip=True)

        # Assign the extracted text to your review data.
        review_data["Comment"] = comment
        review_data["Advantages"] = advantages
        review_data["Disadvantages"] = disadvantages
        
        reviews.append(review_data)
        
    return reviews

def extract_pagination_info(page_content):
    """
    Finds the pagination <ul> element with class "pagination" and extracts
    the maximum page number (from the second last <li> item).
    Defaults to 1 if extraction fails.
    """
    soup = BeautifulSoup(page_content, "html.parser")
    pagination_ul = soup.find("ul", class_="pagination")
    max_page = 1
    if pagination_ul:
        li_items = pagination_ul.find_all("li")
        if len(li_items) >= 2:
            second_last_li = li_items[-2]
            try:
                max_page = int(second_last_li.get_text(strip=True))
            except ValueError:
                print("Unable to convert the second last li text to an integer.")
    print(f"Max page number: {max_page}")
    return max_page

def parse_role(raw_role):
    """
    Splits a raw role string of the format "Role in Country" into separate fields.
    If " in " is not found, returns the raw string as role and an empty country.
    """
    if " in " in raw_role:
        parts = raw_role.split(" in ", 1)
        return parts[0].strip(), parts[1].strip()
    return raw_role.strip(), ""

def parse_industry_employee(raw_ie):
    """
    Splits a raw industry & employee string of the format "Industry, Employee Count"
    into separate fields. If no comma is found, returns the raw string and an empty employee count.
    """
    if "," in raw_ie:
        parts = raw_ie.split(",", 1)
        return parts[0].strip(), parts[1].strip()
    return raw_ie.strip(), ""

def main():
    input_csv = "capterra_products.csv"   # CSV should have columns: Category, Product Link
    output_csv = "capterra_reviews.csv"
    
    fieldnames = [
    "Category", "Product Link",
    "Name", "Role", "Country",
    "Industry", "Employee Count",
    "Use Duration",
    "Rating", 
    "Comment",
    "Advantages",
    "Disadvantages"
]
    
    # Read all products from the input CSV.
    try:
        with open(input_csv, newline="", encoding="utf-8") as csvfile:
            reader = list(csv.DictReader(csvfile))
    except Exception as e:
        print(f"Error reading {input_csv}: {e}")
        sys.exit(1)
    
    start_index = get_start_index()
    print(f"Resuming from product index: {start_index}")
    # For testing only, process only the first product.
    products_to_process = reader[start_index:start_index+1]
    
    # Open the output CSV for appending so that we retain previous results.
    with open(output_csv, "a", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        # If starting from scratch, write the header.
        if start_index == 0:
            writer.writeheader()
        
        # Process each product with a progress bar.
        for i, row in enumerate(tqdm(products_to_process, desc="Processing Products", unit="product"), start=start_index):
            update_heartbeat(i)
            try:
                category = row["Category"]
                product_link = row["Product Link"]
                print(f"\nProcessing product link: {product_link} (Category: {category})")
                
                # Process the first (main) review page.
                page_content = fetch_page(product_link)
                if not page_content:
                    print(f"Skipping {product_link} due to fetch issues.")
                    continue
                
                reviews = extract_reviews(page_content)
                print(f"Found {len(reviews)} reviews on the first page.")
                max_page = extract_pagination_info(page_content)
                
                # Process additional pagination pages if available.
                if max_page > 1:
                    for page in range(2, max_page + 1):
                        page_url = f"{product_link}?page={page}"
                        print(f"Processing pagination page: {page_url}")
                        pag_page_content = fetch_page(page_url)
                        if not pag_page_content:
                            print(f"Skipping pagination page {page_url} due to fetch issues.")
                            continue
                        pag_reviews = extract_reviews(pag_page_content)
                        print(f"Found {len(pag_reviews)} reviews on page {page}.")
                        reviews.extend(pag_reviews)
                        time.sleep(random.uniform(3, 6))
                
                # Process each review.
                for review in reviews:
                    review["Category"] = category
                    review["Product Link"] = product_link
                    
                    # Split Raw Role into Role and Country.
                    role_raw = review.pop("Raw Role", "")
                    role, country = parse_role(role_raw)
                    review["Role"] = role
                    review["Country"] = country
                    
                    # Split Raw Industry & Employee into Industry and Employee Count.
                    raw_ie = review.pop("Raw Industry & Employee", "")
                    industry, employee = parse_industry_employee(raw_ie)
                    review["Industry"] = industry
                    review["Employee Count"] = employee
                    
                    writer.writerow(review)
                    csvfile.flush()  # Flush after each row to ensure data is saved
                update_progress(i)
                time.sleep(random.uniform(5, 8))
            except Exception as e:
                print(f"Error processing product {row.get('Product Link', 'N/A')}: {e}")
    
    print(f"\nExtraction complete. Reviews saved to '{output_csv}'.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted by user. Exiting and saving data so far.")