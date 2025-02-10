import csv
import requests
from bs4 import BeautifulSoup
import time
import random
from urllib.parse import urljoin

# --- Helper Functions ---

def fetch_page(url, retries=3):
    """
    Fetch the content of a page using requests with a randomized User-Agent.
    Retries a few times (with increasing delays) in case of errors.
    """
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:115.0) Gecko/20100101 Firefox/115.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
        # Add more User-Agents if needed.
    ]
    headers = {
        "User-Agent": random.choice(user_agents),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/",
        "Cache-Control": "max-age=0, private, must-revalidate",
        "Connection": "keep-alive",
    }
    for attempt in range(1, retries + 1):
        try:
            print(f"Fetching URL: {url} (Attempt {attempt})")
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                return response.text
            else:
                print(f"Non-200 status code: {response.status_code} for URL: {url}")
        except Exception as e:
            print(f"Error fetching {url} on attempt {attempt}: {e}")
        delay = attempt * 5  # Exponential-like delay
        print(f"Waiting for {delay} seconds before retrying...")
        time.sleep(delay)
    return None

def extract_reviews(page_content):
    """
    Given the HTML content of a product page, this function extracts the reviews.
    It finds the div with id "reviews" and for each direct child (each review),
    extracts:
      - Name (from element with classes "h5 fw-bold mb-2")
      - Role (from element with classes "text-ash mb-2")
      - The first two child elements from the div with class 
        "col-12 col-md-6 col-lg-12 pt-3 pt-md-0 pt-lg-3 text-ash":
          • The first contains "Industry, Employee Number"
          • The second contains "Use Duration"
      - Rating (from the element with class "stars-wrapper" within the div with class "col-lg-7")
      
    Returns a list of dictionaries, one per review.
    """
    soup = BeautifulSoup(page_content, "html.parser")
    review_container = soup.find("div", id="reviews")
    if not review_container:
        print("No review container found on the page.")
        return []
    
    reviews = []
    # Each direct child of review_container is assumed to be a separate review.
    for review in review_container.find_all(recursive=False):
        review_data = {}
        
        # --- Extract Name ---
        name_el = review.find(class_="h5 fw-bold mb-2")
        review_data["Name"] = name_el.get_text(strip=True) if name_el else ""
        
        # --- Extract Role ---
        role_el = review.find(class_="text-ash mb-2")
        review_data["Role"] = role_el.get_text(strip=True) if role_el else ""
        
        # --- Extract Industry & Employee and Use Duration ---
        info_container = review.find("div", class_="col-12 col-md-6 col-lg-12 pt-3 pt-md-0 pt-lg-3 text-ash")
        industry_emp = ""
        use_duration = ""
        if info_container:
            # Grab only the tag children (ignoring navigable strings)
            children = [child for child in info_container.find_all(recursive=False) if child.name]
            if len(children) >= 1:
                industry_emp = children[0].get_text(strip=True)
            if len(children) >= 2:
                use_duration = children[1].get_text(strip=True)
        review_data["Industry & Employee"] = industry_emp
        review_data["Use Duration"] = use_duration
        
        # --- Extract Rating ---
        rating = ""
        col_lg7 = review.find("div", class_="col-lg-7")
        if col_lg7:
            stars = col_lg7.find(class_="stars-wrapper")
            if stars:
                rating = stars.get_text(strip=True)
        review_data["Rating"] = rating
        
        reviews.append(review_data)
    return reviews

def extract_pagination_info(page_content):
    """
    Finds the pagination <ul> element with class "pagination", extracts the max page number 
    from the second last <li> item, and returns it.
    
    Returns:
        max_page (int): The maximum number of pages found. Defaults to 1 if extraction fails.
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

# --- Main Scraping Process ---

def main():
    input_csv = "capterra_products.csv"   # CSV should have columns: Category, Product Link
    output_csv = "capterra_reviews.csv"
    
    all_reviews = []  # List to collect reviews from all product pages
    
    with open(input_csv, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        count = 0
        for row in reader:
            if count >= 3:  # Limit processing to the first 3 products for testing
                break
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
            for review in reviews:
                review["Category"] = category
                review["Product Link"] = product_link
                all_reviews.append(review)
            
            # Extract the maximum page number from the pagination.
            max_page = extract_pagination_info(page_content)
            # Loop through pages 2 to max_page (first page was already processed)
            for page in range(1, max_page + 1):
                page_url = f"{product_link}?page={page}"
                print(f"Processing pagination page: {page_url}")
                pag_page_content = fetch_page(page_url)
                if not pag_page_content:
                    print(f"Skipping pagination page {page_url} due to fetch issues.")
                    continue
                pag_reviews = extract_reviews(pag_page_content)
                print(f"Found {len(pag_reviews)} reviews on page {page}.")
                for review in pag_reviews:
                    review["Category"] = category
                    review["Product Link"] = product_link
                    all_reviews.append(review)
                # Throttle between pagination pages.
                time.sleep(random.uniform(3, 6))
            
            count += 1
            # Throttle between products.
            time.sleep(random.uniform(5, 8))
    
    # Write out the extracted reviews to a CSV file.
    fieldnames = [
        "Category", "Product Link",
        "Name", "Role",
        "Industry & Employee", "Use Duration",
        "Rating"
    ]
    with open(output_csv, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for review in all_reviews:
            writer.writerow(review)
    
    print(f"\nExtraction complete. Reviews saved to '{output_csv}'.")

if __name__ == "__main__":
    main()