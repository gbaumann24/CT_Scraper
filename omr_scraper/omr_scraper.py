import requests
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import scrape_categories

# Get all category slugs
category_slugs = scrape_categories.get_slugs()

# OMR GraphQL API URL
GRAPHQL_URL = "https://api.reviews.omr.com/graphql"

# Headers for the requests
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Content-Type": "application/json",
    "Accept": "application/json",
}

# 🚀 Step 1: Fetch All Product Slugs for All Categories
PRODUCTS_QUERY = """
query productsByCategory(
  $categorySlug: ID!,
  $page: Int!,
  $sortBy: ExploreProductsSortingEnum
) {
  products(
    categorySlug: $categorySlug,
    page: $page,
    sortBy: $sortBy
  ) {
    pagination {
      hasNextPage
    }
    products {
      slug
      title
    }
  }
}
"""

def fetch_product_slugs(category_slug):
    """Fetches all product slugs for a given category."""
    product_slugs = {}
    page = 1

    while True:
        response = requests.post(
            GRAPHQL_URL,
            json={"query": PRODUCTS_QUERY, "variables": {"categorySlug": category_slug, "page": page, "sortBy": "weighted_score"}},
            headers=HEADERS
        )

        if response.status_code != 200:
            print(f"❌ Error fetching page {page} for category {category_slug}")
            break

        data = response.json()
        products = data.get("data", {}).get("products", {}).get("products", [])
        has_next_page = data.get("data", {}).get("products", {}).get("pagination", {}).get("hasNextPage", False)

        for product in products:
            slug = product["slug"]
            if slug in product_slugs:
                product_slugs[slug].add(category_slug)
            else:
                product_slugs[slug] = {category_slug}

        if not has_next_page:
            break

        page += 1
        time.sleep(0.5)

    return product_slugs

# Multi-threading to get all product slugs
def collect_all_product_slugs(category_slugs, max_threads=10):
    all_product_slugs = {}
    
    with ThreadPoolExecutor(max_threads) as executor:
        futures = {executor.submit(fetch_product_slugs, slug): slug for slug in category_slugs}
        
        for future in as_completed(futures):
            try:
                product_slugs = future.result()
                for slug, categories in product_slugs.items():
                    if slug in all_product_slugs:
                        all_product_slugs[slug].update(categories)
                    else:
                        all_product_slugs[slug] = categories
                print(f"✅ Collected {len(product_slugs)} product slugs from {futures[future]}")
            except Exception as e:
                print(f"⚠️ Error collecting slugs for {futures[future]}: {e}")

    return all_product_slugs

# 🚀 Step 2: Fetch Reviews for Each Product
REVIEWS_QUERY = """
query reviewsByProductId($product: ID!, $page: Int!, $sortBy: ReviewsSearchSortingEnum, $perPage: Int = 10) {
  reviews(product: $product, filters: {page: $page, sortBy: $sortBy, perPage: $perPage}) {
    reviews {
      companyName
      companySize
      reviewerFirstName
      reviewerLastName
      publishedAt
      recommendationScore
      companyField
      companyPosition
      negative
      positive
    problems
    }
  }
}
"""

def fetch_reviews_for_product(slug, categories, max_pages=100):
    """Fetches reviews for a given product slug."""
    reviews_data = []
    categories_str = " ".join(categories)

    for page in range(1, max_pages + 1):
        response = requests.post(
            GRAPHQL_URL,
            json={
                "query": REVIEWS_QUERY,
                "variables": {
                    "product": slug,
                    "page": page,
                    "sortBy": "relevance",
                    "perPage": 100,
                }
            },
            headers=HEADERS
        )

        if response.status_code != 200:
            print(f"❌ Error fetching reviews for {slug} (Page {page}): {response.text}")
            continue

        data = response.json()
        reviews = data.get("data", {}).get("reviews", {}).get("reviews", [])

        if not reviews:
            break  # No more reviews, stop fetching

        for review in reviews:
            reviews_data.append({
                "product": slug,
                "categories": categories_str,
                "company": review.get("companyName", "Unknown"),
                "company_field": review.get("companyField", "Unknown"),
                "company_size": review.get("companySize", "Unknown"),
                "reviewer": f"{review.get('reviewerFirstName', '')} {review.get('reviewerLastName', '')}".strip(),
                "company_position": review.get("companyPosition", "Unknown"),
                "published_at": review.get("publishedAt", "Unknown"),
                "recommendation_score": review.get("recommendationScore", "Unknown"),
                "negative_points": review.get("negative", "Unknown"),
                "positive_points": review.get("positive", "Unknown"),
                "problems": review.get("problems", "Unknown"),
            })

    return reviews_data

# Multi-threading to scrape reviews
def scrape_all_reviews(product_slugs, max_threads=10):
    all_reviews = []
    
    with ThreadPoolExecutor(max_threads) as executor:
        future_to_slug = {executor.submit(fetch_reviews_for_product, slug, categories): slug for slug, categories in product_slugs.items()}

        for future in as_completed(future_to_slug):
            slug = future_to_slug[future]
            try:
                reviews = future.result()
                all_reviews.extend(reviews)
                print(f"✅ {len(reviews)} reviews collected for {slug}")
            except Exception as e:
                print(f"⚠️ Error scraping {slug}: {e}")

    return all_reviews

# 🚀 Step 3: Execute the Scraping Process
if __name__ == "__main__":
    print("📌 Collecting all product slugs from all categories...")
    all_product_slugs = collect_all_product_slugs(category_slugs, max_threads=10)
    print(f"✅ Total unique product slugs collected: {len(all_product_slugs)}")

    print("📌 Fetching reviews for all products...")
    all_reviews = scrape_all_reviews(all_product_slugs, max_threads=10)

    # Save to CSV
    df = pd.DataFrame(all_reviews, columns=["product", "categories", "company", "company_size", "reviewer", "published_at", "recommendation_score", "company_field", "company_position"])
    csv_filename = "omr_all_reviews.csv"
    df.to_csv(csv_filename, index=False)

    print(f"✅ Scraping completed! {len(all_reviews)} reviews saved in '{csv_filename}'.")
