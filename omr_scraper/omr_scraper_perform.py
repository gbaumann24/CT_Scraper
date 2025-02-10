import asyncio
import aiohttp
import pandas as pd
import scrape_categories  # assuming this module remains the same

# GraphQL API URL and headers
GRAPHQL_URL = "https://api.reviews.omr.com/graphql"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Content-Type": "application/json",
    "Accept": "application/json",
}

# GraphQL query for fetching products by category
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

# GraphQL query for fetching reviews for a product
REVIEWS_QUERY = """
query reviewsByProductId($product: ID!, $page: Int!, $sortBy: ReviewsSearchSortingEnum, $perPage: Int = 100) {
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

def clean_text(text):
    """
    Clean text by removing characters that might interfere with CSV formatting.
    Adjust or add replacements as needed.
    """
    if not isinstance(text, str):
        text = str(text)
    return (
        text.replace("\n", " ")
            .replace("\r", " ")
            .replace("\u2028", " ")  # Remove Unicode LS (Line Separator)
            .replace("\u2029", " ")  # Remove Unicode PS (Paragraph Separator)
            .replace(",", " ")
            .replace('"', " ")
            .strip()
    )


async def fetch_product_slugs(category_slug: str, session: aiohttp.ClientSession) -> dict:
    """
    Fetch all product slugs for a given category asynchronously.
    Returns a dictionary mapping product slug to a set of categories.
    """
    product_slugs = {}
    page = 1

    while True:
        payload = {
            "query": PRODUCTS_QUERY,
            "variables": {"categorySlug": category_slug, "page": page, "sortBy": "weighted_score"}
        }
        async with session.post(GRAPHQL_URL, json=payload, headers=HEADERS) as response:
            if response.status != 200:
                print(f"❌ Error fetching page {page} for category {category_slug} (Status: {response.status})")
                break

            data = await response.json()

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
        # Respectful delay between requests
        await asyncio.sleep(0.5)

    return product_slugs


async def collect_all_product_slugs(category_slugs: list, connector_limit: int = 10) -> dict:
    """
    Collect product slugs for all categories concurrently.
    Returns a dictionary mapping each unique product slug to the set of categories it belongs to.
    """
    all_product_slugs = {}
    # Limit the number of concurrent connections using a TCPConnector
    connector = aiohttp.TCPConnector(limit=connector_limit)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = {
            category: asyncio.create_task(fetch_product_slugs(category, session))
            for category in category_slugs
        }
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)

        for category, result in zip(tasks.keys(), results):
            if isinstance(result, Exception):
                print(f"⚠️ Error collecting slugs for {category}: {result}")
            else:
                for slug, cats in result.items():
                    if slug in all_product_slugs:
                        all_product_slugs[slug].update(cats)
                    else:
                        all_product_slugs[slug] = cats
                print(f"✅ Collected {len(result)} product slugs from {category}")

    return all_product_slugs


async def fetch_reviews_for_product(slug: str, categories: set, session: aiohttp.ClientSession, max_pages: int = 100) -> list:
    """
    Fetch all reviews for a given product slug asynchronously.
    Returns a list of review dictionaries.
    """
    reviews_data = []
    categories_str = clean_text(" ".join(categories))

    for page in range(1, max_pages + 1):
        payload = {
            "query": REVIEWS_QUERY,
            "variables": {"product": slug, "page": page, "sortBy": "relevance", "perPage": 100}
        }
        async with session.post(GRAPHQL_URL, json=payload, headers=HEADERS) as response:
            if response.status != 200:
                text = await response.text()
                print(f"❌ Error fetching reviews for {slug} (Page {page}): {text}")
                continue

            data = await response.json()

        reviews = data.get("data", {}).get("reviews", {}).get("reviews", [])
        if not reviews:
            break  # No more reviews available

        for review in reviews:
            # Clean each text field to remove problematic CSV characters.
            reviews_data.append({
                "product": clean_text(slug),
                "categories": categories_str,
                "company": clean_text(review.get("companyName", "Unknown")),
                "company_field": clean_text(review.get("companyField", "Unknown")),
                "company_size": clean_text(review.get("companySize", "Unknown")),
                "reviewer": clean_text(f"{review.get('reviewerFirstName', '')} {review.get('reviewerLastName', '')}".strip()),
                "company_position": clean_text(review.get("companyPosition", "Unknown")),
                "published_at": clean_text(review.get("publishedAt", "Unknown")),
                "recommendation_score": clean_text(review.get("recommendationScore", "Unknown")),
                "negative_points": clean_text(review.get("negative", "Unknown")),
                "positive_points": clean_text(review.get("positive", "Unknown")),
                "problems": clean_text(review.get("problems", "Unknown")),
            })

    return reviews_data


async def scrape_all_reviews(product_slugs: dict, connector_limit: int = 20) -> list:
    """
    Concurrently fetch reviews for all products.
    Returns a list of all review dictionaries.
    """
    all_reviews = []
    connector = aiohttp.TCPConnector(limit=connector_limit)
    async with aiohttp.ClientSession(connector=connector) as session:
        # Create a mapping from product slug to its fetch task
        tasks = {
            slug: asyncio.create_task(fetch_reviews_for_product(slug, categories, session))
            for slug, categories in product_slugs.items()
        }
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)

        for slug, result in zip(tasks.keys(), results):
            if isinstance(result, Exception):
                print(f"⚠️ Error scraping {slug}: {result}")
            else:
                all_reviews.extend(result)
                print(f"✅ Collected {len(result)} reviews for {slug}")

    return all_reviews


async def main():
    print("📌 Collecting all product slugs from all categories...")
    # Get all category slugs (assuming this is a synchronous function from your module)
    category_slugs = scrape_categories.get_slugs()

    all_product_slugs = await collect_all_product_slugs(category_slugs, connector_limit=20)
    print(f"✅ Total unique product slugs collected: {len(all_product_slugs)}")

    print("📌 Fetching reviews for all products...")
    all_reviews = await scrape_all_reviews(all_product_slugs, connector_limit=20)
    print(f"✅ Total reviews fetched: {len(all_reviews)}")

    # Create a DataFrame and save the results to CSV.
    # All fields have been cleaned so that they should not interfere with CSV formatting.
    df = pd.DataFrame(
        all_reviews,
        columns=[
            "product",
            "categories",
            "company",
            "company_size",
            "reviewer",
            "published_at",
            "recommendation_score",
            "company_field",
            "company_position",
            "negative_points",
            "positive_points",
            "problems",
        ]
    )
    csv_filename = "omr_all_reviews.csv"
    df.to_csv(csv_filename, index=False)
    print(f"✅ Scraping completed! {len(all_reviews)} reviews saved in '{csv_filename}'.")


if __name__ == "__main__":
    asyncio.run(main())