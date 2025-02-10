# fetch_data.py
import requests

def get_slugs():
    url = "https://api.reviews.omr.com/graphql"
    query = {
        "operationName": "popularCategories",
        "query": "query popularCategories($limit: Int) { categories(filterHidden: true, sortByReviewCount: true, limit: $limit) { slug } }",
        "variables": { "limit": 1000 }
    }
    
    headers = {
        "Content-Type": "application/json"
    }

    response = requests.post(url, headers=headers, json=query)
    
    if response.status_code == 200:
        data = response.json()
        return [category["slug"] for category in data.get("data", {}).get("categories", [])]
    else:
        return f"Error: {response.status_code}, {response.text}"