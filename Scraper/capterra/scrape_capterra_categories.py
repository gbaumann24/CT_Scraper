import asyncio
import json
import random
from bs4 import BeautifulSoup
import httpx
from aiolimiter import AsyncLimiter

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:115.0) Gecko/20100101 Firefox/115.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, wie Gecko) Chrome/115.0.0.0 Safari/537.36",
]

BASE_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.google.com/",
    "Cache-Control": "max-age=0",
    "Connection": "keep-alive",
    # "Cookie": "YOUR_COOKIE_IF_NEEDED",  # Uncomment and add if the site requires cookies.
}

# Rate limiter: adjust these values as needed.
limiter = AsyncLimiter(max_rate=1, time_period=1)

async def fetch_page(url: str, client: httpx.AsyncClient, retries: int = 3) -> httpx.Response:
    for attempt in range(1, retries + 1):
        # Update the client headers with a random user agent each attempt.
        headers = BASE_HEADERS.copy()
        headers["User-Agent"] = random.choice(USER_AGENTS)
        client.headers.update(headers)
        async with limiter:
            try:
                response = await client.get(url, timeout=10)
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 5))
                    print(f"Received 429 Too Many Requests. Waiting for {retry_after} seconds before retrying...")
                    await asyncio.sleep(retry_after)
                    continue
                response.raise_for_status()
                print(f"Successfully fetched {url}")
                return response
            except httpx.HTTPStatusError as exc:
                print(f"HTTP error on attempt {attempt}: {exc}")
            except httpx.RequestError as exc:
                print(f"Request error on attempt {attempt}: {exc}")
        delay = 2 ** attempt
        print(f"Waiting for {delay} seconds before next attempt...")
        await asyncio.sleep(delay)
    raise Exception("Failed to fetch page after several retries.")

async def main():
    url = "https://www.capterra.ch/directory"
    async with httpx.AsyncClient() as client:
        try:
            response = await fetch_page(url, client)
        except Exception as e:
            print(f"Seite konnte nicht abgerufen werden: {e}")
            return

        soup = BeautifulSoup(response.content, "html.parser")
        category_links = soup.select("a.list-group-item")
        results = []
        for link in category_links:
            href = link.get("href")
            text = link.get_text(strip=True)
            results.append({"text": text, "href": href})

    output_filename = "capterra_categories.json"
    with open(output_filename, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=4)
    print(f"Die Kategorien wurden in '{output_filename}' gespeichert.")

if __name__ == "__main__":
    asyncio.run(main())