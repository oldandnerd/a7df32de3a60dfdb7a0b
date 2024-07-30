import asyncio
import logging
import hashlib
from typing import List, AsyncGenerator, Dict
import httpx
from datetime import datetime
from exorde_data import Item, Content, Author, CreatedAt, Url, Domain, ExternalId

# Setup logging
logging.basicConfig(level=logging.INFO)
logging.getLogger('httpx').setLevel(logging.WARNING)  # Set httpx logging level to WARNING to suppress info logs

# Global configuration
DEFAULT_SIZE = 10
DEFAULT_MAXIMUM_ITEMS = 25  # Default maximum items to collect
DELAY_SECONDS = 2  # Delay between each request in seconds

# Function to format created_at datetime
def format_created_at(dt_str):
    dt = datetime.strptime(dt_str, "%a %b %d %H:%M:%S %z %Y")
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

# Main scraping function
async def scrape(size: int, maximum_items_to_collect: int) -> AsyncGenerator[Item, None]:
    url = "http://192.227.159.4:8000/get_tweets"
    headers = {
        "Content-Type": "application/json"
    }
    data = {
        "size": size
    }

    collected_items = 0
    async with httpx.AsyncClient() as client:
        while collected_items < maximum_items_to_collect:
            try:
                await asyncio.sleep(DELAY_SECONDS)  # Add delay before each request
                response = await client.post(url, headers=headers, json=data)
                response.raise_for_status()
                tweets = response.json().get("tweets", [])

                for tweet in tweets:
                    if collected_items >= maximum_items_to_collect:
                        break

                    content = tweet.get("content_", "").strip()
                    if not content:
                        continue

                    post_author = tweet.get("author_", "[deleted]")
                    created_at = tweet.get("created_at_", "")
                    domain = tweet.get("domain_", "x.com")
                    url = tweet.get("url_", "")
                    external_id = tweet.get("external_id_", "")

                    item = Item(
                        content=Content(content),
                        author=Author(hashlib.sha1(bytes(post_author, encoding="utf-8")).hexdigest()),
                        created_at=CreatedAt(format_created_at(created_at)),
                        domain=Domain(domain),
                        url=Url(url),
                        external_id=ExternalId(external_id)
                    )

                    logging.info(f"Yielding item: {item}")
                    yield item
                    collected_items += 1
            except GeneratorExit:
                logging.info("GeneratorExit encountered in scrape. Closing the generator.")
                return

# Main interface function
async def query(parameters: dict) -> AsyncGenerator[Item, None]:
    size = parameters.get("size", DEFAULT_SIZE)  # Use the global default size
    maximum_items_to_collect = parameters.get("maximum_items_to_collect", DEFAULT_MAXIMUM_ITEMS)  # Use the global default max items
    logging.info(f"Querying {size} items per request.")
    try:
        async for item in scrape(size, maximum_items_to_collect):
            yield item
    except GeneratorExit:
        logging.info("GeneratorExit encountered in query. Closing the generator.")
        return

# Function to gather results for testing
async def gather_results(parameters: dict) -> List[Item]:
    results = []
    async for item in query(parameters):
        results.append(item)
    return results