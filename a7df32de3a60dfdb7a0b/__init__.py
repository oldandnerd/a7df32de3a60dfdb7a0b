import asyncio
import logging
import hashlib
import json
from typing import List, AsyncGenerator, Dict
import httpx
from datetime import datetime
from exorde_data import Item, Content, Author, CreatedAt, Url, Domain, ExternalId

# Setup logging
logging.basicConfig(level=logging.INFO)
logging.getLogger('httpx').setLevel(logging.WARNING)  # Suppress info logs for httpx

# Global configuration
DEFAULT_SIZE = 20
DEFAULT_MAXIMUM_ITEMS = 25  # Default maximum items to collect
DELAY_SECONDS = 2  # Delay between each request in seconds
RETRY_DELAY_SECONDS = 60  # Delay before retrying after a 500 error
STATE_FILE = "scraper_state.json"  # File to save the state

# Global cache for items
cached_items = []

# Function to format created_at datetime
def format_created_at(dt_str: str) -> str:
    """Format the created_at string into the desired format."""
    dt = datetime.strptime(dt_str, "%a %b %d %H:%M:%S %z %Y")
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

# Function to fetch data from the API
async def fetch_data(size: int):
    """Fetch data from the API and populate the global cache."""
    url = "http://192.227.159.4:8000/get_tweets"
    headers = {
        "Content-Type": "application/json"
    }
    data = {
        "size": size
    }

    async with httpx.AsyncClient() as client:
        while True:
            try:
                await asyncio.sleep(DELAY_SECONDS)  # Add delay before each request
                response = await client.post(url, headers=headers, json=data)
                response.raise_for_status()
                tweets = response.json().get("tweets", [])
                break
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 500:
                    logging.error(f"Server error '500 Internal Server Error' for url '{url}'. Retrying in {RETRY_DELAY_SECONDS} seconds.")
                    await asyncio.sleep(RETRY_DELAY_SECONDS)
                else:
                    raise

        for tweet in tweets:
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

            cached_items.append(item)

# Function to save the current state to a file
def save_state(items: List[Item]):
    """Save the current state to a file."""
    with open(STATE_FILE, "w") as f:
        json.dump([item.__dict__ for item in items], f)

# Function to load the state from a file
def load_state() -> List[Item]:
    """Load the state from a file."""
    try:
        with open(STATE_FILE, "r") as f:
            items_data = json.load(f)
            return [Item(**item_data) for item_data in items_data]
    except (FileNotFoundError, json.JSONDecodeError):
        return []

# Main scraping function
async def scrape(size: int, maximum_items_to_collect: int) -> AsyncGenerator[Item, None]:
    """Scrape data and yield items up to the maximum specified."""
    collected_items = 0

    # Load the state if it exists
    global cached_items
    cached_items.extend(load_state())

    try:
        while collected_items < maximum_items_to_collect:
            if not cached_items:
                await fetch_data(size)

            try:
                item = cached_items.pop(0)
                logging.info(f"Yielding item: {item}")
                yield item
                collected_items += 1
                save_state(cached_items)
            except GeneratorExit:
                logging.info("GeneratorExit encountered within loop. Saving state and re-raising exception.")
                save_state(cached_items)
                raise
    except GeneratorExit:
        logging.info("GeneratorExit encountered in scrape. Saving state and closing the generator.")
        save_state(cached_items)
    finally:
        save_state(cached_items)
        # Add any necessary cleanup code here (e.g., closing connections)
        pass

# Main interface function
async def query(parameters: Dict) -> AsyncGenerator[Item, None]:
    """Query interface for collecting items."""
    size = parameters.get("size", DEFAULT_SIZE)  # Use the global default size
    maximum_items_to_collect = parameters.get("maximum_items_to_collect", DEFAULT_MAXIMUM_ITEMS)  # Use the global default max items
    logging.info(f"Querying {size} items per request.")
    try:
        async for item in scrape(size, maximum_items_to_collect):
            yield item
    except GeneratorExit:
        logging.info("GeneratorExit encountered in query. Saving state and closing the generator.")
        save_state(cached_items)
    finally:
        save_state(cached_items)
        # Add any necessary cleanup code here (e.g., closing connections)
        pass

# Function to gather results for testing
async def gather_results(parameters: Dict) -> List[Item]:
    """Gather results for testing purposes."""
    results = []
    async for item in query(parameters):
        results.append(item)
    return results

# Load the state on start
cached_items = load_state()
