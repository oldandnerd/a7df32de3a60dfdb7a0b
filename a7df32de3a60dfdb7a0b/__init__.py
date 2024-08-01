import asyncio
import logging
import hashlib
import json
from typing import List, AsyncGenerator, Dict
import httpx
from datetime import datetime
from exorde_data import Item, Content, Author, CreatedAt, Url, Domain, ExternalId

# Setup logging
logging.basicConfig(level=logging.DEBUG)
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
    try:
        dt = datetime.strptime(dt_str, "%a %b %d %H:%M:%S %z %Y")
        formatted_dt = dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        logging.debug(f"Formatted datetime: {formatted_dt}")
        return formatted_dt
    except Exception as e:
        logging.error(f"Error formatting datetime: {e}")
        raise

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
                logging.debug(f"Sending request to {url} with data: {data}")
                await asyncio.sleep(DELAY_SECONDS)  # Add delay before each request
                response = await client.post(url, headers=headers, json=data)
                response.raise_for_status()
                tweets = response.json().get("tweets", [])
                logging.debug(f"Received tweets: {tweets}")
                break
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 500:
                    logging.error(f"Server error '500 Internal Server Error' for url '{url}'. Retrying in {RETRY_DELAY_SECONDS} seconds.")
                    await asyncio.sleep(RETRY_DELAY_SECONDS)
                else:
                    logging.error(f"HTTP error: {e}")
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

            logging.debug(f"Appending item to cache: {item}")
            cached_items.append(item)

# Function to save the current state to a file
def save_state(items: List[Item]):
    """Save the current state to a file."""
    try:
        if json is None:
            raise TypeError("json module is None")
        with open(STATE_FILE, "w") as f:
            json.dump([item.to_dict() for item in items], f)
        logging.info(f"State saved with {len(items)} items.")
    except Exception as e:
        logging.error(f"Error saving state: {e}")

# Function to load the state from a file
def load_state() -> List[Item]:
    """Load the state from a file."""
    try:
        with open(STATE_FILE, "r") as f:
            items_data = json.load(f)
            items = [SerializableItem.from_dict(item_data) for item_data in items_data]
            logging.info(f"Loaded state with {len(items)} items.")
            return items
    except (FileNotFoundError, json.JSONDecodeError):
        logging.info("No previous state found, starting fresh.")
        return []
    except Exception as e:
        logging.error(f"Error loading state: {e}")
        return []

# Extend Item class to support serialization and deserialization
class SerializableItem(Item):
    def to_dict(self):
        return {
            "content": self.content.value,
            "author": self.author.value,
            "created_at": self.created_at.value,
            "domain": self.domain.value,
            "url": self.url.value,
            "external_id": self.external_id.value,
        }

    @classmethod
    def from_dict(cls, data):
        try:
            item = cls(
                content=Content(data["content"]),
                author=Author(data["author"]),
                created_at=CreatedAt(data["created_at"]),
                domain=Domain(data["domain"]),
                url=Url(data["url"]),
                external_id=ExternalId(data["external_id"]),
            )
            logging.debug(f"Created SerializableItem from dict: {item}")
            return item
        except KeyError as e:
            logging.error(f"Missing key in data dict: {e}")
            raise

# Update the main scraping function to use Item from exorde_data
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
