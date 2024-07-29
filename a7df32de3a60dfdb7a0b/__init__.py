import asyncio
import logging
import random
import hashlib
import os
import re
import twikit
from datetime import datetime, timezone, timedelta
from typing import List, AsyncGenerator
from exorde_data import Item, Content, Author, CreatedAt, Url, Domain, ExternalId

# Initialize Twikit client
client = twikit.Client(language='en-US')

# Setup logging
logging.basicConfig(level=logging.INFO)

# Load all cookies and proxies from the ips.txt file
def load_proxies_and_cookies():
    cookies_folder = '/cookies'
    ips_file = os.path.join(cookies_folder, 'ips.txt')
    proxies_and_cookies = []
    with open(ips_file, 'r') as file:
        for line in file:
            ip_port, cookie_file = line.strip().split(',')
            proxy = f"socks5://{ip_port}"
            cookie_path = os.path.join(cookies_folder, cookie_file)
            proxies_and_cookies.append((proxy, cookie_path))
    return proxies_and_cookies

# Function to format created_at datetime
def format_created_at(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

# Function to handle loading proxy and cookie
class ProxyCookieLoader:
    def __init__(self, proxies_and_cookies):
        self.proxies_and_cookies = proxies_and_cookies
        self.rate_limits = {i: 50 for i in range(len(proxies_and_cookies))}
        self.last_used_time = {i: None for i in range(len(proxies_and_cookies))}
        self.total_proxies = len(proxies_and_cookies)

    async def load_next(self):
        while True:
            for index in range(self.total_proxies):
                if self.rate_limits[index] > 0:
                    proxy, cookie_file = self.proxies_and_cookies[index]
                    client.load_cookies(cookie_file)
                    logging.info(f"Loaded cookies from: {cookie_file} with proxy: {proxy}")
                    self.rate_limits[index] -= 1
                    return proxy, cookie_file, index
            await asyncio.sleep(15 * 60 + 5)  # Sleep for 15 minutes + 5 seconds buffer
            self.rate_limits = {i: 50 for i in range(self.total_proxies)}  # Reset rate limits

# Function to scrape tweets based on query
async def scrape(query: str, max_oldness_seconds: int, min_post_length: int, maximum_items_to_collect: int, proxy_cookie_loader: ProxyCookieLoader) -> AsyncGenerator[Item, None]:
    collected_items = 0
    current_time = datetime.now(timezone.utc)
    max_oldness_duration = timedelta(seconds=max_oldness_seconds)
    total_collected_items = 0

    while total_collected_items < maximum_items_to_collect:
        proxy, cookie_file, index = await proxy_cookie_loader.load_next()
        try:
            search_results = await client.search_tweet(query=query, product='Latest')
            logging.info("Search successful.")

            for tweet in search_results:
                tweet_age = current_time - tweet.created_at_datetime
                if tweet_age > max_oldness_duration:
                    continue

                content = tweet.text.strip()
                # Skip tweets with no text content or only media content
                if not content or len(content) < min_post_length or re.match(r"^(?:pic\\.twitter\\.com|https?://t\\.co/)\b", content):
                    logging.debug(f"Skipped tweet with URL: https://x.com/{tweet.user.screen_name}/status/{tweet.id}")
                    continue

                post_author = tweet.user.name if tweet.user.name else '[deleted]'
                tweet_id = str(tweet.id)
                item = Item(
                    content=Content(content),
                    author=Author(hashlib.sha1(bytes(post_author, encoding="utf-8")).hexdigest()),
                    created_at=CreatedAt(format_created_at(tweet.created_at_datetime)),
                    domain=Domain("x.com"),
                    url=Url(f"https://x.com/{tweet.user.screen_name}/status/{tweet_id}"),
                    external_id=ExternalId(tweet_id)
                )
                logging.info(f"Yielding item: {item}")
                yield item
                total_collected_items += 1
                if total_collected_items >= maximum_items_to_collect:
                    return
        except twikit.errors.TooManyRequests as e:
            logging.error(f"Rate limit exceeded: {e}. Retrying in 5 seconds...")
            await asyncio.sleep(5)
        except twikit.errors.BadRequest as e:
            logging.error(f"Bad request with cookies: {cookie_file}")
        except twikit.errors.Unauthorized as e:
            logging.error(f"Unauthorized access with cookies: {cookie_file}")
        except twikit.errors.Forbidden as e:
            logging.error(f"Forbidden access with cookies: {cookie_file}")
        except twikit.errors.NotFound as e:
            logging.error(f"Not found with cookies: {cookie_file}")
        except twikit.errors.RequestTimeout as e:
            logging.error(f"Request timeout with cookies: {cookie_file}")
        except twikit.errors.ServerError as e:
            logging.error(f"Server error with cookies: {cookie_file}")
        except twikit.errors.AccountSuspended as e:
            logging.error(f"Account suspended with cookies: {cookie_file}")
        except twikit.errors.AccountLocked as e:
            logging.error(f"Account locked with cookies: {cookie_file}")
        except twikit.errors.UserUnavailable as e:
            logging.error(f"User unavailable with cookies: {cookie_file}")
        except twikit.errors.UserNotFound as e:
            logging.error(f"User not found with cookies: {cookie_file}")
        except Exception as e:
            logging.error(f"An error occurred with cookies {cookie_file}: {e}")

# Helper function to gather results from async generator
async def gather_results(coroutine) -> List[Item]:
    results = []
    async for item in coroutine:
        results.append(item)
    return results

# Function to query tweets based on parameters in parallel
async def query(parameters) -> AsyncGenerator[Item, None]:
    max_oldness_seconds, maximum_items_to_collect, min_post_length, pick_default_keyword_weight = read_parameters(parameters)
    keywords = generate_keywords(parameters, pick_default_keyword_weight)
    proxies_and_cookies = load_proxies_and_cookies()
    proxy_cookie_loader = ProxyCookieLoader(proxies_and_cookies)

    tasks = [gather_results(scrape(keyword, max_oldness_seconds, min_post_length, maximum_items_to_collect // len(keywords), proxy_cookie_loader)) for keyword in keywords]
    
    results = await asyncio.gather(*tasks)

    for result in results:
        for item in result:
            yield item

# Function to generate multiple keywords based on parameters with specified probabilities
def generate_keywords(parameters, pick_default_keyword_weight, count=4):
    keywords = []
    for _ in range(count):
        if random.random() < pick_default_keyword_weight:  # Use the specified weight
            search_keyword = parameters.get("keyword", random.choice(SPECIAL_KEYWORDS_LIST))
        else:
            search_keyword = random.choice(SPECIAL_KEYWORDS_LIST)
        keywords.append(search_keyword)
    return keywords

# Default values for parameters
DEFAULT_OLDNESS_SECONDS = 120
DEFAULT_MAXIMUM_ITEMS = 100
DEFAULT_MIN_POST_LENGTH = 10
DEFAULT_DEFAULT_KEYWORD_WEIGHT_PICK = 0.5

def read_parameters(parameters):
    if parameters and isinstance(parameters, dict):
        max_oldness_seconds = parameters.get("max_oldness_seconds", DEFAULT_OLDNESS_SECONDS)
        maximum_items_to_collect = parameters.get("maximum_items_to_collect", DEFAULT_MAXIMUM_ITEMS)
        min_post_length = parameters.get("min_post_length", DEFAULT_MIN_POST_LENGTH)
        pick_default_keyword_weight = parameters.get("pick_default_keyword_weight", DEFAULT_DEFAULT_KEYWORD_WEIGHT_PICK)
    else:
        # Assign default values if parameters is empty or None
        max_oldness_seconds = DEFAULT_OLDNESS_SECONDS
        maximum_items_to_collect = DEFAULT_MAXIMUM_ITEMS
        min_post_length = DEFAULT_MIN_POST_LENGTH
        pick_default_keyword_weight = DEFAULT_DEFAULT_KEYWORD_WEIGHT_PICK

    return (
        max_oldness_seconds,
        maximum_items_to_collect,
        min_post_length,
        pick_default_keyword_weight,
    )
