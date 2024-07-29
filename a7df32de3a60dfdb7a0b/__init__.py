import asyncio
import logging
import random
import hashlib
from typing import List, AsyncGenerator
import os
from datetime import datetime, timezone, timedelta
import re
import twikit
import collections

from exorde_data import Item, Content, Author, CreatedAt, Url, Domain, ExternalId

# Initialize Twikit client
client = twikit.Client(language='en-US')

# Setup logging
logging.basicConfig(level=logging.INFO)
logging.getLogger('httpx').setLevel(logging.WARNING)  # Set httpx logging level to WARNING to suppress info logs


##### SPECIAL MODE
# TOP 222
SPECIAL_KEYWORDS_LIST = [    
    "the",
    "the",
    "lord",
    "ords",
    "brc20",
    "paris2024",
    "paris2024",
    "olympic",
    "olympic",
    "Acura",
    "Alfa Romeo",
    "Aston Martin",
    "Audi",
    "Bentley",
    "BMW",
    "Buick",
    "Cadillac",
    "Chevrolet",
    "Chrysler",
    "Dodge",
    "Ferrari",
    "Fiat",
    "Ford",
    "Genesis",
    "GMC",
    "Honda",
    "Hyundai",
    "Infiniti",
    "Jaguar",
    "Jeep",
    "Kia",
    "Lamborghini",
    "Land Rover",
    "Lexus",
    "Lincoln",
    "Lotus",
    "Maserati",
    "Mazda",
    "Taiko",
    "Taiko labs",
    "McLaren",
    "Mercedes-Benz",
    "MINI",
    "Mitsubishi",
    "Nissan",
    "Porsche",
    "Ram",
    "Renault",
    "Rolls-Royce",
    "Subaru",
    "Tesla",
    "Toyota",
    "Volkswagen",
    "Volvo",    
    "BlackRock",
    "Vanguard",
    "State Street",
    "advisors",
    "Fidelity",
    "Fidelity Investments",
    "Asset Management",
    "Asset",
    "digital asset",
    "NASDAQ Composite",
    "Dow Jones Industrial Average",
    "Gold",
    "Silver",
    "Brent Crude",
    "WTI Crude",
    "EUR",
    "US",
    "YEN"
    "UBS",
    "PIMCO",
    "schroders",
    "aberdeen",    
    "louis vuitton",
    "moet Chandon",
    "hennessy",
    "dior",
    "fendi",
    "givenchy",
    "celine",
    "tag heuer",
    "bvlgari",
    "dom perignon",
    "hublot",
    "Zenith",    
    "meme", 
    "coin", 
    "memecoin", 
    "pepe", 
    "doge", 
    "shib",
    "floki",
    "dogtoken",
    "trump token",
    "barron token",    
    "DOGE",
    "SHIB",
    "PEPE",
    "BONK",
    "WIF",
    "FLOKI",
    "MEME",    
    "DOGE",
    "SHIB",
    "PEPE",
    "BONK",
    "WIF",
    "FLOKI",
    "MEME",
    "DOGE",
    "SHIB",
    "PEPE",
    "BONK",
    "WIF",
    "FLOKI",
    "MEME",
    "TRUMP",
    "BabyDoge",
    "ERC20",
    "BONE",
    "COQ",
    "WEN",
    "BITCOIN",
    "ELON",
    "SNEK",
    "MYRO",
    "PORK",
    "TOSHI",
    "SMOG",
    "LADYS",
    "AIDOGE",
    "TURBO",
    "TOKEN",
    "SAMO",
    "KISHU",
    "TSUKA",
    "LEASH",
    "QUACK",
    "VOLT",
    "PEPE2.0",
    "JESUS",
    "MONA",
    "DC",
    "WSM",
    "PIT",
    "QOM",
    "PONKE",
    "SMURFCAT",
    "AKITA",
    "VINU",
    "ANALOS",
    "BAD",
    "CUMMIES",
    "HONK",
    "HOGE",
    "$MONG",
    "SHI",
    "BAN",
    "RAIN",
    "TAMA",
    "PAW",
    "SPX",
    "HOSKY",
    "BOZO",
    "DOBO",
    "PIKA",
    "CCC",
    "REKT",
    "WOOF",
    "MINU",
    "WOW",
    "PUSSY",
    "KEKE",
    "DOGGY",
    "KINGSHIB",
    "CHEEMS",
    "SMI",
    "OGGY",
    "DINGO",
    "DONS",
    "GRLC",
    "AIBB",
    "CATMAN",
    "XRP",
    "CAT",
    "数字資産",  # Digital Asset (Japanese)
    "仮想",  # Virtual (Japanese)
    "仮想通貨",  # Virtual Currency (Japanese)
    "自動化",  # Automation (Japanese)
    "アルゴリズム",  # Algorithm (Japanese)
    "コード",  # Code (Japanese)
    "機械学習",  # Machine Learning (Japanese)
    "ブロックチェーン",  # Blockchain (Japanese)
    "サイバーセキュリティ",  # Cybersecurity (Japanese)
    "人工",  # Artificial (Japanese)
    "合成",  # Synthetic (Japanese)
    "主要",  # Major (Japanese)
    "IoT",
    "クラウド",  # Cloud (Japanese)
    "ソフトウェア",  # Software (Japanese)
    "API",
    "暗号化",  # Encryption (Japanese)
    "量子",  # Quantum (Japanese)
    "ニューラルネットワーク",  # Neural Network (Japanese)
    "オープンソース",  # Open Source (Japanese)
    "ロボティクス",  # Robotics (Japanese)
    "デブオプス",  # DevOps (Japanese)
    "5G",
    "仮想現実",  # Virtual Reality (Japanese)
    "拡張現実",  # Augmented Reality (Japanese)
    "バイオインフォマティクス",  # Bioinformatics (Japanese)
    "ビッグデータ",  # Big Data (Japanese)
    "大統領",  # President (Japanese)
    "行政",  # Administration (Japanese)
    "Binance",
    "Bitcoin ETF",
    "政治",  # Politics (Japanese)
    "政治的",  # Political (Japanese)
    "ダイアグラム",  # Diagram (Japanese)
    "$algo",
    "$algo",
    "%23CAC",
    "%23G20",
    "%23IPO",
    "%23NASDAQ",
    "%23NYSE",
    "%23OilPrice",
    "%23SP500",
    "%23USD",
    "%23airdrop",
    "%23altcoin",
    "%23bonds",
    "%23price",
    "AI",
    "AI",
    "AI",
    "AI",
    "AUDNZD",
    "Alphabet%20(GOOG)",
    "Apple",
    "Aprendizaje Automático",
    "BNB",
    "Berkshire",
    "Biden administration",
    "Binance",
    "Bitcoin%20ETF",
    "Black%20Rock",
    "BlackRock",
    "BlackRock",
    "Branche",
    "Brazil",
    "CAC40",
    "COIN",
    "Canada",
    "China",
    "Coinbase",
    "Congress",
    "Crypto",
    "Crypto",
    "Crypto",
    "Cryptocurrencies",
    "Cryptos",
    "DeFi",
    "Diagramm",
    "Dios mío",
    "DowJones",
    "ETF",
    "ETFs",
    "EU",
    "EU",
    "EURUSD",
    "Elon",
    "Elon",
    "Elon",
    "Elon%20musk",
    "Europe",
    "European%20union%20(EU)",
    "FB%20stock",
    "FTSE",
    "Firma",
    "France",
    "GDP",
    "GPU",
    "GameFi",
    "Gensler",
    "Germany",
    "Gerücht",
    "Geschäft",
    "Gesundheit",
    "Gewinn",
    "Gewinn",
    "Heilung",
    "IA",
    "IA",
    "IPO",
    "Israel",
    "Israel",
    "Israel",
    "Juego",
    "KI",
    "Konflikt",
    "Kraken",
    "LGBTQ rights",
    "LVMH",
    "Land",
    "Luxus",
    "Marke",
    "Maschinelles Lernen",
    "Mexico",
    "NFLX",
    "NFT",
    "NFT",
    "NFTs",
    "NYSE",
    "Nachrichten",
    "Nasdaq%20100",
    "Oh Dios mío",
    "Openfabric",
    "Openfabric AI",
    "Openfabric",
    "OFN",
    "PLTR",
    "Palestine",
    "Palestine",
    "Palestine",
    "País",
    "Politik",
    "Produkt",
    "Roe v. Wade",
    "Silicon Valley",
    "Spiel",
    "Spot%20ETF",
    "Start-up",
    "Streaming",
    "Supreme Court",
    "Technologie",
    "Tesla",
    "UE",
    "UE",
    "USA",
    "USDEUR",
    "United%20states",
    "Unterhaltung",
    "Verlust",
    "Virus",
    "Vorhersage",
    "WallStreet",
    "WarrenBuffett",
    "Warren Buffett",
    "Web3",
    "X.com",
    "XAUUSD",
    "Xitter",
    "abortion",
    "achetez",
    "actualité",
    "airdrop",
    "airdrops",
    "alert",
    "algorand",
    "algorand",
    "algorand",
    "amazon",
    "analytics",
    "announcement",
    "apprentissage",
    "artificial intelligence",
    "artificial intelligence",
    "asset",
    "asset%20management",
    "attack",
    "attack",
    "attack",
    "attentat",
    "authocraty",
    "balance sheet",
    "bank",
    "bear",
    "bearish",
    "bears",
    "beliebt",
    "bezos",
    "biden",
    "biden",
    "biden",
    "biden",
    "data",
    "develop",
    "virtual",
    "automation",
    "algorithm",
    "code",
    "machine learning",
    "blockchain",
    "cybersecurity",
    "artificial",
    "synth",
    "synthetic",
    "major",
    "IoT",
    "cloud",
    "software",
    "API",
    "encryption",
    "quantum",
    "neural",
    "open source",
    "robotics",
    "devop",
    "5G",
    "virtual reality",
    "augmented reality",
    "bioinformatics",
    "big data",
    "billion",
    "bitcoin",
    "bizness",
    "blockchain",
    "bond",
    "breaking news",
    "breaking%20news",
    "btc",
    "btc",
    "btc",
    "btc",
    "btc",
    "btc",
    "btc",
    "btc",
    "budget",
    "bull",
    "bullish",
    "bulls",
    "business",
    "businesses",
    "buy support",
    "cardano",
    "cash flow",
    "cbdc",
    "choquant",
    "climate change action",
    "climate change",
    "climate tech startups",
    "communist",
    "companies",
    "company",
    "compound interest",
    "compra ahora"
    "compra",
    "conflict",
    "conflict",
    "conflicto",
    "conflit",
    "congress",
    "conservatives",
    "corporate",
    "corporation",
    "credit",
    "crime",
    "crisis",
    "crude%20oil",
    "crypto",
    "crypto",
    "crypto",
    "crypto",
    "crypto",
    "cryptocurrency",
    "cryptocurrency",
    "cura",
    "currencies",
    "currency",
    "currency",
    "database",
    "debit",
    "debt",
    "debt",
    "decentralized finance",
    "decentralized",
    "decline",
    "deep learning",
    "defi",
    "democracy",
    "diffusion",
    "digital",
    "divertissement",
    "dividend",
    "doge",
    "dogecoin",
    "démarrage",
    "e-commerce",
    "economy",
    "economy",
    "education startups",
    "education",
    "elections",
    "elisee",
    "embargo",
    "embassy",
    "empresa",
    "entreprise",
    "entretenimiento",
    "equity",
    "erc20",
    "eth",
    "eth",
    "eth",
    "eth",
    "eth",
    "ethereum",
    "exchange rate",
    "expense",
    "extremism",
    "fair%20launch",
    "fascist",
    "finance",
    "finance",
    "financial advisor",
    "financial planning",
    "financing",
    "fintech",
    "fintech",
    "fintech",
    "fiscal policy",
    "fixed income",
    "foreign aid",
    "foreign exchange",
    "foreign policy",
    "forex",
    "forex",
    "founder CEO",
    "founders",
    "fusion",
    "gagner",
    "gain",
    "ganancia",
    "ganar",
    "gas",
    "gaza",
    "gaza",
    "gaza",
    "government",
    "governments",
    "graphique",
    "gross domestic product",
    "growth",
    "gráfico",
    "gun control",
    "gun violence prevention",
    "hamas",
    "hamas",
    "hamas",
    "hamas",
    "healthcare startups",
    "healthcare",
    "helion",
    "hft trading",
    "holdings",
    "hostage",
    "hostage",
    "immigration reform",
    "immigration",
    "impactante",
    "impactante",
    "impeachment",
    "income",
    "increíble",
    "increíble",
    "incroyable",
    "industria",
    "industrie",
    "inflation",
    "inflation",
    "infrastructure",
    "insider trading",
    "insider",
    "insurance",
    "intraday",
    "investing",
    "investment",
    "investor",
    "investors",
    "jerusalem",
    "jeu",
    "kaufen",
    "kremlin",
    "legal",
    "legal%20tender",
    "liability",
    "libertarian",
    "liquidity",
    "loan",
    "long",
    "lujo",
    "luxe",
    "machine learning",
    "macron",
    "macron",
    "macron",
    "en marche",
    "parti",
    "marca",
    "margin",
    "mark%20zuckerberg",
    "market capitalization",
    "market maker",
    "market",
    "markets",
    "marque",
    "mein Gott",
    "middle east",
    "middle east",
    "middle east",
    "million",
    "mint",
    "missile",
    "missile",
    "missile",
    "mon Dieu",
    "monero",
    "money",
    "mortgage",
    "moscow",
    "mutual fund",
    "nasdaq",
    "national security",
    "national%20emergency",
    "national%20security",
    "natural%20gas",
    "negocios",
    "net income",
    "net worth",
    "new project",
    "new startup",
    "news",
    "newsfeed",
    "newsflash",
    "nft",
    "nft%20latform",
    "nftcommunity",
    "nfts",
    "noticias",
    "nuclear",
    "official",
    "oil",
    "parliament",
    "pays",
    "perder",
    "perdido",
    "perdre",
    "perdu",
    "plummet",
    "police",
    "politician",
    "politicians",
    "politique",
    "polkadot",
    "polygon",
    "política",
    "populaire",
    "popular",
    "populism",
    "portfolio",
    "predicción",
    "press",
    "price-to-earnings ratio",
    "producto",
    "produit",
    "profit",
    "promising company",
    "protocols",
    "prédiction",
    "putin",
    "putin",
    "putin",
    "putin",
    "poutine",
    "poutine",
    "poutine",
    "poutine",
    "vladimir putin",
    "vladimir putin",
    "vladimir putin",
    "xi jinping",
    "xi jinping",
    "xi jinping",
    "racial justice",
    "recession",
    "renault trucks",
    "renault trucks",
    "renault",
    "renault",
    "resistance sell",
    "retirement planning",
    "return on investment",
    "riots",
    "ripple",
    "risk",
    "robotics",
    "rumeur",
    "rumor",
    "russia",
    "s&p500",
    "sales",
    "salud",
    "sam20altman",
    "santé",
    "satoshi",
    "schockierend",
    "scraping",
    "securities",
    "security%20token",
    "self-driving cars",
    "senate",
    "senator",
    "senators",
    "shardeum",
    "short",
    "silvio micali",
    "solana",
    "solana%20sol",
    "sp500",
    "space exploration",
    "space tech startups",
    "stablecoin",
    "startup",
    "startup",
    "stock market",
    "stock",
    "stocks",
    "streaming",
    "syria",
    "takeoff",
    "tax",
    "tech startups",
    "technologie",
    "technology",
    "tecnología",
    "token",
    "toyota",
    "trade",
    "trading",
    "trading",
    "traitement",
    "treasury bill",
    "trump",
    "trump",
    "trump",
    "trump",
    "vote",
    "vote",
    "vote",
    "election",
    "election",
    "election",
    "voter",
    "voter",
    "million",
    "club",
    "tech",
    "nvda",
    "machine",
    "generative",
    "reinforcement",
    "official",
    "twitter",
    "ukraine",
    "unglaublich",
    "unicorns",
    "unicorns",
    "us%20president",
    "usdt",
    "usdt",
    "usdt",
    "usdt",
    "utility%20token",
    "venture capital",
    "venture capital",
    "venture capital",
    "verloren",
    "virus",
    "virus",
    "volvo group",
    "volvo trucks",
    "volvo",
    "voting rights",
    "wall street",
    "war in Ukraine",
    "war",
    "web3",
    "web3",
    "white house",
    "worldcoin",
    "xrp",
    "yield",
    "zero knowledge",
    "zksync",    
    "renewables",
    "energy",
    "infrastructure",
    "infrastructure investment",
    "FDI investment",
    "foreign investment",
    "foreign policy",
    "new policy",
    "new policies",
    "ГПУ",
    "ЕС",
    "ИИ",
    "Игра",
    "Илон",
    "Машинное обучение",
    "Страна",
    "бизнес",
    "бренд",
    "вирус",
    "график",
    "здоровье",
    "индустрия",
    "компания",
    "конфликт",
    "купи сейчас",
    "лечение",
    "невероятный",
    "новости",
    "о боже мой",
    "победа",
    "политика",
    "популярный",
    "поражение",
    "потеря",
    "потоковая передача",
    "прибыль",
    "прогноз",
    "продукт",
    "развлечение",
    "роскошь",
    "слух",
    "стартап",
    "технологии",
    "шокирующий",
    "أخبار",
    "أعمال",
    "إيلون",
    "اشتر الآن",
    "الاتحاد الأوروبي",
    "الذكاء الاصطناعي",
    "بث مباشر",
    "بلد",
    "ترفيه",
    "تعلم الآلة",
    "تكنولوجيا",
    "توقع",
    "خسارة",
    "رائع",
    "ربح",
    "رسم بياني",
    "سياسة",
    "شائعة",
    "شركة ناشئة",
    "شركة",
    "شهير",
    "صادم",
    "صحة",
    "صراع",
    "صناعة",
    "ضائع",
    "علاج",
    "علامة تجارية",
    "فخامة",
    "فوز",
    "فيروس",
    "لعبة",
    "منتج",
    "وحدة معالجة الرسومات",
    "يا إلهي",
    "ああ、神様",
    "イーロン",
    "ウイルス",
    "エンターテインメント",
    "ゲーム",
    "スタートアップ",
    "ストリーミング",
    "チャート",
    "テクノロジー",
    "ニュース",
    "ビジネス",
    "ブランド",
    "业务",
    "予測",
    "产品",
    "人工智能",
    "人気",
    "今買う",
    "令人难以置信",
    "令人震惊",
    "会社",
    "信じられない",
    "健康",
    "健康",
    "公司",
    "冲突",
    "初创企业",
    "利益",
    "勝利",
    "品牌",
    "哦，我的天啊",
    "噂",
    "国",
    "国家",
    "图表",
    "埃隆",
    "失われた",
    "失去",
    "娱乐",
    "技术",
    "收益",
    "政治",
    "政治",
    "敗北",
    "新闻",
    "机器学习",
    "機械学習",
    "欧盟",
    "治疗",
    "治療",
    "流媒体",
    "游戏",
    "热门",
    "産業",
    "病毒",
    "立刻购买",
    "紛争",
    "行业",
    "衝撃的",
    "製品",
    "谣言",
    "豪华",
    "赢",
    "输",
    "预测",
    "高級"
    ]
############


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







class ProxyCookieLoader:
    def __init__(self, proxies_and_cookies):
        self.proxies_and_cookies = proxies_and_cookies
        self.total_proxies = len(proxies_and_cookies)
        self.proxy_usage_count = {proxy: 0 for proxy, _ in proxies_and_cookies}
        self.proxy_last_used = {proxy: datetime.min for proxy, _ in proxies_and_cookies}
        self.max_requests_per_proxy = 50
        self.default_cooldown_period = timedelta(minutes=15)  # Default cooldown period for other errors
        self.request_interval = timedelta(seconds=24)
        self.rate_limit_cooldown = timedelta(minutes=15)  # Specific cooldown for rate limits

    async def load_next(self):
        while True:
            now = datetime.now()
            available_proxies = [
                (index, proxy, cookie_file) for index, (proxy, cookie_file) in enumerate(self.proxies_and_cookies)
                if self.proxy_usage_count[proxy] < self.max_requests_per_proxy and 
                (now - self.proxy_last_used[proxy]) >= self.default_cooldown_period
            ]

            if available_proxies:
                index, proxy, cookie_file = random.choice(available_proxies)
                self.proxy_usage_count[proxy] += 1
                self.proxy_last_used[proxy] = now
                client.load_cookies(cookie_file)
                logging.info(f"Loaded cookies from: {cookie_file} with proxy: {proxy}")
                return proxy, cookie_file
            else:
                next_available_time = min(self.proxy_last_used.values()) + self.default_cooldown_period
                wait_time = max((next_available_time - now).total_seconds(), 0)
                logging.info(f"No proxies available. Next proxy available in {wait_time:.2f} seconds.")
                await asyncio.sleep(wait_time)

    def mark_proxy_unavailable(self, proxy, cooldown_period=None):
        if cooldown_period is None:
            cooldown_period = self.default_cooldown_period
        self.proxy_last_used[proxy] = datetime.now() + cooldown_period
        logging.info(f"Marked proxy {proxy} as unavailable for {cooldown_period.total_seconds() / 60} minutes.")

    def reset_usage(self):
        now = datetime.now()
        for proxy, last_used in self.proxy_last_used.items():
            if (now - last_used) >= self.default_cooldown_period:
                self.proxy_usage_count[proxy] = 0
        logging.info("All proxies have been reset.")





buffer = []  # Initialize a global buffer to store extra items

async def scrape(query: str, max_oldness_seconds: int, min_post_length: int, maximum_items_to_collect: int, proxy_cookie_loader: ProxyCookieLoader) -> AsyncGenerator[Item, None]:
    global buffer
    collected_items = 0
    current_time = datetime.now(timezone.utc)
    max_oldness_duration = timedelta(seconds=max_oldness_seconds)

    try:
        while collected_items < maximum_items_to_collect:
            # Check the buffer first
            if buffer:
                yield buffer.pop(0)
                collected_items += 1
                continue

            proxy, cookie_file = await proxy_cookie_loader.load_next()
            try:
                search_results = await client.search_tweet(query=query, product='Latest', count=100)
                logging.info("Search successful.")

                for tweet in search_results:
                    tweet_age = current_time - tweet.created_at_datetime
                    if tweet_age > max_oldness_duration:
                        continue

                    content = tweet.text.strip()
                    # Skip tweets with no text content or only media content
                    if not content or len(content) < min_post_length or re.match(r"^(?:pic\.twitter\.com|https?://t\.co/)\b", content):
                        logging.debug(f"Skipped tweet with URL: https://x.com/{tweet.user.screen_name}/status/{tweet.id}")
                        continue

                    post_author = tweet.user.name if tweet.user.name else '[deleted]'
                    item = Item(
                        content=Content(content),
                        author=Author(hashlib.sha1(bytes(post_author, encoding="utf-8")).hexdigest()),
                        created_at=CreatedAt(format_created_at(tweet.created_at_datetime)),
                        domain=Domain("x.com"),
                        url=Url(f"https://x.com/{tweet.user.screen_name}/status/{tweet.id}"),
                        external_id=ExternalId(str(tweet.id))
                    )
                    if collected_items < maximum_items_to_collect:
                        logging.info(f"Yielding item tweet: {item}")
                        yield item
                        collected_items += 1
                    else:
                        buffer.append(item)

                    # Process replies if available
                    if hasattr(tweet, 'replies') and tweet.replies:
                        for reply in tweet.replies:
                            reply_age = current_time - reply.created_at_datetime
                            if reply_age > max_oldness_duration:
                                continue

                            reply_content = reply.text.strip()
                            if not reply_content or len(reply_content) < min_post_length or re.match(r"^(?:pic\.twitter\.com|https?://t\.co/)\b", reply_content):
                                logging.debug(f"Skipped reply with URL: https://x.com/{reply.user.screen_name}/status/{reply.id}")
                                continue

                            reply_author = reply.user.name if reply.user.name else '[deleted]'
                            reply_item = Item(
                                content=Content(reply_content),
                                author=Author(hashlib.sha1(bytes(reply_author, encoding="utf-8")).hexdigest()),
                                created_at=CreatedAt(format_created_at(reply.created_at_datetime)),
                                domain=Domain("x.com"),
                                url=Url(f"https://x.com/{reply.user.screen_name}/status/{reply.id}"),
                                external_id=ExternalId(str(reply.id))
                            )
                            if collected_items < maximum_items_to_collect:
                                logging.info(f"Yielding reply item replies: {reply_item}")
                                yield reply_item
                                collected_items += 1
                            else:
                                buffer.append(reply_item)

                await asyncio.sleep(1)  # Ensure some delay between requests
            except twikit.errors.TooManyRequests as e:
                logging.error(f"Rate limit exceeded: {e}. Proxy will be unavailable for 15 minutes.")
                proxy_cookie_loader.mark_proxy_unavailable(proxy, proxy_cookie_loader.rate_limit_cooldown)
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
    except GeneratorExit:
        logging.info("GeneratorExit: Cleaning up the scrape generator.")



async def gather_results(coroutine) -> List[Item]:
    results = []
    try:
        async for item in coroutine:
            results.append(item)
    except GeneratorExit:
        logging.info("GeneratorExit: Cleaning up the gather_results generator.")
    return results



async def query(parameters) -> AsyncGenerator[Item, None]:
    max_oldness_seconds, maximum_items_to_collect, min_post_length, pick_default_keyword_weight = read_parameters(parameters)
    proxies_and_cookies = load_proxies_and_cookies()
    proxy_cookie_loader = ProxyCookieLoader(proxies_and_cookies)
    keyword_count = min(4, len(proxies_and_cookies))  # Use the number of proxies as the limit

    keywords = generate_keywords(parameters, pick_default_keyword_weight, proxies_and_cookies, count=keyword_count)

    tasks = [gather_results(scrape(keyword, max_oldness_seconds, min_post_length, maximum_items_to_collect // len(keywords), proxy_cookie_loader)) for keyword in keywords]
    
    results = await asyncio.gather(*tasks)

    try:
        for result in results:
            for item in result:
                yield item
    except GeneratorExit:
        logging.info("GeneratorExit: Cleaning up the query generator.")




# Initialize a deque to keep track of the last 100 keywords used
keyword_history = collections.deque(maxlen=100)

def generate_keywords(parameters, pick_default_keyword_weight, proxies_and_cookies, count=4):
    keywords = []
    actual_count = min(count, len(proxies_and_cookies))  # Ensure we don't exceed available proxies
    for _ in range(actual_count):
        search_keyword = None
        # Ensure we pick a keyword not recently used
        while not search_keyword or search_keyword in keyword_history:
            if random.random() < pick_default_keyword_weight:  # Use the specified weight
                search_keyword = parameters.get("keyword", random.choice(SPECIAL_KEYWORDS_LIST))
            else:
                search_keyword = random.choice(SPECIAL_KEYWORDS_LIST)

        keyword_history.append(search_keyword)
        logging.info(f"Selected keyword: {search_keyword}")
        keywords.append(search_keyword)
    return keywords



# Default values for parameters
DEFAULT_OLDNESS_SECONDS = 120
DEFAULT_MAXIMUM_ITEMS = 25
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

    return max_oldness_seconds, maximum_items_to_collect, min_post_length, pick_default_keyword_weight