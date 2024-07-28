from setuptools import find_packages, setup

setup(
    name="a7df32de3a60dfdb7a0b",
    version="0.13.10",
    packages=find_packages(),
    install_requires=[
        "snscrape",
        "exorde_data",
        "aiohttp",
        "python-dotenv",
        "selenium==4.2.0",
        "twikit",
        "httpx-socks",
        "pathlib"
    ],
    extras_require={"dev": ["pytest", "pytest-cov", "pytest-asyncio"]},
)
