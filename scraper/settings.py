# scraper/settings.py
# Impostazioni Scrapy centralizzate (opzionali). Il runner imposta già le principali.

BOT_NAME = "esg_discovery"
SPIDER_MODULES = ["scraper"]
NEWSPIDER_MODULE = "scraper"

ROBOTSTXT_OBEY = True

CONCURRENT_REQUESTS = 8
DOWNLOAD_DELAY = 0.3
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 0.5
AUTOTHROTTLE_MAX_DELAY = 3.0

DEFAULT_REQUEST_HEADERS = {
    "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7,fr;q=0.6,de;q=0.5,es;q=0.4",
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
}

TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
LOG_LEVEL = "INFO"
