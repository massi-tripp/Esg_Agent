# scripts/smoke_crawl.py
import os
import sys
import asyncio

# Reactor e policy PRIMA degli import Scrapy
os.environ.setdefault("TWISTED_REACTOR", "twisted.internet.asyncioreactor.AsyncioSelectorReactor")
if sys.platform.startswith("win"):
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except Exception:
        pass

import json
from datetime import datetime
from scrapy.crawler import CrawlerProcess
import scrapy

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
INTERIM_DIR = os.path.join(PROJECT_ROOT, "data", "interim")
os.makedirs(INTERIM_DIR, exist_ok=True)
OUT = os.path.join(INTERIM_DIR, "smoke_pages.jsonl")

def now_iso():
    return datetime.utcnow().isoformat(timespec="seconds")+"Z"

class SmokeSpider(scrapy.Spider):
    name = "smoke"
    custom_settings = {"LOG_LEVEL": "DEBUG"}

    def start_requests(self):
        url = "https://example.org/"
        self.logger.info(f"[smoke] requesting {url}")
        yield scrapy.Request(url, callback=self.parse, dont_filter=True)

    def parse(self, response):
        with open(OUT, "a", encoding="utf-8") as f:
            f.write(json.dumps({
                "url": response.url,
                "status": response.status,
                "size": len(response.body or b""),
                "ts": now_iso()
            })+"\n")
        self.logger.info(f"[smoke] wrote {OUT}")

if __name__ == "__main__":
    process = CrawlerProcess({"TELNETCONSOLE_ENABLED": False, "LOG_LEVEL": "DEBUG"})
    process.crawl(SmokeSpider)
    process.start()
