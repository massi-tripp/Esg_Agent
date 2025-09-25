# scraper/run_discovery.py
# PATCH: aggiunti parametri CLI: limit, render_budget, disable_render

import os
import sys
import csv
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from .discover import DiscoverySpider

def get_settings():
    s = get_project_settings()
    try:
        import scrapy_playwright  # noqa
        s.set("DOWNLOAD_HANDLERS", {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        })
    except Exception:
        pass
    return s

def main(csv_path="data/interim/companies_urls.csv",
         max_depth=3, max_pages=100, whitelist="",
         limit=None, render_budget=40, disable_render=False):
    if not os.path.exists(csv_path):
        print(f"CSV non trovato: {csv_path}")
        sys.exit(1)

    settings = get_settings()
    process = CrawlerProcess(settings)

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if limit and i >= limit:
                break
            company_id = row.get("company_id") or row.get("id") or row.get("slug")
            primary_url = row.get("primary_url") or row.get("url")
            if not company_id or not primary_url:
                continue
            process.crawl(
                DiscoverySpider,
                company_id=company_id.strip(),
                primary_url=primary_url.strip(),
                max_depth=max_depth,
                max_pages=max_pages,
                whitelist=whitelist,
                render_budget=render_budget,
                disable_render=disable_render,
            )

    process.start()

if __name__ == "__main__":
    args = sys.argv[1:]
    csv_path  = args[0] if len(args) >= 1 else "data/interim/companies_urls.csv"
    max_depth = int(args[1]) if len(args) >= 2 else 3
    max_pages = int(args[2]) if len(args) >= 3 else 100
    whitelist = args[3] if len(args) >= 4 else ""
    limit     = int(args[4]) if len(args) >= 5 else None
    render_budget = int(args[5]) if len(args) >= 6 else 40
    disable_render = (args[6].lower() in ("true", "1", "yes")) if len(args) >= 7 else False
    main(csv_path, max_depth, max_pages, whitelist, limit, render_budget, disable_render)
