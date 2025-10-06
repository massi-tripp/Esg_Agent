# scraper/run_discovery.py
# Runner con argparse e supporto a --allow-external-pdfs

import os
import sys
import csv
import argparse
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

def main(csv_path,
         max_depth=3, max_pages=100, whitelist="",
         limit=None, render_budget=40, disable_render=False,
         allow_external_pdfs=False):
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
            company_id  = row.get("company_id") or row.get("id") or row.get("slug") or row.get("company_name")
            primary_url = row.get("primary_url") or row.get("url") or row.get("homepage_url")
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
                allow_external_pdfs=allow_external_pdfs,
            )

    process.start()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Discovery ESG (crawler).")
    parser.add_argument("csv_path", nargs="?", default="data/interim/companies_urls.csv",
                        help="Percorso CSV con company_id e primary_url.")
    parser.add_argument("--max-depth", type=int, default=3)
    parser.add_argument("--max-pages", type=int, default=100)
    parser.add_argument("--whitelist", type=str, default="", help="host extra separati da virgola")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--render-budget", type=int, default=40)
    parser.add_argument("--disable-render", action="store_true")
    parser.add_argument("--allow-external-pdfs", action="store_true",
                        help="consenti PDF da host esterni (CDN, annualreport.* ecc.)")

    args = parser.parse_args()
    main(
        csv_path=args.csv_path,
        max_depth=args.max_depth,
        max_pages=args.max_pages,
        whitelist=args.whitelist,
        limit=args.limit,
        render_budget=args.render_budget,
        disable_render=args.disable_render,
        allow_external_pdfs=args.allow_external_pdfs,
    )
    print("Discovery completato. Ora esegui scraper/rank.py per classificare i risultati.")