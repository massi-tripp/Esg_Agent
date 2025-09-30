# scraper/run_discovery.py
# Runner che legge companies_clean.csv (o percorso passato) e avvia lo spider.
# Mantiene i flag e registra scrapy-playwright se disponibile.

import os
import sys
import csv
from typing import List, Dict, Optional

# Reactor PRIMA degli import Scrapy (stabile su Windows)
os.environ.setdefault("TWISTED_REACTOR", "twisted.internet.asyncioreactor.AsyncioSelectorReactor")
try:
    import asyncio
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
except Exception:
    pass

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from .discover import DiscoverySpider

CSV_DEFAULT_PATH = "data/processed/companies_clean.csv"


def _normalize_url(v: str) -> str:
    if not v:
        return ""
    v = v.strip()
    if v.lower().startswith(("http://", "https://")):
        return v
    return f"https://{v.lstrip('/')}"

def _split_semicolon(v: str):
    return [x.strip() for x in (v or "").split(";") if x.strip()]

def _choose_seed(row: Dict[str, str]) -> Optional[str]:
    primary = (row.get("primary_url") or "").strip()
    if primary:
        return _normalize_url(primary)
    urls = _split_semicolon(row.get("urls") or "")
    if urls:
        return _normalize_url(urls[0])
    domains = _split_semicolon(row.get("domains") or "")
    if domains:
        return _normalize_url(domains[0])
    fallback = (row.get("url") or row.get("domain") or "").strip()
    if fallback:
        return _normalize_url(fallback)
    return None

def _get_settings():
    s = get_project_settings()
    s.set("TELNETCONSOLE_ENABLED", False)
    s.set("LOG_LEVEL", "INFO")
    try:
        import scrapy_playwright  # noqa: F401
        s.set("DOWNLOAD_HANDLERS", {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        })
        print("[runner] scrapy-playwright: OK (download handlers registrati)")
    except Exception as e:
        print("[runner] scrapy-playwright non disponibile:", e)
    return s

def main(csv_path: str = CSV_DEFAULT_PATH,
         max_depth: int = 3, max_pages: int = 100, whitelist: str = "",
         limit: Optional[int] = None, render_budget: int = 40, disable_render: bool = False,
         allow_external_pdfs: bool = False, external_pdf_hosts: str = ""):

    print(f"[runner] csv_path: {os.path.abspath(csv_path)}")
    if not os.path.exists(csv_path):
        print(f"[ERRORE] CSV non trovato: {csv_path}")
        sys.exit(1)

    os.makedirs("data/interim", exist_ok=True)
    os.makedirs("data/reports", exist_ok=True)

    settings = _get_settings()

    companies: List[Dict[str, str]] = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if limit is not None and i >= limit:
                break
            company_id = (row.get("company_id") or row.get("id") or row.get("slug") or "").strip()
            if not company_id:
                continue
            seed = _choose_seed(row)
            if not seed:
                continue
            companies.append({"company_id": company_id, "primary_url": seed})

    if not companies:
        print("[runner] Nessuna riga valida nel CSV (manca company_id o seed url).")
        sys.exit(1)

    print(f"[runner] Aziende da processare: {len(companies)}")
    for r in companies[:5]:
        print(f"  - {r['company_id']}: {r['primary_url']}")
    if len(companies) > 5:
        print(f"  ... (+{len(companies)-5} altre)")

    process = CrawlerProcess(settings)
    for r in companies:
        process.crawl(
            DiscoverySpider,
            company_id=r["company_id"],
            primary_url=r["primary_url"],
            max_depth=max_depth,
            max_pages=max_pages,
            whitelist=whitelist,
            render_budget=render_budget,
            disable_render=disable_render,
            allow_external_pdfs=allow_external_pdfs,
            external_pdf_hosts=external_pdf_hosts,
        )

    process.start()

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Run ESG discovery su companies CSV")
    p.add_argument("csv_path", nargs="?", default=CSV_DEFAULT_PATH)
    p.add_argument("--max-depth", type=int, default=3)
    p.add_argument("--max-pages", type=int, default=100)
    p.add_argument("--whitelist", default="")
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--render-budget", type=int, default=40)
    p.add_argument("--disable-render", action="store_true")
    p.add_argument("--allow-external-pdfs", action="store_true")
    p.add_argument("--external-pdf-hosts", default="")
    args = p.parse_args()

    main(
        csv_path=args.csv_path,
        max_depth=args.max_depth,
        max_pages=args.max_pages,
        whitelist=args.whitelist,
        limit=args.limit,
        render_budget=args.render_budget,
        disable_render=args.disable_render,
        allow_external_pdfs=args.allow_external_pdfs,
        external_pdf_hosts=args.external_pdf_hosts,
    )
