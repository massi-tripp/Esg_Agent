# scraper/run_discovery.py
# aggiungo Runner con argparse, filtro esclusioni (per company_id già trovate)
# e supporto a --allow-external-pdfs

import os
import sys
import csv
import argparse
import pandas as pd
from pathlib import Path
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from .discover import DiscoverySpider


# ============================================================
# normalizzo le stringhe
# ============================================================
def _norm(s: str) -> str:
    if s is None:
        return ""
    return str(s).strip().upper()


# ============================================================
# carico l'elenco di company da escludere
# ============================================================
def load_exclude(path: str, key_hint: str | None = None) -> tuple[set[str], set[str]]:
    if not path or not Path(path).exists():
        print("[exclude] Nessun file di esclusione fornito o non trovato.")
        return set(), set()

    try:
        df = pd.read_csv(path, dtype=str, keep_default_na=False, engine="python")
    except Exception as e:
        print(f"[exclude] Errore nel leggere {path}: {e}")
        return set(), set()

    cols = {c.lower(): c for c in df.columns}
    excluded_ids, excluded_names = set(), set()

    id_col = cols.get("company_id")
    name_col = cols.get("company") or cols.get("company_name")

    if key_hint and not name_col and key_hint in df.columns:
        name_col = key_hint

    if id_col:
        excluded_ids = set(df[id_col].map(_norm))
    if name_col:
        excluded_names = set(df[name_col].map(_norm))

    print(f"[exclude] Sorgente: {path}")
    print(f"[exclude]  - colonne trovate: {list(df.columns)}")
    print(f"[exclude]  - company_id esclusi: {len(excluded_ids)}")
    print(f"[exclude]  - company    esclusi: {len(excluded_names)}")
    return excluded_ids, excluded_names


# ============================================================
# Imposto settings Scrapy ( permette di simulare azioni utente (click, scroll, login, ecc.) in modo dinamico
# ============================================================
def get_settings():
    s = get_project_settings()
    try:
        import scrapy_playwright  # noqa: F401
        s.set("DOWNLOAD_HANDLERS", {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        })
    except Exception:
        pass
    return s


# ============================================================
# MAIN
# ============================================================
def main(csv_path,
         max_depth=3, max_pages=100, whitelist="",
         limit=None, render_budget=40, disable_render=False,
         allow_external_pdfs=False,
         exclude_file=None, exclude_key="company"):

    if not os.path.exists(csv_path):
        print(f"CSV non trovato: {csv_path}")
        sys.exit(1)

    #Carico elenco di esclusioni (auto-rileva company_id/company)
    excluded_ids, excluded_names = load_exclude(exclude_file, key_hint=exclude_key)

    settings = get_settings()
    process = CrawlerProcess(settings)

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)  
        rows = list(reader)

    total = len(rows)
    print(f"[runner] Letti {total} record totali da {csv_path}")

    # Filtro aziende da escludere per ID o Nome
    todo_rows = []
    n_skipped = 0
    for row in rows:
        company_id = _norm(row.get("company_id") or row.get("company") or row.get("id") or row.get("slug") or row.get("company_name"))
        company_nm = _norm(row.get("company") or row.get("company_name"))
        primary_url = row.get("primary_url") or row.get("url") or row.get("homepage_url")

        if (company_id and company_id in excluded_ids) or (company_nm and company_nm in excluded_names):
            n_skipped += 1
            continue
        if not company_id or not primary_url:
            continue
        row["__company_id_norm"] = company_id
        row["__company_nm_norm"] = company_nm
        todo_rows.append(row)

    print(f"[runner] Esclusi: {n_skipped} | Da processare: {len(todo_rows)}")

    # Parto con l'esecuzione di discover
    for i, row in enumerate(todo_rows):
        if limit and i >= limit:
            break
        process.crawl(
            DiscoverySpider,
            company_id=row["__company_id_norm"],
            primary_url=(row.get("primary_url") or row.get("url") or row.get("homepage_url")).strip(),
            max_depth=max_depth,
            max_pages=max_pages,
            whitelist=whitelist,
            render_budget=render_budget,
            disable_render=disable_render,
            allow_external_pdfs=allow_external_pdfs,
        )

    process.start()
    print("Discovery completato. Ora esegui scraper/rank.py per classificare i risultati.")


# ============================================================
# CLI
# ============================================================
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
                        help="Consenti PDF da host esterni (CDN, annualreport.* ecc.)")

    # file di esclusione: al momento si chiama candidates_with_validation_FINAL.csv
    parser.add_argument("--exclude-file", type=str, default=None,
                        help="CSV con le aziende da escludere (es. data/output/candidates_with_validation_FINAL.csv). "
                             "Supporta colonne company_id e/o company.")
    parser.add_argument("--exclude-key", type=str, default="company",
                        help="Nome colonna fallback per il file di esclusione se non ha company_id (default: company)")

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
        exclude_file=args.exclude_file,
        exclude_key=args.exclude_key,
    )
    print("Done.")
# ============================================================
