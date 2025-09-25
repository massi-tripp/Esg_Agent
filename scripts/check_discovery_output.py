# scripts/check_discovery_output.py
"""
Validazione output fase di Discovery.
- Legge JSONL (pages_visited, candidates_raw)
- Stampa un mini-report in console (compatto)
- Esporta CSV riassuntivi
- Genera un report HTML leggibile con tabelle e link cliccabili
"""

import os
import pandas as pd
from urllib.parse import urlparse

PAGES_PATH = "data/interim/pages_visited.jsonl"
CANDIDATES_PATH = "data/interim/candidates_raw.jsonl"
OUT_DIR = "data/interim/reports"
HTML_REPORT = os.path.join(OUT_DIR, "discovery_report.html")
CSV_SUMMARY_PAGES = os.path.join(OUT_DIR, "summary_pages_per_company.csv")
CSV_SUMMARY_CANDS = os.path.join(OUT_DIR, "summary_candidates_per_company.csv")
CSV_CANDS_TOP = os.path.join(OUT_DIR, "candidates_top_per_company.csv")


def load_jsonl(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        print(f"[!] File non trovato: {path}")
        return pd.DataFrame()
    try:
        return pd.read_json(path, lines=True)
    except Exception as e:
        print(f"[!] Errore leggendo {path}: {e}")
        return pd.DataFrame()


def make_clickable(url: str) -> str:
    if not isinstance(url, str) or not url:
        return ""
    return f'<a href="{url}" target="_blank" rel="noreferrer noopener">{url}</a>'


def domain(url: str) -> str:
    try:
        return urlparse(url).netloc
    except Exception:
        return ""


def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    pages = load_jsonl(PAGES_PATH)
    cands = load_jsonl(CANDIDATES_PATH)

    print("\n=== DISCOVERY QUICK REPORT ===")
    if pages.empty:
        print("- Nessuna pagina visitata")
    else:
        print(f"- Pagine visitate: {len(pages)} (file: {PAGES_PATH})")
    if cands.empty:
        print("- Nessun candidato trovato")
    else:
        print(f"- Link candidati: {len(cands)} (file: {CANDIDATES_PATH})")

    # ---- Export CSV compatti ----
    if not pages.empty:
        pages_per_company = pages.groupby("company_id")["url"].count().sort_values(ascending=False)
        pages_per_company.to_csv(CSV_SUMMARY_PAGES, header=["pages_count"])
        print(f"- Salvato: {CSV_SUMMARY_PAGES}")

    if not cands.empty:
        cands_per_company = cands.groupby("company_id")["target_url"].count().sort_values(ascending=False)
        cands_per_company.to_csv(CSV_SUMMARY_CANDS, header=["candidates_count"])
        print(f"- Salvato: {CSV_SUMMARY_CANDS}")

        # top 5 per company (per HTML/CSV)
        top_rows = []
        for cid, g in cands.groupby("company_id"):
            tg = g.copy()
            # segnali utili nell’ordinamento: profondità, anno nel path/anchor
            tg["has_pdf"] = tg["target_url"].str.contains(r"\.pdf(\?|$)", case=False, regex=True)
            tg["year_hint"] = tg["target_url"].str.extract(r"(19\d{2}|20[0-4]\d)", expand=False)
            tg["year_hint"] = pd.to_numeric(tg["year_hint"], errors="coerce")
            tg["anchor_year"] = tg["anchor_text"].str.extract(r"(19\d{2}|20[0-4]\d)", expand=False)
            tg["anchor_year"] = pd.to_numeric(tg["anchor_year"], errors="coerce")
            tg["dom"] = tg["target_url"].apply(domain)
            # ordina: PDF prima, anno più recente, profondità minore
            tg = tg.sort_values(
                by=["has_pdf", "anchor_year", "year_hint", "depth"],
                ascending=[False, False, False, True]
            )
            top_rows.append(tg.head(5))
        top_df = pd.concat(top_rows, ignore_index=True) if top_rows else pd.DataFrame()
        if not top_df.empty:
            # salva CSV “flat”
            cols = ["company_id", "source_url", "target_url", "anchor_text", "depth", "lang_hint"]
            (top_df[cols] if all(c in top_df.columns for c in cols) else top_df).to_csv(CSV_CANDS_TOP, index=False)
            print(f"- Salvato: {CSV_CANDS_TOP}")

    # ---- HTML report leggibile ----
    with open(HTML_REPORT, "w", encoding="utf-8") as f:
        f.write("""<!doctype html><html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Discovery Report</title>
<style>
body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 24px; }
h1, h2 { margin: 0.3rem 0; }
table { border-collapse: collapse; width: 100%; margin: 12px 0 24px; }
th, td { border: 1px solid #e5e7eb; padding: 8px 10px; text-align: left; vertical-align: top; }
th { background: #f3f4f6; }
.badge { display:inline-block; padding:2px 8px; border-radius:12px; background:#eef2ff; color:#3730a3; font-size:12px; }
.small { color:#6b7280; font-size:12px; }
.kv { margin: 6px 0 12px; }
.kv span { display:inline-block; min-width: 180px; color:#374151; }
</style>
</head><body>
<h1>Discovery Report</h1>
""")

        # KPI
        f.write('<div class="kv">')
        if not pages.empty:
            f.write(f"<div><span>Pagine visitate (tot):</span> <b>{len(pages):,}</b></div>")
        else:
            f.write(f"<div><span>Pagine visitate (tot):</span> <b>0</b></div>")
        if not cands.empty:
            f.write(f"<div><span>Link candidati (tot):</span> <b>{len(cands):,}</b></div>")
        else:
            f.write(f"<div><span>Link candidati (tot):</span> <b>0</b></div>")
        f.write("</div>")

        # Tabelle per-azienda
        if not pages.empty:
            per_company_pages = pages.groupby("company_id")["url"].count().sort_values(ascending=False).reset_index(name="pages")
            f.write("<h2>Pagine visitate per azienda</h2>")
            f.write(per_company_pages.to_html(index=False, escape=True))
        if not cands.empty:
            per_company_cands = cands.groupby("company_id")["target_url"].count().sort_values(ascending=False).reset_index(name="candidates")
            f.write("<h2>Candidati per azienda</h2>")
            f.write(per_company_cands.to_html(index=False, escape=True))

            # Dettaglio top-5 per azienda con link cliccabili
            f.write("<h2>Top candidati (per azienda)</h2>")
            for cid, g in cands.groupby("company_id"):
                tg = g.copy()
                tg["has_pdf"] = tg["target_url"].str.contains(r"\\.pdf(\\?|$)", case=False, regex=True)
                tg["year_hint"] = pd.to_numeric(tg["target_url"].str.extract(r"(19\\d{2}|20[0-4]\\d)", expand=False), errors="coerce")
                tg["anchor_year"] = pd.to_numeric(tg["anchor_text"].str.extract(r"(19\\d{2}|20[0-4]\\d)", expand=False), errors="coerce")
                tg["dom"] = tg["target_url"].apply(domain)
                tg = tg.sort_values(by=["has_pdf","anchor_year","year_hint","depth"], ascending=[False, False, False, True]).head(5)

                # colonne “pulite”
                view = pd.DataFrame({
                    "target_url": tg["target_url"].apply(make_clickable),
                    "anchor_text": tg["anchor_text"].fillna(""),
                    "depth": tg["depth"],
                    "domain": tg["dom"],
                    "year_in_path": tg["year_hint"],
                })
                f.write(f'<h3>{cid} <span class="badge">top 5</span></h3>')
                f.write(view.to_html(index=False, escape=False))
        f.write("</body></html>")

    print(f"- Report HTML: {HTML_REPORT}")
    print("Aprilo con il browser per una lettura comoda (doppio click dal file explorer).")


if __name__ == "__main__":
    main()
