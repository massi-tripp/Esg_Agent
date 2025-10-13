# scripts/check_discovery_output.py
"""
Validazione output fase di Discovery.

Novità:
- Supporta i file per-run (es. pages_visited.<run_id>.jsonl)
- Opzioni CLI: --run-id, --all
- Se non specifichi nulla, sceglie automaticamente l'ultimo run disponibile
- Report HTML e CSV con nome che include il run-id
"""

import os
import re
import glob
import argparse
import pandas as pd
from urllib.parse import urlparse
from datetime import datetime

INTERIM_DIR = "data/interim"
REPORTS_DIR = os.path.join(INTERIM_DIR, "reports")

BASE_PAGES = os.path.join(INTERIM_DIR, "pages_visited.jsonl")
BASE_CANDS = os.path.join(INTERIM_DIR, "candidates_raw.jsonl")

PAGES_GLOB = os.path.join(INTERIM_DIR, "pages_visited*.jsonl")
CANDS_GLOB = os.path.join(INTERIM_DIR, "candidates_raw*.jsonl")


def list_runs() -> list[str]:
    """Ritorna la lista dei run-id disponibili trovando i suffissi .<run>.jsonl (ordinati per mtime desc)."""
    candidates = []
    for path in glob.glob(PAGES_GLOB) + glob.glob(CANDS_GLOB):
        m = re.match(r".*\.(?P<run>[^./\\]+)\.jsonl$", path)
        if m:
            run = m.group("run")
            candidates.append((run, os.path.getmtime(path)))
    # ordina per mtime desc e dedup run-id
    seen = set()
    ordered = []
    for run, _ in sorted(candidates, key=lambda x: x[1], reverse=True):
        if run not in seen:
            seen.add(run)
            ordered.append(run)
    return ordered


def pick_files(run_id: str | None, use_all: bool) -> tuple[list[str], list[str], str]:
    """
    Determina quali file leggere.
    - Se use_all=True: tutti i files * (base e per-run), aggregati.
    - Se run_id è dato: usa quelli del run specifico.
    - Altrimenti: se esistono, usa i base; se no prova l'ultimo run trovato.
    Ritorna: (pages_files, cands_files, label) dove label è 'base' o il run-id o 'all'.
    """
    pages_files: list[str] = []
    cands_files: list[str] = []
    label = "base"

    if use_all:
        pages_files = sorted(glob.glob(PAGES_GLOB))
        cands_files = sorted(glob.glob(CANDS_GLOB))
        # includi anche i base se presenti
        if os.path.exists(BASE_PAGES):
            pages_files.append(BASE_PAGES)
        if os.path.exists(BASE_CANDS):
            cands_files.append(BASE_CANDS)
        label = "all"
        return pages_files, cands_files, label

    if run_id:
        rp = os.path.join(INTERIM_DIR, f"pages_visited.{run_id}.jsonl")
        rc = os.path.join(INTERIM_DIR, f"candidates_raw.{run_id}.jsonl")
        if os.path.exists(rp):
            pages_files = [rp]
        if os.path.exists(rc):
            cands_files = [rc]
        label = run_id
        return pages_files, cands_files, label

    # Nessun run-id: prova i base
    if os.path.exists(BASE_PAGES) or os.path.exists(BASE_CANDS):
        if os.path.exists(BASE_PAGES):
            pages_files = [BASE_PAGES]
        if os.path.exists(BASE_CANDS):
            cands_files = [BASE_CANDS]
        label = "base"
        return pages_files, cands_files, label

    # Altrimenti prendi l'ultimo run disponibile
    runs = list_runs()
    if runs:
        latest = runs[0]
        rp = os.path.join(INTERIM_DIR, f"pages_visited.{latest}.jsonl")
        rc = os.path.join(INTERIM_DIR, f"candidates_raw.{latest}.jsonl")
        if os.path.exists(rp):
            pages_files = [rp]
        if os.path.exists(rc):
            cands_files = [rc]
        label = latest
        return pages_files, cands_files, label

    # Nessun file trovato
    return pages_files, cands_files, label


def load_many(paths: list[str]) -> pd.DataFrame:
    frames = []
    for p in paths:
        try:
            df = pd.read_json(p, lines=True)
            df["__source_file"] = p
            frames.append(df)
        except Exception as e:
            print(f"[!] Errore leggendo {p}: {e}")
    if frames:
        return pd.concat(frames, ignore_index=True)
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
    parser = argparse.ArgumentParser(description="Report di validazione discovery (pages_visited/candidates_raw).")
    parser.add_argument("--run-id", default=None, help="Esempio: 2025-09-25_16-10 (se assente, autodetect)")
    parser.add_argument("--all", action="store_true", help="Aggrega tutti i run trovati")
    args = parser.parse_args()

    os.makedirs(REPORTS_DIR, exist_ok=True)

    pages_files, cands_files, label = pick_files(args.run_id, args.all)

    if not pages_files and not cands_files:
        print(f"[!] Nessun file trovato in {INTERIM_DIR}. Hai già eseguito la discovery?")
        print(f"[!] Attesi: {BASE_PAGES}, {BASE_CANDS} oppure le versioni .<run>.jsonl")
        return

    print("=== INPUT ===")
    if pages_files:
        print("pages_visited files:")
        for p in pages_files:
            print(" -", p)
    else:
        print("pages_visited files: (nessuno)")

    if cands_files:
        print("candidates_raw files:")
        for p in cands_files:
            print(" -", p)
    else:
        print("candidates_raw files: (nessuno)")

    pages = load_many(pages_files)
    cands = load_many(cands_files)

    # Nomina degli output
    stamp = label if label and label != "base" else datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    html_report = os.path.join(REPORTS_DIR, f"discovery_report.{stamp}.html")
    csv_pages = os.path.join(REPORTS_DIR, f"summary_pages_per_company.{stamp}.csv")
    csv_cands = os.path.join(REPORTS_DIR, f"summary_candidates_per_company.{stamp}.csv")
    csv_top = os.path.join(REPORTS_DIR, f"candidates_top_per_company.{stamp}.csv")

    # KPI
    print("\n=== DISCOVERY QUICK REPORT ===")
    print(f"- Pagine visitate (tot): {0 if pages.empty else len(pages)}")
    print(f"- Link candidati (tot):  {0 if cands.empty else len(cands)}")

    # CSV compatti
    if not pages.empty:
        pages_per_company = pages.groupby("company_id")["url"].count().sort_values(ascending=False)
        pages_per_company.to_csv(csv_pages, header=["pages"])
        print(f"- Salvato: {csv_pages}")

    if not cands.empty:
        cands_per_company = cands.groupby("company_id")["target_url"].count().sort_values(ascending=False)
        cands_per_company.to_csv(csv_cands, header=["candidates"])
        print(f"- Salvato: {csv_cands}")

    # HTML (con top-5 candidati per azienda)
    with open(html_report, "w", encoding="utf-8") as f:
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
.kv span { display:inline-block; min-width: 240px; color:#374151; }
</style>
</head><body>
<h1>Discovery Report</h1>
""")
        f.write('<div class="kv">')
        f.write(f"<div><span>Run:</span> <b>{label}</b></div>")
        f.write(f"<div><span>Pagine visitate (tot):</span> <b>{0 if pages.empty else len(pages):,}</b></div>")
        f.write(f"<div><span>Link candidati (tot):</span> <b>{0 if cands.empty else len(cands):,}</b></div>")
        f.write("</div>")

        if not pages.empty:
            per_company_pages = pages.groupby("company_id")["url"].count().sort_values(ascending=False).reset_index(name="pages")
            f.write("<h2>Pagine visitate per azienda</h2>")
            f.write(per_company_pages.to_html(index=False, escape=True))

        if not cands.empty:
            per_company_cands = cands.groupby("company_id")["target_url"].count().sort_values(ascending=False).reset_index(name="candidates")
            f.write("<h2>Candidati per azienda</h2>")
            f.write(per_company_cands.to_html(index=False, escape=True))

            # Top-5 per azienda con qualche segnale utile
            f.write("<h2>Top candidati (per azienda)</h2>")
            rows = []
            for cid, g in cands.groupby("company_id"):
                tg = g.copy()
                # ordina: PDF prima, anno più recente, profondità minore
                # Aggiungi 'score_hint' alla logica di sorting
                tg["score_hint"] = pd.to_numeric(tg.get("score_hint"), errors="coerce").fillna(0)
                tg["has_pdf"] = tg.get("is_pdf", False)
                tg["year_hint"] = pd.to_numeric(tg.get("year_in_url"), errors="coerce")
                tg["anchor_year"] = pd.to_numeric(tg.get("year_in_anchor"), errors="coerce")

                # ordina: punteggio alto prima, PDF prima, anno più recente, profondità minore
                tg = tg.sort_values(
                    by=["score_hint", "has_pdf", "anchor_year", "year_hint", "depth"],
                    ascending=[False, False, False, False, True]
                ).head(5)

                view = pd.DataFrame({
                    "target_url": tg["target_url"].apply(make_clickable),
                    "anchor_text": tg.get("anchor_text", "").fillna(""),
                    "depth": tg.get("depth", None),
                    "type": tg.get("guess_type", ""),
                    "is_pdf": tg.get("is_pdf", False),
                    "year_path": tg.get("year_in_url", None),
                    "score_hint": tg.get("score_hint","")
                })
                f.write(f'<h3>{cid} <span class="badge">top 5</span></h3>')
                f.write(view.to_html(index=False, escape=False))
                tg["__company_id__"] = cid
                rows.append(tg)

            if rows:
                top_df = pd.concat(rows, ignore_index=True)
                cols = ["__company_id__", "target_url", "anchor_text", "depth", "guess_type", "is_pdf", "year_in_url", "year_in_anchor"]
                (top_df[cols] if all(c in top_df.columns for c in cols) else top_df).to_csv(csv_top, index=False)

        f.write("</body></html>")

    print(f"- Report HTML: {html_report}")
    print("Aprilo con il browser per una lettura comoda.\n")


if __name__ == "__main__":
    main()