# -*- coding: utf-8 -*-
"""
SIMPLE DATASET FIXER for your ESG companies file.

Input (Excel .xlsx) columns:
  - "Ragione socialeCaratteri latini"
  - "Numero ISIN"
  - "Domain"
  - "Indirizzo sito web"   (può avere più link nella stessa cella, separati da newline)

Output:
  - companies_clean.csv   (una riga per azienda con lista link deduplicata)
  - companies_urls.csv    (una riga per coppia (azienda, url) - utile se vuoi esplodere)

Uso:
  - Metodo più semplice: metti il tuo file nella stessa cartella e imposta qui sotto INPUT_XLSX_DEFAULT col NOME FILE (stringa).
  - Oppure: da terminale ->  python prep_from_xlsx.py path/to/file.xlsx

Dipendenze:  pip install pandas openpyxl
"""

import sys, re, csv
from urllib.parse import urlparse
from typing import List, Set, Union, Dict
import pandas as pd

# ====== CONFIG ===============================================================
# Se non passi il path da terminale, uso questo:
INPUT_XLSX_DEFAULT = "Stoxx_europe_600_siti_web.xlsx"   # <-- METTI QUI il nome del tuo file, es. "aziende.xlsx"

# Se conosci il nome esatto del foglio, mettilo qui.
# Altrimenti lascia None: useremo il PRIMO foglio in modo sicuro.
SHEET_NAME: Union[str, int, None] = None

# Nomi colonna come da tuo screenshot (cambiali qui se diversi)
COL_COMPANY = "Ragione socialeCaratteri latini"
COL_ISIN    = "Numero ISIN"
COL_DOMAIN  = "Domain"
COL_SITES   = "Indirizzo sito web"

OUT_MAIN_CSV    = "companies_clean.csv"
OUT_EXPLODE_CSV = "companies_urls.csv"
# ============================================================================

SEP = re.compile(r"[,\s;]+")  # separa per virgole, spazi, newline, punto e virgola
URL_LIKE = re.compile(r"^(https?://)?([a-z0-9.-]+\.[a-z]{2,})(/.*)?$", re.I)

def normalize_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    u = u.strip("<>\"'")
    if not u.startswith(("http://", "https://")):
        u = "https://" + u
    # normalizza eventuali doppie //
    u = re.sub(r"(?<!:)//+", "/", u)
    return u

def domain_of(u: str) -> str:
    try:
        return urlparse(u).netloc.lower()
    except Exception:
        return ""

def split_urls(cell_text: str) -> List[str]:
    parts = [p for p in SEP.split(cell_text or "") if p.strip()]
    out = []
    for p in parts:
        if URL_LIKE.match(p):
            out.append(normalize_url(p))
    return out

def dedupe_preserve(seq: List[str]) -> List[str]:
    seen: Set[str] = set()
    out = []
    for x in seq:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out

def read_first_sheet(xlsx_path: str, sheet_name: Union[str, int, None]):
    """
    Legge il PRIMO foglio in modo sicuro:
    - se sheet_name è stringa o int, usa quello;
    - se è None, legge il primo foglio;
    - se read_excel restituisce un dict (capita se sheet_name=None), prende il PRIMO DataFrame.
    """
    # se sheet_name è None, chiedi a pandas il PRIMO foglio (0)
    sn = 0 if sheet_name is None else sheet_name
    df_or_dict: Union[pd.DataFrame, Dict[str, pd.DataFrame]] = pd.read_excel(
        xlsx_path, sheet_name=sn, engine="openpyxl"
    )
    if isinstance(df_or_dict, dict):
        # prendi il primo DataFrame disponibile
        if not df_or_dict:
            raise ValueError("Excel non contiene fogli leggibili.")
        first_key = next(iter(df_or_dict))
        return df_or_dict[first_key]
    return df_or_dict

def main(input_xlsx: str):
    # ---- DEBUG opzionale: mostra che tipo stiamo passando
    # print("DEBUG input_xlsx =", input_xlsx, type(input_xlsx))

    df = read_first_sheet(input_xlsx, SHEET_NAME)

    # Check colonne minime
    for col in (COL_COMPANY, COL_ISIN, COL_DOMAIN, COL_SITES):
        if col not in df.columns:
            raise ValueError(f"Missing column in Excel: {col}")

    rows_clean = []
    rows_exploded = []

    for _, r in df.iterrows():
        company = str(r.get(COL_COMPANY) or "").strip()
        isin    = str(r.get(COL_ISIN) or "").strip()
        dom_txt = str(r.get(COL_DOMAIN) or "").strip()
        sites   = str(r.get(COL_SITES) or "").strip()

        # prendo URL sia da "Indirizzo sito web" che da "Domain"
        urls = split_urls(sites) + split_urls(dom_txt)

        # se "Domain" contiene solo dominio nudo (es. shell.co.uk) e non abbiamo ancora URL:
        if dom_txt and not urls and URL_LIKE.match(dom_txt.strip()):
            urls.append(normalize_url(dom_txt.strip()))

        urls = dedupe_preserve(urls)
        domains = dedupe_preserve([d for d in (domain_of(u) for u in urls) if d])

        if not company and not urls:
            continue

        rows_clean.append({
            "company_id": company,
            "isin": isin,
            "domains": ";".join(domains),
            "urls": ";".join(urls),
            "primary_url": urls[0] if urls else ""
        })

        for u in urls:
            rows_exploded.append({
                "company_id": company,
                "isin": isin,
                "domain": domain_of(u),
                "url": u
            })

    # Scrivo il CSV “pulito”
    with open(OUT_MAIN_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["company_id","isin","domains","urls","primary_url"])
        w.writeheader()
        w.writerows(rows_clean)

    # (Opzionale) CSV “esploso”
    with open(OUT_EXPLODE_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["company_id","isin","domain","url"])
        w.writeheader()
        w.writerows(rows_exploded)

    print(f"✅ Wrote {len(rows_clean)} companies to {OUT_MAIN_CSV}")
    print(f"✅ Wrote {len(rows_exploded)} rows to {OUT_EXPLODE_CSV}")

if __name__ == "__main__":
    xlsx = sys.argv[1] if len(sys.argv) > 1 else INPUT_XLSX_DEFAULT
    if not isinstance(xlsx, str):
        raise TypeError(f"Expected a file path string, got {type(xlsx)}")
    main(xlsx)
# INPUT_XLSX_DEFAULT = "Stoxx_europe_600_siti_web.xlsx"