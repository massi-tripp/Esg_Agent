# File: marker_stage.py

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pandas as pd

try:
    import fitz  # PyMuPDF
except ImportError as e:
    raise ImportError("Installa PyMuPDF: pip install pymupdf") from e


# =========================
# CONFIG
# =========================
BENCHMARK_XLSX = Path(r"C:\Universita\TESI\esg_agent\RAG_full\documentazione_rag.xlsx")

CACHE_DIR = Path(r"C:\Universita\TESI\esg_agent\RAG_full\cache")
MARKER_OUT_DIR = Path(r"C:\Universita\TESI\esg_agent\RAG_full\marker_artifacts")
MARKER_OUT_DIR.mkdir(parents=True, exist_ok=True)

# ✅ Regola richiesta:
# - se report_year contiene "2024" (es. "2024", "2023/2024") => salva in cartella 2024
# - se report_year è solo "2023" => NON fare nulla
OUTPUT_YEAR = "2024"

# Text-only settings
# (non influiscono su "se usare marker": marker è stato rimosso)
CLEAN_WHITESPACE = True


# =========================
# HELPERS
# =========================
def safe_slug(s: str, max_len: int = 80) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s[:max_len] if len(s) > max_len else s


def _clean_text_block(t: str) -> str:
    t = t or ""
    # normalizza whitespace ma preserva abbastanza struttura
    t = t.replace("\r\n", "\n").replace("\r", "\n")
    t = re.sub(r"[ \t]+\n", "\n", t)
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()


def _should_process_to_2024(report_year_raw: Any) -> bool:
    """
    Processa SOLO se contiene 2024 (anche '2023/2024').
    Se è solo 2023 => skip.
    """
    y = "" if report_year_raw is None else str(report_year_raw).strip()
    return "2024" in y


def _cache_pdf_path_from_year_string(slug: str, year_str: str) -> Path:
    """
    Se year_str è '2023/2024', su Windows la slash crea sottocartelle:
      cache/<slug>/2023/2024/report.pdf
    Gestiamo sia "/" che "\".
    """
    y = (year_str or "").strip()
    parts = [p.strip() for p in re.split(r"[\\/]+", y) if p.strip()]
    if not parts:
        parts = [y] if y else []
    return CACHE_DIR / slug / Path(*parts) / "report.pdf"


def _load_master_from_benchmark(path: Path) -> pd.DataFrame:
    """
    Master per azienda: (company_name_full, report_year)
    """
    df = pd.read_excel(path)

    required = ["company_name_full", "report_year"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Colonne mancanti nel benchmark: {missing}")

    def first_non_null(s: pd.Series):
        s2 = s.dropna()
        return s2.iloc[0] if len(s2) else None

    master = (
        df.groupby("company_name_full", dropna=False, as_index=False)
          .agg(report_year=("report_year", first_non_null))
    )
    return master


def _extract_text_full_pdf(pdf_path: Path) -> str:
    """
    Estrae testo nativo (NO OCR) da tutte le pagine del PDF.
    Se una pagina non ha testo, mette <NO_TEXT>.
    """
    doc = fitz.open(str(pdf_path))
    parts: list[str] = []
    for i in range(doc.page_count):
        page = doc.load_page(i)
        t = page.get_text("text") or ""
        if CLEAN_WHITESPACE:
            t = _clean_text_block(t)
        if t:
            parts.append(f"## Page {i+1}\n\n{t}\n")
        else:
            parts.append(f"## Page {i+1}\n\n<NO_TEXT>\n")
    doc.close()
    return "\n".join(parts)


# =========================
# MAIN
# =========================
def main() -> None:
    master = _load_master_from_benchmark(BENCHMARK_XLSX)

    cnt_ok = 0
    cnt_skip_only_2023 = 0
    cnt_skip_missing_pdf = 0
    cnt_skip_existing = 0
    cnt_err = 0

    for _, row in master.iterrows():
        company = str(row["company_name_full"])
        report_year_raw = row.get("report_year")

        # ✅ Regola: se non contiene 2024, non fare nulla
        if not _should_process_to_2024(report_year_raw):
            cnt_skip_only_2023 += 1
            continue

        slug = safe_slug(company)
        year_out = OUTPUT_YEAR

        # cerca PDF in cache in base al valore year del benchmark
        year_in_cache = "" if report_year_raw is None else str(report_year_raw).strip()
        pdf_path = _cache_pdf_path_from_year_string(slug, year_in_cache)

        # fallback: a volte il PDF è comunque in cache/<slug>/2024/report.pdf
        if not pdf_path.exists():
            pdf_alt = CACHE_DIR / slug / "2024" / "report.pdf"
            if pdf_alt.exists():
                pdf_path = pdf_alt
            else:
                cnt_skip_missing_pdf += 1
                print(f"[SKIP] {company}: PDF non trovato in cache per year='{year_in_cache}' -> {pdf_path}")
                continue

        base_out = MARKER_OUT_DIR / slug / year_out
        base_out.mkdir(parents=True, exist_ok=True)

        final_md = base_out / f"{slug}_{year_out}_report.md"
        meta_path = base_out / "marker_meta.json"

        # ✅ NON SOVRASCRIVERE: se esiste già, skip
        if final_md.exists() and final_md.stat().st_size > 0:
            cnt_skip_existing += 1
            continue

        try:
            md_text = _extract_text_full_pdf(pdf_path)
            final_md.write_text(md_text, encoding="utf-8")

            meta = {
                "company_name_full": company,
                "slug": slug,
                "report_year_source": year_in_cache,
                "report_year_output_folder": year_out,
                "pdf_path": str(pdf_path),
                "extraction_method": "text_only_full",
                "marker_md_final": str(final_md),
            }
            meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

            print(f"[OK] {company}: TEXT-ONLY full -> {final_md}")
            cnt_ok += 1

        except Exception as e:
            cnt_err += 1
            print(f"[ERR] {company} -> {e}")

    print("\n========== SUMMARY ==========")
    print(f"OK:                         {cnt_ok:,}")
    print(f"SKIP (solo 2023 / no 2024): {cnt_skip_only_2023:,}")
    print(f"SKIP (pdf mancante):        {cnt_skip_missing_pdf:,}")
    print(f"SKIP (già estratto 2024):   {cnt_skip_existing:,}")
    print(f"ERROR:                      {cnt_err:,}")
    print("=============================\n")


if __name__ == "__main__":
    main()
