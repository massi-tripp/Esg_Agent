from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Optional, Dict, Any

'''Questo codice estrae i report presi dal paper'''
# =========================
# Config
# =========================
IN_DIR = Path(r"C:\Universita\TESI\esg_agent\RAG_full\data")
OUT_BASE = Path(r"C:\Universita\TESI\esg_agent\RAG_full\marker_artifacts")
REPORT_YEAR = "2023"

WRITE_MD = True          # ✅ salva anche .md con full_report
WRITE_JSON_COPY = True   # ✅ salva anche copia JSON (normalizzata) nella cartella di output
ENCODING = "utf-8"


# =========================
# Helpers
# =========================
def _safe_slug(s: str, max_len: int = 80) -> str:
    """
    Slug robusto per path:
    - lower
    - sostituisce tutto ciò che non è [a-z0-9] con underscore
    - comprime underscore
    """
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    if not s:
        s = "unknown_company"
    return s[:max_len] if len(s) > max_len else s


def _read_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding=ENCODING) as f:
        return json.load(f)


def _normalize_newlines(text: str) -> str:
    # uniforma newline per output md
    text = (text or "").replace("\r\n", "\n").replace("\r", "\n")
    return text


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding=ENCODING)


def _write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding=ENCODING)


# =========================
# Main
# =========================
def export_full_reports_2023(
    in_dir: Path = IN_DIR,
    out_base: Path = OUT_BASE,
    report_year: str = REPORT_YEAR,
    write_md: bool = WRITE_MD,
    write_json_copy: bool = WRITE_JSON_COPY,
) -> None:
    if not in_dir.exists():
        raise FileNotFoundError(f"Cartella input non trovata: {in_dir}")

    json_files = sorted([p for p in in_dir.glob("*.json") if p.is_file()])
    if not json_files:
        print(f"[WARN] Nessun file .json trovato in: {in_dir}")
        return

    cnt_ok = 0
    cnt_missing_full_report = 0
    cnt_parse_error = 0

    audit_rows = []

    for p in json_files:
        try:
            data = _read_json(p)

            company_name = (
                (data.get("company_name") or "")
                if isinstance(data, dict)
                else ""
            )
            if not company_name.strip():
                # fallback: usa lo stem del file
                company_name = p.stem

            slug = _safe_slug(company_name)

            full_report = None
            if isinstance(data, dict):
                full_report = data.get("full_report")

            if not isinstance(full_report, str) or not full_report.strip():
                cnt_missing_full_report += 1
                audit_rows.append({
                    "input_file": str(p),
                    "company_name": company_name,
                    "slug": slug,
                    "year": report_year,
                    "status": "skip_missing_full_report",
                })
                continue

            # output dir: ...\marker_artifacts\<slug>\2023\
            out_dir = out_base / slug / str(report_year)
            out_dir.mkdir(parents=True, exist_ok=True)

            # salva MD con full_report
            if write_md:
                md_path = out_dir / f"{slug}_{report_year}_full_report.md"
                md_text = _normalize_newlines(full_report)
                _write_text(md_path, md_text)

            # salva copia JSON (normalizzata) con anno esplicito + path originale
            if write_json_copy:
                out_json = dict(data) if isinstance(data, dict) else {"raw": data}
                out_json["_export"] = {
                    "source_file": str(p),
                    "export_year_folder": str(report_year),
                    "company_slug": slug,
                }
                json_out_path = out_dir / f"{slug}_{report_year}_artifact.json"
                _write_json(json_out_path, out_json)

            cnt_ok += 1
            audit_rows.append({
                "input_file": str(p),
                "company_name": company_name,
                "slug": slug,
                "year": report_year,
                "status": "ok",
                "out_dir": str(out_dir),
                "md_written": bool(write_md),
                "json_written": bool(write_json_copy),
            })

        except Exception as e:
            cnt_parse_error += 1
            audit_rows.append({
                "input_file": str(p),
                "status": "parse_error",
                "error": str(e)[:1000],
            })

    # audit
    audit_path = out_base / f"export_full_report_{report_year}.audit.json"
    _write_json(audit_path, {"rows": audit_rows})

    print("========== EXPORT 2023 FULL REPORTS ==========")
    print(f"Input dir: {in_dir}")
    print(f"Output base: {out_base}")
    print(f"Anno: {report_year}")
    print("---------------------------------------------")
    print(f"OK:                         {cnt_ok:,}")
    print(f"SKIP missing full_report:   {cnt_missing_full_report:,}")
    print(f"ERROR parse_error:          {cnt_parse_error:,}")
    print(f"AUDIT: {audit_path}")
    print("=============================================")


if __name__ == "__main__":
    export_full_reports_2023()
