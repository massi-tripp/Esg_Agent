from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Tuple, Optional
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests

''' Questo codice serve per scaricare i pdf tramite i link scovati dal tool di ricerca'''
# =========================
# Config
# =========================
CACHE_DIR = Path(r"C:\Universita\TESI\esg_agent\RAG_full\cache")
OUT_DIR   = Path(r"C:\Universita\TESI\esg_agent\RAG_full\output")
CACHE_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

USER_AGENT = "Mozilla/5.0 (compatible; ESG-RAG/1.0; +https://example.local)"
TIMEOUT_S  = 60

# ✅ PATCH: sessione globale per cookie/redirect persistenti
SESSION = requests.Session()


# =========================
# Helpers
# =========================
def _safe_slug(s: str, max_len: int = 80) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s[:max_len] if len(s) > max_len else s


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _looks_like_pdf_bytes(b: bytes) -> bool:
    return b[:5] == b"%PDF-"


def _looks_like_pdf_file(path: Path) -> bool:
    try:
        if not path.exists() or path.stat().st_size < 8:
            return False
        with path.open("rb") as f:
            head = f.read(5)
        return head == b"%PDF-"
    except Exception:
        return False


def _ensure_local_report_pdf(report_pdf_path: Path) -> bool:
    """
    Usa prima i PDF scaricati manualmente in cache.

    - Se esiste report.pdf ed è valido (%PDF-) -> True
    - Altrimenti cerca un qualsiasi *.pdf nella stessa cartella:
        - prende il più grande (euristica)
        - lo copia dentro report.pdf
      e ritorna True se ora report.pdf è valido.
    - Se non trova nulla -> False
    """
    try:
        # 1) report.pdf già presente e valido
        if report_pdf_path.exists() and _looks_like_pdf_file(report_pdf_path):
            return True

        folder = report_pdf_path.parent
        if not folder.exists():
            return False

        # 2) cerca altri PDF nella cartella
        candidates = [p for p in folder.glob("*.pdf") if p.is_file()]
        if not candidates:
            return False

        # scegli il più grande
        candidates.sort(key=lambda p: p.stat().st_size, reverse=True)
        best = candidates[0]

        # se il più grande è già report.pdf ma non è valido, non possiamo fare molto
        if best.resolve() == report_pdf_path.resolve():
            return _looks_like_pdf_file(report_pdf_path)

        # copia bytes -> report.pdf (non elimino l'originale)
        report_pdf_path.parent.mkdir(parents=True, exist_ok=True)
        report_pdf_path.write_bytes(best.read_bytes())

        return _looks_like_pdf_file(report_pdf_path)
    except Exception:
        return False


def _extract_pdf_link_from_html(html: str, base_url: str) -> Optional[str]:
    """
    Heuristica semplice:
    - trova la prima occorrenza di href="...pdf" o ...pdf?... e ritorna URL assoluto.
    """
    m = re.search(r"""href\s*=\s*["']([^"']+\.pdf(?:\?[^"']*)?)["']""", html, flags=re.IGNORECASE)
    if not m:
        m = re.search(r"""(https?://[^\s"']+\.pdf(?:\?[^\s"']*)?)""", html, flags=re.IGNORECASE)
        if not m:
            return None
        return m.group(1)

    href = m.group(1)
    return urljoin(base_url, href)


# ✅ PATCH: usa SESSION + referer origin + header un po' più realistici
def _http_get(url: str, extra_headers: Optional[dict] = None) -> requests.Response:
    u = (url or "").strip()
    parsed = urlparse(u)
    origin = f"{parsed.scheme}://{parsed.netloc}/" if parsed.scheme and parsed.netloc else u

    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,it;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Referer": origin,
        "DNT": "1",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Dest": "document",
    }
    if extra_headers:
        headers.update(extra_headers)

    return SESSION.get(
        u,
        headers=headers,
        stream=True,
        timeout=TIMEOUT_S,
        allow_redirects=True,
    )


def _download_pdf(url: str, out_path: Path, force_redownload: bool = False) -> Tuple[Path, str]:
    """
    Scarica un PDF con fallback HTML->pdf link.
    Ritorna: (path, final_url_usata)

    Comportamento:
    - normalizza URL (strip)
    - se file esiste e sembra PDF -> usa cache
    - tenta download
    - se non è PDF ma HTML, prova ad estrarre un link PDF e ritenta
    """
    url = (url or "").strip()
    if not url:
        raise ValueError("URL vuota")

    if out_path.exists() and out_path.stat().st_size > 0 and not force_redownload:
        if _looks_like_pdf_file(out_path):
            return out_path, url
        out_path.unlink(missing_ok=True)

    out_path.parent.mkdir(parents=True, exist_ok=True)

    # warm-up origin per ottenere cookie/sessione prima di scaricare PDF diretto
    try:
        if url.lower().endswith(".pdf") and "://" in url:
            parsed = urlparse(url)
            origin = f"{parsed.scheme}://{parsed.netloc}/"
            SESSION.get(
                origin,
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9,it;q=0.8",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Connection": "keep-alive",
                    "Referer": origin,
                    "Upgrade-Insecure-Requests": "1",
                },
                timeout=min(15, TIMEOUT_S),
                allow_redirects=True,
            )
    except Exception:
        pass

    # 1) primo tentativo
    with _http_get(url) as r:
        r.raise_for_status()

        first = b""
        for chunk in r.iter_content(chunk_size=4096):
            if chunk:
                first = chunk
                break

        if _looks_like_pdf_bytes(first):
            tmp = out_path.with_suffix(".tmp")
            with tmp.open("wb") as f:
                f.write(first)
                for chunk in r.iter_content(chunk_size=1024 * 256):
                    if chunk:
                        f.write(chunk)
            tmp.replace(out_path)

            if not _looks_like_pdf_file(out_path):
                raise ValueError(f"File salvato ma header PDF non valido: {out_path}")
            return out_path, str(r.url)

        # NON PDF -> HTML
        try:
            rest = b"".join(list(r.iter_content(chunk_size=1024 * 256)))
            raw = first + rest
            html = raw.decode("utf-8", errors="replace")
        except Exception:
            html = ""

    # 2) estrai link pdf dal markup
    pdf_url = _extract_pdf_link_from_html(html, base_url=url)
    if not pdf_url:
        preview = html[:500] if html else "<no html preview>"
        raise ValueError(
            "Download non valido: la risposta NON è un PDF e non ho trovato link .pdf nel HTML.\n"
            f"URL: {url}\n"
            f"FILE: {out_path}\n\n"
            f"Preview:\n{preview}"
        )

    # warm-up anche sul dominio del fallback (se diverso)
    try:
        parsed = urlparse(pdf_url)
        origin2 = f"{parsed.scheme}://{parsed.netloc}/"
        SESSION.get(
            origin2,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9,it;q=0.8",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Referer": origin2,
                "Upgrade-Insecure-Requests": "1",
            },
            timeout=min(15, TIMEOUT_S),
            allow_redirects=True,
        )
    except Exception:
        pass

    # 3) secondo tentativo sul pdf_url estratto
    with _http_get(pdf_url) as r2:
        r2.raise_for_status()

        first2 = b""
        for chunk in r2.iter_content(chunk_size=4096):
            if chunk:
                first2 = chunk
                break

        if not _looks_like_pdf_bytes(first2):
            raise ValueError(
                "Fallback trovato, ma la risorsa non sembra comunque un PDF.\n"
                f"Original URL: {url}\n"
                f"Fallback URL: {pdf_url}\n"
                f"First bytes: {first2[:50]!r}"
            )

        tmp = out_path.with_suffix(".tmp")
        with tmp.open("wb") as f:
            f.write(first2)
            for chunk in r2.iter_content(chunk_size=1024 * 256):
                if chunk:
                    f.write(chunk)
        tmp.replace(out_path)

    if not _looks_like_pdf_file(out_path):
        raise ValueError(f"Fallback download completato ma file non valido: {out_path}")

    return out_path, pdf_url


def _pick_company_master(df: pd.DataFrame) -> pd.DataFrame:
    def first_non_null(s: pd.Series):
        s2 = s.dropna()
        return s2.iloc[0] if len(s2) else None

    def mode_or_min(s: pd.Series):
        s2 = pd.to_numeric(s, errors="coerce").dropna()
        if len(s2) == 0:
            return None
        m = s2.mode()
        if len(m) > 0:
            return int(m.iloc[0])
        return int(s2.min())

    g = df.groupby("company_name_full", dropna=False, as_index=False).agg(
        company_name=("company_name", first_non_null),
        link_report=("link_report", first_non_null),
        report_year=("report_year", first_non_null),
        pages_by_KPI=("pages_by_KPI", mode_or_min),  # mantenuta per compatibilità benchmark
        country=("country", first_non_null),
        sector=("sector", first_non_null),
        industry=("industry", first_non_null),
    )
    return g


def run_pipeline_download_only(
    benchmark_path: Path,
    out_audit_csv: Path,
) -> None:
    """
    Pipeline MODIFICATA:
    - Legge benchmark
    - Groupby su company_name_full (master)
    - Per ogni azienda:
        - usa PDF locale se presente, altrimenti scarica PDF completo
        - scrive meta JSON (sha256 + link finale)
    - NON estrae testo, NON salva JSONL delle pagine
    - Scrive audit CSV finale
    """
    if benchmark_path.suffix.lower() in [".xlsx", ".xls"]:
        df = pd.read_excel(benchmark_path)
    else:
        df = pd.read_csv(benchmark_path)

    required = [
        "company_name", "company_name_full", "link_report", "report_year",
        "pages_by_KPI", "country", "sector", "industry"
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Colonne mancanti nel benchmark: {missing}")

    # =========================
    # Conteggi iniziali (solo print) - PRIMA del groupby
    # =========================
    df_links = df["link_report"].astype(str).str.strip()
    df_links = df_links.where(df["link_report"].notna(), other="").astype(str).str.strip()
    unique_links_raw = set([u for u in df_links.tolist() if u])

    print("========== START STATS ==========")
    print(f"Righe totali benchmark: {len(df):,}")
    print(f"Link univoci (sulle righe del file): {len(unique_links_raw):,}")
    print("=================================")

    master = _pick_company_master(df)

    # =========================
    # Conteggi dopo grouping (solo print)
    # =========================
    master_links = master["link_report"].astype(str).str.strip()
    master_links = master_links.where(master["link_report"].notna(), other="").astype(str).str.strip()
    unique_links_master = set([u for u in master_links.tolist() if u])

    print("====== AFTER GROUPBY STATS ======")
    print(f"Aziende uniche (company_name_full): {len(master):,}")
    print(f"Link univoci effettivamente usati (post-groupby): {len(unique_links_master):,}")
    print("=================================")

    # =========================
    # Counters finali (solo print)
    # =========================
    cnt_ok = 0
    cnt_skip_missing_link = 0
    cnt_skip_download_error = 0

    audit_rows = []
    out_audit_csv.parent.mkdir(parents=True, exist_ok=True)

    for _, row in master.iterrows():
        company_full = str(row["company_name_full"])
        url = row["link_report"]
        year = row["report_year"]

        if not isinstance(url, str) or not url.strip():
            print(f"[SKIP] {company_full}: link_report mancante")
            cnt_skip_missing_link += 1
            audit_rows.append({
                "company_name_full": company_full,
                "reason": "missing_link_report",
                "url": None,
            })
            continue

        slug = _safe_slug(company_full)
        pdf_path = CACHE_DIR / slug / str(year) / "report.pdf"
        meta_path = pdf_path.with_suffix(".meta.json")

        # 1) usa prima PDF locale (scaricato manualmente), altrimenti scarica
        try:
            if _ensure_local_report_pdf(pdf_path):
                final_url = (url or "").strip()
            else:
                pdf_path, final_url = _download_pdf(url, pdf_path, force_redownload=False)
        except Exception as e:
            print(f"[SKIP] {company_full}: download error -> {e}")
            cnt_skip_download_error += 1
            audit_rows.append({
                "company_name_full": company_full,
                "reason": "download_error",
                "url": (url or "").strip(),
                "error": str(e)[:1000],
            })
            continue

        # 2) metadata
        try:
            meta = {
                "company_name_full": company_full,
                "report_year": year,
                "link_report_original": (url or "").strip(),
                "link_report_final": final_url,
                "pdf_path": str(pdf_path),
                "sha256": _sha256_file(pdf_path),
                "size_bytes": int(pdf_path.stat().st_size) if pdf_path.exists() else None,
            }
            meta_path.parent.mkdir(parents=True, exist_ok=True)
            meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as e:
            print(f"[WARN] {company_full}: meta write error -> {e}")

        cnt_ok += 1
        audit_rows.append({
            "company_name_full": company_full,
            "reason": "ok",
            "url": final_url,
            "pdf_path": str(pdf_path),
            "size_bytes": int(pdf_path.stat().st_size) if pdf_path.exists() else None,
        })

    # 3) Salva audit CSV
    try:
        pd.DataFrame(audit_rows).to_csv(out_audit_csv, index=False, encoding="utf-8")
        print(f"[AUDIT] scritto: {out_audit_csv}")
    except Exception as e:
        print(f"[WARN] impossibile scrivere audit CSV: {e}")

    # =========================
    # Conteggi finali (solo print)
    # =========================
    print("\n========== END STATS ==========")
    print(f"OK (download/cache riuscito):      {cnt_ok:,}")
    print(f"SKIP missing link_report:          {cnt_skip_missing_link:,}")
    print(f"SKIP download_error:               {cnt_skip_download_error:,}")
    print("================================\n")


if __name__ == "__main__":
    BENCHMARK = Path(r"C:\Universita\TESI\esg_agent\RAG_full\documentazione_rag.xlsx")

    # ✅ Ora produciamo solo un audit CSV (nessun JSONL pagine)
    AUDIT_CSV = OUT_DIR / "download_only.audit.csv"

    run_pipeline_download_only(
        benchmark_path=BENCHMARK,
        out_audit_csv=AUDIT_CSV,
    )

    print(f"OK -> audit scritto: {AUDIT_CSV}")
