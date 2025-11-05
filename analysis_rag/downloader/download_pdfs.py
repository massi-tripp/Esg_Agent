# scripts/download_pdfs.py
# Downloader per PDF:
# - legge il CSV con le url
# - scarica il file
# - verifica bytes %PDF
# - salva in C:\Universita\TESI\esg_agent\analysis_rag\data\pdfs\<company_id>\
# - logga risultati in C:\Universita\TESI\esg_agent\analysis_rag\data\output\pdf_download_log.csv e .jsonl

import os
import re
import sys
import json
import time
import hashlib
import argparse
from pathlib import Path
from typing import Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pandas as pd

DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

DEFAULT_OUT_DIR = r"C:\Universita\TESI\esg_agent\analysis_rag\data\pdfs"
OUTPUT_DIR      = r"C:\Universita\TESI\esg_agent\analysis_rag\data\output"

# ---------- utility ----------
def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", "ignore")).hexdigest()

def norm_company_id(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r'[\\/:*?"<>|]+', "_", s)  
    s = re.sub(r"\s+", " ", s).strip()
    return s

def suggested_filename_from_headers(resp: requests.Response) -> Optional[str]:
    cd = resp.headers.get("Content-Disposition", "") or resp.headers.get("content-disposition", "")
    if "filename=" in cd:
        m = re.search(r'filename\*=UTF-8\'\'([^;]+)', cd)
        if m:
            return m.group(1)
        m = re.search(r'filename="?([^";]+)"?', cd)
        if m:
            return m.group(1)
    return None

def ensure_pdf_ext(name: str) -> str:
    return name if name.lower().endswith(".pdf") else (name + ".pdf")

def is_pdf_magic_bytes(content_head: bytes) -> bool:
    return content_head[:5] == b"%PDF-"

def sanitize_filename(name: str) -> str:
    name = re.sub(r"[^\w\-. ]+", "_", name)  
    name = re.sub(r"\s+", " ", name).strip()
    return name[:160] 

def pick_filename(company_id: str, url: str, resp: requests.Response) -> str:
    cd_name = suggested_filename_from_headers(resp)
    if cd_name:
        base = sanitize_filename(cd_name)
    else:
        base = re.sub(r"[?#].*$", "", url)
        base = base.rstrip("/").split("/")[-1] or f"{sha1(url)}.pdf"
        base = sanitize_filename(base)
    base = ensure_pdf_ext(base)
    prefix = sanitize_filename(company_id.replace(" ", "_"))
    return f"{prefix}__{base}"

def backoff_sleep(attempt: int, base: float = 0.8, cap: float = 8.0):
    t = min(cap, base * (2 ** attempt))
    time.sleep(t)

def check_existing_is_pdf(path: Path) -> bool:
    try:
        with open(path, "rb") as f:
            head = f.read(8)
        return is_pdf_magic_bytes(head)
    except Exception:
        return False

def download_one(session: requests.Session, row: dict, out_root: Path,
                 timeout: float = 30.0, min_bytes: int = 50_000) -> Tuple[bool, dict]:
    company_id = norm_company_id(row.get("company_id", "UNKNOWN"))
    url        = (row.get("best_url") or row.get("url") or "").strip()
    res = {
        "company_id": company_id,
        "url": url,
        "out_path": None,
        "http_status": None,
        "size": 0,
        "is_pdf": False,
        "error": None,
    }

    if not company_id or not url:
        res["error"] = "missing_company_or_url"
        return False, res

    out_dir = out_root / company_id
    out_dir.mkdir(parents=True, exist_ok=True)

    tries = 0
    while tries < 4:
        tries += 1
        try:
            with session.get(url, stream=True, timeout=timeout, allow_redirects=True) as resp:
                res["http_status"] = resp.status_code
                if resp.status_code >= 400:
                    res["error"] = f"http_{resp.status_code}"
                    break

                first_chunk = next(resp.iter_content(chunk_size=8192), b"")
                if not first_chunk:
                    res["error"] = "empty_response"
                    break
                fname = pick_filename(company_id, resp.url or url, resp)
                out_path = out_dir / fname

                if out_path.exists() and out_path.stat().st_size >= min_bytes:
                    res["out_path"] = str(out_path)
                    res["size"] = out_path.stat().st_size
                    res["is_pdf"] = check_existing_is_pdf(out_path)
                    return True, res

                tmp_path = out_path.with_suffix(out_path.suffix + ".part")
                with open(tmp_path, "wb") as f:
                    f.write(first_chunk)
                    for chunk in resp.iter_content(chunk_size=1024 * 128):
                        if not chunk:
                            break
                        f.write(chunk)

                size = tmp_path.stat().st_size
                if size < min_bytes:
                    res["error"] = f"too_small({size})"
                    tmp_path.unlink(missing_ok=True)
                    break

                with open(tmp_path, "rb") as f:
                    head = f.read(8)
                is_pdf = is_pdf_magic_bytes(head)

                tmp_path.rename(out_path)
                res["out_path"] = str(out_path)
                res["size"] = size
                res["is_pdf"] = is_pdf
                return True, res

        except requests.RequestException as e:
            res["error"] = f"req_error:{type(e).__name__}"
            backoff_sleep(tries)

    return False, res

def main(csv_path: str,
         out_dir: str = DEFAULT_OUT_DIR,
         only_label: Optional[str] = None,
         max_workers: int = 8,
         timeout: float = 30.0,
         user_agent: str = DEFAULT_UA,
         min_bytes: int = 50_000):
    if not os.path.exists(csv_path):
        print(f"[ERRORE] CSV non trovato: {csv_path}")
        sys.exit(1)

    df = pd.read_csv(csv_path, sep=",", quotechar='"', encoding="utf-8", engine="python", on_bad_lines="skip")

    if only_label and "valutazione" in df.columns:
        df = df[df["valutazione"].astype(str).str.lower() == only_label.strip().lower()].copy()

    if "company_id" not in df.columns:
        print("[ERRORE] Colonna 'company_id' mancante nel CSV.")
        sys.exit(1)
    if "best_url" not in df.columns and "url" not in df.columns:
        print("[ERRORE] Colonna 'best_url' (o 'url') mancante nel CSV.")
        sys.exit(1)

    rows = df.to_dict(orient="records")
    out_root = Path(out_dir)
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)   # cartella log/output

    session = requests.Session()
    session.headers.update({
        "User-Agent": user_agent,
        "Accept": "application/pdf,application/octet-stream,*/*;q=0.8",
    })

    results = []
    ok_count = 0
    fail_count = 0

    print(f"[downloader] Avvio: {len(rows)} URL da processare | max_workers={max_workers}")
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(download_one, session, r, out_root, timeout, min_bytes) for r in rows]
        for fut in as_completed(futs):
            ok, info = fut.result()
            results.append(info)
            if ok:
                ok_count += 1
                print(f"[OK] {info['company_id']} -> {info['out_path']} ({info['size']}B, pdf={info['is_pdf']})")
            else:
                fail_count += 1
                print(f"[FAIL] {info['company_id']} -> {info['url']} | {info['error']}")

    log_csv   = Path(OUTPUT_DIR) / "pdf_download_log.csv"
    log_jsonl = Path(OUTPUT_DIR) / "pdf_download_log.jsonl"
    pd.DataFrame(results).to_csv(log_csv, index=False, encoding="utf-8")

    with open(log_jsonl, "w", encoding="utf-8") as f:
        for r in results:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    small = [r for r in results if r.get("size", 0) < min_bytes]
    if small:
        small_path = Path(OUTPUT_DIR) / "pdf_sospetti_sotto_soglia.csv"
        pd.DataFrame(small).to_csv(small_path, index=False, encoding="utf-8")
        print(f"\n⚠️  File sotto soglia (< {min_bytes} B): {len(small)} → {small_path}")

    print("\n=== SUMMARY ===")
    print(f"Totale: {len(rows)} | OK: {ok_count} | FAIL: {fail_count}")
    print(f"PDF dir:  {out_root}")
    print(f"Log CSV:  {log_csv}")
    print(f"Log JSON: {log_jsonl}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scarica PDF dai candidati.")
    parser.add_argument("csv_path", help="CSV con colonne company_id, best_url (e opz. valutazione, score)")
    parser.add_argument("--out-dir", default=DEFAULT_OUT_DIR,
                        help=f"Cartella base di salvataggio (default: {DEFAULT_OUT_DIR})")
    parser.add_argument("--only-label", default=None,
                        help="Scarica solo righe con valutazione uguale a questo valore (giusta/parziale/sbagliata)")
    parser.add_argument("--max-workers", type=int, default=8, help="Concorrenza download")
    parser.add_argument("--timeout", type=float, default=30.0, help="Timeout singola richiesta (s)")
    parser.add_argument("--user-agent", default=DEFAULT_UA, help="User-Agent HTTP")
    parser.add_argument("--min-bytes", type=int, default=50_000,
                        help="Soglia minima (byte) per considerare valido il file scaricato (default: 50000)")

    args = parser.parse_args()
    main(
        csv_path=args.csv_path,
        out_dir=args.out_dir,
        only_label=args.only_label,
        max_workers=args.max_workers,
        timeout=args.timeout,
        user_agent=args.user_agent,
        min_bytes=args.min_bytes,
    )
