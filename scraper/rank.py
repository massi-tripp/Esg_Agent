# rank.py
# Tiny ranker: read data/interim/candidates_raw.jsonl and write data/output/candidates_best.csv

import os
import csv
import json
import re
from collections import defaultdict

RAW = "data/interim/candidates_raw.jsonl"
OUT = "data/output/candidates_best.csv"
os.makedirs(os.path.dirname(OUT), exist_ok=True)

RE_YEAR = re.compile(r"(19\d{2}|20[0-4]\d)")
RE_GOOD = re.compile(r"(sustainab|esg|csr|non[-_\s]?financial|integrated|annual)", re.I)

def score(rec: dict) -> float:
    s = 0.0
    url = rec.get("target_url", "") or ""
    anchor = rec.get("anchor_text", "") or ""
    # PDF bonus
    if rec.get("is_pdf"):
        s += 25  # +5 rispetto a prima
    # keyword signals
    if RE_GOOD.search(url): s += 15
    if RE_GOOD.search(anchor): s += 10
    # year signal (bonus ma non obbligatorio)
    y1 = rec.get("year_in_url")
    y2 = rec.get("year_in_anchor")
    if y1: s += 8
    if y2: s += 5
    # depth prior (shallower a bit better)
    try:
        s += max(0, 5 - int(rec.get("depth", 5)))
    except Exception:
        pass
    return s

best_by_company = {}

if os.path.exists(RAW):
    with open(RAW, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue
            cid = rec.get("company_id") or "UNKNOWN"
            sc = score(rec)
            if (cid not in best_by_company) or (sc > best_by_company[cid][0]):
                best_by_company[cid] = (sc, rec)

rows = []
for cid, (sc, rec) in best_by_company.items():
    url = rec.get("target_url", "")
    title = rec.get("anchor_text", "") or ""
    suspected_year = rec.get("year_in_url") or rec.get("year_in_anchor")
    rows.append({
        "company_id": cid,
        "best_url": url,
        "type": "pdf" if rec.get("is_pdf") else rec.get("guess_type") or "",
        "score": f"{sc:.1f}",
        "title": title,
        "suspected_year": suspected_year or "",
        "render_used": "",
        "notes": "",
    })

# Also include companies with no candidates? Your upstream CSV would be needed; this file only ranks candidates seen.

# Write CSV
fields = ["company_id","best_url","type","score","title","suspected_year","render_used","notes"]
with open(OUT, "w", newline="", encoding="utf-8") as fo:
    w = csv.DictWriter(fo, fieldnames=fields)
    w.writeheader()
    for r in sorted(rows, key=lambda x: x["company_id"]):
        w.writerow(r)

print(f"Wrote {len(rows)} rows to {OUT}")
