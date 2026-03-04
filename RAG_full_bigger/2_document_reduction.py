# File: reduce_to_taxonomy_focused.py

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional

"""
Riduzione report in finestre focalizzate sulle pagine EU Taxonomy.

Migliorie principali (soprattutto per 2023):
- Strong phrases specifiche per anno (2024 vs 2023).
- Weak keywords specifiche per anno.
- Score strutturale (tabelle/codici) per intercettare disclosure 2023 meno standard.
- Fallback robusto: se non ci sono hit, prende le TOP-N pagine per score e costruisce finestre.
"""

# =========================
# CONFIG
# =========================
BASE_DIR = Path(r"C:\Universita\TESI\esg_agent\RAG_full_bigger\marker_artifacts")
YEARS = ["2023", "2024"]

OVERWRITE_EXISTING = True
INCLUDE_PAGE_HEADERS = True

# Cluster: consideriamo lo stesso blocco se gli hit sono vicini
MAX_GAP_BETWEEN_HITS = 2

# Fallback generale
ENABLE_FALLBACK_WEAK = True

# Se il fallback threshold non produce hit, prendi comunque top-N pagine per score
ENABLE_FALLBACK_TOPN = True
FALLBACK_TOPN_PAGES = 3

# =========================
# YEAR-SPECIFIC CONFIG
# =========================
YEAR_CFG: Dict[str, Dict[str, Any]] = {
    "2024": {
        "before_pages": 2,
        "after_pages": 5,
        "weak_threshold": 2,
        "strong_phrases": [
            "A. TAXONOMY-ELIGIBLE ACTIVITIES",
            "A.1. Environmentally sustainable activities (Taxonomy-aligned)",
            "A.2. Taxonomy-eligible but not environmentally sustainable activities (not Taxonomy-aligned activities)",
        ],
        "weak_keywords": [
            r"eu\s*taxonomy",
            r"taxonomy[-\s]*(eligible|aligned)",
            r"\bturnover\b",
            r"\bcapex\b",
            r"\bopex\b",
            r"\bccm\b",
            r"\bcca\b",
            r"\bce\b",
            r"substantial\s+contribution",
            r"minimum\s+safeguards",
            r"\bdnsh\b",
        ],
    },
    "2023": {
        # nel 2023 spesso le tabelle sono spezzate e la sezione è meno “template”
        "before_pages": 2,
        "after_pages": 5,
        "weak_threshold": 2,
        "strong_phrases": [
            # Varianti più “elastiche” (non affidarti solo ad A.1/A.2)
            "EU Taxonomy",
            "EU taxonomy",
            "Taxonomy-eligible activities",
            "Taxonomy eligible activities",
            "Taxonomy-aligned activities",
            "Taxonomy aligned activities",
            "Environmentally sustainable activities",
            "Eligible but not aligned",
            "Article 8",
            "Annex I",
            "Annex II",
        ],
        "weak_keywords": [
            # keyword più adatte al 2023 (spesso disclosure meno standard)
            r"eu\s*taxonomy",
            r"article\s*8",
            r"taxonomy[-\s]*(eligible|aligned)",
            r"environmentally\s+sustainable",
            r"eligible\s+but\s+not",
            r"\bturnover\b",
            r"\bcapex\b",
            r"\bopex\b",
            r"\bnace\b",
            r"\bannex\s*(i|ii)\b",
            r"technical\s+screening\s+criteria",
            r"\bdnsh\b",
            r"minimum\s+safeguards",
        ],
    },
}

# =========================
# NORMALIZATION
# =========================
def normalize_spaces(s: str) -> str:
    s = (s or "").replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def compile_strong_patterns(phrases: List[str]) -> List[re.Pattern]:
    pats: List[re.Pattern] = []
    for p in phrases:
        p_norm = normalize_spaces(p)
        pat = re.escape(p_norm).replace(r"\ ", r"\s+")
        pats.append(re.compile(pat, flags=re.IGNORECASE))
    return pats


# =========================
# STRUCTURAL SIGNALS (molto utili nel 2023)
# =========================
# codici tipici: 6.10, 7.7, CCM 3.3, C17.1.2, A1.6.2
CODE_RE = re.compile(
    r"\b(?:CCM|CCA|CE)\s*\d+(?:\.\d+)?\b"
    r"|\b[A-Z]\d+(?:\.\d+)+\b"
    r"|\b\d+\.\d+\b",
    flags=re.IGNORECASE
)

# segnali tabellari
PIPE_TABLE_RE = re.compile(r"\|.+\|")
MULTI_SPACE_COL_RE = re.compile(r"\S(?:\s{2,})\S")
TABLE_WORDS_RE = re.compile(r"\b(economic\s+activity|activity|code|nace)\b", flags=re.IGNORECASE)


def structural_score_page(text: str) -> int:
    """
    Score “cheap” che intercetta pagine che probabilmente contengono tabelle/righe attività.
    Cap a 6 per mantenere scala compatibile.
    """
    t = text or ""
    s = 0
    if CODE_RE.search(t):
        s += 2
    if PIPE_TABLE_RE.search(t):
        s += 2
    if MULTI_SPACE_COL_RE.search(t):
        s += 1
    if TABLE_WORDS_RE.search(t):
        s += 1
    return min(s, 6)


# =========================
# PAGE PARSERS
# =========================
PAGE_2024_RE = re.compile(r"^\s*##\s*Page\s+(\d+)\s*$", flags=re.IGNORECASE)
BLOCK_2023_RE = re.compile(r"^\{\d+\}-+\s*$")  # {0}------------------------------------------------


def parse_pages_2024(md_text: str) -> List[Tuple[int, str]]:
    lines = md_text.splitlines()
    pages: List[Tuple[int, List[str]]] = []
    current_page: Optional[int] = None
    buf: List[str] = []

    def flush():
        nonlocal buf, current_page
        if current_page is not None:
            pages.append((current_page, buf))
        buf = []

    for line in lines:
        m = PAGE_2024_RE.match(line)
        if m:
            flush()
            current_page = int(m.group(1))
            continue
        buf.append(line)

    flush()

    out: List[Tuple[int, str]] = []
    for p, b in pages:
        out.append((p, "\n".join(b).strip()))
    return out


def parse_pages_2023(md_text: str) -> List[Tuple[int, str]]:
    """
    Formato 2023: split su righe "{n}-----".
    Numero pagina: proviamo a leggerlo da una riga tipo "**1**".
    Se non presente, usiamo l'indice del blocco (1-based) come fallback.
    """
    text = md_text.replace("\r\n", "\n").replace("\r", "\n")
    lines = text.splitlines()

    blocks: List[List[str]] = []
    cur: List[str] = []
    started = False

    for line in lines:
        if BLOCK_2023_RE.match(line.strip()):
            if started:
                blocks.append(cur)
                cur = []
            else:
                started = True
                cur = []
            continue
        if started:
            cur.append(line)

    if started and cur:
        blocks.append(cur)

    pages: List[Tuple[int, str]] = []
    for i, b in enumerate(blocks, start=1):
        block_text = "\n".join(b).strip()

        m = re.search(r"^\s*\*\*(\d{1,4})\*\*\s*$", block_text, flags=re.MULTILINE)
        if m:
            page_no = int(m.group(1))
        else:
            page_no = i

        pages.append((page_no, block_text))

    pages.sort(key=lambda x: x[0])
    return pages


# =========================
# WINDOW LOGIC
# =========================
def strong_hit_pages(pages: List[Tuple[int, str]], strong_patterns: List[re.Pattern], strong_phrases: List[str]) -> Dict[int, List[str]]:
    hits: Dict[int, List[str]] = {}
    for p, t in pages:
        t_norm = normalize_spaces(t)
        matched = []
        for i, rex in enumerate(strong_patterns):
            if rex.search(t_norm):
                matched.append(strong_phrases[i])
        if matched:
            hits[p] = matched
    return hits


def compile_weak_res(weak_keywords: List[str]) -> List[re.Pattern]:
    return [re.compile(p, flags=re.IGNORECASE) for p in weak_keywords]


def weak_score_page(text: str, weak_res: List[re.Pattern]) -> int:
    """
    Score debole: +1 per ogni keyword matchata (cap 6).
    """
    t = text or ""
    s = 0
    for r in weak_res:
        if r.search(t):
            s += 1
    return min(s, 6)


def combined_page_score(text: str, weak_res: List[re.Pattern]) -> int:
    """
    Score combinato: keyword deboli + segnali strutturali.
    Questo è quello che aiuta tantissimo nel 2023.
    """
    return min(6, weak_score_page(text, weak_res) + structural_score_page(text))


def cluster_pages(hit_pages: List[int], max_gap: int) -> List[List[int]]:
    if not hit_pages:
        return []
    hit_pages = sorted(set(hit_pages))
    clusters: List[List[int]] = []
    cur = [hit_pages[0]]
    for p in hit_pages[1:]:
        if p - cur[-1] <= max_gap + 1:
            cur.append(p)
        else:
            clusters.append(cur)
            cur = [p]
    clusters.append(cur)
    return clusters


def expand_cluster(cluster: List[int], before: int, after: int, min_page: int, max_page: int) -> Tuple[int, int]:
    return max(min_page, cluster[0] - before), min(max_page, cluster[-1] + after)


def merge_overlapping_windows(windows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not windows:
        return []

    windows_sorted = sorted(windows, key=lambda w: (w["start_page"], w["end_page"]))
    merged: List[Dict[str, Any]] = []

    for w in windows_sorted:
        if not merged:
            ww = dict(w)
            ww["reason"] = {"merged_reasons": [w.get("reason")]}
            merged.append(ww)
            continue

        last = merged[-1]
        if w["start_page"] <= last["end_page"] + 1:
            last["start_page"] = min(last["start_page"], w["start_page"])
            last["end_page"] = max(last["end_page"], w["end_page"])
            last["hit_pages"] = sorted(set(last.get("hit_pages", []) + w.get("hit_pages", [])))
            last["reason"]["merged_reasons"].append(w.get("reason"))
        else:
            ww = dict(w)
            ww["reason"] = {"merged_reasons": [w.get("reason")]}
            merged.append(ww)

    return merged


def build_focused_md(pages: List[Tuple[int, str]], windows: List[Dict[str, Any]]) -> str:
    page_map = {p: t for p, t in pages}

    out_parts: List[str] = []
    for w in windows:
        for p in range(w["start_page"], w["end_page"] + 1):
            t = page_map.get(p, "")
            if INCLUDE_PAGE_HEADERS:
                out_parts.append(f"## Page {p}\n\n{t if t else '<MISSING_PAGE_TEXT>'}\n")
            else:
                out_parts.append(f"{t if t else '<MISSING_PAGE_TEXT>'}\n")

    return "\n".join(out_parts).strip() + "\n"


def pick_source_md(year_dir: Path) -> Optional[Path]:
    if not year_dir.exists():
        return None
    canon = sorted(year_dir.glob("*_report.md"))
    if canon:
        return canon[0]
    mds = sorted(year_dir.rglob("*.md"))
    return mds[0] if mds else None


# =========================
# MAIN
# =========================
def main() -> None:
    if not BASE_DIR.exists():
        raise FileNotFoundError(f"Base dir non trovata: {BASE_DIR}")

    company_dirs = [p for p in BASE_DIR.iterdir() if p.is_dir()]
    company_dirs.sort()

    total_ok = 0
    total_skip_no_md = 0
    total_skip_no_hits = 0
    total_skip_existing = 0

    for cdir in company_dirs:
        slug = cdir.name

        for year in YEARS:
            year_dir = cdir / year
            src_md = pick_source_md(year_dir)
            if src_md is None:
                continue

            out_dir = year_dir / "focused"
            out_dir.mkdir(parents=True, exist_ok=True)

            out_md = out_dir / f"{slug}_{year}_taxonomy_focused.md"
            out_audit = out_dir / f"{slug}_{year}_taxonomy_focused.audit.json"

            if out_md.exists() and out_md.stat().st_size > 0 and not OVERWRITE_EXISTING:
                total_skip_existing += 1
                continue

            md_text = src_md.read_text(encoding="utf-8", errors="replace")

            # parse pagine per formato anno
            if year == "2024":
                pages = parse_pages_2024(md_text)
            else:
                pages = parse_pages_2023(md_text)

            if not pages:
                total_skip_no_md += 1
                out_audit.write_text(json.dumps({
                    "company_slug": slug,
                    "year": year,
                    "source_md": str(src_md),
                    "status": "no_pages_parsed",
                }, ensure_ascii=False, indent=2), encoding="utf-8")
                continue

            min_page = min(p for p, _ in pages)
            max_page = max(p for p, _ in pages)

            cfg = YEAR_CFG.get(year, YEAR_CFG["2024"])
            before_pages = int(cfg["before_pages"])
            after_pages = int(cfg["after_pages"])
            weak_threshold = int(cfg["weak_threshold"])

            strong_phrases = list(cfg["strong_phrases"])
            strong_patterns = compile_strong_patterns(strong_phrases)

            weak_res = compile_weak_res(list(cfg["weak_keywords"]))

            # 1) HIT FORTI
            strong_hits = strong_hit_pages(pages, strong_patterns, strong_phrases)
            hit_pages = sorted(strong_hits.keys())

            windows: List[Dict[str, Any]] = []

            if hit_pages:
                clusters = cluster_pages(hit_pages, MAX_GAP_BETWEEN_HITS)
                for cl in clusters:
                    start, end = expand_cluster(cl, before_pages, after_pages, min_page, max_page)
                    windows.append({
                        "hit_pages": cl,
                        "start_page": start,
                        "end_page": end,
                        "reason": {
                            "type": "strong_phrase",
                            "matched_phrases_by_page": {str(p): strong_hits[p] for p in cl},
                        }
                    })
            elif ENABLE_FALLBACK_WEAK:
                # 2) FALLBACK (weak + structural score)
                scores: Dict[int, int] = {}
                weak_hits: List[int] = []

                for p, t in pages:
                    s = combined_page_score(t, weak_res)
                    scores[p] = s
                    if s >= weak_threshold:
                        weak_hits.append(p)

                # Se non ho hit sopra soglia, prendo TOP-N pagine per score (se abilitato)
                if not weak_hits and ENABLE_FALLBACK_TOPN:
                    # prendi solo pagine con score > 0
                    scored = [(p, s) for p, s in scores.items() if s > 0]
                    scored.sort(key=lambda x: (-x[1], x[0]))
                    top_pages = [p for p, _ in scored[:FALLBACK_TOPN_PAGES]]
                    weak_hits = sorted(set(top_pages))

                if weak_hits:
                    clusters = cluster_pages(weak_hits, MAX_GAP_BETWEEN_HITS)
                    for cl in clusters:
                        start, end = expand_cluster(cl, before_pages, after_pages, min_page, max_page)
                        windows.append({
                            "hit_pages": cl,
                            "start_page": start,
                            "end_page": end,
                            "reason": {
                                "type": "weak_or_structural_fallback",
                                "page_score_threshold": weak_threshold,
                                "scores": {str(p): scores.get(p, 0) for p in cl},
                            }
                        })

            if not windows:
                total_skip_no_hits += 1
                out_audit.write_text(json.dumps({
                    "company_slug": slug,
                    "year": year,
                    "source_md": str(src_md),
                    "status": "no_hits_found",
                    "strong_phrases": strong_phrases,
                    "weak_enabled": ENABLE_FALLBACK_WEAK,
                    "weak_threshold": weak_threshold,
                    "fallback_topn_enabled": ENABLE_FALLBACK_TOPN,
                    "fallback_topn_pages": FALLBACK_TOPN_PAGES,
                }, ensure_ascii=False, indent=2), encoding="utf-8")
                continue

            # merge finestre sovrapposte/contigue
            windows = merge_overlapping_windows(windows)

            focused_md = build_focused_md(pages, windows)
            out_md.write_text(focused_md, encoding="utf-8")

            out_audit.write_text(json.dumps({
                "company_slug": slug,
                "year": year,
                "source_md": str(src_md),
                "focused_md": str(out_md),
                "status": "ok",
                "params": {
                    "before_pages": before_pages,
                    "after_pages": after_pages,
                    "max_gap_between_hits": MAX_GAP_BETWEEN_HITS,
                    "weak_enabled": ENABLE_FALLBACK_WEAK,
                    "weak_threshold": weak_threshold,
                    "fallback_topn_enabled": ENABLE_FALLBACK_TOPN,
                    "fallback_topn_pages": FALLBACK_TOPN_PAGES,
                    "merge_overlapping_windows": True,
                    "overwrite_existing": OVERWRITE_EXISTING,
                    "include_page_headers": INCLUDE_PAGE_HEADERS,
                    "year_cfg_used": year,
                },
                "windows": windows,
            }, ensure_ascii=False, indent=2), encoding="utf-8")

            print(f"[OK] {slug} {year}: scritto -> {out_md}")
            total_ok += 1

    print("\n========== SUMMARY ==========")
    print(f"Focused creati:               {total_ok:,}")
    print(f"SKIP no pages parsed:         {total_skip_no_md:,}")
    print(f"SKIP no hits/windows:         {total_skip_no_hits:,}")
    print(f"SKIP existing (no overwrite): {total_skip_existing:,}")
    print("=============================\n")


if __name__ == "__main__":
    main()

'''
========== SUMMARY ==============
Focused creati:               220
SKIP no pages parsed:         2
SKIP no hits/windows:         0
SKIP existing (no overwrite): 0
=================================
'''