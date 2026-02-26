from __future__ import annotations

import json
import re
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional

'''
Questo file implementa la logica di riduzione del documento a finestre focalizzate sulle pagine 
che contengono frasi chiave forti (es. "A.1. Environmentally sustainable activities (Taxonomy-aligned)").
Abbiamo tre frasi che sono sempre presenti prima di una tabella che contiene le attività:
"A. TAXONOMY-ELIGIBLE ACTIVITIES",
"A.1. Environmentally sustainable activities (Taxonomy-aligned)",
"A.2. Taxonomy-eligible but not environmentally sustainable activities (not Taxonomy-aligned activities)",
'''

# =========================
# CONFIG
# =========================
BASE_DIR = Path(r"C:\Universita\TESI\esg_agent\RAG_full\marker_artifacts")
YEARS = ["2023", "2024"]

# window expansion
BEFORE_PAGES = 1
AFTER_PAGES = 3

# cluster: consideriamo lo stesso blocco se gli hit sono vicini
MAX_GAP_BETWEEN_HITS = 2

# fallback (debole) se non troviamo le frasi forti
ENABLE_FALLBACK_WEAK = True
WEAK_PAGE_SCORE_THRESHOLD = 3

# se True sovrascrive i file md già esistenti
OVERWRITE_EXISTING = False

# se True NON include righe tipo "## Page N" nel focused (solo testo puro)
INCLUDE_PAGE_HEADERS = True


# =========================
# STRONG PHRASES (massima priorità)
# =========================
STRONG_PHRASES = [
    "A. TAXONOMY-ELIGIBLE ACTIVITIES",
    "A.1. Environmentally sustainable activities (Taxonomy-aligned)",
    "A.2. Taxonomy-eligible but not environmentally sustainable activities (not Taxonomy-aligned activities)",
]


def normalize_spaces(s: str) -> str:
    """
    Normalizza spazi/newline per rendere robusto il match di frasi che in MD
    possono avere newline o doppi spazi.
    """
    s = (s or "").replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"\s+", " ", s).strip()
    return s


# Precompila pattern robusti per le 3 frasi “esatte” (ma tollerando whitespace)
STRONG_PATTERNS = []
for p in STRONG_PHRASES:
    p_norm = normalize_spaces(p)
    pat = re.escape(p_norm).replace(r"\ ", r"\s+")
    STRONG_PATTERNS.append(re.compile(pat, flags=re.IGNORECASE))


# =========================
# WEAK FALLBACK KEYWORDS (bassa importanza)
# =========================
WEAK_KEYWORDS = [
    r"eu\s*taxonomy",
    r"taxonomy[-\s]*(eligible|aligned)",
    r"\bturnover\b",
    r"\bcapex\b",
    r"\bopex\b",
    r"\bccm\b",
    r"\bcca\b",
    r"\bn/el\b",
    r"substantial\s+contribution",
    r"minimum\s+safeguards",
    r"\bdnsh\b",
    r"\b%\b",
]
WEAK_RE = [re.compile(p, flags=re.IGNORECASE) for p in WEAK_KEYWORDS]


# =========================
# PAGE PARSERS
# =========================
PAGE_2024_RE = re.compile(r"^\s*##\s*Page\s+(\d+)\s*$", flags=re.IGNORECASE)
BLOCK_2023_RE = re.compile(r"^\{\d+\}-+\s*$")  # {0}------------------------------------------------


def parse_pages_2024(md_text: str) -> List[Tuple[int, str]]:
    """
    Formato 2024: blocchi con header "## Page N".
    """
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
def strong_hit_pages(pages: List[Tuple[int, str]]) -> Dict[int, List[str]]:
    """
    Ritorna dict: page -> lista frasi forti matchate.
    """
    hits: Dict[int, List[str]] = {}
    for p, t in pages:
        t_norm = normalize_spaces(t)
        matched = []
        for i, rex in enumerate(STRONG_PATTERNS):
            if rex.search(t_norm):
                matched.append(STRONG_PHRASES[i])
        if matched:
            hits[p] = matched
    return hits


def weak_score_page(text: str) -> int:
    """
    Score debole: +1 per ogni keyword matchata (cap 6).
    """
    t = text or ""
    s = 0
    for r in WEAK_RE:
        if r.search(t):
            s += 1
    return min(s, 6)


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
    """
    Unisce finestre sovrapposte o contigue per evitare duplicazioni.
    Regola: se next.start_page <= last.end_page + 1 => merge.
    """
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
    """
    ✅ NUOVO FORMATO: solo contenuto estratto (niente header/summary, niente "WINDOW ...").
    Mantiene (opzionale) "## Page N" per preservare il contesto pagina.
    """
    page_map = {p: t for p, t in pages}

    out_parts: List[str] = []
    for w in windows:
        for p in range(w["start_page"], w["end_page"] + 1):
            t = page_map.get(p, "")
            if INCLUDE_PAGE_HEADERS:
                out_parts.append(f"## Page {p}\n\n{t if t else '<MISSING_PAGE_TEXT>'}\n")
            else:
                out_parts.append(f"{t if t else '<MISSING_PAGE_TEXT>'}\n")

    # separatore leggero tra finestre (opzionale): qui non lo metto per ridurre rumore
    return "\n".join(out_parts).strip() + "\n"


def pick_source_md(year_dir: Path) -> Optional[Path]:
    """
    Preferisci il canonico "*_report.md".
    Altrimenti prendi il primo .md trovato (ricorsivo).
    """
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

            # ✅ controllo sovrascrittura
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

            # 1) HIT FORTI
            strong_hits = strong_hit_pages(pages)
            hit_pages = sorted(strong_hits.keys())

            windows: List[Dict[str, Any]] = []

            if hit_pages:
                clusters = cluster_pages(hit_pages, MAX_GAP_BETWEEN_HITS)
                for cl in clusters:
                    start, end = expand_cluster(cl, BEFORE_PAGES, AFTER_PAGES, min_page, max_page)
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
                # 2) FALLBACK DEBOLE
                weak_hits = []
                weak_scores = {}
                for p, t in pages:
                    s = weak_score_page(t)
                    weak_scores[p] = s
                    if s >= WEAK_PAGE_SCORE_THRESHOLD:
                        weak_hits.append(p)

                if weak_hits:
                    clusters = cluster_pages(weak_hits, MAX_GAP_BETWEEN_HITS)
                    for cl in clusters:
                        start, end = expand_cluster(cl, BEFORE_PAGES, AFTER_PAGES, min_page, max_page)
                        windows.append({
                            "hit_pages": cl,
                            "start_page": start,
                            "end_page": end,
                            "reason": {
                                "type": "weak_fallback",
                                "page_score_threshold": WEAK_PAGE_SCORE_THRESHOLD,
                                "scores": {str(p): weak_scores[p] for p in cl},
                            }
                        })

            if not windows:
                total_skip_no_hits += 1
                out_audit.write_text(json.dumps({
                    "company_slug": slug,
                    "year": year,
                    "source_md": str(src_md),
                    "status": "no_hits_found",
                    "strong_phrases": STRONG_PHRASES,
                    "weak_enabled": ENABLE_FALLBACK_WEAK,
                    "weak_threshold": WEAK_PAGE_SCORE_THRESHOLD,
                }, ensure_ascii=False, indent=2), encoding="utf-8")
                continue

            # merge finestre sovrapposte/contigue
            windows = merge_overlapping_windows(windows)

            # ✅ crea focused senza header/summary/window
            focused_md = build_focused_md(pages, windows)
            out_md.write_text(focused_md, encoding="utf-8")

            # audit resta (utile per debug), ma non finisce dentro l'md
            out_audit.write_text(json.dumps({
                "company_slug": slug,
                "year": year,
                "source_md": str(src_md),
                "focused_md": str(out_md),
                "status": "ok",
                "params": {
                    "before_pages": BEFORE_PAGES,
                    "after_pages": AFTER_PAGES,
                    "max_gap_between_hits": MAX_GAP_BETWEEN_HITS,
                    "weak_enabled": ENABLE_FALLBACK_WEAK,
                    "weak_page_score_threshold": WEAK_PAGE_SCORE_THRESHOLD,
                    "merge_overlapping_windows": True,
                    "overwrite_existing": OVERWRITE_EXISTING,
                    "include_page_headers": INCLUDE_PAGE_HEADERS,
                },
                "windows": windows,
            }, ensure_ascii=False, indent=2), encoding="utf-8")

            print(f"[OK] {slug} {year}: scritto -> {out_md}")
            total_ok += 1

    print("\n========== SUMMARY ==========")
    print(f"Focused creati:          {total_ok:,}")
    print(f"SKIP no pages parsed:    {total_skip_no_md:,}")
    print(f"SKIP no hits/windows:    {total_skip_no_hits:,}")
    print(f"SKIP existing (no overwrite): {total_skip_existing:,}")
    print("=============================\n")


if __name__ == "__main__":
    main()


'''
========== SUMMARY ==========
Focused creati:          188
SKIP no pages parsed:    2
SKIP no hits/windows:    37
SKIP existing (no overwrite): 0
=============================
Al momento ho 188 md focalizzati, con 37 md che non contengono le frasi forti né sufficienti keyword deboli (ma magari in futuro potrei volerli comunque se cambio i parametri).
Per il momento vediamo come va la RAG con i 188 match forti.
'''