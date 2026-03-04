from __future__ import annotations

import re
import unicodedata
from pathlib import Path
from typing import Optional, Dict, List, Tuple, Callable, Set

import pandas as pd

# =========================
# PATH
# =========================
# RAG già pulito: company, report_year, activity, label, code_numeric
RAG_xlsx = Path(r"C:\Universita\TESI\esg_agent\RAG_full_bigger\rag2_out\activities_extracted_clean.xlsx")
BENCH_XLSX = Path(r"C:\Universita\TESI\esg_agent\RAG_full_bigger\documentazione_rag.xlsx")

REMOVE_PARENTHESES_IN_ACTIVITY = True

# Match SOLO su activity, ma dentro la stessa company.
MATCH_ON_YEAR = False

# =========================
# FUZZY MATCH SETTINGS
# =========================
USE_FUZZY_MATCH = True
FUZZY_THRESHOLD = 92      # 0..100 (alto = più "sicuro")
FUZZY_MIN_LEN = 8         # evita fuzzy su stringhe troppo corte
FUZZY_ONE_TO_ONE = True   # evita che una riga RAG matchi più righe BENCH nel fuzzy
# =========================
# NORMALIZZAZIONE ACTIVITY
# =========================
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+", flags=re.IGNORECASE)
_MULTI_SPACE_RE = re.compile(r"\s+")
_PARENS_RE = re.compile(r"\([^)]*\)")


def strip_accents(text: str) -> str:
    nkfd = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in nkfd if not unicodedata.combining(ch))


def clean_activity_text(activity: Optional[str], remove_parentheses: bool = False) -> str:
    if activity is None:
        return ""
    s = str(activity).strip()
    if not s:
        return ""
    if remove_parentheses:
        s = _PARENS_RE.sub(" ", s)
    s = strip_accents(s).lower()
    s = _NON_ALNUM_RE.sub(" ", s)
    s = _MULTI_SPACE_RE.sub(" ", s).strip()
    return s


def make_match_key(company: str, year: str, activity_clean: str) -> str:
    company = (company or "").strip()
    year = (year or "").strip()
    activity_clean = (activity_clean or "").strip()
    if MATCH_ON_YEAR:
        return f"{company}||{year}||{activity_clean}"
    return f"{company}||{activity_clean}"


# =========================
# SIMILARITY (rapidfuzz if available, else difflib)
# =========================
def _get_similarity() -> Callable[[str, str], float]:
    """
    Ritorna una funzione sim(a,b)->score in [0..100].
    - Se c'è rapidfuzz: token_set_ratio (robusto a ordine e parole extra)
    - Altrimenti: SequenceMatcher (stdlib)
    """
    try:
        from rapidfuzz import fuzz  # type: ignore

        def sim(a: str, b: str) -> float:
            return float(fuzz.token_set_ratio(a, b))
        return sim
    except Exception:
        from difflib import SequenceMatcher

        def sim(a: str, b: str) -> float:
            return 100.0 * SequenceMatcher(None, a, b).ratio()
        return sim


SIM = _get_similarity()


def _group_key(company: str, year: str) -> Tuple[str, ...]:
    if MATCH_ON_YEAR:
        return (company, year)
    return (company,)


def fuzzy_match_bench_to_rag(
    bench_df: pd.DataFrame,
    rag_df: pd.DataFrame,
    bench_idx_candidates: Set[int],
    rag_idx_candidates: Set[int],
) -> pd.DataFrame:
    """
    Fuzzy match tra bench_df e rag_df SOLO dentro la stessa company (+year se MATCH_ON_YEAR).
    Ritorna DataFrame con colonne: bench_idx, rag_idx, fuzzy_score, match_type='fuzzy'
    - FUZZY_ONE_TO_ONE: se True, non riusa lo stesso rag_idx
    """
    rag_by_group: Dict[Tuple[str, ...], List[Tuple[int, str]]] = {}
    for r_idx in rag_idx_candidates:
        r = rag_df.loc[r_idx]
        act = str(r.get("activity_clean", "") or "").strip()
        if not act or len(act) < FUZZY_MIN_LEN:
            continue
        comp = str(r.get("company", "") or "").strip()
        yr = str(r.get("report_year", "") or "").strip()
        gk = _group_key(comp, yr)
        rag_by_group.setdefault(gk, []).append((r_idx, act))

    used_rag: Set[int] = set()
    rows: List[Dict[str, object]] = []

    for b_idx in bench_idx_candidates:
        b = bench_df.loc[b_idx]
        b_act = str(b.get("activity_clean", "") or "").strip()
        if not b_act or len(b_act) < FUZZY_MIN_LEN:
            continue

        comp = str(b.get("company", "") or "").strip()
        yr = str(b.get("report_year", "") or "").strip()
        gk = _group_key(comp, yr)

        candidates = rag_by_group.get(gk, [])
        if not candidates:
            continue

        best_score = -1.0
        best_r_idx = None

        for r_idx, r_act in candidates:
            if FUZZY_ONE_TO_ONE and r_idx in used_rag:
                continue
            score = SIM(b_act, r_act)
            if score > best_score:
                best_score = score
                best_r_idx = r_idx

        if best_r_idx is not None and best_score >= FUZZY_THRESHOLD:
            rows.append(
                {"bench_idx": b_idx, "rag_idx": best_r_idx, "fuzzy_score": float(best_score), "match_type": "fuzzy"}
            )
            if FUZZY_ONE_TO_ONE:
                used_rag.add(best_r_idx)

    return pd.DataFrame(rows)


# =====================================================================
# MATCH label+code / code-only helpers (come nei tuoi print)
# =====================================================================
_SPLIT_RE = re.compile(r"\s*(?:/|,|;|\||\n)\s*")
_CODE_NUM_RE = re.compile(r"\d+(?:\.\d+)?")

def _split_multi(s: str) -> List[str]:
    s = (s or "").strip()
    if not s:
        return []
    return [p.strip() for p in _SPLIT_RE.split(s) if p and p.strip()]

def _norm_code_numeric(x: str) -> str:
    x = (x or "").strip()
    if not x:
        return ""
    m = _CODE_NUM_RE.search(x)
    if m:
        return m.group(0)
    return re.sub(r"\s+", " ", x).strip().upper()

def _norm_objective(x: str) -> str:
    x = (x or "").strip().upper()
    if not x:
        return ""
    return re.sub(r"[^A-Z0-9]+", "", x)

def _is_missing_str(x: object) -> bool:
    return (x is None) or (str(x).strip() == "")


def evaluate_one_year(
    rag: pd.DataFrame,
    bench: pd.DataFrame,
    bench_all_dedup: pd.DataFrame,
    year: Optional[str],
) -> None:
    """
    Se year è None: valuta su tutto (comportamento originale).
    Se year è "2023"/"2024": valuta SOLO su quell'anno e stampa:
      - Summary
      - Total match label+code
      - Missing values
      - 4-step match
      - In più: missing label per anno (già dentro missing values)
    """
    if year is not None:
        rag_y = rag[rag["report_year"].astype(str).str.strip() == str(year)].copy()
        bench_y = bench[bench["report_year"].astype(str).str.strip() == str(year)].copy()
        bench_all_y = bench_all_dedup[bench_all_dedup["report_year"].astype(str).str.strip() == str(year)].copy()
        title = f"YEAR = {year}"
    else:
        rag_y = rag.copy()
        bench_y = bench.copy()
        bench_all_y = bench_all_dedup.copy()
        title = "ALL YEARS"

    # =========================
    # COMMON COMPANIES
    # =========================
    rag_companies = set(rag_y["company"].unique())
    bench_companies = set(bench_y["company"].unique())
    common_companies = sorted(rag_companies & bench_companies)

    print(f"\n\n==================== {title} ====================")

    if not common_companies:
        print("[WARN] Nessuna company comune tra RAG e benchmark per questo filtro.")
        return

    rag_c = rag_y[rag_y["company"].isin(common_companies)].copy()
    bench_c = bench_y[bench_y["company"].isin(common_companies)].copy()

    # indici espliciti
    rag_c["rag_idx"] = rag_c.index
    bench_c["bench_idx"] = bench_c.index

    # =========================
    # MATCH ATTIVITÀ (Exact + Fuzzy)
    # =========================
    merged_exact = bench_c.merge(
        rag_c,
        on="match_key",
        how="inner",
        suffixes=("_bench", "_rag")
    )
    merged_exact["match_type"] = "exact"
    merged_exact["fuzzy_score"] = 100.0

    # fix suffissi su colonne non-collidenti
    if "bench_idx_bench" not in merged_exact.columns and "bench_idx" in merged_exact.columns:
        merged_exact = merged_exact.rename(columns={"bench_idx": "bench_idx_bench"})
    if "rag_idx_rag" not in merged_exact.columns and "rag_idx" in merged_exact.columns:
        merged_exact = merged_exact.rename(columns={"rag_idx": "rag_idx_rag"})

    matched_bench_idx: Set[int] = set(merged_exact["bench_idx_bench"].tolist()) if not merged_exact.empty else set()
    matched_rag_idx: Set[int] = set(merged_exact["rag_idx_rag"].tolist()) if not merged_exact.empty else set()

    merged_fuzzy = pd.DataFrame()
    if USE_FUZZY_MATCH:
        bench_candidates = set(
            b for b in bench_c.index
            if b not in matched_bench_idx and str(bench_c.loc[b, "activity_clean"]).strip() != ""
        )
        rag_candidates = set(
            r for r in rag_c.index
            if r not in matched_rag_idx and str(rag_c.loc[r, "activity_clean"]).strip() != ""
        )

        fm = fuzzy_match_bench_to_rag(
            bench_df=bench_c,
            rag_df=rag_c,
            bench_idx_candidates=bench_candidates,
            rag_idx_candidates=rag_candidates,
        )

        if not fm.empty:
            bench_hit = bench_c.loc[fm["bench_idx"].values].copy().reset_index(drop=True)
            rag_hit = rag_c.loc[fm["rag_idx"].values].copy().reset_index(drop=True)
            fm = fm.reset_index(drop=True)

            merged_fuzzy = pd.concat(
                [
                    bench_hit.add_suffix("_bench"),
                    rag_hit.add_suffix("_rag"),
                    fm[["fuzzy_score", "match_type"]],
                ],
                axis=1,
            )

    if not merged_fuzzy.empty:
        merged = pd.concat([merged_exact, merged_fuzzy], ignore_index=True, sort=False)
    else:
        merged = merged_exact.copy()

    if not merged.empty and "bench_idx_bench" in merged.columns:
        matched_bench_idx = set(merged["bench_idx_bench"].tolist())
    else:
        matched_bench_idx = set()

    if not merged.empty and "rag_idx_rag" in merged.columns:
        matched_rag_idx = set(merged["rag_idx_rag"].tolist())
    else:
        matched_rag_idx = set()

    # =========================
    # SUMMARY (attività exact + fuzzy)
    # =========================
    total_bench = len(bench_c)
    total_rag = len(rag_c)
    total_matched_activities = len(matched_bench_idx)

    bench_rate = (total_matched_activities / total_bench) if total_bench else 0.0
    rag_rate = (len(matched_rag_idx) / total_rag) if total_rag else 0.0

    # =====================================================================
    # MATCH label+code / code-only (stessa logica tua)
    # =====================================================================
    rag_pairs_by_company: Dict[str, set] = {}
    for _, r in rag_c.iterrows():
        comp = (r.get("company") or "").strip()
        if not comp:
            continue

        code_raw = str(r.get("sub_activity_code_rag") or "")
        obj_raw = str(r.get("rag_label") or "")

        codes = [_norm_code_numeric(t) for t in _split_multi(code_raw)] or ([_norm_code_numeric(code_raw)] if code_raw.strip() else [])
        objs = [_norm_objective(t) for t in _split_multi(obj_raw)] or ([_norm_objective(obj_raw)] if obj_raw.strip() else [])

        codes = [c for c in codes if c]
        objs = [o for o in objs if o]
        if not codes or not objs:
            continue

        sset = rag_pairs_by_company.setdefault(comp, set())
        for c in codes:
            for o in objs:
                sset.add((c, o))

    bench_noyear = bench_c.copy()
    bench_noyear["bench_code_raw"] = bench_noyear["sub_activity_code"].astype(str).str.strip()
    bench_noyear["bench_obj_raw"] = bench_noyear["bench_objective"].astype(str).str.strip()
    bench_noyear = bench_noyear.drop_duplicates(subset=["company", "bench_code_raw", "bench_obj_raw"])

    total_bench_rows_effective = 0
    total_matched_rows = 0
    total_matched_pairs = 0

    for comp in common_companies:
        rag_pairs = rag_pairs_by_company.get(comp, set())
        bench_comp = bench_noyear[bench_noyear["company"] == comp]

        for _, br in bench_comp.iterrows():
            code_raw = str(br.get("bench_code_raw") or "")
            obj_raw = str(br.get("bench_obj_raw") or "")

            codes = {_norm_code_numeric(t) for t in _split_multi(code_raw)} or ({_norm_code_numeric(code_raw)} if code_raw.strip() else set())
            objs = {_norm_objective(t) for t in _split_multi(obj_raw)} or ({_norm_objective(obj_raw)} if obj_raw.strip() else set())

            codes = {c for c in codes if c}
            objs = {o for o in objs if o}
            if not codes or not objs:
                continue

            total_bench_rows_effective += 1
            matched_pairs_here = {(c, o) for c in codes for o in objs if (c, o) in rag_pairs}
            if matched_pairs_here:
                total_matched_rows += 1
                total_matched_pairs += len(matched_pairs_here)

    total_rate = (total_matched_rows / total_bench_rows_effective) if total_bench_rows_effective else 0.0

    # CODE-ONLY
    rag_codes_by_company: Dict[str, set] = {}
    for _, r in rag_c.iterrows():
        comp = (r.get("company") or "").strip()
        if not comp:
            continue
        code_raw = str(r.get("sub_activity_code_rag") or "")
        codes = [_norm_code_numeric(t) for t in _split_multi(code_raw)] or ([_norm_code_numeric(code_raw)] if code_raw.strip() else [])
        codes = [c for c in codes if c]
        if not codes:
            continue
        sset = rag_codes_by_company.setdefault(comp, set())
        for c in codes:
            sset.add(c)

    bench_code_noyear = bench_c.copy()
    bench_code_noyear["bench_code_raw"] = bench_code_noyear["sub_activity_code"].astype(str).str.strip()
    bench_code_noyear = bench_code_noyear.drop_duplicates(subset=["company", "bench_code_raw"])

    total_bench_code_rows = 0
    total_matched_code_rows = 0

    for comp in common_companies:
        rag_codes = rag_codes_by_company.get(comp, set())
        bench_comp = bench_code_noyear[bench_code_noyear["company"] == comp]
        for _, br in bench_comp.iterrows():
            code_raw = str(br.get("bench_code_raw") or "")
            codes = {_norm_code_numeric(t) for t in _split_multi(code_raw)} or ({_norm_code_numeric(code_raw)} if code_raw.strip() else set())
            codes = {c for c in codes if c}
            if not codes:
                continue
            total_bench_code_rows += 1
            if any(c in rag_codes for c in codes):
                total_matched_code_rows += 1

    code_only_rate = (total_matched_code_rows / total_bench_code_rows) if total_bench_code_rows else 0.0

    # =========================
    # MISSING VALUES IN RAG
    # =========================
    miss_label = int(rag_c["rag_label"].apply(_is_missing_str).sum()) if "rag_label" in rag_c.columns else -1
    miss_activity = int(rag_c["activity_raw"].apply(_is_missing_str).sum()) if "activity_raw" in rag_c.columns else -1
    miss_subcode = int(rag_c["sub_activity_code_rag"].apply(_is_missing_str).sum()) if "sub_activity_code_rag" in rag_c.columns else -1

    # =====================================================================
    # PRINT ORDER (come richiesto)
    # =====================================================================
    print("\n=== SUMMARY ===")
    print(f"Companies in common: {len(common_companies)}")
    print(f"Matched activities: {total_matched_activities}")
    print(f"Benchmark total activities (common companies): {total_bench} | match rate: {bench_rate:.2%}")
    print(f"RAG total activities (common companies): {total_rag} | match rate: {rag_rate:.2%}")

    print("\n=== TOTAL MATCH (Label + sub_code) ===")
    print(f"Benchmark rows (effective, company common): {total_bench_rows_effective}")
    print(f"Matched rows (OR on slash):               {total_matched_rows} | match rate: {total_rate:.2%}")
    print(f"Matched pairs sum (counts alternatives):  {total_matched_pairs}")
    print(f"Matched rows (solo codice):               {total_matched_code_rows} | match rate: {code_only_rate:.2%}")

    print("\n=== RAG MISSING VALUES (common companies) ===")
    print(f"Missing label (rag_label): {miss_label}")
    print(f"Missing activity (activity): {miss_activity}")
    print(f"Missing subcode (code_numeric): {miss_subcode}")

    # =====================================================================
    # 4-STEP MATCH (per BENCH filtered)
    # =====================================================================
    bench_remaining_idx = set(bench_c.index)

    # Step1: label+code
    step1 = set()
    for b_idx in list(bench_remaining_idx):
        br = bench_c.loc[b_idx]
        comp = str(br.get("company") or "").strip()
        if not comp:
            continue
        code_raw = str(br.get("sub_activity_code") or "")
        obj_raw = str(br.get("bench_objective") or "")

        codes = {_norm_code_numeric(t) for t in _split_multi(code_raw)} or ({_norm_code_numeric(code_raw)} if code_raw.strip() else set())
        objs = {_norm_objective(t) for t in _split_multi(obj_raw)} or ({_norm_objective(obj_raw)} if obj_raw.strip() else set())
        codes = {c for c in codes if c}
        objs = {o for o in objs if o}
        if not codes or not objs:
            continue

        rp = rag_pairs_by_company.get(comp, set())
        if any((c, o) in rp for c in codes for o in objs):
            step1.add(b_idx)
    bench_remaining_idx -= step1

    # Step2: code-only
    step2 = set()
    for b_idx in list(bench_remaining_idx):
        br = bench_c.loc[b_idx]
        comp = str(br.get("company") or "").strip()
        if not comp:
            continue
        code_raw = str(br.get("sub_activity_code") or "")
        codes = {_norm_code_numeric(t) for t in _split_multi(code_raw)} or ({_norm_code_numeric(code_raw)} if code_raw.strip() else set())
        codes = {c for c in codes if c}
        if not codes:
            continue
        rcodes = rag_codes_by_company.get(comp, set())
        if any(c in rcodes for c in codes):
            step2.add(b_idx)
    bench_remaining_idx -= step2

    # Step3: exact activity (match_key)
    rag_match_keys = set(rag_c["match_key"].astype(str).tolist())
    step3 = set()
    for b_idx in list(bench_remaining_idx):
        mk = str(bench_c.loc[b_idx, "match_key"] or "")
        if mk and mk in rag_match_keys:
            step3.add(b_idx)
    bench_remaining_idx -= step3

    # Step4: fuzzy activity
    step4 = set()
    if USE_FUZZY_MATCH and bench_remaining_idx:
        rag_candidates_step4 = set(
            r for r in rag_c.index if str(rag_c.loc[r, "activity_clean"]).strip() != ""
        )
        fm4 = fuzzy_match_bench_to_rag(
            bench_df=bench_c,
            rag_df=rag_c,
            bench_idx_candidates=set(bench_remaining_idx),
            rag_idx_candidates=rag_candidates_step4,
        )
        if not fm4.empty:
            step4 = set(fm4["bench_idx"].tolist())
    bench_remaining_idx -= step4

    total_4step = len(step1) + len(step2) + len(step3) + len(step4)
    denom_common = len(bench_c)
    denom_total = len(bench_all_y)

    rate_common_4 = (total_4step / denom_common) if denom_common else 0.0
    rate_total_4 = (total_4step / denom_total) if denom_total else 0.0

    print("\n=== 4-STEP MATCH ===")
    print(f"Step1 (label+code) matched: {len(step1)}")
    print(f"Step2 (code-only) matched:  {len(step2)}")
    print(f"Step3 (exact activity) matched: {len(step3)}")
    print(f"Step4 (fuzzy activity) matched: {len(step4)}")
    print(f"TOTAL matched (sum steps): {total_4step}")
    print(f"Benchmark activities (common companies): {denom_common} | match rate: {rate_common_4:.2%}")
    print(f"Benchmark activities (ALL companies):    {denom_total} | match rate: {rate_total_4:.2%}")


def main() -> None:
    # =========================
    # LOAD
    # =========================
    rag = pd.read_excel(RAG_xlsx, dtype=str).fillna("")
    rag.columns = [c.strip() for c in rag.columns]

    bench = pd.read_excel(BENCH_XLSX, dtype=str).fillna("")
    bench.columns = [c.strip() for c in bench.columns]

    # =========================
    # PREP RAG
    # =========================
    required_rag = {"company", "report_year", "activity", "label", "code_numeric"}
    missing_rag = required_rag - set(rag.columns)
    if missing_rag:
        raise ValueError(f"Nel RAG clean mancano colonne richieste: {sorted(missing_rag)}")

    rag["company"] = rag["company"].astype(str).str.strip()
    rag["report_year"] = rag["report_year"].astype(str).str.strip()

    rag["activity_raw"] = rag["activity"].astype(str)
    rag["activity_clean"] = rag["activity_raw"].apply(
        lambda x: clean_activity_text(x, remove_parentheses=REMOVE_PARENTHESES_IN_ACTIVITY)
    )

    rag["sub_activity_code_rag"] = rag["code_numeric"].astype(str).str.strip()
    rag["rag_label"] = rag["label"].astype(str)
    rag["rag_sub_activity_code_raw"] = rag["sub_activity_code_rag"]

    rag["match_key"] = rag.apply(
        lambda r: make_match_key(r["company"], r["report_year"], r["activity_clean"]),
        axis=1
    )

    rag = rag.drop_duplicates(subset=["match_key"]).copy()

    # =========================
    # PREP BENCH
    # =========================
    required_bench = {"company_name_full", "report_year", "activity", "Sub_activity_code", "environmental_objective"}
    missing_bench = required_bench - set(bench.columns)
    if missing_bench:
        raise ValueError(f"Nel benchmark mancano colonne richieste: {sorted(missing_bench)}")

    bench = bench[list(required_bench)].copy()
    bench["company"] = bench["company_name_full"].astype(str).str.strip()
    bench["report_year"] = bench["report_year"].astype(str).str.strip()

    bench["activity_raw"] = bench["activity"].astype(str)
    bench["activity_clean"] = bench["activity_raw"].apply(
        lambda x: clean_activity_text(x, remove_parentheses=REMOVE_PARENTHESES_IN_ACTIVITY)
    )

    bench["sub_activity_code"] = bench["Sub_activity_code"].astype(str).str.strip()
    bench["bench_objective"] = bench["environmental_objective"].astype(str).str.strip()

    bench["match_key"] = bench.apply(
        lambda r: make_match_key(r["company"], r["report_year"], r["activity_clean"]),
        axis=1
    )

    bench = bench.drop_duplicates(subset=["match_key"]).copy()
    bench_all_dedup = bench.copy()

    # =========================
    # 1) RISULTATI GLOBALI (come prima)
    # =========================
    evaluate_one_year(rag=rag, bench=bench, bench_all_dedup=bench_all_dedup, year=None)

    # =========================
    # 2) RISULTATI PER ANNO (richiesti)
    # =========================
    for y in ["2023", "2024"]:
        evaluate_one_year(rag=rag, bench=bench, bench_all_dedup=bench_all_dedup, year=y)


if __name__ == "__main__":
    main()

# python C:\Universita\TESI\esg_agent\RAG_full_bigger\5_confronto_risultati.py

'''
risultati opzione 1:
==================== ALL YEARS ====================

=== SUMMARY ===
Companies in common: 171
Matched activities: 1048
Benchmark total activities (common companies): 1184 | match rate: 88.51%
RAG total activities (common companies): 1613 | match rate: 64.97%

=== TOTAL MATCH (Label + sub_code) ===
Benchmark rows (effective, company common): 1182
Matched rows (OR on slash):               721 | match rate: 61.00%
Matched pairs sum (counts alternatives):  722
Matched rows (solo codice):               1028 | match rate: 87.64%

=== RAG MISSING VALUES (common companies) ===
Missing label (rag_label): 689
Missing activity (activity): 0
Missing subcode (code_numeric): 203

=== 4-STEP MATCH ===
Step1 (label+code) matched: 723
Step2 (code-only) matched:  316
Step3 (exact activity) matched: 52
Step4 (fuzzy activity) matched: 10
TOTAL matched (sum steps): 1101
Benchmark activities (common companies): 1184 | match rate: 92.99%
Benchmark activities (ALL companies):    1476 | match rate: 74.59%


==================== YEAR = 2023 ====================

=== SUMMARY ===
Companies in common: 136
Matched activities: 733
Benchmark total activities (common companies): 859 | match rate: 85.33%
RAG total activities (common companies): 1236 | match rate: 59.30%

=== TOTAL MATCH (Label + sub_code) ===
Benchmark rows (effective, company common): 857
Matched rows (OR on slash):               486 | match rate: 56.71%
Matched pairs sum (counts alternatives):  487
Matched rows (solo codice):               726 | match rate: 85.41%

=== RAG MISSING VALUES (common companies) ===
Missing label (rag_label): 579
Missing activity (activity): 0
Missing subcode (code_numeric): 177

=== 4-STEP MATCH ===
Step1 (label+code) matched: 488
Step2 (code-only) matched:  247
Step3 (exact activity) matched: 39
Step4 (fuzzy activity) matched: 9
TOTAL matched (sum steps): 783
Benchmark activities (common companies): 859 | match rate: 91.15%
Benchmark activities (ALL companies):    1120 | match rate: 69.91%


==================== YEAR = 2024 ====================

=== SUMMARY ===
Companies in common: 30
Matched activities: 262
Benchmark total activities (common companies): 268 | match rate: 97.76%
RAG total activities (common companies): 290 | match rate: 90.34%

=== TOTAL MATCH (Label + sub_code) ===
Benchmark rows (effective, company common): 268
Matched rows (OR on slash):               195 | match rate: 72.76%
Matched pairs sum (counts alternatives):  195
Matched rows (solo codice):               251 | match rate: 94.36%

=== RAG MISSING VALUES (common companies) ===
Missing label (rag_label): 71
Missing activity (activity): 0
Missing subcode (code_numeric): 6

=== 4-STEP MATCH ===
Step1 (label+code) matched: 195
Step2 (code-only) matched:  58
Step3 (exact activity) matched: 11
Step4 (fuzzy activity) matched: 1
TOTAL matched (sum steps): 265
Benchmark activities (common companies): 268 | match rate: 98.88%
Benchmark activities (ALL companies):    307 | match rate: 86.32%

risultati opzione 2:
==================== ALL YEARS ====================

=== SUMMARY ===
Companies in common: 216
Matched activities: 825
Benchmark total activities (common companies): 1471 | match rate: 56.08%
RAG total activities (common companies): 1327 | match rate: 62.17%

=== TOTAL MATCH (Label + sub_code) ===
Benchmark rows (effective, company common): 1467
Matched rows (OR on slash):               521 | match rate: 35.51%
Matched pairs sum (counts alternatives):  521
Matched rows (solo codice):               808 | match rate: 55.42%

=== RAG MISSING VALUES (common companies) ===
Missing label (rag_label): 666
Missing activity (activity): 69
Missing subcode (code_numeric): 210

=== 4-STEP MATCH ===
Step1 (label+code) matched: 521
Step2 (code-only) matched:  294
Step3 (exact activity) matched: 56
Step4 (fuzzy activity) matched: 12
TOTAL matched (sum steps): 883
Benchmark activities (common companies): 1471 | match rate: 60.03%
Benchmark activities (ALL companies):    1476 | match rate: 59.82%


==================== YEAR = 2023 ====================

=== SUMMARY ===
Companies in common: 180
Matched activities: 583
Benchmark total activities (common companies): 1120 | match rate: 52.05%
RAG total activities (common companies): 1019 | match rate: 57.21%

=== TOTAL MATCH (Label + sub_code) ===
Benchmark rows (effective, company common): 1116
Matched rows (OR on slash):               349 | match rate: 31.27%
Matched pairs sum (counts alternatives):  349
Matched rows (solo codice):               571 | match rate: 51.49%

=== RAG MISSING VALUES (common companies) ===
Missing label (rag_label): 549
Missing activity (activity): 64
Missing subcode (code_numeric): 187

=== 4-STEP MATCH ===
Step1 (label+code) matched: 349
Step2 (code-only) matched:  229
Step3 (exact activity) matched: 43
Step4 (fuzzy activity) matched: 11
TOTAL matched (sum steps): 632
Benchmark activities (common companies): 1120 | match rate: 56.43%
Benchmark activities (ALL companies):    1120 | match rate: 56.43%


==================== YEAR = 2024 ====================

=== SUMMARY ===
Companies in common: 32
Matched activities: 190
Benchmark total activities (common companies): 299 | match rate: 63.55%
RAG total activities (common companies): 241 | match rate: 78.84%

=== TOTAL MATCH (Label + sub_code) ===
Benchmark rows (effective, company common): 299
Matched rows (OR on slash):               134 | match rate: 44.82%
Matched pairs sum (counts alternatives):  134
Matched rows (solo codice):               187 | match rate: 62.96%

=== RAG MISSING VALUES (common companies) ===
Missing label (rag_label): 91
Missing activity (activity): 4
Missing subcode (code_numeric): 19

=== 4-STEP MATCH ===
Step1 (label+code) matched: 134
Step2 (code-only) matched:  53
Step3 (exact activity) matched: 11
Step4 (fuzzy activity) matched: 1
TOTAL matched (sum steps): 199
Benchmark activities (common companies): 299 | match rate: 66.56%
Benchmark activities (ALL companies):    307 | match rate: 64.82%

opzione 3: 
(--top_k 12 --w_bm25 0.5 --w_emb 0.5):
==================== ALL YEARS ====================

=== SUMMARY ===
Companies in common: 173
Matched activities: 1026
Benchmark total activities (common companies): 1197 | match rate: 85.71%
RAG total activities (common companies): 1912 | match rate: 53.66%

=== TOTAL MATCH (Label + sub_code) ===
Benchmark rows (effective, company common): 1195
Matched rows (OR on slash):               664 | match rate: 55.56%
Matched pairs sum (counts alternatives):  665
Matched rows (solo codice):               1038 | match rate: 87.52%

=== RAG MISSING VALUES (common companies) ===
Missing label (rag_label): 1007
Missing activity (activity): 0
Missing subcode (code_numeric): 286

=== 4-STEP MATCH ===
Step1 (label+code) matched: 665
Step2 (code-only) matched:  382
Step3 (exact activity) matched: 47
Step4 (fuzzy activity) matched: 5
TOTAL matched (sum steps): 1099
Benchmark activities (common companies): 1197 | match rate: 91.81%
Benchmark activities (ALL companies):    1476 | match rate: 74.46%


==================== YEAR = 2023 ====================

=== SUMMARY ===
Companies in common: 138
Matched activities: 709
Benchmark total activities (common companies): 874 | match rate: 81.12%
RAG total activities (common companies): 1454 | match rate: 48.76%

=== TOTAL MATCH (Label + sub_code) ===
Benchmark rows (effective, company common): 872
Matched rows (OR on slash):               447 | match rate: 51.26%
Matched pairs sum (counts alternatives):  448
Matched rows (solo codice):               732 | match rate: 84.62%

=== RAG MISSING VALUES (common companies) ===
Missing label (rag_label): 808
Missing activity (activity): 0
Missing subcode (code_numeric): 224

=== 4-STEP MATCH ===
Step1 (label+code) matched: 448
Step2 (code-only) matched:  291
Step3 (exact activity) matched: 35
Step4 (fuzzy activity) matched: 5
TOTAL matched (sum steps): 779
Benchmark activities (common companies): 874 | match rate: 89.13%
Benchmark activities (ALL companies):    1120 | match rate: 69.55%


==================== YEAR = 2024 ====================

=== SUMMARY ===
Companies in common: 31
Matched activities: 265
Benchmark total activities (common companies): 271 | match rate: 97.79%
RAG total activities (common companies): 355 | match rate: 74.65%

=== TOTAL MATCH (Label + sub_code) ===
Benchmark rows (effective, company common): 271
Matched rows (OR on slash):               167 | match rate: 61.62%
Matched pairs sum (counts alternatives):  167
Matched rows (solo codice):               256 | match rate: 95.17%

=== RAG MISSING VALUES (common companies) ===
Missing label (rag_label): 162
Missing activity (activity): 0
Missing subcode (code_numeric): 25

=== 4-STEP MATCH ===
Step1 (label+code) matched: 167
Step2 (code-only) matched:  91
Step3 (exact activity) matched: 10
Step4 (fuzzy activity) matched: 0
TOTAL matched (sum steps): 268
Benchmark activities (common companies): 271 | match rate: 98.89%
Benchmark activities (ALL companies):    307 | match rate: 87.30%

secondo test con --top_k 8 --w_bm25 0.65 --w_emb 0.35:

==================== ALL YEARS ====================

=== SUMMARY ===
Companies in common: 173
Matched activities: 1018
Benchmark total activities (common companies): 1196 | match rate: 85.12%
RAG total activities (common companies): 1810 | match rate: 56.24%

=== TOTAL MATCH (Label + sub_code) ===
Benchmark rows (effective, company common): 1194
Matched rows (OR on slash):               669 | match rate: 56.03%
Matched pairs sum (counts alternatives):  670
Matched rows (solo codice):               1018 | match rate: 85.91%

=== RAG MISSING VALUES (common companies) ===
Missing label (rag_label): 924
Missing activity (activity): 2
Missing subcode (code_numeric): 307

=== 4-STEP MATCH ===
Step1 (label+code) matched: 670
Step2 (code-only) matched:  357
Step3 (exact activity) matched: 51
Step4 (fuzzy activity) matched: 9
TOTAL matched (sum steps): 1087
Benchmark activities (common companies): 1196 | match rate: 90.89%
Benchmark activities (ALL companies):    1476 | match rate: 73.64%


==================== YEAR = 2023 ====================

=== SUMMARY ===
Companies in common: 138
Matched activities: 711
Benchmark total activities (common companies): 873 | match rate: 81.44%
RAG total activities (common companies): 1379 | match rate: 51.56%

=== TOTAL MATCH (Label + sub_code) ===
Benchmark rows (effective, company common): 871
Matched rows (OR on slash):               445 | match rate: 51.09%
Matched pairs sum (counts alternatives):  446
Matched rows (solo codice):               723 | match rate: 83.68%

=== RAG MISSING VALUES (common companies) ===
Missing label (rag_label): 748
Missing activity (activity): 2
Missing subcode (code_numeric): 249

=== 4-STEP MATCH ===
Step1 (label+code) matched: 446
Step2 (code-only) matched:  284
Step3 (exact activity) matched: 33
Step4 (fuzzy activity) matched: 8
TOTAL matched (sum steps): 771
Benchmark activities (common companies): 873 | match rate: 88.32%
Benchmark activities (ALL companies):    1120 | match rate: 68.84%


==================== YEAR = 2024 ====================

=== SUMMARY ===
Companies in common: 31
Matched activities: 259
Benchmark total activities (common companies): 271 | match rate: 95.57%
RAG total activities (common companies): 340 | match rate: 76.18%

=== TOTAL MATCH (Label + sub_code) ===
Benchmark rows (effective, company common): 271
Matched rows (OR on slash):               182 | match rate: 67.16%
Matched pairs sum (counts alternatives):  182
Matched rows (solo codice):               253 | match rate: 94.05%

=== RAG MISSING VALUES (common companies) ===
Missing label (rag_label): 132
Missing activity (activity): 0
Missing subcode (code_numeric): 14

=== 4-STEP MATCH ===
Step1 (label+code) matched: 182
Step2 (code-only) matched:  73
Step3 (exact activity) matched: 12
Step4 (fuzzy activity) matched: 1
TOTAL matched (sum steps): 268
Benchmark activities (common companies): 271 | match rate: 98.89%
Benchmark activities (ALL companies):    307 | match rate: 87.30%

'''