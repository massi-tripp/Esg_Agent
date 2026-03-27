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
RAG_xlsx = Path(r"C:\Universita\TESI\esg_agent\RAG_full\rag_out\optionB\activities_extracted_clean.xlsx")
BENCH_XLSX = Path(r"C:\Universita\TESI\esg_agent\RAG_full\documentazione_rag.xlsx")

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
'''
risultati opzione 1: Drop 2023 duplicates covered by 2024: 37
==================== ALL YEARS ====================

=== SUMMARY ===
Companies in common: 171
Matched activities: 1032
Benchmark total activities (common companies): 1193 | match rate: 86.50%
RAG total activities (common companies): 1400 | match rate: 73.71%

=== TOTAL MATCH (Label + sub_code) ===
Benchmark rows (effective, company common): 1190
Matched rows (OR on slash):               832 | match rate: 69.92%
Matched pairs sum (counts alternatives):  834
Matched rows (solo codice):               1024 | match rate: 86.71%

=== RAG MISSING VALUES (common companies) ===
Missing label (rag_label): 337
Missing activity (activity): 0
Missing subcode (code_numeric): 90

=== 4-STEP MATCH ===
Step1 (label+code) matched: 834
Step2 (code-only) matched:  201
Step3 (exact activity) matched: 52
Step4 (fuzzy activity) matched: 14
TOTAL matched (sum steps): 1101
Benchmark activities (common companies): 1193 | match rate: 92.29%
Benchmark activities (ALL companies):    1476 | match rate: 74.59%


==================== YEAR = 2023 ====================

=== SUMMARY ===
Companies in common: 136
Matched activities: 723
Benchmark total activities (common companies): 870 | match rate: 83.10%
RAG total activities (common companies): 1053 | match rate: 68.66%

=== TOTAL MATCH (Label + sub_code) ===
Benchmark rows (effective, company common): 867
Matched rows (OR on slash):               555 | match rate: 64.01%
Matched pairs sum (counts alternatives):  557
Matched rows (solo codice):               720 | match rate: 83.72%

=== RAG MISSING VALUES (common companies) ===
Missing label (rag_label): 302
Missing activity (activity): 0
Missing subcode (code_numeric): 89

=== 4-STEP MATCH ===
Step1 (label+code) matched: 557
Step2 (code-only) matched:  172
Step3 (exact activity) matched: 39
Step4 (fuzzy activity) matched: 13
TOTAL matched (sum steps): 781
Benchmark activities (common companies): 870 | match rate: 89.77%
Benchmark activities (ALL companies):    1120 | match rate: 69.73%


==================== YEAR = 2024 ====================

=== SUMMARY ===
Companies in common: 31
Matched activities: 261
Benchmark total activities (common companies): 275 | match rate: 94.91%
RAG total activities (common companies): 288 | match rate: 90.62%

=== TOTAL MATCH (Label + sub_code) ===
Benchmark rows (effective, company common): 275
Matched rows (OR on slash):               231 | match rate: 84.00%
Matched pairs sum (counts alternatives):  231
Matched rows (solo codice):               258 | match rate: 94.51%

=== RAG MISSING VALUES (common companies) ===
Missing label (rag_label): 35
Missing activity (activity): 0
Missing subcode (code_numeric): 1

=== 4-STEP MATCH ===
Step1 (label+code) matched: 231
Step2 (code-only) matched:  29
Step3 (exact activity) matched: 11
Step4 (fuzzy activity) matched: 1
TOTAL matched (sum steps): 272
Benchmark activities (common companies): 275 | match rate: 98.91%
Benchmark activities (ALL companies):    311 | match rate: 87.46%

risultati opzione 2: Drop 2023 duplicates covered by 2024: 17
==================== ALL YEARS ====================

=== SUMMARY ===
Companies in common: 180
Matched activities: 831
Benchmark total activities (common companies): 1225 | match rate: 67.84%
RAG total activities (common companies): 1346 | match rate: 61.74%

=== TOTAL MATCH (Label + sub_code) ===
Benchmark rows (effective, company common): 1222
Matched rows (OR on slash):               639 | match rate: 52.29%
Matched pairs sum (counts alternatives):  639
Matched rows (solo codice):               843 | match rate: 69.50%

=== RAG MISSING VALUES (common companies) ===
Missing label (rag_label): 548
Missing activity (activity): 18
Missing subcode (code_numeric): 182

=== 4-STEP MATCH ===
Step1 (label+code) matched: 639
Step2 (code-only) matched:  212
Step3 (exact activity) matched: 53
Step4 (fuzzy activity) matched: 9
TOTAL matched (sum steps): 913
Benchmark activities (common companies): 1225 | match rate: 74.53%
Benchmark activities (ALL companies):    1476 | match rate: 61.86%


==================== YEAR = 2023 ====================

=== SUMMARY ===
Companies in common: 145
Matched activities: 545
Benchmark total activities (common companies): 902 | match rate: 60.42%
RAG total activities (common companies): 1003 | match rate: 54.34%

=== TOTAL MATCH (Label + sub_code) ===
Benchmark rows (effective, company common): 899
Matched rows (OR on slash):               379 | match rate: 42.16%
Matched pairs sum (counts alternatives):  379
Matched rows (solo codice):               551 | match rate: 61.77%

=== RAG MISSING VALUES (common companies) ===
Missing label (rag_label): 486
Missing activity (activity): 17
Missing subcode (code_numeric): 172

=== 4-STEP MATCH ===
Step1 (label+code) matched: 379
Step2 (code-only) matched:  178
Step3 (exact activity) matched: 41
Step4 (fuzzy activity) matched: 8
TOTAL matched (sum steps): 606
Benchmark activities (common companies): 902 | match rate: 67.18%
Benchmark activities (ALL companies):    1120 | match rate: 54.11%


==================== YEAR = 2024 ====================

=== SUMMARY ===
Companies in common: 31
Matched activities: 238
Benchmark total activities (common companies): 275 | match rate: 86.55%
RAG total activities (common companies): 288 | match rate: 82.64%

=== TOTAL MATCH (Label + sub_code) ===
Benchmark rows (effective, company common): 275
Matched rows (OR on slash):               214 | match rate: 77.82%
Matched pairs sum (counts alternatives):  214
Matched rows (solo codice):               246 | match rate: 90.11%

=== RAG MISSING VALUES (common companies) ===
Missing label (rag_label): 58
Missing activity (activity): 0
Missing subcode (code_numeric): 6

=== 4-STEP MATCH ===
Step1 (label+code) matched: 214
Step2 (code-only) matched:  34
Step3 (exact activity) matched: 10
Step4 (fuzzy activity) matched: 1
TOTAL matched (sum steps): 259
Benchmark activities (common companies): 275 | match rate: 94.18%
Benchmark activities (ALL companies):    311 | match rate: 83.28%

opzione 3: 
(--top_k 12 --w_bm25 0.5 --w_emb 0.5): Drop 2023 duplicates covered by 2024: 29

==================== ALL YEARS ====================

=== SUMMARY ===
Companies in common: 174
Matched activities: 958
Benchmark total activities (common companies): 1204 | match rate: 79.57%
RAG total activities (common companies): 1783 | match rate: 53.73%

=== TOTAL MATCH (Label + sub_code) ===
Benchmark rows (effective, company common): 1201
Matched rows (OR on slash):               740 | match rate: 61.62%
Matched pairs sum (counts alternatives):  741
Matched rows (solo codice):               975 | match rate: 81.80%

=== RAG MISSING VALUES (common companies) ===
Missing label (rag_label): 828
Missing activity (activity): 1
Missing subcode (code_numeric): 424

=== 4-STEP MATCH ===
Step1 (label+code) matched: 741
Step2 (code-only) matched:  244
Step3 (exact activity) matched: 47
Step4 (fuzzy activity) matched: 15
TOTAL matched (sum steps): 1047
Benchmark activities (common companies): 1204 | match rate: 86.96%
Benchmark activities (ALL companies):    1476 | match rate: 70.93%


==================== YEAR = 2023 ====================

=== SUMMARY ===
Companies in common: 139
Matched activities: 646
Benchmark total activities (common companies): 881 | match rate: 73.33%
RAG total activities (common companies): 1370 | match rate: 47.15%

=== TOTAL MATCH (Label + sub_code) ===
Benchmark rows (effective, company common): 878
Matched rows (OR on slash):               470 | match rate: 53.53%
Matched pairs sum (counts alternatives):  471
Matched rows (solo codice):               666 | match rate: 76.46%

=== RAG MISSING VALUES (common companies) ===
Missing label (rag_label): 716
Missing activity (activity): 1
Missing subcode (code_numeric): 373

=== 4-STEP MATCH ===
Step1 (label+code) matched: 471
Step2 (code-only) matched:  203
Step3 (exact activity) matched: 34
Step4 (fuzzy activity) matched: 14
TOTAL matched (sum steps): 722
Benchmark activities (common companies): 881 | match rate: 81.95%
Benchmark activities (ALL companies):    1120 | match rate: 64.46%


==================== YEAR = 2024 ====================

=== SUMMARY ===
Companies in common: 31
Matched activities: 259
Benchmark total activities (common companies): 275 | match rate: 94.18%
RAG total activities (common companies): 338 | match rate: 76.63%

=== TOTAL MATCH (Label + sub_code) ===
Benchmark rows (effective, company common): 275
Matched rows (OR on slash):               219 | match rate: 79.64%
Matched pairs sum (counts alternatives):  219
Matched rows (solo codice):               257 | match rate: 94.14%

=== RAG MISSING VALUES (common companies) ===
Missing label (rag_label): 99
Missing activity (activity): 0
Missing subcode (code_numeric): 39

=== 4-STEP MATCH ===
Step1 (label+code) matched: 219
Step2 (code-only) matched:  40
Step3 (exact activity) matched: 12
Step4 (fuzzy activity) matched: 1
TOTAL matched (sum steps): 272
Benchmark activities (common companies): 275 | match rate: 98.91%
Benchmark activities (ALL companies):    311 | match rate: 87.46%

secondo test con --top_k 8 --w_bm25 0.65 --w_emb 0.35: Drop 2023 duplicates covered by 2024: 25

==================== ALL YEARS ====================

=== SUMMARY ===
Companies in common: 172
Matched activities: 944
Benchmark total activities (common companies): 1196 | match rate: 78.93%
RAG total activities (common companies): 1670 | match rate: 56.53%

=== TOTAL MATCH (Label + sub_code) ===
Benchmark rows (effective, company common): 1193
Matched rows (OR on slash):               740 | match rate: 62.03%
Matched pairs sum (counts alternatives):  741
Matched rows (solo codice):               962 | match rate: 81.25%

=== RAG MISSING VALUES (common companies) ===
Missing label (rag_label): 716
Missing activity (activity): 2
Missing subcode (code_numeric): 308

=== 4-STEP MATCH ===
Step1 (label+code) matched: 741
Step2 (code-only) matched:  230
Step3 (exact activity) matched: 46
Step4 (fuzzy activity) matched: 18
TOTAL matched (sum steps): 1035
Benchmark activities (common companies): 1196 | match rate: 86.54%
Benchmark activities (ALL companies):    1476 | match rate: 70.12%


==================== YEAR = 2023 ====================

=== SUMMARY ===
Companies in common: 136
Matched activities: 638
Benchmark total activities (common companies): 871 | match rate: 73.25%
RAG total activities (common companies): 1263 | match rate: 50.51%

=== TOTAL MATCH (Label + sub_code) ===
Benchmark rows (effective, company common): 868
Matched rows (OR on slash):               481 | match rate: 55.41%
Matched pairs sum (counts alternatives):  482
Matched rows (solo codice):               659 | match rate: 76.54%

=== RAG MISSING VALUES (common companies) ===
Missing label (rag_label): 602
Missing activity (activity): 2
Missing subcode (code_numeric): 258

=== 4-STEP MATCH ===
Step1 (label+code) matched: 482
Step2 (code-only) matched:  184
Step3 (exact activity) matched: 33
Step4 (fuzzy activity) matched: 17
TOTAL matched (sum steps): 716
Benchmark activities (common companies): 871 | match rate: 82.20%
Benchmark activities (ALL companies):    1120 | match rate: 63.93%


==================== YEAR = 2024 ====================

=== SUMMARY ===
Companies in common: 31
Matched activities: 260
Benchmark total activities (common companies): 275 | match rate: 94.55%
RAG total activities (common companies): 327 | match rate: 79.51%

=== TOTAL MATCH (Label + sub_code) ===
Benchmark rows (effective, company common): 275
Matched rows (OR on slash):               216 | match rate: 78.55%
Matched pairs sum (counts alternatives):  216
Matched rows (solo codice):               257 | match rate: 94.14%

=== RAG MISSING VALUES (common companies) ===
Missing label (rag_label): 86
Missing activity (activity): 0
Missing subcode (code_numeric): 25

=== 4-STEP MATCH ===
Step1 (label+code) matched: 216
Step2 (code-only) matched:  43
Step3 (exact activity) matched: 12
Step4 (fuzzy activity) matched: 1
TOTAL matched (sum steps): 272
Benchmark activities (common companies): 275 | match rate: 98.91%
Benchmark activities (ALL companies):    311 | match rate: 87.46%
'''
