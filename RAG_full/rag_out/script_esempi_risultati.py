from __future__ import annotations

import re
import unicodedata
from collections import defaultdict
from pathlib import Path
from typing import Optional, Dict, List, Tuple, Callable, Set

import pandas as pd

# ============================================================
# CONFIG
# ============================================================
# FILE DELLA CONFIGURAZIONE FINALE SCELTA:
# Option 1 + restrictive reduction
RAG_FILE = Path(r"C:\Universita\TESI\esg_agent\RAG_full\rag_out\activities_extracted_clean.csv")
BENCH_FILE = Path(r"C:\Universita\TESI\esg_agent\RAG_full\documentazione_rag.xlsx")

# OUTPUT
OUT_DIR = Path(r"C:\Universita\TESI\esg_agent\RAG_full\rag_out\diagnostics_option1_restrictive")

# Se None, la company del case study viene scelta automaticamente.
# Se vuoi forzarne una, metti il nome esatto, es:
# CASE_STUDY_COMPANY = "AB VOLVO"
CASE_STUDY_COMPANY: Optional[str] = None

REMOVE_PARENTHESES_IN_ACTIVITY = True
MATCH_ON_YEAR = False

# Fuzzy settings: allineati al matching ufficiale
USE_FUZZY_MATCH = True
FUZZY_THRESHOLD = 92
FUZZY_MIN_LEN = 8
FUZZY_ONE_TO_ONE = True

# Quanti esempi salvare nelle varie sezioni
MAX_CASE_MATCHED_ROWS = 8
MAX_CASE_UNMATCHED_BENCH = 5
MAX_CASE_EXTRA_RAG = 5
MAX_TAXONOMY_EXAMPLES_PER_CATEGORY = 3

# ============================================================
# NORMALIZZAZIONE
# ============================================================
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+", flags=re.IGNORECASE)
_MULTI_SPACE_RE = re.compile(r"\s+")
_PARENS_RE = re.compile(r"\([^)]*\)")
_SPLIT_RE = re.compile(r"\s*(?:/|,|;|\||\n)\s*")
_CODE_NUM_RE = re.compile(r"\d+(?:\.\d+)?")


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


def _group_key(company: str, year: str) -> Tuple[str, ...]:
    if MATCH_ON_YEAR:
        return (company, year)
    return (company,)


# ============================================================
# SIMILARITY
# ============================================================
def _get_similarity() -> Callable[[str, str], float]:
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


# ============================================================
# IO HELPERS
# ============================================================
def read_tabular(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"File non trovato: {path}")

    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path, dtype=str).fillna("")
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path, dtype=str).fillna("")
    raise ValueError(f"Formato file non supportato: {path}")


def slugify(text: str) -> str:
    s = strip_accents(str(text))
    s = re.sub(r"[^A-Za-z0-9]+", "_", s).strip("_")
    return s[:120] if s else "company"


def md_escape(x: object) -> str:
    s = "" if x is None else str(x)
    s = s.replace("\n", " ").replace("\r", " ").replace("|", "/")
    return s.strip()


def df_to_markdown(df: pd.DataFrame) -> str:
    if df.empty:
        return "_No rows._"

    cols = list(df.columns)
    lines = []
    lines.append("| " + " | ".join(cols) + " |")
    lines.append("| " + " | ".join(["---"] * len(cols)) + " |")
    for _, row in df.iterrows():
        vals = [md_escape(row[c]) for c in cols]
        lines.append("| " + " | ".join(vals) + " |")
    return "\n".join(lines)


# ============================================================
# PREP DATA
# ============================================================
def prepare_rag(raw_rag: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    required_rag = ["company", "report_year", "activity", "label", "code_numeric"]
    missing = [c for c in required_rag if c not in raw_rag.columns]
    if missing:
        raise ValueError(f"Nel RAG mancano colonne richieste: {missing}")

    rag = raw_rag.copy()
    rag.columns = [c.strip() for c in rag.columns]

    rag["company"] = rag["company"].astype(str).str.strip()
    rag["report_year"] = rag["report_year"].astype(str).str.strip()

    rag["activity_raw"] = rag["activity"].astype(str)
    rag["activity_clean"] = rag["activity_raw"].apply(
        lambda x: clean_activity_text(x, remove_parentheses=REMOVE_PARENTHESES_IN_ACTIVITY)
    )

    rag["rag_label"] = rag["label"].astype(str)
    rag["sub_activity_code_rag"] = rag["code_numeric"].astype(str).str.strip()

    rag["match_key"] = rag.apply(
        lambda r: make_match_key(r["company"], r["report_year"], r["activity_clean"]),
        axis=1,
    )

    rag["raw_row_id"] = range(len(rag))

    rag_eval = rag.drop_duplicates(subset=["match_key"]).copy()
    rag_eval["rag_idx"] = rag_eval.index

    return rag, rag_eval


def prepare_benchmark(raw_bench: pd.DataFrame) -> pd.DataFrame:
    required_bench = [
        "company_name_full",
        "report_year",
        "activity",
        "Sub_activity_code",
        "environmental_objective",
    ]
    missing = [c for c in required_bench if c not in raw_bench.columns]
    if missing:
        raise ValueError(f"Nel benchmark mancano colonne richieste: {missing}")

    bench = raw_bench.copy()
    bench.columns = [c.strip() for c in bench.columns]

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
        axis=1,
    )

    # allineato alla logica del matching ufficiale
    bench = bench.drop_duplicates(subset=["match_key"]).copy()
    bench["bench_idx"] = bench.index
    return bench


# ============================================================
# LOOKUPS
# ============================================================
def build_rag_lookups(rag_c: pd.DataFrame):
    pair_to_rag: Dict[str, Dict[Tuple[str, str], List[int]]] = defaultdict(lambda: defaultdict(list))
    code_to_rag: Dict[str, Dict[str, List[int]]] = defaultdict(lambda: defaultdict(list))
    mk_to_rag: Dict[str, List[int]] = defaultdict(list)
    rag_by_group: Dict[Tuple[str, ...], List[Tuple[int, str]]] = defaultdict(list)

    for r_idx, r in rag_c.iterrows():
        comp = str(r.get("company", "") or "").strip()
        yr = str(r.get("report_year", "") or "").strip()
        if not comp:
            continue

        # per step1
        code_raw = str(r.get("sub_activity_code_rag", "") or "")
        obj_raw = str(r.get("rag_label", "") or "")

        codes = [_norm_code_numeric(t) for t in _split_multi(code_raw)] or (
            [_norm_code_numeric(code_raw)] if code_raw.strip() else []
        )
        objs = [_norm_objective(t) for t in _split_multi(obj_raw)] or (
            [_norm_objective(obj_raw)] if obj_raw.strip() else []
        )

        codes = [c for c in codes if c]
        objs = [o for o in objs if o]

        for c in codes:
            code_to_rag[comp][c].append(r_idx)
            for o in objs:
                pair_to_rag[comp][(c, o)].append(r_idx)

        # per step3
        mk = str(r.get("match_key", "") or "").strip()
        if mk:
            mk_to_rag[mk].append(r_idx)

        # per step4
        act = str(r.get("activity_clean", "") or "").strip()
        if act and len(act) >= FUZZY_MIN_LEN:
            gk = _group_key(comp, yr)
            rag_by_group[gk].append((r_idx, act))

    # sorting deterministico
    for comp in pair_to_rag:
        for k in pair_to_rag[comp]:
            pair_to_rag[comp][k] = sorted(set(pair_to_rag[comp][k]))
    for comp in code_to_rag:
        for k in code_to_rag[comp]:
            code_to_rag[comp][k] = sorted(set(code_to_rag[comp][k]))
    for k in mk_to_rag:
        mk_to_rag[k] = sorted(set(mk_to_rag[k]))

    return pair_to_rag, code_to_rag, mk_to_rag, rag_by_group


def fuzzy_match_bench_to_rag(
    bench_df: pd.DataFrame,
    rag_df: pd.DataFrame,
    bench_idx_candidates: Set[int],
    rag_idx_candidates: Set[int],
) -> pd.DataFrame:
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
                {
                    "bench_idx": b_idx,
                    "rag_idx": best_r_idx,
                    "fuzzy_score": float(best_score),
                    "match_type": "fuzzy",
                }
            )
            if FUZZY_ONE_TO_ONE:
                used_rag.add(best_r_idx)

    return pd.DataFrame(rows)


# ============================================================
# MATCHING CORE
# ============================================================
def build_all_year_assignments(rag_eval: pd.DataFrame, bench_eval: pd.DataFrame):
    rag_companies = set(rag_eval["company"].unique())
    bench_companies = set(bench_eval["company"].unique())
    common_companies = sorted(rag_companies & bench_companies)

    rag_c = rag_eval[rag_eval["company"].isin(common_companies)].copy()
    bench_c = bench_eval[bench_eval["company"].isin(common_companies)].copy()

    pair_to_rag, code_to_rag, mk_to_rag, _ = build_rag_lookups(rag_c)

    assignments: List[Dict[str, object]] = []
    assigned_bench: Set[int] = set()

    # -------------------------
    # STEP 1: label + code
    # -------------------------
    for b_idx, br in bench_c.iterrows():
        comp = str(br.get("company", "") or "").strip()
        code_raw = str(br.get("sub_activity_code", "") or "")
        obj_raw = str(br.get("bench_objective", "") or "")

        codes = {_norm_code_numeric(t) for t in _split_multi(code_raw)} or (
            {_norm_code_numeric(code_raw)} if code_raw.strip() else set()
        )
        objs = {_norm_objective(t) for t in _split_multi(obj_raw)} or (
            {_norm_objective(obj_raw)} if obj_raw.strip() else set()
        )

        codes = {c for c in codes if c}
        objs = {o for o in objs if o}
        if not comp or not codes or not objs:
            continue

        chosen_rag = None
        for c in sorted(codes):
            for o in sorted(objs):
                candidates = pair_to_rag.get(comp, {}).get((c, o), [])
                if candidates:
                    chosen_rag = candidates[0]
                    break
            if chosen_rag is not None:
                break

        if chosen_rag is not None:
            assignments.append(
                {
                    "bench_idx": b_idx,
                    "rag_idx": chosen_rag,
                    "matched_step": "Step1",
                    "why": "Same normalized sub-activity code and objective label.",
                }
            )
            assigned_bench.add(b_idx)

    remaining_bench = set(bench_c.index) - assigned_bench

    # -------------------------
    # STEP 2: code only
    # -------------------------
    for b_idx in sorted(remaining_bench):
        br = bench_c.loc[b_idx]
        comp = str(br.get("company", "") or "").strip()
        code_raw = str(br.get("sub_activity_code", "") or "")

        codes = {_norm_code_numeric(t) for t in _split_multi(code_raw)} or (
            {_norm_code_numeric(code_raw)} if code_raw.strip() else set()
        )
        codes = {c for c in codes if c}
        if not comp or not codes:
            continue

        chosen_rag = None
        for c in sorted(codes):
            candidates = code_to_rag.get(comp, {}).get(c, [])
            if candidates:
                chosen_rag = candidates[0]
                break

        if chosen_rag is not None:
            assignments.append(
                {
                    "bench_idx": b_idx,
                    "rag_idx": chosen_rag,
                    "matched_step": "Step2",
                    "why": "Same normalized sub-activity code, but no label+code match.",
                }
            )
            assigned_bench.add(b_idx)

    remaining_bench = set(bench_c.index) - assigned_bench

    # -------------------------
    # STEP 3: exact activity
    # -------------------------
    for b_idx in sorted(remaining_bench):
        br = bench_c.loc[b_idx]
        mk = str(br.get("match_key", "") or "").strip()
        if not mk:
            continue

        candidates = mk_to_rag.get(mk, [])
        if candidates:
            assignments.append(
                {
                    "bench_idx": b_idx,
                    "rag_idx": candidates[0],
                    "matched_step": "Step3",
                    "why": "Same normalized activity text, but no structured code match.",
                }
            )
            assigned_bench.add(b_idx)

    remaining_bench = set(bench_c.index) - assigned_bench

    # -------------------------
    # STEP 4: fuzzy activity
    # -------------------------
    if USE_FUZZY_MATCH and remaining_bench:
        rag_candidates = set(
            r for r in rag_c.index if str(rag_c.loc[r, "activity_clean"]).strip() != ""
        )
        fm = fuzzy_match_bench_to_rag(
            bench_df=bench_c,
            rag_df=rag_c,
            bench_idx_candidates=set(remaining_bench),
            rag_idx_candidates=rag_candidates,
        )

        if not fm.empty:
            for _, row in fm.iterrows():
                assignments.append(
                    {
                        "bench_idx": int(row["bench_idx"]),
                        "rag_idx": int(row["rag_idx"]),
                        "matched_step": "Step4",
                        "why": f"Fuzzy activity match (score={row['fuzzy_score']:.1f}) after failing Steps 1--3.",
                    }
                )
                assigned_bench.add(int(row["bench_idx"]))

    assignments_df = pd.DataFrame(assignments)
    if assignments_df.empty:
        raise RuntimeError("Nessun assignment prodotto. Controlla i file di input.")

    # arricchimento leggibile
    bench_info = bench_c[
        ["company", "report_year", "activity_raw", "sub_activity_code", "bench_objective", "activity_clean"]
    ].copy()
    rag_info = rag_c[
        ["company", "report_year", "activity_raw", "sub_activity_code_rag", "rag_label", "activity_clean"]
    ].copy()

    assignments_df = assignments_df.merge(
        bench_info,
        left_on="bench_idx",
        right_index=True,
        how="left",
        suffixes=("", "_bench"),
    )

    assignments_df = assignments_df.merge(
        rag_info,
        left_on="rag_idx",
        right_index=True,
        how="left",
        suffixes=("_bench", "_rag"),
    )

    assignments_df = assignments_df.rename(
        columns={
            "company_bench": "company",
            "report_year_bench": "report_year",
            "activity_raw_bench": "benchmark_activity",
            "sub_activity_code": "benchmark_code",
            "bench_objective": "benchmark_objective",
            "activity_clean_bench": "benchmark_activity_clean",
            "activity_raw_rag": "rag_activity",
            "sub_activity_code_rag": "rag_code",
            "rag_label": "rag_label",
            "activity_clean_rag": "rag_activity_clean",
        }
    )

    matched_bench_idx = set(assignments_df["bench_idx"].tolist())
    unmatched_bench = bench_c.loc[sorted(set(bench_c.index) - matched_bench_idx)].copy()

    return common_companies, rag_c, bench_c, assignments_df, unmatched_bench


# ============================================================
# STEP EXAMPLES
# ============================================================
def find_best_near_miss_for_unmatched_bench(bench_row: pd.Series, rag_c: pd.DataFrame) -> Tuple[Optional[int], float]:
    comp = str(bench_row.get("company", "") or "").strip()
    b_act = str(bench_row.get("activity_clean", "") or "").strip()
    if not comp or not b_act:
        return None, -1.0

    rag_same_company = rag_c[rag_c["company"] == comp]
    if rag_same_company.empty:
        return None, -1.0

    best_idx = None
    best_score = -1.0
    for r_idx, rr in rag_same_company.iterrows():
        r_act = str(rr.get("activity_clean", "") or "").strip()
        if not r_act:
            continue
        score = SIM(b_act, r_act)
        if score > best_score:
            best_score = score
            best_idx = r_idx
    return best_idx, best_score


def build_step_examples(assignments_df: pd.DataFrame, unmatched_bench: pd.DataFrame, rag_c: pd.DataFrame) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []

    for step in ["Step1", "Step2", "Step3", "Step4"]:
        sub = assignments_df[assignments_df["matched_step"] == step].copy()
        if sub.empty:
            continue

        # scelta semplice ma leggibile: primo esempio ordinato
        sub = sub.sort_values(["company", "report_year", "benchmark_code", "benchmark_activity"])
        r = sub.iloc[0]

        rows.append(
            {
                "example_type": step,
                "company": r["company"],
                "year": r["report_year"],
                "benchmark_activity": r["benchmark_activity"],
                "benchmark_code": r["benchmark_code"],
                "benchmark_objective": r["benchmark_objective"],
                "rag_activity": r["rag_activity"],
                "rag_code_numeric": r["rag_code"],
                "rag_label": r["rag_label"],
                "matched_step": r["matched_step"],
                "note": r["why"],
            }
        )

    # eventuale unmatched
    if not unmatched_bench.empty:
        ub = unmatched_bench.sort_values(["company", "report_year", "sub_activity_code", "activity_raw"]).iloc[0]
        near_rag_idx, near_score = find_best_near_miss_for_unmatched_bench(ub, rag_c)

        if near_rag_idx is not None:
            rr = rag_c.loc[near_rag_idx]
            rag_activity = rr.get("activity_raw", "")
            rag_code = rr.get("sub_activity_code_rag", "")
            rag_label = rr.get("rag_label", "")
            note = f"No match across Steps 1--4. Best same-company activity similarity: {near_score:.1f}."
        else:
            rag_activity = ""
            rag_code = ""
            rag_label = ""
            note = "No match across Steps 1--4 and no same-company extracted activity available."

        rows.append(
            {
                "example_type": "Unmatched",
                "company": ub["company"],
                "year": ub["report_year"],
                "benchmark_activity": ub["activity_raw"],
                "benchmark_code": ub["sub_activity_code"],
                "benchmark_objective": ub["bench_objective"],
                "rag_activity": rag_activity,
                "rag_code_numeric": rag_code,
                "rag_label": rag_label,
                "matched_step": "Unmatched",
                "note": note,
            }
        )

    return pd.DataFrame(rows)


# ============================================================
# CASE STUDY COMPANY
# ============================================================
def build_company_candidates(
    common_companies: List[str],
    rag_c: pd.DataFrame,
    bench_c: pd.DataFrame,
    assignments_df: pd.DataFrame,
    unmatched_bench: pd.DataFrame,
) -> pd.DataFrame:
    rows = []

    for company in common_companies:
        a = assignments_df[assignments_df["company"] == company]
        b = bench_c[bench_c["company"] == company]
        r = rag_c[rag_c["company"] == company]
        ub = unmatched_bench[unmatched_bench["company"] == company]

        step_counts = a["matched_step"].value_counts().to_dict()
        distinct_steps = len(step_counts)

        # privilegia company abbastanza ricche, con molti match e almeno qualche mismatch
        score = (
            len(a)
            + 0.3 * len(b)
            + 1.5 * distinct_steps
            + (3.0 if len(ub) > 0 else 0.0)
        )

        rows.append(
            {
                "company": company,
                "bench_rows": len(b),
                "rag_rows": len(r),
                "matched_rows": len(a),
                "unmatched_bench_rows": len(ub),
                "distinct_steps": distinct_steps,
                "step1": step_counts.get("Step1", 0),
                "step2": step_counts.get("Step2", 0),
                "step3": step_counts.get("Step3", 0),
                "step4": step_counts.get("Step4", 0),
                "score": round(score, 2),
            }
        )

    cand = pd.DataFrame(rows).sort_values(
        ["score", "bench_rows", "matched_rows"],
        ascending=[False, False, False],
    )
    return cand


def select_case_study_company(candidates: pd.DataFrame) -> str:
    if CASE_STUDY_COMPANY:
        if CASE_STUDY_COMPANY not in set(candidates["company"]):
            raise ValueError(f"CASE_STUDY_COMPANY '{CASE_STUDY_COMPANY}' non trovata tra le company comuni.")
        return CASE_STUDY_COMPANY

    # criterio automatico:
    # preferisci una company con almeno 5 benchmark rows, almeno 3 matched,
    # e almeno 1 unmatched benchmark row.
    filt = candidates[
        (candidates["bench_rows"] >= 5)
        & (candidates["matched_rows"] >= 3)
        & (candidates["unmatched_bench_rows"] >= 1)
    ]

    if not filt.empty:
        return str(filt.iloc[0]["company"])

    # fallback
    return str(candidates.iloc[0]["company"])


def build_company_case_study(
    company: str,
    rag_c: pd.DataFrame,
    bench_c: pd.DataFrame,
    assignments_df: pd.DataFrame,
    unmatched_bench: pd.DataFrame,
) -> Tuple[pd.DataFrame, str]:
    a = assignments_df[assignments_df["company"] == company].copy()
    b = bench_c[bench_c["company"] == company].copy()
    r = rag_c[rag_c["company"] == company].copy()
    ub = unmatched_bench[unmatched_bench["company"] == company].copy()

    linked_rag_idx = set(a["rag_idx"].dropna().astype(int).tolist())
    extra_rag = r.loc[sorted(set(r.index) - linked_rag_idx)].copy()

    case_df = b[
        ["company", "report_year", "activity_raw", "sub_activity_code", "bench_objective"]
    ].copy().rename(
        columns={
            "activity_raw": "benchmark_activity",
            "sub_activity_code": "benchmark_code",
        }
    )

    case_df = case_df.merge(
        a[
            [
                "bench_idx",
                "matched_step",
                "rag_activity",
                "rag_code",
                "rag_label",
                "why",
            ]
        ],
        left_index=True,
        right_on="bench_idx",
        how="left",
    )

    case_df["matched_step"] = case_df["matched_step"].fillna("Unmatched")
    case_df["why"] = case_df["why"].fillna("No match across the four-step protocol.")
    case_df = case_df.drop(columns=["bench_idx"], errors="ignore")
    case_df = case_df.sort_values(["report_year", "matched_step", "benchmark_code", "benchmark_activity"])

    step_counts = case_df["matched_step"].value_counts().to_dict()

    # markdown summary
    lines = []
    lines.append(f"# Company case study: {company}")
    lines.append("")
    lines.append("This case study was generated automatically for the final configuration: **Option 1 + restrictive reduction**.")
    lines.append("")
    lines.append("## Summary")
    lines.append(f"- Benchmark rows: {len(b)}")
    lines.append(f"- Extracted rows (evaluation set): {len(r)}")
    lines.append(f"- Matched benchmark rows: {len(a)}")
    lines.append(f"- Unmatched benchmark rows: {len(ub)}")
    lines.append(f"- Step1 matches: {step_counts.get('Step1', 0)}")
    lines.append(f"- Step2 matches: {step_counts.get('Step2', 0)}")
    lines.append(f"- Step3 matches: {step_counts.get('Step3', 0)}")
    lines.append(f"- Step4 matches: {step_counts.get('Step4', 0)}")
    lines.append("")

    matched_preview = case_df[case_df["matched_step"] != "Unmatched"][
        [
            "report_year",
            "benchmark_activity",
            "benchmark_code",
            "bench_objective",
            "rag_activity",
            "rag_code",
            "rag_label",
            "matched_step",
        ]
    ].head(MAX_CASE_MATCHED_ROWS)

    unmatched_preview = case_df[case_df["matched_step"] == "Unmatched"][
        [
            "report_year",
            "benchmark_activity",
            "benchmark_code",
            "bench_objective",
        ]
    ].head(MAX_CASE_UNMATCHED_BENCH)

    extra_rag_preview = extra_rag[
        ["report_year", "activity_raw", "sub_activity_code_rag", "rag_label"]
    ].rename(
        columns={
            "activity_raw": "rag_activity",
            "sub_activity_code_rag": "rag_code",
        }
    ).head(MAX_CASE_EXTRA_RAG)

    lines.append("## Representative matched rows")
    lines.append(df_to_markdown(matched_preview))
    lines.append("")

    lines.append("## Unmatched benchmark rows")
    lines.append(df_to_markdown(unmatched_preview))
    lines.append("")

    lines.append("## Extracted rows not linked to benchmark rows")
    lines.append(df_to_markdown(extra_rag_preview))
    lines.append("")

    md_text = "\n".join(lines)
    return case_df, md_text


# ============================================================
# ERROR TAXONOMY
# ============================================================
def build_error_taxonomy(
    common_companies: List[str],
    rag_raw: pd.DataFrame,
    rag_eval: pd.DataFrame,
    rag_c: pd.DataFrame,
    assignments_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, str]:
    linked_rag_idx = set(assignments_df["rag_idx"].dropna().astype(int).tolist())

    rag_eval_common = rag_eval[rag_eval["company"].isin(common_companies)].copy()

    # 1) Correct code, unstable/missing label  -> Step2
    step2 = assignments_df[assignments_df["matched_step"] == "Step2"].copy()
    cat1_examples = step2[
        [
            "company",
            "report_year",
            "benchmark_activity",
            "benchmark_code",
            "benchmark_objective",
            "rag_activity",
            "rag_code",
            "rag_label",
            "why",
        ]
    ].head(MAX_TAXONOMY_EXAMPLES_PER_CATEGORY).copy()
    cat1_examples["category"] = "Correct code, missing or unstable label"

    # 2) Missing subcode -> rag_eval common companies
    miss_sub = rag_eval_common[rag_eval_common["sub_activity_code_rag"].apply(_is_missing_str)].copy()
    cat2_examples = miss_sub[
        ["company", "report_year", "activity_raw", "rag_label", "sub_activity_code_rag"]
    ].rename(
        columns={
            "activity_raw": "rag_activity",
            "sub_activity_code_rag": "rag_code",
        }
    ).head(MAX_TAXONOMY_EXAMPLES_PER_CATEGORY)
    cat2_examples["category"] = "Missing subcode"

    # 3) Missing activity text
    miss_act = rag_eval_common[rag_eval_common["activity_raw"].apply(_is_missing_str)].copy()
    cat3_examples = miss_act[
        ["company", "report_year", "activity_raw", "rag_label", "sub_activity_code_rag"]
    ].rename(
        columns={
            "activity_raw": "rag_activity",
            "sub_activity_code_rag": "rag_code",
        }
    ).head(MAX_TAXONOMY_EXAMPLES_PER_CATEGORY)
    cat3_examples["category"] = "Missing activity text / partial row"

    # 4) Over-generation or duplicate-like row
    rag_raw_common = rag_raw[rag_raw["company"].isin(common_companies)].copy()
    duplicate_like = rag_raw_common[rag_raw_common.duplicated(subset=["match_key"], keep="first")].copy()
    cat4_examples = duplicate_like[
        ["company", "report_year", "activity_raw", "rag_label", "sub_activity_code_rag"]
    ].rename(
        columns={
            "activity_raw": "rag_activity",
            "sub_activity_code_rag": "rag_code",
        }
    ).head(MAX_TAXONOMY_EXAMPLES_PER_CATEGORY)
    cat4_examples["category"] = "Over-generation or duplicate-like row"

    # 5) Unmatched extracted activity
    unmatched_rag = rag_c.loc[sorted(set(rag_c.index) - linked_rag_idx)].copy()
    cat5_examples = unmatched_rag[
        ["company", "report_year", "activity_raw", "rag_label", "sub_activity_code_rag"]
    ].rename(
        columns={
            "activity_raw": "rag_activity",
            "sub_activity_code_rag": "rag_code",
        }
    ).head(MAX_TAXONOMY_EXAMPLES_PER_CATEGORY)
    cat5_examples["category"] = "Unmatched extracted activity"

    counts = pd.DataFrame(
        [
            {
                "category": "Correct code, missing or unstable label",
                "count": len(step2),
                "definition": "Benchmark rows recovered at Step2 (code-only) after failing Step1.",
            },
            {
                "category": "Missing subcode",
                "count": len(miss_sub),
                "definition": "Extracted rows with missing code_numeric in the evaluation set.",
            },
            {
                "category": "Missing activity text / partial row",
                "count": len(miss_act),
                "definition": "Extracted rows with empty activity text in the evaluation set.",
            },
            {
                "category": "Over-generation or duplicate-like row",
                "count": len(duplicate_like),
                "definition": "Raw extracted rows that become duplicates under the official evaluation deduplication key.",
            },
            {
                "category": "Unmatched extracted activity",
                "count": len(unmatched_rag),
                "definition": "Evaluation-set extracted rows in common companies not linked to any benchmark row by the four-step protocol.",
            },
        ]
    )

    examples = pd.concat(
        [
            cat1_examples,
            cat2_examples,
            cat3_examples,
            cat4_examples,
            cat5_examples,
        ],
        ignore_index=True,
        sort=False,
    )

    lines = []
    lines.append("# Error taxonomy snapshot")
    lines.append("")
    lines.append("This snapshot refers to the final configuration: **Option 1 + restrictive reduction**.")
    lines.append("")
    lines.append("## Counts")
    lines.append(df_to_markdown(counts))
    lines.append("")
    lines.append("## Representative examples")
    for cat in counts["category"].tolist():
        lines.append(f"### {cat}")
        sub = examples[examples["category"] == cat].copy()
        if not sub.empty:
            keep_cols = [c for c in ["company", "report_year", "rag_activity", "rag_code", "rag_label", "benchmark_activity", "benchmark_code", "benchmark_objective", "why"] if c in sub.columns]
            lines.append(df_to_markdown(sub[keep_cols]))
        else:
            lines.append("_No examples available._")
        lines.append("")

    return counts, examples, "\n".join(lines)


# ============================================================
# SAVE ALL OUTPUTS
# ============================================================
def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    raw_rag = read_tabular(RAG_FILE)
    raw_bench = read_tabular(BENCH_FILE)

    rag_raw, rag_eval = prepare_rag(raw_rag)
    bench_eval = prepare_benchmark(raw_bench)

    common_companies, rag_c, bench_c, assignments_df, unmatched_bench = build_all_year_assignments(
        rag_eval=rag_eval,
        bench_eval=bench_eval,
    )

    # --------------------------------------------------------
    # 1) FOUR-STEP EXAMPLES
    # --------------------------------------------------------
    step_examples = build_step_examples(assignments_df, unmatched_bench, rag_c)
    step_examples_csv = OUT_DIR / "match_examples_step1_4.csv"
    step_examples.to_csv(step_examples_csv, index=False, encoding="utf-8-sig")

    # --------------------------------------------------------
    # 2) CASE STUDY COMPANY
    # --------------------------------------------------------
    candidates = build_company_candidates(
        common_companies=common_companies,
        rag_c=rag_c,
        bench_c=bench_c,
        assignments_df=assignments_df,
        unmatched_bench=unmatched_bench,
    )

    candidates_csv = OUT_DIR / "company_case_candidates.csv"
    candidates.to_csv(candidates_csv, index=False, encoding="utf-8-sig")

    selected_company = select_case_study_company(candidates)
    case_df, case_md = build_company_case_study(
        company=selected_company,
        rag_c=rag_c,
        bench_c=bench_c,
        assignments_df=assignments_df,
        unmatched_bench=unmatched_bench,
    )

    case_slug = slugify(selected_company)
    case_csv = OUT_DIR / f"company_case_study_{case_slug}.csv"
    case_md_path = OUT_DIR / f"company_case_study_{case_slug}.md"
    case_df.to_csv(case_csv, index=False, encoding="utf-8-sig")
    case_md_path.write_text(case_md, encoding="utf-8")

    # --------------------------------------------------------
    # 3) ERROR TAXONOMY
    # --------------------------------------------------------
    counts_df, examples_df, taxonomy_md = build_error_taxonomy(
        common_companies=common_companies,
        rag_raw=rag_raw,
        rag_eval=rag_eval,
        rag_c=rag_c,
        assignments_df=assignments_df,
    )

    taxonomy_counts_csv = OUT_DIR / "error_taxonomy_counts.csv"
    taxonomy_examples_csv = OUT_DIR / "error_taxonomy_examples.csv"
    taxonomy_md_path = OUT_DIR / "error_taxonomy_snapshot.md"

    counts_df.to_csv(taxonomy_counts_csv, index=False, encoding="utf-8-sig")
    examples_df.to_csv(taxonomy_examples_csv, index=False, encoding="utf-8-sig")
    taxonomy_md_path.write_text(taxonomy_md, encoding="utf-8")

    # --------------------------------------------------------
    # 4) ASSIGNMENTS FULL TABLE (utile per controlli manuali)
    # --------------------------------------------------------
    assignments_csv = OUT_DIR / "all_benchmark_assignments.csv"
    assignments_df.to_csv(assignments_csv, index=False, encoding="utf-8-sig")

    unmatched_bench_csv = OUT_DIR / "unmatched_benchmark_rows.csv"
    unmatched_bench.to_csv(unmatched_bench_csv, index=False, encoding="utf-8-sig")

    print("\n=== DIAGNOSTICS GENERATED ===")
    print(f"Output directory: {OUT_DIR}")
    print(f"- Four-step examples:      {step_examples_csv}")
    print(f"- Company candidates:      {candidates_csv}")
    print(f"- Selected case study:     {selected_company}")
    print(f"- Case study CSV:          {case_csv}")
    print(f"- Case study Markdown:     {case_md_path}")
    print(f"- Error taxonomy counts:   {taxonomy_counts_csv}")
    print(f"- Error taxonomy examples: {taxonomy_examples_csv}")
    print(f"- Error taxonomy Markdown: {taxonomy_md_path}")
    print(f"- Full assignments:        {assignments_csv}")
    print(f"- Unmatched benchmark:     {unmatched_bench_csv}")


if __name__ == "__main__":
    main()