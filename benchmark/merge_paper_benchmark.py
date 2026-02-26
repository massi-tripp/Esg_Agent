# File: benchmark_finale.py

from __future__ import annotations

import re
import unicodedata
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

MANUAL_PATH = Path(r"C:\Universita\TESI\esg_agent\benchmark\Copia_Benchmark_rag1.xlsx")
PAPER_PATH  = Path(r"C:\Universita\TESI\esg_agent\benchmark\dataframe_paper.xlsx")

# Output
OUT_XLSX = Path(r"C:\Universita\TESI\esg_agent\benchmark\benchmark_merged.xlsx")

# ✅ Nuovo: excel audit match completo
AUDIT_XLSX = Path(r"C:\Universita\TESI\esg_agent\benchmark\benchmark_matches_audit.xlsx")

# =========================
# CONFIG
# =========================
COMMON_2023_2024 = {
    "ADIDAS AG",
    "BAYER AG",
    "BECHTLE AG",
    "BEIERSDORF AG",
    "BMW AG VZ",
    "BRENNTAG SE",
    "DEUTSCHE TELEKOM AG",
    "E.ON SE",
    "EVONIK INDUSTRIES AG",
    "FRESENIUS SE & CO. KGAA",
    "FUCHS SE",
    "VERBUND AG",
    "ZALANDO SE",
    "PORSCHE AG Vz",
}

DEFAULT_YEAR_MANUAL = "2024"
DEFAULT_YEAR_PAPER = "2023"
COMMON_YEAR = "2023/2024"

FUZZY_THRESHOLD = 0.77 # 0-1

AUDIT_MAX_ROWS = 80

# ✅ stopwords: usate SOLO per creare le chiavi di match (_act_key)
STOPWORDS = {
    "of", "for", "and", "the", "a", "an", "to", "in", "on", "by", "with", "from", "using",
    "including", "related", "activities", "activity", "service", "services",
}


# =========================
# UTILS: normalizzazione testo (solo per matching)
# =========================
def _normalize_text(s: Optional[str]) -> str:
    """
    Normalizzazione usata SOLO per il match:
    - lowercase, rimozione accenti
    - rimozione punteggiatura
    - split tokens
    - rimozione STOPWORDS
    - rimozione token troppo corti (<=2)
    NB: non altera le colonne originali nel df finale.
    """
    if s is None:
        return ""
    s = str(s).strip().lower()
    if not s:
        return ""

    # accenti
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))

    # separatori -> spazio
    s = s.replace("/", " ").replace("-", " ").replace("_", " ")
    s = s.replace("&", " and ")

    # tieni solo alfanumerico/spazi
    s = re.sub(r"[^a-z0-9\s]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    # tokenizzazione + stopword removal + token corti
    tokens = [t for t in s.split() if t not in STOPWORDS and len(t) > 2]

    return " ".join(tokens)


def _normalize_company(s: Optional[str]) -> str:
    if s is None:
        return ""
    return str(s).strip()


# =========================
# fuzzy matching (no libs)
# =========================
def _similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    A = set(a.split())
    B = set(b.split())
    if not A or not B:
        return 0.0
    return len(A & B) / len(A | B)


def _best_fuzzy_match(key: str, candidates: pd.Index) -> Tuple[Optional[str], float]:
    best = None
    best_score = 0.0
    for c in candidates:
        score = _similarity(key, c)
        if score > best_score:
            best_score = score
            best = c
    return best, best_score


# =========================
# LOADERS
# =========================
def _load_any(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"File non trovato: {path}")

    if path.suffix.lower() in {".xlsx", ".xls"}:
        try:
            return pd.read_excel(path, sheet_name="benchmark")
        except Exception:
            return pd.read_excel(path)

    raise ValueError(f"Estensione non supportata: {path.suffix}")


# =========================
# CORE: merge logico + AUDIT stampa + EXCEL audit
# =========================
def merge_manual_and_paper(df_manual: pd.DataFrame, df_paper: pd.DataFrame) -> pd.DataFrame:
    df_manual = df_manual.copy()
    df_paper = df_paper.copy()

    df_manual["company_name"] = df_manual["company_name"].apply(_normalize_company)
    df_paper["company_name"] = df_paper["company_name"].apply(_normalize_company)

    # --- anni
    df_manual["report_year"] = DEFAULT_YEAR_MANUAL
    df_paper["report_year"] = DEFAULT_YEAR_PAPER

    df_manual.loc[df_manual["company_name"].isin(COMMON_2023_2024), "report_year"] = COMMON_YEAR
    df_paper.loc[df_paper["company_name"].isin(COMMON_2023_2024), "report_year"] = COMMON_YEAR

    # --- link_report: solo manuale
    if "link_report" not in df_paper.columns:
        df_paper["link_report"] = ""
    else:
        df_paper["link_report"] = df_paper["link_report"].fillna("")
    df_manual["link_report"] = df_manual.get("link_report", "").fillna("")

    # --- attività
    if "Sub_activity_label" not in df_manual.columns:
        raise KeyError("Nel DF manuale manca la colonna 'Sub_activity_label'")
    if "activity" not in df_paper.columns:
        raise KeyError("Nel DF paper manca la colonna 'activity'")

    # NB: queste sono colonne "originali" e restano tali nel df finale
    df_manual["activity"] = df_manual["Sub_activity_label"].astype(str)

    # ====== AUDIT columns ======
    df_paper["paper_activity_original"] = df_paper["activity"].astype(str)
    df_paper["match_method"] = "none"
    df_paper["match_score"] = 0.0
    df_paper["matched_act_key"] = pd.NA
    df_paper["manual_activity_example"] = pd.NA

    # --- keys (usate SOLO per matching)
    df_manual["_act_key"] = df_manual["Sub_activity_label"].apply(_normalize_text)
    df_paper["_act_key"] = df_paper["activity"].apply(_normalize_text)

    # --- lookup manuale -> meta
    meta_cols = ["main_activity_code", "main_activity_label", "environmental_objective", "Sub_activity_code", "Sub_activity_label"]
    for c in meta_cols:
        if c not in df_manual.columns:
            df_manual[c] = pd.NA

    manual_lookup = (
        df_manual
        .sort_values(["company_name"])
        .groupby("_act_key", dropna=False)[meta_cols]
        .agg(lambda s: s.dropna().iloc[0] if len(s.dropna()) else pd.NA)
    )

    # --- merge exact
    df_paper_enriched = df_paper.merge(
        manual_lookup,
        left_on="_act_key",
        right_index=True,
        how="left",
        suffixes=("", "_from_manual"),
    )

    # marca exact
    exact_mask = df_paper_enriched["main_activity_code"].notna()
    df_paper_enriched.loc[exact_mask, "match_method"] = "exact"
    df_paper_enriched.loc[exact_mask, "match_score"] = 1.0
    df_paper_enriched.loc[exact_mask, "matched_act_key"] = df_paper_enriched.loc[exact_mask, "_act_key"]
    df_paper_enriched.loc[exact_mask, "manual_activity_example"] = df_paper_enriched.loc[exact_mask, "Sub_activity_label"]

    # --- fuzzy / scoring per missing:
    missing_mask = df_paper_enriched["main_activity_code"].isna() & df_paper_enriched["_act_key"].astype(bool)

    if missing_mask.any():
        candidates = manual_lookup.index

        for idx, row in df_paper_enriched.loc[missing_mask, ["_act_key"]].itertuples():
            key = row
            best, score = _best_fuzzy_match(key, candidates)

            # salva comunque il miglior candidato + score (audit)
            if best is not None:
                df_paper_enriched.loc[idx, "matched_act_key"] = best
                df_paper_enriched.loc[idx, "match_score"] = float(score)
                try:
                    df_paper_enriched.loc[idx, "manual_activity_example"] = manual_lookup.loc[best, "Sub_activity_label"]
                except Exception:
                    pass

            # applica fill SOLO se supera soglia
            if best is not None and score >= FUZZY_THRESHOLD:
                df_paper_enriched.loc[idx, meta_cols] = manual_lookup.loc[best, meta_cols].values
                df_paper_enriched.loc[idx, "match_method"] = "fuzzy"

    # paper: link_report deve essere vuoto
    df_paper_enriched["link_report"] = ""

    # pages_by_KPI
    if "pages_by_KPI" not in df_paper_enriched.columns:
        df_paper_enriched["pages_by_KPI"] = ""
    df_manual["pages_by_KPI"] = df_manual.get("pages_by_KPI", "").fillna("")
    df_paper_enriched["pages_by_KPI"] = df_paper_enriched["pages_by_KPI"].fillna("")

    # ✅ key normalizzata del paper (audit) - usa SEMPRE il df arricchito
    df_paper_enriched["paper_act_key"] = df_paper_enriched["_act_key"]

    # =========================
    # ✅ STAMPA AUDIT (console)
    # =========================
    audit_cols = [
        "company_name",
        "paper_activity_original",
        "paper_act_key",
        "match_method",
        "match_score",
        "matched_act_key",
        "manual_activity_example",
        "Sub_activity_code",
        "Sub_activity_label",
        "main_activity_code",
        "main_activity_label",
        "environmental_objective",
    ]
    audit_cols = [c for c in audit_cols if c in df_paper_enriched.columns]

    print("\n================ AUDIT SUMMARY ================")
    print(df_paper_enriched["match_method"].value_counts(dropna=False))

    fuzzy_df = df_paper_enriched[df_paper_enriched["match_method"] == "fuzzy"][audit_cols] \
        .sort_values(["match_score", "company_name"], ascending=[True, True])

    none_df = df_paper_enriched[df_paper_enriched["match_method"] == "none"][audit_cols] \
        .sort_values(["match_score", "company_name"], ascending=[False, True])

    print(f"\n================ FUZZY MATCHES (lowest scores first) - showing up to {AUDIT_MAX_ROWS} ================")
    if len(fuzzy_df) == 0:
        print("Nessun fuzzy match applicato.")
    else:
        with pd.option_context("display.max_rows", AUDIT_MAX_ROWS, "display.max_colwidth", 80, "display.width", 160):
            print(fuzzy_df.head(AUDIT_MAX_ROWS))

    print(f"\n================ NON MATCHED (none) - showing up to {AUDIT_MAX_ROWS} ================")
    if len(none_df) == 0:
        print("Tutte le attività del paper hanno trovato un mapping (exact o fuzzy).")
    else:
        with pd.option_context("display.max_rows", AUDIT_MAX_ROWS, "display.max_colwidth", 80, "display.width", 160):
            print(none_df.head(AUDIT_MAX_ROWS))

    # =========================
    # ✅ EXCEL AUDIT COMPLETO (tutti i match)
    # =========================
    audit_all = df_paper_enriched[audit_cols].copy()

    method_order = pd.CategoricalDtype(categories=["none", "fuzzy", "exact"], ordered=True)
    audit_all["match_method"] = audit_all["match_method"].astype(method_order)

    audit_all = audit_all.sort_values(
        ["match_method", "match_score", "company_name", "paper_activity_original"],
        ascending=[True, True, True, True],
    )

    print("\nMin/Max score by method:")
    print(df_paper_enriched.groupby("match_method")["match_score"].agg(["min", "max", "count"]))

    audit_all.to_excel(AUDIT_XLSX, index=False, sheet_name="match_audit")
    print("\nSaved AUDIT XLSX:", AUDIT_XLSX)

    # =========================
    # UNION columns + concat
    # =========================
    all_cols = sorted(set(df_manual.columns) | set(df_paper_enriched.columns))

    for c in all_cols:
        if c not in df_manual.columns:
            df_manual[c] = pd.NA
        if c not in df_paper_enriched.columns:
            df_paper_enriched[c] = pd.NA

    df_final = pd.concat([df_manual[all_cols], df_paper_enriched[all_cols]], ignore_index=True)

    # ✅ qui togliamo solo la colonna tecnica, NON tocchiamo activity originali
    df_final = df_final.drop(columns=[c for c in ["_act_key"] if c in df_final.columns], errors="ignore")

    preferred_order = [
        "company_name",
        "link_report",
        "report_year",
        "activity",
        "Sub_activity_label",
        "Sub_activity_code",
        "main_activity_code",
        "main_activity_label",
        "environmental_objective",
        "pages_by_KPI",
        # paper meta
        "industry", "country", "sector", "company_type", "revenue", "number_of_employees",
        "currency", "units",
        # KPI
        "turnover_total_value", "turnover_total_pct",
        "turnover_eligible_value", "turnover_eligible_pct",
        "turnover_aligned_value", "turnover_aligned_pct",
        "turnover_non_eligible_value", "turnover_non_eligible_pct",
        "capex_total_value", "capex_total_pct",
        "capex_eligible_value", "capex_eligible_pct",
        "capex_aligned_value", "capex_aligned_pct",
        "capex_eligible_not_aligned_value", "capex_eligible_not_aligned_pct",
        "capex_non_eligible_value", "capex_non_eligible_pct",
        "opex_total_value", "opex_total_pct",
        "opex_eligible_value", "opex_eligible_pct",
        "opex_aligned_value", "opex_aligned_pct",
        "opex_non_eligible_value", "opex_non_eligible_pct",
    ]
    final_cols = [c for c in preferred_order if c in df_final.columns] + [c for c in df_final.columns if c not in preferred_order]
    df_final = df_final[final_cols]

    return df_final


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    df_manual = _load_any(MANUAL_PATH)
    df_paper = _load_any(PAPER_PATH)

    df_final = merge_manual_and_paper(df_manual, df_paper)

    print("DF finale shape:", df_final.shape)
    print(df_final.head(10))

    df_final.to_excel(OUT_XLSX, index=False, sheet_name="benchmark_finale")
    print("Saved XLSX:", OUT_XLSX)
