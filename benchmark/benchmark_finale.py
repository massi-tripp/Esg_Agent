# File: benchmark_fill_from_attivita_totali.py

from __future__ import annotations

import re
import unicodedata
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

# =========================
# PATHS (input)
# =========================
MERGED_PATH = Path(r"C:\Universita\TESI\esg_agent\benchmark\benchmark_merged.xlsx")
MAPPING_PATH = Path(r"C:\Universita\TESI\esg_agent\benchmark\attività_totali_clean.xlsx")

# =========================
# PATHS (output) - nomi diversi
# =========================
OUT_XLSX = Path(r"C:\Universita\TESI\esg_agent\benchmark\benchmark_merged_filled_from_attivita_totali.xlsx")
AUDIT_XLSX = Path(r"C:\Universita\TESI\esg_agent\benchmark\benchmark_matches_audit_from_attivita_totali.xlsx")

# =========================
# CONFIG
# =========================
FUZZY_THRESHOLD = 0.92
AUDIT_MAX_ROWS = 80

STOPWORDS = {
    "of", "for", "and", "the", "a", "an", "to", "in", "on", "by", "with", "from", "using",
    "including", "related", "activities", "activity", "service", "services",
}

# campi da riempire (non overwrite)
FILL_COLS = [
    "Sub_activity_code",
    "main_activity_code",
    "main_activity_label",
    "environmental_objective",
]

# mapping file columns
MAP_REQUIRED_COLS = [
    "main_activity_code",
    "main_activity_label",
    "environmental_objective",
    "Sub_activity_code",
    "Sub_activity_label",
]


# =========================
# UTILS
# =========================
def _normalize_text(s: Optional[str]) -> str:
    """
    Identico concetto al tuo: normalizza SOLO per matching.
    """
    if s is None:
        return ""
    s = str(s).strip().lower()
    if not s:
        return ""

    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))

    s = s.replace("/", " ").replace("-", " ").replace("_", " ")
    s = s.replace("&", " and ")

    s = re.sub(r"[^a-z0-9\s]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    tokens = [t for t in s.split() if t not in STOPWORDS and len(t) > 2]
    return " ".join(tokens)


def _is_missing(x) -> bool:
    if x is None:
        return True
    if isinstance(x, float) and pd.isna(x):
        return True
    if pd.isna(x):
        return True
    s = str(x).strip()
    if s == "":
        return True
    s_low = s.lower()
    return s_low in {"nan", "na", "n/a", "none", "null"}


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


def _load_excel(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"File non trovato: {path}")
    if path.suffix.lower() not in {".xlsx", ".xls"}:
        raise ValueError(f"Estensione non supportata: {path.suffix}")
    return pd.read_excel(path)


def _strip_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


# =========================
# CORE
# =========================
def fill_missing_activity_fields_from_mapping(
    df_merged: pd.DataFrame,
    df_map: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = _strip_columns(df_merged)
    mp = _strip_columns(df_map)

    # check colonne nel merged: la chiave è ACTIVITY
    if "activity" not in df.columns:
        raise KeyError(f"Nel merged manca la colonna 'activity'. Colonne: {list(df.columns)}")

    # check mapping
    for c in MAP_REQUIRED_COLS:
        if c not in mp.columns:
            raise KeyError(f"Nel mapping manca '{c}'. Colonne: {list(mp.columns)}")

    # garantisci che i campi target esistano nel merged
    for c in FILL_COLS:
        if c not in df.columns:
            df[c] = pd.NA

    # =========================
    # 1) seleziona SOLO righe con almeno 1 campo target mancante
    # =========================
    need_fill_mask = (
        df["activity"].apply(lambda x: not _is_missing(x))
        & (
            df["Sub_activity_code"].apply(_is_missing)
            | df["main_activity_code"].apply(_is_missing)
            | df["main_activity_label"].apply(_is_missing)
            | df["environmental_objective"].apply(_is_missing)
        )
    )

    df_work = df.loc[need_fill_mask].copy()

    # audit vuoto se niente da fare
    if df_work.empty:
        audit_empty = pd.DataFrame(columns=[
            "company_name",
            "paper_activity_original",
            "paper_act_key",
            "match_method",
            "match_score",
            "matched_act_key",
            "manual_activity_example",
            *FILL_COLS,
        ])
        return df, audit_empty

    # =========================
    # 2) chiavi normalizzate SOLO per matching (come nel tuo)
    #    merged: activity
    #    mapping: Sub_activity_label
    # =========================
    df_work["_act_key"] = df_work["activity"].apply(_normalize_text)
    mp["_act_key"] = mp["Sub_activity_label"].apply(_normalize_text)

    # =========================
    # 3) lookup mapping per key (come manual_lookup nel tuo script)
    # =========================
    map_lookup = (
        mp.groupby("_act_key", dropna=False)[FILL_COLS + ["Sub_activity_label"]]
        .agg(lambda s: s.dropna().iloc[0] if len(s.dropna()) else pd.NA)
    )

    # =========================
    # 4) audit columns (stessa logica tua)
    # =========================
    df_work["paper_activity_original"] = df_work["activity"].astype(str)
    df_work["match_method"] = "none"
    df_work["match_score"] = 0.0
    df_work["matched_act_key"] = pd.NA
    df_work["manual_activity_example"] = pd.NA  # qui = esempio Sub_activity_label mapping

    # =========================
    # 5) exact merge SOLO sul workset
    # =========================
    df_enriched = df_work.merge(
        map_lookup,
        left_on="_act_key",
        right_index=True,
        how="left",
        suffixes=("", "_from_map"),
    )

    # exact se nel mapping abbiamo almeno 1 valore utile
    has_any_map_value = pd.Series(False, index=df_enriched.index)
    for c in FILL_COLS:
        has_any_map_value = has_any_map_value | (~df_enriched[c].apply(_is_missing))

    exact_mask = has_any_map_value

    df_enriched.loc[exact_mask, "match_method"] = "exact"
    df_enriched.loc[exact_mask, "match_score"] = 1.0
    df_enriched.loc[exact_mask, "matched_act_key"] = df_enriched.loc[exact_mask, "_act_key"]
    df_enriched.loc[exact_mask, "manual_activity_example"] = df_enriched.loc[exact_mask, "Sub_activity_label"]

    # fill exact SOLO dove target è missing (non overwrite)
    for c in FILL_COLS:
        tgt_missing = df_enriched[c + "_x"].apply(_is_missing) if (c + "_x") in df_enriched.columns else df_enriched[c].apply(_is_missing)

    # Nota: per evitare ambiguità sui suffissi, gestiamo esplicitamente:
    # nel df_enriched, dopo merge, pandas potrebbe creare colonne duplicate se già presenti.
    # quindi ricostruiamo la fonte "map" e il target "merged" in modo robusto.

    # colonne target nel merged work
    for c in FILL_COLS:
        # se il merge ha creato suffissi, il target originale sarà c + "_x" e la fonte sarà c + "_y"
        if (c + "_x") in df_enriched.columns and (c + "_y") in df_enriched.columns:
            tgt_col = c + "_x"
            src_col = c + "_y"
        else:
            # caso in cui non ci siano duplicati (improbabile qui, ma gestiamo)
            tgt_col = c
            src_col = c

        tgt_missing = df_enriched[tgt_col].apply(_is_missing)
        src_has = ~df_enriched[src_col].apply(_is_missing)
        fill_mask = exact_mask & tgt_missing & src_has
        df_enriched.loc[fill_mask, tgt_col] = df_enriched.loc[fill_mask, src_col]

    # =========================
    # 6) fuzzy SOLO per match_method none
    # =========================
    still_none = (df_enriched["match_method"] == "none") & df_enriched["_act_key"].astype(bool)

    if still_none.any():
        candidates = map_lookup.index

        for idx, key in df_enriched.loc[still_none, ["_act_key"]].itertuples():
            best, score = _best_fuzzy_match(key, candidates)

            if best is not None:
                df_enriched.loc[idx, "matched_act_key"] = best
                df_enriched.loc[idx, "match_score"] = float(score)
                try:
                    df_enriched.loc[idx, "manual_activity_example"] = map_lookup.loc[best, "Sub_activity_label"]
                except Exception:
                    pass

            if best is not None and score >= FUZZY_THRESHOLD:
                df_enriched.loc[idx, "match_method"] = "fuzzy"

                # fill SOLO mancanti (sempre sul target originale)
                for c in FILL_COLS:
                    if (c + "_x") in df_enriched.columns and (c + "_y") in df_enriched.columns:
                        tgt_col = c + "_x"
                    else:
                        tgt_col = c

                    if _is_missing(df_enriched.loc[idx, tgt_col]):
                        v = map_lookup.loc[best, c]
                        if not _is_missing(v):
                            df_enriched.loc[idx, tgt_col] = v

    # =========================
    # 7) ricostruisci df finale: aggiorna SOLO righe target
    # =========================
    df_out = df.copy()

    # estrai le colonne finali da rimettere nel df_out
    for c in FILL_COLS:
        if (c + "_x") in df_enriched.columns:
            filled_series = df_enriched[c + "_x"]
        else:
            filled_series = df_enriched[c]
        df_out.loc[need_fill_mask, c] = filled_series.values

    # =========================
    # 8) audit SOLO workset
    # =========================
    audit_cols = [
        "company_name",
        "paper_activity_original",
        "_act_key",
        "match_method",
        "match_score",
        "matched_act_key",
        "manual_activity_example",
        *[c + "_x" for c in FILL_COLS if (c + "_x") in df_enriched.columns],
        *[c for c in FILL_COLS if c in df_enriched.columns],
    ]
    audit_cols = [c for c in audit_cols if c in df_enriched.columns]

    audit_all = df_enriched[audit_cols].copy()
    audit_all = audit_all.rename(columns={"_act_key": "paper_act_key"})

    # normalizza audit: rinomina eventuali colonne "_x" nei nomi originali
    rename_map = {c + "_x": c for c in FILL_COLS if (c + "_x") in audit_all.columns}
    audit_all = audit_all.rename(columns=rename_map)

    return df_out, audit_all


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    df_merged = _load_excel(MERGED_PATH)
    df_map = _load_excel(MAPPING_PATH)

    df_out, audit_all = fill_missing_activity_fields_from_mapping(df_merged, df_map)

    print("\n================ AUDIT SUMMARY (ONLY MISSING ROWS) ================")
    if audit_all.empty:
        print("Nessuna riga con campi mancanti da riempire.")
    else:
        print(audit_all["match_method"].value_counts(dropna=False))
        print("\nMin/Max score by method:")
        print(audit_all.groupby("match_method")["match_score"].agg(["min", "max", "count"]))

        fuzzy_df = audit_all[audit_all["match_method"] == "fuzzy"] \
            .sort_values(["match_score", "company_name"], ascending=[True, True])
        none_df = audit_all[audit_all["match_method"] == "none"] \
            .sort_values(["match_score", "company_name"], ascending=[False, True])

        print(f"\n================ FUZZY MATCHES (lowest scores first) - showing up to {AUDIT_MAX_ROWS} ================")
        if len(fuzzy_df) == 0:
            print("Nessun fuzzy match applicato.")
        else:
            with pd.option_context("display.max_rows", AUDIT_MAX_ROWS, "display.max_colwidth", 80, "display.width", 160):
                print(fuzzy_df.head(AUDIT_MAX_ROWS))

        print(f"\n================ NON MATCHED (none) - showing up to {AUDIT_MAX_ROWS} ================")
        if len(none_df) == 0:
            print("Tutte le righe missing hanno trovato un mapping (exact o fuzzy).")
        else:
            with pd.option_context("display.max_rows", AUDIT_MAX_ROWS, "display.max_colwidth", 80, "display.width", 160):
                print(none_df.head(AUDIT_MAX_ROWS))

    audit_all.to_excel(AUDIT_XLSX, index=False, sheet_name="match_audit_only_missing")
    print("\nSaved AUDIT XLSX:", AUDIT_XLSX)

    df_out.to_excel(OUT_XLSX, index=False, sheet_name="benchmark_merged_filled")
    print("Saved XLSX:", OUT_XLSX)
