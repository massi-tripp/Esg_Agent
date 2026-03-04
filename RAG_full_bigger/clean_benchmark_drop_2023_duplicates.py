# File: python RAG_full_bigger\clean_benchmark_drop_2023_duplicates.py

from __future__ import annotations

import re
from pathlib import Path
from typing import Tuple

import pandas as pd

IN_XLSX = Path(r"C:\Universita\TESI\esg_agent\RAG_full_bigger\documentazione_rag.xlsx")

OUT_CSV = Path(r"C:\Universita\TESI\esg_agent\RAG_full_bigger\documentazione_rag_clean.csv")
OUT_XLSX = Path(r"C:\Universita\TESI\esg_agent\RAG_full_bigger\documentazione_rag_clean.xlsx")

_MULTI_SPACE_RE = re.compile(r"\s+")


def _norm_space(s: str) -> str:
    return _MULTI_SPACE_RE.sub(" ", (s or "").strip())


def drop_2023_if_same_in_2024_benchmark(df: pd.DataFrame) -> pd.DataFrame:
    """
    Regola richiesta (BENCH):
    se per la stessa company_name_full esiste una riga 2023 e una riga 2024 con:
      - activity identica (normalizzata spazi)
      - Sub_activity_code identico (normalizzato spazi)
      - environmental_objective identico (strip + upper)
    allora elimina la riga 2023 e tiene la 2024.
    """
    tmp = df.copy()

    tmp["company_full_norm"] = tmp["company_name_full"].astype(str).apply(_norm_space)
    tmp["year_norm"] = tmp["report_year"].astype(str).apply(_norm_space)
    tmp["activity_norm"] = tmp["activity"].astype(str).apply(_norm_space)
    tmp["subcode_norm"] = tmp["Sub_activity_code"].astype(str).apply(_norm_space)
    tmp["obj_norm"] = tmp["environmental_objective"].astype(str).apply(_norm_space).str.upper()

    # chiave confronto cross-year
    tmp["cross_key"] = (
        tmp["company_full_norm"]
        + "||"
        + tmp["activity_norm"]
        + "||"
        + tmp["subcode_norm"]
        + "||"
        + tmp["obj_norm"]
    )

    keys_2024 = set(tmp.loc[tmp["year_norm"] == "2024", "cross_key"].tolist())

    # drop: righe 2023 che hanno una corrispondente in 2024
    mask_drop = (tmp["year_norm"] == "2023") & (tmp["cross_key"].isin(keys_2024))

    dropped = int(mask_drop.sum())
    if dropped:
        print(f"[INFO] Drop BENCH 2023 duplicates covered by 2024: {dropped:,}")

    tmp = tmp.loc[~mask_drop].copy()

    # cleanup colonne helper
    tmp = tmp.drop(
        columns=["company_full_norm", "year_norm", "activity_norm", "subcode_norm", "obj_norm", "cross_key"],
        errors="ignore",
    )

    return tmp


def main() -> None:
    df = pd.read_excel(IN_XLSX, dtype=str).fillna("")
    df.columns = [c.strip() for c in df.columns]

    required = {"company_name_full", "report_year", "activity", "Sub_activity_code", "environmental_objective"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Nel benchmark mancano colonne richieste: {sorted(missing)}")

    n0 = len(df)
    print(f"[INFO] Loaded benchmark rows: {n0:,}")

    df = drop_2023_if_same_in_2024_benchmark(df)

    n1 = len(df)
    print(f"[INFO] Output rows after drop: {n1:,} (removed {n0 - n1:,})")

    # (facoltativo) ordina per leggibilità
    sort_cols = [c for c in ["company_name_full", "report_year", "environmental_objective", "Sub_activity_code", "activity"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols, na_position="last")

    df.to_csv(OUT_CSV, index=False, encoding="utf-8")
    df.to_excel(OUT_XLSX, index=False)

    print(f"[OK] Salvato CSV : {OUT_CSV}")
    print(f"[OK] Salvato XLSX: {OUT_XLSX}")


if __name__ == "__main__":
    main()