from __future__ import annotations

import re
from pathlib import Path
from typing import Tuple

import pandas as pd

IN_CSV = Path(r"C:\Universita\TESI\esg_agent\RAG_full_bigger\rag2_out\activities_extracted.csv")

OUT_CSV = Path(r"C:\Universita\TESI\esg_agent\RAG_full_bigger\rag2_out\activities_extracted_clean.csv")
OUT_XLSX = Path(r"C:\Universita\TESI\esg_agent\RAG_full_bigger\rag2_out\activities_extracted_clean.xlsx")

# =========================
# REGEX parsing Sub_activity_code
# =========================
LABEL_CODE_RE = re.compile(r"^\s*([A-Za-z]{2,6})\s*[-:/]?\s*([0-9]+(?:\.[0-9]+)?)\s*\*?\s*$")
CODE_LABEL_REVERSED_RE = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*[-:/]?\s*([A-Za-z]{2,6})\s*\*?\s*$")
CODE_ONLY_RE = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*\*?\s*$")
LABEL_ONLY_RE = re.compile(r"^\s*([A-Za-z]{2,6})\s*\*?\s*$")
_MULTI_SPACE_RE = re.compile(r"\s+")


def _norm_space(s: str) -> str:
    return _MULTI_SPACE_RE.sub(" ", (s or "").strip())


def split_label_and_numeric(raw: str) -> Tuple[str, str]:
    s = "" if raw is None else str(raw)
    s = s.replace("\u00a0", " ")
    s = _norm_space(s)

    if not s:
        return "", ""

    m = LABEL_CODE_RE.match(s)
    if m:
        return m.group(1).upper().strip(), m.group(2).strip()

    m = CODE_LABEL_REVERSED_RE.match(s)
    if m:
        return m.group(2).upper().strip(), m.group(1).strip()

    m = CODE_ONLY_RE.match(s)
    if m:
        return "", m.group(1).strip()

    m = LABEL_ONLY_RE.match(s)
    if m:
        return m.group(1).upper().strip(), ""

    label_any = re.search(r"\b([A-Za-z]{2,6})\b", s)
    code_any = re.search(r"\b([0-9]+(?:\.[0-9]+)?)\b", s)
    if label_any and code_any:
        return label_any.group(1).upper().strip(), code_any.group(1).strip()

    return "", s


def print_duplicate_subcodes_per_company(df: pd.DataFrame) -> None:
    tmp = df.copy()
    tmp["company"] = tmp["company"].astype(str).str.strip()
    tmp["report_year"] = tmp["report_year"].astype(str).str.strip()
    tmp["activity"] = tmp["activity"].astype(str).str.strip()

    tmp["subcode_raw_norm"] = tmp["Sub_activity_code"].astype(str).apply(_norm_space)
    tmp = tmp[tmp["subcode_raw_norm"] != ""].copy()

    if tmp.empty:
        print("\n[INFO] Nessun Sub_activity_code non vuoto: niente duplicati da stampare.\n")
        return

    dup = (
        tmp.groupby(["company", "subcode_raw_norm"], dropna=False)
           .size()
           .reset_index(name="n")
    )
    dup = dup[dup["n"] >= 2].copy()

    if dup.empty:
        print("\n[INFO] Nessun duplicato trovato su Sub_activity_code (raw) per company.\n")
        return

    print("\n========== DUPLICATI Sub_activity_code (RAW) PER AZIENDA ==========")
    for _, r in dup.sort_values(["company", "subcode_raw_norm"]).iterrows():
        comp = r["company"]
        subcode = r["subcode_raw_norm"]
        n = int(r["n"])

        rows = tmp[(tmp["company"] == comp) & (tmp["subcode_raw_norm"] == subcode)].copy()
        rows = rows.sort_values(["report_year", "activity"])

        print(f"\nCOMPANY: {comp} | Sub_activity_code: '{subcode}' | occorrenze: {n}")
        for _, rr in rows.iterrows():
            print(f"  - YEAR: {rr['report_year']} | ACTIVITY: {rr['activity']}")
    print("\n==================================================================\n")


def drop_2023_if_same_in_2024(df: pd.DataFrame) -> pd.DataFrame:
    """
    Regola richiesta:
    se per la stessa company esiste una riga 2023 e una riga 2024 con
    - activity identica (normalizzata spazi)
    - subcode identico (normalizzato spazi)  [usiamo la stringa raw, non label/code]
    allora elimina la riga 2023 e tiene la 2024.
    """
    tmp = df.copy()

    tmp["company_norm"] = tmp["company"].astype(str).str.strip()
    tmp["year_norm"] = tmp["report_year"].astype(str).str.strip()
    tmp["activity_norm"] = tmp["activity"].astype(str).apply(_norm_space)
    tmp["subcode_raw_norm"] = tmp["Sub_activity_code"].astype(str).apply(_norm_space)

    # chiave confronto cross-year
    tmp["cross_key"] = tmp["company_norm"] + "||" + tmp["activity_norm"] + "||" + tmp["subcode_raw_norm"]

    keys_2024 = set(tmp.loc[tmp["year_norm"] == "2024", "cross_key"].tolist())

    # drop: righe 2023 che hanno una corrispondente in 2024
    mask_drop = (tmp["year_norm"] == "2023") & (tmp["cross_key"].isin(keys_2024))

    dropped = int(mask_drop.sum())
    if dropped:
        print(f"[INFO] Drop 2023 duplicates covered by 2024: {dropped:,}")

    tmp = tmp.loc[~mask_drop].copy()

    # rimuovi colonne helper
    tmp = tmp.drop(columns=["company_norm", "year_norm", "activity_norm", "subcode_raw_norm", "cross_key"])

    return tmp


def main() -> None:
    df = pd.read_csv(IN_CSV, dtype=str).fillna("")
    df.columns = [c.strip() for c in df.columns]

    required = {"company", "report_year", "activity", "Sub_activity_code"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Mancano colonne nel CSV: {sorted(missing)}")

    # (1) stampa duplicati raw
    print_duplicate_subcodes_per_company(df)

    # pulizia base
    df["company"] = df["company"].astype(str).str.strip()
    df["report_year"] = df["report_year"].astype(str).str.strip()
    df["activity"] = df["activity"].astype(str).str.strip()
    df["Sub_activity_code"] = df["Sub_activity_code"].astype(str).str.strip()

    # ✅ nuova regola: elimina 2023 se identico a 2024
    df = drop_2023_if_same_in_2024(df)

    # split label / code_numeric
    df[["label", "code_numeric"]] = df["Sub_activity_code"].apply(
        lambda x: pd.Series(split_label_and_numeric(x))
    )

    # safety swap: label="3.9", code="CCM"
    def _swap_if_inverted(row) -> Tuple[str, str]:
        lab = (row["label"] or "").strip()
        cod = (row["code_numeric"] or "").strip()
        if re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", lab) and re.fullmatch(r"[A-Za-z]{2,6}", cod):
            return cod.upper(), lab
        return lab, cod

    df[["label", "code_numeric"]] = df.apply(lambda r: pd.Series(_swap_if_inverted(r)), axis=1)

    # pulizia finale
    df["label"] = df["label"].astype(str).str.strip()
    df["code_numeric"] = df["code_numeric"].astype(str).str.strip()

    df = df[["company", "report_year", "activity", "label", "code_numeric"]].copy()

    # Salvataggi
    df.to_csv(OUT_CSV, index=False, encoding="utf-8")
    df.to_excel(OUT_XLSX, index=False)

    print(f"[OK] Salvato CSV : {OUT_CSV}")
    print(f"[OK] Salvato XLSX: {OUT_XLSX}")
    print("\nPreview:")
    print(df.head(20).to_string(index=False))


if __name__ == "__main__":
    main()

# python C:\Universita\TESI\esg_agent\RAG_full_bigger\4_clean_rag_activities.py