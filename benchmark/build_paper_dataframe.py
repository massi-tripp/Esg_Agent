# File: build_benchmark_dataframe.py

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd


# =========================
# Config
# =========================
DATA_DIR = Path(r"C:\Universita\TESI\esg_agent\benchmark\data")

# Chiavi pesanti da escludere
DROP_TOP_KEYS = {"taxonomy_section", "report_without_taxonomy", "full_report"}

# KPI groups che vogliamo “flattenare”
KPI_GROUPS = {
    "turnoverKPI": "turnover",
    "capexKPI": "capex",
    "opexKPI": "opex",
}

# Mappatura nomi metriche
METRIC_CANON_MAP = {
    # Turnover
    "totalTurnover": "total",
    "eligibleTurnover": "eligible",
    "alignedTurnover": "aligned",
    "nonEligibleTurnover": "non_eligible",

    # Capex
    "totalCapex": "total",
    "eligibleCapex": "eligible",
    "alignedCapex": "aligned",
    "nonEligibleCapex": "non_eligible",
    "eligibleNotAlignedCapex": "eligible_not_aligned",

    # Opex
    "totalOpex": "total",
    "eligibleOpex": "eligible",
    "alignedOpex": "aligned",
    "nonEligibleOpex": "non_eligible",
}

# ✅ Colonne finali da tenere (selezionate già nei record, prima del DF)
FINAL_COLS: List[str] = [
    "company_name",
    "industry",
    "country",
    "revenue",
    "number_of_employees",
    "company_type",
    "sector",
    "currency",
    "units",
    "turnover_total_value",
    "turnover_total_pct",
    "turnover_eligible_value",
    "turnover_eligible_pct",
    "turnover_aligned_value",
    "turnover_aligned_pct",
    "turnover_non_eligible_value",
    "turnover_non_eligible_pct",
    "capex_total_value",
    "capex_total_pct",
    "capex_eligible_value",
    "capex_eligible_pct",
    "capex_aligned_value",
    "capex_aligned_pct",
    "capex_eligible_not_aligned_value",
    "capex_eligible_not_aligned_pct",
    "capex_non_eligible_value",
    "capex_non_eligible_pct",
    "opex_total_value",
    "opex_total_pct",
    "opex_eligible_value",
    "opex_eligible_pct",
    "opex_aligned_value",
    "opex_aligned_pct",
    "opex_non_eligible_value",
    "opex_non_eligible_pct",
    "activity",
]


# =========================
# Helpers
# =========================
def _read_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _safe_get(d: Any, *keys: str, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
        if cur is None:
            return default
    return cur


def _canon_metric_name(metric_key: str) -> str:
    if metric_key in METRIC_CANON_MAP:
        return METRIC_CANON_MAP[metric_key]
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", metric_key)
    s2 = re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1)
    return s2.lower()


def _parse_num0(x: Any) -> float:
    """
    Regola: None / "" / "n/a" / mancante => 0.0
    """
    if x is None:
        return 0.0
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        s = x.strip()
        if s == "" or s.lower() in {"n/a", "na", "null", "none", "-"}:
            return 0.0
        s = s.replace(",", ".")
        m = re.search(r"[-+]?\d*\.?\d+", s)
        if not m:
            return 0.0
        try:
            return float(m.group(0))
        except Exception:
            return 0.0
    return 0.0


def _parse_pct0(x: Any) -> float:
    """
    Regola: None / "" / "n/a" / mancante => 0.0
    """
    if x is None:
        return 0.0
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        s = x.strip()
        if s == "" or s.lower() in {"n/a", "na", "null", "none", "-"}:
            return 0.0
        s = s.replace("%", "").strip()
        s = s.replace(",", ".")
        m = re.search(r"[-+]?\d*\.?\d+", s)
        if not m:
            return 0.0
        try:
            return float(m.group(0))
        except Exception:
            return 0.0
    return 0.0


def _flatten_company_info(company_info: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if not isinstance(company_info, dict):
        return out
    for k, v in company_info.items():
        out[k] = v
    return out


def _flatten_kpis(extracted_kpis: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if not isinstance(extracted_kpis, dict):
        return out

    out["currency"] = extracted_kpis.get("currency")
    out["units"] = extracted_kpis.get("units")

    for group_key, prefix in KPI_GROUPS.items():
        group_obj = extracted_kpis.get(group_key)
        if not isinstance(group_obj, dict):
            continue

        for metric_key, metric_obj in group_obj.items():
            if not isinstance(metric_obj, dict):
                continue

            canon_metric = _canon_metric_name(metric_key)

            value = _parse_num0(metric_obj.get("value"))
            pct = _parse_pct0(metric_obj.get("percentage"))

            out[f"{prefix}_{canon_metric}_value"] = value
            out[f"{prefix}_{canon_metric}_pct"] = pct

    return out


def _extract_activities(obj: Dict[str, Any]) -> List[str]:
    """
    ✅ Nuova regola: se non ci sono attività -> [] (azienda esclusa)
    """
    acts = _safe_get(obj, "taxonomy_data", "activities", default=None)

    if not acts:
        return []

    if isinstance(acts, list):
        cleaned: List[str] = []
        for a in acts:
            if a is None:
                continue
            s = str(a).strip()
            if s:
                cleaned.append(s)
        return cleaned

    # caso stringa singola
    s = str(acts).strip()
    return [s] if s else []


def _select_final_cols(row: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for c in FINAL_COLS:
        out[c] = row.get(c, None)
    return out


def build_dataframe_from_folder(folder: Path) -> pd.DataFrame:
    if not folder.exists():
        raise FileNotFoundError(f"Cartella non trovata: {folder}")

    json_files = sorted(folder.glob("*.json"))
    if not json_files:
        raise FileNotFoundError(f"Nessun file .json trovato in: {folder}")

    records: List[Dict[str, Any]] = []

    for p in json_files:
        try:
            obj = _read_json(p)
        except Exception as e:
            print(f"[WARN] Impossibile leggere {p.name}: {e}")
            continue

        # Drop chiavi pesanti a priori
        for k in DROP_TOP_KEYS:
            obj.pop(k, None)

        # ✅ attività: se assenti -> skip azienda
        activities = _extract_activities(obj)
        if not activities:
            continue

        company_name = obj.get("company_name") or p.stem

        base: Dict[str, Any] = {"company_name": company_name}
        base.update(_flatten_company_info(obj.get("company_info", {})))
        base.update(_flatten_kpis(obj.get("extracted_kpis", {})))

        for act in activities:
            row = dict(base)
            row["activity"] = act
            records.append(_select_final_cols(row))

    df = pd.DataFrame.from_records(records)

    # Tipizzazione: revenue/employees (gli altri KPI sono già float)
    for c in {"revenue", "number_of_employees"}:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Assicura KPI float e 0.0 se NaN (dovrebbero già esserlo)
    for c in df.columns:
        if c.endswith("_value") or c.endswith("_pct"):
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    return df


if __name__ == "__main__":
    df = build_dataframe_from_folder(DATA_DIR)

    print("Shape:", df.shape)
    print("Columns:", list(df.columns))
    print(df.head(10))

    # Salvataggio (opzionale)
    out_xlsx = DATA_DIR.parent / "dataframe_paper.xlsx"
    # XLSX (più comodo da “vedere” direttamente in Excel)
    df.to_excel(out_xlsx, index=False, sheet_name="benchmark")
    print("Saved XLSX to:", out_xlsx)
