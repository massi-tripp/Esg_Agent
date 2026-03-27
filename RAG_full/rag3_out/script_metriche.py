from __future__ import annotations

import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd

# questo codice calcola metriche di base sui risultati di estrazione e retrieval,
# a livello di singole chiamate, report, e complessivo.
# lo fa per entrambe le opzioni in entrambi i dataset, salva anche csv di dettaglio,
# e in più salva un JSON unico finale nella stessa cartella dello script.

# ============================================================
# CONFIGURATION
# ============================================================

CONFIGS = [
    {
        "label": "Restrictive-A",
        "base_dir": Path(r"C:\Universita\TESI\esg_agent\RAG_full\rag3_out\optionA"),
    },
    {
        "label": "Restrictive-B",
        "base_dir": Path(r"C:\Universita\TESI\esg_agent\RAG_full\rag3_out\optionB"),
    },
    {
        "label": "Robust-A",
        "base_dir": Path(r"C:\Universita\TESI\esg_agent\RAG_full_bigger\rag3_out\optionA"),
    },
    {
        "label": "Robust-B",
        "base_dir": Path(r"C:\Universita\TESI\esg_agent\RAG_full_bigger\rag3_out\optionB"),
    },
]

OUTPUT_DIRNAME = "analysis_option3"

EXTRACTION_JSONL = "activities_extracted_metrics.jsonl"
EXTRACTION_SUMMARY_JSON = "activities_extracted_metrics_summary.json"

RETRIEVAL_DOC_JSONL = "retrieval_doc_metrics.jsonl"
RETRIEVAL_SUMMARY_JSON = "retrieval_doc_metrics_summary.json"

DOC_KEY_COLS = ["slug", "company", "report_year", "source_md"]


# ============================================================
# JSON HELPERS
# ============================================================
def _json_safe(obj: Any) -> Any:
    """
    Converte oggetti Python / numpy / pandas in formato serializzabile JSON.
    """
    if obj is None:
        return None

    if isinstance(obj, (str, int, bool)):
        return obj

    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj

    if isinstance(obj, (np.integer,)):
        return int(obj)

    if isinstance(obj, (np.floating,)):
        val = float(obj)
        if math.isnan(val) or math.isinf(val):
            return None
        return val

    if isinstance(obj, (pd.Timestamp, datetime)):
        return obj.isoformat()

    if isinstance(obj, Path):
        return str(obj)

    if isinstance(obj, pd.DataFrame):
        if obj.empty:
            return []
        df = obj.copy()
        df = df.replace({np.nan: None})
        return [
            {str(k): _json_safe(v) for k, v in row.items()}
            for row in df.to_dict(orient="records")
        ]

    if isinstance(obj, pd.Series):
        s = obj.replace({np.nan: None})
        return {str(k): _json_safe(v) for k, v in s.to_dict().items()}

    if isinstance(obj, dict):
        return {str(k): _json_safe(v) for k, v in obj.items()}

    if isinstance(obj, (list, tuple, set)):
        return [_json_safe(v) for v in obj]

    return str(obj)


def _save_json_report(report_dict: Dict[str, Any], filename: str = "option3_full_report.json") -> Path:
    script_dir = Path(__file__).resolve().parent
    out_path = script_dir / filename
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(_json_safe(report_dict), f, ensure_ascii=False, indent=2)
    return out_path


# ============================================================
# IO HELPERS
# ============================================================
def load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def load_jsonl(path: Path) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return pd.DataFrame(rows)


# ============================================================
# GENERIC STATS
# ============================================================
def describe_series(s: pd.Series) -> Dict[str, float]:
    x = pd.to_numeric(s, errors="coerce").dropna()

    if x.empty:
        return {
            "count": 0,
            "min": np.nan,
            "q1": np.nan,
            "mean": np.nan,
            "median": np.nan,
            "q3": np.nan,
            "p90": np.nan,
            "p95": np.nan,
            "max": np.nan,
            "std": np.nan,
            "iqr": np.nan,
            "sum": np.nan,
        }

    q1 = float(x.quantile(0.25))
    q3 = float(x.quantile(0.75))

    return {
        "count": int(x.count()),
        "min": float(x.min()),
        "q1": q1,
        "mean": float(x.mean()),
        "median": float(x.median()),
        "q3": q3,
        "p90": float(x.quantile(0.90)),
        "p95": float(x.quantile(0.95)),
        "max": float(x.max()),
        "std": float(x.std(ddof=1)) if len(x) > 1 else 0.0,
        "iqr": q3 - q1,
        "sum": float(x.sum()),
    }


def safe_mean(series: pd.Series) -> float:
    x = pd.to_numeric(series, errors="coerce").dropna()
    return float(x.mean()) if not x.empty else np.nan


def mode_or_nan(series: pd.Series):
    x = series.dropna()
    if x.empty:
        return np.nan
    m = x.mode()
    return m.iloc[0] if not m.empty else np.nan


# ============================================================
# NORMALIZATION
# ============================================================
def prepare_extraction_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    if "report_year" in out.columns:
        out["report_year"] = out["report_year"].astype(str)

    numeric_cols = [
        "retrieval_rank",
        "retrieval_top_k",
        "chunk_id",
        "start_page",
        "end_page",
        "chunk_tokens_est",
        "prompt_tokens_est",
        "elapsed_s",
        "attempt_used",
        "prompt_tokens_api",
        "completion_tokens_api",
        "total_tokens_api",
        "n_activities_raw",
        "n_rows_normalized",
        "retrieval_score_hybrid",
        "retrieval_score_bm25_raw",
        "retrieval_score_emb_raw",
    ]
    for col in numeric_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    if "status" not in out.columns:
        out["status"] = "ok"

    return out


def prepare_doc_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    if "report_year" in out.columns:
        out["report_year"] = out["report_year"].astype(str)

    numeric_cols = [
        "top_k_requested",
        "w_bm25",
        "w_emb",
        "n_queries",
        "n_index_chunks",
        "n_retrieved_chunks",
        "index_elapsed_s",
        "retrieval_elapsed_s",
        "extraction_elapsed_sum_s",
        "doc_total_elapsed_s",
        "n_llm_calls_ok",
        "n_llm_calls_error",
    ]
    for col in numeric_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    if "status" not in out.columns:
        out["status"] = "ok"

    out["coverage_ratio"] = np.where(
        pd.to_numeric(out.get("n_index_chunks"), errors="coerce") > 0,
        pd.to_numeric(out.get("n_retrieved_chunks"), errors="coerce")
        / pd.to_numeric(out.get("n_index_chunks"), errors="coerce"),
        np.nan,
    )

    out["topk_capped"] = (
        pd.to_numeric(out.get("n_index_chunks"), errors="coerce")
        > pd.to_numeric(out.get("n_retrieved_chunks"), errors="coerce")
    )
    out["retrieved_all_chunks"] = (
        pd.to_numeric(out.get("n_index_chunks"), errors="coerce")
        == pd.to_numeric(out.get("n_retrieved_chunks"), errors="coerce")
    )

    return out


# ============================================================
# SUMMARIES
# ============================================================
def extraction_overall_summary(df_all: pd.DataFrame) -> pd.DataFrame:
    df_ok = df_all[df_all["status"] == "ok"].copy()
    df_nz = df_ok[df_ok["n_rows_normalized"] > 0].copy()

    row = {
        "raw_total_calls": len(df_all),
        "successful_calls": len(df_ok),
        "failed_calls": int((df_all["status"] != "ok").sum()),
        "success_share": len(df_ok) / len(df_all) if len(df_all) else np.nan,
        "failure_share": (df_all["status"] != "ok").mean() if len(df_all) else np.nan,
        "share_nonzero_chunks": (df_ok["n_rows_normalized"] > 0).mean() if len(df_ok) else np.nan,
        "total_rows_normalized": df_ok["n_rows_normalized"].sum(),
        "mean_rows_normalized": safe_mean(df_ok["n_rows_normalized"]),
        "mean_rows_nonzero_only": safe_mean(df_nz["n_rows_normalized"]),
        "total_tokens_api_sum": df_ok["total_tokens_api"].sum(),
        "mean_total_tokens_api": safe_mean(df_ok["total_tokens_api"]),
        "mean_prompt_tokens_api": safe_mean(df_ok["prompt_tokens_api"]),
        "mean_completion_tokens_api": safe_mean(df_ok["completion_tokens_api"]),
        "sum_elapsed_s": df_ok["elapsed_s"].sum(),
        "mean_elapsed_s": safe_mean(df_ok["elapsed_s"]),
        "median_elapsed_s": float(df_ok["elapsed_s"].median()) if len(df_ok) else np.nan,
        "mean_chunk_tokens_est": safe_mean(df_ok["chunk_tokens_est"]),
        "mean_prompt_tokens_est": safe_mean(df_ok["prompt_tokens_est"]),
    }
    return pd.DataFrame([row])


def retrieval_overall_summary(df_doc_all: pd.DataFrame) -> pd.DataFrame:
    df_ok = df_doc_all[df_doc_all["status"] == "ok"].copy()

    row = {
        "raw_total_docs": len(df_doc_all),
        "successful_docs": len(df_ok),
        "failed_docs": int((df_doc_all["status"] != "ok").sum()),
        "success_share": len(df_ok) / len(df_doc_all) if len(df_doc_all) else np.nan,
        "top_k_requested_mode": mode_or_nan(df_ok["top_k_requested"]) if "top_k_requested" in df_ok.columns else np.nan,
        "n_queries_mode": mode_or_nan(df_ok["n_queries"]) if "n_queries" in df_ok.columns else np.nan,
        "mean_w_bm25": safe_mean(df_ok["w_bm25"]) if "w_bm25" in df_ok.columns else np.nan,
        "mean_w_emb": safe_mean(df_ok["w_emb"]) if "w_emb" in df_ok.columns else np.nan,
        "sum_index_elapsed_s": df_ok["index_elapsed_s"].sum(),
        "mean_index_elapsed_s": safe_mean(df_ok["index_elapsed_s"]),
        "sum_retrieval_elapsed_s": df_ok["retrieval_elapsed_s"].sum(),
        "mean_retrieval_elapsed_s": safe_mean(df_ok["retrieval_elapsed_s"]),
        "sum_extraction_elapsed_sum_s": df_ok["extraction_elapsed_sum_s"].sum(),
        "mean_extraction_elapsed_sum_s": safe_mean(df_ok["extraction_elapsed_sum_s"]),
        "sum_doc_total_elapsed_s": df_ok["doc_total_elapsed_s"].sum(),
        "mean_doc_total_elapsed_s": safe_mean(df_ok["doc_total_elapsed_s"]),
        "mean_n_index_chunks": safe_mean(df_ok["n_index_chunks"]),
        "mean_n_retrieved_chunks": safe_mean(df_ok["n_retrieved_chunks"]),
        "share_retrieved_all_chunks": safe_mean(df_ok["retrieved_all_chunks"].astype(float)),
        "share_topk_capped_docs": safe_mean(df_ok["topk_capped"].astype(float)),
        "mean_coverage_ratio": safe_mean(df_ok["coverage_ratio"]),
        "sum_llm_calls_ok": df_ok["n_llm_calls_ok"].sum(),
        "sum_llm_calls_error": df_ok["n_llm_calls_error"].sum(),
    }
    return pd.DataFrame([row])


def year_summary(df_extr_all: pd.DataFrame, df_doc_all: pd.DataFrame) -> pd.DataFrame:
    df_extr_ok = df_extr_all[df_extr_all["status"] == "ok"].copy()
    df_doc_ok = df_doc_all[df_doc_all["status"] == "ok"].copy()

    years = sorted(
        set(df_extr_ok["report_year"].dropna().astype(str).tolist())
        | set(df_doc_ok["report_year"].dropna().astype(str).tolist())
    )

    rows = []
    for year in years:
        a = df_extr_ok[df_extr_ok["report_year"] == year].copy()
        d = df_doc_ok[df_doc_ok["report_year"] == year].copy()
        a_nz = a[a["n_rows_normalized"] > 0].copy()

        rows.append(
            {
                "report_year": year,
                "n_success_calls": len(a),
                "n_nonzero_calls": int((a["n_rows_normalized"] > 0).sum()),
                "share_nonzero_calls": (a["n_rows_normalized"] > 0).mean() if len(a) else np.nan,
                "mean_elapsed_s": safe_mean(a["elapsed_s"]),
                "mean_prompt_tokens_api": safe_mean(a["prompt_tokens_api"]),
                "mean_completion_tokens_api": safe_mean(a["completion_tokens_api"]),
                "mean_total_tokens_api": safe_mean(a["total_tokens_api"]),
                "total_rows_normalized": a["n_rows_normalized"].sum(),
                "mean_rows_normalized": safe_mean(a["n_rows_normalized"]),
                "mean_rows_nonzero_only": safe_mean(a_nz["n_rows_normalized"]),
                "n_docs": len(d),
                "mean_index_elapsed_s": safe_mean(d["index_elapsed_s"]),
                "mean_retrieval_elapsed_s": safe_mean(d["retrieval_elapsed_s"]),
                "mean_extraction_elapsed_sum_s": safe_mean(d["extraction_elapsed_sum_s"]),
                "mean_doc_total_elapsed_s": safe_mean(d["doc_total_elapsed_s"]),
                "mean_n_index_chunks": safe_mean(d["n_index_chunks"]),
                "mean_n_retrieved_chunks": safe_mean(d["n_retrieved_chunks"]),
                "mean_coverage_ratio": safe_mean(d["coverage_ratio"]),
                "share_topk_capped_docs": safe_mean(d["topk_capped"].astype(float)) if len(d) else np.nan,
                "share_retrieved_all_chunks": safe_mean(d["retrieved_all_chunks"].astype(float)) if len(d) else np.nan,
            }
        )

    return pd.DataFrame(rows)


def report_aggregation(df_extr_all: pd.DataFrame, df_doc_all: pd.DataFrame) -> pd.DataFrame:
    df_extr_ok = df_extr_all[df_extr_all["status"] == "ok"].copy()
    df_doc_ok = df_doc_all[df_doc_all["status"] == "ok"].copy()

    base_aggs = {
        "elapsed_s": "sum",
        "prompt_tokens_api": "sum",
        "completion_tokens_api": "sum",
        "total_tokens_api": "sum",
        "chunk_tokens_est": "mean",
        "retrieval_rank": "count",
    }

    rows_sum = (
        df_extr_ok.groupby(DOC_KEY_COLS)
        .agg(base_aggs)
        .reset_index()
        .rename(
            columns={
                "elapsed_s": "llm_total_elapsed_sum_s",
                "prompt_tokens_api": "total_prompt_tokens_api_sum",
                "completion_tokens_api": "total_completion_tokens_api_sum",
                "total_tokens_api": "total_tokens_api_sum",
                "chunk_tokens_est": "mean_chunk_tokens_est",
                "retrieval_rank": "n_llm_calls_ok",
            }
        )
    )

    rows_stats = (
        df_extr_ok.groupby(DOC_KEY_COLS)["n_rows_normalized"]
        .agg(["sum", "mean", "max"])
        .reset_index()
        .rename(
            columns={
                "sum": "n_rows_normalized_doc",
                "mean": "mean_rows_per_retrieved_chunk",
                "max": "max_rows_single_chunk",
            }
        )
    )

    out = rows_sum.merge(rows_stats, on=DOC_KEY_COLS, how="left")

    if "retrieval_score_hybrid" in df_extr_ok.columns:
        tmp = (
            df_extr_ok.groupby(DOC_KEY_COLS)["retrieval_score_hybrid"]
            .mean()
            .reset_index(name="mean_hybrid_score")
        )
        out = out.merge(tmp, on=DOC_KEY_COLS, how="left")

    if "retrieval_score_bm25_raw" in df_extr_ok.columns:
        tmp = (
            df_extr_ok.groupby(DOC_KEY_COLS)["retrieval_score_bm25_raw"]
            .mean()
            .reset_index(name="mean_bm25_score_raw")
        )
        out = out.merge(tmp, on=DOC_KEY_COLS, how="left")

    if "retrieval_score_emb_raw" in df_extr_ok.columns:
        tmp = (
            df_extr_ok.groupby(DOC_KEY_COLS)["retrieval_score_emb_raw"]
            .mean()
            .reset_index(name="mean_emb_score_raw")
        )
        out = out.merge(tmp, on=DOC_KEY_COLS, how="left")

    nonzero = (
        df_extr_ok.assign(nonzero_chunk=(df_extr_ok["n_rows_normalized"] > 0).astype(int))
        .groupby(DOC_KEY_COLS)["nonzero_chunk"]
        .sum()
        .reset_index(name="n_nonzero_chunks")
    )

    out = df_doc_ok.merge(out, on=DOC_KEY_COLS, how="left").merge(nonzero, on=DOC_KEY_COLS, how="left")
    out["n_nonzero_chunks"] = out["n_nonzero_chunks"].fillna(0).astype(int)
    out["share_nonzero_retrieved_chunks"] = np.where(
        out["n_retrieved_chunks"] > 0,
        out["n_nonzero_chunks"] / out["n_retrieved_chunks"],
        np.nan,
    )

    return out.sort_values(["doc_total_elapsed_s", "total_tokens_api_sum"], ascending=[False, False])


def rank_summary(df_extr_all: pd.DataFrame) -> pd.DataFrame:
    df_ok = df_extr_all[df_extr_all["status"] == "ok"].copy()
    grp = df_ok.groupby("retrieval_rank", dropna=False)

    out = grp.agg(
        n_calls=("elapsed_s", "count"),
        mean_elapsed_s=("elapsed_s", "mean"),
        median_elapsed_s=("elapsed_s", "median"),
        mean_total_tokens_api=("total_tokens_api", "mean"),
        total_rows_normalized=("n_rows_normalized", "sum"),
        mean_rows_normalized=("n_rows_normalized", "mean"),
        median_rows_normalized=("n_rows_normalized", "median"),
    ).reset_index()

    nz = (
        grp["n_rows_normalized"]
        .apply(lambda s: (pd.to_numeric(s, errors="coerce") > 0).mean())
        .reset_index(name="share_nonzero_chunks")
    )
    out = out.merge(nz, on="retrieval_rank", how="left")

    if "retrieval_score_hybrid" in df_ok.columns:
        out = out.merge(
            grp["retrieval_score_hybrid"].mean().reset_index(name="mean_hybrid_score"),
            on="retrieval_rank",
            how="left",
        )

    if "retrieval_score_bm25_raw" in df_ok.columns:
        out = out.merge(
            grp["retrieval_score_bm25_raw"].mean().reset_index(name="mean_bm25_score_raw"),
            on="retrieval_rank",
            how="left",
        )

    if "retrieval_score_emb_raw" in df_ok.columns:
        out = out.merge(
            grp["retrieval_score_emb_raw"].mean().reset_index(name="mean_emb_score_raw"),
            on="retrieval_rank",
            how="left",
        )

    return out.sort_values("retrieval_rank").reset_index(drop=True)


def cumulative_rank_utility(df_extr_all: pd.DataFrame, df_doc_all: pd.DataFrame) -> pd.DataFrame:
    df_ok = df_extr_all[df_extr_all["status"] == "ok"].copy()
    doc_ok = df_doc_all[df_doc_all["status"] == "ok"].copy()

    if df_ok.empty or doc_ok.empty or "retrieval_rank" not in df_ok.columns:
        return pd.DataFrame()

    base_docs = doc_ok[DOC_KEY_COLS].drop_duplicates().copy()
    total_rows_all = df_ok["n_rows_normalized"].sum()
    max_rank = int(df_ok["retrieval_rank"].max())

    rows = []
    for k in range(1, max_rank + 1):
        tmp = (
            df_ok[df_ok["retrieval_rank"] <= k]
            .groupby(DOC_KEY_COLS)["n_rows_normalized"]
            .sum()
            .reset_index(name="cum_rows")
        )
        tmp = base_docs.merge(tmp, on=DOC_KEY_COLS, how="left")
        tmp["cum_rows"] = tmp["cum_rows"].fillna(0)

        cum_rows = tmp["cum_rows"].sum()

        rows.append(
            {
                "top_k": k,
                "cumulative_rows": cum_rows,
                "share_total_rows_captured": cum_rows / total_rows_all if total_rows_all else np.nan,
                "mean_cumulative_rows_per_doc": tmp["cum_rows"].mean(),
                "median_cumulative_rows_per_doc": tmp["cum_rows"].median(),
                "share_docs_with_any_output": (tmp["cum_rows"] > 0).mean(),
            }
        )

    return pd.DataFrame(rows)


def score_band_summary(df_extr_all: pd.DataFrame) -> pd.DataFrame:
    df_ok = df_extr_all[df_extr_all["status"] == "ok"].copy()

    if "retrieval_score_hybrid" not in df_ok.columns:
        return pd.DataFrame()

    score = pd.to_numeric(df_ok["retrieval_score_hybrid"], errors="coerce")
    if score.dropna().nunique() < 4:
        return pd.DataFrame()

    bands = pd.qcut(score, q=4, duplicates="drop")

    tmp = df_ok.copy()
    tmp["hybrid_score_band"] = bands.astype(str)

    out = (
        tmp.groupby("hybrid_score_band", dropna=False)
        .agg(
            n_calls=("elapsed_s", "count"),
            mean_hybrid_score=("retrieval_score_hybrid", "mean"),
            mean_bm25_score_raw=("retrieval_score_bm25_raw", "mean"),
            mean_emb_score_raw=("retrieval_score_emb_raw", "mean"),
            share_nonzero_chunks=("n_rows_normalized", lambda s: (pd.to_numeric(s, errors="coerce") > 0).mean()),
            mean_rows_normalized=("n_rows_normalized", "mean"),
            median_rows_normalized=("n_rows_normalized", "median"),
            mean_total_tokens_api=("total_tokens_api", "mean"),
        )
        .reset_index()
        .sort_values("mean_hybrid_score", ascending=False)
    )

    return out


def top_single_calls(df_extr_all: pd.DataFrame, n: int = 25) -> pd.DataFrame:
    df_ok = df_extr_all[df_extr_all["status"] == "ok"].copy()

    cols = [
        "company",
        "report_year",
        "retrieval_rank",
        "chunk_id",
        "start_page",
        "end_page",
        "elapsed_s",
        "prompt_tokens_api",
        "completion_tokens_api",
        "total_tokens_api",
        "n_rows_normalized",
        "retrieval_score_hybrid",
        "retrieval_score_bm25_raw",
        "retrieval_score_emb_raw",
        "attempt_used",
    ]
    cols = [c for c in cols if c in df_ok.columns]

    return df_ok.sort_values(["elapsed_s", "total_tokens_api"], ascending=[False, False])[cols].head(n)


# ============================================================
# MAIN ANALYSIS
# ============================================================
def analyze_one_config(label: str, base_dir: Path) -> Dict[str, Any]:
    extr_jsonl = base_dir / EXTRACTION_JSONL
    retr_jsonl = base_dir / RETRIEVAL_DOC_JSONL

    if not extr_jsonl.exists() or not retr_jsonl.exists():
        raise FileNotFoundError(
            f"Missing required files for {label}. Expected at least:\n- {extr_jsonl}\n- {retr_jsonl}"
        )

    out_dir = base_dir / OUTPUT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)

    df_extr_all = prepare_extraction_df(load_jsonl(extr_jsonl))
    df_doc_all = prepare_doc_df(load_jsonl(retr_jsonl))

    df_extr_ok = df_extr_all[df_extr_all["status"] == "ok"].copy()
    df_doc_ok = df_doc_all[df_doc_all["status"] == "ok"].copy()

    extraction_overall = extraction_overall_summary(df_extr_all)
    retrieval_overall = retrieval_overall_summary(df_doc_all)
    yearly = year_summary(df_extr_all, df_doc_all)
    reports = report_aggregation(df_extr_all, df_doc_all)
    ranks = rank_summary(df_extr_all)
    cum_ranks = cumulative_rank_utility(df_extr_all, df_doc_all)
    score_bands = score_band_summary(df_extr_all)
    top_calls = top_single_calls(df_extr_all, n=25)

    top_slowest_reports = reports.sort_values("doc_total_elapsed_s", ascending=False).head(20)
    top_output_reports = reports.sort_values("n_rows_normalized_doc", ascending=False).head(20)
    top_token_reports = reports.sort_values("total_tokens_api_sum", ascending=False).head(20)
    top_index_reports = reports.sort_values("n_index_chunks", ascending=False).head(20)

    extraction_descriptives = {
        col: describe_series(df_extr_ok[col])
        for col in [
            "elapsed_s",
            "chunk_tokens_est",
            "prompt_tokens_est",
            "prompt_tokens_api",
            "completion_tokens_api",
            "total_tokens_api",
            "n_rows_normalized",
        ]
        if col in df_extr_ok.columns
    }

    retrieval_descriptives = {
        col: describe_series(df_doc_ok[col])
        for col in [
            "index_elapsed_s",
            "retrieval_elapsed_s",
            "extraction_elapsed_sum_s",
            "doc_total_elapsed_s",
            "n_index_chunks",
            "n_retrieved_chunks",
            "coverage_ratio",
        ]
        if col in df_doc_ok.columns
    }

    checks: Dict[str, Any] = {}

    # Optional checks against provided summary JSONs
    extr_summary_json = base_dir / EXTRACTION_SUMMARY_JSON
    retr_summary_json = base_dir / RETRIEVAL_SUMMARY_JSON

    if extr_summary_json.exists():
        extr_summary = load_json(extr_summary_json)
        checks["extraction_summary_check"] = {
            "summary_overall_elapsed_count": extr_summary.get("overall", {}).get("elapsed_s", {}).get("count"),
            "computed_successful_calls": len(df_extr_ok),
        }

    if retr_summary_json.exists():
        retr_summary = load_json(retr_summary_json)
        checks["retrieval_summary_check"] = {
            "summary_overall_doc_total_elapsed_s_count": retr_summary.get("overall", {}).get("doc_total_elapsed_s", {}).get("count"),
            "computed_successful_docs": len(df_doc_ok),
        }

    # Save CSVs / JSONs
    extraction_overall.to_csv(out_dir / "option3_extraction_overall.csv", index=False)
    retrieval_overall.to_csv(out_dir / "option3_retrieval_overall.csv", index=False)
    yearly.to_csv(out_dir / "option3_year_summary.csv", index=False)
    reports.to_csv(out_dir / "option3_report_agg.csv", index=False)
    ranks.to_csv(out_dir / "option3_rank_summary.csv", index=False)
    cum_ranks.to_csv(out_dir / "option3_cumulative_rank_utility.csv", index=False)
    score_bands.to_csv(out_dir / "option3_score_band_summary.csv", index=False)
    top_slowest_reports.to_csv(out_dir / "option3_top_slowest_reports.csv", index=False)
    top_output_reports.to_csv(out_dir / "option3_top_output_reports.csv", index=False)
    top_token_reports.to_csv(out_dir / "option3_top_token_reports.csv", index=False)
    top_index_reports.to_csv(out_dir / "option3_top_index_reports.csv", index=False)
    top_calls.to_csv(out_dir / "option3_top_single_calls.csv", index=False)

    with (out_dir / "option3_extraction_descriptives.json").open("w", encoding="utf-8") as f:
        json.dump(_json_safe(extraction_descriptives), f, ensure_ascii=False, indent=2)

    with (out_dir / "option3_retrieval_descriptives.json").open("w", encoding="utf-8") as f:
        json.dump(_json_safe(retrieval_descriptives), f, ensure_ascii=False, indent=2)

    saved_files = [str(p) for p in sorted(out_dir.glob("option3_*"))]

    # versioni con colonna config per i csv globali
    extraction_overall_global = extraction_overall.copy()
    retrieval_overall_global = retrieval_overall.copy()
    yearly_global = yearly.copy()
    ranks_global = ranks.copy()
    cum_ranks_global = cum_ranks.copy()
    score_bands_global = score_bands.copy()

    extraction_overall_global.insert(0, "config", label)
    retrieval_overall_global.insert(0, "config", label)
    yearly_global.insert(0, "config", label)
    ranks_global.insert(0, "config", label)
    if not cum_ranks_global.empty:
        cum_ranks_global.insert(0, "config", label)
    if not score_bands_global.empty:
        score_bands_global.insert(0, "config", label)

    return {
        "label": label,
        "base_dir": str(base_dir),
        "out_dir": str(out_dir),
        "saved_files": saved_files,
        "checks": checks,
        "tables_for_json": {
            "extraction_overall": extraction_overall,
            "retrieval_overall": retrieval_overall,
            "year_summary": yearly,
            "rank_summary": ranks,
            "cumulative_rank_utility": cum_ranks,
            "score_band_summary": score_bands,
            "top_slowest_reports": top_slowest_reports,
            "top_reports_by_extracted_rows": top_output_reports,
            "top_reports_by_total_tokens": top_token_reports,
            "top_reports_by_index_size": top_index_reports,
            "top_single_slowest_calls": top_calls,
        },
        "descriptives": {
            "extraction": extraction_descriptives,
            "retrieval": retrieval_descriptives,
        },
        "global_frames": {
            "extraction_overall": extraction_overall_global,
            "retrieval_overall": retrieval_overall_global,
            "yearly": yearly_global,
            "ranks": ranks_global,
            "cum_ranks": cum_ranks_global,
            "score_bands": score_bands_global,
        },
    }


def main() -> None:
    collected = {
        "extraction_overall": [],
        "retrieval_overall": [],
        "yearly": [],
        "ranks": [],
        "cum_ranks": [],
        "score_bands": [],
    }

    report_json: Dict[str, Any] = {
        "generated_at": datetime.now().isoformat(),
        "script_name": Path(__file__).name,
        "script_path": str(Path(__file__).resolve()),
        "configs": {},
        "skipped_configs": [],
        "global_outputs": {},
    }

    existing_base_dirs = [cfg["base_dir"] for cfg in CONFIGS if cfg["base_dir"].exists()]
    first_existing_base = existing_base_dirs[0] if existing_base_dirs else None

    global_out = Path.cwd() / "analysis_option3_all_configs"
    if first_existing_base is not None:
        global_out = first_existing_base.parent / "analysis_option3_all_configs"

    global_out.mkdir(parents=True, exist_ok=True)

    for cfg in CONFIGS:
        label = cfg["label"]
        base_dir = cfg["base_dir"]

        if not base_dir.exists():
            report_json["skipped_configs"].append(
                {
                    "label": label,
                    "reason": "folder_not_found",
                    "base_dir": str(base_dir),
                }
            )
            print(f"[SKIP] {label} :: folder not found -> {base_dir}")
            continue

        try:
            result = analyze_one_config(label, base_dir)

            report_json["configs"][label] = {
                "base_dir": result["base_dir"],
                "out_dir": result["out_dir"],
                "saved_files": result["saved_files"],
                "checks": result["checks"],
                "descriptives": result["descriptives"],
                "tables": result["tables_for_json"],
            }

            for key in collected:
                if key in result["global_frames"] and not result["global_frames"][key].empty:
                    collected[key].append(result["global_frames"][key])

            print(f"[OK] {label} :: analysis completed")

        except FileNotFoundError as e:
            report_json["skipped_configs"].append(
                {
                    "label": label,
                    "reason": "missing_required_files",
                    "base_dir": str(base_dir),
                    "message": str(e),
                }
            )
            print(f"[SKIP] {label} :: {e}")

    if not any(collected.values()):
        report_json["global_outputs"]["status"] = "no_valid_configuration_analyzed"
        json_path = _save_json_report(report_json, "option3_full_report.json")
        print(f"\nNo valid configuration was analyzed.")
        print(f"JSON salvato in: {json_path}")
        return

    global_csv_files: Dict[str, str] = {}

    for key, frames in collected.items():
        if frames:
            out_csv = global_out / f"{key}.csv"
            pd.concat(frames, ignore_index=True).to_csv(out_csv, index=False)
            global_csv_files[key] = str(out_csv)

    report_json["global_outputs"] = {
        "output_dir": str(global_out),
        "csv_files": global_csv_files,
    }

    json_path = _save_json_report(report_json, "option3_full_report.json")

    print("\n" + "=" * 72)
    print("ANALISI COMPLETATA")
    print("=" * 72)
    print(f"JSON unico salvato in: {json_path}")
    print(f"Directory CSV globali: {global_out}")


if __name__ == "__main__":
    main()