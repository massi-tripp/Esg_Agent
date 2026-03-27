from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd

# ============================================================
# PATHS
# ============================================================
BASE_DIR = Path(r"C:\Universita\TESI\esg_agent\RAG_full\rag2_out")

LLM_JSONL = BASE_DIR / "llm_metrics.jsonl"
LLM_SUMMARY_JSON = BASE_DIR / "llm_metrics_summary.json"

DOC_JSONL = BASE_DIR / "doc_metrics.jsonl"           # opzionale / solo cross-check
DOC_SUMMARY_JSON = BASE_DIR / "doc_metrics_summary.json"  # opzionale / solo cross-check

OUT_DIR = BASE_DIR / "analysis_option2"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================
# HELPERS
# ============================================================
def load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows

def series_stats(values: pd.Series) -> Dict[str, Any]:
    s = pd.to_numeric(values, errors="coerce").dropna()
    if len(s) == 0:
        return {
            "count": 0,
            "min": None,
            "q1": None,
            "mean": None,
            "median": None,
            "q3": None,
            "p90": None,
            "p95": None,
            "max": None,
            "std": None,
            "sum": None,
        }

    return {
        "count": int(s.count()),
        "min": float(s.min()),
        "q1": float(s.quantile(0.25)),
        "mean": float(s.mean()),
        "median": float(s.median()),
        "q3": float(s.quantile(0.75)),
        "p90": float(s.quantile(0.90)),
        "p95": float(s.quantile(0.95)),
        "max": float(s.max()),
        "std": float(s.std(ddof=1)) if len(s) > 1 else 0.0,
        "sum": float(s.sum()),
    }

def print_stats_block(title: str, stats_map: Dict[str, Dict[str, Any]]) -> None:
    print(f"\n=== {title} ===")
    for k, v in stats_map.items():
        print(f"\n{k}")
        print(v)

def pct(num: float, den: float) -> float | None:
    if den == 0:
        return None
    return 100.0 * num / den

def sort_and_save(df: pd.DataFrame, filename: str) -> None:
    out = OUT_DIR / filename
    df.to_csv(out, index=False, encoding="utf-8-sig")

# ============================================================
# LOAD RAW DATA
# ============================================================
llm_rows = load_jsonl(LLM_JSONL)
llm_df = pd.DataFrame(llm_rows)

if llm_df.empty:
    raise ValueError(f"Nessun record trovato in {LLM_JSONL}")

# opzionale
doc_df = pd.DataFrame(load_jsonl(DOC_JSONL)) if DOC_JSONL.exists() else pd.DataFrame()

# ============================================================
# CLEAN / TYPES
# ============================================================
numeric_cols = [
    "chunk_index",
    "batch_index",
    "n_chunks",
    "start_page",
    "end_page",
    "chunk_tokens_est",
    "n_evidence_lines_in_batch",
    "prompt_tokens_est",
    "elapsed_s",
    "attempt_used",
    "prompt_tokens_api",
    "completion_tokens_api",
    "total_tokens_api",
    "n_evidence_rows_norm",
    "n_rows_normalized",
]

for col in numeric_cols:
    if col in llm_df.columns:
        llm_df[col] = pd.to_numeric(llm_df[col], errors="coerce")

llm_df["status"] = llm_df["status"].fillna("unknown")
llm_df["stage"] = llm_df["stage"].fillna("unknown")
llm_df["report_year"] = llm_df["report_year"].astype(str)

# successful / failed calls
ok_calls = llm_df[llm_df["status"] == "ok"].copy()
err_calls = llm_df[llm_df["status"] != "ok"].copy()

# ============================================================
# SOURCE OF TRUTH NOTE
# ============================================================
print("NOTE:")
print("- Per Option 2 uso come fonte principale llm_metrics.jsonl.")
if not doc_df.empty:
    print(f"- doc_metrics.jsonl trovato con {len(doc_df)} righe.")
    print(f"- llm_metrics.jsonl contiene {len(llm_df)} righe raw / {len(ok_calls)} call OK.")
    if len(doc_df) < 50 and ok_calls["report_year"].nunique() > 1:
        print("WARNING: doc_metrics.jsonl sembra parziale o non allineato al run completo.")
else:
    print("- doc_metrics.jsonl non trovato: nessun cross-check document-level esterno.")

# ============================================================
# OVERALL LLM METRICS (SUCCESSFUL CALLS ONLY)
# ============================================================
overall_stats = {
    "elapsed_s": series_stats(ok_calls["elapsed_s"]),
    "prompt_tokens_api": series_stats(ok_calls["prompt_tokens_api"]),
    "completion_tokens_api": series_stats(ok_calls["completion_tokens_api"]),
    "total_tokens_api": series_stats(ok_calls["total_tokens_api"]),
}
print_stats_block("OVERALL LLM CALLS (SUCCESS ONLY)", overall_stats)

# ============================================================
# BY STAGE
# ============================================================
stage_tables = []
for stage_name in ["pass1", "pass2"]:
    sdf = ok_calls[ok_calls["stage"] == stage_name].copy()

    row = {
        "stage": stage_name,
        "n_calls": len(sdf),
        "mean_elapsed_s": sdf["elapsed_s"].mean(),
        "median_elapsed_s": sdf["elapsed_s"].median(),
        "mean_prompt_tokens_api": sdf["prompt_tokens_api"].mean(),
        "mean_completion_tokens_api": sdf["completion_tokens_api"].mean(),
        "mean_total_tokens_api": sdf["total_tokens_api"].mean(),
        "sum_elapsed_s": sdf["elapsed_s"].sum(),
        "sum_prompt_tokens_api": sdf["prompt_tokens_api"].sum(),
        "sum_completion_tokens_api": sdf["completion_tokens_api"].sum(),
        "sum_total_tokens_api": sdf["total_tokens_api"].sum(),
    }

    if stage_name == "pass1":
        row["mean_n_evidence_rows_norm"] = sdf["n_evidence_rows_norm"].mean()
        row["sum_n_evidence_rows_norm"] = sdf["n_evidence_rows_norm"].sum()
    else:
        row["mean_n_rows_normalized"] = sdf["n_rows_normalized"].mean()
        row["sum_n_rows_normalized"] = sdf["n_rows_normalized"].sum()

    stage_tables.append(row)

stage_df = pd.DataFrame(stage_tables)
print("\n=== BY STAGE (SUCCESS ONLY) ===")
print(stage_df.to_string(index=False))

# ============================================================
# CALL-LEVEL SUMMARY
# ============================================================
retry_calls = ok_calls[ok_calls["attempt_used"].fillna(1) > 1]

call_level = {
    "raw_total_calls": int(len(llm_df)),
    "successful_calls": int(len(ok_calls)),
    "failed_calls": int(len(err_calls)),
    "success_share": len(ok_calls) / len(llm_df),
    "failure_share": len(err_calls) / len(llm_df),
    "pass1_success_calls": int((ok_calls["stage"] == "pass1").sum()),
    "pass2_success_calls": int((ok_calls["stage"] == "pass2").sum()),
    "pass1_failed_calls": int(((llm_df["stage"] == "pass1") & (llm_df["status"] != "ok")).sum()),
    "pass2_failed_calls": int(((llm_df["stage"] == "pass2") & (llm_df["status"] != "ok")).sum()),
    "retried_success_calls": int(len(retry_calls)),
    "retried_success_share": len(retry_calls) / len(ok_calls) if len(ok_calls) else None,
}
print("\n=== CALL LEVEL ===")
for k, v in call_level.items():
    print(f"{k}: {v}")

# ============================================================
# DOC-LIKE RECONSTRUCTION FROM LLM LOGS
# This is the safest document-level reconstruction when doc_metrics is partial.
# ============================================================
group_cols = ["slug", "company", "report_year"]

doc_like = (
    llm_df.groupby(group_cols, dropna=False)
    .apply(
        lambda g: pd.Series({
            "n_chunks_declared": g["n_chunks"].dropna().max(),
            "pass1_calls_ok": int(((g["stage"] == "pass1") & (g["status"] == "ok")).sum()),
            "pass1_calls_error": int(((g["stage"] == "pass1") & (g["status"] != "ok")).sum()),
            "pass2_calls_ok": int(((g["stage"] == "pass2") & (g["status"] == "ok")).sum()),
            "pass2_calls_error": int(((g["stage"] == "pass2") & (g["status"] != "ok")).sum()),
            "pass1_elapsed_sum_s": g.loc[(g["stage"] == "pass1") & (g["status"] == "ok"), "elapsed_s"].sum(),
            "pass2_elapsed_sum_s": g.loc[(g["stage"] == "pass2") & (g["status"] == "ok"), "elapsed_s"].sum(),
            "llm_total_elapsed_sum_s": g.loc[g["status"] == "ok", "elapsed_s"].sum(),
            "pass1_prompt_tokens_api_sum": g.loc[(g["stage"] == "pass1") & (g["status"] == "ok"), "prompt_tokens_api"].sum(),
            "pass2_prompt_tokens_api_sum": g.loc[(g["stage"] == "pass2") & (g["status"] == "ok"), "prompt_tokens_api"].sum(),
            "total_prompt_tokens_api_sum": g.loc[g["status"] == "ok", "prompt_tokens_api"].sum(),
            "pass1_completion_tokens_api_sum": g.loc[(g["stage"] == "pass1") & (g["status"] == "ok"), "completion_tokens_api"].sum(),
            "pass2_completion_tokens_api_sum": g.loc[(g["stage"] == "pass2") & (g["status"] == "ok"), "completion_tokens_api"].sum(),
            "total_completion_tokens_api_sum": g.loc[g["status"] == "ok", "completion_tokens_api"].sum(),
            "pass1_total_tokens_api_sum": g.loc[(g["stage"] == "pass1") & (g["status"] == "ok"), "total_tokens_api"].sum(),
            "pass2_total_tokens_api_sum": g.loc[(g["stage"] == "pass2") & (g["status"] == "ok"), "total_tokens_api"].sum(),
            "total_tokens_api_sum": g.loc[g["status"] == "ok", "total_tokens_api"].sum(),
            "n_evidence_rows_doc": g.loc[(g["stage"] == "pass1") & (g["status"] == "ok"), "n_evidence_rows_norm"].fillna(0).sum(),
            "n_rows_normalized_doc": g.loc[(g["stage"] == "pass2") & (g["status"] == "ok"), "n_rows_normalized"].fillna(0).sum(),
            "max_attempt_used": g.loc[g["status"] == "ok", "attempt_used"].fillna(1).max(),
        })
    )
    .reset_index()
)

doc_like["status_like"] = np.where(
    doc_like["pass2_calls_ok"] > 0, "ok",
    np.where(doc_like["n_evidence_rows_doc"] > 0, "pass1_only", "empty")
)

print("\n=== DOC-LIKE LEVEL (RECONSTRUCTED FROM llm_metrics.jsonl) ===")
doc_level = {
    "total_reports": int(len(doc_like)),
    "reports_reaching_pass2": int((doc_like["pass2_calls_ok"] > 0).sum()),
    "reports_with_evidence_in_pass1": int((doc_like["n_evidence_rows_doc"] > 0).sum()),
    "reports_empty_after_pass1": int((doc_like["n_evidence_rows_doc"] == 0).sum()),
    "reports_with_any_failed_call": int(((doc_like["pass1_calls_error"] + doc_like["pass2_calls_error"]) > 0).sum()),
    "share_reaching_pass2": (doc_like["pass2_calls_ok"] > 0).mean(),
    "share_with_evidence_in_pass1": (doc_like["n_evidence_rows_doc"] > 0).mean(),
    "mean_chunks_per_report": float(doc_like["n_chunks_declared"].mean()),
    "total_evidence_rows": float(doc_like["n_evidence_rows_doc"].sum()),
    "mean_evidence_rows_per_report": float(doc_like["n_evidence_rows_doc"].mean()),
    "total_extracted_rows": float(doc_like["n_rows_normalized_doc"].sum()),
    "mean_extracted_rows_per_report": float(doc_like["n_rows_normalized_doc"].mean()),
    "mean_extracted_rows_per_successful_report": float(
        doc_like.loc[doc_like["pass2_calls_ok"] > 0, "n_rows_normalized_doc"].mean()
    ) if (doc_like["pass2_calls_ok"] > 0).any() else None,
}
for k, v in doc_level.items():
    print(f"{k}: {v}")

# ============================================================
# YEAR-WISE SUMMARY
# ============================================================
year_rows = []
for year, g in ok_calls.groupby("report_year"):
    docs_year = doc_like[doc_like["report_year"] == year].copy()

    year_rows.append({
        "report_year": year,
        "n_success_calls": len(g),
        "n_success_pass1": int((g["stage"] == "pass1").sum()),
        "n_success_pass2": int((g["stage"] == "pass2").sum()),
        "mean_elapsed_s": g["elapsed_s"].mean(),
        "mean_total_tokens_api": g["total_tokens_api"].mean(),
        "sum_elapsed_s": g["elapsed_s"].sum(),
        "sum_total_tokens_api": g["total_tokens_api"].sum(),
        "n_reports": len(docs_year),
        "reports_reaching_pass2": int((docs_year["pass2_calls_ok"] > 0).sum()),
        "share_reports_reaching_pass2": (docs_year["pass2_calls_ok"] > 0).mean() if len(docs_year) else None,
        "total_evidence_rows": docs_year["n_evidence_rows_doc"].sum(),
        "total_extracted_rows": docs_year["n_rows_normalized_doc"].sum(),
    })

year_df = pd.DataFrame(year_rows).sort_values("report_year")
print("\n=== YEAR-WISE SUMMARY ===")
print(year_df.to_string(index=False))

# ============================================================
# TOP REPORTS (aggregated by company-year)
# ============================================================
report_agg = doc_like.copy()

top_slowest_reports = report_agg.sort_values(
    ["llm_total_elapsed_sum_s", "total_tokens_api_sum"],
    ascending=[False, False]
).head(20)

top_evidence_reports = report_agg.sort_values(
    ["n_evidence_rows_doc", "llm_total_elapsed_sum_s"],
    ascending=[False, False]
).head(20)

top_output_reports = report_agg.sort_values(
    ["n_rows_normalized_doc", "llm_total_elapsed_sum_s"],
    ascending=[False, False]
).head(20)

top_token_reports = report_agg.sort_values(
    ["total_tokens_api_sum", "llm_total_elapsed_sum_s"],
    ascending=[False, False]
).head(20)

print("\n=== TOP SLOWEST REPORTS ===")
print(
    top_slowest_reports[
        [
            "company", "report_year", "n_chunks_declared",
            "pass1_calls_ok", "pass2_calls_ok",
            "llm_total_elapsed_sum_s", "total_prompt_tokens_api_sum",
            "total_completion_tokens_api_sum", "total_tokens_api_sum",
            "n_evidence_rows_doc", "n_rows_normalized_doc"
        ]
    ].to_string(index=False)
)

print("\n=== TOP REPORTS BY EVIDENCE ROWS ===")
print(
    top_evidence_reports[
        [
            "company", "report_year", "n_chunks_declared",
            "n_evidence_rows_doc", "n_rows_normalized_doc",
            "llm_total_elapsed_sum_s", "total_tokens_api_sum"
        ]
    ].to_string(index=False)
)

print("\n=== TOP REPORTS BY EXTRACTED ROWS ===")
print(
    top_output_reports[
        [
            "company", "report_year", "n_chunks_declared",
            "n_evidence_rows_doc", "n_rows_normalized_doc",
            "llm_total_elapsed_sum_s", "total_tokens_api_sum"
        ]
    ].to_string(index=False)
)

print("\n=== TOP REPORTS BY TOTAL TOKENS ===")
print(
    top_token_reports[
        [
            "company", "report_year", "n_chunks_declared",
            "llm_total_elapsed_sum_s", "total_prompt_tokens_api_sum",
            "total_completion_tokens_api_sum", "total_tokens_api_sum",
            "n_evidence_rows_doc", "n_rows_normalized_doc"
        ]
    ].to_string(index=False)
)

# ============================================================
# TOP SINGLE CALLS
# ============================================================
top_single_calls = ok_calls.sort_values(
    ["elapsed_s", "total_tokens_api"],
    ascending=[False, False]
).head(25)

print("\n=== TOP SINGLE SLOWEST CALLS ===")
print(
    top_single_calls[
        [
            "stage", "company", "report_year", "chunk_index", "batch_index",
            "elapsed_s", "prompt_tokens_api", "completion_tokens_api", "total_tokens_api",
            "n_evidence_rows_norm", "n_rows_normalized", "attempt_used"
        ]
    ].to_string(index=False)
)

# ============================================================
# FAILED CALLS
# ============================================================
if not err_calls.empty:
    print("\n=== FAILED CALLS ===")
    failed_view = err_calls[
        [
            "stage", "company", "report_year", "chunk_index", "batch_index",
            "elapsed_s", "status", "error_message"
        ]
    ].sort_values(["elapsed_s"], ascending=False)
    print(failed_view.to_string(index=False))
else:
    print("\n=== FAILED CALLS ===")
    print("No failed calls found.")

# ============================================================
# CROSS-CHECK WITH doc_metrics.jsonl (if available)
# ============================================================
if not doc_df.empty:
    print("\n=== CROSS-CHECK WITH doc_metrics.jsonl ===")
    try:
        if "report_year" in doc_df.columns:
            doc_df["report_year"] = doc_df["report_year"].astype(str)

        print(f"doc_metrics rows: {len(doc_df)}")
        print(f"reconstructed doc-like rows: {len(doc_like)}")

        if {"slug", "report_year"}.issubset(doc_df.columns):
            merged = doc_like.merge(
                doc_df[["slug", "report_year", "status", "n_chunks", "n_evidence_rows_doc", "n_pass2_batches"]],
                on=["slug", "report_year"],
                how="outer",
                suffixes=("_reconstructed", "_docjson")
            )

            missing_in_docjson = merged["status_docjson"].isna().sum()
            missing_in_reconstructed = merged["status_like"].isna().sum()

            print(f"missing_in_docjson: {missing_in_docjson}")
            print(f"missing_in_reconstructed: {missing_in_reconstructed}")

            if missing_in_docjson > 0 or missing_in_reconstructed > 0:
                print("WARNING: doc_metrics.jsonl non è allineato al run completo.")
        else:
            print("doc_metrics.jsonl non contiene colonne sufficienti per il merge di controllo.")
    except Exception as e:
        print(f"Cross-check error: {e}")

# ============================================================
# SAVE TABLES
# ============================================================
sort_and_save(stage_df, "option2_stage_summary.csv")
sort_and_save(year_df, "option2_year_summary.csv")
sort_and_save(report_agg.sort_values(["llm_total_elapsed_sum_s"], ascending=False), "option2_report_agg.csv")
sort_and_save(top_slowest_reports, "option2_top_slowest_reports.csv")
sort_and_save(top_evidence_reports, "option2_top_evidence_reports.csv")
sort_and_save(top_output_reports, "option2_top_output_reports.csv")
sort_and_save(top_token_reports, "option2_top_token_reports.csv")
sort_and_save(top_single_calls, "option2_top_single_calls.csv")

print("\n=== FILES SAVED ===")
for p in sorted(OUT_DIR.glob("*.csv")):
    print(p)

# ============================================================
# OPTIONAL: compare against llm_metrics_summary.json
# ============================================================
if LLM_SUMMARY_JSON.exists():
    try:
        summary = load_json(LLM_SUMMARY_JSON)
        print("\n=== QUICK CHECK vs llm_metrics_summary.json ===")
        overall = summary.get("overall", {})
        elapsed_count_summary = overall.get("elapsed_s", {}).get("count")
        total_tokens_count_summary = overall.get("total_tokens_api", {}).get("count")
        print(f"summary overall elapsed count: {elapsed_count_summary}")
        print(f"summary overall total_tokens_api count: {total_tokens_count_summary}")
        print(f"computed successful calls: {len(ok_calls)}")
    except Exception as e:
        print(f"Summary check error: {e}")