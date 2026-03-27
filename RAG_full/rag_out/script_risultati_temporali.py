from __future__ import annotations

import json
from pathlib import Path
import pandas as pd

JSONL_PATH = Path(r"C:\Universita\TESI\esg_agent\RAG_full\rag_out\activities_extracted_metrics.jsonl")

def summarize_series(s: pd.Series) -> dict:
    s = pd.to_numeric(s, errors="coerce").dropna()
    if s.empty:
        return {}
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

rows = []
with JSONL_PATH.open("r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        rows.append(json.loads(line))

df = pd.DataFrame(rows)

# colonne numeriche utili
num_cols = [
    "elapsed_s",
    "prompt_tokens_est",
    "prompt_tokens_api",
    "completion_tokens_api",
    "total_tokens_api",
    "n_activities_raw",
    "n_rows_normalized",
    "chunk_tokens_est",
    "chunk_index",
    "n_chunks",
]

for c in num_cols:
    if c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce")

# flag utili
df["is_productive_chunk"] = df["n_rows_normalized"].fillna(0) > 0
df["is_empty_chunk"] = df["n_rows_normalized"].fillna(0) == 0

overall = {
    "elapsed_s": summarize_series(df["elapsed_s"]),
    "prompt_tokens_api": summarize_series(df["prompt_tokens_api"]),
    "completion_tokens_api": summarize_series(df["completion_tokens_api"]),
    "total_tokens_api": summarize_series(df["total_tokens_api"]),
    "n_rows_normalized": summarize_series(df["n_rows_normalized"]),
}

# statistiche call-level semplici
call_level = {
    "total_calls": int(len(df)),
    "productive_calls": int(df["is_productive_chunk"].sum()),
    "empty_calls": int(df["is_empty_chunk"].sum()),
    "productive_call_share": float(df["is_productive_chunk"].mean()) if len(df) else 0.0,
    "empty_call_share": float(df["is_empty_chunk"].mean()) if len(df) else 0.0,
    "total_extracted_rows": int(df["n_rows_normalized"].fillna(0).sum()),
}

# statistiche company-level
company_level = (
    df.groupby(["company", "report_year"], dropna=False)
      .agg(
          n_calls=("chunk_index", "count"),
          n_chunks_declared=("n_chunks", "max"),
          total_elapsed_s=("elapsed_s", "sum"),
          total_prompt_tokens=("prompt_tokens_api", "sum"),
          total_completion_tokens=("completion_tokens_api", "sum"),
          total_tokens=("total_tokens_api", "sum"),
          total_rows=("n_rows_normalized", "sum"),
          productive_calls=("is_productive_chunk", "sum"),
      )
      .reset_index()
)

company_level["empty_calls"] = company_level["n_calls"] - company_level["productive_calls"]

# top casi inefficienti
top_most_calls = company_level.sort_values(["n_calls", "total_elapsed_s"], ascending=[False, False]).head(20)
top_slowest = company_level.sort_values("total_elapsed_s", ascending=False).head(20)
top_most_empty = company_level.sort_values(["empty_calls", "n_calls"], ascending=[False, False]).head(20)

print("\n=== OVERALL ===")
for k, v in overall.items():
    print(f"\n{k}")
    print(v)

print("\n=== CALL LEVEL ===")
for k, v in call_level.items():
    print(f"{k}: {v}")

print("\n=== TOP MOST CALLS ===")
print(top_most_calls.to_string(index=False))

print("\n=== TOP SLOWEST REPORTS ===")
print(top_slowest.to_string(index=False))

print("\n=== TOP MOST EMPTY ===")
print(top_most_empty.to_string(index=False))