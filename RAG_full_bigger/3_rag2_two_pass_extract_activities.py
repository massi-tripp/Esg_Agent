# File: 3_rag2_two_pass_extract_activities.py

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from openai import AzureOpenAI


# =========================
# Azure OpenAI ENV
# =========================

ENDPOINT_URL = os.getenv("ENDPOINT_URL", "https://maurinokeys.openai.azure.com/openai/v1").strip()

API_KEY = os.getenv(
    "AZURE_OPENAI_API_KEY",
    "FsEzVtCW88IklIn1gdtodRT8at2wzo84YubWnvQ9eZNxiCnl4CXfJQQJ99CBACI8hq2XJ3w3AAABACOGWjK9",
).strip()
if not API_KEY:
    raise RuntimeError("Missing AZURE_OPENAI_API_KEY env var.")

API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION", "2025-01-01-preview").strip()
CHAT_DEPLOYMENT = os.getenv("AZURE_DEPLOYMENT_GPT5_MINI", "gpt-5.1-chat").strip()

REASONING_EFFORT = os.getenv("AZURE_REASONING_EFFORT", "medium").strip().lower()
MAX_COMPLETION_TOKENS = int(os.getenv("AZURE_MAX_COMPLETION_TOKENS", "2500"))
SLEEP_BETWEEN_CALLS_S = float(os.getenv("AZURE_SLEEP_S", "0.2"))

AZURE_INPUT_MAX_TOKENS = int(os.getenv("AZURE_INPUT_MAX_TOKENS", "111616"))

# =========================
# PATHS
# =========================
MARKER_ARTIFACTS_DIR = Path(r"C:\Universita\TESI\esg_agent\RAG_full_bigger\marker_artifacts")

OUT_DIR = Path(r"C:\Universita\TESI\esg_agent\RAG_full_bigger\rag2_out")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Pass 1 output (evidence per chunk)
OUT_EVIDENCE_JSONL = OUT_DIR / "evidence_pass1.jsonl"
# Pass 2 output (final extraction per batch)
OUT_FINAL_JSONL = OUT_DIR / "activities_extracted_pass2.jsonl"
OUT_CSV = OUT_DIR / "activities_extracted.csv"

OUT_METRICS_JSONL = OUT_DIR / "llm_metrics.jsonl"
OUT_METRICS_CSV = OUT_DIR / "llm_metrics.csv"
OUT_METRICS_SUMMARY_JSON = OUT_DIR / "llm_metrics_summary.json"

OUT_DOC_METRICS_JSONL = OUT_DIR / "doc_metrics.jsonl"
OUT_DOC_METRICS_CSV = OUT_DIR / "doc_metrics.csv"
OUT_DOC_METRICS_SUMMARY_JSON = OUT_DIR / "doc_metrics_summary.json"

# =========================
# DATA STRUCTURES
# =========================
@dataclass
class FocusedDoc:
    slug: str
    report_year: str
    focused_md: Path


# =========================
# TOKENIZER (tiktoken)
# =========================
def _get_token_encoder():
    try:
        import tiktoken  # type: ignore
        return tiktoken.get_encoding("cl100k_base")
    except Exception:
        return None


ENCODER = _get_token_encoder()


def count_tokens(text: str) -> int:
    if not text:
        return 0
    if ENCODER is not None:
        return len(ENCODER.encode(text))
    return max(1, len(text) // 4)

def _usage_to_dict(resp: Any) -> Dict[str, Optional[int]]:
    usage = getattr(resp, "usage", None)
    if usage is None:
        return {
            "prompt_tokens": None,
            "completion_tokens": None,
            "total_tokens": None,
        }

    return {
        "prompt_tokens": getattr(usage, "prompt_tokens", None),
        "completion_tokens": getattr(usage, "completion_tokens", None),
        "total_tokens": getattr(usage, "total_tokens", None),
    }


def _read_jsonl_rows(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []

    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                continue
    return rows


def _n0(x: Optional[int]) -> int:
    return int(x) if isinstance(x, int) else 0


def _series_stats(s: pd.Series) -> Dict[str, Optional[float]]:
    s = pd.to_numeric(s, errors="coerce").dropna()
    if s.empty:
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
            "iqr": None,
            "sum": None,
        }

    q1 = s.quantile(0.25)
    q3 = s.quantile(0.75)

    return {
        "count": int(s.shape[0]),
        "min": float(s.min()),
        "q1": float(q1),
        "mean": float(s.mean()),
        "median": float(s.median()),
        "q3": float(q3),
        "p90": float(s.quantile(0.90)),
        "p95": float(s.quantile(0.95)),
        "max": float(s.max()),
        "std": float(s.std(ddof=1)) if len(s) > 1 else 0.0,
        "iqr": float(q3 - q1),
        "sum": float(s.sum()),
    }


def build_metrics_summary(df_metrics: pd.DataFrame) -> Dict[str, Any]:
    if df_metrics.empty:
        return {"overall": {}, "by_stage": {}, "by_year": {}}

    ok = df_metrics[df_metrics["status"] == "ok"].copy()
    if ok.empty:
        return {"overall": {}, "by_stage": {}, "by_year": {}}

    summary = {
        "overall": {
            "elapsed_s": _series_stats(ok["elapsed_s"]),
            "prompt_tokens_est": _series_stats(ok["prompt_tokens_est"]),
            "prompt_tokens_api": _series_stats(ok["prompt_tokens_api"]),
            "completion_tokens_api": _series_stats(ok["completion_tokens_api"]),
            "total_tokens_api": _series_stats(ok["total_tokens_api"]),
        },
        "by_stage": {},
        "by_year": {},
    }

    for stage, g in ok.groupby("stage", dropna=False):
        summary["by_stage"][str(stage)] = {
            "n_calls": int(len(g)),
            "elapsed_s": _series_stats(g["elapsed_s"]),
            "prompt_tokens_est": _series_stats(g["prompt_tokens_est"]),
            "prompt_tokens_api": _series_stats(g["prompt_tokens_api"]),
            "completion_tokens_api": _series_stats(g["completion_tokens_api"]),
            "total_tokens_api": _series_stats(g["total_tokens_api"]),
            "n_evidence_rows_norm": _series_stats(g["n_evidence_rows_norm"]),
            "n_rows_normalized": _series_stats(g["n_rows_normalized"]),
        }

    for year, g in ok.groupby("report_year", dropna=False):
        summary["by_year"][str(year)] = {
            "n_calls": int(len(g)),
            "elapsed_s": _series_stats(g["elapsed_s"]),
            "total_tokens_api": _series_stats(g["total_tokens_api"]),
        }

    return summary


def build_doc_metrics_summary(df_doc: pd.DataFrame) -> Dict[str, Any]:
    if df_doc.empty:
        return {"overall": {}, "by_year": {}}

    valid = df_doc[df_doc["status"] != "error"].copy()
    if valid.empty:
        return {"overall": {}, "by_year": {}}

    summary = {
        "overall": {
            "pass1_elapsed_sum_s": _series_stats(valid["pass1_elapsed_sum_s"]),
            "pass2_elapsed_sum_s": _series_stats(valid["pass2_elapsed_sum_s"]),
            "llm_total_elapsed_sum_s": _series_stats(valid["llm_total_elapsed_sum_s"]),
            "doc_total_elapsed_s": _series_stats(valid["doc_total_elapsed_s"]),
            "pass1_prompt_tokens_api_sum": _series_stats(valid["pass1_prompt_tokens_api_sum"]),
            "pass1_completion_tokens_api_sum": _series_stats(valid["pass1_completion_tokens_api_sum"]),
            "pass1_total_tokens_api_sum": _series_stats(valid["pass1_total_tokens_api_sum"]),
            "pass2_prompt_tokens_api_sum": _series_stats(valid["pass2_prompt_tokens_api_sum"]),
            "pass2_completion_tokens_api_sum": _series_stats(valid["pass2_completion_tokens_api_sum"]),
            "pass2_total_tokens_api_sum": _series_stats(valid["pass2_total_tokens_api_sum"]),
            "total_prompt_tokens_api_sum": _series_stats(valid["total_prompt_tokens_api_sum"]),
            "total_completion_tokens_api_sum": _series_stats(valid["total_completion_tokens_api_sum"]),
            "total_tokens_api_sum": _series_stats(valid["total_tokens_api_sum"]),
            "n_chunks": _series_stats(valid["n_chunks"]),
            "n_evidence_rows_doc": _series_stats(valid["n_evidence_rows_doc"]),
            "n_pass2_batches": _series_stats(valid["n_pass2_batches"]),
        },
        "by_year": {},
    }

    for year, g in valid.groupby("report_year", dropna=False):
        summary["by_year"][str(year)] = {
            "n_docs": int(len(g)),
            "pass1_elapsed_sum_s": _series_stats(g["pass1_elapsed_sum_s"]),
            "pass2_elapsed_sum_s": _series_stats(g["pass2_elapsed_sum_s"]),
            "llm_total_elapsed_sum_s": _series_stats(g["llm_total_elapsed_sum_s"]),
            "doc_total_elapsed_s": _series_stats(g["doc_total_elapsed_s"]),
            "total_tokens_api_sum": _series_stats(g["total_tokens_api_sum"]),
        }

    return summary

# =========================
# JSON helpers
# =========================
def _read_json(path: Path) -> Optional[dict]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


# =========================
# COMPANY NAME (benchmark rules)
# =========================
def resolve_company_name_for_benchmark(root: Path, slug: str, year: str) -> str:
    """
    Regole richieste:
      - 2024: <root>/<slug>/2024/marker_meta.json -> company_name_full (o company_full_name)
      - 2023: <root>/<slug>/2023/*_artifact.json -> company_name
    Fallback: slug
    """
    year = str(year)
    company_year_dir = root / slug / year
    if not company_year_dir.exists():
        return slug

    if year == "2024":
        meta_path = company_year_dir / "marker_meta.json"
        meta = _read_json(meta_path) if meta_path.exists() else None
        if isinstance(meta, dict):
            v = meta.get("company_name_full") or meta.get("company_full_name")
            if isinstance(v, str) and v.strip():
                return v.strip()
        return slug

    if year == "2023":
        candidates = sorted(company_year_dir.glob(f"*_{year}_*_artifact.json"))
        if not candidates:
            candidates = sorted(company_year_dir.glob(f"*_{year}_artifact.json"))
        if not candidates:
            candidates = sorted(company_year_dir.glob("*_artifact.json"))

        if candidates:
            obj = _read_json(candidates[0])
            if isinstance(obj, dict):
                v = obj.get("company_name")
                if isinstance(v, str) and v.strip():
                    return v.strip()
                ci = obj.get("company_info")
                if isinstance(ci, dict):
                    nm = ci.get("name")
                    if isinstance(nm, str) and nm.strip():
                        return nm.strip()
        return slug

    meta_path = company_year_dir / "marker_meta.json"
    meta = _read_json(meta_path) if meta_path.exists() else None
    if isinstance(meta, dict):
        v = meta.get("company_name_full") or meta.get("company_full_name")
        if isinstance(v, str) and v.strip():
            return v.strip()

    candidates = sorted(company_year_dir.glob("*_artifact.json"))
    if candidates:
        obj = _read_json(candidates[0])
        if isinstance(obj, dict):
            v = obj.get("company_name")
            if isinstance(v, str) and v.strip():
                return v.strip()

    return slug


# =========================
# DISCOVERY: SOLO focused md
# =========================
def discover_focused_docs(root: Path, years: Optional[List[str]] = None) -> List[FocusedDoc]:
    years = years or ["2023", "2024"]
    docs: List[FocusedDoc] = []

    for year in years:
        pattern = f"*/{year}/focused/*_taxonomy_focused.md"
        for md_path in root.glob(pattern):
            if not md_path.is_file():
                continue
            slug = md_path.parent.parent.parent.name
            docs.append(FocusedDoc(slug=slug, report_year=year, focused_md=md_path))

    return sorted(docs, key=lambda d: (d.slug, d.report_year, str(d.focused_md)))


# =========================
# MARKDOWN LOADING / NORMALIZATION
# =========================
def load_markdown(md_path: Path) -> str:
    text = md_path.read_text(encoding="utf-8", errors="replace")
    text = text.replace("\u00a0", " ")
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{4,}", "\n\n", text).strip()
    return text


# =========================
# SPLIT BY PAGES and chunking token-based
# =========================
PAGE_HDR_RE = re.compile(r"^\s*##\s*Page\s+(\d+)\s*$", flags=re.IGNORECASE)


def split_into_pages(md_text: str) -> List[Tuple[Optional[int], str]]:
    lines = md_text.splitlines()
    pages: List[Tuple[Optional[int], List[str]]] = []
    cur_page: Optional[int] = None
    buf: List[str] = []
    saw_any_page = False

    def flush():
        nonlocal buf, cur_page
        if buf or cur_page is not None:
            pages.append((cur_page, buf))
        buf = []

    for line in lines:
        m = PAGE_HDR_RE.match(line)
        if m:
            saw_any_page = True
            flush()
            cur_page = int(m.group(1))
            continue
        buf.append(line)

    flush()

    if not saw_any_page:
        return [(None, md_text.strip())]

    out: List[Tuple[Optional[int], str]] = []
    for p, b in pages:
        out.append((p, "\n".join(b).strip()))
    return [(p, t) for (p, t) in out if t]


def build_chunks_from_pages(
    pages: List[Tuple[Optional[int], str]],
    target_chunk_tokens: int,
    max_chunk_tokens: int,
) -> List[Dict[str, Any]]:
    chunks: List[Dict[str, Any]] = []
    cur_parts: List[str] = []
    cur_tokens = 0
    cur_start_page: Optional[int] = None
    cur_end_page: Optional[int] = None

    for page_no, page_text in pages:
        piece = f"## Page {page_no}\n\n{page_text}\n" if page_no is not None else page_text + "\n"
        piece_tokens = count_tokens(piece)

        if piece_tokens > max_chunk_tokens:
            if cur_parts:
                chunks.append({"text": "\n".join(cur_parts).strip(), "start_page": cur_start_page, "end_page": cur_end_page, "tokens": cur_tokens})
                cur_parts, cur_tokens, cur_start_page, cur_end_page = [], 0, None, None

            paras = re.split(r"\n{2,}", piece)
            sub_parts: List[str] = []
            sub_tokens = 0

            for para in paras:
                para = para.strip()
                if not para:
                    continue
                para_piece = para + "\n\n"
                pt = count_tokens(para_piece)

                if sub_tokens + pt > max_chunk_tokens and sub_parts:
                    chunks.append({"text": "".join(sub_parts).strip(), "start_page": page_no, "end_page": page_no, "tokens": sub_tokens})
                    sub_parts, sub_tokens = [], 0

                sub_parts.append(para_piece)
                sub_tokens += pt

            if sub_parts:
                chunks.append({"text": "".join(sub_parts).strip(), "start_page": page_no, "end_page": page_no, "tokens": sub_tokens})
            continue

        if not cur_parts:
            cur_start_page = page_no
            cur_end_page = page_no

        if cur_tokens + piece_tokens > max_chunk_tokens and cur_parts:
            chunks.append({"text": "\n".join(cur_parts).strip(), "start_page": cur_start_page, "end_page": cur_end_page, "tokens": cur_tokens})
            cur_parts, cur_tokens, cur_start_page, cur_end_page = [], 0, page_no, page_no

        cur_parts.append(piece)
        cur_tokens += piece_tokens
        cur_end_page = page_no if page_no is not None else cur_end_page

        if cur_tokens >= target_chunk_tokens:
            chunks.append({"text": "\n".join(cur_parts).strip(), "start_page": cur_start_page, "end_page": cur_end_page, "tokens": cur_tokens})
            cur_parts, cur_tokens, cur_start_page, cur_end_page = [], 0, None, None

    if cur_parts:
        chunks.append({"text": "\n".join(cur_parts).strip(), "start_page": cur_start_page, "end_page": cur_end_page, "tokens": cur_tokens})

    return chunks


# =========================
# LLM helpers
# =========================
def _extract_first_json_object(text: str) -> str:
    s = (text or "").strip()
    if not s:
        raise ValueError("Empty LLM output")
    start = s.find("{")
    if start < 0:
        raise ValueError("No JSON object found in LLM output")
    s = s[start:]
    depth = 0
    in_str = False
    esc = False
    for i, ch in enumerate(s):
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
        else:
            if ch == '"':
                in_str = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return s[: i + 1].strip()
    raise ValueError("Unbalanced JSON braces in LLM output")


def _normalize_azure_endpoint(url: str) -> str:
    u = (url or "").strip().rstrip("/")
    if not u:
        raise ValueError("ENDPOINT_URL is empty")
    if "/openai/v1" in u:
        u = u.split("/openai/v1")[0].rstrip("/")
    return u


def get_client() -> AzureOpenAI:
    azure_endpoint = _normalize_azure_endpoint(ENDPOINT_URL)
    return AzureOpenAI(api_key=API_KEY, api_version=API_VERSION, azure_endpoint=azure_endpoint)


def call_llm_json(
    client: AzureOpenAI,
    developer: str,
    user: str,
    max_retries: int = 3,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    last_err: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        try:
            resp = client.chat.completions.create(
                model=CHAT_DEPLOYMENT,
                messages=[
                    {"role": "developer", "content": developer},
                    {"role": "user", "content": user},
                ],
                reasoning_effort=REASONING_EFFORT,
                max_completion_tokens=MAX_COMPLETION_TOKENS,
            )
            content = resp.choices[0].message.content or ""
            json_text = _extract_first_json_object(content)
            data = json.loads(json_text)
            if not isinstance(data, dict):
                raise ValueError("LLM output is not a JSON object")

            usage = _usage_to_dict(resp)
            meta = {
                "attempt_used": attempt,
                "prompt_tokens": usage["prompt_tokens"],
                "completion_tokens": usage["completion_tokens"],
                "total_tokens": usage["total_tokens"],
            }

            return data, meta

        except Exception as e:
            last_err = e
            time.sleep(1.5 * attempt)

    raise RuntimeError(f"LLM call failed after {max_retries} retries: {last_err}")


# =========================
# Normalization / dedup utils
# =========================
_MULTI_SPACE_RE = re.compile(r"\s+")
OBJ_RE = re.compile(r"\b(CCM|CCA|CE)\b", flags=re.IGNORECASE)

# IMPORTANT: qui includiamo anche codici tipo C17.1.2 / A1.6.2
CODE_RE = re.compile(
    r"\b(?:CCM|CCA|CE)\s*\d+(?:\.\d+)?\b"      # CCM 3.3
    r"|\b[A-Z]\d+(?:\.\d+)+\b"                # C17.1.2, A1.6.2
    r"|\b\d+\.\d+\b",                         # 6.10, 7.7
    flags=re.IGNORECASE
)


def _norm_space(s: str) -> str:
    return _MULTI_SPACE_RE.sub(" ", (s or "")).strip()


def _norm_code(code: Optional[str]) -> Optional[str]:
    if not code:
        return None
    c = _norm_space(code).upper()
    c = re.sub(r"^(CCM|CCA|CE)\s*([0-9])", r"\1 \2", c)
    return c


def _norm_obj(obj: Optional[str]) -> Optional[str]:
    if not obj:
        return None
    o = _norm_space(obj).upper()
    m = OBJ_RE.search(o)
    if not m:
        return None
    return m.group(1).upper()


def _looks_tabular(s: str) -> bool:
    return ("|" in (s or "")) or ("\t" in (s or "")) or (re.search(r"\s{2,}", (s or "")) is not None)


def _heuristic_extract_obj_and_code(s: str) -> Tuple[Optional[str], Optional[str]]:
    text = _norm_space(s or "")
    if not text:
        return None, None
    obj = _norm_obj(text)
    m = CODE_RE.search(text)
    code = _norm_space(m.group(0)) if m else None
    return obj, code


def _heuristic_split_row(raw_row: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Estrae in modo conservativo:
      (activity, objective, code)
    """
    row = _norm_space(raw_row)
    if not row:
        return None, None, None

    # 1) pipe table
    if "|" in row:
        parts = [p.strip() for p in row.split("|")]
        parts = [p for p in parts if p]
        if not parts:
            return None, None, None

        obj = None
        code = None
        obj_idx = None
        code_idx = None

        for idx, p in enumerate(parts):
            p_obj = _norm_obj(p)
            if p_obj and obj is None:
                obj = p_obj
                obj_idx = idx

            m = CODE_RE.search(p)
            if m and code is None:
                code = _norm_space(m.group(0))
                code_idx = idx

        # attività: preferisci una colonna non (obj/code), tipicamente la prima
        non_special = [p for j, p in enumerate(parts) if j not in {obj_idx, code_idx}]
        non_special = [p for p in non_special if p]

        activity = None
        if non_special:
            activity = _norm_space(non_special[0])
            # se la prima è “troppo corta”, prova la più lunga
            if len(activity) < 4:
                non_special_sorted = sorted(non_special, key=lambda x: len(x), reverse=True)
                activity = _norm_space(non_special_sorted[0])

        return (activity if activity else None), obj, (code if code else None)

    # 2) non tabellare: estrai code/obj e prova a ricavare activity rimuovendo code/obj
    obj, code = _heuristic_extract_obj_and_code(row)
    act = row
    if code:
        act = re.sub(re.escape(code), " ", act, flags=re.IGNORECASE)
    if obj:
        act = re.sub(rf"\b{re.escape(obj)}\b", " ", act, flags=re.IGNORECASE)
    act = _norm_space(act)
    if act:
        return act, obj, code
    return None, obj, code


# =========================
# PASS 1: EVIDENCE BUILDER (TRUE REFACTOR)
# =========================
def build_prompt_pass1(
    company: str,
    report_year: str,
    chunk_text: str,
    chunk_idx: int,
) -> Tuple[str, str]:
    developer = (
        "You are an information extraction system. "
        "Goal: BUILD AN EVIDENCE DOCUMENT from the provided report text. "
        "Do NOT produce final answers. Do NOT infer. Return STRICT JSON only."
    )

    user = f"""
PASS 1 — EVIDENCE BUILDER (COMPRESSION, HIGH RECALL)

You will receive a report chunk in Markdown (from a sustainability report taxonomy section).
Your job is to BUILD a compact evidence document capturing ANY line/row that MAY represent:
- an economic activity row (taxonomy activities, eligible/aligned activities)
- rows that contain objective labels (CCM/CCA/CE/WTR/BIO) and/or codes (6.10, 7.7, CCM 3.3, C17.1.2)
- a NACE activity row/list within the taxonomy section (codes like C17.1.2, A1.6.2 etc.)

IMPORTANT: Be recall-oriented.
- If unsure whether a row is relevant, INCLUDE it.
- Still EXCLUDE obvious noise: headings-only, group titles-only, totals/subtotals, KPI totals/percentages.

For each extracted evidence row, output an object with:
- page: integer page number if clearly present in the chunk context, else null
- raw_row: full row text as ONE SINGLE LINE (join wrapped lines with single spaces)
- raw_activity: activity cell/phrase ONLY if clearly present (else null)
- raw_code: code token ONLY if clearly present (else null)
- raw_objective: CCM/CCA/CE/WTR/BIO ONLY if clearly present (else null)

Deduplicate within this chunk by raw_row exact string (case-insensitive).

Return exactly ONE JSON object:
{{
  "company": "{company}",
  "report_year": "{report_year}",
  "chunk_index": {chunk_idx},
  "evidence_rows": [
    {{
      "page": 12,
      "raw_row": "Acquisition and ownership of buildings | 7.7 | ...",
      "raw_activity": "Acquisition and ownership of buildings",
      "raw_code": "7.7",
      "raw_objective": "CCM"
    }}
  ]
}}

--- BEGIN REPORT CHUNK (MARKDOWN) ---
{chunk_text}
--- END REPORT CHUNK ---
"""
    return developer, user


def normalize_evidence_rows(obj: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Normalizza output del Pass1.
    - filtra KPI/totali
    - dedup per raw_row intra-chunk
    - riempie raw_activity/raw_code/raw_objective SOLO con euristica conservativa se manca
    """
    rows = obj.get("evidence_rows")
    if not isinstance(rows, list):
        return []

    out: List[Dict[str, Any]] = []
    seen = set()

    for it in rows:
        if not isinstance(it, dict):
            continue

        raw_row = it.get("raw_row")
        if not isinstance(raw_row, str):
            continue
        raw_row = _norm_space(raw_row)
        if not raw_row:
            continue

        # filtri minimi anti-KPI/total
        if re.search(r"\bturnover\b|\bcapex\b|\bopex\b|\bsubtotal\b|\btotal\b", raw_row, flags=re.IGNORECASE):
            continue

        key = raw_row.lower()
        if key in seen:
            continue
        seen.add(key)

        page = it.get("page")
        page_int: Optional[int] = None
        if isinstance(page, int):
            page_int = page
        elif isinstance(page, str) and page.strip().isdigit():
            page_int = int(page.strip())

        raw_activity = it.get("raw_activity")
        raw_code = it.get("raw_code")
        raw_obj = it.get("raw_objective")

        raw_activity = _norm_space(raw_activity) if isinstance(raw_activity, str) and raw_activity.strip() else None
        raw_code = _norm_space(raw_code) if isinstance(raw_code, str) and raw_code.strip() else None
        raw_obj = _norm_obj(raw_obj) if isinstance(raw_obj, str) and raw_obj.strip() else None

        # euristica: se manca qualcosa e la riga sembra tabellare o contiene pattern
        if raw_activity is None or raw_code is None or raw_obj is None:
            h_act, h_obj, h_code = _heuristic_split_row(raw_row)
            if raw_activity is None and h_act:
                raw_activity = h_act
            if raw_obj is None and h_obj:
                raw_obj = h_obj
            if raw_code is None and h_code:
                raw_code = h_code

        out.append(
            {
                "page": page_int,
                "raw_row": raw_row,
                "raw_activity": raw_activity,
                "raw_code": raw_code,
                "raw_objective": raw_obj,
            }
        )

    return out


def _evidence_row_score(r: Dict[str, Any]) -> int:
    return int(bool(r.get("raw_activity"))) + int(bool(r.get("raw_code"))) + int(bool(r.get("raw_objective")))


def format_evidence_doc_lines(evidence_rows: List[Dict[str, Any]]) -> List[str]:
    """
    Costruisce il documento evidenza (una riga per evidence row),
    in formato deterministico e compatto per il Pass2.
    """
    lines: List[str] = []
    for r in evidence_rows:
        p = r.get("page")
        page_s = str(p) if isinstance(p, int) else ""
        obj = r.get("raw_objective") or ""
        code = r.get("raw_code") or ""
        act = r.get("raw_activity") or ""
        rr = r.get("raw_row") or ""
        # una sola linea stabile
        lines.append(f"page={page_s} | objective={obj} | code={code} | activity={act} | raw_row={rr}")
    return lines


# =========================
# PASS 2: FINAL EXTRACTOR (from EVIDENCE DOC)
# =========================
def build_prompt_pass2(company: str, report_year: str, evidence_doc: str, batch_idx: int, n_batches: int) -> Tuple[str, str]:
    developer = (
        "You are a Sustainable Finance Analyst specialized in EU Taxonomy disclosures. "
        "You must EXTRACT (not predict) EU Taxonomy activity rows from provided EVIDENCE. "
        "Be extremely literal. Return STRICT JSON only."
    )

    user = f"""
PASS 2 — FINAL EXTRACTOR (FROM EVIDENCE DOC)

You will receive an EVIDENCE DOCUMENT (lines). Each line includes:
- objective (CCM/CCA/CE) if explicitly present
- code (could be '6.10', 'CCM 3.3', 'C17.1.2', etc.) if explicitly present
- activity (best effort from evidence)
- raw_row (backup)

Your job: extract final EU taxonomy / taxonomy-section activities.

RULES:
- Only use what is explicitly present in evidence. Do NOT infer missing codes or objectives.
- activity: take it EXACTLY as in the evidence field "activity" when present; otherwise use the activity phrase in raw_row.
- Sub_activity_code:
  - If evidence objective is present and evidence code is purely numeric (e.g., 3.3, 6.10), output "OBJECTIVE <code>" (e.g., "CCM 3.3").
  - If evidence code already includes an objective (e.g., "CCM 3.3"), keep it.
  - If evidence code is a NACE-like code (e.g., "C17.1.2", "A1.6.2"), keep it as is.
  - If no code is present, set null.
- Deduplicate within this batch: same (activity, Sub_activity_code) output only once.
- Return JSON ONLY.

OUTPUT:
{{
  "company": "{company}",
  "report_year": "{report_year}",
  "batch_index": {batch_idx},
  "activities": [
    {{"activity": "...", "Sub_activity_code": "CCM 3.3"}}
  ]
}}

--- BEGIN EVIDENCE DOC (one line per row) ---
{evidence_doc}
--- END EVIDENCE DOC ---
"""
    return developer, user


def normalize_rows_pass2(obj: Dict[str, Any], company_forced: str, year_forced: str) -> List[Dict[str, Any]]:
    batch_index = obj.get("batch_index")
    acts = obj.get("activities")
    if not isinstance(acts, list):
        return []

    rows: List[Dict[str, Any]] = []
    for a in acts:
        if not isinstance(a, dict):
            continue
        activity = _norm_space(str(a.get("activity") or ""))
        code = a.get("Sub_activity_code")
        code = _norm_code(code) if isinstance(code, str) else None

        if not activity:
            continue
        if re.search(r"\bturnover\b|\bcapex\b|\bopex\b|\btotal\b", activity, flags=re.IGNORECASE):
            continue

        rows.append(
            {
                "company": company_forced,
                "report_year": year_forced,
                # qui = batch_index del Pass2
                "chunk_index": batch_index,
                "activity": activity,
                "Sub_activity_code": code,
            }
        )
    return rows


def pack_lines_into_batches(lines: List[str], max_tokens: int) -> List[List[str]]:
    batches: List[List[str]] = []
    cur: List[str] = []
    cur_t = 0

    for line in lines:
        lt = count_tokens(line) + 2
        if cur and (cur_t + lt > max_tokens):
            batches.append(cur)
            cur = [line]
            cur_t = lt
        else:
            cur.append(line)
            cur_t += lt

    if cur:
        batches.append(cur)
    return batches


# =========================
# Existing outputs reader (for resume/append)
# =========================
def _rows_from_existing_final_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                acts = obj.get("activities")
                if isinstance(acts, list):
                    for a in acts:
                        if not isinstance(a, dict):
                            continue
                        activity = _norm_space(str(a.get("activity") or ""))
                        code = a.get("Sub_activity_code")
                        code = _norm_code(code) if isinstance(code, str) else None
                        if not activity:
                            continue
                        rows.append(
                            {
                                "company": str(obj.get("company") or ""),
                                "report_year": str(obj.get("report_year") or ""),
                                "chunk_index": obj.get("batch_index"),
                                "activity": activity,
                                "Sub_activity_code": code,
                            }
                        )
            except Exception:
                continue
    return rows


def dedup_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    best_by_code: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    best_by_act: Dict[Tuple[str, str, str], Dict[str, Any]] = {}

    for r in rows:
        company = str(r.get("company") or "")
        year = str(r.get("report_year") or "")
        act = _norm_space(str(r.get("activity") or ""))
        code = r.get("Sub_activity_code")
        code = _norm_code(code) if isinstance(code, str) else None

        if not act:
            continue

        if code:
            k = (company, year, code)
            prev = best_by_code.get(k)
            if prev is None or len(act) > len(_norm_space(str(prev.get("activity") or ""))):
                best_by_code[k] = r
        else:
            k2 = (company, year, act.lower())
            if k2 not in best_by_act:
                best_by_act[k2] = r

    out = list(best_by_code.values()) + list(best_by_act.values())

    uniq: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
    for r in out:
        k = (
            str(r.get("company") or ""),
            str(r.get("report_year") or ""),
            _norm_space(str(r.get("activity") or "")).lower(),
            (_norm_code(r.get("Sub_activity_code")) or "").lower(),
        )
        uniq[k] = r
    return list(uniq.values())


# =========================
# Main run (Two-pass true refactor)
# =========================

def run(
    only_first: bool = False,
    slug_filter: Optional[str] = None,
    year_filter: Optional[str] = None,
    chunk_tokens: int = 20_000,
    safe_input_budget: int = 90_000,
    pass2_max_input_tokens: int = 60_000,
    no_append_jsonl: bool = False,
) -> None:
    if safe_input_budget >= AZURE_INPUT_MAX_TOKENS:
        safe_input_budget = AZURE_INPUT_MAX_TOKENS - 2000
    if pass2_max_input_tokens >= AZURE_INPUT_MAX_TOKENS:
        pass2_max_input_tokens = AZURE_INPUT_MAX_TOKENS - 2000

    docs = discover_focused_docs(MARKER_ARTIFACTS_DIR, years=["2023", "2024"])
    if not docs:
        print(f"[ERR] Nessun focused md trovato in: {MARKER_ARTIFACTS_DIR}")
        return

    if slug_filter:
        docs = [d for d in docs if d.slug == slug_filter]
    if year_filter:
        docs = [d for d in docs if str(d.report_year) == str(year_filter)]

    docs = sorted(docs, key=lambda x: (x.slug, x.report_year))
    if not docs:
        print("[ERR] Nessun documento dopo i filtri.")
        return

    if only_first:
        docs = docs[:1]

    client = get_client()

    processed_company_years = set()
    all_flat_rows: List[Dict[str, Any]] = _rows_from_existing_final_jsonl(OUT_FINAL_JSONL) if not no_append_jsonl else []
    metrics_rows: List[Dict[str, Any]] = _read_jsonl_rows(OUT_METRICS_JSONL) if not no_append_jsonl else []
    doc_metrics_rows: List[Dict[str, Any]] = _read_jsonl_rows(OUT_DOC_METRICS_JSONL) if not no_append_jsonl else []

    mode = "w" if no_append_jsonl else "a"
    with (
        OUT_EVIDENCE_JSONL.open(mode, encoding="utf-8") as w_evi,
        OUT_FINAL_JSONL.open(mode, encoding="utf-8") as w_final,
        OUT_METRICS_JSONL.open(mode, encoding="utf-8") as w_metrics,
        OUT_DOC_METRICS_JSONL.open(mode, encoding="utf-8") as w_doc_metrics,
    ):
        print(f"[OUT] PASS1 EVIDENCE JSONL ({'overwrite' if no_append_jsonl else 'append'}) -> {OUT_EVIDENCE_JSONL}")
        print(f"[OUT] PASS2 FINAL    JSONL ({'overwrite' if no_append_jsonl else 'append'}) -> {OUT_FINAL_JSONL}")
        print(f"[OUT] LLM METRICS JSONL ({'overwrite' if no_append_jsonl else 'append'}) -> {OUT_METRICS_JSONL}")
        print(f"[OUT] DOC METRICS JSONL ({'overwrite' if no_append_jsonl else 'append'}) -> {OUT_DOC_METRICS_JSONL}")

        for i, doc in enumerate(docs, start=1):
            year = str(doc.report_year)
            doc_t0 = time.perf_counter()

            pass1_elapsed_sum_s = 0.0
            pass2_elapsed_sum_s = 0.0

            pass1_prompt_tokens_est_sum = 0
            pass2_prompt_tokens_est_sum = 0

            pass1_prompt_tokens_api_sum = 0
            pass1_completion_tokens_api_sum = 0
            pass1_total_tokens_api_sum = 0

            pass2_prompt_tokens_api_sum = 0
            pass2_completion_tokens_api_sum = 0
            pass2_total_tokens_api_sum = 0

            n_pass1_calls_ok = 0
            n_pass1_calls_error = 0
            n_pass2_calls_ok = 0
            n_pass2_calls_error = 0
            company_name = resolve_company_name_for_benchmark(MARKER_ARTIFACTS_DIR, doc.slug, year)
            processed_company_years.add((company_name, year))

            print(f"[{i}/{len(docs)}] {company_name} ({doc.slug}) {year} -> {doc.focused_md}")

            md_text = load_markdown(doc.focused_md)
            pages = split_into_pages(md_text)

            overhead_tokens = 4000
            max_chunk_tokens = max(1000, safe_input_budget - overhead_tokens)
            target = min(chunk_tokens, max_chunk_tokens)

            chunks = build_chunks_from_pages(pages=pages, target_chunk_tokens=target, max_chunk_tokens=max_chunk_tokens)
            if not chunks:
                doc_metric = {
                    "slug": doc.slug,
                    "company": company_name,
                    "report_year": year,
                    "status": "empty",
                    "error_stage": "chunking",
                    "error_message": None,
                    "n_chunks": 0,
                    "n_evidence_rows_doc": 0,
                    "n_pass2_batches": 0,
                    "pass1_elapsed_sum_s": 0.0,
                    "pass2_elapsed_sum_s": 0.0,
                    "llm_total_elapsed_sum_s": 0.0,
                    "doc_total_elapsed_s": round(time.perf_counter() - doc_t0, 6),
                    "pass1_prompt_tokens_est_sum": 0,
                    "pass2_prompt_tokens_est_sum": 0,
                    "total_prompt_tokens_est_sum": 0,
                    "pass1_prompt_tokens_api_sum": 0,
                    "pass1_completion_tokens_api_sum": 0,
                    "pass1_total_tokens_api_sum": 0,
                    "pass2_prompt_tokens_api_sum": 0,
                    "pass2_completion_tokens_api_sum": 0,
                    "pass2_total_tokens_api_sum": 0,
                    "total_prompt_tokens_api_sum": 0,
                    "total_completion_tokens_api_sum": 0,
                    "total_tokens_api_sum": 0,
                    "n_pass1_calls_ok": 0,
                    "n_pass1_calls_error": 0,
                    "n_pass2_calls_ok": 0,
                    "n_pass2_calls_error": 0,
                }
                w_doc_metrics.write(json.dumps(doc_metric, ensure_ascii=False) + "\n")
                doc_metrics_rows.append(doc_metric)

                print(f"[SKIP] nessun chunk creato per {company_name} {year}")
                continue

            n_chunks = len(chunks)
            print(f"   -> chunks: {n_chunks} (target={target} max={max_chunk_tokens})")

            # PASS1: accumula evidence rows a livello documento
            doc_evidence_rows: List[Dict[str, Any]] = []

            for chunk_idx, ch in enumerate(chunks, start=1):
                chunk_text = ch["text"]

                if count_tokens(chunk_text) > max_chunk_tokens:
                    if ENCODER is not None:
                        toks = ENCODER.encode(chunk_text)[:max_chunk_tokens]
                        chunk_text = ENCODER.decode(toks)
                    else:
                        chunk_text = chunk_text[: max_chunk_tokens * 4]

                developer, user = build_prompt_pass1(company_name, year, chunk_text, chunk_idx)
                prompt_tokens_est = count_tokens(developer) + count_tokens(user)

                t0 = time.perf_counter()
                try:
                    out1, llm_meta1 = call_llm_json(client, developer, user, max_retries=3)
                    elapsed_s = time.perf_counter() - t0
                except Exception as e:
                    elapsed_s = time.perf_counter() - t0

                    pass1_elapsed_sum_s += elapsed_s
                    pass1_prompt_tokens_est_sum += prompt_tokens_est
                    n_pass1_calls_error += 1

                    metric_row = {
                        "stage": "pass1",
                        "slug": doc.slug,
                        "company": company_name,
                        "report_year": year,
                        "chunk_index": chunk_idx,
                        "batch_index": None,
                        "source_md": str(doc.focused_md),
                        "n_chunks": n_chunks,
                        "start_page": ch.get("start_page"),
                        "end_page": ch.get("end_page"),
                        "chunk_tokens_est": ch.get("tokens"),
                        "n_evidence_lines_in_batch": None,
                        "prompt_tokens_est": prompt_tokens_est,
                        "elapsed_s": round(elapsed_s, 6),
                        "status": "error",
                        "error_message": str(e),
                        "attempt_used": None,
                        "prompt_tokens_api": None,
                        "completion_tokens_api": None,
                        "total_tokens_api": None,
                        "n_evidence_rows_norm": None,
                        "n_rows_normalized": None,
                    }

                    w_metrics.write(json.dumps(metric_row, ensure_ascii=False) + "\n")
                    metrics_rows.append(metric_row)

                    print(f"[SKIP] PASS1 LLM error: {company_name} {year} chunk={chunk_idx}/{n_chunks} -> {e}")
                    continue

                out1["company"] = company_name
                out1["report_year"] = year
                out1["chunk_index"] = chunk_idx

                evi_norm = normalize_evidence_rows(out1)
                doc_evidence_rows.extend(evi_norm)

                pass1_elapsed_sum_s += elapsed_s
                pass1_prompt_tokens_est_sum += prompt_tokens_est
                pass1_prompt_tokens_api_sum += _n0(llm_meta1.get("prompt_tokens"))
                pass1_completion_tokens_api_sum += _n0(llm_meta1.get("completion_tokens"))
                pass1_total_tokens_api_sum += _n0(llm_meta1.get("total_tokens"))
                n_pass1_calls_ok += 1

                out1["_meta"] = {
                    "stage": "pass1_evidence",
                    "slug": doc.slug,
                    "company_name_forced": company_name,
                    "year": year,
                    "source_md": str(doc.focused_md),
                    "chunk_index": chunk_idx,
                    "n_chunks": n_chunks,
                    "start_page": ch.get("start_page"),
                    "end_page": ch.get("end_page"),
                    "chunk_tokens_est": ch.get("tokens"),
                    "prompt_tokens_est": prompt_tokens_est,
                    "elapsed_s": round(elapsed_s, 6),
                    "attempt_used": llm_meta1.get("attempt_used"),
                    "prompt_tokens_api": llm_meta1.get("prompt_tokens"),
                    "completion_tokens_api": llm_meta1.get("completion_tokens"),
                    "total_tokens_api": llm_meta1.get("total_tokens"),
                    "n_evidence_rows_norm": len(evi_norm),
                }

                metric_row = {
                    "stage": "pass1",
                    "slug": doc.slug,
                    "company": company_name,
                    "report_year": year,
                    "chunk_index": chunk_idx,
                    "batch_index": None,
                    "source_md": str(doc.focused_md),
                    "n_chunks": n_chunks,
                    "start_page": ch.get("start_page"),
                    "end_page": ch.get("end_page"),
                    "chunk_tokens_est": ch.get("tokens"),
                    "n_evidence_lines_in_batch": None,
                    "prompt_tokens_est": prompt_tokens_est,
                    "elapsed_s": round(elapsed_s, 6),
                    "status": "ok",
                    "error_message": None,
                    "attempt_used": llm_meta1.get("attempt_used"),
                    "prompt_tokens_api": llm_meta1.get("prompt_tokens"),
                    "completion_tokens_api": llm_meta1.get("completion_tokens"),
                    "total_tokens_api": llm_meta1.get("total_tokens"),
                    "n_evidence_rows_norm": len(evi_norm),
                    "n_rows_normalized": None,
                }

                out1["evidence_rows"] = evi_norm
                w_evi.write(json.dumps(out1, ensure_ascii=False) + "\n")
                w_metrics.write(json.dumps(metric_row, ensure_ascii=False) + "\n")

                metrics_rows.append(metric_row)
                time.sleep(SLEEP_BETWEEN_CALLS_S)

            # dedup a livello documento per raw_row
            dedup_map: Dict[str, Dict[str, Any]] = {}
            for r in doc_evidence_rows:
                rr = _norm_space(str(r.get("raw_row") or ""))
                if not rr:
                    continue
                key = rr.lower()
                prev = dedup_map.get(key)
                if prev is None or _evidence_row_score(r) > _evidence_row_score(prev):
                    dedup_map[key] = r

            doc_evidence_dedup = list(dedup_map.values())
            if not doc_evidence_dedup:
                doc_metric = {
                    "slug": doc.slug,
                    "company": company_name,
                    "report_year": year,
                    "status": "empty",
                    "error_stage": "pass1_to_pass2",
                    "error_message": None,
                    "n_chunks": n_chunks,
                    "n_evidence_rows_doc": 0,
                    "n_pass2_batches": 0,
                    "pass1_elapsed_sum_s": round(pass1_elapsed_sum_s, 6),
                    "pass2_elapsed_sum_s": 0.0,
                    "llm_total_elapsed_sum_s": round(pass1_elapsed_sum_s, 6),
                    "doc_total_elapsed_s": round(time.perf_counter() - doc_t0, 6),
                    "pass1_prompt_tokens_est_sum": pass1_prompt_tokens_est_sum,
                    "pass2_prompt_tokens_est_sum": 0,
                    "total_prompt_tokens_est_sum": pass1_prompt_tokens_est_sum,
                    "pass1_prompt_tokens_api_sum": pass1_prompt_tokens_api_sum,
                    "pass1_completion_tokens_api_sum": pass1_completion_tokens_api_sum,
                    "pass1_total_tokens_api_sum": pass1_total_tokens_api_sum,
                    "pass2_prompt_tokens_api_sum": 0,
                    "pass2_completion_tokens_api_sum": 0,
                    "pass2_total_tokens_api_sum": 0,
                    "total_prompt_tokens_api_sum": pass1_prompt_tokens_api_sum,
                    "total_completion_tokens_api_sum": pass1_completion_tokens_api_sum,
                    "total_tokens_api_sum": pass1_total_tokens_api_sum,
                    "n_pass1_calls_ok": n_pass1_calls_ok,
                    "n_pass1_calls_error": n_pass1_calls_error,
                    "n_pass2_calls_ok": 0,
                    "n_pass2_calls_error": 0,
                }

                w_doc_metrics.write(json.dumps(doc_metric, ensure_ascii=False) + "\n")
                doc_metrics_rows.append(doc_metric)

                print(f"   -> [PASS2 SKIP] nessuna evidence row per {company_name} {year}")
                continue

            # costruisci evidence document lines (deterministico)
            doc_lines = format_evidence_doc_lines(doc_evidence_dedup)

            pass2_overhead = 4000
            pass2_max_tokens = max(2000, pass2_max_input_tokens - pass2_overhead)
            batches = pack_lines_into_batches(doc_lines, max_tokens=pass2_max_tokens)
            print(f"   -> evidence rows: {len(doc_evidence_dedup)} | pass2 batches: {len(batches)} (max_tokens={pass2_max_tokens})")

            for b_idx, lines_batch in enumerate(batches, start=1):
                evidence_doc = "\n".join(lines_batch)

                developer2, user2 = build_prompt_pass2(company_name, year, evidence_doc, b_idx, len(batches))
                prompt_tokens_est2 = count_tokens(developer2) + count_tokens(user2)

                t0 = time.perf_counter()
                try:
                    out2, llm_meta2 = call_llm_json(client, developer2, user2, max_retries=3)
                    elapsed_s2 = time.perf_counter() - t0
                except Exception as e:
                    elapsed_s2 = time.perf_counter() - t0

                    pass2_elapsed_sum_s += elapsed_s2
                    pass2_prompt_tokens_est_sum += prompt_tokens_est2
                    n_pass2_calls_error += 1

                    metric_row = {
                        "stage": "pass2",
                        "slug": doc.slug,
                        "company": company_name,
                        "report_year": year,
                        "chunk_index": None,
                        "batch_index": b_idx,
                        "source_md": str(doc.focused_md),
                        "n_chunks": n_chunks,
                        "start_page": None,
                        "end_page": None,
                        "chunk_tokens_est": count_tokens(evidence_doc),
                        "n_evidence_lines_in_batch": len(lines_batch),
                        "prompt_tokens_est": prompt_tokens_est2,
                        "elapsed_s": round(elapsed_s2, 6),
                        "status": "error",
                        "error_message": str(e),
                        "attempt_used": None,
                        "prompt_tokens_api": None,
                        "completion_tokens_api": None,
                        "total_tokens_api": None,
                        "n_evidence_rows_norm": None,
                        "n_rows_normalized": None,
                    }

                    w_metrics.write(json.dumps(metric_row, ensure_ascii=False) + "\n")
                    metrics_rows.append(metric_row)

                    print(f"[SKIP] PASS2 LLM error: {company_name} {year} batch={b_idx}/{len(batches)} -> {e}")
                    continue

                out2["company"] = company_name
                out2["report_year"] = year
                out2["batch_index"] = b_idx

                rows_pass2 = normalize_rows_pass2(out2, company_forced=company_name, year_forced=year)

                pass2_elapsed_sum_s += elapsed_s2
                pass2_prompt_tokens_est_sum += prompt_tokens_est2
                pass2_prompt_tokens_api_sum += _n0(llm_meta2.get("prompt_tokens"))
                pass2_completion_tokens_api_sum += _n0(llm_meta2.get("completion_tokens"))
                pass2_total_tokens_api_sum += _n0(llm_meta2.get("total_tokens"))
                n_pass2_calls_ok += 1

                out2["_meta"] = {
                    "stage": "pass2_final",
                    "slug": doc.slug,
                    "company_name_forced": company_name,
                    "year": year,
                    "source_md": str(doc.focused_md),
                    "pass2_batch_index": b_idx,
                    "pass2_n_batches": len(batches),
                    "n_evidence_lines_in_batch": len(lines_batch),
                    "prompt_tokens_est": prompt_tokens_est2,
                    "elapsed_s": round(elapsed_s2, 6),
                    "attempt_used": llm_meta2.get("attempt_used"),
                    "prompt_tokens_api": llm_meta2.get("prompt_tokens"),
                    "completion_tokens_api": llm_meta2.get("completion_tokens"),
                    "total_tokens_api": llm_meta2.get("total_tokens"),
                    "evidence_preview": lines_batch[:10],
                }

                metric_row = {
                    "stage": "pass2",
                    "slug": doc.slug,
                    "company": company_name,
                    "report_year": year,
                    "chunk_index": None,
                    "batch_index": b_idx,
                    "source_md": str(doc.focused_md),
                    "n_chunks": n_chunks,
                    "start_page": None,
                    "end_page": None,
                    "chunk_tokens_est": count_tokens(evidence_doc),
                    "n_evidence_lines_in_batch": len(lines_batch),
                    "prompt_tokens_est": prompt_tokens_est2,
                    "elapsed_s": round(elapsed_s2, 6),
                    "status": "ok",
                    "error_message": None,
                    "attempt_used": llm_meta2.get("attempt_used"),
                    "prompt_tokens_api": llm_meta2.get("prompt_tokens"),
                    "completion_tokens_api": llm_meta2.get("completion_tokens"),
                    "total_tokens_api": llm_meta2.get("total_tokens"),
                    "n_evidence_rows_norm": None,
                    "n_rows_normalized": len(rows_pass2),
                }

                w_final.write(json.dumps(out2, ensure_ascii=False) + "\n")
                w_metrics.write(json.dumps(metric_row, ensure_ascii=False) + "\n")

                metrics_rows.append(metric_row)
                all_flat_rows.extend(rows_pass2)
                time.sleep(SLEEP_BETWEEN_CALLS_S)
        if n_pass1_calls_ok > 0 and n_pass2_calls_ok > 0 and n_pass1_calls_error == 0 and n_pass2_calls_error == 0:
            doc_status = "ok"
        elif (n_pass1_calls_ok + n_pass2_calls_ok) > 0:
            doc_status = "partial"
        else:
            doc_status = "error"

        doc_metric = {
            "slug": doc.slug,
            "company": company_name,
            "report_year": year,
            "status": doc_status,
            "error_stage": None,
            "error_message": None,
            "n_chunks": n_chunks,
            "n_evidence_rows_doc": len(doc_evidence_dedup),
            "n_pass2_batches": len(batches),
            "pass1_elapsed_sum_s": round(pass1_elapsed_sum_s, 6),
            "pass2_elapsed_sum_s": round(pass2_elapsed_sum_s, 6),
            "llm_total_elapsed_sum_s": round(pass1_elapsed_sum_s + pass2_elapsed_sum_s, 6),
            "doc_total_elapsed_s": round(time.perf_counter() - doc_t0, 6),
            "pass1_prompt_tokens_est_sum": pass1_prompt_tokens_est_sum,
            "pass2_prompt_tokens_est_sum": pass2_prompt_tokens_est_sum,
            "total_prompt_tokens_est_sum": pass1_prompt_tokens_est_sum + pass2_prompt_tokens_est_sum,
            "pass1_prompt_tokens_api_sum": pass1_prompt_tokens_api_sum,
            "pass1_completion_tokens_api_sum": pass1_completion_tokens_api_sum,
            "pass1_total_tokens_api_sum": pass1_total_tokens_api_sum,
            "pass2_prompt_tokens_api_sum": pass2_prompt_tokens_api_sum,
            "pass2_completion_tokens_api_sum": pass2_completion_tokens_api_sum,
            "pass2_total_tokens_api_sum": pass2_total_tokens_api_sum,
            "total_prompt_tokens_api_sum": pass1_prompt_tokens_api_sum + pass2_prompt_tokens_api_sum,
            "total_completion_tokens_api_sum": pass1_completion_tokens_api_sum + pass2_completion_tokens_api_sum,
            "total_tokens_api_sum": pass1_total_tokens_api_sum + pass2_total_tokens_api_sum,
            "n_pass1_calls_ok": n_pass1_calls_ok,
            "n_pass1_calls_error": n_pass1_calls_error,
            "n_pass2_calls_ok": n_pass2_calls_ok,
            "n_pass2_calls_error": n_pass2_calls_error,
        }

        w_doc_metrics.write(json.dumps(doc_metric, ensure_ascii=False) + "\n")
        doc_metrics_rows.append(doc_metric)
    # =========================
    # CSV finale (sempre scritto + quoting)
    # =========================
    base_cols = ["company", "report_year", "chunk_index", "activity", "Sub_activity_code"]

    if all_flat_rows:
        deduped = dedup_rows(all_flat_rows)
        df = pd.DataFrame(deduped)
        if "chunk_index" not in df.columns:
            df["chunk_index"] = None
        df = df.drop_duplicates(subset=["company", "report_year", "activity", "Sub_activity_code"])
    else:
        df = pd.DataFrame(columns=base_cols)

    for c in base_cols:
        if c not in df.columns:
            df[c] = None

    existing_pairs = set()
    if not df.empty:
        existing_pairs = set(zip(df["company"].astype(str), df["report_year"].astype(str)))

    placeholders: List[Dict[str, Any]] = []
    for comp, yr in sorted(processed_company_years):
        if (str(comp), str(yr)) not in existing_pairs:
            placeholders.append(
                {
                    "company": comp,
                    "report_year": yr,
                    "chunk_index": None,
                    "activity": "",
                    "Sub_activity_code": None,
                }
            )

    if placeholders:
        df = pd.concat([df, pd.DataFrame(placeholders)], ignore_index=True)

    df = df.drop_duplicates(subset=["company", "report_year", "activity", "Sub_activity_code"])
    df = df.sort_values(["company", "report_year", "Sub_activity_code", "activity"], na_position="last")

    # Quoting per evitare rotture CSV con virgole in company/activity
    df.to_csv(OUT_CSV, index=False, encoding="utf-8", quoting=csv.QUOTE_MINIMAL)

    if all_flat_rows:
        print(f"[OUT] CSV  -> {OUT_CSV} ({len(df)} righe, deduped + placeholder)")
    else:
        print(f"[OUT] CSV  -> {OUT_CSV} ({len(df)} righe, SOLO placeholder: nessuna attività estratta)")
    print("      Nota: 'chunk_index' nel CSV (RAG2) corrisponde a 'batch_index' del Pass2, non ai chunk originali.")

    if metrics_rows:
        df_metrics = pd.DataFrame(metrics_rows)
        for c in ["chunk_index", "batch_index", "n_evidence_rows_norm", "n_rows_normalized"]:
            if c not in df_metrics.columns:
                df_metrics[c] = None

        df_metrics = df_metrics.sort_values(
            ["company", "report_year", "stage", "chunk_index", "batch_index"],
            na_position="last"
        )
        df_metrics.to_csv(OUT_METRICS_CSV, index=False, encoding="utf-8", quoting=csv.QUOTE_MINIMAL)

        metrics_summary = build_metrics_summary(df_metrics)
        OUT_METRICS_SUMMARY_JSON.write_text(
            json.dumps(metrics_summary, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        print(f"[OUT] LLM METRICS CSV     -> {OUT_METRICS_CSV} ({len(df_metrics)} righe)")
        print(f"[OUT] LLM METRICS SUMMARY -> {OUT_METRICS_SUMMARY_JSON}")
    else:
        print("[WARN] Nessuna metrica LLM raccolta.")


    if doc_metrics_rows:
        df_doc_metrics = pd.DataFrame(doc_metrics_rows)
        df_doc_metrics = df_doc_metrics.sort_values(
            ["company", "report_year"],
            na_position="last"
        )
        df_doc_metrics.to_csv(OUT_DOC_METRICS_CSV, index=False, encoding="utf-8", quoting=csv.QUOTE_MINIMAL)

        doc_summary = build_doc_metrics_summary(df_doc_metrics)
        OUT_DOC_METRICS_SUMMARY_JSON.write_text(
            json.dumps(doc_summary, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        print(f"[OUT] DOC METRICS CSV     -> {OUT_DOC_METRICS_CSV} ({len(df_doc_metrics)} righe)")
        print(f"[OUT] DOC METRICS SUMMARY -> {OUT_DOC_METRICS_SUMMARY_JSON}")

        valid_doc = df_doc_metrics[df_doc_metrics["status"] != "error"].copy()
        if not valid_doc.empty:
            print(
                "[DOC TIME] "
                f"n={len(valid_doc)} | "
                f"pass1_mean={valid_doc['pass1_elapsed_sum_s'].mean():.3f}s | "
                f"pass2_mean={valid_doc['pass2_elapsed_sum_s'].mean():.3f}s | "
                f"llm_total_mean={valid_doc['llm_total_elapsed_sum_s'].mean():.3f}s | "
                f"doc_total_mean={valid_doc['doc_total_elapsed_s'].mean():.3f}s"
            )
    else:
        print("[WARN] Nessuna metrica documento raccolta.")
        
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only_first", action="store_true", help="Processa solo il primo documento (debug rapido).")
    ap.add_argument("--slug", type=str, default=None)
    ap.add_argument("--year", type=str, default=None)
    ap.add_argument("--chunk_tokens", type=int, default=20000)
    ap.add_argument("--safe_input_budget", type=int, default=90000)
    ap.add_argument("--pass2_max_input_tokens", type=int, default=60000)
    ap.add_argument("--no_append_jsonl", action="store_true")
    return ap.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(
        only_first=args.only_first,
        slug_filter=args.slug,
        year_filter=args.year,
        chunk_tokens=args.chunk_tokens,
        safe_input_budget=args.safe_input_budget,
        pass2_max_input_tokens=args.pass2_max_input_tokens,
        no_append_jsonl=args.no_append_jsonl,
    )

# Esempi:
# python 3_rag2_two_pass_extract_activities.py --only_first --no_append_jsonl
# python 3_rag2_two_pass_extract_activities.py --slug a_p_moller_maersk_a_s --year 2024 --no_append_jsonl
# python c:/Universita/TESI/esg_agent/RAG_full_bigger/3_rag2_two_pass_extract_activities.py --pass2_max_input_tokens 50000 --no_append_jsonl