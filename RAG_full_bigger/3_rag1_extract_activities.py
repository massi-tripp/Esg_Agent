# File: 3_rag1_extract_activities.py

from __future__ import annotations

import argparse
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

API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION", "2025-01-01-preview").strip()
CHAT_DEPLOYMENT = os.getenv("AZURE_DEPLOYMENT_GPT5_MINI", "gpt-5.1-chat").strip()

REASONING_EFFORT = os.getenv("AZURE_REASONING_EFFORT", "medium").strip().lower()
MAX_COMPLETION_TOKENS = int(os.getenv("AZURE_MAX_COMPLETION_TOKENS", "2500"))
SLEEP_BETWEEN_CALLS_S = float(os.getenv("AZURE_SLEEP_S", "0.2"))

# ✅ FIX env var corretta
AZURE_INPUT_MAX_TOKENS = int(os.getenv("AZURE_INPUT_MAX_TOKENS", "111616"))


# =========================
# PATHS
# =========================
MARKER_ARTIFACTS_DIR = Path(r"C:\Universita\TESI\esg_agent\RAG_full_bigger\marker_artifacts")
OUT_DIR = Path(r"C:\Universita\TESI\esg_agent\RAG_full_bigger\rag_out")
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_JSONL = OUT_DIR / "activities_extracted.jsonl"
OUT_CSV = OUT_DIR / "activities_extracted.csv"

OUT_METRICS_JSONL = OUT_DIR / "activities_extracted_metrics.jsonl"
OUT_METRICS_CSV = OUT_DIR / "activities_extracted_metrics.csv"
OUT_METRICS_SUMMARY_JSON = OUT_DIR / "activities_extracted_metrics_summary.json"
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
        return {"overall": {}, "by_year": {}}

    ok = df_metrics[df_metrics["status"] == "ok"].copy()
    if ok.empty:
        return {"overall": {}, "by_year": {}}

    summary = {
        "overall": {
            "elapsed_s": _series_stats(ok["elapsed_s"]),
            "chunk_tokens_est": _series_stats(ok["chunk_tokens_est"]),
            "prompt_tokens_est": _series_stats(ok["prompt_tokens_est"]),
            "prompt_tokens_api": _series_stats(ok["prompt_tokens_api"]),
            "completion_tokens_api": _series_stats(ok["completion_tokens_api"]),
            "total_tokens_api": _series_stats(ok["total_tokens_api"]),
            "n_rows_normalized": _series_stats(ok["n_rows_normalized"]),
        },
        "by_year": {},
    }

    for year, g in ok.groupby("report_year", dropna=False):
        summary["by_year"][str(year)] = {
            "n_calls": int(len(g)),
            "elapsed_s": _series_stats(g["elapsed_s"]),
            "completion_tokens_api": _series_stats(g["completion_tokens_api"]),
            "total_tokens_api": _series_stats(g["total_tokens_api"]),
            "n_rows_normalized": _series_stats(g["n_rows_normalized"]),
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
# COMPANY NAME (benchmark rules)  ✅ ALLINEATO A OPZIONE 2
# =========================
def resolve_company_name_for_benchmark(root: Path, slug: str, year: str) -> str:
    """
    Regole richieste (come Opzione 2):
      - 2024: <root>/<slug>/2024/marker_meta.json -> company_name_full (o company_full_name)
      - 2023: <root>/<slug>/2023/*_artifact.json -> company_name (fallback company_info.name)
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
        # preferisci file che contiene _2023_ nel nome, altrimenti primo *_artifact.json
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

    # altri anni (fallback robusto)
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
            ci = obj.get("company_info")
            if isinstance(ci, dict):
                nm = ci.get("name")
                if isinstance(nm, str) and nm.strip():
                    return nm.strip()

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
# PROMPT + CLIENT + DEDUP (invariati)
# =========================
def build_prompt(company_name: str, report_year: str, chunk_text: str, chunk_idx: int, n_chunks: int) -> Tuple[str, str]:
    developer = (
        "You are a Sustainable Finance Analyst specialized in EU Taxonomy disclosures. "
        "You must EXTRACT (not predict) EU Taxonomy activity rows from the provided text. "
        "Be extremely literal: only extract what is explicitly present. "
        "Return STRICT JSON only, no prose, no markdown."
    )

    user = f"""
TASK (EXTRACTION ONLY):
From the text below, extract EU Taxonomy economic activity ROWS that are explicitly present in tables or clearly formatted lists.

IMPORTANT:
- Deduplicate within this chunk: if the same (activity, Sub_activity_code) appears multiple times, output it only once.
- Do NOT infer missing codes or guess activities.

EXTRACT ONLY THESE FIELDS:
- company: "{company_name}"
- report_year: "{report_year}"
- activity: the activity name exactly as written in the report
- Sub_activity_code: the taxonomy code exactly as written (e.g., "CCM 3.3", "CCA 4.1", "6.10"). Accept also codes like C17.1.2 / A1.6.2 when they appear as part of activity lists in the taxonomy section. If no code is shown for that row, use null.

WHAT TO INCLUDE:
- Rows that look like “Economic activity | Code | …”
- Rows under sections like taxonomy-eligible / taxonomy-aligned / environmentally sustainable activities.

WHAT TO EXCLUDE:
- Any headings, section titles, group labels (e.g., "A. TAXONOMY-ELIGIBLE ACTIVITIES")
- Totals/summary KPI lines (turnover/capex/opex totals, percentages, subtotals)
- Anything not clearly an activity row

OUTPUT FORMAT:
Return exactly ONE JSON object, no extra text:
{{
  "company": "{company_name}",
  "report_year": "{report_year}",
  "chunk_index": {chunk_idx},
  "activities": [
    {{"activity": "...", "Sub_activity_code": "CCM 3.3"}}
  ]
}}

--- BEGIN REPORT CHUNK (MARKDOWN) ---
{chunk_text}
--- END REPORT CHUNK ---
"""
    return developer, user


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


def call_llm_extract(
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
            if "activities" not in data or not isinstance(data["activities"], list):
                raise ValueError("LLM JSON missing 'activities' list")

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

    raise RuntimeError(f"LLM extraction failed after {max_retries} retries: {last_err}")


def _norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _norm_code(code: Optional[str]) -> Optional[str]:
    if not code:
        return None
    c = _norm_space(code).upper()
    c = re.sub(r"^(CCM|CCA)\s*([0-9])", r"\1 \2", c)
    return c


def normalize_rows(result: Dict[str, Any]) -> List[Dict[str, Any]]:
    company = result.get("company")
    year = result.get("report_year")
    chunk_index = result.get("chunk_index")

    rows: List[Dict[str, Any]] = []
    for a in result.get("activities", []):
        if not isinstance(a, dict):
            continue
        activity = _norm_space(a.get("activity") or "")
        code = a.get("Sub_activity_code")
        code = _norm_code(code) if isinstance(code, str) else None
        if not activity:
            continue
        if re.search(r"\bturnover\b|\bcapex\b|\bopex\b|\btotal\b", activity, flags=re.IGNORECASE):
            continue
        rows.append({"company": company, "report_year": year, "chunk_index": chunk_index, "activity": activity, "Sub_activity_code": code})
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
        k = (str(r.get("company") or ""), str(r.get("report_year") or ""), _norm_space(str(r.get("activity") or "")).lower(), (_norm_code(r.get("Sub_activity_code")) or "").lower())
        uniq[k] = r
    return list(uniq.values())


def _rows_from_existing_jsonl(jsonl_path: Path) -> List[Dict[str, Any]]:
    if not jsonl_path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    with jsonl_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                rows.extend(normalize_rows(obj))
            except Exception:
                continue
    return rows


def run(
    only_first: bool = False,
    slug_filter: Optional[str] = None,
    year_filter: Optional[str] = None,
    chunk_tokens: int = 20_000,
    safe_input_budget: int = 90_000,
    no_append_jsonl: bool = False,
) -> None:
    if safe_input_budget >= AZURE_INPUT_MAX_TOKENS:
        safe_input_budget = AZURE_INPUT_MAX_TOKENS - 2000

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

    client = get_client()
    all_flat_rows: List[Dict[str, Any]] = _rows_from_existing_jsonl(OUT_JSONL) if not no_append_jsonl else []
    metrics_rows: List[Dict[str, Any]] = []

    mode = "w" if no_append_jsonl else "a"
    with OUT_JSONL.open(mode, encoding="utf-8") as w, OUT_METRICS_JSONL.open(mode, encoding="utf-8") as w_metrics:
        print(f"[OUT] JSONL ({'overwrite' if no_append_jsonl else 'append'}) -> {OUT_JSONL}")
        print(f"[OUT] METRICS JSONL ({'overwrite' if no_append_jsonl else 'append'}) -> {OUT_METRICS_JSONL}")

        for i, doc in enumerate(docs, start=1):
            year = str(doc.report_year)

            # ✅ ALLINEATO A OPZIONE 2
            company_name_full = resolve_company_name_for_benchmark(MARKER_ARTIFACTS_DIR, doc.slug, year)
            company_name = company_name_full

            print(f"[{i}/{len(docs)}] {company_name} {year} -> {doc.focused_md}")

            md_text = load_markdown(doc.focused_md)
            pages = split_into_pages(md_text)

            overhead_tokens = 4000
            max_chunk_tokens = max(1000, safe_input_budget - overhead_tokens)
            target = min(chunk_tokens, max_chunk_tokens)

            chunks = build_chunks_from_pages(pages=pages, target_chunk_tokens=target, max_chunk_tokens=max_chunk_tokens)
            if not chunks:
                print(f"[SKIP] nessun chunk creato per {company_name} {year}")
                continue

            n_chunks = len(chunks)
            print(f"   -> chunks: {n_chunks} (target={target} max={max_chunk_tokens})")

            for chunk_idx, ch in enumerate(chunks, start=1):
                chunk_text = ch["text"]

                if count_tokens(chunk_text) > max_chunk_tokens:
                    if ENCODER is not None:
                        toks = ENCODER.encode(chunk_text)[:max_chunk_tokens]
                        chunk_text = ENCODER.decode(toks)
                    else:
                        chunk_text = chunk_text[: max_chunk_tokens * 4]

                developer, user = build_prompt(company_name, year, chunk_text, chunk_idx, n_chunks)
                prompt_tokens_est = count_tokens(developer) + count_tokens(user)

                t0 = time.perf_counter()
                try:
                    result, llm_meta = call_llm_extract(client, developer, user, max_retries=3)
                    elapsed_s = time.perf_counter() - t0
                except Exception as e:
                    elapsed_s = time.perf_counter() - t0

                    metric_row = {
                        "slug": doc.slug,
                        "company": company_name,
                        "company_name_full": company_name_full,
                        "report_year": year,
                        "source_md": str(doc.focused_md),
                        "chunk_index": chunk_idx,
                        "n_chunks": n_chunks,
                        "start_page": ch.get("start_page"),
                        "end_page": ch.get("end_page"),
                        "chunk_tokens_est": ch.get("tokens"),
                        "prompt_tokens_est": prompt_tokens_est,
                        "elapsed_s": round(elapsed_s, 6),
                        "status": "error",
                        "error_message": str(e),
                        "attempt_used": None,
                        "prompt_tokens_api": None,
                        "completion_tokens_api": None,
                        "total_tokens_api": None,
                        "n_activities_raw": None,
                        "n_rows_normalized": None,
                    }

                    w_metrics.write(json.dumps(metric_row, ensure_ascii=False) + "\n")
                    metrics_rows.append(metric_row)

                    print(f"[SKIP] LLM error: {company_name} {year} chunk={chunk_idx}/{n_chunks} -> {e}")
                    continue

                normalized_rows = normalize_rows(result)

                result["_meta"] = {
                    "slug": doc.slug,
                    "company_name_full": company_name_full,
                    "year": year,
                    "source_md": str(doc.focused_md),
                    "chunk_index": chunk_idx,
                    "n_chunks": n_chunks,
                    "start_page": ch.get("start_page"),
                    "end_page": ch.get("end_page"),
                    "chunk_tokens_est": ch.get("tokens"),
                    "prompt_tokens_est": prompt_tokens_est,
                    "elapsed_s": round(elapsed_s, 6),
                    "attempt_used": llm_meta.get("attempt_used"),
                    "prompt_tokens_api": llm_meta.get("prompt_tokens"),
                    "completion_tokens_api": llm_meta.get("completion_tokens"),
                    "total_tokens_api": llm_meta.get("total_tokens"),
                }

                metric_row = {
                    "slug": doc.slug,
                    "company": company_name,
                    "company_name_full": company_name_full,
                    "report_year": year,
                    "source_md": str(doc.focused_md),
                    "chunk_index": chunk_idx,
                    "n_chunks": n_chunks,
                    "start_page": ch.get("start_page"),
                    "end_page": ch.get("end_page"),
                    "chunk_tokens_est": ch.get("tokens"),
                    "prompt_tokens_est": prompt_tokens_est,
                    "elapsed_s": round(elapsed_s, 6),
                    "status": "ok",
                    "error_message": None,
                    "attempt_used": llm_meta.get("attempt_used"),
                    "prompt_tokens_api": llm_meta.get("prompt_tokens"),
                    "completion_tokens_api": llm_meta.get("completion_tokens"),
                    "total_tokens_api": llm_meta.get("total_tokens"),
                    "n_activities_raw": len(result.get("activities", [])) if isinstance(result.get("activities"), list) else None,
                    "n_rows_normalized": len(normalized_rows),
                }

                w.write(json.dumps(result, ensure_ascii=False) + "\n")
                w_metrics.write(json.dumps(metric_row, ensure_ascii=False) + "\n")

                metrics_rows.append(metric_row)
                all_flat_rows.extend(normalized_rows)

                time.sleep(SLEEP_BETWEEN_CALLS_S)

    if all_flat_rows:
        deduped = dedup_rows(all_flat_rows)
        df = pd.DataFrame(deduped)
        df = df.drop_duplicates(subset=["company", "report_year", "activity", "Sub_activity_code"])
        df = df.sort_values(["company", "report_year", "Sub_activity_code", "activity"], na_position="last")
        df.to_csv(OUT_CSV, index=False, encoding="utf-8")
        print(f"[OUT] CSV  -> {OUT_CSV} ({len(df)} righe, deduped)")
    else:
        print("[WARN] Nessuna riga estratta, CSV non creato.")

    if metrics_rows:
        df_metrics = pd.DataFrame(metrics_rows)
        df_metrics = df_metrics.sort_values(
            ["company", "report_year", "chunk_index"],
            na_position="last"
        )
        df_metrics.to_csv(OUT_METRICS_CSV, index=False, encoding="utf-8")

        summary = build_metrics_summary(df_metrics)
        OUT_METRICS_SUMMARY_JSON.write_text(
            json.dumps(summary, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        print(f"[OUT] METRICS CSV     -> {OUT_METRICS_CSV} ({len(df_metrics)} righe)")
        print(f"[OUT] METRICS SUMMARY -> {OUT_METRICS_SUMMARY_JSON}")

        ok = df_metrics[df_metrics["status"] == "ok"].copy()
        if not ok.empty:
            print(
                "[TIME] "
                f"n={len(ok)} | "
                f"min={ok['elapsed_s'].min():.3f}s | "
                f"q1={ok['elapsed_s'].quantile(0.25):.3f}s | "
                f"mean={ok['elapsed_s'].mean():.3f}s | "
                f"median={ok['elapsed_s'].median():.3f}s | "
                f"q3={ok['elapsed_s'].quantile(0.75):.3f}s | "
                f"max={ok['elapsed_s'].max():.3f}s"
            )
    else:
        print("[WARN] Nessuna metrica raccolta.")
        
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only_first", action="store_true")
    ap.add_argument("--slug", type=str, default=None)
    ap.add_argument("--year", type=str, default=None)
    ap.add_argument("--chunk_tokens", type=int, default=20000)
    ap.add_argument("--safe_input_budget", type=int, default=90000)
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
        no_append_jsonl=args.no_append_jsonl,
    )
# Esempi:
# python c:/Universita/TESI/esg_agent/RAG_full_bigger/3_rag1_extract_activities.py --only_first
# python 3_rag1_extract_activities.py --slug a_p_moller_maersk_a_s --year 2024
# python 3_rag1_extract_activities.py --chunk_tokens 25000
# python  c:/Universita/TESI/esg_agent/RAG_full_bigger/3_rag1_extract_activities.py --chunk_tokens 25000 --no_append_jsonl
