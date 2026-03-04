# File: 3_rag3_hybrid_retrieval_extract_activities.py

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
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

# Chat model for extraction
CHAT_DEPLOYMENT = os.getenv("AZURE_DEPLOYMENT_GPT5_MINI", "gpt-5.1-chat").strip()

# Embeddings deployment (Azure requires a deployment name)
EMBEDDINGS_DEPLOYMENT = os.getenv("AZURE_DEPLOYMENT_EMBEDDINGS", "text-embedding-3-small").strip()

REASONING_EFFORT = os.getenv("AZURE_REASONING_EFFORT", "medium").strip().lower()
MAX_COMPLETION_TOKENS = int(os.getenv("AZURE_MAX_COMPLETION_TOKENS", "2500"))
SLEEP_BETWEEN_CALLS_S = float(os.getenv("AZURE_SLEEP_S", "0.2"))

AZURE_INPUT_MAX_TOKENS = int(os.getenv("AZURE_INPUT_MAX_TOKENS", "111616"))


# =========================
# PATHS
# =========================
MARKER_ARTIFACTS_DIR = Path(r"C:\Universita\TESI\esg_agent\RAG_full_bigger\marker_artifacts")

OUT_DIR = Path(r"C:\Universita\TESI\esg_agent\RAG_full_bigger\rag3_out")
OUT_DIR.mkdir(parents=True, exist_ok=True)

INDEX_DIR = OUT_DIR / "index"
INDEX_DIR.mkdir(parents=True, exist_ok=True)

OUT_JSONL = OUT_DIR / "activities_extracted.jsonl"
OUT_CSV = OUT_DIR / "activities_extracted.csv"


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
# SPLIT BY PAGES
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


# =========================
# CHUNKING for RETRIEVAL (pagina -> paragrafi -> chunk ~1000-1500 tokens)
# =========================
def build_retrieval_chunks_from_pages(
    pages: List[Tuple[Optional[int], str]],
    target_chunk_tokens: int = 1200,
    max_chunk_tokens: int = 1600,
) -> List[Dict[str, Any]]:
    """
    Chunking più fine rispetto a rag1/rag2:
    - spezza per pagina
    - dentro pagina spezza per paragrafi
    - raggruppa paragrafi finché raggiunge target_chunk_tokens
    """
    chunks: List[Dict[str, Any]] = []
    cur_parts: List[str] = []
    cur_tokens = 0
    cur_start_page: Optional[int] = None
    cur_end_page: Optional[int] = None

    def flush():
        nonlocal cur_parts, cur_tokens, cur_start_page, cur_end_page
        if cur_parts:
            chunks.append(
                {
                    "text": "\n\n".join(cur_parts).strip(),
                    "start_page": cur_start_page,
                    "end_page": cur_end_page,
                    "tokens": cur_tokens,
                }
            )
        cur_parts, cur_tokens, cur_start_page, cur_end_page = [], 0, None, None

    for page_no, page_text in pages:
        paras = [p.strip() for p in re.split(r"\n{2,}", page_text) if p.strip()]
        for para in paras:
            piece = f"## Page {page_no}\n\n{para}" if page_no is not None else para
            pt = count_tokens(piece)

            # para enorme -> taglia brutalmente (raro nei focused)
            if pt > max_chunk_tokens:
                if ENCODER is not None:
                    toks = ENCODER.encode(piece)[:max_chunk_tokens]
                    piece = ENCODER.decode(toks)
                    pt = count_tokens(piece)
                else:
                    piece = piece[: max_chunk_tokens * 4]
                    pt = count_tokens(piece)

            if not cur_parts:
                cur_start_page = page_no
                cur_end_page = page_no

            if cur_tokens + pt > max_chunk_tokens and cur_parts:
                flush()
                cur_start_page = page_no
                cur_end_page = page_no

            cur_parts.append(piece)
            cur_tokens += pt
            cur_end_page = page_no

            if cur_tokens >= target_chunk_tokens:
                flush()

    flush()
    return chunks


# =========================
# SIMPLE BM25 (no external deps)
# =========================
_TOKEN_RE = re.compile(r"[A-Za-z0-9\.\-_/]+")


def bm25_tokenize(text: str) -> List[str]:
    toks = _TOKEN_RE.findall((text or "").lower())
    # filtri leggeri: elimina token troppo corti
    return [t for t in toks if len(t) >= 2]


def build_bm25_index(tokenized_docs: List[List[str]]) -> Dict[str, Any]:
    """
    Implementazione BM25 classica (Okapi) per un "corpus" (i chunk di UN documento).
    """
    N = len(tokenized_docs)
    df: Dict[str, int] = {}
    doc_lens = np.array([len(d) for d in tokenized_docs], dtype=np.float32)
    avgdl = float(doc_lens.mean()) if N > 0 else 0.0

    for doc in tokenized_docs:
        seen = set(doc)
        for t in seen:
            df[t] = df.get(t, 0) + 1

    # idf con smoothing (BM25+ style light)
    idf: Dict[str, float] = {}
    for t, f in df.items():
        idf[t] = float(np.log(1.0 + (N - f + 0.5) / (f + 0.5)))

    return {"N": N, "idf": idf, "doc_lens": doc_lens, "avgdl": avgdl}


def bm25_scores(
    query_tokens: List[str],
    tokenized_docs: List[List[str]],
    bm25_state: Dict[str, Any],
    k1: float = 1.5,
    b: float = 0.75,
) -> np.ndarray:
    N = bm25_state["N"]
    if N == 0:
        return np.zeros((0,), dtype=np.float32)

    idf: Dict[str, float] = bm25_state["idf"]
    doc_lens: np.ndarray = bm25_state["doc_lens"]
    avgdl: float = bm25_state["avgdl"] or 1.0

    q = query_tokens
    if not q:
        return np.zeros((N,), dtype=np.float32)

    scores = np.zeros((N,), dtype=np.float32)

    # precompute term frequencies per doc (sì, è O(N*len(doc)), ma qui corpus è piccolo: chunk di un focused)
    # per velocizzare un minimo, usiamo dict per doc
    for i, doc in enumerate(tokenized_docs):
        tf: Dict[str, int] = {}
        for t in doc:
            tf[t] = tf.get(t, 0) + 1

        dl = float(doc_lens[i])
        denom_norm = k1 * (1.0 - b + b * (dl / avgdl))

        s = 0.0
        for t in q:
            if t not in tf:
                continue
            f = float(tf[t])
            term_idf = idf.get(t, 0.0)
            s += term_idf * (f * (k1 + 1.0)) / (f + denom_norm)
        scores[i] = float(s)

    return scores


# =========================
# Embeddings helpers (Azure)
# =========================
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


def embed_texts(client: AzureOpenAI, texts: List[str], batch_size: int = 64, max_retries: int = 3) -> np.ndarray:
    """
    Restituisce array shape (len(texts), dim).
    """
    all_vecs: List[List[float]] = []

    for start in range(0, len(texts), batch_size):
        batch = texts[start : start + batch_size]
        last_err: Optional[Exception] = None

        for attempt in range(1, max_retries + 1):
            try:
                resp = client.embeddings.create(
                    model=EMBEDDINGS_DEPLOYMENT,
                    input=batch,
                )
                # l'API restituisce resp.data in ordine
                vecs = [d.embedding for d in resp.data]
                all_vecs.extend(vecs)
                break
            except Exception as e:
                last_err = e
                time.sleep(1.5 * attempt)
        else:
            raise RuntimeError(f"Embeddings call failed after {max_retries} retries: {last_err}")

        time.sleep(SLEEP_BETWEEN_CALLS_S)

    arr = np.array(all_vecs, dtype=np.float32)
    return arr


def l2_normalize(mat: np.ndarray, eps: float = 1e-8) -> np.ndarray:
    if mat.size == 0:
        return mat
    norms = np.linalg.norm(mat, axis=1, keepdims=True)
    norms = np.maximum(norms, eps)
    return mat / norms


def cosine_sim_matrix(query_vecs: np.ndarray, doc_vecs: np.ndarray) -> np.ndarray:
    """
    query_vecs: (Q, D) normalized
    doc_vecs: (N, D) normalized
    return: (Q, N) dot product
    """
    if query_vecs.size == 0 or doc_vecs.size == 0:
        return np.zeros((query_vecs.shape[0], doc_vecs.shape[0]), dtype=np.float32)
    return (query_vecs @ doc_vecs.T).astype(np.float32)


# =========================
# Hybrid scoring utilities
# =========================
def minmax_norm(x: np.ndarray) -> np.ndarray:
    if x.size == 0:
        return x
    mn = float(x.min())
    mx = float(x.max())
    if mx - mn < 1e-12:
        return np.zeros_like(x, dtype=np.float32)
    return ((x - mn) / (mx - mn)).astype(np.float32)


def merge_scores_hybrid(bm25: np.ndarray, emb: np.ndarray, w_bm25: float, w_emb: float) -> np.ndarray:
    """
    bm25: (N,)
    emb:  (N,)  (già aggregato su multi-query: es. max sim)
    """
    bm25_n = minmax_norm(bm25)
    emb_n = minmax_norm(emb)
    return (w_bm25 * bm25_n + w_emb * emb_n).astype(np.float32)


# =========================
# Index persistence (per doc-year)
# =========================
def _hash_text(s: str) -> str:
    return hashlib.sha1((s or "").encode("utf-8", errors="ignore")).hexdigest()[:16]


def index_paths(slug: str, year: str) -> Tuple[Path, Path]:
    base = f"{slug}__{year}"
    meta = INDEX_DIR / f"{base}.jsonl"
    vecs = INDEX_DIR / f"{base}.npz"
    return meta, vecs


def load_index(slug: str, year: str) -> Optional[Dict[str, Any]]:
    meta_path, vec_path = index_paths(slug, year)
    if not meta_path.exists() or not vec_path.exists():
        return None

    chunks: List[Dict[str, Any]] = []
    with meta_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                chunks.append(json.loads(line))
            except Exception:
                continue

    try:
        data = np.load(vec_path)
        vecs = data["vecs"].astype(np.float32)
    except Exception:
        return None

    if len(chunks) != vecs.shape[0]:
        return None

    return {"chunks": chunks, "vecs": vecs}


def save_index(slug: str, year: str, chunks: List[Dict[str, Any]], vecs: np.ndarray) -> None:
    meta_path, vec_path = index_paths(slug, year)

    with meta_path.open("w", encoding="utf-8") as f:
        for ch in chunks:
            f.write(json.dumps(ch, ensure_ascii=False) + "\n")

    np.savez_compressed(vec_path, vecs=vecs.astype(np.float32))


def build_or_load_doc_index(
    client: AzureOpenAI,
    doc: FocusedDoc,
    rebuild: bool,
    target_chunk_tokens: int,
    max_chunk_tokens: int,
) -> Dict[str, Any]:
    """
    Costruisce (o carica) l'indice per UN documento (slug+year):
    - chunks
    - tokenizzazione BM25 per chunk
    - embeddings per chunk (text-embedding-3-large)
    """
    year = str(doc.report_year)
    cached = None if rebuild else load_index(doc.slug, year)
    if cached is not None:
        return cached

    md_text = load_markdown(doc.focused_md)
    pages = split_into_pages(md_text)
    chunks = build_retrieval_chunks_from_pages(
        pages=pages,
        target_chunk_tokens=target_chunk_tokens,
        max_chunk_tokens=max_chunk_tokens,
    )

    # metadata per chunk
    for idx, ch in enumerate(chunks):
        ch["chunk_id"] = idx + 1
        ch["text_hash"] = _hash_text(ch["text"])

    # embeddings
    texts = [ch["text"] for ch in chunks]
    vecs = embed_texts(client, texts=texts, batch_size=64, max_retries=3)
    vecs = l2_normalize(vecs)

    # salva
    save_index(doc.slug, year, chunks=chunks, vecs=vecs)

    return {"chunks": chunks, "vecs": vecs}


# =========================
# Query set (multi-query)
# =========================
def build_queries() -> List[str]:
    base = [
        "EU taxonomy eligible activities table code capex opex",
        "EU taxonomy aligned activities environmentally sustainable",
        "taxonomy section economic activities activity code CCM CCA CE WTR BIO",
        "NACE activity list taxonomy section C17.1.2 A1.6.2",
        "taxonomy-eligible activities table",
        "taxonomy-aligned activities table",
        "economic activity code 6.10 7.7 CCM 3.3 CCA 4.1",
        "sustainability report taxonomy section activities",
        # tue intestazioni
        "A. TAXONOMY-ELIGIBLE ACTIVITIES",
        "A.1. Environmentally sustainable activities (Taxonomy-aligned)",
        "A.2. Taxonomy-eligible but not environmentally sustainable activities (not Taxonomy-aligned activities)",
    ]
    # Varianti “short” per recall
    variants = [
        "taxonomy eligible activities",
        "taxonomy aligned activities",
        "environmentally sustainable activities taxonomy aligned",
        "taxonomy eligible not aligned activities",
        "CapEx OpEx turnover taxonomy eligible aligned",
        "CCM CCA CE taxonomy activity code",
    ]
    return base + variants


# =========================
# RETRIEVAL (ibrido)
# =========================
def retrieve_topk_chunks_hybrid(
    client: AzureOpenAI,
    index: Dict[str, Any],
    queries: List[str],
    top_k: int,
    w_bm25: float,
    w_emb: float,
) -> List[Dict[str, Any]]:
    chunks: List[Dict[str, Any]] = index["chunks"]
    vecs: np.ndarray = index["vecs"]  # (N, D) normalized
    N = len(chunks)
    if N == 0:
        return []

    # BM25 setup (per doc)
    tokenized_docs = [bm25_tokenize(ch["text"]) for ch in chunks]
    bm25_state = build_bm25_index(tokenized_docs)

    # BM25 multi-query: prendiamo il MAX score su tutte le query (alta recall)
    bm25_all = np.zeros((N,), dtype=np.float32)
    for q in queries:
        qtok = bm25_tokenize(q)
        s = bm25_scores(qtok, tokenized_docs, bm25_state)
        if s.size:
            bm25_all = np.maximum(bm25_all, s.astype(np.float32))

    # Embeddings multi-query: calcola embedding delle query, poi MAX cosine su queries
    q_vecs = embed_texts(client, texts=queries, batch_size=32, max_retries=3)
    q_vecs = l2_normalize(q_vecs)
    sims = cosine_sim_matrix(q_vecs, vecs)  # (Q, N)
    emb_all = sims.max(axis=0).astype(np.float32) if sims.size else np.zeros((N,), dtype=np.float32)

    # Merge ibrido
    hybrid = merge_scores_hybrid(bm25_all, emb_all, w_bm25=w_bm25, w_emb=w_emb)

    # top-k
    k = min(top_k, N)
    idxs = np.argsort(-hybrid)[:k]

    out: List[Dict[str, Any]] = []
    for rank, i in enumerate(idxs, start=1):
        ch = dict(chunks[int(i)])  # copia
        ch["_retrieval"] = {
            "rank": rank,
            "score_hybrid": float(hybrid[int(i)]),
            "score_bm25_raw": float(bm25_all[int(i)]),
            "score_emb_raw": float(emb_all[int(i)]),
        }
        out.append(ch)

    return out

# =========================
# LLM extraction (IDENTICO STILE RAG1, ma su retrieved chunk)
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
- Sub_activity_code: the taxonomy code exactly as written (e.g., "CCM 3.3", "CCA 4.1", "6.10").
  Accept also codes like C17.1.2 / A1.6.2 when they appear as part of activity lists in the taxonomy section.
  If no code is shown for that row, use null.

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


def call_llm_extract(client: AzureOpenAI, developer: str, user: str, max_retries: int = 3) -> Dict[str, Any]:
    last_err: Optional[Exception] = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = client.chat.completions.create(
                model=CHAT_DEPLOYMENT,
                messages=[{"role": "developer", "content": developer}, {"role": "user", "content": user}],
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
            return data
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


# =========================
# Main run (Hybrid RAG)
# =========================
def run(
    only_first: bool = False,
    slug_filter: Optional[str] = None,
    year_filter: Optional[str] = None,
    rebuild_index: bool = False,
    top_k: int = 8,
    w_bm25: float = 0.55,
    w_emb: float = 0.45,
    retrieval_chunk_tokens: int = 1200,
    retrieval_chunk_max_tokens: int = 1600,
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

    if only_first:
        docs = docs[:1]

    client = get_client()
    all_flat_rows: List[Dict[str, Any]] = _rows_from_existing_jsonl(OUT_JSONL) if not no_append_jsonl else []

    queries = build_queries()

    mode = "w" if no_append_jsonl else "a"
    with OUT_JSONL.open(mode, encoding="utf-8") as w:
        print(f"[OUT] JSONL ({'overwrite' if no_append_jsonl else 'append'}) -> {OUT_JSONL}")
        print(f"[IDX] INDEX_DIR -> {INDEX_DIR}")
        print(f"[RET] top_k={top_k} | w_bm25={w_bm25} w_emb={w_emb} | emb_model(depl)={EMBEDDINGS_DEPLOYMENT}")

        for i, doc in enumerate(docs, start=1):
            year = str(doc.report_year)

            # nomenclatura azienda identica
            company_name_full = resolve_company_name_for_benchmark(MARKER_ARTIFACTS_DIR, doc.slug, year)
            company_name = company_name_full

            print(f"[{i}/{len(docs)}] {company_name} ({doc.slug}) {year} -> {doc.focused_md}")

            # 1) build/load index per doc-year
            try:
                index = build_or_load_doc_index(
                    client=client,
                    doc=doc,
                    rebuild=rebuild_index,
                    target_chunk_tokens=retrieval_chunk_tokens,
                    max_chunk_tokens=retrieval_chunk_max_tokens,
                )
            except Exception as e:
                print(f"[SKIP] index error: {company_name} {year} -> {e}")
                continue

            # 2) retrieval top-k
            try:
                top_chunks = retrieve_topk_chunks_hybrid(
                    client=client,
                    index=index,
                    queries=queries,
                    top_k=top_k,
                    w_bm25=w_bm25,
                    w_emb=w_emb,
                )
            except Exception as e:
                print(f"[SKIP] retrieval error: {company_name} {year} -> {e}")
                continue

            if not top_chunks:
                print(f"[SKIP] nessun chunk recuperato per {company_name} {year}")
                continue

            print(f"   -> retrieved chunks: {len(top_chunks)} (top_k={top_k})")

            # 3) estrazione LLM SOLO su retrieved chunks
            # chunk_index nel JSON = retrieval rank (1..k)
            for rank, ch in enumerate(top_chunks, start=1):
                chunk_text = ch["text"]

                overhead_tokens = 4000
                max_input = max(1000, safe_input_budget - overhead_tokens)

                if count_tokens(chunk_text) > max_input:
                    if ENCODER is not None:
                        toks = ENCODER.encode(chunk_text)[:max_input]
                        chunk_text = ENCODER.decode(toks)
                    else:
                        chunk_text = chunk_text[: max_input * 4]

                developer, user = build_prompt(company_name, year, chunk_text, rank, len(top_chunks))

                try:
                    result = call_llm_extract(client, developer, user, max_retries=3)
                except Exception as e:
                    print(f"[SKIP] LLM error: {company_name} {year} retrieval_rank={rank}/{len(top_chunks)} -> {e}")
                    continue

                result["_meta"] = {
                    "slug": doc.slug,
                    "company_name_full": company_name_full,
                    "year": year,
                    "source_md": str(doc.focused_md),
                    # retrieval info
                    "retrieval_rank": rank,
                    "retrieval_top_k": len(top_chunks),
                    "retrieval_scores": ch.get("_retrieval", {}),
                    # chunk provenance
                    "chunk_id": ch.get("chunk_id"),
                    "text_hash": ch.get("text_hash"),
                    "start_page": ch.get("start_page"),
                    "end_page": ch.get("end_page"),
                    "chunk_tokens_est": ch.get("tokens"),
                    # query set (debug)
                    "queries_used_preview": queries[:8],
                    "n_queries": len(queries),
                    # model info
                    "embeddings_deployment": EMBEDDINGS_DEPLOYMENT,
                    "chat_deployment": CHAT_DEPLOYMENT,
                }

                # forza chunk_index coerente (rank)
                result["company"] = company_name
                result["report_year"] = year
                result["chunk_index"] = rank

                w.write(json.dumps(result, ensure_ascii=False) + "\n")
                all_flat_rows.extend(normalize_rows(result))
                time.sleep(SLEEP_BETWEEN_CALLS_S)

    # =========================
    # CSV finale
    # =========================
    base_cols = ["company", "report_year", "chunk_index", "activity", "Sub_activity_code"]

    if all_flat_rows:
        deduped = dedup_rows(all_flat_rows)
        df = pd.DataFrame(deduped)
        for c in base_cols:
            if c not in df.columns:
                df[c] = None

        df = df.drop_duplicates(subset=["company", "report_year", "activity", "Sub_activity_code"])
        df = df.sort_values(["company", "report_year", "Sub_activity_code", "activity"], na_position="last")

        # quoting per sicurezza su virgole ecc.
        df.to_csv(OUT_CSV, index=False, encoding="utf-8", quoting=csv.QUOTE_MINIMAL)
        print(f"[OUT] CSV  -> {OUT_CSV} ({len(df)} righe, deduped)")
    else:
        df = pd.DataFrame(columns=base_cols)
        df.to_csv(OUT_CSV, index=False, encoding="utf-8", quoting=csv.QUOTE_MINIMAL)
        print(f"[WARN] Nessuna riga estratta. CSV vuoto scritto -> {OUT_CSV}")


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only_first", action="store_true", help="Processa solo il primo documento (debug).")
    ap.add_argument("--slug", type=str, default=None)
    ap.add_argument("--year", type=str, default=None)

    ap.add_argument("--rebuild_index", action="store_true", help="Ricostruisce l'indice (chunk+embeddings) anche se già presente.")
    ap.add_argument("--top_k", type=int, default=8)
    ap.add_argument("--w_bm25", type=float, default=0.55)
    ap.add_argument("--w_emb", type=float, default=0.45)

    ap.add_argument("--retrieval_chunk_tokens", type=int, default=1200)
    ap.add_argument("--retrieval_chunk_max_tokens", type=int, default=1600)

    ap.add_argument("--safe_input_budget", type=int, default=90000)
    ap.add_argument("--no_append_jsonl", action="store_true")

    return ap.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(
        only_first=args.only_first,
        slug_filter=args.slug,
        year_filter=args.year,
        rebuild_index=args.rebuild_index,
        top_k=args.top_k,
        w_bm25=args.w_bm25,
        w_emb=args.w_emb,
        retrieval_chunk_tokens=args.retrieval_chunk_tokens,
        retrieval_chunk_max_tokens=args.retrieval_chunk_max_tokens,
        safe_input_budget=args.safe_input_budget,
        no_append_jsonl=args.no_append_jsonl,
    )

# Esempi:
# python 3_rag3_hybrid_retrieval_extract_activities.py --only_first --rebuild_index --no_append_jsonl
# python 3_rag3_hybrid_retrieval_extract_activities.py --slug a_p_moller_maersk_a_s --year 2024 --rebuild_index --no_append_jsonl
# python c:/Universita/TESI/esg_agent/RAG_full_bigger/3_rag3_hybrid_retrieval_extract_activities.py --top_k 12 --w_bm25 0.5 --w_emb 0.5 --no_append_jsonl
# secondo test con --top_k 8 --w_bm25 0.65 --w_emb 0.35
'''
Se aumenti w_bm25 (e abbassi w_emb)
Cosa succede:
Il retrieval favorisce chunk che contengono esattamente i termini della query:
“taxonomy-eligible”, “aligned”, “CapEx”, “OpEx”, “CCM”, “A.1.”, “A.2.”, ecc.
La selezione diventa più “keyword-driven”.

Effetti tipici:
Precision ↑ (meno rumore: più probabile che i chunk siano davvero la sezione/tabella giusta)
Recall ↓ in alcuni casi (se la tabella usa sinonimi o formati non testuali standard)

Quando conviene:
I tuoi focused MD hanno tabelle con header e parole “standard” (Taxonomy, eligible/aligned, CapEx/OpEx).
I documenti usano terminologia stabile e “copiata” dalla normativa.
Vuoi ridurre il numero di chunk irrilevanti che arrivano all LLM.

Sintomo che BM25 è troppo alto:
Vedi che recuperi sempre gli stessi chunk con tante keyword, ma ti perdi attività che in benchmark
ci sono (perché magari stanno in una tabella senza parole “forti”).

Se aumenti w_emb (e abbassi w_bm25)
Cosa succede:
Il retrieval favorisce chunk che sono semanticamente simili alle query, anche se:
non contengono esattamente le keyword
usano frasi diverse (“environmentally sustainable” vs “aligned”, ecc.)

Effetti tipici:
Recall ↑ (più probabile beccare contenuti riformulati, tabelle “strane”, liste non standard)
Precision ↓ (più “rumore”: può tirare dentro pezzi discorsivi sulla taxonomy ma non righe attività)

Quando conviene:
I report sono molto vari nello stile e spesso non ripetono le keyword “canoniche”.
Le attività compaiono in liste/testo e non sempre in tabelle con “Code” esplicito.
Stai perdendo chunk buoni con BM25.

Sintomo che embeddings è troppo alto:
Recuperi chunk “giusti come tema” (taxonomy), ma non contengono righe tabellari/attività.
LLM estrae poco o nulla dai top-k perché il contesto è narrativo.

Interazione con top_k (molto importante):
I pesi decidono quali chunk entrano nei top-k.
Se alzi w_emb (più recall ma più rumore), spesso devi anche alzare top_k per non perdere i chunk tabellari.
Se alzi w_bm25 (più preciso), puoi spesso tenere top_k più basso.

Esempio pratico:
w_bm25=0.70, w_emb=0.30, top_k=8 → molto “chirurgico”
w_bm25=0.45, w_emb=0.55, top_k=12/16 → più “a rete larga”

top_k = 6 o 8

w_bm25 = 0.65

w_emb = 0.35
'''