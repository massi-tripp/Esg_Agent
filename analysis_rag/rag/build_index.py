# -*- coding: utf-8 -*-
# FILE: analysis_rag/rag/build_index.py
"""
Indicizza i .txt dei report ESG in Chroma con batching e rate-limit handling:
- Env allineate al prof (AZURE_OPENAI_API_VERSION, AZURE_DEPLOYMENT_EMBEDDINGS, ...)
- Embeddings Azure via deployment name
- Batching + retry su 429 (RateLimitReached)
"""

import os
import re
import json
import time
from pathlib import Path
from typing import Dict, List
from tqdm import tqdm

from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_chroma import Chroma
from langchain_openai import AzureOpenAIEmbeddings
from openai import RateLimitError


# ======== ENV (come il prof) ========
ENDPOINT_URL   = os.getenv("ENDPOINT_URL", "https://openaimaurino2.openai.azure.com/")
API_KEY        = os.getenv("AZURE_OPENAI_API_KEY", "wLPBFmPkwquNFwn5IKDR3W8mv1ZKb95FGnxLZ0RgUiEl32D9qFaGJQQJ99BHACI8hq2XJ3w3AAABACOGyD04")
API_VERSION    = os.getenv("AZURE_OPENAI_API_VERSION", "2025-01-01-preview")
EMB_DEPLOYMENT = os.getenv("AZURE_DEPLOYMENT_EMBEDDINGS", "text-embedding-3-large")
CHAT_DEPLOY    = os.getenv("AZURE_DEPLOYMENT_COMPLETIONS", "gpt-5-mini")

# ======== Parametri indice ========
TEXT_DIR        = Path(os.getenv("TEXT_DIR", "analysis_rag/data/output/text_images"))
CHROMA_DIR      = Path(os.getenv("CHROMA_DIR", "analysis_rag/data/benchmark/chroma"))
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "ESG_RAG")
CHUNK_SIZE      = int(os.getenv("CHUNK_SIZE", "2000"))
CHUNK_OVERLAP   = int(os.getenv("CHUNK_OVERLAP", "300"))
MIN_CHARS       = int(os.getenv("MIN_CHARS", "300"))

# ======== Batching / Rate limit ========
EMBED_BATCH_SIZE = int(os.getenv("EMBED_BATCH_SIZE", "64"))     # riduci se 429 persiste (32/16)
MAX_RETRIES      = int(os.getenv("EMBED_MAX_RETRIES", "12"))    # tentativi per batch
COOLDOWN_SEC     = int(os.getenv("EMBED_COOLDOWN_SEC", "65"))   # sleep dopo 429
PAUSE_BETWEEN    = float(os.getenv("EMBED_PAUSE_BETWEEN", "0.2"))  # pausa tra batch (secondi)


def _require(var_name: str, value: str):
    if not value:
        raise RuntimeError(f"Variabile d'ambiente mancante: {var_name}. Impostala e riprova.")
    return value


def _guess_metadata_from_path(p: Path) -> Dict:
    """
    Regola concordata:
    - company_id = prefisso del filename fino al PRIMO '_' (underscore)
    - doc_id     = tutto il resto del filename (senza .txt); se non c'è '_', usa lo stem intero
    - page       = non dedotta (None)
    - fallback company_id = nome della cartella padre se il filename non contiene '_'
    Esempi:
      "AB_VOLVO_sustainability_2024_en.txt" -> company_id="AB", doc_id="VOLVO_sustainability_2024_en"
      "A2A-Report-2024.txt" (senza underscore) -> company_id=<nome cartella padre>, doc_id="A2A-Report-2024"
    """
    md = {"company_id": None, "doc_id": None, "page": None, "year": None, "lang": None, "section_title": None}

    stem = p.stem  # nome senza .txt
    if "_" in stem:
        company_part, rest = stem.split("_", 1)
        md["company_id"] = (company_part or "").strip() or None
        md["doc_id"]     = (rest or "").strip() or stem
    else:
        md["doc_id"] = stem
        # fallback: prendi la cartella padre come company_id se è informativa
        parent = (p.parent.name or "").strip()
        if parent and parent.lower() not in {"", "text_images", "output", "data"}:
            md["company_id"] = parent

    return md


def _iter_text_files(text_dir: Path) -> List[Path]:
    return sorted(text_dir.rglob("*.txt"))


def _normalize_space(s: str) -> str:
    import re as _re
    return _re.sub(r"\s+", " ", s).strip()


def _make_embeddings():
    _require("AZURE_OPENAI_API_KEY", API_KEY)
    _require("AZURE_DEPLOYMENT_EMBEDDINGS", EMB_DEPLOYMENT)
    print(f"[emb] Azure via azure_deployment='{EMB_DEPLOYMENT}'")
    emb = AzureOpenAIEmbeddings(
        azure_deployment=EMB_DEPLOYMENT,
        azure_endpoint=ENDPOINT_URL,
        api_key=API_KEY,
        openai_api_version=API_VERSION,
        max_retries=8,              # lato client
        timeout=60,
    )
    # smoke test
    _ = emb.embed_query("hello world")
    print("[emb] Azure embeddings OK")
    return emb


def _add_texts_with_retry(vs: Chroma, texts: List[str], metadatas: List[Dict]):
    """Aggiunge in Chroma con batching e gestione 429."""
    total = len(texts)
    pbar = tqdm(total=total, desc="[index] Embedding+Add")
    for start in range(0, total, EMBED_BATCH_SIZE):
        end = min(start + EMBED_BATCH_SIZE, total)
        batch_texts = texts[start:end]
        batch_metas = metadatas[start:end]

        attempt = 0
        while True:
            try:
                vs.add_texts(batch_texts, metadatas=batch_metas)
                pbar.update(len(batch_texts))
                if PAUSE_BETWEEN > 0:
                    time.sleep(PAUSE_BETWEEN)  # micro pausa tra batch
                break
            except RateLimitError as e:
                attempt += 1
                if attempt > MAX_RETRIES:
                    raise RuntimeError(f"Troppi 429 consecutivi dopo {MAX_RETRIES} tentativi") from e
                wait = COOLDOWN_SEC * attempt  # backoff lineare
                print(f"[429] Rate limit, retry {attempt}/{MAX_RETRIES} tra {wait}s...")
                time.sleep(wait)
            except Exception as e:
                # altri errori transitori → prova qualche retry corto
                attempt += 1
                if attempt > MAX_RETRIES:
                    raise
                wait = min(30, 5 * attempt)
                print(f"[warn] Errore batch ({type(e).__name__}): {e} → retry tra {wait}s")
                time.sleep(wait)
    pbar.close()


def main():
    print(f"[DEBUG] endpoint_url = {ENDPOINT_URL}")
    print(f"[DEBUG] api_version  = {API_VERSION}")
    print(f"[DEBUG] emb_deploy   = {EMB_DEPLOYMENT}")
    print(f"[DEBUG] chat_deploy  = {CHAT_DEPLOY}")
    print(f"[DEBUG] batch        = {EMBED_BATCH_SIZE}, cooldown={COOLDOWN_SEC}s, retries={MAX_RETRIES}")

    CHROMA_DIR.mkdir(parents=True, exist_ok=True)

    splitter = RecursiveCharacterTextSplitter(
        chunk_size=CHUNK_SIZE,
        chunk_overlap=CHUNK_OVERLAP,
        length_function=len,
        separators=["\n\n", "\n", ". ", " ", ""],
    )

    text_files = _iter_text_files(TEXT_DIR)
    if not text_files:
        raise FileNotFoundError(f"Nessun .txt trovato in: {TEXT_DIR}")

    print(f"[build_index] Trovati {len(text_files)} file di testo. Inizio chunking...")
    docs, metadatas = [], []
    total_chunks = 0
    files_without_company = 0

    for p in tqdm(text_files):
        raw = _normalize_space(p.read_text(encoding="utf-8", errors="ignore"))
        if len(raw) < MIN_CHARS:
            continue

        chunks = [c for c in splitter.split_text(raw) if len(c) >= MIN_CHARS]
        md_base = _guess_metadata_from_path(p)

        if not md_base.get("company_id"):
            files_without_company += 1

        # Propaga metadati a ogni chunk
        for c in chunks:
            docs.append(c)
            metadatas.append(md_base.copy())
        total_chunks += len(chunks)

    if files_without_company:
        print(f"[warn] File senza company_id inferito: {files_without_company} (controlla i nomi file/cartelle)")

    if total_chunks == 0:
        raise RuntimeError("Dopo il chunking non sono rimasti chunk indicizzabili (controlla MIN_CHARS e i .txt).")

    print(f"[build_index] Numero totale di chunk: {total_chunks}")
    print("[build_index] Calcolo embeddings e costruzione Chroma...")

    embeddings = _make_embeddings()
    vs = Chroma(
        collection_name=COLLECTION_NAME,
        embedding_function=embeddings,
        persist_directory=str(CHROMA_DIR),
    )

    # >>> qui il batching con retry
    _add_texts_with_retry(vs, docs, metadatas)
    # Persist compatibile con versioni diverse di langchain_chroma
    if hasattr(vs, "persist"):
        vs.persist()
    elif hasattr(vs, "_client") and hasattr(vs._client, "persist"):
        vs._client.persist()

    meta = {
        "total_text_files": len(text_files),
        "total_chunks": total_chunks,
        "chunk_size": CHUNK_SIZE,
        "chunk_overlap": CHUNK_OVERLAP,
        "collection_name": COLLECTION_NAME,
        "persist_directory": str(CHROMA_DIR),
        "embedding_deployment": EMB_DEPLOYMENT,
        "endpoint_url": ENDPOINT_URL,
        "api_version": API_VERSION,
        "batch_size": EMBED_BATCH_SIZE,
    }
    with open(CHROMA_DIR / "index.meta.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print("[build_index] Done.")


if __name__ == "__main__":
    _require("AZURE_OPENAI_API_KEY", API_KEY)
    main()

'''per runnare:
# credenziali Azure
$env:AZURE_OPENAI_API_KEY="wLPBFmPkwquNFwn5IKDR3W8mv1ZKb95FGnxLZ0RgUiEl32D9qFaGJQQJ99BHACI8hq2XJ3w3AAABACOGyD04"
$env:ENDPOINT_URL="https://openaimaurino2.openai.azure.com/"
$env:AZURE_OPENAI_API_VERSION="2025-01-01-preview"

# deployment chat ed embeddings (deployment *name* esatto)
$env:AZURE_DEPLOYMENT_COMPLETIONS="gpt-5-mini"
$env:AZURE_DEPLOYMENT_EMBEDDINGS="text-embedding-3-large"   # <-- SE (e solo se) esiste con quel nome su *openaimaurino2*

python -m analysis_rag.rag.build_index
'''