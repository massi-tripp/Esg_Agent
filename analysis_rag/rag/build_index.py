# -*- coding: utf-8 -*-
# FILE: analysis_rag/rag/build_index.py
"""
Indicizza i .txt dei report ESG in Chroma:
- credenziali/parametri da variabili d'ambiente (os.getenv)
- embeddings Azure via *deployment name* letto da AZURE_DEPLOYMENT_EMBEDDINGS
- stessa convenzione ENV usata dal professore
"""

import os
import re
import json
from pathlib import Path
from typing import Dict, List
from tqdm import tqdm

from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_chroma import Chroma
from langchain_openai import AzureOpenAIEmbeddings

# ======== ENV (allineate al codice del prof) ========
ENDPOINT_URL   = os.getenv("ENDPOINT_URL", "https://openaimaurino2.openai.azure.com/")
API_KEY        = os.getenv("AZURE_OPENAI_API_KEY", "wLPBFmPkwquNFwn5IKDR3W8mv1ZKb95FGnxLZ0RgUiEl32D9qFaGJQQJ99BHACI8hq2XJ3w3AAABACOGyD04")
API_VERSION    = os.getenv("AZURE_OPENAI_API_VERSION", "2025-01-01-preview")  # <-- come usa il prof
# Deployment embeddings (DEVE essere il *deployment name* esatto sulla risorsa):
EMB_DEPLOYMENT = os.getenv("AZURE_DEPLOYMENT_EMBEDDINGS", "text-embedding-3-large")
# Non usato qui ma utile in debug: deployment chat come nel prof
CHAT_DEPLOY    = os.getenv("AZURE_DEPLOYMENT_COMPLETIONS", "gpt-5-mini")

# Percorsi/parametri indicizzazione
TEXT_DIR        = Path(os.getenv("TEXT_DIR", "analysis_rag/data/output/text_images"))
CHROMA_DIR      = Path(os.getenv("CHROMA_DIR", "analysis_rag/data/benchmark/chroma"))
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "ESG_RAG")
CHUNK_SIZE      = int(os.getenv("CHUNK_SIZE", "1000"))
CHUNK_OVERLAP   = int(os.getenv("CHUNK_OVERLAP", "200"))
MIN_CHARS       = int(os.getenv("MIN_CHARS", "200"))

FILENAME_RE = re.compile(r"^(?P<company>.+?)__+(?P<docid>.+?)__+.*\.txt$", re.IGNORECASE)


def _require(var_name: str, value: str):
    if not value:
        raise RuntimeError(f"Variabile d'ambiente mancante: {var_name}. Impostala e riprova.")
    return value


def _guess_metadata_from_path(p: Path) -> Dict:
    md = {"company_id": None, "doc_id": None, "page": None, "year": None, "lang": None, "section_title": None}
    m = FILENAME_RE.match(p.name)
    if m:
        md["company_id"] = m.group("company").strip()
        md["doc_id"]     = m.group("docid").strip()
    else:
        md["doc_id"]     = p.stem
    return md


def _iter_text_files(text_dir: Path) -> List[Path]:
    return sorted(text_dir.rglob("*.txt"))


def _normalize_space(s: str) -> str:
    import re as _re
    return _re.sub(r"\s+", " ", s).strip()


def _make_embeddings():
    """Embeddings Azure: usa il *deployment name* come fa il prof (AZURE_DEPLOYMENT_EMBEDDINGS)."""
    _require("AZURE_OPENAI_API_KEY", API_KEY)
    _require("AZURE_DEPLOYMENT_EMBEDDINGS", EMB_DEPLOYMENT)

    print(f"[emb] Azure via azure_deployment='{EMB_DEPLOYMENT}'")
    emb = AzureOpenAIEmbeddings(
        azure_deployment=EMB_DEPLOYMENT,      # << deployment name (NON model name generico)
        azure_endpoint=ENDPOINT_URL,
        api_key=API_KEY,
        openai_api_version=API_VERSION,
    )
    # Smoke test per intercettare subito 404/chiave/endpoint errati
    try:
        _ = emb.embed_query("hello world")
        print("[emb] Azure embeddings OK")
    except Exception as e:
        msg = (
            f"\n[ERRORE EMBEDDINGS]\n"
            f"- Endpoint: {ENDPOINT_URL}\n"
            f"- API version: {API_VERSION}\n"
            f"- Deployment embeddings: {EMB_DEPLOYMENT}\n"
            f"Eccezione: {type(e).__name__}: {e}\n\n"
            f"Controlla che *su questa risorsa* esista un deployment con nome esatto '{EMB_DEPLOYMENT}'.\n"
            f"Se il nome è diverso, esporta AZURE_DEPLOYMENT_EMBEDDINGS con il nome corretto."
        )
        raise RuntimeError(msg) from e
    return emb


def main():
    print(f"[DEBUG] endpoint_url = {ENDPOINT_URL}")
    print(f"[DEBUG] api_version  = {API_VERSION}")
    print(f"[DEBUG] emb_deploy   = {EMB_DEPLOYMENT}")
    print(f"[DEBUG] chat_deploy  = {CHAT_DEPLOY}")

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
    for p in tqdm(text_files):
        raw = _normalize_space(p.read_text(encoding="utf-8", errors="ignore"))
        if len(raw) < MIN_CHARS:
            continue
        chunks = [c for c in splitter.split_text(raw) if len(c) >= MIN_CHARS]
        md_base = _guess_metadata_from_path(p)

        docs.extend(chunks)
        metadatas.extend([md_base.copy() for _ in chunks])
        total_chunks += len(chunks)

    if total_chunks == 0:
        raise RuntimeError("Dopo il chunking non sono rimasti chunk indicizzabili (controlla MIN_CHARS e i .txt).")

    print(f"[build_index] Numero totale di chunk: {total_chunks}")
    print("[build_index] Calcolo embeddings e costruzione Chroma...")

    embeddings = _make_embeddings()
    vs = Chroma.from_texts(
        texts=docs,
        embedding=embeddings,
        metadatas=metadatas,
        collection_name=COLLECTION_NAME,
        persist_directory=str(CHROMA_DIR),
    )
    vs.persist()

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
python -m analysis_rag.rag.extract_activities
'''