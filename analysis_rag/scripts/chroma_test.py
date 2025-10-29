# -*- coding: utf-8 -*-
# FILE: analysis_rag/scripts/debug_chroma_sample.py

import os
from pathlib import Path
from langchain_chroma import Chroma
from langchain_openai import AzureOpenAIEmbeddings

CHROMA_DIR      = Path(os.getenv("CHROMA_DIR", "analysis_rag/data/benchmark/chroma"))
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "ESG_RAG")
EMB_DEPLOYMENT  = os.getenv("AZURE_DEPLOYMENT_EMBEDDINGS", "text-embedding-3-large")
ENDPOINT_URL    = os.getenv("ENDPOINT_URL")
API_KEY         = os.getenv("AZURE_OPENAI_API_KEY")
API_VERSION     = os.getenv("AZURE_OPENAI_API_VERSION", "2025-01-01-preview")

print(f"[DEBUG] opening Chroma at {CHROMA_DIR}")

emb = AzureOpenAIEmbeddings(
    azure_deployment=EMB_DEPLOYMENT,
    azure_endpoint=ENDPOINT_URL,
    api_key=API_KEY,
    openai_api_version=API_VERSION,
)

vs = Chroma(collection_name=COLLECTION_NAME, embedding_function=emb, persist_directory=str(CHROMA_DIR))
res = vs._collection.get(limit=5, include=["metadatas"])
print("\n=== Sample metadatas (first 5) ===")
for i, md in enumerate(res.get("metadatas", []), 1):
    print(f"\n#{i} → {md}")
