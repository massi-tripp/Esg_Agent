# -*- coding: utf-8 -*-
# FILE: analysis_rag/rag/extract_activities.py
"""
RAG: usa Chroma come retriever + GPT-5-mini (Azure) per estrarre attività ESG per azienda × label tassonomica.
- credenziali/parametri da variabili d'ambiente (os.getenv)
- embeddings Azure via deployment name confermato: 'text-embedding-3-large'
"""

import os
import json
from pathlib import Path
from typing import Dict, List, Tuple, Any

import yaml
from tqdm import tqdm
from langchain_chroma import Chroma
from langchain_openai import AzureOpenAIEmbeddings, AzureChatOpenAI

# ======== ENV ========
ENDPOINT_URL   = os.getenv("ENDPOINT_URL", "https://openaimaurino2.openai.azure.com/")
API_KEY        = os.getenv("AZURE_OPENAI_API_KEY", "wLPBFmPkwquNFwn5IKDR3W8mv1ZKb95FGnxLZ0RgUiEl32D9qFaGJQQJ99BHACI8hq2XJ3w3AAABACOGyD04")
API_VERSION    = os.getenv("API_VERSION", "2025-01-01-preview")

CHAT_DEPLOY    = os.getenv("DEPLOYMENT_NAME", "gpt-5-mini")
EMB_DEPLOYMENT = os.getenv("EMBEDDING_DEPLOYMENT", "text-embedding-3-large")

CHROMA_DIR      = Path(os.getenv("CHROMA_DIR", "analysis_rag/data/benchmark/chroma"))
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "ESG_RAG")
RETRIEVER_K     = int(os.getenv("RETRIEVER_K", "8"))
TAXONOMY_YAML   = Path(os.getenv("TAXONOMY_YAML", "configs/taxonomy.yaml"))
PRED_OUT        = Path(os.getenv("PRED_OUT", "analysis_rag/data/benchmark/predictions.jsonl"))

SYSTEM_PROMPT = (
    "Sei un analista ESG. Devi ESTRARRE attività ESG solo dal CONTEXTO fornito. "
    "Non inventare. Riporta un JSON compatto con i campi: "
    "`company_id`, `label`, `activities` (lista di frasi sintetiche), "
    "`pages` (lista di numeri di pagina), `evidence` (lista di oggetti {page, span}), "
    "`confidence` (0..1), `source` ({doc_id}). "
    "Se il contesto non contiene informazioni, restituisci attività vuote."
)

USER_PROMPT_TEMPLATE = (
    "Azienda: {company_id}\n"
    "Label tassonomia: {label}\n\n"
    "Contesto (top {k} chunk):\n{context}\n\n"
    "Istruzioni:\n"
    "- Usa SOLO il contesto sopra.\n"
    "- Cita evidenze brevi (2-3 frasi max) con la pagina.\n"
    "- Se non trovi nulla, lascia activities=[] e evidence=[].\n"
    "Rispondi SOLO con JSON valido."
)

def _require(var_name: str, value: str):
    if not value:
        raise RuntimeError(f"Variabile d'ambiente mancante: {var_name}. Impostala e riprova.")
    return value

def _build_context(docs: List[Tuple[str, Dict]]) -> str:
    parts = []
    for d, md in docs:
        page = md.get("page")
        docid = md.get("doc_id")
        parts.append(f"[page={page or '?'}, doc={docid or '?'}] {d}")
    return "\n---\n".join(parts)

def _parse_json_safe(s: str) -> Dict[str, Any]:
    s = s.strip()
    try:
        import json as _json
        return _json.loads(s)
    except Exception:
        import re, json as _json
        m = re.search(r"\{.*\}$", s, re.S)
        if m:
            return _json.loads(m.group(0))
        raise

def _load_taxonomy_labels(taxonomy_yaml: Path) -> List[str]:
    with open(taxonomy_yaml, "r", encoding="utf-8") as f:
        tax = yaml.safe_load(f)
    labels = []
    for macro, nodes in tax.items():
        if isinstance(nodes, dict):
            for sub in nodes.keys():
                labels.append(f"{macro}.{sub}")
        else:
            labels.append(str(macro))
    return sorted(labels)

def _make_embeddings():
    _require("AZURE_OPENAI_API_KEY", API_KEY)
    print(f"[emb] Azure via azure_deployment='{EMB_DEPLOYMENT}'")
    return AzureOpenAIEmbeddings(
        azure_deployment=EMB_DEPLOYMENT,
        azure_endpoint=ENDPOINT_URL,
        api_key=API_KEY,
        openai_api_version=API_VERSION,
    )

def main():
    _require("AZURE_OPENAI_API_KEY", API_KEY)
    print(f"[DEBUG] endpoint_url = {ENDPOINT_URL}")
    print(f"[DEBUG] api_version  = {API_VERSION}")
    print(f"[DEBUG] chat_deploy  = {CHAT_DEPLOY}")
    print(f"[DEBUG] emb_deploy   = {EMB_DEPLOYMENT}")

    # Vectorstore + retriever
    embeddings = _make_embeddings()
    vs = Chroma(
        collection_name=COLLECTION_NAME,
        embedding_function=embeddings,
        persist_directory=str(CHROMA_DIR),
    )
    retriever = vs.as_retriever(search_kwargs={"k": RETRIEVER_K})

    # LLM Azure (chat)
    llm = AzureChatOpenAI(
        model=CHAT_DEPLOY,                          # deployment chat
        azure_endpoint=ENDPOINT_URL,
        api_key=API_KEY,
        api_version=API_VERSION,
        temperature=0.0,
        max_tokens=1200,
    )

    labels = _load_taxonomy_labels(TAXONOMY_YAML)

    # Estrai lista aziende dal DB (fetch metadati)
    results = vs._collection.get(limit=100000, include=["metadatas"])
    companies = sorted({md.get("company_id") for md in results["metadatas"] if md.get("company_id")})

    if not companies:
        raise RuntimeError("Nessuna company_id trovata nell'indice Chroma.")

    print(f"[extract_activities] Aziende: {len(companies)} | Labels: {len(labels)} | k={RETRIEVER_K}")

    PRED_OUT.parent.mkdir(parents=True, exist_ok=True)
    with PRED_OUT.open("w", encoding="utf-8") as fout:
        for company_id in tqdm(companies):
            for label in labels:
                query = f"{company_id}: attività {label} (ESG) con evidenza e pagina"
                docs = retriever.get_relevant_documents(query)
                if not docs:
                    rec = {
                        "company_id": company_id,
                        "label": label,
                        "activities": [],
                        "pages": [],
                        "evidence": [],
                        "confidence": 0.0,
                        "source": {"doc_id": None},
                    }
                    fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    continue

                context = _build_context([(d.page_content, d.metadata) for d in docs])
                messages = [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": USER_PROMPT_TEMPLATE.format(company_id=company_id, label=label, k=RETRIEVER_K, context=context)},
                ]
                resp = llm.invoke(messages)
                text = resp.content

                try:
                    data = _parse_json_safe(text)
                except Exception:
                    data = {
                        "company_id": company_id,
                        "label": label,
                        "activities": [],
                        "pages": [],
                        "evidence": [],
                        "confidence": 0.0,
                        "source": {"doc_id": None},
                        "_raw": text[:2000],
                    }

                if "company_id" not in data or not data["company_id"]:
                    data["company_id"] = company_id
                if "label" not in data or not data["label"]:
                    data["label"] = label
                if "source" not in data or not isinstance(data["source"], dict):
                    data["source"] = {}
                if data["source"].get("doc_id") is None and docs:
                    data["source"]["doc_id"] = docs[0].metadata.get("doc_id")

                data["pages"] = data.get("pages") or []
                data["evidence"] = data.get("evidence") or []
                data["activities"] = data.get("activities") or []
                data["confidence"] = float(data.get("confidence", 0.0))

                fout.write(json.dumps(data, ensure_ascii=False) + "\n")

    print(f"[extract_activities] Output → {PRED_OUT}")

if __name__ == "__main__":
    main()
