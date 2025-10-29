# -*- coding: utf-8 -*-
# FILE: analysis_rag/rag/extract_activities.py
"""
RAG ESG:
- Vector store: Chroma
- Company-context filtering: per ogni azienda filtriamo i chunk via metadata.company_id
- Tassonomia allineata: usiamo gli ID dalla sezione `labels` del taxonomy.yaml + synonyms per le query
- Compatibilità LangChain nuova: usare .invoke() per il retrieval (o fallback)
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
API_KEY        = os.getenv("AZURE_OPENAI_API_KEY", "wLPBFmPkwquNFwn5IKDR3W8mv1ZKb95FGnxLZ0RgUiEl32D9qFaGJQQJ99BHACI8hq2XJ3w3AAABACOGyD04").strip()
API_VERSION    = os.getenv("AZURE_OPENAI_API_VERSION", "2025-01-01-preview").strip()

CHAT_DEPLOY    = os.getenv("AZURE_DEPLOYMENT_COMPLETIONS", "gpt-5-mini").strip()
EMB_DEPLOYMENT = os.getenv("AZURE_DEPLOYMENT_EMBEDDINGS", "text-embedding-3-large").strip()

CHROMA_DIR      = Path(os.getenv("CHROMA_DIR", "analysis_rag/data/benchmark/chroma"))
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "ESG_RAG")
RETRIEVER_K     = int(os.getenv("RETRIEVER_K", "8"))
TAXONOMY_YAML   = Path(os.getenv("TAXONOMY_YAML", "analysis_rag/configs/taxonomy.yaml"))
PRED_OUT        = Path(os.getenv("PRED_OUT", "analysis_rag/data/benchmark/predictions.jsonl"))

SYSTEM_PROMPT = (
    "Sei un analista ESG. Devi ESTRARRE attività ESG solo dal CONTEXT fornito. "
    "Non inventare. Rispondi SOLO con JSON con i campi: "
    "`company_id`, `label`, `activities` (lista di frasi sintetiche), "
    "`pages` (lista di numeri di pagina), `evidence` (lista di oggetti {page, span}), "
    "`confidence` (0..1), `source` ({doc_id}). "
    "Se il contesto non contiene informazioni, restituisci activities=[]."
)

USER_PROMPT_TEMPLATE = (
    "Azienda: {company_id}\n"
    "Label: {label}  (categoria: {category})\n"
    "Query sinsemantica: {query_hint}\n\n"
    "Context (top {k} chunk filtrati per azienda):\n{context}\n\n"
    "Istruzioni:\n"
    "- Identifica le ATTIVITÀ rilevanti per la label richiesta (usa SOLO il contesto).\n"
    "- Sintetizza 2–6 attività chiare (se presenti).\n"
    "- Cita pagine ed evidenze brevi (massimo 2-3 frasi/estratti) quando disponibili.\n"
    "- Se non trovi nulla, lascia activities=[], evidence=[], pages=[].\n"
    "Rispondi SOLO con JSON valido."
)

# ---------- UTILS ----------
def _require(var_name: str, value: str):
    if not value:
        raise RuntimeError(f"Variabile d'ambiente mancante: {var_name}. Impostala e riprova.")
    return value

def _resolve_existing_path(p: Path) -> Path:
    """Rende assoluto un path relativo e prova alternative sensate (WD e root progetto)."""
    if p.is_absolute() and p.exists():
        return p
    wd = Path.cwd() / p
    if wd.exists():
        return wd.resolve()
    repo_root = Path(__file__).resolve().parents[2]  # <repo>/
    alt = repo_root / p
    if alt.exists():
        return alt.resolve()
    if p.as_posix().endswith("configs/taxonomy.yaml"):
        alt2 = repo_root / "analysis_rag" / "configs" / "taxonomy.yaml"
        if alt2.exists():
            return alt2.resolve()
    raise FileNotFoundError(f"File non trovato: {p} (provati: {wd}, {alt})")

def _build_context(docs: List[Tuple[str, Dict]]) -> str:
    parts = []
    for d, md in docs:
        page = md.get("page")
        docid = md.get("doc_id")
        parts.append(f"[page={page if page is not None else '?'} | doc={docid or '?'}] {d}")
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

# ---------- TAXONOMY ----------
class LabelSpec(Dict[str, Any]):
    """Convenience type: {'id','category','description','synonyms',...}"""
    pass

def _load_taxonomy(taxonomy_yaml: Path) -> Dict[str, Any]:
    taxonomy_yaml = _resolve_existing_path(taxonomy_yaml)
    with open(taxonomy_yaml, "r", encoding="utf-8") as f:
        tax = yaml.safe_load(f)
    return tax

def _labels_from_taxonomy(tax: Dict[str, Any]) -> List[LabelSpec]:
    """
    Ritorna lista di label 'vere' dalla sezione `labels` del taxonomy.yaml.
    Ogni item è un dict con chiavi almeno: id, category, synonyms (se presenti), description.
    """
    out: List[LabelSpec] = []
    for item in (tax.get("labels") or []):
        if not isinstance(item, dict):
            continue
        lid = item.get("id")
        cat = item.get("category")
        if not lid or not cat:
            continue
        out.append(item)  # manteniamo tutto: id/category/description/synonyms/regex/normalize...
    return out

def _query_hint_from_label(label: LabelSpec) -> str:
    """
    Crea una query ricca per il retriever usando id, description e primi sinonimi (se presenti).
    Esempio: "renewable energy; energia rinnovabile; PPA; ISO 50001; program"
    """
    bits = []
    lid = label.get("id")
    desc = label.get("description")
    syns = label.get("synonyms") or []
    if lid:
        bits.append(str(lid).replace("_", " "))
    if desc:
        bits.append(desc)
    if syns:
        # prendi i primi 5 sinonimi per non esagerare
        bits.extend(syns[:5])
    # una leggera spinta “azione/programma”
    bits.append("program initiative activity")
    return "; ".join([b for b in bits if b])

# ---------- AZURE ----------
def _make_embeddings():
    _require("AZURE_OPENAI_API_KEY", API_KEY)
    _require("ENDPOINT_URL", ENDPOINT_URL)
    print(f"[emb] Azure via azure_deployment='{EMB_DEPLOYMENT}'")
    return AzureOpenAIEmbeddings(
        azure_deployment=EMB_DEPLOYMENT,
        azure_endpoint=ENDPOINT_URL,
        api_key=API_KEY,
        openai_api_version=API_VERSION,
    )

def _make_llm():
    _require("AZURE_OPENAI_API_KEY", API_KEY)
    _require("ENDPOINT_URL", ENDPOINT_URL)
    return AzureChatOpenAI(
        azure_deployment=CHAT_DEPLOY,
        azure_endpoint=ENDPOINT_URL,
        api_key=API_KEY,
        api_version=API_VERSION,
        temperature=1.0,
        max_tokens=1200,
    )

# ---------- MAIN ----------
def main():
    print(f"[DEBUG] endpoint_url = {ENDPOINT_URL or '(manca)'}")
    print(f"[DEBUG] api_version  = {API_VERSION}")
    print(f"[DEBUG] chat_deploy  = {CHAT_DEPLOY}")
    print(f"[DEBUG] emb_deploy   = {EMB_DEPLOYMENT}")

    embeddings = _make_embeddings()
    # Apriamo la root del DB run (non la sottocartella della collection)
    vs = Chroma(
        collection_name=COLLECTION_NAME,
        embedding_function=embeddings,
        persist_directory=str(CHROMA_DIR),
    )

    # Carichiamo tassonomia e “vere” labels
    tax = _load_taxonomy(TAXONOMY_YAML)
    labels = _labels_from_taxonomy(tax)
    if not labels:
        raise RuntimeError("Nessuna label valida trovata in taxonomy.yaml → sezione 'labels' vuota?")

    # Elenco aziende dal vector store
    results = vs._collection.get(limit=100000, include=["metadatas"])
    companies = sorted({(md or {}).get("company_id") for md in results["metadatas"] if (md or {}).get("company_id")})
    if not companies:
        raise RuntimeError("Nessuna company_id trovata nell'indice Chroma.")

    print(f"[extract_activities] Aziende: {len(companies)} | Labels: {len(labels)} | k={RETRIEVER_K}")

    # LLM
    llm = _make_llm()

    # Output
    PRED_OUT.parent.mkdir(parents=True, exist_ok=True)
    with PRED_OUT.open("w", encoding="utf-8") as fout:
        for company_id in tqdm(companies):
            # company-context filtering: usiamo SOLO i chunk della company
            company_filter = {"company_id": company_id}

            for label in labels:
                label_id  = label.get("id")
                category  = label.get("category")
                query_hint = _query_hint_from_label(label)

                # Retrieval con filtro per metadati (Chroma supporta 'filter' direttamente)
                # Per evitare incongruenze tra retriever.* preferiamo usare vectorstore.similarity_search(...)
                try:
                    docs = vs.similarity_search(
                        query=query_hint,
                        k=RETRIEVER_K,
                        filter=company_filter
                    )
                except TypeError:
                    # fallback in caso di versioni che non accettano keyword 'query'
                    docs = vs.similarity_search(query_hint, k=RETRIEVER_K, filter=company_filter)

                if not docs:
                    rec = {
                        "company_id": company_id,
                        "label": label_id,
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
                    {"role": "user", "content": USER_PROMPT_TEMPLATE.format(
                        company_id=company_id,
                        label=label_id,
                        category=category,
                        query_hint=query_hint,
                        k=RETRIEVER_K,
                        context=context,
                    )},
                ]
                resp = llm.invoke(messages)
                text = resp.content or ""

                try:
                    data = _parse_json_safe(text)
                except Exception:
                    data = {
                        "company_id": company_id,
                        "label": label_id,
                        "activities": [],
                        "pages": [],
                        "evidence": [],
                        "confidence": 0.0,
                        "source": {"doc_id": None},
                        "_raw": text[:2000],
                    }

                # Normalizzazioni/riempitivi
                if not data.get("company_id"):
                    data["company_id"] = company_id
                if not data.get("label"):
                    data["label"] = label_id
                if "source" not in data or not isinstance(data["source"], dict):
                    data["source"] = {}
                if data["source"].get("doc_id") is None and docs:
                    data["source"]["doc_id"] = docs[0].metadata.get("doc_id")

                data["pages"] = data.get("pages") or []
                data["evidence"] = data.get("evidence") or []
                data["activities"] = data.get("activities") or []
                try:
                    data["confidence"] = float(data.get("confidence", 0.0))
                except Exception:
                    data["confidence"] = 0.0

                fout.write(json.dumps(data, ensure_ascii=False) + "\n")

    print(f"[extract_activities] Output → {PRED_OUT}")

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

# parametri RAG (opzionali)
$env:VECTORSTORE="chroma"
$env:RETRIEVER_K="8"   # quanti chunk passare al modello
$env:TAXONOMY_YAML="analysis_rag/configs/taxonomy.yaml"
$env:CHROMA_DIR="analysis_rag\data\benchmark\chroma\"
$env:COLLECTION_NAME="ESG_RAG"
$env:PRED_OUT="analysis_rag/data/benchmark/predictions.jsonl"
$env:VECTORSTORE="chroma"

python -m analysis_rag.rag.extract_activities
'''