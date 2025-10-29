# -*- coding: utf-8 -*-
# FILE: analysis_rag/app/rag_chat.py
"""
Chat ESG RAG con due modalità:

- Company-specific:
  * riconoscimento azienda con normalizzazione/alias (es. 'volvo' -> 'AB VOLVO')
  * PREFILTER per company_id + selezione top chunk per similarità
  * contesto = solo testo, limitato da budget (MAX_CTX_CHARS)
  * retrieval query "densa" (azienda + hint ENV) ma prompt con la domanda originale

- Cross-company:
  * similarity_search (k=8 di default)
  * raggruppa per company i chunk e costruisce CONTEXT per azienda
  * prompt che chiede: elenco aziende + 1 riga di evidenza
"""

import os
import re
import unicodedata
from pathlib import Path
from typing import List, Optional, Dict, Tuple
from collections import defaultdict, Counter

from langchain_chroma import Chroma
from langchain_openai import AzureChatOpenAI, AzureOpenAIEmbeddings

# ======== ENV ========
ENDPOINT_URL = os.getenv("ENDPOINT_URL", "https://openaimaurino2.openai.azure.com/").strip()
API_KEY      = os.getenv("AZURE_OPENAI_API_KEY", "").strip()
API_VERSION  = os.getenv("AZURE_OPENAI_API_VERSION", "2025-01-01-preview").strip()

# Passa la ROOT della cartella dei run Chroma (non una sottocartella UUID)
CHROMA_DIR_ENV  = os.getenv("CHROMA_DIR", "analysis_rag/data/benchmark/chroma")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "ESG_RAG")
K               = int(os.getenv("CHAT_RETRIEVER_K", "8"))
DEBUG_CHAT      = os.getenv("RAG_DEBUG", "0") == "1"

# Company-mode: quante evidenze recuperare e quanto lungo può essere il contesto
FETCH_K_COMPANY = int(os.getenv("FETCH_K_COMPANY", "21"))
TOP_K_COMPANY   = int(os.getenv("TOP_K_COMPANY", "21"))
MAX_CTX_CHARS   = int(os.getenv("MAX_CTX_CHARS", "10000"))

# Sinonimi di supporto (non rimuoviamo il brand dalla query!)
ENV_HINTS = [
    "environment", "ambiente", "emissions", "emissioni", "energy", "energia",
    "renewable", "rinnovabile", "efficiency", "efficienza", "water", "acqua",
    "waste", "rifiuti", "net zero", "decarbonization", "decarbonizzazione",
    "climate", "carbon", "ISO 50001", "LCA", "circular economy", "biodiversity", "pollution", "inquinamento"
]
POLLUTION_HINTS = [
    "inquinamento", "pollution", "controllo emissioni", "emission control",
    "NOx", "particolato", "PM", "SOx", "air quality", "emission standards", "abbattimento emissioni"
]

# ======== Utils ========
def _resolve_chroma_dir(base: str | Path) -> str:
    p = Path(base)
    if (p / "chroma.sqlite3").exists():
        return str(p.resolve())
    candidates = []
    if p.exists() and p.is_dir():
        for child in p.iterdir():
            if child.is_dir() and (child / "chroma.sqlite3").exists():
                candidates.append(child)
    if not candidates:
        raise FileNotFoundError(
            f"Nessun database Chroma trovato in '{base}'. "
            "Ricostruisci l'indice o imposta CHROMA_DIR alla cartella RUN che contiene 'chroma.sqlite3'."
        )
    chosen = max(candidates, key=lambda d: d.stat().st_mtime)
    return str(chosen.resolve())

def _norm(s: Optional[str]) -> str:
    return (s or "").strip().lower()

def _make_context_text_only(docs: List) -> str:
    return "\n---\n".join([d.page_content for d in docs])

def _densify_query(q: str, extra_terms: List[str]) -> str:
    return q.strip() + " ; " + " ; ".join(extra_terms)

def _group_docs_by_company(docs: List) -> Dict[str, Dict]:
    agg = defaultdict(lambda: {"snippets": [], "citations": []})
    for d in docs:
        md = d.metadata or {}
        comp = (md.get("company_id") or "?").strip()
        text = d.page_content.strip().replace("\n", " ")
        if len(text) > 350:
            text = text[:350] + "…"
        agg[comp]["snippets"].append(text)
        agg[comp]["citations"].append(f"{md.get('doc_id')} p.{md.get('page')}")
    return agg

def _select_by_budget(chunks: List[str], max_chars: int) -> str:
    out, used = [], 0
    for c in chunks:
        c = c.strip()
        if not c:
            continue
        if used + len(c) + 5 > max_chars:
            break
        out.append(c)
        used += len(c) + 5
    return "\n---\n".join(out)

def _company_dense_query(company: str) -> str:
    base = f"{company} environment emissions energy renewable efficiency water waste biodiversity circular LCA pollution inquinamento ISO 14001 ISO 50001 climate"
    return _densify_query(base, ENV_HINTS[:10])

# ======== Normalizzazione/alias per nomi azienda ========
LEGAL_SUFFIXES = {
    "spa","s.p.a.","s p a","plc","ag","nv","se","sa","srl","s.r.l.","s r l",
    "ab","asa","oyj","oy","as","bv","bvba","kgaa","kg","gmbh","llc","ltd","limited",
    "inc","corp","corporation","company","co","group","holding","holdings","società","societa",
    "nv/sa","spa","s.p.a","p.l.c","p l c","s a","n v"
}
STOPWORDS = {
    "the","and","&","of","di","la","le","il","spa","plc","group","holding","holdings","company","co","corp",
    "inc","ltd","ag","nv","se","sa","srl","gmbh","oyj","oy","as","ab"
}

def _strip_accents(text: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", text) if unicodedata.category(c) != "Mn")

def _normalize_company_name(s: str) -> str:
    if not s:
        return ""
    s = _strip_accents(s.lower())
    s = re.sub(r"[^a-z0-9]+", " ", s)
    tokens = [t for t in s.split() if t]
    tokens = [t for t in tokens if t not in LEGAL_SUFFIXES and t not in STOPWORDS]
    return " ".join(tokens).strip()

def _build_alias_index(companies: List[str]) -> Tuple[Dict[str, str], Dict[str, List[str]]]:
    norm2canon: Dict[str, str] = {}
    token2companies: Dict[str, List[str]] = defaultdict(list)
    for c in companies:
        canon = c
        norm = _normalize_company_name(canon)
        if norm:
            norm2canon[norm] = canon
            for tok in norm.split():
                if len(tok) >= 2 and tok not in STOPWORDS:
                    token2companies[tok].append(canon)
        else:
            raw = re.sub(r"[^a-z0-9]+", " ", c.lower()).strip()
            if raw:
                token2companies[raw].append(canon)
    return norm2canon, token2companies

def _guess_company_from_text(user_q: str,
                             norm2canon: Dict[str, str],
                             token2companies: Dict[str, List[str]]) -> Optional[str]:
    q_norm = _normalize_company_name(user_q)
    if not q_norm:
        return None
    if q_norm in norm2canon:
        return norm2canon[q_norm]
    for norm_name, canon in norm2canon.items():
        if norm_name and norm_name in q_norm:
            return canon
    votes = Counter()
    for tok in set(q_norm.split()):
        if len(tok) < 2:
            continue
        for cand in token2companies.get(tok, []):
            votes[cand] += 1
    if not votes:
        return None
    best, best_score = votes.most_common(1)[0]
    if len(votes) > 1:
        second = votes.most_common(2)[1][1]
        if best_score == second:
            return None
    return best

# ======== Azure: Embeddings + LLM ========
emb = AzureOpenAIEmbeddings(
    azure_deployment=os.getenv("AZURE_DEPLOYMENT_EMBEDDINGS", "text-embedding-3-large"),
    azure_endpoint=ENDPOINT_URL,
    api_key=API_KEY,
    openai_api_version=API_VERSION,
)
llm = AzureChatOpenAI(
    azure_deployment=os.getenv("AZURE_DEPLOYMENT_COMPLETIONS", "gpt-5-mini"),
    azure_endpoint=ENDPOINT_URL,
    api_key=API_KEY,
    api_version=API_VERSION,
    temperature=1,
    max_tokens=900,
)

# ======== Vector store ========
CHROMA_DIR = _resolve_chroma_dir(CHROMA_DIR_ENV)
vs = Chroma(collection_name=COLLECTION_NAME, embedding_function=emb, persist_directory=CHROMA_DIR)

# Elenco company_id disponibili
meta = vs._collection.get(limit=100000, include=["metadatas"])
COMPANIES: List[str] = sorted({
    (md or {}).get("company_id")
    for md in meta.get("metadatas", [])
    if (md or {}).get("company_id")
})

# Placeholder (veri valori calcolati nel main)
NORM2CANON: Dict[str, str] = {}
TOKEN2COMP: Dict[str, List[str]] = {}

def guess_company(q: str) -> Optional[str]:
    comp = _guess_company_from_text(q, NORM2CANON, TOKEN2COMP)
    if DEBUG_CHAT:
        print(f"[DEBUG] guess_company: {comp}")
    return comp

# ======== Core ========
def ask(q: str) -> str:
    company = guess_company(q)

    # ========== MODE 1: Company-specific con PREFILTER + selezione ==========
    if company:
        dense_q = _company_dense_query(company)
        try:
            docs_with_scores = vs.similarity_search_with_score(
                dense_q, k=FETCH_K_COMPANY, filter={"company_id": company}
            )
        except TypeError:
            try:
                docs_with_scores = vs.similarity_search_with_score(
                    dense_q, FETCH_K_COMPANY, {"company_id": company}
                )
            except Exception:
                docs_with_scores = []

        if not docs_with_scores:
            return f"(Filtro: {company}) Nessun contesto trovato nei documenti indicizzati."

        try:
            docs_with_scores.sort(key=lambda t: t[1])  # distanza: più basso = migliore
        except Exception:
            pass
        top_docs = [d for d, _ in docs_with_scores[:TOP_K_COMPANY]]
        context = _select_by_budget([d.page_content for d in top_docs], MAX_CTX_CHARS)

        if DEBUG_CHAT:
            print(f"[DEBUG] company={company} | fetched={len(docs_with_scores)} | used={len(top_docs)} | ctx_chars={len(context)}")

        system = "Sei un esperto analista ESG. Rispondi brevemente, senza essere ripetitivo, con solo ciò che trovi nel contesto. Se ci sono più punti, elencali."
        user = f"Context:\n{context}\n\nDomanda: {q}"

        res = llm.invoke([
            {"role": "system", "content": system},
            {"role": "user", "content": user}
        ])
        content = (res.content or "").strip()
        return f"(Filtro: {company}) " + (content if content else "Nessuna attività trovata nel contesto.")

    # ========== MODE 2: Cross-company con evidenze ==========
    extra_terms = POLLUTION_HINTS if any(t in _norm(q) for t in ["inquin", "pollut", "emission"]) else ENV_HINTS[:8]
    q_dense = _densify_query(q, extra_terms)
    docs = vs.similarity_search(q_dense, k=max(8, K))
    if not docs:
        return "Nessun contesto trovato."

    grouped = _group_docs_by_company(docs)
    top = sorted(grouped.items(), key=lambda kv: len(kv[1]["snippets"]), reverse=True)[:6]

    per_company_blocks = []
    for comp, data in top:
        snippets = data["snippets"][:2]
        cites    = data["citations"][:2]
        per_company_blocks.append(
            f"[{comp}]\n" +
            "\n".join(f"- {s}" for s in snippets) +
            (f"\n(cite: {', '.join(cites)})" if cites else "")
        )
    ctx = "\n\n---\n\n".join(per_company_blocks)

    if DEBUG_CHAT:
        print(f"[DEBUG] cross-company | companies_in_ctx={len(top)} | ctx_chars={len(ctx)}")

    system = (
        "Sei un esperto analista ESG. Usa SOLO il CONTEXT per rispondere. "
        "Devi elencare le aziende che nel CONTEXT risultano trattare il tema richiesto, "
        "e per ciascuna scrivi SEMPRE una riga con la spiegazione di come viene trattato il tema dall'azienda."
    )
    user = (
        f"Domanda: {q}\n\n"
        f"CONTEXT (evidenze per azienda):\n{ctx}\n\n"
        "Istruzione: rispondi con un elenco puntato '• Azienda — evidenza'. "
        "Non inventare aziende né evidenze non presenti nel CONTEXT."
    )

    res = llm.invoke([
        {"role": "system", "content": system},
        {"role": "user", "content": user}
    ])
    content = (res.content or "").strip()
    if not content:
        rough = " ; ".join([c for c, _ in top]) or "N/A"
        return f"Aziende rilevate (senza sintesi): {rough}"
    return content

# ======== Main ========
if __name__ == "__main__":
    print(f"[chat] Using CHROMA_DIR: {_resolve_chroma_dir(CHROMA_DIR_ENV)}")
    # (ri)calcola alias DOPO aver letto il DB
    meta = vs._collection.get(limit=100000, include=["metadatas"])
    COMPANIES = sorted({
        (md or {}).get("company_id")
        for md in meta.get("metadatas", [])
        if (md or {}).get("company_id")
    })
    NORM2CANON, TOKEN2COMP = _build_alias_index(COMPANIES)

    print(f"[chat] Loaded {len(COMPANIES)} companies. Digita 'exit' per uscire.")
    if not COMPANIES:
        print("[hint] Nessuna company trovata: verifica CHROMA_DIR e COLLECTION_NAME.")
    while True:
        try:
            q = input("\nESG Q> ").strip()
        except (EOFError, KeyboardInterrupt):
            break
        if not q or q.lower() in {"exit", "quit"}:
            break
        print("\n" + ask(q) + "\n")

''' come runno:
# credenziali Azure
$env:AZURE_OPENAI_API_KEY="wLPBFmPkwquNFwn5IKDR3W8mv1ZKb95FGnxLZ0RgUiEl32D9qFaGJQQJ99BHACI8hq2XJ3w3AAABACOGyD04"
$env:ENDPOINT_URL="https://openaimaurino2.openai.azure.com/"
$env:AZURE_OPENAI_API_VERSION="2025-01-01-preview"

# deployment chat ed embeddings 
$env:AZURE_DEPLOYMENT_COMPLETIONS="gpt-5-mini"
$env:AZURE_DEPLOYMENT_EMBEDDINGS="text-embedding-3-large"   # <-- SE (e solo se) esiste con quel nome su *openaimaurino2*

# parametri RAG 
$env:VECTORSTORE="chroma"
#$env:RETRIEVER_K="8"   # quanti chunk passare al modello
$env:CHROMA_DIR="analysis_rag\data\benchmark\chroma\"
$env:COLLECTION_NAME="ESG_RAG"
$env:VECTORSTORE="chroma"
$env:RAG_DEBUG="1"   # opzionale: stampa query e lunghezza contesto

python -m analysis_rag.rag.rag_chat
'''