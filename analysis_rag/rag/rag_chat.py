# -*- coding: utf-8 -*-
# FILE: analysis_rag/app/rag_chat.py
""" Chat ESG RAG con tre modalità:

- Company-specific:
  * riconoscimento azienda con normalizzazione (es. 'volvo' -> 'AB VOLVO')
  * PREFILTER per company_id + selezione top chunk per similarità
  * contesto = solo testo

- Cross-company:
  * similarity_search (k=8 di default)
  * raggruppa per company i chunk e costruisce CONTEXT per azienda
  * prompt che chiede: elenco aziende + 1 riga di evidenza

- Compare-mode:
  * attivato se la query contiene ≥2 aziende o parole chiave tipo "vs", "confronta", "differenze"
  * costruisce contesto per ciascuna azienda e chiede confronto sintetico """

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
API_KEY      = os.getenv("AZURE_OPENAI_API_KEY", "wLPBFmPkwquNFwn5IKDR3W8mv1ZKb95FGnxLZ0RgUiEl32D9qFaGJQQJ99BHACI8hq2XJ3w3AAABACOGyD04").strip()
API_VERSION  = os.getenv("AZURE_OPENAI_API_VERSION", "2025-01-01-preview").strip()

CHROMA_DIR_ENV  = os.getenv("CHROMA_DIR", "analysis_rag/data/benchmark/chroma")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "ESG_RAG")
K               = int(os.getenv("CHAT_RETRIEVER_K", "8"))
DEBUG_CHAT      = os.getenv("RAG_DEBUG", "0") == "1"

FETCH_K_COMPANY = int(os.getenv("FETCH_K_COMPANY", "60"))
TOP_K_COMPANY   = int(os.getenv("TOP_K_COMPANY", "30"))
MAX_CTX_CHARS   = int(os.getenv("MAX_CTX_CHARS", "10000"))

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
    base = f"{company} environment emissions energy renewable efficiency water waste biodiversity circular LCA pollution inquinamento ISO 14001 ISO 50001 climate net zero scope 1 scope 2 scope 3 EU Taxonomy CSRD sustainability"
    return _densify_query(base, ENV_HINTS[:15])

# ======== Normalizzazione/alias per nomi azienda ========
LEGAL_SUFFIXES = {
    "spa","s.p.a.","s p a","plc","ag","nv","se","sa","srl","s.r.l.","s r l",
    "ab","asa","oyj","oy","as","bv","bvba","kgaa","kg","gmbh","llc","ltd","limited",
    "inc","corp","corporation","company","co","group","holding","holdings","società","societa",
    "nv/sa","p.l.c","s a","n v"
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

meta = vs._collection.get(limit=100000, include=["metadatas"])
COMPANIES: List[str] = sorted({
    (md or {}).get("company_id")
    for md in meta.get("metadatas", [])
    if (md or {}).get("company_id")
})

NORM2CANON: Dict[str, str] = {}
TOKEN2COMP: Dict[str, List[str]] = {}

# ======== MULTI-COMPANY DETECTION ========
def guess_companies_multi(q: str, limit: int = 3) -> List[str]:
    """Riconosce 0, 1 o più aziende nella query (robusto, evita falsi positivi tipo 'energia')."""
    q_norm = _normalize_company_name(q)
    if not q_norm:
        return []
    q_tokens = q_norm.split()
    hits = []

    for norm, canon in NORM2CANON.items():
        if not norm:
            continue
        # match esatto o substring
        if norm in q_norm:
            hits.append(canon)
            continue
        # almeno due token in comune
        company_tokens = norm.split()
        common = len(set(company_tokens) & set(q_tokens))
        if common >= 2:
            hits.append(canon)

    # fallback token-based
    if not hits:
        votes = Counter()
        for tok in set(q_tokens):
            if len(tok) < 3:
                continue
            for cand in TOKEN2COMP.get(tok, []):
                votes[cand] += 1
        hits = [c for c, _ in votes.most_common(limit)]

    # filtra match generici
    weak_terms = {"energia", "energy", "group", "holding", "company"}
    strong_hits = []
    for h in hits:
        norm_h = _normalize_company_name(h)
        toks = set(norm_h.split())
        if len(toks & weak_terms) < len(toks):
            strong_hits.append(h)
    hits = strong_hits or hits

    # dedup
    seen, out = set(), []
    for h in hits:
        if h and h not in seen:
            seen.add(h)
            out.append(h)
    return out[:limit]

# ======== Core ========
def _compare_companies(comps: List[str], q: str) -> str:
    comps = comps[:2]
    blocks, valid = [], 0

    for c in comps:
        dense_q = _company_dense_query(c)
        try:
            docs_with_scores = vs.similarity_search_with_score(
                dense_q, k=FETCH_K_COMPANY, filter={"company_id": c}
            )
        except Exception:
            docs_with_scores = []

        if not docs_with_scores:
            try:
                docs_with_scores = vs.similarity_search_with_score(dense_q, k=FETCH_K_COMPANY)
            except Exception:
                docs_with_scores = []

        if not docs_with_scores:
            blocks.append(f"#### COMPANY: {c} ####\n(nessun contesto disponibile)")
            continue

        docs_with_scores.sort(key=lambda t: t[1])
        top_docs = [d for d, _ in docs_with_scores[:TOP_K_COMPANY]]
        context = _select_by_budget([d.page_content for d in top_docs], 5000)
        valid += 1

        print("\n" + "=" * 100)
        print(f"[CONTEXT - {c}] ({len(context)} chars)")
        print("=" * 100)
        print(context[:800] + ("..." if len(context) > 800 else ""))
        print("=" * 100 + "\n")

        blocks.append(f"#### COMPANY: {c} ####\n{context}")

    if valid < 2:
        return f"Confronto non possibile: trovato contesto per {valid} aziende.\n\n" + "\n\n".join(blocks)

    ctx_all = "\n\n\n".join(blocks)
    total_chars = len(ctx_all)
    if DEBUG_CHAT:
        print(f"[DEBUG] compare-mode | total_ctx_chars={total_chars}")

    system = (
        f"Sei un analista ESG esperto. Analizza i testi per {comps[0]} e {comps[1]} qui sotto. "
        "Ogni testo può contenere numeri, tabelle o dati tecnici. "
        "1) Per ciascuna azienda, riassumi in 2-3 frasi le principali informazioni ambientali: "
        "energia, emissioni, materiali, economia circolare, acqua, rifiuti, biodiversità, governance. "
        "2) Poi confrontale evidenziando differenze e similitudini. "
        "Usa un formato come:\n\n"
        f"**{comps[0]}:** ...\n**{comps[1]}:** ...\n\n**Confronto sintetico:** ..."
    )

    user = (
        f"Domanda: {q}\n\n"
        f"CONTEXT (estratti ESG per {comps[0]} e {comps[1]}):\n{ctx_all[:6000]}\n\n"
        "Scrivi SEMPRE un'analisi breve ma chiara formulando le informazioni del context. "
        "Se un'azienda ha solo dati numerici o quantitativi, spiegali a parole (es. intensità energetica, percentuali rinnovabili, ecc.)."
    )

    print("\n" + "=" * 120)
    print("[PROMPT FINALE INVIATO AL MODELLO]")
    print("=" * 120)
    print(f"\n[SYSTEM PROMPT]\n{system}\n")
    print("-" * 120)
    print(f"[USER PROMPT]\n{user}\n")
    print("=" * 120 + "\n")

    try:
        res = llm.invoke([
            {"role": "system", "content": system},
            {"role": "user", "content": user}
        ])
        content = (res.content or "").strip()
    except Exception as e:
        return f"[ERROR] durante confronto: {e}"

    if not content:
        print("[DEBUG] Nessuna risposta, ritento con prompt tabellare semplice...")
        fallback_prompt = (
            f"Confronta {comps[0]} e {comps[1]} su temi ESG (energia, emissioni, materiali, acqua, biodiversità, governance). "
            f"Basati SOLO su:\n{ctx_all[:3500]}\n\n"
            f"Scrivi una tabella Markdown con colonne: Tema | {comps[0]} | {comps[1]} | Differenza sintetica."
        )
        res2 = llm.invoke([
            {"role": "system", "content": "Crea una tabella di confronto ESG sintetica."},
            {"role": "user", "content": fallback_prompt}
        ])
        content = (res2.content or "").strip()

    if not content:
        print("[DEBUG] Nessuna risposta, ritento con confronto libero...")
        simple_prompt = (
            f"Confronta in modo sintetico le politiche ambientali di {comps[0]} e {comps[1]} "
            f"basandoti solo su questi estratti:\n{ctx_all[:3000]}"
        )
        res3 = llm.invoke([
            {"role": "system", "content": "Confronta in modo chiaro e sintetico due aziende."},
            {"role": "user", "content": simple_prompt}
        ])
        content = (res3.content or "").strip()

    if not content:
        return "❌ Nessuna risposta prodotta dal modello."

    return content

def guess_company(q: str) -> Optional[str]:
    comp = _guess_company_from_text(q, NORM2CANON, TOKEN2COMP)
    if DEBUG_CHAT:
        print(f"[DEBUG] guess_company: {comp}")
    return comp

def ask(q: str) -> str:
    """Routing automatico tra company, cross e compare."""
    companies = guess_companies_multi(q, limit=3)
    need_compare = any(w in _norm(q) for w in [" vs ", "confront", "differenz", "paragona", "compare"])

    if DEBUG_CHAT:
        print(f"[DEBUG] companies_detected={companies} | need_compare={need_compare}")

    # MODE 3: Compare-mode
    if need_compare or len(companies) >= 2:
        if not companies:
            return "Confronto richiesto ma nessuna azienda riconosciuta."
        return _compare_companies(companies, q)

    # MODE 1: Company-specific
    company = companies[0] if len(companies) == 1 else guess_company(q)
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
            docs_with_scores.sort(key=lambda t: t[1])
        except Exception:
            pass
        top_docs = [d for d, _ in docs_with_scores[:TOP_K_COMPANY]]
        context = _select_by_budget([d.page_content for d in top_docs], MAX_CTX_CHARS)
        if DEBUG_CHAT:
            print(f"[DEBUG] company={company} | fetched={len(docs_with_scores)} | used={len(top_docs)} | ctx_chars={len(context)}")
        system = "Sei un esperto analista ESG. Rispondi brevemente e solo con informazioni dal contesto."
        user = f"Context:\n{context}\n\nDomanda: {q}"
        res = llm.invoke([{"role": "system", "content": system}, {"role": "user", "content": user}])
        content = (res.content or "").strip()
        return f"(Filtro: {company}) " + (content if content else "Nessuna attività trovata nel contesto.")

    # MODE 2: Cross-company
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
        cites = data["citations"][:2]
        per_company_blocks.append(
            f"[{comp}]\n" + "\n".join(f"- {s}" for s in snippets) +
            (f"\n(cite: {', '.join(cites)})" if cites else "")
        )
    ctx = "\n\n---\n\n".join(per_company_blocks)
    if DEBUG_CHAT:
        print(f"[DEBUG] cross-company | companies_in_ctx={len(top)} | ctx_chars={len(ctx)}")
    system = (
        "Sei un esperto ESG. Usa SOLO il CONTEXT. "
        "Elenca le aziende che nel CONTEXT affrontano il tema richiesto, "
        "ognuna con una breve evidenza."
    )
    user = (
        f"Domanda: {q}\n\nCONTEXT:\n{ctx}\n\n"
        "Rispondi con '• Azienda — evidenza'."
    )
    res = llm.invoke([{"role": "system", "content": system}, {"role": "user", "content": user}])
    content = (res.content or "").strip()
    if not content:
        rough = " ; ".join([c for c, _ in top]) or "N/A"
        return f"Aziende rilevate (senza sintesi): {rough}"
    return content

# ======== Main ========
if __name__ == "__main__":
    print(f"[chat] Using CHROMA_DIR: {_resolve_chroma_dir(CHROMA_DIR_ENV)}")
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

esempio di query:

Single-company:
Quali sono le principali iniziative ambientali adottate da Volvo nel 2024?
Di cosa si occupa Equinor riguardo ad attività sostenibili?

Cross-company:
Quali aziende promuovono l'uso di energia rinnovabile nei propri stabilimenti?
Chi si occupa di ridurre gli sprechi d'acqua e migliorare l'efficienza idrica?

Confronto:
Confronta le politiche ambientali di Volvo e Galp Energia.
'''