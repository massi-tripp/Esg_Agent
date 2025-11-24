# -*- coding: utf-8 -*-
# FILE: primary_search_llm_queries.py
"""
Ricerca primaria report 2024 con Tavily, usando GPT per costruire le query.

Strategia:
- Per ogni azienda:
  1) GPT genera fino a 2 query Tavily (llm_queries).
  2) Usiamo le llm_queries per cercare sul dominio (include_domains=[domain]).
  3) Se nessun risultato utile:
     - fallback: build_site_query(company) sul dominio.
  4) Per l'open web:
     - riutilizziamo le llm_queries senza include_domains.
     - se ancora nulla: fallback build_open_query(company).
  5) Applichiamo lo stesso ranking/filtri (BAD_TOKENS, GOOD_KEYWORDS, path_bonus).
  6) GPT sceglie il miglior URL tra i candidati (ask_llm_pick_one),
     con fallback su choose_best se necessario.
"""

import os
import re
import csv
import time
import json
from typing import List, Dict, Tuple

from tqdm import tqdm
from langchain_tavily import TavilySearch
from langchain_openai import AzureChatOpenAI

# ============================================================
# CONFIG PATH
# ============================================================

INPUT_FILE = "search_agent/data/input/test_next.csv"
OUTPUT_FILE = "search_agent/data/output/sustainability_reports_next_2024.csv"

TARGET_YEAR = "2024"


# ============================================================
# QUERY DI BASE (FALLBACK CLASSICI)
# ============================================================

def build_site_query(company_name: str) -> str:
    return (
        f'{company_name} {TARGET_YEAR} sustainability report ESG '
        f'"annual report" "integrated report" "universal registration document" link or pdf'
    )


def build_open_query(company_name: str) -> str:
    return (
        f'Find {company_name} {TARGET_YEAR} sustainability report ESG, or "annual report" '
        f'or "integrated report" or "universal registration document" link or pdf'
    )


# ============================================================
# RANKING
# ============================================================

BAD_TOKENS = [
    "press", "slides", "presentation", "news", "update", "outlook",
    "framework", "ownership", "governance", "index", "highlights", "summary",
    "q1", "q2", "q3", "q4", "financial_statements", "corporate-governance",
    "governance-only", "kpi", "methodology"
]

GOOD_KEYWORDS = [
    "2024", "sustainability", "sustainable",
    "integrated", "non-financial", "esg", "sustainability-report",
    "annual", "universal", "registration", "report",
    "impact", "nonfinancial", "urd", "universal registration", "ESG Report"
]


def score_result(item: Dict, company: str) -> int:
    title = (item.get("title") or "").lower()
    url = (item.get("url") or "").lower()
    score = 0

    # anno nel titolo/url
    if TARGET_YEAR in url or TARGET_YEAR in title:
        score += 2

    # parole buone nel titolo
    if any(k.lower() in title for k in GOOD_KEYWORDS):
        score += 5

    # combinazione annual + sustainability nel titolo
    if "annual" in title and "sustainability" in title:
        score += 3

    # leggera preferenza per il brand nel dominio/path
    brand_token = re.sub(r"[^a-z]", "", (company.split()[0] if company else "").lower())
    if brand_token and brand_token in re.sub(r"[^a-z]", "", url):
        score += 1

    # penalità per token cattivi nel path
    if any(b in url for b in BAD_TOKENS):
        score -= 2

    # NB: questa logica è piuttosto restrittiva (penalizza tutto ciò che non è PDF)
    if (not url.endswith(".pdf")) or url.endswith(".html") or url.endswith(".htm"):
        score -= 3

    return score


# ============================================================
# FILTRI SU DOMINIO / PATH
# ============================================================

def filter_pdf_links(links: List[str], domain: str) -> List[str]:
    base = domain.lower().split(".")[-2]  # es: bayer.com -> bayer
    good: List[str] = []
    for url in links:
        u = url.lower()
        if not u.endswith(".pdf"):
            continue
        if any(bad in u for bad in BAD_TOKENS):
            continue
        if base not in u:
            continue
        good.append(url)

    seen = set()
    out: List[str] = []
    for x in good:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out


def filter_pdf_links_open(links: List[str]) -> List[str]:
    good: List[str] = []
    for url in links:
        u = url.lower()
        if not u.endswith(".pdf"):
            continue
        if any(bad in u for bad in BAD_TOKENS):
            continue
        good.append(url)

    seen = set()
    out: List[str] = []
    for x in good:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out


def path_bonus(url: str) -> int:
    u = url.lower()
    bonus = 0

    # anno 2024 nel path in forma "pulita"
    if re.search(r"(^|[/_-])2024([/_\-\.]|$)", u):
        bonus += 3

    if "annual-report-2024" in u or "integrated-report-2024" in u:
        bonus += 3
    if "sustainability-report-2024" in u or "esg-report-2024" in u:
        bonus += 3
    if "universal-registration-document" in u or "urd" in u:
        bonus += 2
    if "full" in u or "complete" in u:
        bonus += 1
    if "summary" in u or "highlights" in u:
        bonus -= 2

    return bonus


def choose_best(candidates: List[str]) -> str:
    """
    Sceglie il migliore solo in base al path_bonus (fallback se GPT non decide).
    """
    if not candidates:
        return "NONE"
    ranked = sorted(
        candidates,
        key=lambda u: (path_bonus(u)),
        reverse=True
    )
    return ranked[0]


# ============================================================
# LLM CONFIG (AZURE GPT-5-mini)
# ============================================================

llm = AzureChatOpenAI(
    azure_endpoint=os.getenv("ENDPOINT_URL"),
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    deployment_name=os.getenv("AZURE_DEPLOYMENT", "gpt-5-mini"),
    api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2025-01-01-preview"),
    temperature=1.0,   # fisso come da tua configurazione
    max_tokens=300,
)


# ============================================================
# LLM: GENERAZIONE QUERY PER TAVILY (VERSIONE ROBUSTA)
# ============================================================

def ask_llm_build_queries(
    company: str,
    domain: str,
    year: str = TARGET_YEAR,
    n_queries: int = 2
) -> List[str]:
    """
    Chiede a GPT di costruire fino a n_queries query Tavily ottimizzate.
    Usa lo stesso client AzureChatOpenAI (`llm`) e lo stesso template
    di chiamata usato in `ask_llm_pick_one`.
    Ritorna una lista di query (al massimo n_queries), oppure [] in caso di errore.
    """
    system = (
        "You are an expert assistant that builds highly effective web search queries for Tavily.\n"
        "Your goal is to find corporate sustainability/ESG reports for a given company and year.\n"
        "You must output STRICTLY a JSON object of the form: {\"queries\": [\"...\"]}.\n"
        "No explanations, no markdown."
    )

    user = (
        f"Company: {company}\n"
        f"Preferred domain: {domain}\n"
        f"Target year: {year}\n\n"
        "Task:\n"
        f"- Build up to {n_queries} different English web search queries.\n"
        "- Optimize for finding the official 2024 corporate sustainability / ESG report, "
        "annual report, integrated report or universal registration document (URD).\n"
        "- Include relevant keywords like 'sustainability report', 'ESG report', "
        "'integrated report', 'annual report', 'universal registration document', 'URD', "
        "and the target year.\n"
        "- Queries must be well-formed and suitable for a search engine.\n\n"
        "Return strictly a JSON object like:\n"
        "{\"queries\": [\"first query\", \"second query\"]}"
    )

    try:
        # 👉 qui usiamo lo stesso template di ask_llm_pick_one: llm.invoke + resp.content
        resp = llm.invoke(
            [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ]
        )

        raw = (resp.content or "").strip()
        print("\n[DEBUG LLM RAW QUERY RESPONSE]")
        print(raw)

        if not raw:
            print("[DEBUG] Risposta LLM vuota, nessuna query generata.")
            return []

        # Parsing JSON della risposta
        try:
            data = json.loads(raw)
        except Exception as e:
            print(f"[DEBUG] JSON parse failed: {e}")
            return []

        queries: List[str] = []
        qlist = data.get("queries", [])

        # accetta sia stringa singola che lista
        if isinstance(qlist, str):
            qlist = [qlist]

        if isinstance(qlist, list):
            for q in qlist:
                q = str(q).strip()
                if q:
                    queries.append(q)

        # normalizza / deduplica / limita a n_queries
        cleaned_list: List[str] = []
        seen = set()
        for q in queries:
            q_clean = " ".join(q.split())
            if q_clean and q_clean not in seen:
                cleaned_list.append(q_clean)
                seen.add(q_clean)

        final_queries = cleaned_list[:n_queries]
        print(f"[DEBUG] Final LLM queries for {company}: {final_queries}")

        return final_queries

    except Exception as e:
        print(f"[ERROR LLM] generazione query fallita: {e}")
        return []




def ask_llm_pick_one(company: str, domain: str, candidates: List[str]) -> str:
    if not candidates:
        return "NONE"

    system = (
        "You are a precise ESG analyst. You must output exactly one direct PDF or HTML URL, "
        "or the word NONE if no candidate is suitable. No explanations."
    )
    user = (
        f"Company: {company}\n"
        f"Preferred domain: {domain}\n\n"
        f"Choose the single best 2024 corporate report among these candidates (only pick from this list!):\n"
        + "\n".join(f"- {u}" for u in candidates) +
        "\n\nPriority: prefer paths that include 'annual-report-2024', 'integrated-report-2024', "
        "'sustainability-report-2024', 'esg-report-2024', or clearly contain '2024'. "
        "If unsure, choose the most complete/official report. "
        "Return only the URL (no markdown, no text). If none suits, return NONE."
    )

    try:
        resp = llm.invoke(
            [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ]
        )
        text = (resp.content or "").strip()
        m = re.search(r"https?://\S+?\.pdf(?:[?#]\S*)?", text, flags=re.I)
        return m.group(0) if m else ("NONE" if text.upper() == "NONE" else "")
    except Exception as e:
        print(f"[ERROR LLM] scelta URL fallita: {e}")
        return ""


# ============================================================
# RICERCATORE TAVILY (con LLM-query)
# ============================================================

class ESGPDFSearcherLLM:
    def __init__(self, max_results: int = 20):
        self.tavily = TavilySearch(
            max_results=max_results,
            topic="general",
            search_depth="advanced",
            start_date=f"{TARGET_YEAR}-01-01",
            include_answer=False,
            include_raw_content=False,
        )

    def _tavily_query(self, query: str, include_domains: List[str] | None = None) -> List[Dict]:
        payload: Dict = {"query": query}
        if include_domains:
            payload["include_domains"] = include_domains

        try:
            res = self.tavily.invoke(payload)
            results = (res or {}).get("results", [])
            return results
        except Exception as e:
            print(f"[ERROR Tavily] query fallita: {e}")
            return []

    def search(self, company: str, domain: str) -> Tuple[List[str], Dict[str, int]]:

        # 1) LLM: genera fino a 2 query "intelligenti"
        llm_queries = ask_llm_build_queries(company, domain, year=TARGET_YEAR, n_queries=2)

        if not llm_queries:
            print(f"[WARN] {company} | LLM non ha generato query, userò solo le fallback.")

        # 2) Query di fallback
        fallback_site = build_site_query(company)
        fallback_open = build_open_query(company)

        items_site: List[Dict] = []
        html_site_raw: List[str] = []

        # 2.1) Prova con le llm_queries
        for q in llm_queries:
            res = self._tavily_query(q, include_domains=[domain])
            items_site.extend(res)

        # 2.2) Se ancora niente, prova con la query classica build_site_query
        if not items_site:
            res = self._tavily_query(fallback_site, include_domains=[domain])
            items_site.extend(res)

        pdfs_site: List[Dict] = []
        for it in items_site:
            url = (it.get("url") or "").strip()
            if not url:
                continue
            if url.lower().endswith(".pdf"):
                it["score"] = score_result(it, company)
                pdfs_site.append(it)
            else:
                html_site_raw.append(url)

        pdfs_site.sort(key=lambda x: x.get("score", 0), reverse=True)
        links_site_scored = [x["url"] for x in pdfs_site if x.get("score", 0) >= 0]
        cands_site = filter_pdf_links(links_site_scored, domain)

        # ======== OPEN-WEB SEARCH (include_domains=None) ========

        items_open: List[Dict] = []
        html_open_raw: List[str] = []

        # 3.1) Prova llm_queries sull'open web
        for q in llm_queries:
            res = self._tavily_query(q, include_domains=None)
            items_open.extend(res)

        # 3.2) Se ancora niente, prova con la query classica open
        if not items_open:
            res = self._tavily_query(fallback_open, include_domains=None)
            items_open.extend(res)

        pdfs_open: List[Dict] = []
        for it in items_open:
            url = (it.get("url") or "").strip()
            if not url:
                continue
            if url.lower().endswith(".pdf"):
                it["score"] = score_result(it, company)
                pdfs_open.append(it)
            else:
                html_open_raw.append(url)

        pdfs_open.sort(key=lambda x: x.get("score", 0), reverse=True)
        links_open_scored = [x["url"] for x in pdfs_open if x.get("score", 0) >= 0]
        cands_open = filter_pdf_links_open(links_open_scored)

        # ======== HTML CANDIDATES ========

        # HTML sul dominio
        html_site: List[str] = []
        base = domain.lower().split(".")[-2]  # es: bayer.com -> bayer
        for u in html_site_raw:
            lu = u.lower()
            if base in lu and not any(bad in lu for bad in BAD_TOKENS):
                html_site.append(u)
        html_site = list(dict.fromkeys(html_site))

        # HTML open-web
        html_open: List[str] = []
        for u in html_open_raw:
            lu = u.lower()
            if not any(bad in lu for bad in BAD_TOKENS):
                html_open.append(u)
        html_open = list(dict.fromkeys(html_open))

        # ======== MERGE CANDIDATES (ordine di priorità) ========

        merged: List[str] = []
        seen = set()

        # 1) PDF sul dominio
        if cands_site:
            for url in cands_site:
                if url not in seen:
                    merged.append(url)
                    seen.add(url)

        # 2) Se non ci sono PDF, HTML sul dominio
        if not merged and html_site:
            for url in html_site:
                if url not in seen:
                    merged.append(url)
                    seen.add(url)

        # 3) Se ancora niente, PDF open-web
        if not merged and cands_open:
            for url in cands_open:
                if url not in seen:
                    merged.append(url)
                    seen.add(url)

        # 4) Ultima spiaggia, HTML open-web
        if not merged and html_open:
            for url in html_open:
                if url not in seen:
                    merged.append(url)
                    seen.add(url)

        stats = {
            "hits_site": len(items_site),
            "cands_site": len(cands_site),
            "hits_open": len(items_open),
            "cands_open": len(cands_open),
            "html_site": len(html_site),
            "html_open": len(html_open),
            "llm_queries_used": llm_queries,
            "fallback_site_query": fallback_site,
            "fallback_open_query": fallback_open,
        }

        return merged[:20], stats


# ============================================================
# MAIN SCRIPT
# ============================================================

def main():
    print("=== Ricerca primaria report 2024 (Tavily + LLM queries + GPT-5-mini) ===")

    searcher = ESGPDFSearcherLLM(max_results=20)

    with open(INPUT_FILE, newline='', encoding='utf-8') as infile, \
         open(OUTPUT_FILE, "w", newline='', encoding='utf-8') as outfile:

        reader = csv.DictReader(infile)
        fieldnames = ["company_id", "domain", "best_link", "hits_site", "cands_site",
                      "hits_open", "cands_open", "html_site", "html_open",
                      "llm_queries", "fallback_site_query", "fallback_open_query"]
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in tqdm(reader, desc="Processing companies"):
            company = (row.get("company_id") or "").strip()
            site = (row.get("primary_url") or "").strip()
            if not company or not site:
                continue

            domain = site.replace("https://", "").replace("http://", "").split("/")[0]

            try:
                candidates, stats = searcher.search(company, domain)

                print(f"[DEBUG] {company} | hits_site: {stats['hits_site']} | cands_site: {stats['cands_site']}")
                print(f"[DEBUG] {company} | hits_open: {stats['hits_open']} | cands_open: {stats['cands_open']}")
                print(f"[DEBUG] {company} | llm_queries: {stats['llm_queries_used']}")

                # 1) GPT sceglie il miglior link tra i candidati
                best_link = ask_llm_pick_one(company, domain, candidates)

                # 2) Fallback: ranking semplice se GPT non restituisce nulla
                if not best_link:
                    best_link = choose_best(candidates)

                if best_link == "NONE":
                    print(f"[DEBUG] {company} | fallback-candidates (prime 5): {candidates[:5]}")

                writer.writerow({
                    "company_id": company,
                    "domain": domain,
                    "best_link": best_link,
                    "hits_site": stats["hits_site"],
                    "cands_site": stats["cands_site"],
                    "hits_open": stats["hits_open"],
                    "cands_open": stats["cands_open"],
                    "html_site": stats["html_site"],
                    "html_open": stats["html_open"],
                    "llm_queries": " || ".join(stats["llm_queries_used"]),
                    "fallback_site_query": stats["fallback_site_query"],
                    "fallback_open_query": stats["fallback_open_query"],
                })
                outfile.flush()

                if best_link and best_link != "NONE":
                    print(f"✅ {company}: {best_link}")
                else:
                    print(f"⚠️ {company}: nessun link valido")

            except Exception as e:
                print(f"❌ Errore con {company}: {e}")
                writer.writerow({
                    "company_id": company,
                    "domain": domain,
                    "best_link": "",
                    "hits_site": 0,
                    "cands_site": 0,
                    "hits_open": 0,
                    "cands_open": 0,
                    "html_site": 0,
                    "html_open": 0,
                    "llm_queries": "",
                    "fallback_site_query": "",
                    "fallback_open_query": "",
                })
                outfile.flush()

            # throttling per non stressare Tavily/OpenAI
            time.sleep(1.5)

    print("\n=== Ricerca completata ===")
    print(f"Risultati salvati in: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()

'''
# === CREDENZIALI AZURE OPENAI ===
$env:AZURE_OPENAI_API_KEY="wLPBFmPkwquNFwn5IKDR3W8mv1ZKb95FGnxLZ0RgUiEl32D9qFaGJQQJ99BHACI8hq2XJ3w3AAABACOGyD04"
$env:ENDPOINT_URL="https://openaimaurino2.openai.azure.com/"
$env:AZURE_OPENAI_API_VERSION="2025-01-01-preview"
$env:AZURE_DEPLOYMENT="gpt-5-mini"

# === CREDENZIALI TAVILY ===
$env:TAVILY_API_KEY="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"

python search_agent/find_reports.py

fino a 131(109 corrette) sulle totali, ovvero VESTAS WIND SYSTEMS A/S
per ora circa 100 corrette, quando tavily da dei pdf a gpt al 90% son corretti, rimangono molti casi in cui tavily non trova nulla. Posso provare con tavily map o tavily crawl per vedere se migliora.
Adesso testo con altre 70, quindi fino a 179, STORA ENSO OYJ
Per ora 209 su 220 corrette  (95%)
Aggiunte altre 60, quindi fino all'indice 239, fino a BECHTLE AG. Quelle di prima che non trovavano finiscono a AEGON LTD.
tot su 250 su 280, ma con un solo run, aggiungo altre 20 per arrivare alla cifra tonda di 300 totali, fino all'indice 259, quindi fino ad ACCOR.
ora 281 su 300 corrette, 93,67%
'''