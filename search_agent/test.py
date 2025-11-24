# -*- coding: utf-8 -*-

import os
import re
import csv
import time
from typing import List, Dict, Tuple

from tqdm import tqdm
from langchain_tavily import TavilySearch
from langchain_openai import AzureChatOpenAI

INPUT_FILE = "search_agent/data/input/test_next.csv"
OUTPUT_FILE = "search_agent/data/output/sustainability_reports_next_2024.csv"


# query Tavily

def build_site_query(company_name: str) -> str:
    return (
        f'{company_name} 2024 sustainability report ESG "annual report" "integrated report" "universal registration document" link or pdf'
    )


def build_open_query(company_name: str) -> str:
    return (
        f'Find {company_name} 2024 sustainability report ESG, or "annual report" or "integrated report" or "universal registration document" link or pdf'
    )


# Ranking 

BAD_TOKENS = [
    "press", "slides", "presentation", "news", "update", "outlook",
    "framework", "ownership", "governance", "index", "highlights", "summary",
    "q1", "q2", "q3", "q4", "financial_statements", "corporate-governance",
    "governance-only", "kpi", "methodology"
]

GOOD_KEYWORDS = [
    "2024", "sustainability", "sustainable",
    "integrated", "non-financial", "esg", "sustainability-report"
    "annual", "universal", "registration", "report",
    "impact", "nonfinancial", "urd", "universal registration", "ESG Report"
]


def score_result(item: Dict, company: str) -> int:
    title = (item.get("title") or "").lower()
    url = (item.get("url") or "").lower()
    score = 0

    # anno
    if "2024" in url or "2024" in title:
        score += 2

    # parole buone
    if any(k in title for k in GOOD_KEYWORDS):
        score += 5

    if "annual" in title and "sustainability" in title:
        score += 3

    brand_token = re.sub(r"[^a-z]", "", (company.split()[0] if company else "").lower())
    if brand_token and brand_token in re.sub(r"[^a-z]", "", url):
        score += 1

    # penalità per token cattive
    if any(b in url for b in BAD_TOKENS):
        score -= 2

    # se non è pdf o html lo penajizzo
    if not url.endswith(".pdf") or url.endswith(".html") or url.endswith(".htm"):
        score -= 3

    return score


# Filtro su dominio/path

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


# ranking anche per il path

def path_bonus(url: str) -> int:
    u = url.lower()
    bonus = 0

    # anno
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
    if not candidates:
        return "NONE"
    ranked = sorted(
        candidates,
        key=lambda u: (path_bonus(u)),
        reverse=True
    )
    return ranked[0]


# === Ricercatore Tavily (solo Search) ===

class ESGPDFSearcher:
    def __init__(self, max_results: int = 20):
        # NOTA: niente start_date qui, per evitare filtri temporali strani.
        self.tavily = TavilySearch(
            max_results=max_results,
            topic="general",
            search_depth="advanced",
            start_date="2024-01-01",
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

    def search(self, company: str, domain: str) -> Tuple[str, str, List[str], Dict[str, int]]:
        q_site = build_site_query(company)
        q_open = build_open_query(company)

        # 1) partendo dal dominio:
        items_site = self._tavily_query(q_site, include_domains=[domain])
        pdfs_site: List[Dict] = []
        html_site_raw: List[str] = []

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

        # 2) senza dominio
        items_open = self._tavily_query(q_open, include_domains=None)
        pdfs_open: List[Dict] = []
        html_open_raw: List[str] = []

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


        # nel caso non ci siano PDF, provo a fornire HTML
        html_site: List[str] = []
        base = domain.lower().split(".")[-2]  # es: bayer.com -> bayer
        for u in html_site_raw:
            lu = u.lower()
            if base in lu and not any(bad in lu for bad in BAD_TOKENS):
                html_site.append(u)
        html_site = list(dict.fromkeys(html_site))

        html_open: List[str] = []
        for u in html_open_raw:
            lu = u.lower()
            if not any(bad in lu for bad in BAD_TOKENS):
                html_open.append(u)
        html_open = list(dict.fromkeys(html_open))

        # Output per gpt:
        # 1) Link dei PDF sul dominio
        # 2) Link HTML sul dominio
        # 3) Link dei PDF open-web
        # 4) Link HTML open-web
        merged: List[str] = []
        seen = set()

        # 1) Link dei PDF sul dominio
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

        # 3) Se ancora niente, usa PDF open-web
        if not merged and cands_open:
            for url in cands_open:
                if url not in seen:
                    merged.append(url)
                    seen.add(url)

        # 4) ultima spiaggia, HTML open-web
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
        }
        return q_site, q_open, merged[:20], stats




# === LLM (Azure GPT-5-mini) ===

llm = AzureChatOpenAI(
    azure_endpoint=os.getenv("ENDPOINT_URL"),
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    deployment_name=os.getenv("AZURE_DEPLOYMENT", "gpt-5-mini"),
    api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2025-01-01-preview"),
    temperature=1.0, 
    max_tokens=600,
)


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


# === MAIN ===

def main():
    print("=== Ricerca primaria report 2024 (Tavily Search + GPT-5-mini) ===")

    searcher = ESGPDFSearcher(max_results=20)

    with open(INPUT_FILE, newline='', encoding='utf-8') as infile, \
         open(OUTPUT_FILE, "w", newline='', encoding='utf-8') as outfile:

        reader = csv.DictReader(infile)
        fieldnames = ["company_id", "domain", "query", "best_link"]
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in tqdm(reader, desc="Processing companies"):
            company = (row.get("company_id") or "").strip()
            site = (row.get("primary_url") or "").strip()
            if not company or not site:
                continue

            domain = site.replace("https://", "").replace("http://", "").split("/")[0]

            try:
                q_site, q_open, candidates, stats = searcher.search(company, domain)

                print(f"[DEBUG] {company} | site-query: {q_site}")
                print(f"[DEBUG] {company} | hits_site: {stats['hits_site']} | cands_site: {stats['cands_site']}")
                print(f"[DEBUG] {company} | hits_open: {stats['hits_open']} | cands_open: {stats['cands_open']}")

                # 1) 
                best_link = ask_llm_pick_one(company, domain, candidates)

                # 2) 
                if not best_link:
                    best_link = choose_best(candidates)

                # 3) 
                if best_link == "NONE":
                    print(f"[DEBUG] {company} | fallback-candidates (prime 5): {candidates[:5]}")

                writer.writerow({
                    "company_id": company,
                    "domain": domain,
                    "query": q_site,
                    "best_link": best_link
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
                    "query": "",
                    "best_link": ""
                })
                outfile.flush()

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

python search_agent/test.py

ora 281 su 300 corrette, 93,67%
'''
