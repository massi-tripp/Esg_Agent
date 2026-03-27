# -*- coding: utf-8 -*-
import os
import re
import csv
import time
import json
from typing import List, Dict, Tuple

from tqdm import tqdm
from langchain_tavily import TavilySearch
from langchain_openai import AzureChatOpenAI
from openai import AzureOpenAI

import statistics
from time import perf_counter
from collections import defaultdict

INPUT_FILE = "search_agent/data/input/test_next.csv"
OUTPUT_FILE = "search_agent/data/output/sustainability_reports_next_2024.csv"

TARGET_YEAR = "2024"

# =========================
# TIMING / PROFILING
# =========================
TIMINGS = defaultdict(list)  # key -> list of seconds
COUNTS = defaultdict(int)    # key -> int

def _t0() -> float:
    return perf_counter()

def _t1(t_start: float) -> float:
    return perf_counter() - t_start

def record_timing(key: str, seconds: float) -> None:
    TIMINGS[key].append(seconds)

def summarize_timings() -> None:
    print("\n=== TIMING SUMMARY (seconds) ===")
    if not TIMINGS:
        print("No timing data collected.")
        return

    def _fmt(vals: list[float]) -> str:
        if not vals:
            return "-"
        vals_sorted = sorted(vals)
        mean = statistics.mean(vals_sorted)
        p50 = vals_sorted[len(vals_sorted)//2]
        p90 = vals_sorted[int(0.9*(len(vals_sorted)-1))]
        mx = max(vals_sorted)
        return f"n={len(vals_sorted)} mean={mean:.3f} p50={p50:.3f} p90={p90:.3f} max={mx:.3f}"

    for k in sorted(TIMINGS.keys()):
        print(f"{k:28s} {_fmt(TIMINGS[k])}")

# ===================================================================
# QUERY se non dovessero funzionare quelle generate da GPT
# ===================================================================

def build_site_query(company_name: str) -> str:
    return (
        f'{company_name} 2024 sustainability report ESG '
        f'"annual report" "integrated report" "universal registration document" link or pdf'
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

    if TARGET_YEAR in url or TARGET_YEAR in title:
        score += 2

    if any(k.lower() in title for k in GOOD_KEYWORDS):
        score += 5

    if "annual" in title and "sustainability" in title:
        score += 3

    brand_token = re.sub(r"[^a-z]", "", (company.split()[0] if company else "").lower())
    if brand_token and brand_token in re.sub(r"[^a-z]", "", url):
        score += 1

    if any(b in url for b in BAD_TOKENS):
        score -= 2

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


def choose_best(candidates: List[str]) -> str:  # fallback se GPT non decide
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
    deployment_name=os.getenv("AZURE_DEPLOYMENT", "gpt-5.1-chat"),
    api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2025-01-01-preview"),
    temperature=1.0,
    max_tokens=None,
)


# ============================================================
#             LLM: GENERAZIONE QUERY PER TAVILY
# ============================================================

def ask_llm_build_queries(
    company: str,
    domain: str,
    year: str = TARGET_YEAR,
    n_queries: int = 1
) -> List[str]:

    system = (
        "You are an expert assistant that builds highly effective web search queries for Tavily.\n"
        "Your goal is to find corporate annual reports (including sustainability/ESG sections) "
        "for a given company in 2024.\n"
        "You must output ONLY a search query string. DO NOT include the domain.\n"
        "No explanations, no markdown, no JSON, no bullet points."
    )

    user = (
        f"Company: {company}\n"
        f"Preferred domain: {domain}\n"
        f"Target year: {year}\n\n"
        "Write one query for an English Tavily web search to find the official 2024 "
        "sustainability / ESG or annual / integrated or universal registration (URD) or strategic report "
        "of this company.\n"
        "The query must be suitable for the search engine Tavily.\n"
        "You MUST output only the query string, and nothing else."
    )

    try:
        t = _t0()
        resp = llm.invoke(
            [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ]
        )
        record_timing("llm_query_1_invoke", _t1(t))
        raw = (resp.content or "").strip()

        if not raw:
            print("[DEBUG] Risposta LLM vuota, nessuna query generata.")
            return []

        queries: List[str] = []
        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue
            queries.append(line)

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


# ============================================================
#        LLM: GENERAZIONE SECONDA QUERY PER TAVILY
# ============================================================

def ask_llm_build_alternative_query(
    company: str,
    domain: str,
    year: str,
    previous_query: str
) -> str:
    system = (
        "You are an expert assistant that builds highly effective web search queries for Tavily.\n"
        f"Your goal is to find corporate annual reports (including sustainability/ESG sections) "
        f"for a given company in {year}.\n"
        "You must output ONLY a search query string. DO NOT include the domain.\n"
        "No explanations, no markdown, no JSON, no bullet points."
    )

    user = (
        f"Company: {company}\n"
        f"Preferred domain: {domain}\n"
        f"Target year: {year}\n\n"
        f"The following query was already tried and did NOT return good official {year} reports:\n"
        f"PREVIOUS_QUERY: {previous_query}\n\n"
        "Task:\n"
        f"- Propose a DIFFERENT Tavily web search query to find the official {year} "
        "sustainability / ESG or annual / integrated or universal registration (URD) or strategic report of this company.\n"
        "- Do NOT repeat the previous query.\n"
        "- Avoid adding very generic extra keywords if they are not needed.\n\n"
        "You MUST output only the NEW query string, and nothing else. DO NOT include the domain."
    )

    try:
        t = _t0()
        resp = llm.invoke(
            [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ]
        )
        record_timing("llm_query_2_invoke", _t1(t))
        raw = (resp.content or "").strip()
        print("\n[DEBUG LLM RAW ALT-QUERY RESPONSE]")
        print(raw)

        if not raw:
            print("[DEBUG] Risposta LLM vuota per alternative query.")
            return ""

        for line in raw.splitlines():
            line = line.strip()
            if line:
                return line

        return ""

    except Exception as e:
        print(f"[ERROR LLM] generazione query alternativa fallita: {e}")
        return ""


# ============================================================
#        LLM: GENERAZIONE TERZA QUERY PER TAVILY
# ============================================================

def ask_llm_build_third_query(
    company: str,
    domain: str,
    year: str,
    previous_queries: List[str],
) -> str:

    prev_block = "\n".join(
        f"- {q}" for q in previous_queries if q.strip()
    ) or "(none)"

    system = (
        "You are an expert assistant that builds highly effective web search queries for Tavily.\n"
        f"Your goal is to find corporate annual reports (including sustainability/ESG sections) "
        f"for a given company in {year}.\n"
        "You must output ONLY a search query string. DO NOT include the domain.\n"
        "No explanations, no markdown, no JSON, no bullet points."
    )

    user = (
        f"Company: {company}\n"
        f"Preferred domain: {domain}\n"
        f"Target year: {year}\n\n"
        "The following queries were already tried and did NOT return good official reports:\n"
        f"{prev_block}\n\n"
        "Task:\n"
        f"- Propose a NEW and DIFFERENT Tavily web search query to find the official {year} "
        "sustainability / ESG or annual / integrated or universal registration (URD)  or strategic report of this company.\n"
        "- Do NOT repeat or trivially rephrase the previous queries.\n"
        "- Avoid adding very generic extra keywords if they are not needed.\n\n"
        "You MUST output only the NEW query string, and nothing else. DO NOT include the domain."
    )

    try:
        t = _t0()
        resp = llm.invoke(
            [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ]
        )
        record_timing("llm_query_3_invoke", _t1(t))
        raw = (resp.content or "").strip()
        print("\n[DEBUG LLM RAW THIRD-QUERY RESPONSE]")
        print(raw)

        if not raw:
            print("[DEBUG] Risposta LLM vuota per third query.")
            return ""

        for line in raw.splitlines():
            line = line.strip()
            if line:
                return line

        return ""

    except Exception as e:
        print(f"[ERROR LLM] generazione third query fallita: {e}")
        return ""


# ============================================================
#             GPT: SCEGLI IL MIGLIOR URL
# ============================================================

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
        "'sustainability-report-2024', 'esg-report-2024', 'strategic-report' or clearly contain '2024'. "
        "If unsure, choose the most complete/official report. "
        "Return only the URL (no markdown, no text). If none suits, return NONE."
    )

    try:
        t = _t0()
        resp = llm.invoke(
            [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ]
        )
        record_timing("llm_pick_one_invoke", _t1(t))
        text = (resp.content or "").strip()
        m = re.search(r"https?://\S+?\.pdf(?:[?#]\S*)?", text, flags=re.I)
        return m.group(0) if m else ("NONE" if text.upper() == "NONE" else "")
    except Exception as e:
        print(f"[ERROR LLM] scelta URL fallita: {e}")
        return ""


# ============================================================
# RICERCATORE TAVILY (con query di GPT)
# ============================================================

class ESGPDFSearcherLLM:
    def __init__(self, max_results: int = 20):
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
            t = _t0()
            res = self.tavily.invoke(payload)
            record_timing("tavily_invoke", _t1(t))
            if isinstance(res, list):
                return res
            if isinstance(res, dict):
                return res.get("results", [])

            print(f"[WARN Tavily] Risposta inattesa dal client: {res!r}")
            return []
        except Exception as e:
            print(f"[ERROR Tavily] query fallita: {e}")
            return []

    def search(self, company: str, domain: str) -> Tuple[List[str], Dict[str, int]]:

        # 1) GPT: PRIMA QUERY
        llm_queries: List[str] = ask_llm_build_queries(
            company, domain, year=TARGET_YEAR, n_queries=1
        )

        if not llm_queries:
            print(f"[WARN] {company} | LLM non ha generato query, userò solo fallback.")

        # 2) Query di fallback manuale
        fallback_site = build_site_query(company)

        items_site: List[Dict] = []       # risultati grezzi Tavily
        html_site_raw: List[str] = []     # URL HTML grezzi
        pdfs_site: List[Dict] = []        # risultati PDF con score

        llm_queries_alt: List[str] = []     # query 2
        llm_queries_third: List[str] = []   # query 3

        site_query_source = "none"

        # helper per aggiungere risultati Tavily
        def add_results(results: List[Dict]):
            nonlocal items_site
            items_site.extend(results)
            for it in results:
                url = (it.get("url") or "").strip()
                if not url:
                    continue
                if url.lower().endswith(".pdf"):
                    it["score"] = score_result(it, company)
                    pdfs_site.append(it)
                else:
                    html_site_raw.append(url)

        def recompute_candidates() -> List[str]:
            t = _t0()
            pdfs_site.sort(key=lambda x: x.get("score", 0), reverse=True)
            links_site_scored = [x["url"] for x in pdfs_site if x.get("score", 0) >= 0]
            out = filter_pdf_links(links_site_scored, domain)
            record_timing("filter_recompute_candidates", _t1(t))
            return out

        # =========================
        # STEP 1: GPT QUERY #1
        # =========================
        if llm_queries:
            first_query = llm_queries[0]
            print(f"[INFO] {company} | GPT query #1: {first_query}")
            res_llm1 = self._tavily_query(first_query, include_domains=[domain])
            if res_llm1:
                site_query_source = "llm1"
            add_results(res_llm1)

        cands_site = recompute_candidates()

        # =========================
        # STEP 2: GPT QUERY #2 (ALTERNATIVA)
        # solo se dopo GPT1 NON ci sono PDF buoni
        # =========================
        if not cands_site and llm_queries:
            previous_query = llm_queries[0]
            alt_query = ask_llm_build_alternative_query(
                company=company,
                domain=domain,
                year=TARGET_YEAR,
                previous_query=previous_query,
            )

            if alt_query:
                llm_queries_alt.append(alt_query)
                print(f"[INFO] {company} | GPT query alternativa #2: {alt_query}")
                res_llm2 = self._tavily_query(alt_query, include_domains=[domain])
                if res_llm2:
                    site_query_source = (
                        "llm1+llm2" if site_query_source == "llm1" else "llm2"
                    )
                add_results(res_llm2)

                # ricalcolo candidati dopo GPT2
                cands_site = recompute_candidates()

        # =========================
        # STEP 3: GPT QUERY #3 (TERZA)
        # solo se dopo GPT1+GPT2 NON ci sono PDF buoni
        # =========================
        if not cands_site and (llm_queries or llm_queries_alt):
            prev_list = []
            if llm_queries:
                prev_list.append(llm_queries[0])
            if llm_queries_alt:
                prev_list.append(llm_queries_alt[0])

            third_query = ask_llm_build_third_query(
                company=company,
                domain=domain,
                year=TARGET_YEAR,
                previous_queries=prev_list,
            )

            if third_query:
                llm_queries_third.append(third_query)
                print(f"[INFO] {company} | GPT query #3: {third_query}")
                res_llm3 = self._tavily_query(third_query, include_domains=[domain])
                if res_llm3:
                    if site_query_source == "none":
                        site_query_source = "llm3"
                    else:
                        site_query_source = site_query_source + "+llm3"
                add_results(res_llm3)

                cands_site = recompute_candidates()

        # =========================
        # STEP 4: QUERY MANUALE (FALLBACK)
        # solo se dopo GPT1+GPT2+GPT3 NON ci sono PDF buoni
        # =========================
        if not cands_site:
            print(f"[INFO] {company} | uso fallback manuale: {fallback_site}")
            res_fb = self._tavily_query(fallback_site, include_domains=[domain])
            if res_fb:
                site_query_source = (
                    "fallback" if site_query_source == "none"
                    else site_query_source + "+fallback"
                )
            add_results(res_fb)

            # ricalcolo candidati dopo fallback
            cands_site = recompute_candidates()

        # ======== HTML SUL DOMINIO ========
        html_site: List[str] = []
        base = domain.lower().split(".")[-2]  # es: bayer.com -> bayer
        for u in html_site_raw:
            lu = u.lower()
            if base in lu and not any(bad in lu for bad in BAD_TOKENS):
                html_site.append(u)
        html_site = list(dict.fromkeys(html_site))

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

        stats = {
            "hits_site": len(items_site),
            "cands_site": len(cands_site),
            "html_site": len(html_site),
            "llm_queries_used": llm_queries,
            "llm_queries_alternative_used": llm_queries_alt,
            "llm_queries_third_used": llm_queries_third,
            "site_query_source": site_query_source,
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
        fieldnames = [
            "company_id", "domain", "best_link",
            "hits_site", "cands_site", "html_site",
            "llm_queries", "llm_queries_alternative", "llm_queries_third",
            "site_query_source"
        ]
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in tqdm(reader, desc="Processing companies"):
            company = (row.get("company_id") or "").strip()
            site = (row.get("primary_url") or "").strip()
            if not company or not site:
                continue

            domain = site.replace("https://", "").replace("http://", "").split("/")[0]
            t_company = _t0()
            try:
                candidates, stats = searcher.search(company, domain)

                print(f"[DEBUG] {company} | hits_site: {stats['hits_site']} | cands_site: {stats['cands_site']}")
                print(f"[DEBUG] {company} | llm_queries: {stats['llm_queries_used']}")
                print(f"[DEBUG] {company} | llm_queries_alternative: {stats['llm_queries_alternative_used']}")
                print(f"[DEBUG] {company} | llm_queries_third: {stats['llm_queries_third_used']}")
                print(f"[DEBUG] {company} | site_query_source: {stats['site_query_source']}")

                # 1) GPT sceglie il miglior link tra i candidati
                best_link = ask_llm_pick_one(company, domain, candidates)

                # 2) Fallback: ranking semplice se GPT non restituisce nulla
                if not best_link:
                    best_link = choose_best(candidates)

                if best_link == "NONE":
                    print(f"[DEBUG] {company} | fallback-candidates (prime 5): {candidates[:5]}")
                record_timing("company_total", _t1(t_company))
                COUNTS["companies_processed"] += 1
                if best_link and best_link != "NONE":
                    COUNTS["companies_success"] += 1
                else:
                    COUNTS["companies_none"] += 1
                COUNTS[f"source_{stats['site_query_source']}"] += 1
                writer.writerow({
                    "company_id": company,
                    "domain": domain,
                    "best_link": best_link,
                    "hits_site": stats["hits_site"],
                    "cands_site": stats["cands_site"],
                    "html_site": stats["html_site"],
                    "llm_queries": " || ".join(stats["llm_queries_used"]),
                    "llm_queries_alternative": " || ".join(stats["llm_queries_alternative_used"]),
                    "llm_queries_third": " || ".join(stats["llm_queries_third_used"]),
                    "site_query_source": stats["site_query_source"],
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
                    "html_site": 0,
                    "llm_queries": "",
                    "llm_queries_alternative": "",
                    "llm_queries_third": "",
                    "site_query_source": "",
                })
                outfile.flush()

            time.sleep(1.5)

    print("\n=== Ricerca completata ===")
    print("\n=== RUN SUMMARY ===")
    print(f"companies_processed: {COUNTS['companies_processed']}")
    print(f"companies_success:   {COUNTS['companies_success']}")
    print(f"companies_none:      {COUNTS['companies_none']}")
    print("\n=== site_query_source counts ===")
    for k in sorted([k for k in COUNTS.keys() if k.startswith("source_")]):
        print(f"{k.replace('source_',''):24s} {COUNTS[k]}")

    summarize_timings()

    print(f"Risultati salvati in: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()


# conda deactivate
# cd C:\Universita\TESI\esg_agent
# (C:\Users\massi\anaconda3\shell\condabin\conda-hook.ps1) ; (conda activate esg_agent)
'''
# === CREDENZIALI AZURE OPENAI ===
$env:AZURE_OPENAI_API_KEY="FsEzVtCW88IklIn1gdtodRT8at2wzo84YubWnvQ9eZNxiCnl4CXfJQQJ99CBACI8hq2XJ3w3AAABACOGWjK9"
$env:ENDPOINT_URL="https://maurinokeys.openai.azure.com/"
$env:AZURE_OPENAI_API_VERSION="2025-01-01-preview"
$env:AZURE_DEPLOYMENT="gpt-5.1-chat"

# === CREDENZIALI TAVILY ===
$env:TAVILY_API_KEY="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"

python search_agent/find_reports.py
'''