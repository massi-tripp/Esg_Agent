# -*- coding: utf-8 -*-

import os
import re
import csv
import time
from tqdm import tqdm
from langchain_tavily import TavilySearch
from langchain_openai import AzureChatOpenAI
from langchain.agents import create_agent


# === Percorsi ===
INPUT_FILE  = "search_agent/data/input/test_volvo.csv"
OUTPUT_FILE = "search_agent/data/output/sustainability_reports_missed_2024.csv"


# === Setup Tavily ===
tavily_tool = TavilySearch(
    max_results=30,
    topic="general",
    search_depth="advanced",
    include_answer=False,
    include_raw_content=False,
)


# === Setup LLM (Azure GPT-5-mini) ===
llm = AzureChatOpenAI(
    azure_endpoint=os.getenv("ENDPOINT_URL"),
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    deployment_name=os.getenv("AZURE_DEPLOYMENT", "gpt-5-mini"),
    api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2025-01-01-preview"),
    temperature=1.0,
    max_tokens=900,
)

agent = create_agent(llm, [tavily_tool])


# === Costruisce query Tavily ===
def build_query(company_name: str, domain: str) -> str:
    return (
        f"\"{company_name}\" site:{domain} \"Annual Report 2024\" filetype:pdf | "
        f"\"{company_name}\" site:{domain} "
        f"(\"Sustainability Report 2024\" OR \"ESG Report 2024\" OR \"Integrated Report 2024\" "
        f"OR \"Non-financial Statement 2024\" OR \"Annual Integrated Report 2024\" "
        f"OR \"AnnualReport2024\" OR \"AnnualIntegratedReport2024\") filetype:pdf"
        f"filetype:pdf -press -news -presentation -slides -update -outlook -index -governance"
    )


# === MAIN ===
def main():
    print("=== 🌍 Ricerca automatica report 2024 (Tavily + GPT-5-mini) ===")

    with open(INPUT_FILE, newline='', encoding='utf-8') as infile, \
         open(OUTPUT_FILE, "w", newline='', encoding='utf-8') as outfile:

        reader = csv.DictReader(infile)
        fieldnames = ["company_id", "domain", "query", "best_link"]
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in tqdm(reader, desc="Processing companies"):
            company = row.get("company_id", "").strip()
            site = row.get("primary_url", "").strip()
            if not company or not site:
                continue

            domain = site.replace("https://", "").replace("http://", "").split("/")[0]
            query = build_query(company, domain)
            best_link = ""

            try:
                # === Tavily search ===
                tavily_results = tavily_tool.invoke({"query": query})

                pdf_links = []

                # ✅ Gestione robusta di tutti i tipi di risposta Tavily
                if isinstance(tavily_results, str):
                    print(f"[DEBUG] Tavily ha restituito una stringa per {company}: {tavily_results[:120]}")

                    # Controlla se la stringa contiene un link PDF
                    found_links = re.findall(r"https?://\S+\.pdf", tavily_results)
                    if found_links:
                        pdf_links.extend(found_links)
                        results_data = []  # per compatibilità
                    else:
                        results_data = []

                elif isinstance(tavily_results, dict):
                    results_data = tavily_results.get("results", [])

                else:
                    print(f"[DEBUG] Tavily ha restituito un tipo inatteso per {company}: {type(tavily_results)}")
                    results_data = []

                # ✅ Estrazione sicura dei link PDF
                if isinstance(results_data, list):
                    for r in results_data:
                        if isinstance(r, dict):
                            url = r.get("url", "")
                            if isinstance(url, str) and url.lower().endswith(".pdf"):
                                pdf_links.append(url)
                        else:
                            print(f"[DEBUG] Elemento non-dict nei risultati Tavily per {company}: {type(r)}")
                else:
                    print(f"[DEBUG] Nessuna lista valida di risultati per {company} (type={type(results_data)})")
                    results_data = []

                # ✅ Filtro dominio corretto
                pdf_links = [p for p in pdf_links if domain.split('.')[0] in p]

                # ✅ FIX 2: filtro per nome azienda nel link (evita falsi positivi)
                name_token = company.split()[0].lower()
                pdf_links = [p for p in pdf_links if name_token in p.lower()]

                # ✅ Filtra per anno 2024
                pdf_links_2024 = [p for p in pdf_links if "2024" in p]

                exclusion_terms = ["press", "news", "release", "presentation", "slides", "outlook", "update", "index", "q1", "q2", "q3", "q4"]
                pdf_links = [p for p in pdf_links if not any(term in p.lower() for term in exclusion_terms)]

                # Se Tavily non trova PDF validi → fallback GPT
                if not pdf_links_2024:
                    prompt = (
                        f"You are an expert ESG analyst.\n"
                        f"Use Tavily to identify the single most relevant PDF link hosted on '{domain}' "
                        f"for the 2024 Annual Report or Sustainability Report of '{company}'.\n\n"
                        f"Tavily has returned up to 30 possible URLs. "
                        f"Choose ONLY among those links returned by Tavily, not invented ones. "
                        f"Prioritize links ending with '2024.pdf' or containing words like "
                        f"'annual-report-2024', 'sustainability-report-2024', 'integrated-report-2024', "
                        f"or 'non-financial-statement-2024'.\n\n"
                        f"Do not consider links related to press releases, index, presentations, news, slides, outlooks, or updates.\n\n"
                        f"Return ONLY the direct URL (no markdown, no explanation, no lists). "
                        f"If nothing relevant exists, return exactly 'NONE'."
                    )

                    result = agent.invoke({"messages": [{"role": "user", "content": prompt}]})
                    raw_output = str(result["messages"][-1].content).strip()

                    urls = re.findall(r"https?://\S+\.pdf", raw_output)
                    best_link = urls[0] if urls else raw_output
                else:
                    # Se Tavily ha trovato buoni PDF → seleziona il migliore
                    best_link = sorted(pdf_links_2024, key=len)[0]

                # === Scrivi risultato ===
                writer.writerow({
                    "company_id": company,
                    "domain": domain,
                    "query": query,
                    "best_link": best_link
                })
                outfile.flush()

                if best_link and best_link != "NONE":
                    print(f"✅ {company}: {best_link}")
                else:
                    print(f"⚠️ {company}: nessun link trovato")

            except Exception as e:
                print(f"❌ Errore con {company}: {e}")
                writer.writerow({
                    "company_id": company,
                    "domain": domain,
                    "query": query,
                    "best_link": ""
                })
                outfile.flush()

            time.sleep(2)  # Rispetta rate limit Tavily

    print("\n=== ✅ Ricerca completata ===")
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

"Sustainability Report 2024" OR "ESG Report 2024" OR "Integrated Report 2024" OR "Non-financial Statement 2024" OR "Annual Integrated Report 2024" OR "AnnualReport2024" OR "AnnualIntegratedReport2024" OR "Annual-Report-2024"
'''