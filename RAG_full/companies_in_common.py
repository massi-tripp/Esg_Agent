from pathlib import Path
import pandas as pd
'''codice per vedere al volo quali sono i nomi delle aziende che la rag non salva bene'''
RAG2_XLSX = Path(r"C:\Universita\TESI\esg_agent\RAG_full\rag3_out\activities_extracted_clean.xlsx")
BENCH_XLSX = Path(r"C:\Universita\TESI\esg_agent\RAG_full\documentazione_rag.xlsx")

def norm_name(s: str) -> str:
    return " ".join(str(s).strip().split())

def main():
    rag = pd.read_excel(RAG2_XLSX, dtype=str).fillna("")
    bench = pd.read_excel(BENCH_XLSX, dtype=str).fillna("")

    if "company" not in rag.columns:
        raise ValueError("Nel file RAG2 manca la colonna 'company'")
    if "company_name_full" not in bench.columns:
        raise ValueError("Nel benchmark manca la colonna 'company_name_full'")

    rag_companies = {norm_name(x) for x in rag["company"].tolist() if norm_name(x)}
    bench_companies = {norm_name(x) for x in bench["company_name_full"].tolist() if norm_name(x)}

    only_in_rag = sorted(rag_companies - bench_companies)
    only_in_bench = sorted(bench_companies - rag_companies)
    common = sorted(rag_companies & bench_companies)

    print("=== COMPANY SETS ===")
    print(f"RAG2 companies:   {len(rag_companies)}")
    print(f"BENCH companies:  {len(bench_companies)}")
    print(f"Common companies: {len(common)}")
    print(f"Only in RAG2:     {len(only_in_rag)}")
    print(f"Only in BENCH:    {len(only_in_bench)}")

    print("\n--- Companies ONLY IN RAG2 ---")
    for c in only_in_rag:
        print(f"- {c}")

    print("\n--- Companies ONLY IN BENCH ---")
    for c in only_in_bench:
        print(f"- {c}")

if __name__ == "__main__":
    main()