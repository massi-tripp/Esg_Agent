Parte 2 — ESG Report Analysis (RAG): passo-passo

1) Preparo la lista dei report (benchmark)

Compilo analysis_rag/data/benchmark/metadata.csv con: company_id,year,language,url,filename.

Obiettivo: definire ~20 PDF su cui fare benchmark.

2) Scarico i PDF

Eseguo analysis_rag/downloader/download_from_links.py.

Output: PDF in analysis_rag/data/benchmark/pdfs/ + log in analysis_rag/data/benchmark/download_log.csv.

3) Diagnosi PDF (scansionati vs digitali)

Eseguo analysis_rag/parser/detect_scanned.py.

Output: report a console con quante pagine hanno poco testo (candidate OCR).

4) Estrazione contenuti (testo + immagini, con fallback OCR)

Eseguo analysis_rag/parser/pdf_extract.py.

Output:

Testo per pagina: analysis_rag/data/benchmark/parsed/<file>.pages.jsonl

Metadati (pagine OCR, numero immagini): analysis_rag/data/benchmark/parsed_meta/<file>.meta.json

Immagini estratte: analysis_rag/data/benchmark/images/…

Report a console con quante pagine hanno poco testo (candidate OCR).

5) Definire la tassonomia ESG

Rivedi/aggiorna analysis_rag/configs/taxonomy.yaml con le attività/etichette che vuoi riconoscere (ENV, SOC, GOV, TARGET, STANDARD).

6) Indicizzazione per RAG (embedding + FAISS)

Imposto le variabili Azure (AZURE_OPENAI_API_KEY, ENDPOINT_URL, DEPLOYMENT_NAME, EMBEDDING_DEPLOYMENT).

Eseguo analysis_rag/rag/build_index.py.

Output: indice FAISS + metadati in analysis_rag/data/benchmark/index/.

7) Estrazione attività ESG (RAG con GPT-5-mini)

Eseguo analysis_rag/rag/extract_activities.py.

Il modello interroga l’indice, legge i chunk rilevanti e produce JSON strutturato con: label, pages, evidence.

Output: analysis_rag/data/benchmark/predictions.jsonl.

8) Valutazione benchmark

Prepara il manuale in analysis_rag/data/benchmark/gold.jsonl (etichette presenti + evidenze).

Eseguo analysis_rag/evaluation/run_benchmark.py.

Output: metriche (Macro-F1 multilabel, copertura per etichetta) a console.