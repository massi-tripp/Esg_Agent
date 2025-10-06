# 🌱 ESG Agent – Fase 1: Discovery & Ranking dei report di sostenibilità

## 📘 Panoramica del lavoro svolto

In questa prima fase del progetto di tesi è stato sviluppato un **agente di web discovery automatizzato** per l’individuazione dei **report di sostenibilità (ESG)** delle principali aziende europee, partendo unicamente dal sito principale di ciascuna.

Il sistema è stato implementato in **Python**, utilizzando **Scrapy + Playwright** per l’esplorazione dinamica delle pagine web e una pipeline in tre fasi:

1. **🌐 Discovery (crawler mirato)**  
   - Scansione in ampiezza (BFS) del sito corporate e dei suoi sottodomini.  
   - Filtraggio basato su **keyword multilingua** (ESG, sustainability, CSR, CSRD, DNF, ecc.) in URL e testo del link.  
   - Rilevamento automatico di **PDF** e pagine HTML candidati con metadati (profondità, anno, lingua, ecc.).  
   - Output: `data/interim/candidates_raw.jsonl`.

2. **📊 Ranking (euristico)**  
   - Calcolo di uno **score** per ogni candidato, basato su:
     - tipo di file (`pdf` favorito),
     - presenza di keyword nel link/testo,
     - anno nel percorso,
     - profondità nel sito (più superficiale → punteggio maggiore).  
   - Selezione automatica del **miglior candidato per azienda**.  
   - Output: `data/output/candidates_best.csv`.

3. **📈 Validazione e report automatico**  
   - Generazione di un report HTML interattivo con indicatori di coverage, pagine visitate e top-5 candidati per azienda.  
   - Output: `data/interim/reports/discovery_report.<timestamp>.html`.

---

## ⚙️ Parametri della run corrente

Esecuzione del crawler (`scraper.run_discovery`) con i seguenti parametri:

| Parametro | Valore | Significato |
|------------|---------|-------------|
| `--limit` | **4** | Numero massimo di aziende processate in questa run |
| `--max-depth` | **2** | Profondità massima di esplorazione per dominio |
| `--max-pages` | **60** | Numero massimo di pagine visitabili per azienda |
| `--render-budget` | **10** | Numero massimo di pagine da renderizzare via Playwright (per siti dinamici) |
| `--disable-render` | **False** | Il rendering JS è attivo, ma limitato dal budget |
| `--allow-external-pdfs` | **True** | Permette di seguire link PDF anche esterni al dominio principale |

📂 Output generato:
- `data/interim/pages_visited.jsonl`
- `data/interim/candidates_raw.jsonl`
- `data/output/candidates_best.csv`
- `data/interim/reports/discovery_report.<timestamp>.html`

---

## 🧩 Risultati del ranking (Top-1 per azienda)

| Company | Best URL | Type | Score | Title | Year |
|----------|-----------|------|--------|--------|------|
| **GLENCORE PLC** | [GLEN-2024-Annual-Report.pdf](https://www.glencore.com/.rest/api/v1/documents/static/7a4295e4-3674-45e9-94c4-7d7fb285faff/GLEN-2024-Annual-Report.pdf) | PDF | 66.0 | 2024 Annual Report (21 MB) | 2024 |
| **STELLANTIS N.V.** | [Semi-Annual Report 2025](https://www.stellantis.com/content/dam/stellantis-corporate/investors/financial-reports/StellantisNV-20250630-Semi-Annual-Report.pdf) | PDF | 66.0 | Semi-Annual Report – June 2025 | 2025 |
| **TOTALENERGIES SE** | [Sustainability & Climate Progress Report 2025](https://totalenergies.com/system/files/documents/totalenergies_sustainability-climate-2025-progress-report_2025_en.pdf) | PDF | 67.0 | Sustainability & Climate 2025 Progress Report | 2025 |
| **VOLKSWAGEN AG** | [Code of Conduct (business partners)](https://www.vwgroupsupply.com/one-kbp-pub/media/shared_media/documents_1/nachhaltigkeit/brochure__volkswagen_group_requirements_regarding_sustainability_in_its_relationships_with_business_partners__code_of_conduct_fo/coc_geschaeftspartner_20230309.pdf) | PDF | 51.0 | Code of Conduct – Business Partners | 2023 |

---

## 🔍 Analisi e prossimi passi

Per un’analisi più approfondita, aprire il file:

> 📄 `data/interim/reports/discovery_report.<timestamp>.html`

Questo report permette di:
- visualizzare **tutte le pagine esplorate e i link candidati** per ciascuna azienda;
- individuare **pattern ricorrenti** (es. pagine “/sustainability/” che portano ai PDF);
- identificare possibili **miglioramenti del crawler**, come:
  - estendere le keyword multilingua (es. “nachhaltigkeit”, “durabilité”, “informe de sostenibilidad”),
  - aumentare `max-depth` o `render-budget` per siti molto dinamici,
  - introdurre un ranking basato sul **peso del testo vicino al link** (contesto semantico).

---

📘 *Questa fase rappresenta la base del dataset ESG che verrà poi utilizzato per le fasi successive di parsing, normalizzazione e analisi automatica dei report aziendali.*
