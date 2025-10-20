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
   - Output: `data/interim/reports/discovery_report.run1.html`.

---
###################################
###################################
###################################
###################################
## ⚙️ Parametri della run numero 1

Esecuzione del crawler (`scraper.run_discovery`) con i seguenti parametri:

| Parametro | Valore | Significato |
|------------|---------|-------------|
| `--limit` | **10** | Numero massimo di aziende processate in questa run |
| `--max-depth` | **3** | Profondità massima di esplorazione per dominio |
| `--max-pages` | **200** | Numero massimo di pagine visitabili per azienda |
| `--render-budget` | **0** | Numero massimo di pagine da renderizzare via Playwright (per siti dinamici) |
| `--disable-render` | **False** | Il rendering JS è attivo, ma limitato dal budget |
| `--allow-external-pdfs` | **True** | Permette di seguire link PDF anche esterni al dominio principale |
---

## 🔍 Analisi e prossimi passi

Per un’analisi più approfondita, aprire il file:

> 📄 `data/interim/reports/discovery_report.run2.html`

Questo report permette di:
- visualizzare **tutte le pagine esplorate e i link candidati** per ciascuna azienda;
- individuare **pattern ricorrenti** (es. pagine “/sustainability/” che portano ai PDF);
- identificare possibili **miglioramenti del crawler**, come:
  - estendere le keyword multilingua (es. “nachhaltigkeit”, “durabilité”, “informe de sostenibilidad”),
  - aumentare `max-depth` o `render-budget` per siti molto dinamici,
  - introdurre un ranking basato sul **peso del testo vicino al link** (contesto semantico).

---

📘 *Questa fase rappresenta la base del dataset ESG che verrà poi utilizzato per le fasi successive di parsing, normalizzazione e analisi automatica dei report aziendali.*

## 🧩 Risultati del ranking (Top-1 per azienda)

| Company | Best URL | Type | Score | Title | Year |
|----------|-----------|------|--------|--------|------|
| **GLENCORE PLC** | [GLEN-2024-Annual-Report.pdf](https://www.glencore.com/.rest/api/v1/documents/static/7a4295e4-3674-45e9-94c4-7d7fb285faff/GLEN-2024-Annual-Report.pdf) | PDF | 66.0 | 2024 Annual Report (21 MB) | 2024 |
| **STELLANTIS N.V.** | [Semi-Annual Report 2025](https://www.stellantis.com/content/dam/stellantis-corporate/investors/financial-reports/StellantisNV-20250630-Semi-Annual-Report.pdf) | PDF | 66.0 | Semi-Annual Report – June 2025 | 2025 |
| **TOTALENERGIES SE** | [Sustainability & Climate Progress Report 2025](https://totalenergies.com/system/files/documents/totalenergies_sustainability-climate-2025-progress-report_2025_en.pdf) | PDF | 67.0 | Sustainability & Climate 2025 Progress Report | 2025 |
| **VOLKSWAGEN AG** | [Code of Conduct (business partners)](https://www.vwgroupsupply.com/one-kbp-pub/media/shared_media/documents_1/nachhaltigkeit/brochure__volkswagen_group_requirements_regarding_sustainability_in_its_relationships_with_business_partners__code_of_conduct_fo/coc_geschaeftspartner_20230309.pdf) | PDF | 51.0 | Code of Conduct – Business Partners | 2023 |

---
###################################
###################################
###################################
###################################
## ⚙️ Parametri della run numero 2

Esecuzione del crawler (`scraper.run_discovery`) con i seguenti parametri:

| Parametro | Valore | Significato |
|------------|---------|-------------|
| `--limit` | **50** | Numero massimo di aziende processate in questa run |
| `--max-depth` | **3** | Profondità massima di esplorazione per dominio |
| `--max-pages` | **200** | Numero massimo di pagine visitabili per azienda |
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
| **AXA SOCIETE ANONYME** | [Politique d’investissement ESG](https://www.axa.fr/content/dam/axa/desktop/Documents/PDF-BANQUE/politique-d-investissement-ESG.pdf) | PDF | 53.0 | Politique d’investissement ESG | — |
| **BP PLC** | [Annual Report 2024](https://www.bp.com/content/dam/bp/business-sites/en/global/corporate/pdfs/investors/bp-annual-report-and-form-20f-2024.pdf) | PDF | 67.0 | Annual Report 2024 (3.2 MB) | 2024 |
| **CHRISTIAN DIOR** | [Availability of the 2024 Annual Report](https://www.dior-finance.com/pdf/d/2/1056/Christian%20Dior%20SE%20-%20Availability%20of%20the%202024%20annual%20report.pdf) | PDF | 65.0 | Notice of availability of the 2024 annual financial report | 2020 |
| **DEUTSCHE POST AG** | [Dialogmarketing Monitor 2023](https://www.dpdhl.com/dam/dpdhl/de/media-center/media-relations/documents/2023/dp-dialogmarketing-monitor-2023.pdf) | PDF | 41.0 | Dialogmarketing Monitor 2023 – Werbung mit Dialogelementen | 2023 |
| **DEUTSCHE TELEKOM AG** | [Annual Financial Statements 2024](https://www.telekom.com/resource/blob/1086762/8b28ad81f040852828db2627f35c6432/dl-deutsche-telekom-ag-annual-financial-statements-as-of-december-31-2024-data.pdf) | PDF | 66.0 | Annual Financial Statements as of Dec 31 2024 (2.1 MB) | 2024 |
| **ENI S.P.A.** | [Sustainability Local Report Côte d’Ivoire 2024](https://www.eni.com/content/dam/enicom/documents/eng/sustainability/2024/Eni-Local-Report-Cote-d-ivoire-2024.pdf) | PDF | 66.0 | Sustainability Local Report Côte d’Ivoire 2024 (English version) | 2024 |
| **EQUINOR ASA** | [Annual Report 2024](https://cdn.equinor.com/files/h61q9gi9/global/16ccbc5a098c3b971979118420c4f83ddee18fb4.pdf?annual-report-2024-equinor.pdf=) | PDF | 66.0 | 2024 Annual Report | 1979 |
| **GLENCORE PLC** | [GLEN 2024 Annual Report](https://www.glencore.com/.rest/api/v1/documents/static/7a4295e4-3674-45e9-94c4-7d7fb285faff/GLEN-2024-Annual-Report.pdf) | PDF | 66.0 | 2024 Annual Report (21 MB) | 2024 |
| **KONINKLIJKE AHOLD DELHAIZE N.V.** | [Annual Report 2024](https://www.aholddelhaize.com/media/wcqil04n/ad_annual-report_2024_interactive.pdf) | PDF | 66.0 | Ahold Delhaize’s 2024 Annual Report | 2024 |
| **STELLANTIS N.V.** | [Semi-Annual Report 2025](https://www.stellantis.com/content/dam/stellantis-corporate/investors/financial-reports/StellantisNV-20250630-Semi-Annual-Report.pdf) | PDF | 66.0 | Semi-Annual Report (June 2025) | 2025 |
| **TOTALENERGIES SE** | [Sustainability & Climate Progress Report 2025](https://totalenergies.com/system/files/documents/totalenergies_sustainability-climate-2025-progress-report_2025_en.pdf) | PDF | 67.0 | Sustainability and Climate 2025 Progress Report | 2025 |
| **VOLKSWAGEN AG** | [Code of Conduct (Business Partners)](https://www.vwgroupsupply.com/one-kbp-pub/media/shared_media/documents_1/nachhaltigkeit/brochure__volkswagen_group_requirements_regarding_sustainability_in_its_relationships_with_business_partners__code_of_conduct_fo/coc_geschaeftspartner_20230309.pdf) | PDF | 51.0 | Code of Conduct – Business Partners | 2023 |


## 🔍 Analisi e prossimi passi

Per un’analisi più approfondita, aprire il file:

> 📄 `data/interim/reports/discovery_report.<timestamp>.html`

###################################
###################################
###################################
###################################
## ⚙️ Parametri della run numero 3

Esecuzione del crawler (`scraper.run_discovery`) con i seguenti parametri:

| Parametro | Valore | Significato |
|------------|---------|-------------|
| `--limit` | **600** | Numero massimo di aziende processate in questa run |
| `--max-depth` | **3** | Profondità massima di esplorazione per dominio |
| `--max-pages` | **200** | Numero massimo di pagine visitabili per azienda |
| `--render-budget` | **80** | Numero massimo di pagine da renderizzare via Playwright (per siti dinamici) |
| `--disable-render` | **False** | Il rendering JS è attivo, ma limitato dal budget |
| `--allow-external-pdfs` | **True** | Permette di seguire link PDF anche esterni al dominio principale |

📂 Output generato:
- `data/interim/pages_visited.jsonl`
- `data/interim/candidates_raw.jsonl`
- `data/output/candidates_best.csv`
- `data/interim/reports/discovery_report.<timestamp>.html`

---

## 🧩 Risultati del ranking (Top-1 per azienda)

| Company                                           | Best URL                                                                                                                                                                                                                                                                                                                                         | Type    | Score | Title                                                                                 | Year |
| ------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------- | ----- | ------------------------------------------------------------------------------------- | ---- |
| **A P MOLLER-MAERSK A/S**                         | [Logistics can help chemical companies reduce GHG emissions – Infographic](https://www.maersk.com/~/media_sc9/maersk/insights/sustainability/2024/files/logistics-can-help-chemical-companies-reduce-ghg-emissions-infographic.pdf)                                                                                                              | PDF     | 50.0  | Logistics can help chemical companies reduce GHG emissions – Infographic (PDF 5009KB) | 2024 |
| **AB VOLVO**                                      | [Volvo Group – Annual Report 2024](https://www.volvogroup.com/content/dam/volvo-group/markets/master/events/2025/annual-reports/volvo-group-annual-report-2024.pdf)                                                                                                                                                                              | PDF     | 66.0  | Volvo Group – Annual Report 2024 (PDF, 10 MB)                                         | 2025 |
| **ABB LTD**                                       | [RV HS2025 Overview](https://uzh.ch/dam/jcr:195d35ab-3ff1-4bb0-8271-e0176e465504/RV_HS2025_Overview.pdf)                                                                                                                                                                                                                                         | PDF     | 40.0  | More about  Overview Ringvorlesungen Fall 2025                                        | 2025 |
| **ABN AMRO BANK NV**                              | [ESG & Economie – EU: weg naar 2030…](https://www.abnamro.com/research/nl/onze-research/esg-en-economie-eu-weg-naar-2030-klimaatdoelen-nog-steeds-hobbelig)                                                                                                                                                                                      | Unknown | 41.0  | ESG & Economie - EU: weg naar 2030…                                                   | 2030 |
| **ACCIONA SA**                                    | [Climate emergency (overview page)](https://www.acciona.com/our-purpose/sustainability/climate-emergency)                                                                                                                                                                                                                                        | Unknown | 28.0  | GET TO KNOW WHAT WE DO                                                                | —    |
| **AIRBUS SE**                                     | [Digital Accessibility Scheme 2024–2026](https://www.airbus.com/sites/g/files/jlcbta136/files/2024-07/Airbus_Multiannual_Digital_Accessibility_Scheme_2024-2026.pdf)                                                                                                                                                                             | PDF     | 65.0  | Airbus Multiannual Digital Accessibility Scheme 2024-2026                             | 2024 |
| **AKZO NOBEL NV**                                 | [Annual Report 2024](https://www.akzonobel.com/content/dam/akzonobel-corporate/global/en/investor-relations-images/result-center/archive-annual-reports/2029-2020/AkzoNobel-annual-report-2024.pdf)                                                                                                                                              | PDF     | 51.0  | INTERACTIVE PDF                                                                       | 2029 |
| **ANDRITZ AG**                                    | [Non-financial statement 2024 (CSRD/ESRS)](https://www.andritz.com/resource/blob/620974/e66acf15517f7bab56818dbc86194b84/andritz-non-financial-statement-2024-e-data.pdf)                                                                                                                                                                        | PDF     | 61.0  | Download non-financial report according to CSRD/ESRS                                  | 2024 |
| **ANHEUSER-BUSCH INBEV SA/NV**                    | [ESG Report 2021](https://www.ab-inbev.com/assets/presentations/ESG/ABINBEV_ESG%202021_Final.pdf)                                                                                                                                                                                                                                                | PDF     | 56.0  | 2021 Environmental, Social & Governance Report                                        | 2020 |
| **ASHTEAD GROUP PLC**                             | [Annual Report 2023](https://www.ashtead-group.com/files/downloads/reports/Ashtead-Group-Annual-Report-FY2023.pdf)                                                                                                                                                                                                                               | PDF     | 66.0  | 2023 Annual Report (PDF 7.9MB)                                                        | 2023 |
| **ASML HOLDING N.V.**                             | [2024 Annual Report (US GAAP)](https://ourbrand.asml.com/m/79d325b168e0fd7e/original/2024-Annual-Report-based-on-US-GAAP.pdf)                                                                                                                                                                                                                    | PDF     | 66.0  | 2024 Annual Report based on US GAAP                                                   | 2024 |
| **ASSICURAZIONI GENERALI SPA**                    | [Annual Integrated Report 2024](https://www.generali.com/doc/jcr:259c5d6e-46f7-4a43-9512-58e5dcbd2a56/Annual%20Integrated%20Report%20and%20Consolidated%20Financial%20Statements%202024_Generali%20Group_final_interactive.pdf/lang:en/Annual_Integrated_Report_and_Consolidated_Financial_Statements_2024_Generali_Group_final_interactive.pdf) | PDF     | 67.0  | Annual Integrated Report and Consolidated Financial Statements 2024                   | 2020 |
| **ASTRAZENECA PLC**                               | [UK Carbon Reduction Plan (Apr 2025)](https://www.astrazeneca.co.uk/content/dam/intelligentcontent/unbranded/astrazeneca/uk/en/pdf/sustainability/Carbon-Reduction-Plan-April-2025.pdf.coredownload.pdf)                                                                                                                                         | PDF     | 56.0  | UK Carbon Reduction Plan April 2025                                                   | 2025 |
| **ATLAS COPCO AB**                                | [SHEQ Policy (ESLA)](https://www.atlascopco.com/content/dam/atlas-copco/local-countries/peru/documents/4-Sustainability-SHEQ-Policy-Poster-ESLA%20%283%29%20%281%29.pdf)                                                                                                                                                                         | PDF     | 43.0  | Política de Seguridad, Salud, Medio Ambiente y Calidad                                | —    |
| **AURUBIS AG**                                    | [Annual Report 2022/23 (excerpt)](https://www.aurubis.com/en/dam/jcr:67c948b2-870c-4fa3-b0df-34b62359019b/Aurubis_Annual%20Report_FY%202022_23%20%28Meeting%20participation%29.pdf)                                                                                                                                                              | PDF     | 66.0  | Meeting participation (excerpt from Annual Report 2022/23)                            | 2020 |
| **AVIVA PLC**                                     | [EMTN Prospectus (2016)](https://www.aviva.com/content/dam/aviva-corporate/documents/investors/pdfs/credit-investors/Final-2016-Aviva-EMTN-Prospectus-%2822-04-2016%29.pdf)                                                                                                                                                                      | PDF     | 36.0  | Prospectus                                                                            | 2016 |
| **AXA SOCIETE ANONYME**                           | [Politique d’investissement ESG](https://www.axa.fr/content/dam/axa/desktop/Documents/PDF-BANQUE/politique-d-investissement-ESG.pdf)                                                                                                                                                                                                             | PDF     | 53.0  | Politique d’investissement ESG                                                        | —    |
| **BALFOUR BEATTY PLC**                            | [Annual Report & Accounts 2024](https://www.balfourbeatty.com/media/yx0pn423/balfour-beatty-annual-report-and-accounts-2024.pdf)                                                                                                                                                                                                                 | PDF     | 67.0  | Download our 2024 Annual Report and Accounts                                          | 2024 |
| **BANCO BILBAO VIZCAYA ARGENTARIA SA**            | [Sustainability Policy](https://shareholdersandinvestors.bbva.com/wp-content/uploads/2022/09/2022-09-Sustainability-General-Policy-BBVA_ENG.pdf)                                                                                                                                                                                                 | PDF     | 61.0  | BBVA's Sustainability Policy                                                          | 2022 |
| **BARCLAYS PLC**                                  | [Barclays Bank UK – Annual Report 2024](https://home.barclays/content/dam/home-barclays/documents/investor-relations/reports-and-events/annual-reports/2024/Barclays-Bank-UK-Annual-Report-2024.pdf)                                                                                                                                             | PDF     | 66.0  | Barclays Bank UK PLC Annual Report 2024 (PDF 2.4MB)                                   | 2024 |
| **BARRY CALLEBAUT AG**                            | [ESG Datasheet 2023/24](https://www.barry-callebaut.com/system/files/2024-11/ESG%20Datasheet%202023_24_0.pdf)                                                                                                                                                                                                                                    | PDF     | 66.0  | ESG Datasheet 2023/24                                                                 | 2024 |
| **BAYER AG**                                      | [EMAS news (club)](https://www.bayer04.de/de-de/news/nachhaltigkeit/bayer-04-fuehrt-umweltmanagementsystem-nach-eu-standard-emas-ein)                                                                                                                                                                                                            | Unknown | 8.0   | Bayer 04 führt Umweltmanagementsystem nach EMAS ein                                   | 2025 |
| **BOUYGUES**                                      | [Conditions Particulières – Sept 2024](https://www.bouyguestelecom.fr/static/cms/tarifs/Conditions_Particulieres_Cedant_Utilisateur_2024.pdf)                                                                                                                                                                                                    | PDF     | 40.0  | Conditions Particulières Utilisateurs – Septembre 2024                                | 2024 |
| **BP PLC**                                        | [Annual Report 2024](https://www.bp.com/content/dam/bp/business-sites/en/global/corporate/pdfs/investors/bp-annual-report-and-form-20f-2024.pdf)                                                                                                                                                                                                 | PDF     | 67.0  | Annual Report 2024 (pdf / 3.2 MB)                                                     | 2024 |
| **BRENNTAG SE**                                   | [Sustainability (overview)](https://corporate.brenntag.com/en/sustainability)                                                                                                                                                                                                                                                                    | Unknown | 28.0  | Sustainability                                                                        | —    |
| **CAIXABANK, S.A.**                               | [Informe Comisión de Riesgos 2024](https://www.caixabank.com/deployedfiles/caixabank_com/Estaticos/PDFs/Accionistasinversores/Gobierno_Corporativo/JGA/2025/Informe_Actividades_Comision_Riesgos_2024_ES.pdf)                                                                                                                                    | PDF     | 61.0  | Informe de funcionamiento de la Comisión de Riesgos                                   | 2025 |
| **CHRISTIAN DIOR**                                | [Availability of the 2024 annual report](https://www.dior-finance.com/pdf/d/2/1056/Christian%20Dior%20SE%20-%20Availability%20of%20the%202024%20annual%20report.pdf)                                                                                                                                                                             | PDF     | 65.0  | Notice of availability of the 2024 annual financial report                            | 2020 |
| **COMMERZBANK AG**                                | [Sustainability indicators for crypto-assets](https://www.commerzbank.com/ms/documents/en/regulatory-disclosure-sustainability-indicators-for-crypto-assets.pdf)                                                                                                                                                                                 | PDF     | 51.0  | Sustainability indicators for crypto-assets                                           | —    |
| **COMPAGNIE FINANCIERE RICHEMONT SA**             | [Non-Financial Report 2024](https://www.richemont.com/media/tjbjiob5/richemont-non-financial-report-2024.pdf)                                                                                                                                                                                                                                    | PDF     | 66.0  | Non-Financial Report 2024 (5.7 MB)                                                    | 2024 |
| **COMPUTACENTER PLC**                             | [Sustainability Report 2024](https://www.computacenter.com/docs/default-source/documents/sustainability-report-2024_final_links.pdf?sfvrsn=283e604b_3)                                                                                                                                                                                           | PDF     | 61.0  | Download our latest Sustainability Report                                             | 2024 |
| **D'IETEREN GROUP SA/NV**                         | [Stratégie ESG 2024 (FR)](https://www.dieteren.be/media/z4kjizcm/dieteren-esg-strategy-mobility-in-motion-fr-2024.pdf)                                                                                                                                                                                                                           | PDF     | 61.0  | Découvrez notre stratégie ESG                                                         | 2024 |
| **DAIMLER TRUCK HOLDING AG**                      | [ESG Factbook FY 2024](https://www.daimlertruck.com/fileadmin/user_upload/documents/sustainability/daimler-truck-esg-factbook-fy-2024.pdf)                                                                                                                                                                                                       | PDF     | 65.0  | Daimler Truck ESG Factbook for Full Year 2024                                         | 2024 |
| **DANONE**                                        | [Forest annual update 2023](https://www.danone.com/content/dam/corp/global/danonecom/about-us-impact/policies-and-commitments/en/2024/forest-annual-update-2023.pdf)                                                                                                                                                                             | PDF     | 66.0  | Forest annual update 2023                                                             | 2024 |
| **DANSKE BANK A/S**                               | [Udvalgte fonde – månedsinvestering](https://danskebank.dk/-/media/pdf/danske-bank/dk/investeringsprodukter/udvalgte-fonde-til-maanedsinvestering-v1b.pdf?rev=343a19354c224d4fb62168aee8896f35&amp%3Bhash=44271555B62AD761D795C43DB3A5A09A)                                                                                                      | PDF     | 36.0  | Se alle fonde (pdf)                                                                   | 1935 |
| **DCC PLC**                                       | [Annual Report 2023](https://www.dcc.ie/~/media/Files/D/Dcc-Corp-v3/documents/investors/annual-and-sustainability-reports/2023/annual-report-2023.pdf)                                                                                                                                                                                           | PDF     | 66.0  | Annual report 2023                                                                    | 2023 |
| **DEUTSCHE BANK AG**                              | [Inclusion of sustainability risks](https://www.deutsche-bank.de/dam/deutschebank/de/shared/pdf/rechtliche-hinweise/Inclusion-of-sustainability-risks-20210310.pdf)                                                                                                                                                                              | PDF     | 60.0  | Inclusion of sustainability risks…                                                    | 2021 |
| **DEUTSCHE POST AG**                              | [Dialogmarketing Monitor 2023](https://www.dpdhl.com/dam/dpdhl/de/media-center/media-relations/documents/2023/dp-dialogmarketing-monitor-2023.pdf)                                                                                                                                                                                               | PDF     | 41.0  | Dialogmarketing Monitor 2023                                                          | 2023 |
| **DEUTSCHE TELEKOM AG**                           | [Annual Financial Statements 2024](https://www.telekom.com/resource/blob/1086762/8b28ad81f040852828db2627f35c6432/dl-deutsche-telekom-ag-annual-financial-statements-as-of-december-31-2024-data.pdf)                                                                                                                                            | PDF     | 66.0  | Annual Financial Statements as of Dec 31, 2024 (2.1 MB)                               | 2024 |
| **DIAGEO PLC**                                    | [Annual Report 2025](https://www.diageo.com/pdf-viewer.aspx?gid=307336661&src=%2F~%2Fmedia%2FFiles%2FD%2FDiageo-V2%2FDiageo-Corp%2Finvestors%2Fresults-reports-and-events%2Fannual-reports%2F2025%2Fannual-report-2025.pdf)                                                                                                                      | PDF     | 66.0  | Annual Report 2025                                                                    | 2025 |
| **DS SMITH LIMITED**                              | [Annual Report 2024](https://www.dssmith.com/globalassets/investors/annual-reports-archive/ds-smith-annual-report-2024.pdf)                                                                                                                                                                                                                      | PDF     | 66.0  | DS Smith Annual Report 2024                                                           | 2024 |
| **EIFFAGE**                                       | [Sustainable development charter](https://www.eiffage.com/files/live/sites/eiffagev2/files/Transition%20%c3%a9cologique/La%20biodiversit%c3%a9%20au%20coeur%20de%20nos%20m%c3%a9tiers/Sustainable%20Development%20charter%20%281%29.pdf)                                                                                                         | PDF     | 52.0  | Sustainable development                                                               | —    |
| **ENEL SPA**                                      | [Samsung TV UHD – doc](https://www.enel.it/content/dam/asset/documenti/elettrodomestici/tv-43-8000f/SAMSUNG%20TV%20Crystal%20UHD%204K%2043%27%27%20U8000F.pdf)                                                                                                                                                                                   | PDF     | 36.0  | Download                                                                              | 2043 |
| **ENGIE**                                         | [ESG at ENGIE (2024)](https://www.engie.com/sites/default/files/assets/documents/2025-07/Engie%20-2024%20ESG%20at%20ENGIE%20VDEF.pdf)                                                                                                                                                                                                            | PDF     | 65.0  | 2024 - ESG at ENGIE                                                                   | 2025 |
| **ENI S.P.A.**                                    | [Local Report Côte d’Ivoire 2024](https://www.eni.com/content/dam/enicom/documents/eng/sustainability/2024/Eni-Local-Report-Cote-d-ivoire-2024.pdf)                                                                                                                                                                                              | PDF     | 66.0  | Sustainability Local Report Côte d'Ivoire 2024                                        | 2024 |
| **EQUINOR ASA**                                   | [Annual Report 2024](https://cdn.equinor.com/files/h61q9gi9/global/16ccbc5a098c3b971979118420c4f83ddee18fb4.pdf?annual-report-2024-equinor.pdf=)                                                                                                                                                                                                 | PDF     | 66.0  | 2024 Annual report                                                                    | 1979 |
| **ESSILORLUXOTTICA**                              | [2024 Annual Report (page)](https://www.essilorluxottica.com/en/2024-annual-report)                                                                                                                                                                                                                                                              | Unknown | 39.0  | 2024 Annual Report                                                                    | 2024 |
| **FERROVIAL S.E.**                                | [Annual Corporate Governance Report 2015](https://static.ferrovial.com/wp-content/uploads/2019/11/13133148/annual-corporate-governance-report-2015.pdf)                                                                                                                                                                                          | PDF     | 65.0  | 2015 – Annual Corporate Governance Report                                             | 2019 |
| **FORVIA SE**                                     | [Sustainability Report 2023](https://forvia.com/sites/default/files/2024-03/FORVIA_%202023_Sustainability%20Report_WEB.pdf)                                                                                                                                                                                                                      | PDF     | 61.0  | Download our Sustainability Report                                                    | 2024 |
| **GALP ENERGIA SGPS S.A.**                        | [Annual Integrated Report 2024](https://www.galp.com/corp/Portals/0/Recursos/Investidores/IMR2024/AnnualIntegratedReport2024.pdf)                                                                                                                                                                                                                | PDF     | 54.0  | Relatório Anual Integrado 2024                                                        | 2024 |
| **GLENCORE PLC**                                  | [GLEN 2024 Annual Report](https://www.glencore.com/.rest/api/v1/documents/static/7a4295e4-3674-45e9-94c4-7d7fb285faff/GLEN-2024-Annual-Report.pdf)                                                                                                                                                                                               | PDF     | 66.0  | 2024 Annual Report (21 MB)                                                            | 2024 |
| **HEINEKEN HOLDING N.V.**                         | [Annual Report 2023](https://www.heinekenholding.com/sites/heinekenholding-v2/files/heineken-holding/investors/results-reports-webcasts-presentations/heineken-holding-nv-annual-report-2023-final.pdf)                                                                                                                                          | PDF     | 66.0  | heineken-holding-nv-annual-report-2023-final.pdf                                      | 2023 |
| **HENKEL AG & CO. KGAA**                          | [Annual Report 2024](https://www.henkel.de/resource/blob/2043318/9b8425a944b077ab7165b775398c72a1/data/2024-annual-report.pdf)                                                                                                                                                                                                                   | PDF     | 66.0  | Annual Report 2024 (8,24 MB)                                                          | 2043 |
| **HOLCIM AG**                                     | [Sustainability ambitions 2024 (IT)](https://www.holcim.ch/sites/switzerland/files/docs/holcim-2024-sustainability-ambitions-italiano-1_0.pdf)                                                                                                                                                                                                   | PDF     | 65.0  | holcim-2024-sustainability-ambitions-italiano                                         | 2024 |
| **HSBC HOLDINGS PLC**                             | [Annual Report & Accounts 2024](https://www.hsbc.com/-/files/hsbc/investors/hsbc-results/2024/annual/pdfs/hsbc-holdings-plc/250219-annual-report-and-accounts-2024.pdf)                                                                                                                                                                          | PDF     | 66.0  | 2024 Annual Report (PDF, 8.34MB)                                                      | 2024 |
| **ING GROEP NV**                                  | [Viewpoint – Sustainability due diligence](https://www.ing.com/MediaEditPage/ING-Viewpoint-Sustainability-due-diligence-the-case-for-an-EU-approach.htm)                                                                                                                                                                                         | PDF     | 43.0  | —                                                                                     | —    |
| **INTERNATIONAL CONSOLIDATED AIRLINES GROUP S.A** | [Freight Forwarder Declaration](https://marketing.iagcargo.com/l/533642/2024-09-23/gg8c5l/533642/1727099222UA0jS61k/EBRDeclaration.pdf)                                                                                                                                                                                                          | PDF     | 36.0  | Freight Forwarder Declaration                                                         | 2024 |
| **INTERNATIONAL DISTRIBUTION SERVICES PLC**       | [AGM Proxy Form 2024](https://www.internationaldistributionservices.com/media/12341/2024-annual-general-meeting-proxy-form.pdf)                                                                                                                                                                                                                  | PDF     | 66.0  | 2024 Annual General Meeting Proxy Form                                                | 2024 |
| **INTESA SANPAOLO S.P.A.**                        | [Relazione remunerazione 2025](https://www.intesasanpaoloprivatebanking.it/bin/paginaGenerica/308/C_32_paginaGenerica_1_7_4_objAllegato.pdf)                                                                                                                                                                                                     | PDF     | 32.0  | Relazione sulla politica… 2025                                                        | 2025 |
| **ISS A/S**                                       | [Annual Report 2024](https://brand.issworld.com/m/1bb99b2c33c4d7cf/original/ISS-Annual-Report-2024.pdf)                                                                                                                                                                                                                                          | PDF     | 67.0  | Annual Report 2024                                                                    | 2024 |
| **IVECO GROUP NV**                                | [Note Privacy](https://www.iveco.com/global/-/media/IVECOdotcom/global/metallica/GDPR_PN_Metallica_IT.pdf?rev=77aec45e5b394cd192760341a172b356)                                                                                                                                                                                                  | PDF     | 36.0  | Note Privacy                                                                          | 1927 |
| **KBC GROEP NV/ KBC GROUPE SA**                   | [Sustainability-related disclosures](https://www.kbcbrussels.be/particuliers/fr/informations-legales/documentation-investissements/Sustainability-related-disclosures.html)                                                                                                                                                                      | HTML    | 18.0  | [www.kbc.be/fr/SRD](http://www.kbc.be/fr/SRD)                                         | —    |
| **KERING**                                        | [Articles of association (Apr 2025)](https://www.kering.com/api/download-file?path=KERING_Articles_of_association_April_24_2025_27a58479b7.pdf)                                                                                                                                                                                                  | PDF     | 36.0  | See more                                                                              | 2025 |
| **KESKO OYJ**                                     | [Green Bond Annual Review 2024](https://www.kesko.fi/4978a1/globalassets/03-sijoittaja/kesko-sijoituskohteena/kesko-green-bond-annual-review-2024.pdf)                                                                                                                                                                                           | PDF     | 66.0  | Kesko Green Bond Annual Review 2024                                                   | 2024 |
| **KION GROUP AG**                                 | [Supplementary Non-financial Insights 2024](https://www.kiongroup.com/KION-Website-Main/About-us/Sustainability/Insights/KION-Supplementary-Non-financial-Insights-2024.pdf)                                                                                                                                                                     | PDF     | 66.0  | Supplementary Non-financial Insights 2024                                             | 2024 |
| **KONE OYJ**                                      | [Sustainability Report 2023](https://www.kone.com/en/Images/KONE%20Sustainability%20Report%202023_tcm17-129519.pdf)                                                                                                                                                                                                                              | PDF     | 66.0  | KONE Sustainability Report 2023                                                       | 2020 |
| **KONINKLIJKE AHOLD DELHAIZE N.V.**               | [Annual Report 2024](https://www.aholddelhaize.com/media/wcqil04n/ad_annual-report_2024_interactive.pdf)                                                                                                                                                                                                                                         | PDF     | 66.0  | Ahold Delhaize's 2024 Annual Report                                                   | 2024 |
| **KONINKLIJKE PHILIPS N.V.**                      | [Philips Foundation – 2024 Annual Report (press)](https://www.philips.com/a-w/about/news/archive/standard/news/press/2025/philips-foundation-2024-annual-report-expanding-access-to-quality-healthcare-for-46-5-million-people.html)                                                                                                             | HTML    | 39.0  | Philips Foundation 2024 Annual Report…                                                | 2025 |
| **L'AIR LIQUIDE S.A.**                            | [Integrated Annual Report 2024](https://www.airliquide.com/sites/airliquide.com/files/2025-03/2024-integrated-annual-report.pdf)                                                                                                                                                                                                                 | PDF     | 67.0  | 2024 Integrated Annual Report                                                         | 2025 |
| **L'OREAL**                                       | [Annual Report 2024](https://www.loreal.com/-/media/project/loreal/brand-sites/corp/master/lcorp/2-group/annual-reports/loreal2024annualreport.pdf?rev=60be90d75fdb49aba373457b5c0d720e)                                                                                                                                                         | PDF     | 65.0  | 2024 Annual Report                                                                    | 2024 |
| **LEGRAND**                                       | [Annual Report 2010](https://www.legrand.com/sites/default/files/Documents_PDF_Legrand/Finance/2010/autres/Annual_Rapport_2010_Legrand.pdf)                                                                                                                                                                                                      | PDF     | 56.0  | Access to the anual report 2010                                                       | 2010 |
| **LONDON STOCK EXCHANGE GROUP PLC**               | [Annual Report 2019](https://www.lseg.com/content/dam/lseg/en_us/documents/investor-relations/annual-reports/lseg-annual-report-2019.pdf)                                                                                                                                                                                                        | PDF     | 65.0  | LSEG Annual Report 2019                                                               | 2019 |
| **MERCK KGAA**                                    | [Beiträge und Rechengrößen 2025](https://www.merck-bkk.de/fileadmin/medien/dokumente/downloadcenter/downloads_fuer/arbeitgeber/Rechengroessen_und_Grenzwerte_ab_Juli_2025.pdf)                                                                                                                                                                   | PDF     | 41.0  | Beiträge und Rechengrößen 2025                                                        | 2025 |
| **MTU AERO ENGINES AG**                           | [AGM Invitation 2024](https://www.mtu.de/fileadmin/EN/5_Investors/9_1_Annual_General_Meeting/2024_AGM_A_00_Einladungbeakanntmachung_en.pdf)                                                                                                                                                                                                      | PDF     | 66.0  | Invitation to the AGM 2024 (PDF)                                                      | 2024 |
| **MÜNCHENER RÜCKVERSICHERUNGSGESELLSCHAFT AG**    | [Annual Report 2024](https://www.munichre.com/content/dam/munichre/mrwebsiteslaunches/2024-annual-report/MunichRe-Group-Annual-Report-2024-en.pdf/_jcr_content/renditions/original./MunichRe-Group-Annual-Report-2024-en.pdf)                                                                                                                    | PDF     | 66.0  | Annual Report 2024                                                                    | 2024 |
| **NATURGY ENERGY GROUP, S.A.**                    | [Fixed Income Annual Report 2024](https://www.bolsasymercados.es/bme-exchange/docs/docsSubidos/Listing/Fixed-Income-Annual-Report-2024.pdf)                                                                                                                                                                                                      | PDF     | 60.0  | Annual Report                                                                         | 2024 |
| **NN GROUP NV**                                   | [Annual Report 2024](https://www.nn-group.com/site/binaries/content/assets/nn-group/annual-reports/2024/nn-group-annual-report-2024.pdf)                                                                                                                                                                                                         | PDF     | 66.0  | NN Group Annual Report 2024                                                           | 2024 |
| **NOKIA OYJ**                                     | [Modern slavery statement 2024](https://www.nokia.com/sites/default/files/2025-05/nokia-modern-slavery-statement-2024.pdf)                                                                                                                                                                                                                       | PDF     | 37.0  | Modern slavery statement                                                              | 2025 |
| **NORDEA BANK ABP**                               | [Annual Report 2015 (Transferor)](https://www.nordea.com/en/doc/nordea-bank-annual-report-2015.pdf)                                                                                                                                                                                                                                              | PDF     | 66.0  | Appendix 5 – Annual report 2015                                                       | 2015 |
| **NOVO NORDISK A/S**                              | [Annual Report 2023 (published 2024)](https://www.novonordisk.com/content/dam/nncorp/global/en/investors/irmaterial/annual_report/2024/novo-nordisk-annual-report-2023.pdf)                                                                                                                                                                      | PDF     | 65.0  | Annual Report 2023 (PDF)                                                              | 2024 |
| **ORANGE**                                        | [New Orange vessel… (news 2023)](https://www.orange.com/en/news/2023/new-orange-vessel-provide-efficient-and-sustainable-global-connectivity)                                                                                                                                                                                                    | Unknown | 40.0  | A new Orange vessel…                                                                  | 2023 |
| **PERNOD RICARD**                                 | [Sustainability-Linked Financing Framework (Sep 2023)](https://www.pernod-ricard.com/sites/default/files/inline-files/Sustainability-Linked%20Financing%20Framework%20%28September%202023%29.pdf)                                                                                                                                                | PDF     | 65.0  | Sustainability-Linked Financing Framework                                             | 2020 |
| **PUBLICIS GROUPE S A**                           | [Statutory Auditors’ report FY 2022](https://www.publicisgroupe.com/sites/default/files/investors-document/2023-05/14%20-%20Statutory%20Auditors%27%20report%20on%20the%20annual%20financial%20statements%20-%20Fiscal%20year%202022.pdf)                                                                                                        | PDF     | 49.0  | Statutory Auditors’ report                                                            | 2023 |
| **RENAULT**                                       | [Renault 5 brochure (2025)](https://e-brochure.renault.fr/renault_r5_coty/mobile/m_coty_renault5_fr_july_2025.pdf)                                                                                                                                                                                                                               | PDF     | 35.0  | téléchargez la brochure Renault 5                                                     | 2025 |
| **REPSOL SA.**                                    | [Equity Story ESG Day 2023](https://www.repsol.com/content/dam/repsol-corporate/es/accionistas-e-inversores/equity-story-esg-2023.pdf)                                                                                                                                                                                                           | PDF     | 65.0  | Equity Story ESG Day 2023 (5,83 MB)                                                   | 2023 |
| **RHEINMETALL AG**                                | [Arbeitgeberbroschüre (2025)](https://www.rheinmetall.com/Rheinmetall%20Group/Karriere/Rheinmetall%20als%20Arbeitgeber/2025-05-26_Rheinmetall_KarriereBrosch%C3%BCre.pdf)                                                                                                                                                                        | PDF     | 34.0  | Arbeitgeberbroschüre                                                                  | 2025 |
| **RIO TINTO PLC**                                 | [Business integrity standard](https://www.riotinto.com/-/media/Content/Documents/Sustainability/Corporate-policies/RT-Business-integrity-standard-EN.pdf)                                                                                                                                                                                        | PDF     | 43.0  | Business integrity procedure                                                          | —    |
| **ROCHE HOLDING AG**                              | [Annual Report 2024 (page)](https://www.roche.com/investors/annualreport24)                                                                                                                                                                                                                                                                      | Unknown | 34.0  | Annual Report 2024                                                                    | 2024 |
| **RYANAIR HOLDINGS PLC**                          | [DCC class action documents](https://www.ryanair.com/content/dam/ryanair3/2021/legal/DCC-class-action-publication-documents.pdf)                                                                                                                                                                                                                 | PDF     | 36.0  | Israeli class action                                                                  | 2021 |
| **SAIPEM SPA**                                    | [Sustainability Report 2024](https://www.saipem.com/sites/default/files/2025-04/2024%20Sustainability%20Report_0.pdf)                                                                                                                                                                                                                            | PDF     | 65.0  | 2024 Sustainability Report                                                            | 2025 |
| **SCHINDLER HOLDING AG**                          | [Sustainability Report 2019](https://group.schindler.com/content/dam/website/group/docs/responsibility/2019-schindler-sustainability-report.pdf/_jcr_content/renditions/original./2019-schindler-sustainability-report.pdf)                                                                                                                      | PDF     | 65.0  | Sustainability Report (2019)                                                          | 2019 |
| **SEB SA**                                        | [ESG Investor Day – roadmap 2024–2030](https://www.groupeseb.com/sites/default/files/2024-12/PR-GroupeSEB-ESG_Investor_Day-12122024%20%281%29.pdf)                                                                                                                                                                                               | PDF     | 65.0  | New 2024-2030 ESG ambition                                                            | 2024 |
| **SOCIETE GENERALE**                              | [Sustainable & Positive Impact Bond Framework (2020)](https://www.societegenerale.com/sites/default/files/documents/Notations%20Financi%C3%A8res/sg_sustainable_and_positive_impact_bond_framework_june_2020.pdf)                                                                                                                                | PDF     | 60.0  | Sustainable and Positive Impact Bond Framework                                        | 2020 |
| **SODEXO**                                        | [Sustainable Food Barometer 2023 (post)](https://www.sodexo.com/blog/our-everyday-stories/reports/sustainable-food-barometer-2023)                                                                                                                                                                                                               | Unknown | 36.0  | Discover the first international sustainable food barometer                           | 2023 |
| **SSE PLC**                                       | [Sustainability Report 2025](https://www.sse.com/media/lifo554i/sse-sustainability-report-2025.pdf)                                                                                                                                                                                                                                              | PDF     | 66.0  | Sustainability Report 2025                                                            | 2025 |
| **STELLANTIS N.V.**                               | [Semi-Annual Report (Jun 2025)](https://www.stellantis.com/content/dam/stellantis-corporate/investors/financial-reports/StellantisNV-20250630-Semi-Annual-Report.pdf)                                                                                                                                                                            | PDF     | 66.0  | Semi-Annual Report (June 2025)                                                        | 2025 |
| **SWISSCOM AG**                                   | [Adverse Sustainability Indicators 2024](https://www.swisscom.ch/content/dam/assets/about/investoren/berichte/documents/2025/adverse-sustainability-indicators-reporting-2024.pdf)                                                                                                                                                               | PDF     | 64.0  | Adverse Sustainability Indicators Reporting 2024                                      | 2025 |
| **TELIA COMPANY AB**                              | [Trend Report Security (Sep 2024)](https://www.teliacompany.com/assets/u5c1v3pt22v8/2x2vEZvhniymYXpCJn7lRP/32f82e3619de369bf54c583abbeacf9c/Buckle_up_-_Telia_Trend_Report_Security_-_September_18th_2024_-_First_edition.pdf)                                                                                                                   | PDF     | 34.0  | Trend Report Security – First edition                                                 | 2024 |
| **THYSSENKRUPP AG**                               | [Annual Report 2023/2024](https://www.thyssenkrupp.com/_binary/UCPthyssenkruppAG/f60f13e3-289d-4041-a535-fdf7ea9210b3/thyssenkrupp-GB_2023-2024_EN_WEB.pdf)                                                                                                                                                                                      | PDF     | 50.0  | Annual Report 2023/2024                                                               | 2023 |
| **TOTALENERGIES SE**                              | [Sustainability & Climate Progress Report 2025](https://totalenergies.com/system/files/documents/totalenergies_sustainability-climate-2025-progress-report_2025_en.pdf)                                                                                                                                                                          | PDF     | 67.0  | Sustainability and Climate 2025 Progress Report                                       | 2025 |
| **UNICREDIT SPA**                                 | [Annual Report 2024](https://www.unicreditgroup.eu/content/dam/unicreditgroup-eu/documents/en/investors/financial-reports/2024/4Q24/2024-Annual-Reports-and-Accounts.pdf)                                                                                                                                                                        | PDF     | 67.0  | Download our 2024 Annual Report                                                       | 2024 |
| **UNIVERSAL MUSIC GROUP N.V.**                    | [Spelvoorwaarden (terms)](https://universalmusic.nl/wp-content/uploads/2020/11/Spelvoorwaarden-UMG-05102020.pdf)                                                                                                                                                                                                                                 | PDF     | 37.0  | Spelvoorwaarden                                                                       | 2020 |
| **VESTAS WIND SYSTEMS A/S**                       | [Annual Report 2024](https://www.vestas.com/content/dam/vestas-com/global/en/investor/reports-and-presentations/financial/2024/fy-2024/Vestas%20Annual%20Report%202024.pdf.coredownload.inline.pdf)                                                                                                                                              | PDF     | 67.0  | Vestas Sustainability Statement 2024 (p. 51+)                                         | 2024 |
| **VINCI**                                         | [Avis de convocation (BALO – 26/03/2025)](https://www.vinci.com/sites/default/files/medias/file/2025/03/VINCI-avis-de-convocation_BALO-26032025.pdf)                                                                                                                                                                                             | PDF     | 41.0  | Avis de convocation publié au BALO le 26 mars 2025                                    | 2025 |
| **VIVENDI SE**                                    | [Annual Results 2023 – press release](https://www.vivendi.com/wp-content/uploads/2024/03/20240307_VIV_PR_Annual-Results-2023-vf.pdf)                                                                                                                                                                                                             | PDF     | 65.0  | Annual Results 2023 (press)                                                           | 2024 |
| **VOLKSWAGEN AG**                                 | [Code of Conduct – Business Partners](https://www.vwgroupsupply.com/one-kbp-pub/media/shared_media/documents_1/nachhaltigkeit/brochure__volkswagen_group_requirements_regarding_sustainability_in_its_relationships_with_business_partners__code_of_conduct_fo/coc_geschaeftspartner_20230309.pdf)                                               | PDF     | 51.0  | Code of Conduct – Business Partners                                                   | 2023 |
| **WENDEL SE**                                     | [Rapport de la performance ESG (2023)](https://wendelgroup.com/performance-esg-2023.pdf)                                                                                                                                                                                                                                                         | PDF     | 66.0  | Rapport de la performance ESG des participations… 2023                                | 2023 |


## 🔍 Analisi e prossimi passi

Per un’analisi più approfondita, aprire il file:

> 📄 `data/interim/reports/discovery_report_run3.html`


Ho fatto controllare a chatgpt se i link portassero davvero a dei report di sostenibilità, ecco i risultati:
Categoria	Conteggio
✅ Giusti (main ESG/integrated report)	37
⚠️ Parziali / ESG-related ma non report principale	circa 28
❌ Sbagliati / irrilevanti o obsoleti	circa 41
Totale link verificati	106

📊 Distribuzione semantica (più realistica per il tuo progetto di tesi)

Tipo di documento trovato	Descrizione	Stima %
Annual / Sustainability / Integrated Report 2023–2025	il vero target del ranking	~35%
Policy, framework, disclosure ESG parziali	contesto utile ma non “main”	~25%
Pagine web overview o report datati / errati	da filtrare o penalizzare	~40%

Sono andato a verificare tramite il file check_results.py varie statistiche descrittive dello score del ranking:
Descriptive statistics for 'score':
count    106.000000
mean      55.037736
std       13.812296
min        8.000000
25%       41.000000
50%       64.500000
75%       66.000000
max       67.000000

Successivamente ho stampato tutte le compagnie con il valore massimo pari a 67 e tramite chatgpt ho verificato che i link fossero giusti, lo sono. Ecco la lista delle aziende:
                                            company_id
11                          ASSICURAZIONI GENERALI SPA 
17                                  BALFOUR BEATTY PLC 
23                                              BP PLC 
58                                             ISS A/S 
67   L'AIR LIQUIDE SOCIETE ANONYME POUR L'ETUDE ET ... 
98                                    TOTALENERGIES SE 
99                                       UNICREDIT SPA 
101                            VESTAS WIND SYSTEMS A/S 

ora faccio creare un file a chatgpt con le sue valutazioni (validation_esg_links_full.csv) con due colonne:

- company

- valutazione

E userò la classificazione moderata, cioè:

✅ giusto → main ESG / Integrated / Annual Report 2023-2025 ufficiale;

⚙️ parziale → documenti ESG-relativi ma non report principale (policy, framework, local report, pagine tematiche);

❌ sbagliato → documenti irrilevanti, obsoleti, o completamente fuori contesto.

In questo caso posso filtrare lo score medio per i file corretti e andare a creare una soglia che ci faccia dire quali sono i report corretti e quali sbagliati.

Tutti quelli con 66 come valore di ranking risultano giusti.

Descriptive statistics for 'score':
count    106.000000
mean      55.037736
std       13.812296
min        8.000000
25%       41.000000
50%       64.500000
75%       66.000000
max       67.000000
Name: score, dtype: float64
                                            company_id  ... valutazione
11                          ASSICURAZIONI GENERALI SPA  ...      giusto
17                                  BALFOUR BEATTY PLC  ...      giusto
23                                              BP PLC  ...      giusto
58                                             ISS A/S  ...      giusto
67   L'AIR LIQUIDE SOCIETE ANONYME POUR L'ETUDE ET ...  ...      giusto
98                                    TOTALENERGIES SE  ...      giusto
99                                       UNICREDIT SPA  ...      giusto
101                            VESTAS WIND SYSTEMS A/S  ...      giusto

[8 rows x 4 columns]
   company_id  ... valutazione
21   BAYER AG  ...   sbagliato

[1 rows x 4 columns]

Valori 'valutazione' per Q1:
['sbagliato' 'sbagliato' 'parziale' 'sbagliato' 'sbagliato' 'sbagliato'
 'parziale' 'sbagliato' 'parziale' 'sbagliato' 'giusto' 'sbagliato'
 'parziale' 'sbagliato' 'parziale' 'sbagliato' 'parziale' 'sbagliato'
 'parziale' 'sbagliato' 'sbagliato' 'sbagliato' 'giusto' 'sbagliato'
 'parziale' 'sbagliato' 'sbagliato' 'parziale']

Descriptive statistics for 'score' when valutazione = 'giusto':
count    39.000000
mean     63.102564
std       7.500157
min      34.000000
25%      65.000000
50%      66.000000
75%      66.000000
max      67.000000
Name: score, dtype: float64

Descriptive statistics for 'score' when valutazione = 'parziale':
count    43.00000
mean     54.00000
std      13.10943
min      18.00000
25%      43.00000
50%      60.00000
75%      65.00000
max      66.00000
Name: score, dtype: float64

Descriptive statistics for 'score' when valutazione = 'sbagliato':
count    24.000000
mean     43.791667
std      14.829404
min       8.000000
25%      36.000000
50%      38.500000
75%      58.250000
max      66.000000
Name: score, dtype: float64

Valori 'valutazione' per Q2:
['parziale' 'giusto' 'giusto' 'parziale' 'parziale' 'parziale' 'parziale'
 'parziale' 'parziale' 'parziale' 'giusto' 'parziale' 'parziale'
 'parziale' 'giusto' 'giusto' 'parziale' 'sbagliato' 'parziale' 'parziale'
 'parziale' 'parziale' 'parziale' 'giusto' 'parziale']

Descriptive statistics for 'score' when valutazione = 'giusto':
count    39.000000
mean     63.102564
std       7.500157
min      34.000000
25%      65.000000
50%      66.000000
75%      66.000000
max      67.000000
Name: score, dtype: float64

Descriptive statistics for 'score' when valutazione = 'parziale':
count    43.00000
mean     54.00000
std      13.10943
min      18.00000
25%      43.00000
50%      60.00000
75%      65.00000
max      66.00000
Name: score, dtype: float64

Descriptive statistics for 'score' when valutazione = 'sbagliato':
count    24.000000
mean     43.791667
std      14.829404
min       8.000000
25%      36.000000
50%      38.500000
75%      58.250000
max      66.000000
Name: score, dtype: float64

Valori 'valutazione' per Q3:
['giusto' 'sbagliato' 'giusto' 'giusto' 'parziale' 'parziale' 'parziale'
 'parziale' 'giusto' 'giusto' 'parziale' 'giusto' 'parziale' 'giusto'
 'giusto' 'parziale' 'parziale' 'giusto' 'sbagliato' 'giusto' 'giusto'
 'giusto' 'parziale' 'giusto' 'sbagliato' 'parziale' 'giusto' 'giusto'
 'giusto' 'giusto' 'sbagliato' 'sbagliato' 'giusto' 'giusto' 'sbagliato'
 'giusto' 'parziale' 'parziale' 'giusto' 'parziale' 'parziale' 'giusto'
 'parziale' 'parziale' 'giusto']

Descriptive statistics for 'score' when valutazione = 'giusto':
count    39.000000
mean     63.102564
std       7.500157
min      34.000000
25%      65.000000
50%      66.000000
75%      66.000000
max      67.000000
Name: score, dtype: float64

Descriptive statistics for 'score' when valutazione = 'parziale':
count    43.00000
mean     54.00000
std      13.10943
min      18.00000
25%      43.00000
50%      60.00000
75%      65.00000
max      66.00000
Name: score, dtype: float64

Descriptive statistics for 'score' when valutazione = 'sbagliato':
count    24.000000
mean     43.791667
std      14.829404
min       8.000000
25%      36.000000
50%      38.500000
75%      58.250000
max      66.000000
Name: score, dtype: float64

Valori 'valutazione' per Q4:
['giusto' 'giusto' 'giusto' 'giusto' 'giusto' 'giusto' 'giusto' 'giusto']

Descriptive statistics for 'score' when valutazione = 'giusto':
count    39.000000
mean     63.102564
std       7.500157
min      34.000000
25%      65.000000
50%      66.000000
75%      66.000000
max      67.000000
Name: score, dtype: float64

Descriptive statistics for 'score' when valutazione = 'parziale':
count    43.00000
mean     54.00000
std      13.10943
min      18.00000
25%      43.00000
50%      60.00000
75%      65.00000
max      66.00000
Name: score, dtype: float64

Descriptive statistics for 'score' when valutazione = 'sbagliato':
count    24.000000
mean     43.791667
std      14.829404
min       8.000000
25%      36.000000
50%      38.500000
75%      58.250000
max      66.000000

Ora ho provato a runnare escludendo tutte le aziende già viste.
Farò gli stessi passaggi per vedere i risultati.

###################################
###################################
###################################
###################################
Run numero 4

Ecco i migliori risultati del rank:

company_id	best_url	type	score	title	suspected_year	render_used	notes
GLENCORE PLC	https://www.glencore.com/.rest/api/v1/documents/static/7a4295e4-3674-45e9-94c4-7d7fb285faff/GLEN-2024-Annual-Report.pdf
	pdf	66.0	2024 Annual Report (21 MB)	2024		
STELLANTIS N.V.	https://www.stellantis.com/content/dam/stellantis-corporate/investors/financial-reports/StellantisNV-20250630-Semi-Annual-Report.pdf
	pdf	66.0	Semi-Annual Report – June 2025	2025		
TOTALENERGIES SE	https://totalenergies.com/system/files/documents/totalenergies_sustainability-climate-2025-progress-report_2025_en.pdf
	pdf	67.0	Sustainability & Climate 2025 Progress Report	2025		
VOLKSWAGEN AG	https://www.vwgroupsupply.com/one-kbp-pub/media/shared_media/documents_1/nachhaltigkeit/brochure__volkswagen_group_requirements_regarding_sustainability_in_its_relationships_with_business_partners__code_of_conduct_fo/coc_geschaeftspartner_20230309.pdf
	pdf	51.0	Code of Conduct – Business Partners	2023		
A P MOLLER-MAERSK A/S	https://www.maersk.com/~/media_sc9/maersk/insights/sustainability/2024/files/logistics-can-help-chemical-companies-reduce-ghg-emissions-infographic.pdf
	pdf	50.0	Logistics can help chemical companies reduce GHG emissions - Infographic PDF (5009KB)	2024		
A2A S.P.A.	https://content.gruppoa2a.it/sites/default/files/2025-09/a2a-blue-finance-addendum-sustainable-finance-framework.pdf
	pdf	65.0	Blue Finance Addendum to Sustainable Finance Framework – Settembre 2025 PDF (312.60 Kb) Scarica Download	2025		
AB SKF	https://www.skfab.se/wp-content/uploads/2023/10/Sarskilt-forsakringsvillkor-Olycksfallsforsakring-2024-2.pdf
	pdf	41.0	Villkor Olycksfallsförsäkring SKFAB 2024	2023		
AB VOLVO	https://www.volvogroup.com/content/dam/volvo-group/markets/master/events/2025/annual-reports/volvo-group-annual-report-2024.pdf
	pdf	66.0	Volvo Group – Annual Report 2024 (PDF, 10 MB)	2025		
ABB LTD	https://www.sustainability.uzh.ch/dam/jcr:9b36dc04-f4b2-456c-b368-9d6020adcc76/UZH_Sustainability%20Policy_D.pdf
	pdf	52.0	UZH Sustainability Policy (pdf)			
ABN AMRO BANK NV	https://assets.ctfassets.net/1u811bvgvthc/7sd3WsftS6xvTKABRvsUSH/6aecdaa324ea3bf027de97eaec5757ff/ESG_Economist_-_EU_NECPs_and_2030_targets_nl.pdf
	pdf	65.0	ESG Economist - EU NECPs and 2030 targets nl.pdf 147 KB	2030		
ACCIONA SA	https://mc-cd8320d4-36a1-40ac-83cc-3389-cdn-endpoint.azureedge.net/-/media/Files/IRENA/Agency/Publication/2023/Mar/IRENA_WETO_Preview_2023.pdf?rev=
	pdf	41.0	IRENA (2023), World Energy Transitions Outlook 2023: 1.5°C Pathway;	2023		
ADECCO GROUP AG	https://www.adecco.com/en-id/ace-reporting-line
	unknown	3.0	ace reporting line			
AEGON LTD.	https://www.aegon.com/Investors/annual-reports
	unknown	29.0	Annual reports			
AIRBUS SE	https://www.airbus.com/sites/g/files/jlcbta136/files/2024-07/Airbus_Multiannual_Digital_Accessibility_Scheme_2024-2026.pdf
	pdf	65.0	Airbus Multiannual Digital Accessibility Scheme 2024-2026	2024		
AKZO NOBEL NV	https://www.akzonobel.com/content/dam/akzonobel-corporate/global/en/investor-relations-images/result-center/archive-annual-reports/2029-2020/AkzoNobel-annual-report-2024.pdf
	pdf	51.0	INTERACTIVE PDF	2029		
ANDRITZ AG	https://www.andritz.com/resource/blob/620974/e66acf15517f7bab56818dbc86194b84/andritz-non-financial-statement-2024-e-data.pdf
	pdf	61.0	Download non-financial report according to CSRD/ESRS	2024		
ANHEUSER-BUSCH INBEV SA/NV	https://www.ab-inbev.com/assets/presentations/ESG/ABINBEV_ESG%202021_Final.pdf
	pdf	56.0	2021 Environmental, Social & Governance Report	2020		
ARKEMA	https://www.arkema.com/files/live/sites/shared_arkema/files/downloads/investorrelations/en/finance/arkema_presentation_ESG%20Conference_Feb.%202022_V2.pdf
	pdf	66.0	dbAccess Global ESG Conference 2022	2020		
ASHTEAD GROUP PLC	https://www.ashtead-group.com/files/downloads/reports/Ashtead-Group-Annual-Report-FY2023.pdf
	pdf	66.0	2023 Annual Report – PDF 7.9MB – Download	2023		
ASML HOLDING N.V.	https://ourbrand.asml.com/m/79d325b168e0fd7e/original/2024-Annual-Report-based-on-US-GAAP.pdf
	pdf	66.0	2024 Annual Report based on US GAAP File	2024		
ASSICURAZIONI GENERALI SPA	https://www.generali.com/doc/jcr:259c5d6e-46f7-4a43-9512-58e5dcbd2a56/Annual%20Integrated%20Report%20and%20Consolidated%20Financial%20Statements%202024_Generali%20Group_final_interactive.pdf/lang:en/Annual_Integrated_Report_and_Consolidated_Financial_Statements_2024_Generali_Group_final_interactive.pdf
	pdf	67.0	Annual Integrated Report and Consolidated Financial Statements 2024 – Download	2020		
ASTRAZENECA PLC	https://www.astrazeneca.co.uk/content/dam/intelligentcontent/unbranded/astrazeneca/uk/en/pdf/sustainability/Carbon-Reduction-Plan-April-2025.pdf.coredownload.pdf
	pdf	56.0	UK Carbon Reduction Plan April 2025	2025		
ATLAS COPCO AB	https://atlascopco.com/content/dam/atlas-copco/local-countries/canada/documents/Signed-Annual-Report-on-Measures-to-Combat-Forced-Labor-and-Child-Labor-for-2024-Atlas-Copco.pdf
	pdf	66.0	Annual Report on Measures to Combat Forced Labour and Child Labour for 2024	2024		
AURUBIS AG	https://www.aurubis.com/en/dam/jcr:67c948b2-870c-4fa3-b0df-34b62359019b/Aurubis_Annual%20Report_FY%202022_23%20(Meeting%20participation).pdf
	pdf	66.0	Meeting participation (excerpt from the Annual Report 2022/23)	2020		
AVIVA PLC	https://static.aviva.io/content/dam/aviva-corporate/documents/investors/pdfs/reports/2024/aviva-plc-annual-report-and-accounts-2024.pdf
	pdf	67.0	Annual Report and Accounts 2024	2024		
AXA SOCIETE ANONYME	https://www.axa.fr/content/dam/axa/desktop/Documents/PDF-BANQUE/politique-d-investissement-ESG.pdf
	pdf	53.0	Politique d’investissement ESG			
BALFOUR BEATTY PLC	https://www.balfourbeatty.com/media/yx0pn423/balfour-beatty-annual-report-and-accounts-2024.pdf
	pdf	67.0	Download our 2024 Annual Report and Accounts	2024		
BANCO BILBAO VIZCAYA ARGENTARIA SA	https://shareholdersandinvestors.bbva.com/wp-content/uploads/2022/09/2022-09-Sustainability-General-Policy-BBVA_ENG.pdf
	pdf	61.0	BBVA's Sustainability Policy	2022		
BARCLAYS PLC	https://home.barclays/content/dam/home-barclays/documents/investor-relations/reports-and-events/annual-reports/2024/Barclays-Bank-UK-Annual-Report-2024.pdf
	pdf	66.0	Barclays Bank UK PLC Annual Report 2024 (PDF 2.4MB)	2024		
BARRY CALLEBAUT AG	https://www.barry-callebaut.com/system/files/2024-11/ESG%20Datasheet%202023_24_0.pdf
	pdf	66.0	ESG Datasheet 2023/24	2024		
BAYER AG	https://b04-ep-media-prod.azureedge.net/pickerdocuments/ATGBs_Allgemein_DINA4_20250617_788341_original.pdf
	pdf	41.0	ATGB 2025/26	2025		
BNP PARIBAS	https://cdn-group.bnpparibas.com/uploads/file/BNPParibas_ataglance2025_EN.pdf
	pdf	39.0	Read the 2025 “At a glance” issue	2025		
BOLIDEN AB	https://www.boliden.com/4952ce/globalassets/about-boliden/new-about-boliden/our-policies-and-commitments/policy-documents/icmm-content-index-to-annual-and-sustainability-report-2024.pdf
	pdf	66.0	ICMM index to Boliden’s Annual and Sustainability Report 2024	2024		
BOLLORE SE	https://www.bollore.com/bollo-content/uploads/2025/05/chapitre-2-boll24t035_urd_gb_2024_mel.pdf
	pdf	44.0	Read our sustainability report to learn more	2025		
BOUYGUES	https://www.bouyguestelecom.fr/static/cms/tarifs/Conditions_Particulieres_Cedant_Utilisateur_2024.pdf
	pdf	40.0	Conditions Particulières Utilisateurs – Septembre 2024	2024		
BP PLC	https://www.bp.com/content/dam/bp/business-sites/en/global/corporate/pdfs/investors/bp-annual-report-and-form-20f-2024.pdf
	pdf	67.0	Annual Report 2024 – pdf / 3.2 MB	2024		
BRENNTAG SE	https://brenntagprod-media.e-spirit.cloud/06432017-be1f-41ce-8d1d-564e2a66d213/documents/corporate/investor-relations/2024/annual-report-2024/brenntag-se_group-sustainability-statement-2024-presentation_en.pdf
	pdf	65.0	Sustainability reporting highlights 2024	2017		
BUNZL PLC	https://www.bunzl.com/media/yyiowbfu/bunzl-plc-sustainability-report-2024.pdf
	pdf	66.0	Sustainability Report 2024	2024		
CAIXABANK, S.A.	https://www.caixabank.com/deployedfiles/caixabank_com/Estaticos/PDFs/Accionistasinversores/Gobierno_Corporativo/JGA/2025/Informe_Actividades_Comision_Riesgos_2024_ES.pdf
	pdf	61.0	Informe de funcionamiento de la Comisión de Riesgos (Abre en una ventana nueva)	2025		
CARLSBERG A/S	https://www.carlsberggroup.com/media/zzfbaaxn/carlsberg-breweries-group_2024-annual-report.pdf
	pdf	66.0	2024 Annual Report	2024		
CHRISTIAN DIOR	https://www.dior-finance.com/pdf/d/2/1056/Christian%20Dior%20SE%20-%20Availability%20of%20the%202024%20annual%20report.pdf
	pdf	65.0	Notice of availability of the 2024 annual financial report	2020		
COMMERZBANK AG	https://www.commerzbank.com/ms/documents/en/regulatory-disclosure-sustainability-indicators-for-crypto-assets.pdf
	pdf	51.0	Sustainability indicators for crypto-assets			
COMPAGNIE FINANCIERE RICHEMONT SA	https://www.richemont.com/media/tjbjiob5/richemont-non-financial-report-2024.pdf
	pdf	66.0	Non-Financial Report 2024 (5.7 MB)	2024		
COMPUTACENTER PLC	https://www.computacenter.com/docs/default-source/documents/sustainability-report-2024_final_links.pdf?sfvrsn=283e604b_3
	pdf	61.0	Download our latest Sustainability Report	2024		
D'IETEREN GROUP SA/NV	https://www.dieteren.be/media/z4kjizcm/dieteren-esg-strategy-mobility-in-motion-fr-2024.pdf
	pdf	61.0	Découvrez notre stratégie ESG	2024		
DAIMLER TRUCK HOLDING AG	https://www.daimlertruck.com/fileadmin/user_upload/documents/sustainability/daimler-truck-esg-factbook-fy-2024.pdf
	pdf	66.0	Daimler Truck ESG Factbook for Full Year 2024 – PDF (0,09 MB)	2024		
DANONE	https://www.danone.com/content/dam/corp/global/danonecom/about-us-impact/policies-and-commitments/en/2024/forest-annual-update-2023.pdf
	pdf	66.0	Forest annual update 2023	2024		
DANSKE BANK A/S	https://danskebank.com/-/media/danske-bank-com/file-cloud/2025/2/danske-bank---uddrag-af-annual-report-2024.pdf?rev=4b2c493d91bf4ce680eda4f416e183fd&amp%3Bsc_lang=da
	pdf	66.0	07. feb 2025 – Danske Bank - Uddrag af Annual Report 2024	2025		
DCC PLC	https://www.dcc.ie/~/media/Files/D/Dcc-Corp-v3/documents/investors/annual-and-sustainability-reports/2023/annual-report-2023.pdf
	pdf	66.0	Annual report 2023	2023		
DEUTSCHE BANK AG	https://www.deutsche-bank.de/dam/deutschebank/de/shared/pdf/rechtliche-hinweise/Inclusion-of-sustainability-risks-20210310.pdf
	pdf	60.0	Inclusion of sustainability risks in the scope of investment advice, financial portfolio management and insurance advice (PDF, 77,3 KB)	2021		
DEUTSCHE POST AG	https://www.dpdhl.com/dam/dpdhl/de/media-center/media-relations/documents/2023/dp-dialogmarketing-monitor-2023.pdf
	pdf	41.0	Dialogmarketing Monitor 2023 – Unternehmen setzen auf Werbung mit Dialogelementen	2023		
DEUTSCHE TELEKOM AG	https://www.telekom.com/resource/blob/1086762/8b28ad81f040852828db2627f35c6432/dl-deutsche-telekom-ag-annual-financial-statements-as-of-december-31-2024-data.pdf
	pdf	66.0	Deutsche Telekom AG Annual Financial Statements as of December 31, 2024 (pdf, 2.1 MB)	2024		
DIAGEO PLC	https://www.diageo.com/pdf-viewer.aspx?gid=307336661&src=%2F~%2Fmedia%2FFiles%2FD%2FDiageo-V2%2FDiageo-Corp%2Finvestors%2Fresults-reports-and-events%2Fannual-reports%2F2025%2Fannual-report-2025.pdf
	pdf	66.0	Annual Report 2025	2025		
DS SMITH LIMITED	https://www.dssmith.com/globalassets/investors/annual-reports-archive/ds-smith-annual-report-2024.pdf
	pdf	66.0	DS Smith Annual Report 2024 – Download here	2024		
EIFFAGE	https://www.eiffage.com/files/live/sites/eiffagev2/files/Transition%20%c3%a9cologique/La%20biodiversit%c3%a9%20au%20coeur%20de%20nos%20m%c3%a9tiers/Sustainable%20Development%20charter%20(1).pdf
	pdf	52.0	Sustainable development			
ENDESA, S.A. (SPAIN)	https://www.endesa.com/content/dam/enel-es/endesa-en/home/investors/financialinformation/annualreports/documents/2024/individual-annual-report-endesa-2024.pdf
	pdf	65.0	Annual Report 2024 (pdf)	2024		
ENEL SPA	https://www.dataforce.de/wp-content/uploads/25.04.17-Comunicato-Stampa-Dataforce-Osservatorio-Canoni-Noleggio-Q1-2025.pdf
	pdf	40.0	Comunicato Stampa Dataforce – Osservatorio Canoni Noleggio Q1-2025	2025		
ENGIE	https://www.engie.com/sites/default/files/assets/documents/2025-07/Engie%20-2024%20ESG%20at%20ENGIE%20VDEF.pdf
	pdf	65.0	2024 - ESG at ENGIE (anglais uniquement)	2025		
ENI S.P.A.	https://www.eni.com/content/dam/enicom/documents/eng/sustainability/2024/Eni-Local-Report-Cote-d-ivoire-2024.pdf
	pdf	66.0	Sustainability Local Report Côte d'Ivoire 2024 (versione inglese)	2024		
EQUINOR ASA	https://cdn.equinor.com/files/h61q9gi9/global/16ccbc5a098c3b971979118420c4f83ddee18fb4.pdf?annual-report-2024-equinor.pdf=
	pdf	66.0	2024 Annual report	1979		
ESSILORLUXOTTICA	http://cms.essilorluxottica.com/caas/v1/media/253398/data/download/f3fc336829625bff7ba4953197866281.pdf
	pdf	49.0	EssilorLuxottica 2024 Sustainability Report	1978		
FERROVIAL S.E.	https://static.ferrovial.com/wp-content/uploads/2019/11/13133148/annual-corporate-governance-report-2015.pdf
	pdf	66.0	2015 – Annual Corporate Governance Report of Ferrovial, S.A. (PDF 804 KB)	2019		
FLUTTER ENTERTAINMENT PUBLIC LIMITED COMPANY	https://www.flutter.com/media/pzuc0n0y/flutter-entertainment-plc-annual-report-accounts-2022.pdf
	pdf	66.0	Annual Report and Accounts 2022	2022		
FORVIA SE	https://forvia.com/sites/default/files/2024-03/FORVIA_%202023_Sustainability%20Report_WEB.pdf
	pdf	61.0	Download our Sustainability Report	2024		
GALP ENERGIA SGPS S.A.	https://www.galp.com/corp/Portals/0/Recursos/Investidores/IMR2024/AnnualIntegratedReport2024.pdf
	pdf	54.0	Relatório Anual Integrado 2024	2024		
GLENCORE PLC	https://www.glencore.com/.rest/api/v1/documents/static/7a4295e4-3674-45e9-94c4-7d7fb285faff/GLEN-2024-Annual-Report.pdf
	pdf	66.0	2024 Annual Report – pdf (21 MB)	2024		
HEINEKEN HOLDING N.V.	https://www.heinekenholding.com/sites/heinekenholding-v2/files/heineken-holding/investors/results-reports-webcasts-presentations/heineken-holding-nv-annual-report-2023-final.pdf
	pdf	66.0	heineken-holding-nv-annual-report-2023-final.pdf	2023		
HENKEL AG & CO. KGAA	https://www.henkel.de/resource/blob/2043318/9b8425a944b077ab7165b775398c72a1/data/2024-annual-report.pdf
	pdf	66.0	Annual Report 2024 (8,24 MB)	2043		
HOLCIM AG	https://www.holcim.ch/sites/switzerland/files/docs/holcim-2024-sustainability-ambitions-italiano-1_0.pdf
	pdf	65.0	holcim-2024-sustainability-ambitions-italiano (pdf, 5.32 MB)	2024		
HSBC HOLDINGS PLC	https://www.hsbc.com/-/files/hsbc/investors/hsbc-results/2024/annual/pdfs/hsbc-holdings-plc/250219-annual-report-and-accounts-2024.pdf
	pdf	66.0	2024 Annual Report (PDF, 8.34MB)	2024		
INCHCAPE PLC	https://www.inchcape.com/~/media/files/i/inchcape/corp/results-and-presentations/2025/inchcape-2024-sustainability-report.pdf
	pdf	66.0	Inchcape 2024 Sustainability Report	2025		
ING GROEP NV	https://www.ing.com/Investors/Financial-performance/Annual-reports/2024/2024-Global-systemically-important-banks-indicators.htm
	pdf	51.0		2024		
INTERNATIONAL CONSOLIDATED AIRLINES GROUP S.A	https://assets.ctfassets.net/tjnj6enyzr6b/7qu7c1b6yYb1Ki2OALopFe/08ed2b60bd61fd753fef437eb35e0658/IAG_-_Modern_Slavery_Statement_June_2025_-_UK_-_Final.pdf
	pdf	37.0	Modern Slavery Statement	2025		
INTERNATIONAL DISTRIBUTION SERVICES PLC	https://www.internationaldistributionservices.com/media/12341/2024-annual-general-meeting-proxy-form.pdf
	pdf	66.0	2024 Annual General Meeting Proxy Form – pdf / 81.1KB	2024		
INTESA SANPAOLO S.P.A.	https://www.intesasanpaoloprivatebanking.it/bin/paginaGenerica/308/C_32_paginaGenerica_1_7_4_objAllegato.pdf
	pdf	32.0	Relazione sulla politica in materia di remunerazione 2025 e sui compensi corrisposti nell'esercizio 2024 di Intesa Sanpaolo	2025		
ISS A/S	https://brand.issworld.com/m/1bb99b2c33c4d7cf/original/ISS-Annual-Report-2024.pdf
	pdf	67.0	Annual Report 2024 – https://brand.issworld.com/m/1bb99b2c33c4d7cf/original/ISS-Annual-Report-2024.pdf
	2024		
IVECO GROUP NV	https://www.iveco.com/global/-/media/IVECOdotcom/global/metallica/GDPR_PN_Metallica_IT.pdf?rev=77aec45e5b394cd192760341a172b356
	pdf	36.0	Note Privacy	1927		
JD SPORTS FASHION PLC	https://s204.q4cdn.com/980191062/files/doc_downloads/jd-foundation-social-impact-report-2021.pdf
	pdf	37.0	Social Impact Report	1910		
JDE PEET'S N.V.	https://www.jdepeets.com/siteassets/home/investors/annual-reports/jde-peets-annual-report-2024.pdf
	pdf	66.0	2024 Annual Report	2024		
KBC GROEP NV/ KBC GROUPE SA	https://multimediafiles.kbcgroup.eu/ng/published/KBC/PDF/VERZEKERINGEN/hospitalisatievzk/kbc-assurance-hospitalisation-liste-des-hopitaux-chers.pdf
	pdf	28.0	liste la plus récente des hôpitaux chers, elle est disponibile ici			
KERING	https://www.kering.com/api/download-file?path=KERING_Articles_of_association_April_24_2025_27a58479b7.pdf
	pdf	36.0	See more	2025		
KESKO OYJ	https://www.kesko.fi/4978a1/globalassets/03-sijoittaja/kesko-sijoituskohteena/kesko-green-bond-annual-review-2024.pdf
	pdf	66.0	Kesko Green Bond Annual Review 2024	2024		
KION GROUP AG	https://www.kiongroup.com/KION-Website-Main/About-us/Sustainability/Insights/KION-Supplementary-Non-financial-Insights-2024.pdf
	pdf	66.0	Supplementary Non-financial Insights 2024	2024		
KNORR-BREMSE AG	https://ir-api.eqs.com/media/document/ff6e1bf7-16f3-4acd-9abb-a7af5caf73d4/assets/Knorr-Bremse_AG_AnnualReport_2024.pdf?disposition=inline
	pdf	66.0	Sustainability Report 2024 (Group Sustainability Statement)	2024		
KONE OYJ	https://www.kone.com/en/Images/KONE%20Sustainability%20Report%202023_tcm17-129519.pdf
	pdf	66.0	KONE Sustainability Report 2023	2020		
KONINKLIJKE AHOLD DELHAIZE N.V.	https://www.aholddelhaize.com/media/wcqil04n/ad_annual-report_2024_interactive.pdf
	pdf	66.0	Ahold Delhaize's 2024 Annual Report	2024		
KONINKLIJKE PHILIPS N.V.	https://www.philips.com/c-dam/corporate/about-philips/investors/debt-info/events/Philips-Sustainable-Finance-Framework-March-2024.pdf
	pdf	64.0	Philips Sustainable Finance Framework (March 2024) (1.13MB)	2024		
L'AIR LIQUIDE SOCIETE ANONYME POUR L'ETUDE ET L'EXPLOITATION DES PROCEDES GEORGES CLAUDE	https://www.airliquide.com/sites/airliquide.com/files/2025-03/2024-integrated-annual-report.pdf
	pdf	67.0	2024 Integrated Annual Report	2025		
L'OREAL	https://www.loreal.com/-/media/project/loreal/brand-sites/corp/master/lcorp/2-group/annual-reports/loreal2024annualreport.pdf?rev=60be90d75fdb49aba373457b5c0d720e
	pdf	65.0	2024 Annual Report – Download	2024		
LEGRAND	https://www.legrand.com/sites/default/files/Documents_PDF_Legrand/Finance/2010/autres/Annual_Rapport_2010_Legrand.pdf
	pdf	56.0	Access to the anual report 2010	2010		
LEONARDO S.P.A.	https://www.leonardo.com/documents/15646808/0/leonardo+esg+investor+day_presentation.pdf?t=1655311687135
	pdf	57.0	ESG Investor Day (PDF, 1911 KB)	1911		
LONDON STOCK EXCHANGE GROUP PLC	https://www.lseg.com/content/dam/lseg/en_us/documents/investor-relations/annual-reports/lseg-annual-report-2019.pdf
	pdf	65.0	LSEG Annual Report 2019 – download	2019		
MARKS AND SPENCER GROUP PLC	https://corporate.marksandspencer.com/sites/marksandspencer/files/2025-05/Modern_Slavery_Statement_2025.pdf
	pdf	36.0	Modern Slavery Act	2025		
MERCK KGAA	https://www.merck-bkk.de/fileadmin/medien/dokumente/downloadcenter/downloads_fuer/arbeitgeber/Rechengroessen_und_Grenzwerte_ab_Juli_2025.pdf
	pdf	41.0	Beiträge und Rechengrößen 2025	2025		
MTU AERO ENGINES AG	https://www.mtu.de/fileadmin/EN/5_Investors/9_1_Annual_General_Meeting/2024_AGM_A_00_Einladungbeakanntmachung_en.pdf
	pdf	66.0	Invitation to the Annual General Meeting 2024 (Convocation incl. agenda) (PDF)	2024		
MUNCHENER RUCKVERSICHERUNGS-GESELLSCHAFT AKTIENGESELLSCHAFT IN MUNCHEN	https://www.munichre.com/content/dam/munichre/mrwebsiteslaunches/2024-annual-report/MunichRe-Group-Annual-Report-2024-en.pdf/_jcr_content/renditions/original./MunichRe-Group-Annual-Report-2024-en.pdf
	pdf	66.0	Annual Report 2024	2024		
NATURGY ENERGY GROUP, S.A.	https://www.bolsasymercados.es/bme-exchange/docs/docsSubidos/Listing/Fixed-Income-Annual-Report-2024.pdf
	pdf	60.0	Annual Report	2024		
NEXANS	https://www.nexans.fr/dam/jcr:66838c26-bfaa-42b9-9258-b8fec76cdb5b/2025-07%20Catalogue%20Flipbook%20VF.pdf
	pdf	41.0	Catalogue Câbles & fils Residentiels, Tertiaires & Industriels France 2025	2025		
NN GROUP NV	https://www.nn-group.com/site/binaries/content/assets/nn-group/annual-reports/2024/nn-group-annual-report-2024.pdf
	pdf	66.0	NN Group Annual Report 2024	2024		
NOKIA OYJ	https://www.nokia.com/about-us/news/releases/2024/10/30/nokia-selected-to-lead-european-lighthouse-project-on-6g-sustainability
	unknown	40.0	Press release – Nokia selected to lead European lighthouse project on 6G sustainability - Oct, 2024	2024		
NORDEA BANK ABP	https://www.nordea.com/en/doc/nordea-bank-annual-report-2015.pdf
	pdf	66.0	Appendix 5 – Annual report 2015 of the Transferor Company	2015		
NOVO NORDISK A/S	https://www.novonordisk.com/content/dam/nncorp/global/en/investors/irmaterial/annual_report/2024/novo-nordisk-annual-report-2023.pdf
	pdf	65.0	Annual Report 2023 (PDF)	2024		
ORANGE	https://www.orange-business.com/sites/default/files/manifesto_2016_orange_business_services.pdf
	pdf	45.0	an inclusive and sustainable recovery	2016		
ORLEN S.A.	https://www.orlen.pl/content/dam/internet/orlen/pl/pl/relacje-inwestorskie/akcje-i-obligacje/dobre-praktyki-gpw/Raport_1DP_2025_Naruszenie_zasady_kandydatury.pdf.coredownload.pdf
	pdf	41.0	Raport 1DP/2025 dotyczący incydentalnego naruszenia Dobrych Praktyk – Format pdf – Size 23 KB	2025		
ORSTED A/S	https://cdn.orsted.com/-/media/www/docs/corp/com/sustainability/orsted-brancheaftalerapport-2021.pdf?rev=8ae91fd2ca8b4f47bb6ecb077d26814a&amp%3Bhash=707A9D9AC39B421857409348B9D1D699
	pdf	51.0	Vi aflægger en årlig statusrapport for vores brug af bæredygtig biomasse, som kan tilgås her	2021		
PERNOD RICARD	https://www.pernod-ricard.com/sites/default/files/inline-files/Sustainability-Linked%20Financing%20Framework%20%28September%202023%29.pdf
	pdf	66.0	Sustainability-Linked Financing Framework (September 2023)	2020		
PUBLICIS GROUPE S A	https://www.publicisgroupe.com/sites/default/files/investors-document/2023-05/14%20-%20Statutory%20Auditors%27%20report%20on%20the%20annual%20financial%20statements%20-%20Fiscal%20year%202022.pdf
	pdf	49.0	View more	2023		
RECKITT BENCKISER GROUP PLC	https://www.reckitt.com/media/vudpe3c0/19-03-25_2_reckitt_sustainability-report-2024-1.pdf
	pdf	49.0	Download PDF	2024		
RENAULT	https://e-brochure.renault.fr/renault_r5_coty/mobile/m_coty_renault5_fr_july_2025.pdf
	pdf	35.0	téléchargez la brochure Renault 5	2025		
REPSOL SA.	https://www.repsol.com/content/dam/repsol-corporate/es/accionistas-e-inversores/equity-story-esg-2023.pdf
	pdf	65.0	Equity Story ESG Day 2023 – Ver pdf (5,83 MB)	2023		
RHEINMETALL AG	https://www.rheinmetall.com/Rheinmetall%20Group/Karriere/Rheinmetall%20als%20Arbeitgeber/2025-05-26_Rheinmetall_KarriereBrosch%C3%BCre.pdf
	pdf	34.0	Laden Sie hier unsere Arbeitgeberbroschüre herunter.	2025		
RIO TINTO PLC	https://cdn-rio.dataweavers.io/-/media/content/documents/invest/reports/annual-reports/2024-annual-report.pdf
	pdf	66.0	2024 Annual Report	2024		
ROCHE HOLDING AG	https://assets.roche.com/f/176343/x/9d0629113a/2024-sustainability-reporting-indicators-definitions-and-scope.pdf
	pdf	65.0	2024 Sustainability reporting indicators definitions and scope	2024		
RYANAIR HOLDINGS PLC	https://www.ryanair.com/content/dam/ryanair3/2021/legal/DCC-class-action-publication-documents.pdf
	pdf	36.0	Israeli class action	2021		
SAIPEM SPA	https://www.saipem.com/sites/default/files/2025-04/2024%20Sustainability%20Report_0.pdf
	pdf	65.0	2024 Sustainability Report	2025		
SANDVIK AB	https://www.annualreport.sandvik/en/2024/_assets/downloads/entire-en-svk-ar24.pdf
	pdf	66.0	Annual report 2024	2024		
SCHINDLER HOLDING AG	https://group.schindler.com/content/dam/website/group/docs/responsibility/2019-schindler-sustainability-report.pdf/_jcr_content/renditions/original./2019-schindler-sustainability-report.pdf
	pdf	65.0	Sustainability Report (2019) (PDF, 2 MB)	2019		
SEB SA	https://www.groupeseb.com/sites/default/files/2024-12/PR-GroupeSEB-ESG_Investor_Day-12122024%20%281%29.pdf
	pdf	65.0	GROUPE SEB announces its new 2024-2030 ESG ambition… (press, 2024-12-12)	2024		
SECURITAS AB	https://www.securitas.com/globalassets/com/files/annual-reports/eng/securitas_annual-and-sustainability-report_2024.pdf
	pdf	66.0	Annual and Sustainability Report 2024	2024		
SOCIETE GENERALE	https://www.societegenerale.com/sites/default/files/documents/Notations%20Financi%C3%A8res/sg_sustainable_and_positive_impact_bond_framework_june_2020.pdf
	pdf	60.0	Sustainable and Positive Impact Bond Framework (english only)	2020		
SODEXO	https://edge.sitecorecloud.io/sodexofrance1-sodexocorpsites-prod-e74c/media/Project/Sodexo-Corp/Asia/IN/Media/PDFs/Annual-Return-2023-24---Sodexo-India-Services-Private-Limited.pdf
	pdf	59.0	Annual Return	2023		
SPIE SA	https://www.spiebatignolles.fr/wp-content/uploads/2025/06/DPEF-2024.pdf
	pdf	41.0	Déclaration de performance extra-financière 2024	2025		
SSE PLC	https://www.sse.com/media/lifo554i/sse-sustainability-report-2025.pdf
	pdf	66.0	Sustainability Report 2025	2025		
STANDARD CHARTERED PLC	https://www.sc.com/en/uploads/sites/66/content/docs/esg-reporting-index-2023.pdf
	pdf	66.0	ESG Reporting Index 2023	2023		
STELLANTIS N.V.	https://www.stellantis.com/content/dam/stellantis-corporate/investors/financial-reports/StellantisNV-20250630-Semi-Annual-Report.pdf
	pdf	66.0	Semi-Annual Report as of and for the six months ended June 30, 2025	2025		
SWISSCOM AG	https://www.swisscom.ch/content/dam/assets/about/investoren/berichte/documents/2025/adverse-sustainability-indicators-reporting-2024.pdf
	pdf	64.0	Adverse Sustainability Indicators Reporting 2024	2025		
TELIA COMPANY AB	https://www.teliacompany.com/assets/u5c1v3pt22v8/2x2vEZvhniymYXpCJn7lRP/32f82e3619de369bf54c583abbeacf9c/Buckle_up_-_Telia_Trend_Report_Security_-_September_18th_2024_-_First_edition.pdf
	pdf	34.0	Buckle up – Telia Trend Report Security – September 18th 2024 – First edition	2024		
THYSSENKRUPP AG	https://www.thyssenkrupp.com/_binary/UCPthyssenkruppAG/f60f13e3-289d-4041-a535-fdf7ea9210b3/thyssenkrupp-GB_2023-2024_EN_WEB.pdf
	pdf	50.0	Annual Report 2023/2024	2023		
TOTALENERGIES SE	https://totalenergies.com/system/files/documents/totalenergies_sustainability-climate-2025-progress-report_2025_en.pdf
	pdf	67.0	Sustainability and Climate 2025 Progress Report	2025		
UMICORE	https://www.umicore.com/storage/group/integrated-annual-report-2023.pdf
	pdf	66.0	2023 Annual Report	2023		
UNICREDIT SPA	https://www.unicreditgroup.eu/content/dam/unicreditgroup-eu/documents/en/investors/financial-reports/2024/4Q24/2024-Annual-Reports-and-Accounts.pdf
	pdf	67.0	Download our 2024 Annual Report	2024		
UNIVERSAL MUSIC GROUP N.V.	https://universalmusic.nl/wp-content/uploads/2020/11/Spelvoorwaarden-UMG-05102020.pdf
	pdf	37.0	Spelvoorwaarden	2020		
VESTAS WIND SYSTEMS A/S	https://www.vestas.com/content/dam/vestas-com/global/en/investor/reports-and-presentations/financial/2024/fy-2024/Vestas%20Annual%20Report%202024.pdf.coredownload.inline.pdf
	pdf	67.0	Vestas Sustainability Statement 2024 (p. 51 onwards)	2024		
VINCI	https://www.vinci.com/sites/default/files/medias/file/2025/03/VINCI-avis-de-convocation_BALO-26032025.pdf
	pdf	41.0	Avis de convocation publié au BALO le 26 mars 2025	2025		
VIVENDI SE	https://www.vivendi.com/wp-content/uploads/2024/03/20240307_VIV_PR_Annual-Results-2023-vf.pdf
	pdf	65.0	20240307_VIV_PR_Annual Results 2023 vf	2024		
VOLKSWAGEN AG	https://www.vwgroupsupply.com/one-kbp-pub/media/shared_media/documents_1/nachhaltigkeit/brochure__volkswagen_group_requirements_regarding_sustainability_in_its_relationships_with_business_partners__code_of_conduct_fo/coc_geschaeftspartner_20230309.pdf
	pdf	51.0	Code of Conduct – Öffnet einen externen Link	2023		
WENDEL SE	https://wendelgroup.com/performance-esg-2023.pdf
	pdf

Ora vado a chiedere a chat quanti sono giusti.
Ho visto di aver sbagliato e molte aziende erano già state reperite dalla scorsa run, quindi ora unisco i csv delle due run e rivedo tutte le statistiche.

(133, 4)
Descriptive statistics for 'score':
count    133.00000
mean      57.24812
std       12.34627
min        3.00000
25%       50.00000
50%       65.00000
75%       66.00000
max       67.00000
Name: score, dtype: float64

Aziende con valore di score = max
                                            company_id  ... valutazione
16                          ASSICURAZIONI GENERALI SPA  ...      giusta
20                                           AVIVA PLC  ...   sbagliata
22                                  BALFOUR BEATTY PLC  ...      giusta
31                                              BP PLC  ...      giusta
71                                             ISS A/S  ...      giusta
83   L'AIR LIQUIDE SOCIETE ANONYME POUR L'ETUDE ET ...  ...      giusta
124                                   TOTALENERGIES SE  ...      giusta
126                                      UNICREDIT SPA  ...      giusta
128                            VESTAS WIND SYSTEMS A/S  ...      giusta

Aziende con valore di score = min

        company_id  ... valutazione
7  ADECCO GROUP AG  ...   sbagliata

Descriptive statistics for 'score':
count    133.00000
mean      57.24812
std       12.34627
min        3.00000
25%       50.00000
50%       65.00000
75%       66.00000
max       67.00000
Name: score, dtype: float64

Valori 'valutazione' per Q1:
['sbagliata' 'parziale' 'sbagliata' 'parziale' 'sbagliata' 'sbagliata'
 'sbagliata' 'parziale' 'sbagliata' 'sbagliata' 'parziale' 'sbagliata'
 'parziale' 'parziale' 'sbagliata' 'parziale' 'sbagliata' 'sbagliata'
 'parziale' 'sbagliata' 'sbagliata' 'sbagliata' 'sbagliata' 'giusta'
 'sbagliata' 'sbagliata' 'parziale']

Valori 'valutazione' per Q2:
['parziale' 'sbagliata' 'giusta' 'giusta' 'parziale' 'parziale' 'parziale'
 'parziale' 'parziale' 'parziale' 'parziale' 'giusta' 'parziale'
 'parziale' 'parziale' 'giusta' 'giusta' 'giusta' 'parziale' 'parziale'
 'sbagliata' 'parziale' 'parziale' 'sbagliata' 'sbagliata' 'parziale'
 'giusta' 'parziale' 'parziale' 'parziale' 'giusta' 'parziale']

Valori 'valutazione' per Q3:
['parziale' 'giusta' 'sbagliata' 'sbagliata' 'sbagliata' 'giusta' 'giusta'
 'parziale' 'parziale' 'parziale' 'parziale' 'parziale' 'parziale'
 'giusta' 'parziale' 'parziale' 'giusta' 'giusta' 'parziale' 'sbagliata'
 'giusta' 'parziale' 'giusta' 'giusta' 'parziale' 'parziale' 'parziale'
 'giusta' 'sbagliata' 'parziale' 'giusta' 'giusta' 'giusta' 'parziale'
 'giusta' 'giusta' 'sbagliata' 'giusta' 'parziale' 'giusta' 'giusta'
 'giusta' 'giusta' 'giusta' 'sbagliata' 'sbagliata' 'giusta' 'giusta'
 'sbagliata' 'giusta' 'parziale' 'parziale' 'parziale' 'giusta' 'giusta'
 'parziale' 'parziale' 'parziale' 'giusta' 'giusta' 'parziale' 'parziale'
 'parziale' 'parziale' 'giusta']

Valori 'valutazione' per Q4:
['giusta' 'sbagliata' 'giusta' 'giusta' 'giusta' 'giusta' 'giusta'
 'giusta' 'giusta']

Descriptive statistics for 'score' when valutazione = 'giusta':
count    46.000000
mean     63.543478
std       6.032160
min      41.000000
25%      65.000000
50%      66.000000
75%      66.000000
max      67.000000

Descriptive statistics for 'score' when valutazione = 'parziale':
count    56.000000
mean     57.446429
std      11.089585
min      28.000000
25%      51.000000
50%      64.000000
75%      66.000000
max      66.000000

Descriptive statistics for 'score' when valutazione = 'sbagliata':
count    31.000000
mean     47.548387
std      15.250877
min       3.000000
25%      37.000000
50%      41.000000
75%      65.000000
max      67.000000


ora devo farmi un benchmark con una ventina di report che sono corretti, per poi utilizzare gpt5mini per andare a verificare quali sono le attività di sostenibilità che ogni azienda affronta.