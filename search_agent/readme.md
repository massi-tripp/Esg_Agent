# ESG Agent – Fase 1: Ricerca dei report di sostenibilità

Codici visibili nel file find_reports.py. Questo script legge un elenco di dominii di aziende da un file CSV e, per ciascuna, usa GPT per creare delle “frasi di ricerca” efficaci e poi interroga Tavily (un motore di ricerca) per trovare il report ufficiale di sostenibilità/ESG 2024.
I risultati trovati vengono filtrati e ordinati (scarta link “rumorosi” tipo news/slide/summary, premia quelli con parole chiave giuste e soprattutto i PDF), poi GPT sceglie il miglior link finale tra i candidati.
Infine salva tutto in un nuovo CSV con: azienda, dominio, link migliore e alcune statistiche (quante pagine trovate, quante candidate, quali query sono state usate).
Questo csv si trova nel seguente percorso: data/output/report_corretti.csv
Al momento l'agente ha trovato 390 report corretti su 432 aziende, pari al 90.28%