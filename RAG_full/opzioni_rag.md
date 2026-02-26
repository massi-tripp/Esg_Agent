Opzione 1 — “Iterative chunking” (senza retrieval) più vicina alla mia idea

 do al modello il testo completo ma a chunk, iterando finché consumo tutto il file.

Come funziona:

Prendo il focused md per (azienda, anno)

Splitt in chunk da ~20k token

Per ogni chunk faccio una chiamata LLM che estrae attività

Accumulo risultati ed elimino doppioni

Se sto vicino al limite token, riduco chunk size automaticamente

Pro

Implementazione semplice

Non serve embeddings/retrieval

Massima copertura: processo “tutto” (ma solo il focused)

Contro:

Costoso (più chiamate)

Possibili duplicati

Se il focused contiene ancora molto rumore, sprechi token


Opzione 2 — “Two-pass compress then extract” (senza embeddings, ma più efficiente)

Invece di estrarre direttamente da chunk lunghi:

Pass 1: per ogni chunk chiedo al modello di restituire SOLO le righe tabellari rilevanti (es. in CSV/JSON “raw rows”)

Pass 2: do al modello l’unione delle righe raw e gli chiedi l’estrazione finale “pulita” (activity + code)

Pro:

Riduce allucinazioni (più “evidence-based”)

Riduce token nel pass 2

Ottima qualità per benchmark

Contro:

2x chiamate (ma pass2 è corto)

Più logica

Opzione 3 — RAG vera (BM25/embeddings + top-k) ✅

Indicizzo i chunk (pagine o paragrafi) e per estrarre:

query tipo “EU taxonomy eligible activities table CCM code”

prendi top-k chunk

estraggo attività dal contesto

Pro

Molto efficiente (non leggi tutto)

Scalabile a 600 aziende

Contro

Più ingegneria (vector store, indexing, ecc.)

Rischio recall se la query non becca tutto (ma mitigabile con multi-query + espansione)
