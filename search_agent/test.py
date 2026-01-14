import pandas as pd 
df=pd.read_csv("search_agent/data/output/sustainability_reports_next_2024.csv")
#questo codice serve per capire quale metodo di query ha funzionato meglio
#company_id,domain,query,best_link

print(df.groupby(["site_query_source"]).company_id.nunique())

df['query']=df[['llm_queries']]

df=df[["company_id","domain","query","best_link"]]

df=df[df['best_link']!='NONE']

print(df.head())

df.to_csv("search_agent/data/output/sustainability_reports_next_2024.csv",index=False)

# python search_agent\colonne.py

'''
fino a 131(109 corrette) sulle totali, ovvero VESTAS WIND SYSTEMS A/S
per ora circa 100 corrette, quando tavily da dei pdf a gpt al 90% son corretti, rimangono molti casi in cui tavily non trova nulla. Posso provare con tavily map o tavily crawl per vedere se migliora.
Adesso testo con altre 70, quindi fino a 179, STORA ENSO OYJ
Per ora 209 su 220 corrette  (95%)
Aggiunte altre 60, quindi fino all'indice 239, fino a BECHTLE AG. Quelle di prima che non trovavano finiscono a AEGON LTD.
tot su 250 su 280, ma con un solo run, aggiungo altre 20 per arrivare alla cifra tonda di 300 totali, fino all'indice 259, quindi fino ad ACCOR.
ora 281 su 300 corrette, 93,67%

da 260 a 361 fino a THE SAGE GROUP PLC
sulle 120(comprese le 20 che prima sbagliava) runnate ora, abbiamo questi risultati:
site_query_source
fallback               5
llm1                  74
llm1+fallback          4
llm1+llm2              7
llm1+llm2+fallback    17
llm2                   5
llm2+fallback          3
none                   6
ora 370 su 402, 92,03% corrette.
Ne aggiungo 18 quindi da 362 fino a 379, fino a L E LUNDBERGFORETAGEN AB
420 totali, ora metto a runnare le 50 restanti. Ho anche aggiunto un altro iter di gpt per le query.
Risultati:
site_query_source
fallback                    2
llm1                       32
llm1+fallback               1
llm1+llm2                   1
llm1+llm2+fallback          1
llm1+llm2+llm3              1
llm1+llm2+llm3+fallback     4
llm1+llm3                   2
llm2+fallback               1
llm2+llm3                   1
llm2+llm3+fallback          1
llm3                        1
llm3+fallback               1
none                        1
tot                        50
390 su 432 corrette, 90,28%
'''