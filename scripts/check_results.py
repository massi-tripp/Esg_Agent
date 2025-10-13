import pandas as pd
import os
import sys

path='C:\\Universita\\TESI\\esg_agent\\data\\output\\candidates_with_validation_FINAL.csv'

df = pd.read_csv(
    path,
    sep=",",
    quotechar='"',
    encoding="utf-8",
    engine="python",  # più tollerante ai campi multilinea
    on_bad_lines="skip"  # (opzionale) ignora righe malformate
)

# Mostriamo prime righe e colonne
#print(df.head(5))
print(df.columns)
print(df.shape)

# Calcoliamo tutte le metriche descrittive per la colonna 'score'
desc_score = df['score'].describe()
print("Descriptive statistics for 'score':")
print(desc_score)

print(df[df['score'] == df['score'].max()])
print(df[df['score'] == df['score'].min()])

desc_score = df['score'].describe()
print("Descriptive statistics for 'score':")
print(desc_score)

# Suddivido in quartili usando i valori specificati di score
df_prova = df.copy()
bins = [df_prova['score'].min()-1, 41, 64.5, 66, 67]
labels = ['Q1', 'Q2', 'Q3', 'Q4']

df_prova['quartile'] = pd.cut(df_prova['score'], bins=bins, labels=labels)

for q in labels:
    quartile_rows = df_prova[df_prova['quartile'] == q]
    print(f"\nValori 'valutazione' per {q}:")
    print(quartile_rows['valutazione'].values)

    for val in ['giusta', 'parziale', 'sbagliata']:
        filtered = df[df['valutazione'] == val]
        print(f"\nDescriptive statistics for 'score' when valutazione = '{val}':")
        print(filtered['score'].describe())

