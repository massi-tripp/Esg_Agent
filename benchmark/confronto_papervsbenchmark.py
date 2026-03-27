import json
import pandas as pd
from pathlib import Path

DATA_DIR = Path("benchmark\data")

records = []

for file in DATA_DIR.glob("*.json"):
    with open(file, "r", encoding="utf-8") as f:
        data = json.load(f)

    data["company_name"] = file.stem
    records.append(data)
df = pd.DataFrame(records)

df1=df[['company_name', 'taxonomy_data']]

def to_long_activities(df1: pd.DataFrame) -> pd.DataFrame:
    df = df1.copy()

    # 1) Estraggo la lista di activities in una colonna dedicata
    def extract_activities(x):
        if isinstance(x, dict):
            return x.get("activities", [])
        return []

    df["Activities"] = df["taxonomy_data"].apply(extract_activities)

    # 2) Esplodo: una riga per ogni attività
    df = df.explode("Activities", ignore_index=True)

    # 3) Pulizia: tolgo righe vuote/null
    df["Activities"] = df["Activities"].astype("string").str.strip()
    df = df[df["Activities"].notna() & (df["Activities"] != "")]

    # 4) Rinomino company_name -> company_id
    df = df.rename(columns={"company_name": "company_id"})

    # 5) Tengo solo le colonne che ti servono
    df = df[["company_id", "Activities"]].reset_index(drop=True)

    return df

benchmark_long = to_long_activities(df1)

#print(benchmark_long.head(10))
#print(benchmark_long.shape)

df1= benchmark_long

#print(df1)

df2= pd.read_excel("benchmark\Copia_Benchmark_rag1.xlsx", sheet_name="benchmark")
print("DF2 columns:", list(df2.columns))
dfiniziale=df2.copy()
df2=df2[['company_name', 'Sub_activity_label']]
df2 = df2.rename(columns={"Sub_activity_label": "Activities"})
df2 = df2.rename(columns={"company_name": "company_id"})
print(df2)

# 1) Normalizzazione (per evitare mismatch)
df1 = df1.copy()
df2 = df2.copy()

df1["company_id"] = df1["company_id"].astype("string").str.strip().str.upper()
df2["company_id"] = df2["company_id"].astype("string").str.strip().str.upper()

df1["Activities"] = df1["Activities"].astype("string").str.strip()
df2["Activities"] = df2["Activities"].astype("string").str.strip()

# Deduplica per evitare ripetizioni inutili
df1 = df1.drop_duplicates(subset=["company_id", "Activities"]).reset_index(drop=True)
df2 = df2.drop_duplicates(subset=["company_id", "Activities"]).reset_index(drop=True)

# 2) Company_id in comune
common = sorted(set(df1["company_id"]) & set(df2["company_id"]))

print("\n=========================")
print("N company_id in comune:", len(common))
print("Lista company_id in comune:")
for cid in common:
    print(" -", cid)

# 3) Stampa attività per ciascuna company_id in comune
for cid in common:
    acts1 = sorted(set(df1.loc[df1["company_id"] == cid, "Activities"].dropna()))
    acts2 = sorted(set(df2.loc[df2["company_id"] == cid, "Activities"].dropna()))

    print("\n" + "=" * 110)
    print(f"COMPANY_ID: {cid}")
    print("-" * 110)

    '''print("Attività DF1 (benchmark paper):")
    if acts1:
        for a in acts1:
            print("  -", a)
    else:
        print("  (nessuna attività)")

    print("\nAttività DF2 (dataset manuale):")
    if acts2:
        for a in acts2:
            print("  -", a)
    else:
        print("  (nessuna attività)")'''

    # 4) Confronto: in comune / solo df1 / solo df2
    s1, s2 = set(acts1), set(acts2)

    print("\nAttività IN COMUNE (DF1 ∩ DF2):")
    inter = sorted(s1 & s2)
    if inter:
        for a in inter:
            print("  -", a)
    else:
        print("  (nessuna attività in comune)")

    print("\nAttività SOLO paper (DF1 \\ DF2):")
    only1 = sorted(s1 - s2)
    if only1:
        for a in only1:
            print("  -", a)
    else:
        print("  (nessuna)")

    print("\nAttività SOLO nostro benchmark (DF2 \\ DF1):")
    only2 = sorted(s2 - s1)
    if only2:
        for a in only2:
            print("  -", a)
    else:
        print("  (nessuna)")

###
# Secondo confronto con test_totali.csv per vedere quali company_id sono in comune tra tutte le nostre e le loro
###
'''df3= pd.read_csv(r'search_agent\data\input\test_totali.csv')

# 1) Normalizzazione (per evitare mismatch)

df3["company_id"] = df3["company_id"].astype("string").str.strip().str.upper()

df3["primary_url"] = df3["primary_url"].astype("string").str.strip()

# Deduplica per evitare ripetizioni inutili

df3 = df3.drop_duplicates(subset=["company_id", "primary_url"]).reset_index(drop=True)

# 2) Company_id in comune
common = sorted(set(df1["company_id"]) & set(df3["company_id"]))

print("\n=========================")
print("N company_id in comune:", len(common))
print("Lista company_id in comune:")
for cid in common:
    print(" -", cid)'''

dfiniziale= dfiniziale[['company_name', 'link_report', 'report_year', 'main_activity_code',
       'main_activity_label', 'environmental_objective', 'Sub_activity_code',
       'Sub_activity_label', 'pages_by_KPI']]

import re
import pandas as pd

COL = "Sub_activity_code"  # <-- se la tua colonna ha un nome diverso, cambialo qui

def normalize_sub_activity_code(x):
    """
    Esempi:
      "CCM4.5" -> "4.5"
      "CCM5.1/WTR2.1" -> "5.1/2.1"
      "CCM7.3, CCA 7.3" -> "7.3/7.3"
      "CCM7.2, CCA7.2, CE" -> "7.2/7.2"
      "" / None -> <NA>
    """
    if pd.isna(x):
        return pd.NA
    s = str(x).strip()
    if s == "":
        return pd.NA

    # Estrae tutti i numeri con possibile decimale (es. 7.3, 4.24, 2.1)
    nums = re.findall(r"\d+(?:\.\d+)?", s)
    if not nums:
        return pd.NA

    # Join con "/" (equivalente alla tua richiesta: virgole -> "/")
    return " /".join(nums)

# Applica la normalizzazione
dfiniziale[COL] = dfiniziale[COL].apply(normalize_sub_activity_code)

# (opzionale) controllo veloce
print(dfiniziale[COL].head(30))
path= r'C:\Universita\TESI\esg_agent\benchmark\Copia_Benchmark_rag1.xlsx'
#dfiniziale.to_excel(path, index=False, sheet_name="benchmark")
#print("Saved XLSX to:", path)