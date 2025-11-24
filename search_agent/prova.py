import pandas as pd

# === Percorsi dei file ===
path_1 = r"C:\Universita\TESI\esg_agent\search_agent\data\input\test_corretti.csv"
path_2 = r"C:\Universita\TESI\esg_agent\search_agent\data\output\report_corretti.csv"

# === Caricamento ===
df1 = pd.read_csv(path_1)
df2 = pd.read_csv(path_2)

# === Trova i company_id presenti nel primo ma non nel secondo ===
missing = df1[~df1["company_id"].isin(df2["company_id"])]

print("\n► COMPANY_ID presenti in test_corretti.csv ma NON in report_corretti.csv:")
print(missing)
