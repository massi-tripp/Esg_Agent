import pandas as pd
import csv
import time
'''
df= pd.read_csv('search_agent/data/input/companies_clean.csv')
df_info = df[['company_id', 'primary_url']]

#print(df_info.head())

# --- Print duplicates based on company_id ---
# Rows that have duplicated company_id (shows all occurrences)
duplicates = df_info[df_info.duplicated(subset=['company_id'], keep=False)]
if not duplicates.empty:
	print('\nDuplicate rows by company_id:')
	print(duplicates.sort_values('company_id'))
else:
	print('\nNo duplicate rows found by company_id.')

# Also print the list of duplicated company_id values (unique)
dup_ids = df_info['company_id'][df_info['company_id'].duplicated(keep=False)].unique()
if dup_ids.size:
	print('\nDuplicated company_id values:')
	for cid in dup_ids:
		print('-', cid)
else:
	print('\nNo duplicated company_id values found.')
print(df_info.shape)

df_info.to_csv('search_agent/data/input/test_totali.csv', index=False)
time.sleep(2)  # Pause to ensure file write completion
'''

df= pd.read_csv('search_agent/data/input/test_totali.csv', sep=',')
df_trovati= pd.read_csv('search_agent/data/input/test_corretti.csv', sep=',')

print(df_trovati.shape)

if 'company_id' in df.columns and 'company_id' in df_trovati.columns:
	df_fin = df[~df['company_id'].isin(df_trovati['company_id'])].copy()
	print('Original df shape:', df.shape)
	print('Filtered df_fin shape:', df_fin.shape)
	print('\nSample of filtered rows:')
	print(df_fin.head())
else:
	print('company_id column missing in one of the dataframes; df_fin set to original df')
	df_fin = df.copy()
	print('df_fin shape:', df_fin.shape)

df_fin.to_csv('search_agent/data/input/test_totali.csv', index=False)
time.sleep(2) 
