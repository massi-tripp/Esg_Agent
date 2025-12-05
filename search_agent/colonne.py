import pandas as pd 
df=pd.read_csv("search_agent/data/output/sustainability_reports_next_2024.csv")

#company_id,domain,query,best_link

print(df.groupby(["site_query_source"]).company_id.nunique())

'''
site_query_source
fallback               5
llm1                  74
llm1+fallback          4
llm1+llm2              7
llm1+llm2+fallback    17
llm2                   5
llm2+fallback          3
none                   6
'''
df['query']=df[['llm_queries']]

df=df[["company_id","domain","query","best_link"]]

df=df[df['best_link']!='NONE']

print(df.head())

df.to_csv("search_agent/data/output/sustainability_reports_next_2024.csv",index=False)

# python search_agent\colonne.py