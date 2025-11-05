import pandas as pd
import csv
import time

df= pd.read_csv('search_agent/data/input/test_volvo.csv')
df_info = df[['company_id', 'primary_url']]
def _keep_up_to_com(url):
	"""Return the substring of url up to and including the first occurrence of '.com'.

	If url is NaN or doesn't contain '.com', return it unchanged.
	"""
	try:
		if pd.isna(url):
			return url
	except Exception:
		# If url is not a pandas NA-compatible value, continue
		pass

	if not isinstance(url, str):
		return url

	idx = url.find('.com')
	if idx != -1:
		return url[: idx + 4]
	return url


# Trim the primary_url column to keep only up to the first '.com'
df_info['primary_url'] = df_info['primary_url'].apply(_keep_up_to_com)

print(df_info.head())

df_info.to_csv('search_agent/data/input/test_volvo.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
time.sleep(2)  # Pause to ensure file write completion