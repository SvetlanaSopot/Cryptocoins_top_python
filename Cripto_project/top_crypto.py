#pip install requests
#pip install pandas
#Required: install Database Navigator Plugin

import requests
import pandas as pd
import sqlite3 as sq

# Getting top 100 coins via CoinGecko
url = 'https://api.coingecko.com/api/v3/coins/markets'
# specifying parameters according requirements
params = {
         'vs_currency': 'USD',
         'order': 'market_cap_desc'
         #'per_page': '100',
         #'page': '1'
}
# Replace specified key with your actual API key (not sure will work with individual key)
headers = {
    'User-Agent': 'Mozilla/5.0'
}
#response = requests.get(url, params=params, headers=headers)

#print(response.text)
response = requests.get(url, params=params, headers=headers)
if response.status_code == 200:
    j_data = response.json()
    print(j_data)
else:
    print('Failed to retrieve data')

# Convert data from json to Dataframe
df = pd.json_normalize(j_data)
df_filtered = df[['id','symbol', 'name', 'current_price', 'market_cap']].copy()

# Adding new column with market share for each cap calculation in %
df_filtered['sum_cap_top_percentage'] = (df_filtered['market_cap'].astype('float')/(df_filtered['market_cap'].sum())*100).round(2)

df_check = df_filtered.to_excel('test_crypt.xlsx', sheet_name='test1', index=False)

#print(df_filtered)

#Connection to DB creation

with sq.connect("crypto.db") as con:
    cur = con.cursor()
    print("connection was created")

    df_filtered.to_sql("top_crypto", con=con, if_exists="replace", index=False)

    test = pd.read_sql('''
        SELECT *
        FROM top_crypto
      ''', con)
    print(test)

# SQL to create view with TOP 10

    create_view_query = """
    CREATE VIEW IF NOT EXISTS top_10_cryptos_view AS
    SELECT id, 
           sum_cap_top_percentage AS market_share
    FROM top_crypto
    ORDER BY sum_cap_top_percentage DESC
    LIMIT 10;
    """
    cur.execute(create_view_query)
    con.commit()

    # Check results
    top_10_cryptos = pd.read_sql("SELECT * FROM top_10_cryptos_view", con)
    print(top_10_cryptos)


