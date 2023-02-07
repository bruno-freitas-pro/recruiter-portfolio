import requests
import pandas as pd
import json

url = "https://api.apilayer.com/exchangerates_data/latest?base=EUR&apikey=*******" #Make sure to change ******* to your API key.
query = requests.get(url)
api = query.text

df = pd.read_json(api)
print(df)

df = df.drop(columns=['success', 'timestamp', 'base', 'date'], axis=1)
print('Euro based currencies: \n', df)

df.to_csv('exchange_rates_1.csv')
