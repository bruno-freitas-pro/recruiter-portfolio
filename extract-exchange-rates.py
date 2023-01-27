#!mamba install pandas==1.3.3 -y
#!mamba install requests==2.26.0 -y

import requests
import pandas as pd
import json

url = "https://api.apilayer.com/exchangerates_data/latest?base=EUR&apikey=kRFfp4wOHWQ9g9RLljUfIgM5NmJ9EAvv" #Make sure to change ******* to your API key.
query = requests.get(url)
api = query.text

df = pd.read_json(api)
print(df)

df = df.drop(columns=['success', 'timestamp', 'base', 'date'], axis=1)
print('Euro based currencies: \n', df)

df.to_csv('exchange_rates_1.csv')
