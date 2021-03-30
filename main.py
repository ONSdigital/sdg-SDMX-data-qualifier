import requests
import yaml
import pandas as pd

config = yaml.safe_load(open('config.yml'))

meta_url = config['meta_url']
data = (requests.get(meta_url)).json

required_cols = config['required_cols']

df = pd.read_json(meta_url, orient='index')
df = df[required_cols]

print(df.head())