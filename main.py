import yaml
import pandas as pd

# Load config
config = yaml.safe_load(open('config.yml'))


# reading all the meta data in from url
meta_url = config['meta_url']
df = pd.read_json(meta_url, orient='index')

# dropping uneeded cols
required_cols = config['required_cols']
df = df[required_cols]

# Identifying proxy terms
proxy_terms_list = config['proxy_terms']

# Getting terms into str for regex with OR | operator
str = ''
for item in proxy_terms_list:
    str += item
    if item != proxy_terms_list[-1]:
        str += "|"
proxy_terms = str
# I am sure there's a better way to do this.
# TODO: optimise proxy_terms_list creation
# proxy_terms = ''.join(config['proxy_terms']).replace(" ", "|")

# Filter the dataframe on other_info containing any of the proxy terms
df = df[df.other_info.str.contains(proxy_terms, na=False, regex=True)]

print(df.head())
print(df.shape)