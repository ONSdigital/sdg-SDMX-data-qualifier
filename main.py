import yaml
import pandas as pd
import numpy as np
import os

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

# other_info col has "None" in, which needs to be nan
df.other_info.replace("None", np.nan, inplace=True)

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

# Make proxy boolean mask
proxy_boolean = df.other_info.str.contains(proxy_terms, na=False, regex=True)

# Create new col, 'proxy_indicator'
df['proxy_indicator'] = proxy_boolean

# Make a check: none of proxy_indicator = True should contain this official sentence
def check_if_proxies_contain_official():
    """Checks if the records which contain the proxy key words in the 
        other_info column also contain the official wording to say that 
        the stats follow the UN specification, which would imply a 
        contradiction."""
    official = "Data follows the UN specification for this indicator"
    # Isolate those records that contain the proxy keywords
    proxies_df =df[df.proxy_indicator==True] 
    # Make a boolean mask
    official_mask = proxies_df[proxies_df.proxy_indicator].other_info.str.contains(official)
    # Apply mask to proxies_df
    contractions_list = proxies_df[official_mask].index.to_list()
    for index_num in contractions_list:
        print(f"""There seem to be contradictory statements in other_info in indicator {index_num}""")
    return contractions_list

# quality check
check_if_proxies_contain_official()

# cleaning up the national_geo col
df.national_geographical_coverage = df.national_geographical_coverage.str.replace("nan","None")

# get output filename
csv_nm = os.path.join(os.getcwd(),config['outfile'])
# write out to csv
df.to_csv(csv_nm)