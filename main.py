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

# Getting terms into str for regex with OR | 
def regex_or_str(termslist):
    "Joins items of a list with the regex OR operator"
    regex_terms = ''
    for item in termslist:
        regex_terms += item
        if item != termslist[-1]:
            regex_terms += "|"
    return regex_terms

proxy_terms = regex_or_str(proxy_terms_list)

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

# creating mapping for uk coverage
uk_terms = config['uk_terms']
df['only_uk_data'] = df.national_geographical_coverage.map(lambda x: x in uk_terms)

# Pulling disagg report
disag_url = config['disag_url']
disag_df = pd.read_csv(disag_url)

# checking if Disaggregations col contains keywords geo_disag_terms
geo_disag_terms_list = config['geo_disag_terms']
# Join terms in list with regex or operator
geo_disag_terms = regex_or_str(geo_disag_terms_list)
# Creating boolean
disag_boolean = disag_df.Disaggregations.str.contains(geo_disag_terms, regex=True)
disag_df['geo_disag'] = disag_boolean
# Alter the indicator names so they match the other df
disag_df.Indicator = disag_df.Indicator.str[1:]
# Drop the now uneeded Disaggregations cols
disag_df.drop(['Disaggregations', 'Number of disaggregations'], axis=1, inplace=True)
# Set index and merge on index
disag_df.set_index("Indicator", inplace=True)
# Left joining df onto disag_df
df = df.join(disag_df)

# Making UK terms uniform --> United Kingdom
uk_terms = regex_or_str(uk_terms)
df.national_geographical_coverage = df.national_geographical_coverage.str.replace(uk_terms, "United Kingdom", regex=True)

print(df.head())

# get output filename
csv_nm = os.path.join(os.getcwd(),config['outfile'])
# write out to csv
df.to_csv(csv_nm)