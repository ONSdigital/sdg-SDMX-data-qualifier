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

# Remove archived indicators
df = df[~df.index.str.contains("archived")]

def get_disag_report():
    """Gets the disagregation report from the URL specified in the config file
        then it changes the indicator names so they are the same as metadata df"""
    # Pulling disagg report
    disag_url = config['disag_url']
    disag_df = pd.read_csv(disag_url)
    # Alter the indicator names so they match the other df
    disag_df.Indicator = disag_df.Indicator.str[1:]
    return disag_df

disag_df = get_disag_report()

# checking if Disaggregations col contains keywords geo_disag_terms
geo_disag_terms_list = config['geo_disag_terms']
# Join terms in list with regex or operator
geo_disag_terms = regex_or_str(geo_disag_terms_list)
# Creating boolean
disag_boolean = disag_df.Disaggregations.str.contains(geo_disag_terms, regex=True)
disag_df['geo_disag'] = disag_boolean

# Drop the now uneeded Disaggregations cols
disag_df.drop(['Disaggregations', 'Number of disaggregations'], axis=1, inplace=True)
# Set index and merge on index
disag_df.set_index("Indicator", inplace=True)
# Left joining df onto disag_df
df = df.join(disag_df)

# Making UK terms uniform --> United Kingdom
uk_terms = regex_or_str(uk_terms)
df.national_geographical_coverage = df.national_geographical_coverage.str.replace(uk_terms, "United Kingdom", regex=True)

# Including 8-1-1 by setting proxy to false
df.loc['8-1-1', 'proxy_indicator'] = False

# sorting the index of the main df
df = df.sort_index()

# get output filename
csv_nm = os.path.join(os.getcwd(),config['outfile'])
# write out to csv
df.to_csv(csv_nm)

#Get SDMX suitability test
suit = config['suitability_test']

# Build logic test query string
query_string=''
for col_nm,col_val in suit.items():
    if col_val not in [True, False]:
        col_val = f"'{col_val}'"
    query_string+=f"{col_nm}=={col_val}"
    if col_nm!=list(suit.keys())[-1]:
        query_string+=" & "



# make the df of included indicators
inc_df = df.query(query_string)

# Getting unique column headers in included datasets only
disag_series = get_disag_report().loc[:,["Indicator", "Disaggregations"]].set_index("Indicator")
filtered_disags = disag_series.join(inc_df, how="inner")
split_disags = filtered_disags["Disaggregations"].str.split(", ")
unique_disags = split_disags.explode().unique()
print(unique_disags)
