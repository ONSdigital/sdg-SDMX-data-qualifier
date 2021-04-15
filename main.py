import yaml
import pandas as pd
import numpy as np
import os
import re

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
    """Joins items of a list with the regex OR operator

    Args:
        termslist (list): list of terms (strings) that should be joined
            with the | operator. 

    Returns:
        re.Pattern: the regex pattern in unicode
    """    
    regex_terms = ''
    for item in termslist:
        if item == termslist[0]:
            regex_terms += ",\\b"
        regex_terms += item
        if item != termslist[-1]:
            regex_terms += ",\\b|\\b"
        else:
            regex_terms += ",\\b"
    # raw_regex_terms = r"{}".format(regex_terms)
    return re.compile(regex_terms)

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
    proxies_df = df[df.proxy_indicator==True] 
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

# Get the disagregation report
disag_df = get_disag_report()

# checking if Disaggregations col contains keywords geo_disag_terms
geo_disag_terms_list = config['geo_disag_terms']
# Join terms in list with regex or operator
geo_disag_terms = regex_or_str(geo_disag_terms_list)
# Creating boolean
print("Searching for ", geo_disag_terms)
disag_boolean = disag_df.Disaggregations.str.contains(geo_disag_terms, regex=True)
print(disag_boolean.value_counts())
disag_df['geo_disag'] = disag_boolean   

# Drop the now uneeded Disaggregations cols
disag_df.drop(['Disaggregations', 'Number of disaggregations'], axis=1, inplace=True)
# Set index and merge on index
disag_df.set_index("Indicator", inplace=True)
# Left joining df onto disag_df
df = df.join(disag_df)

# creating local variable to map for uk coverage
uk_terms_list = config['uk_terms'] 

def check_only_uk_data(nat_geo_series, geo_disag_series, uk_terms):
    """Checks if Both of these conditions need to met
        1) value in the national_geographical_coverage is listed in uk_terms 
        2) value in geo_disag column is FALSE
        and returns True if conditions are met, False otherwise. 
        Function to be used to map/apply to create new series 
    Args:
        nat_geo_series (pd.Series): The national_geographical_coverage series
        geo_disag_series (pd.Series): The geo_disag series
    Returns:
        Boolean : True if conditions are met, False otherwise.
    """
    if nat_geo_series in uk_terms and geo_disag_series is False:
        return True
    return False

# Applying the check_only_uk_data to map True/False to new 'only_uk_data' series
df['only_uk_data'] = (df.apply(lambda x:
                        check_only_uk_data(x.national_geographical_coverage,
                        x.geo_disag,
                        uk_terms_list),
                        axis=1))

# Making UK terms uniform --> United Kingdom
uk_terms_reg = regex_or_str(uk_terms_list)
print("Searching for regex string", uk_terms_reg)
df.national_geographical_coverage = df.national_geographical_coverage.str.replace(uk_terms_reg, "United Kingdom", regex=True)

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
print("Querying df for ", query_string)
inc_df = df.query(query_string)
print(f"The shape of inc_df is {inc_df.shape}")

# Getting unique column headers in included datasets only
disag_series = get_disag_report().loc[:,["Indicator", "Disaggregations"]].set_index("Indicator")
filtered_disags_df = disag_series.join(inc_df, how="inner")
print(f"The shape of filtered_disags_df is {filtered_disags_df.shape}")
split_disags = filtered_disags_df["Disaggregations"].str.split(", ")
unique_disags = split_disags.explode().unique()
print(f"""These are the unique disaggregations, {unique_disags}
Length of disags = {len(unique_disags)})
""")
