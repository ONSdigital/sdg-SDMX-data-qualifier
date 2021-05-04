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
            regex_terms += "\\b"
        regex_terms += item
        if item != termslist[-1]:
            regex_terms += "\\b|\\b"
        else:
            regex_terms += "\\b"
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

csv_nm = os.path.join(os.getcwd(),'disag_df.csv')

# Drop the now uneeded Disaggregations cols
disag_df.drop(['Disaggregations', 'Number of disaggregations'], axis=1, inplace=True)
# Set index and merge on index
disag_df.set_index("Indicator", inplace=True)
# Left joining df onto disag_df
df = df.join(disag_df)

# Replacing nans with False in the geo_disag series 
df.geo_disag.replace(np.nan,False, inplace=True)

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

print("========================Printing df")
print(df.head(20))

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

# Manually dropping '13-2-2', '17-5-1', '17-6-1' from df because
# they have been changed into the 2020 indicators, so we do not want to 
# consider them for SDMX at this point
inc_df = inc_df.drop(['13-2-2', '17-5-1', '17-6-1'], axis=0)

print(f"The shape of inc_df is {inc_df.shape}")

# Getting unique column headers in included datasets only
disag_series = get_disag_report().loc[:,["Indicator", "Disaggregations"]].set_index("Indicator")
# Filtering 
filtered_disags_df = disag_series.join(inc_df, how="inner")
print(f"The shape of filtered_disags_df is {filtered_disags_df.shape}")
split_disags = filtered_disags_df["Disaggregations"].str.split(", ")
unique_disags = split_disags.explode().unique()

# Column names should be sdg_column_name, SDMX_concept_name (empty column)
df_build_dict ={
    "sdg_column_name" : unique_disags,
    "SDMX_concept_name" : np.empty_like(unique_disags)
    }
# write out the csv
pd.DataFrame(data=df_build_dict).to_csv("SDG_column_names.csv")

#Ticket #19
# The excel file should have been manually updated with mappings
# from SDG column names to their SDMX concept name mapping
# Import the manually updated file
EXCEL_FILE = config["manual_excel_file_name"]
WANTED_COLS = ["sdg_column_name", "SDMX_concept_name"]

def manual_excel(excel_file, wanted_cols):
    try:
        df = pd.read_excel(excel_file, 
                            usecols=wanted_cols,
                            engine="openpyxl")
        df.dropna(axis=0, subset=["SDMX_concept_name"], inplace=True)
        return df    
    except Exception as ex:
        print(f"""There has been an error with the import of the 
                    manually updated Excel file \n Check that the 
                    dependecies (openpyxl) are installed and that 
                    the file is named correctly in config
                    \n\n 
                    Error Message: {ex}""")

# Make a df of the cols         
mapped_columns_df = manual_excel(EXCEL_FILE, WANTED_COLS)
print(mapped_columns_df)

# Ticket 20  - Get all disagregation values and match them with their
# respective column titles. Output as a df and csv  
URL_prefix = f"https://sdgdata.gov.uk/sdg-data/values--disaggregation--"
URL_suffix = ".csv"
col_name_slugs = mapped_columns_df.sdg_column_name.str.lower().str.replace(" ","-")
mapped_columns_df["disag_val_urls"] = URL_prefix + col_name_slugs + URL_suffix
# Empty lists to capture the column names and values
col_names = []
col_values = []
# Grab the column names and their respective URL values csv resource
col_series = mapped_columns_df.sdg_column_name
value_urls = mapped_columns_df.disag_val_urls
for col_name,url in zip(col_series, value_urls):
    # Get all the value disaggregations for each column 
    values = pd.read_csv(url, usecols=["Value"]).to_numpy()
    for value in values:
        col_names.append(col_name)
        col_values.append(*value)
emptycells = np.empty_like(col_names)
construct_dict = {"column_value":col_values,
                  "sdg_column_name":col_names,
                  "SDMX_code": emptycells,
                  "comments":emptycells}

# Creating the dataframe of vals and column match
val_col_pairs_df = pd.DataFrame(construct_dict)
val_col_pairs_df.to_csv("val_col_pairs-#20.csv")