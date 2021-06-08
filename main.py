import yaml
import pandas as pd
import numpy as np
import os
import re
from functools import cache
from fuzzywuzzy import process, fuzz

from time import sleep

# Load config
config = yaml.safe_load(open('config.yml'))

# reading all the meta data in from url
meta_url = config['meta_url']
meta_data_df = pd.read_json(meta_url, orient='index')

# Verbose setting for print outs
VERBOSE = False


def keep_needed_df_cols(df: pd.DataFrame, required_col_list: list):
    """Drops uneeded columns from a datframe using a list of the
        required columns.

    Args:
        df (pd.DataFrame): the dataframe to be changed
        required_col_list (list): a list of exact column names that
            are to be kept
    """
    df = df[required_col_list]
    return df


# dropping uneeded cols
required_cols = config['required_cols']
meta_data_df = keep_needed_df_cols(meta_data_df, required_cols)

# Identifying proxy terms
proxy_terms_list = config['proxy_terms']

# other_info col has "None" in, which needs to be nan
meta_data_df.other_info.replace("None", np.nan, inplace=True)


def regex_or_str(termslist: list):
    """Joins items of a list with the regex OR operator
        Getting terms of the list into str for regex with OR |

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
proxy_boolean = (meta_data_df
                 .other_info
                 .str.contains(proxy_terms, na=False, regex=True))

# Create new col, 'proxy_indicator'
meta_data_df['proxy_indicator'] = proxy_boolean


def check_if_proxies_contain_official():
    """Checks if the records which contain the proxy key words in the
        other_info column also contain the official wording to say that
        the stats follow the UN specification, which would imply a
        contradiction."""
    official = "Data follows the UN specification for this indicator"
    # Isolate those records that contain the proxy keywords
    proxies_df = meta_data_df[meta_data_df.proxy_indicator == True]
    # Make a boolean mask
    official_mask = (proxies_df[proxies_df.proxy_indicator]
                     .other_info
                     .str.contains(official))
    # Apply mask to proxies_df
    contradictions_list = proxies_df[official_mask].index.to_list()
    for index_num in contradictions_list:
        print(f"""There seem to be contradictory
              statements in other_info in indicator {index_num}""")
    return contradictions_list


# quality check
# Make a check: none of proxy_indicator = True
# should contain this official sentence
check_if_proxies_contain_official()

# cleaning up the national_geo col
meta_data_df.national_geographical_coverage = (meta_data_df
                                               .national_geographical_coverage
                                               .str.replace("nan", "None"))

# Remove archived indicators
meta_data_df = meta_data_df[~meta_data_df.index.str.contains("archived")]


def get_disag_report(disag_url):
    """Gets the disagregation report from the URL specified in the
        config file then it changes the indicator names so they are
        the same as metadata df"""
    # Pulling disagg report
    disag_df = pd.read_csv(disag_url)
    # Alter the indicator names so they match the other df
    disag_df.Indicator = disag_df.Indicator.str.lstrip("#")
    return disag_df


# Get the disagregation report
DISAG_URL = config['disag_url']
disag_df = get_disag_report(DISAG_URL)

# checking if Disaggregations col contains keywords geo_disag_terms
GEO_DISAG_TERMS_LIST = config['geo_disag_terms']
# Join terms in list with regex or operator
geo_disag_terms = regex_or_str(GEO_DISAG_TERMS_LIST)
# Creating boolean
print("Searching for ", geo_disag_terms)
disag_boolean = (disag_df
                 .Disaggregations
                 .str.contains(geo_disag_terms, regex=True))
if VERBOSE:
    print(disag_boolean.value_counts())

disag_df['geo_disag'] = disag_boolean

csv_nm = os.path.join(os.getcwd(), config["disag_outfile"])

# Drop the now uneeded Disaggregations cols
required_disag_cols = config["required_disag_cols"]
disag_df = keep_needed_df_cols(disag_df, required_disag_cols)

# Set index and merge on index
disag_df.set_index("Indicator", inplace=True)
# Left joining df onto disag_df
meta_data_df = meta_data_df.join(disag_df)

# Replacing nans with False in the geo_disag series
meta_data_df.geo_disag.replace(np.nan, False, inplace=True)

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


# Applying check_only_uk_data to map True/False to 'only_uk_data' series
meta_data_df['only_uk_data'] = (meta_data_df.apply(
                                lambda x: check_only_uk_data
                                (x.national_geographical_coverage,
                                 x.geo_disag,
                                 uk_terms_list),
                                axis=1))

# Making UK terms uniform --> United Kingdom
uk_terms_reg = regex_or_str(uk_terms_list)
print("Searching for regex string", uk_terms_reg)
meta_data_df.national_geographical_coverage = (meta_data_df
                                               .national_geographical_coverage
                                               .str.replace(uk_terms_reg,
                                                            "United Kingdom",
                                                            regex=True))

# Including 8-1-1 by setting proxy to false
meta_data_df.loc['8-1-1', 'proxy_indicator'] = False


# ticket #29
def df_sorter(df: pd.DataFrame, sort_order: list) -> pd.DataFrame:
    """Sorts a dataframe which has indicators as strigns, such as
        '1-2-1', then it sorts them according to the hierache in the
        config file.
        Goal: numeric
            then by
        Target: numeric then alphabetic
            then by
        Target: numeric

    Args:
        df (pd.DataFrame): A pandas dataframe to be sorted, which has
            indicators as its index
        sort_order (list): the order in which the indicators should be
            sort

    Returns:
        pd.DataFrame: a pandas dataframe sorted as required.
    """
    df.reset_index(inplace=True)
    df.rename(columns={"index": "g-t-i"}, inplace=True)
    # goal_targ_ind = df["g-t-i"].str.split("-", expand=True)
    df[sort_order] = meta_data_df['g-t-i'].str.split("-", expand=True)
    df.sort_values(sort_order, axis=0, inplace=True)
    df.drop([*sort_order, "g-t-i"], axis=1)
    df.set_index("g-t-i", inplace=True)
    return df


sort_order = config["sort_order"]
meta_data_df = df_sorter(meta_data_df, sort_order)

print("========================Printing df")
print(meta_data_df.head(20))

# Get output filename from config
csv_nm = os.path.join(os.getcwd(), config['meta_outfile'])
# write out to csv
meta_data_df.to_csv(csv_nm)

# Get SDMX suitability test
suitability_dict = config['suitability_test']


def build_query(query_words: dict) -> str:
    """
    Builds an SQL style query string from a dictionary
        where keys are column titles and values are values
        that the query will test for.

    Args:
        query_words (dict): a dictionary where keys are column
            titles and values are values

    Returns:
        str: an string that can be used as an SQL query
    """
    query_string = ''
    for col_nm, col_val in query_words.items():
        if col_val not in [True, False]:
            col_val = f"'{col_val}'"
        query_string += f"{col_nm}=={col_val}"
        if col_nm != list(query_words.keys())[-1]:
            query_string += " & "
    return query_string


# make the df of included indicators
query_string = build_query(suitability_dict)
print("Querying meta_data_df for ", query_string)
inc_df = meta_data_df.query(query_string)

# Manually dropping '13-2-2', '17-5-1', '17-6-1' from df because
# they have been changed into the 2020 indicators, so we do not want to
# consider them for SDMX at this point
inc_df = inc_df.drop(config["2020indicators"], axis=0)

print(f"The shape of inc_df is {inc_df.shape}")

# Getting unique column headers in included datasets only
disag_series = (get_disag_report(DISAG_URL)
                .loc[:, ["Indicator", "Disaggregations"]]
                .set_index("Indicator"))

# Filtering
filtered_disags_df = disag_series.join(inc_df, how="inner")
print(f"The shape of filtered_disags_df is {filtered_disags_df.shape}")
split_disags = filtered_disags_df["Disaggregations"].str.split(", ")
unique_disags = split_disags.explode().unique()

# Column names should be sdg_column_name, SDMX_concept_name (empty column)
df_build_dict = {
    "sdg_column_name": unique_disags,
    "SDMX_concept_name": np.empty_like(unique_disags)
    }

# write out the csv
pd.DataFrame(data=df_build_dict).to_csv(config["sdg_cols_outfile"])

# Ticket #19
# The excel file should have been manually updated with mappings
# from SDG column names to their SDMX concept name mapping
# Import the manually updated file
EXCEL_FILE = config["manual_excel_file_name"]
WANTED_COLS = ["sdg_column_name", "SDMX_concept_name"]
DROP_COLS = ["SDMX_concept_name"]


def manual_excel(excel_file, wanted_cols, drop_cols=None):
    try:
        df = pd.read_excel(excel_file,
                           usecols=wanted_cols,
                           engine="openpyxl")
        if drop_cols:
            df.dropna(axis=0, subset=drop_cols, inplace=True)
        print(f"{excel_file} has been imported.")
        return df
    except Exception as ex:
        print(f"""There has been an error with the import of the
                    manually updated Excel file \n Check that the
                    dependecies (openpyxl) are installed and that
                    the file is named correctly in config
                    \n\n
                    Error Message: {ex}""")


# Make a df of the cols
mapped_columns_df = manual_excel(EXCEL_FILE, WANTED_COLS, DROP_COLS)

# Ticket 20  - Get all disagregation values and match them with their
# respective column titles. Output as a df and csv
URL_prefix = "https://sdgdata.gov.uk/sdg-data/values--disaggregation--"
URL_suffix = ".csv"
col_name_slugs = (mapped_columns_df
                  .sdg_column_name
                  .str.lower()
                  .str.replace(" ", "-"))

mapped_columns_df["disag_val_urls"] = URL_prefix + col_name_slugs + URL_suffix

# Empty lists to capture the column names and values
col_names = []
col_values = []
# Grab the column names and their respective URL values csv resource
col_series = mapped_columns_df.sdg_column_name
value_urls = mapped_columns_df.disag_val_urls
for col_name, url in zip(col_series, value_urls):
    # Get all the value disaggregations for each column
    values = pd.read_csv(url, usecols=["Value"]).to_numpy()
    for value in values:
        col_names.append(col_name)
        col_values.append(*value)
emptycells = np.empty_like(col_names)
construct_dict = {"column_value": col_values,
                  "sdg_column_name": col_names,
                  "SDMX_code": emptycells,
                  "comments": emptycells}

# Creating the dataframe of vals and column match
val_col_pairs_df = pd.DataFrame(construct_dict)

# Output for #20
val_col_pairs_df.to_csv("val_col_pairs-#20.csv")

# Ticket 21 Swap SDG_column_names col for SDMX_column_name in
# sdg_col_names_vals_df

@cache  # Caching provides a 20x speed-up here
def get_SDMX_colnm(search_value):
    # TODO: merge this with the pd_vlookup function
    """Gets the SDMX equivilent of all of the SDG column names,
        by looking up the SDG column name. To be used on a the
        sdg_column_name column of the dataframe containing the
        SDG column names. It looks up the values sdg_column_name
        column and returns their equivilent from SDMX_concept_name
        column of `mapped_columns_df` which came from the
        manual-input Excel file.

    Args:
        search_value (str): strings from the sdg_column_name column

    Returns:
        str: SDMX concept name equivilent
    """
    val_df = (mapped_columns_df[mapped_columns_df
                                .sdg_column_name == search_value])
    row = val_df.index[0]
    val = val_df.loc[:].at[row, "SDMX_concept_name"]
    return val


def pd_vlookup(search_value, search_df, search_series_nm, return_series_nm):
    """A more generic version of the get_SDMX_colnm function"""
    val_df = search_df[search_series_nm == search_value]
    row = val_df.index[0]
    val = val_df.loc[:].at[row, return_series_nm]
    return val


# Creating a new column in val_col_pairs df called sdmx_col_nm
# which contains the SDMX equivilent of all of the SDG column names
val_col_pairs_df["sdmx_col_nm"] = (val_col_pairs_df
                                   .sdg_column_name
                                   .apply(lambda x: get_SDMX_colnm(x)))

# Dropping the old SDG column names
val_col_pairs_df.drop(columns=["sdg_column_name"], inplace=True)
# Renaming the SDMX col names as "column_name"
val_col_pairs_df.rename(columns={"sdmx_col_nm": "column_name",
                        "SDMX_code": "sdmx_code"},
                        inplace=True)
# Reordering columns
order_cols = ['column_name', 'column_value', 'sdmx_code', 'comments']
val_col_pairs_df = val_col_pairs_df[order_cols]

# De-duping column_value and column_name because there will be some duplicates
before_shape = val_col_pairs_df.shape
val_col_pairs_df.drop_duplicates(subset=["column_name",
                                         "column_value"],
                                 inplace=True)
after_shape = val_col_pairs_df.shape
print(f"""De-depuping finished.
      {before_shape[0] - after_shape[0]} records were dropped.""", end="")

# Outputting result to csv
val_col_pairs_df.to_csv("SDMX_colnames_values_matched-#21.csv")

# Import DSD
dsd_xls = pd.ExcelFile(config['dsd_url'])

concept_sch = (pd.read_excel(dsd_xls,
                             engine="openpyxl",
                             sheet_name="3.Concept Scheme",
                             skiprows=11,
                             header=0,
                             usecols=[2, 7]))


def get_dsd_tab_name(concept_sch, concept_name):
    res = concept_sch[concept_sch['Concept Name:en'] == concept_name]
    if res.shape[0] > 0:
        row_num = res.index[0]
        return res.at[row_num, "Code List or Uncoded"]
    else:
        print(f"Skipping {concept_name}; not found in the concept scheme")
        return None


dsd_code_name_list_dict = {}
for col_name in val_col_pairs_df.loc[:, 'column_name'].unique():
    tab_name = get_dsd_tab_name(concept_sch, col_name)
    if not tab_name:
        print(f"Warning: No tab name for {col_name} was found")
        continue
    # Get the SDMX data from the correct tab in the spreadsheet.
    dsd_from_tab = pd.read_excel(dsd_xls,
                                 engine="openpyxl",
                                 sheet_name=f"{tab_name.upper()}",
                                 skiprows=12,
                                 header=0,
                                 usecols=[0, 4])
    # Column 0 is the SDMX code, 1 is the SDMX name (more human friendly)
    names = dsd_from_tab.iloc[:, 1].to_list()
    codes = dsd_from_tab.iloc[:, 0].to_list()
    # Put the SDMX codes and names into a dictionary for user choosing later.
    dsd_code_name_list_dict[col_name] = {name: code for name, code
                                         in zip(names, codes)}


def valid_int_input(prompt, highest_input):
    while True:
        try:
            inp = int(input(prompt))
            if inp > highest_input:
                print("\n That value is too high. Try again")
                sleep(0.5)
                continue
            elif inp < 1:
                print("\n That value is too low. Try again")
                sleep(0.5)
                continue
            return inp
        except ValueError as e:
            print("Not a proper integer! Try it again")
            print(e)


input_prompt = """\n The SDG value to be matched is
    '{}'
Choose a number from above options to select best matching SDMX value.
Or press {} if there is no suitable match:  """


def get_name_list(column_name, dsd_code_list_dict=dsd_code_name_list_dict):
    return dsd_code_list_dict[column_name].keys()


def suggest_dsd_value(column_name: str, sdg_column_value: str,
                      dsd_code_list_dict: dict):
    """This is the iterrows solution

    Args:
        column_name ([type]): [description]
        sdg_column_value ([type]): [description]
        dsd_code_list_dict (dict): [description]

    Returns:
        [type]: [description]
    """

    dsd_code_list = get_name_list(column_name, dsd_code_list_dict)
    possible_matches = process.extract(sdg_column_value,
                                       dsd_code_list,
                                       scorer=fuzz.partial_token_sort_ratio,
                                       limit=8)
    if any(possible_matches):
        count_matches = len(possible_matches)
        # get the index/position of the last option in the list, for None
        last_option_index = count_matches+1
        # Present the match options to the user
        for option_index, match in enumerate(possible_matches):
            print(f"{option_index+1}: {match[0]} : {match[1]}%")
        print(f"{count_matches+1}: None")
        # Get user input to choose the best match from the options
        prompt = input_prompt.format(sdg_column_value, last_option_index)
        user_match_choice = (valid_int_input
                             (prompt,
                              highest_input=last_option_index)-1)
        if user_match_choice < count_matches:
            selected_match = possible_matches[user_match_choice][0]
            print(f"\nChosen value: {selected_match}")
            sleep(0.75)
            sdmx_code_ = dsd_code_list_dict[column_name][selected_match]
            return sdmx_code_, "Matching SDG value was manually chosen"
        elif user_match_choice == count_matches:
            # This should catch the "None" option, usually 9
            return "None", f"Manual. No matches chosen for {sdg_column_value}"
        else:
            print("There has been some exceptional error")
    return "None", f"Automatic. No matches found for {sdg_column_value}"


# ticket #46 function to map "Name:en" to "Code*"
# Set map_manual_names_to_codes if you have a manually edited file
# that needs mapping from SDMX names (English) to SDMX concept codes.
map_manual_names_to_codes = False

if map_manual_names_to_codes:
    # Correcting the manually inputted SDMX names to SDMX codes
    # load the manual values df
    manual_values_df = pd.read_excel("manually_chosen_values.xlsx")
    # create a mapping dictionary that is specific to each column
    whole_df_mapper_dict = {}
    # Grouping by 'column_name' yields 1) the 'column_name' and 2) a df with
    # only values that contain that particular 'column_name' value
    for col_name, manual_values_df in manual_values_df.groupby("column_name"):
        # Get the mapping dictionary for those particular values
        mapping_dict = dsd_code_name_list_dict[col_name]
        for value in manual_values_df.sdmx_code.unique():
            value = value.strip("'")
            whole_df_mapper_dict[f'\'{value}\''] = mapping_dict.get(value)
    # Reload the "manually_chosen_values.xlsx" file
    manual_values_df = pd.read_excel("manually_chosen_values.xlsx")
    # Map all the SDMX names to SDMX codes
    manual_values_df["sdmx_code"] = (
        manual_values_df.sdmx_code.map(whole_df_mapper_dict))
    # Write the corrected df out to Excel
    manual_values_df.to_excel("manually_chosen_values_corrected.xlsx")

manually_choose_code_mapping = False

if manually_choose_code_mapping:
    # Setting up a dictionary to ready for the construction of the
    # dataframe for output
    code_comments_dict = {"index_code": [], "sdmx_code": [], "comments": []}

    all_records = val_col_pairs_df.shape[0]
    for i, row in enumerate(val_col_pairs_df.iterrows()):
        print(f"Progress: {(i/all_records)*100:.2f}%")
        index_number = row[0]
        sdmx_code, comments = (suggest_dsd_value
                               (row[1].column_name,
                                row[1].column_value,
                                dsd_code_name_list_dict))
        print(f"\nCorresponding code: {sdmx_code}\n")
        sleep(1)
        code_comments_dict["index_code"].append(index_number)
        code_comments_dict["sdmx_code"].append(f"'{sdmx_code}'")
        code_comments_dict["comments"].append(comments)

    match_values_df = (pd.DataFrame
                       .from_dict(code_comments_dict)
                       .set_index("index_code"))

    match_values_df.rename(columns={"index_code": "index"}, inplace=True)

    val_col_pairs_df.drop(['sdmx_code', 'comments'], axis=1, inplace=True)
    val_col_pairs_df = val_col_pairs_df.join(match_values_df)

    print(val_col_pairs_df.sample(20))

    val_col_pairs_df.to_excel("manually_chosen_values.xlsx")
    val_col_pairs_df.to_csv("manually_chosen_values", quotechar="'")


# Ticket 44 Code Mapping in correct format -
# https://github.com/ONSdigital/sdg-SDMX-data-qualifier/issues/44
WANTED_COLS_44 = ["column_value", "column_name", "sdmx_code"]
code_mapping_44_df = (manual_excel
                      ("manually_chosen_values_corrected.xlsx",
                       WANTED_COLS_44))

# The concept names need mapping to the concept IDs. This comes from the DSD
# Import the needed columns from the DSD for the name-->ID mapping
concept_id_names_df = pd.read_excel(dsd_xls,
                                    engine="openpyxl",
                                    sheet_name="3.Concept Scheme",
                                    skiprows=11,
                                    header=0,
                                    usecols=[1, 7])
# Get a dictionary for the name-->ID mapping, with this slightly hacky code
concept_id_names_df.rename(columns={'Concept Name:en': "concept_name",
                                    'Concept ID': 'concept_id'},
                           inplace=True)
concept_id_names_mapping_dict = (concept_id_names_df
                                 .set_index("concept_name")
                                 .to_dict()['concept_id'])
# Create the Dimension column as required in ticket 44
code_mapping_44_df['Dimension'] = (code_mapping_44_df
                                   .column_name
                                   .map(concept_id_names_mapping_dict))
# column_name was only needed for mapping - dropping it now
code_mapping_44_df.drop("column_name", axis=1, inplace=True)
code_mapping_44_df.rename(columns={'sdmx_code': "Value",
                                   'column_value': 'Text'},
                          inplace=True)
# Reorder the columns as required in ticket 44.
ORDER_44 = ['Text', 'Dimension', 'Value']
code_mapping_44_df = code_mapping_44_df[ORDER_44]
# Drop empty rows
code_mapping_44_df.dropna(subset=["Value", "Text"], axis='index', inplace=True)
# Write out to csv
code_mapping_44_df.to_csv("code_mapping_44.csv", sep="\t", index=False)


# Ticket 45 Column Mapping in correct format -
# https://github.com/ONSdigital/sdg-SDMX-data-qualifier/issues/45
WANTED_COLS_45 = ["sdg_column_name", "SDMX_Concept_ID"]
column_mapping_45_df = manual_excel(EXCEL_FILE, WANTED_COLS_45)
column_mapping_45_df.dropna(subset=["SDMX_Concept_ID"],
                            axis='index',
                            inplace=True)
column_mapping_45_df.rename(columns={"sdg_column_name": "Text",
                                     "SDMX_Concept_ID": "Value"},
                            inplace=True)
column_mapping_45_df.to_csv("column_mapping_45.csv", sep="\t", index=False)
