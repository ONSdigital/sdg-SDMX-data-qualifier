# Statistical Exchange Datasets Reshaper

# Process: What this script does

This script selects suitable datasets from an Open data platform and manipulates the data into the form and format that is required by the [UN SDGs datalab](https://unstats.un.org/sdglab/).

The datasets are selected according to user-defined criteria, which are set in the config file. For example which geographical disaggregations the indicators to be selected cover can be specified for with the "uk_terms" parameter in the config file.

## Process Diagram

![Overview of the process](https://github.com/ONSdigital/sdg-SDMX-data-qualifier/blob/c8ec7caa75251859e93ff05a68bd734ab2dbf341/images/Overview%20of%20the%20sdg-sdmx%20mapping%20process.jpg)

See the [technical process diagram](https://github.com/ONSdigital/sdg-SDMX-data-qualifier/blob/c8ec7caa75251859e93ff05a68bd734ab2dbf341/images/SDMX_qual_flow_diagram.jpg).

## Code mapping

The _disaggregation values_, in the SDG datasets are mapped to _SDMX code IDs_. For example Female within the Sex disaggregation would be mapped to the SDMX code “F”. This mapping is carried out via a semi-manual/computer-assisted process. The script looks for the best matches for the each of those values, and presents them to the user. The user has the final decision on which of the values is mapped to which SDMX value (its name in English). Then, based on the user choice of the SDMX value, the script then couples selects the SDMX code associated with that SDMX value and inserts it into the data table.

## Column mapping

Similiarly the _disaggregation names_, for example, Sex would be mapped to the SDMX concept SEX _SDMX concepts_. The script as it is currently leaves this step to be done entirely manually. Without a manually created csv in place (the name of which is specified in config file). Please see the "Possible next steps" section for further discussion on how this could be improved.

## Why wasn't the process automated

We chose to make neither the column mapping (of disaggregation name) nor code mapping (of disaggregation values) a fully automatic process. Ultimately we decided that a human must be involved in the process of intelligently choosing those the correct mappings, as some knowledge of the what the data actually mean is required.

Instead the two steps that require human intervention are as follows:


| Process                                         | Means of transformation          |
|-------------------------------------------------|----------------------------------|
| 1. SDG disaggregation values --> SDMX Codes IDs | Computer-assisted manual process |
| 2. SDG disaggregation name --> SDMX Concepts    | Fully manual process             |

*this manual process may be changed to a computer-assisted process later. See "Features implement in the future" for more details

## Default Criteria for Selection

The config file holds values that control the criteria of the filters which remove unsuitable datasets from the selection for the SDMX datalab

These are configured using `suitability_test` in the config file.

| Config Field                   | Default Config Value | Explanation                                                                                                                                                                                                                                                               |
|--------------------------------|----------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| data_non_statistical           | false                | The dataset needs to be a statisical dataset to be suitable for inclusion on the UN SDGs datalab. As SDG data includes some non-statistical indicators and these must be excluded.                                                 |
| national_geographical_coverage | "United Kingdom"     | The dataset should only relate to the whole of the United Kingdom, rather than a sub-set of it                                                                                                                                                                            |
| only_uk_data                   | true                 | Checks if the values in the national geographic coverage column of the meta data contain either "UK" or "United Kingdom" or any value that the user specifies under uk_terms in the config file.                                                                          |
| geo_disag                      | false                | Checks the disaggregation report to see if each indicator is disaggregated by any of the disaggregation names that would indicate that there is sub-national (e.g. regional) disaggregation.                                                                              |
| reporting_status               | "complete"           | Work on production of the data should be complete so the data is as up-to-date, complete and accurate as possible.                                                                                                                                                        |
| proxy_indicator                | false                | Some of the datasets report data for a related to the global target when the exact data is not available for the UK. The data is selected to be a good proxy for the international target, but since it is not measuring the same thing will not be directly comparable.  |Explanation                                                                                                                                                                                                                                                               |
|--------------------------------|----------------------|


## Challenges faced

Under geographical coverage, we found two terms that were used in the UK SDG data that meant that observation covered the whole of the UK, they were as follows:

  - United Kingdom
  - UK

As such, they are listed under the "uk_terms" section of the config file.

The script searches for all of these words under the geographical column, and creates a "only_uk_data" column with True and False values which is later used for testing.

Some of the datasets have geographical disagregations in the data, which is not required or wanted for the UN SDGs datalab only wants country level data in all cases e.g. no geographical breakdowns. As such, terms that would indicate that the data are disagregated regionally are looked for in the national column. These termns include:
  - Region
  - Country
  - Local Authority

If any of these terms show up then a True will be placed in the geo_disag column. As specified in the suitability tests, only if geo_disag is False would the dataset be selected.

## Next possible steps

- Testing for each of the functions, which should include dataframe size/shape checking.
- Streamline the logic of the suitability testing - e.g. the tests for the uk_only_data overlap with national_geographical_coverage.
- Make the SDG disagregation name --> SDMX concept matching computer-assisted just as the SDG disaggregation values --> SDMX code ID matching is
- Make the `check_only_uk_data` function more generic so it can check for multiple terms and apply logic to other columns - e.g. the search for `geo_disag_terms`, which is currently done with a `df.col_name.str.contains(geo_disag_terms)`. Making the `check_only_uk_data` function into a more generic function would also make the code more resuable for other OpenSDG users.
- Improve the `check_if_proxies_contain_official` as this is a useful Quality Assurance function to check if there were any contradictions between what is described as a proxie and what contains the . In the UK case there were a couple of contradictory indicators that were both listed as proxies but also contained the sentence in their descripton (8-1-1 and 6-2-1) and these were removed manually - perhaps this removal should be automatic.
- Make a more generic version of `get_SDMX_colnm` as this is essentially a "VLookup" function (like in Excel) for dataframes. A VLookup function could be used in the disagregation name  --> SDMX concept matching, if that was ever to be made computer-assisted.

## How to Install and run the script

The script was created and run using Python 3.9.2 and a conda environment. All the major dependencies are listed in the requirements.txt file.

## Instructions on how to use script

1) Clone the repo
`git clone https://github.com/ONSdigital/sdg-SDMX-data-qualifier.git `
2) Create an environment with conda, e.g.
`conda create --name sdmx_qual python=3.9`
3) Activate the environment you have just created
`conda activate sdmx_qual`
3) From the project directory, install the dependences from the requirements.txt using either pip or conda
`conda install --yes --file requirements.txt`
5) Run the script from either your editor (e.g. VS Code, Spyder) or from the command line
`python main.py`


# Glossary

column mapping:
code mapping:
