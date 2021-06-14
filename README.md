# Statistical Exchange Datasets Reshaper

## What this script does

This script selects suitable datasets from an Open data platform and manipulates the data into the form and format that is required by the UN SDMX datalab platform.

The datasets are selected according to user-defined criteria, which are set in the config file. For example which geographical disaggregations the indicators to be selected cover can be specified for with the "uk_terms" parameter in the config file. The user can therefore control which datasets are selected, and apply criteria to select data that meet UN SDG Lab.

The values used in each disaggregation, or _disaggregation values_, in the SDG datasets are mapped to _SDMX code IDs_. For example the disaggregation of "Age" in the SDG datasets is broken down into different _disaggregation values_ which are ages groups, e.g. "0 to 4" and "45 and over". This mapping is carried out via a semi-manual/computer-assisted process. The script looks for the best matches for the each of those values, and presents them to the user. The user has the final decision on which of the values is mapped to which SDMX value (its name in English). Then, based on the user choice of the SDMX value, the script then couples selects the SDMX code associated with that SDMX value and inserts it into the data table.

Similiarly the column headers, or _disaggregation names_, e.g. "Sex" or "Age" need to be likewise mapped to _SDMX concepts_. The script as it is currently leaves this step to be done entirely manually. Without a manually created csv in place (the name of which is specified in config file). Please see the "Possible next steps" section for further discussion on how this could be improved.

We chose to make neither the disaggregation name mapping nor disaggregation value mapping a fully automatic process. Ultimately we decided that a human must be involved in the process of intelligently choosing those the correct mappings, as some knowledge of the what the data actually mean is required.

Instead the two steps that require human intervention are as follows:


| Process                                         | Means of transformation          |
|-------------------------------------------------|----------------------------------|
| 1. SDG disaggregation values --> SDMX Codes IDs | Computer-assisted manual process |
| 2. SDG disaggregation name --> SDMX Concepts    | Fully manual process             |

*this manual process may be changed to a computer-assisted process later. See "Features implement in the future" for more details

## Default values for the logic test

The config file holds values that control the logic tests which test the datasets for suitability to be included in the SDMX datalab
These are all detailed under "suitability_test".

| Config Field                   | Default Config Value | Explanation                                                                                                                                                                                                                                                               |
|--------------------------------|----------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| data_non_statistical           | false                | The dataset needs to be a statisical dataset to be suitable for inclusion on the SDMX datalab platform. As the UK data includes some indicators which are reported as non-statistical indicators, these must be excluded.                                                 |
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

Some of the datasets have geographical disagregations in the data, which is not required or wanted for the SDMX datalab platform as SDG Lab only wants country level data in all cases e.g. no geographical breakdowns. As such, terms that would indicate that the data are disagregated regionally are looked for in the national column. These termns include:
  - Region
  - Country
  - Local Authority

If any of these terms show up then a True will be placed in the geo_disag column. As specified in the suitability tests, only if geo_disag is False would the dataset be selected.

## Next possible steps

- Testing for each of the functions, which should include data size/shape checking.
- Streamline the logic of the suitability testing - e.g. the tests for the uk_only_data overlap with national_geographical_coverage.
- Make the SDG disagregation name --> SDMX concept matching computer-assisted just as the SDG disaggregation values --> SDMX code ID matching is
- Make the `check_only_uk_data` function more generic so it can check for multiple terms and apply logic to other columns - e.g. the search for `geo_disag_terms`, which is currently done with a `df.col_name.str.contains(geo_disag_terms)`. Making the `check_only_uk_data` function into a more generic function would also make the code more resuable for other OpenSDG users.
- Improve the `check_if_proxies_contain_official` as this is a useful Quality Assurance function to check if there were any contradictions between what is described as a proxie and what contains the . In the UK case there were a couple of contradictory indicators that were both listed as proxies but also contained the sentence in their descripton (8-1-1 and 6-2-1) and these were removed manually - perhaps this removal should be automatic.

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
