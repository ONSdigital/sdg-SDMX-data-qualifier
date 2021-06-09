# UK National Standardiser for Statistical Exchange Datasets (UNDERSTand)

## What UNDERSTand does

UNDERSTand is a script written in Python that selects suitable datasets from the UK SDG data platform and manipulates the data into the form and format that is required by the UN SDMX datalab platform. 

The datasets are selected according to criteria *how were they decided upon?* the logic for which is built into the script and the values controlled from the config file, which can be edited by users. 

The values in the SDG datasets are mapped to SDMX code IDs, and the column headers in the SDG datasets are mapped to SDMX, variously by a manual or semi-manual/assisted process. Ultimately we thought that a human must be involved in the process of choosing the values, hence a fully automated mapping process was never the desired outcome of this project. 

Instead the two steps that require human intervention are as follows:


| Process                                 | Means of transformation          |
|-----------------------------------------|----------------------------------|
| 1. SDG values --> SDMX Codes            | Computer-assisted manual process |
| 2. SDG column headers --> SDMX Concepts | Fully manual process             |

*this manual process may be changed to a computer-assisted process later. See "Features implement in the future" for more details

## Default values for the logic test

The config file holds values that control the logic tests which test the datasets for suitability to be included in the SDMX datalab
These are all detailed under "suitability_test". 

| Config Field                   | Default Config Value | Explanation                                                                                                                                                                                                                                                               |
|--------------------------------|----------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| data_non_statistical           | false                | The dataset needs to be a statisical dataset to be suitable for inclusion on the SDMX datalab platform. As the UK data includes some indicators which are reported as non-statistical indicators, these must be excluded.                                                 |
| national_geographical_coverage | "United Kingdom"     | The dataset should only relate to the whole of the United Kingdom, rather than a sub-set of it                                                                                                                                                                            |
| only_uk_data                   | true                 |                                                                                                                                                                                                                                                                           |
| geo_disag                      | false                |                                                                                                                                                                                                                                                                           |
| reporting_status               | "complete"           | Work on production of the data should be complete so the data is as up-to-date, complete and accurate as possible.                                                                                                                                                        |
| proxy_indicator                | false                | Some of the datasets report data for a related to the global target when the exact data is not available for the UK. The data is selected to be a good proxy for the international target, but since it is not measuring the same thing will not be directly comparable.  |


## Challenges faced

Under geographical coverage, we found three terms that were used in the UK SDG data that meant that observation covered the whole of the UK, they were as follows:

  - United Kingdom
  - UK
  - England, Scotland, Wales, Northern Ireland

As such, they are listed under the "uk_terms" section of the config file. 

The UNDERSTand script searches for all of these words under the geographical column *What is the specific name of this column?* , and creates a "only_uk_data" column with True and False values which is later used for testing. 

Some of the datasets have geographical disagregations in the data, which is not required or wanted for the SDMX datalab platform *Why is this?*. As such, terms that would indicate that the data are disagregated regionally are looked for in the *which column is this looked for in?* column. These termns include:
  - Region
  - Country
  - Local Authority

If any of these terms show up then a True will be placed in the geo_disag column. As specified in the suitability tests, only if geo_disag is False would the dataset be selected. 

## Features implement in the future

- Testing for each of the functions, which should include data size/shape checking.
- Streamline the logic of the suitability testing - e.g. the tests for the uk_only_data overlap with national_geographical_coverage.

## How to Install and run UN-STtaTED

The script was created and run using Python 3.9.2 and a conda environment. All the major dependencies are listed in the requirements.txt file.

## Instructions on how to use U-STtaTED

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
