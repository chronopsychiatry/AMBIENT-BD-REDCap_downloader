# REDCap_downloader

Python script to download, clean-up and organise data from REDCap

## Running the downloader

The package can be installed from PyPI with the command: `pip install redcap_downloader`

Accessing REDCap data requires having an API token. This must be requested through the REDCap platform, and stored in a .txt file.

Create the "REDCap_downloader.properties" file with the command `redcap_generate_config`. The config file will contain the following fields:

- `token-file`: the path to the text file containing your REDCap API token
- `download-dir`: path to the directory where the REDCap data will be downloaded
- `report-id`: ID of the report to download. For Ambient-BD questionnaire data, use 159, for EMA data use 190
- `log-level`: set to INFO by default. Change to DEBUG if you have an issue with the downloader and want more info on what is happening

Finally, run the following command from the directory that contains the properties file:

```bash
redcap_download
```

## Folder structure

The program will create the following folder structure:

```markdown
├── download_20250716.log
├── meta
│   ├── Ques_variables_20250716.csv
│   └── Scre_variables_20250716.csv
├── raw
│   ├── Report_raw_20250716.csv
│   └── Variables_raw_20250716.csv
└── reports
    ├── ABD001
    │   ├── ABD001_PROM-Ques_20250716.csv
    │   └── ABD001_PROM-Scre_20250716.csv
    ├── ABD002
    │   ├── ABD002_PROM-Ques_20250716.csv
    │   └── ABD002_PROM-Scre_20250716.csv
    ├── ABD003
    ...
```

All file names contain the date at which the downloader was run (20250716 in this case).

- `download.log`: contains a log of the program run
- `meta`: questionnaire metadata. Contains one .csv file per questionnaire. Each .csv file contains a list of all variables in the questionnaire (as found in the reports), along with a description
- `raw`: raw data as obtained from REDCap, without any cleaning done. There are two files:
  - `Report_raw.csv`: questionnaire results for all participants, and all questionnaires
  - `Variables_raw.csv`: list of variables for all questionnaires
- `reports`: cleaned-up questionnaire data, split by participant and questionnaire type
  - `PROM-Scre`: contains only the screening questionnaire
  - `PROM-Ques`: contains the baseline questionnaire, as well as the 6-, 12- and 18-months follow-up questionnaires
  - `PROM-EMA`: contains EMA data

## Ambient-BD questionnaires

The Ambient-BD study uses 6 different questionnaires:

- Initial contact
- Screening
- Baseline
- 6-month followup
- 12-month followup
- 18-month followup

The "Initial contact" questionnaire is saved as part of the raw data, but contains very little information (since most fields are direct identifiers that are removed during the REDCap export process). It is therefore not saved as part of the cleaned data (`meta` and `reports` folders).
