# REDCap_downloader

Python script to download, clean-up and organise data from Ambient-BD REDCap projects (EMA or participant questionnaires).

## Running the downloader

The package can be installed from PyPI with the command: `pip install redcap_downloader`

Accessing REDCap data requires having an API token. This must be requested through the REDCap platform, and stored in a .txt file (multiple tokens per file possible, see below).

Create the "REDCap_downloader.properties" file with the command `redcap_generate_config`. The config file will contain the following fields:

- `token-file`: the path to the text file containing your REDCap API token. The token will define which project data will be downloaded from.
- `download-dir`: path to the directory where the REDCap data will be downloaded
- `include-identifiers`: true or false; whether to include identifier fields in the exported data. Note: if the user does not have permission to see identifier fields in REDCap, those will be excluded anyway.
- `log-level`: set to INFO by default. Change to DEBUG if you have an issue with the downloader and want more info on what is happening

Finally, run the following command from the directory that contains the properties file:

```bash
redcap_download
```

### Multiple tokens

Ambient-BD EMA data is stored in multiple REDCap projects (one per EMA period). To download all EMA data, include all EMA project tokens in your token-file (one token per line). The data will be concatenated, and an "EMA_period_number" column inserted in the report data.

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

The "Initial contact" questionnaire is saved as part of the raw data, but contains very little information if direct identifiers are not included. It is therefore not saved as part of the cleaned data (`meta` and `reports` folders).

## EMA data

The EMA data contains 4 modules:

- participant_id
- mood_anxiety
- sleep_diary
- end_period

These will all be saved as separate reports, for each participant. Results from different EMA periods will be concatenated in the files.

### Data cleaning

- Participant IDs are recorded in the EMA app as 1, 2, 3... and will be formatted as ABD001, ABD002, etc.
- Participant "ABD999" is removed from the data export (test data)
- Column names for period 2 and folllowing ones end with the suffix "_p2". This is removed during cleaning
- Module and column names for EMA period 1 are recorded differently to the subsequent periods in REDCap. This is edited to be consistent in the data export
- In the sleep diary, participants sometimes record their sleep time ("try_sleep_time" column) in 12h format instead of 24h. To correct this, the data cleaning shifts any times in that column that are between 6am and 12pm by 12h (so e.g. 10:15:00 becomes 22:15:00)
- All duplicate entries in reports and variables are removed
