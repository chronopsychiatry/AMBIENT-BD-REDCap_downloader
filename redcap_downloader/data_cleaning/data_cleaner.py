from functools import reduce
import logging
import numpy as np
import pandas as pd

from ..redcap_api.dom import Variables, Report
from ..config.properties import Properties
from ..storage.path_resolver import PathResolver
from .helpers import replace_strings, merge_duplicate_columns
from .replacements import FORM_NAMES, FIELD_NAMES


class DataCleaner:
    """
    Handles the cleaning and saving of data from REDCap.

    Attributes:
        redcap (REDCap): Instance of the REDCap API client.
        paths (PathResolver): Instance of PathResolver to manage file paths.

    Methods:
        save_variables(): Cleans and saves variables.
        save_reports(): Cleans and saves reports.
    """
    def __init__(self, paths: PathResolver, report: Report, variables: Variables, properties: Properties):
        self._logger = logging.getLogger('DataCleaner')
        self.paths = paths
        self.report = report
        self.variables = variables
        self.properties = properties

    def save_cleaned_variables(self):
        """
        Clean-up and save variables from REDCap.

        Args:
            None

        Returns:
            None

        """
        self.variables.save_raw_data(paths=self.paths)

        self.variables = self.clean_variables(self.variables)
        self.variables.save_cleaned_data(paths=self.paths, by=['output_form'], remove_empty_columns=True)
        self._logger.info(f'Saved cleaned variables to {self.paths.get_meta_dir()}.')

    def save_cleaned_reports(self):
        """
        Clean-up and save reports from REDCap.

        Args:
            None

        Returns:
            None
        """
        if not self.properties.include_identifiers:
            self.reports = self.remove_identifiers(self.reports, self.variables)
        self.reports.save_raw_data(paths=self.paths)

        self.reports = self.clean_reports(self.reports)
        self.reports.save_cleaned_data(self.paths, by=['participant_id', 'output_form'], remove_empty_columns=True)
        self._logger.info(f'Saved cleaned reports to {self.paths.get_reports_dir()}.')

    def remove_identifiers(self, reports: Report, variables: Variables) -> Report:
        """
        Remove identifier fields from the reports DataFrame.

        Args:
            reports (Report): Report instance.
            variables (Variables): Variables instance.
        Returns:
            Report: Report instance with identifier fields removed.
        """
        identifier_fields = (variables
                             .data
                             .query('identifier == "y"')
                             ['field_name']
                             .tolist()
                             )
        self._logger.info(f'Removing identifier fields: {identifier_fields}')
        reports.data = reports.data.drop(columns=identifier_fields, axis='columns', errors='ignore')
        reports.raw_data = reports.raw_data.drop(columns=identifier_fields, axis='columns', errors='ignore')
        return reports

    def clean_variables(self, variables: Variables) -> Variables:
        """
        Clean-up the variables DataFrame.

        Args:
            variables (Variables): Variables instance containing raw data.

        Returns:
            Variables: Variables instance with cleaned data added.
        """
        cleaned_var = (variables
                       .data
                       .query('form_name != "participant_information"')
                       .pipe(self.remove_html_tags)
                       .pipe(self.filter_variables_columns)
                       .pipe(self.clean_variables_form_names, data_type=variables.data_type)
                       )
        variables.data = cleaned_var
        return variables

    def clean_reports(self, reports: Report) -> Report:
        """
        Clean-up the reports DataFrame.

        Args:
            reports (Report): Report instance containing raw data.

        Returns:
            Report: Report instance with cleaned data added.
        """
        cleaned_reports = (reports
                           .data
                           .pipe(self.clean_reports_form_names, data_type=reports.data_type)
                           )
        if reports.data_type == 'questionnaire':
            reports.data = cleaned_reports.query('redcap_event_name != "initial_contact"')
        elif reports.data_type == 'ema':
            reports.data = (
                cleaned_reports
                .assign(
                    participant_id=lambda df: df['participant_id'].ffill().astype('int').apply(lambda x: f"ABD{x:03d}")
                )
                .query('participant_id != "ABD999"')
                )
        return reports

    def clean_variables_form_names(self, df: pd.DataFrame, data_type: str) -> pd.DataFrame:
        """
        Replace form names by human-readable names and merge researcher and participant forms.

        Args:
            df (pd.DataFrame): DataFrame containing variable data.
            data_type (str): The type of data ('questionnaire' or 'ema').

        Returns:
            pd.DataFrame: DataFrame with cleaned form names.
        """
        df = (df
              .assign(
                      form_name=lambda df: replace_strings(df.form_name, FORM_NAMES),
                      field_name=lambda df: replace_strings(df.field_name, FIELD_NAMES)
                      ))
        if data_type == 'questionnaire':
            df = df.assign(output_form=lambda df: np.where(df.form_name == 'Screening', 'Scre', 'Ques'))
        elif data_type == 'ema':
            df = df.assign(output_form=lambda df: df.form_name)
        return (df.pipe(merge_duplicate_columns))

    def clean_reports_form_names(self, df: pd.DataFrame, data_type: str) -> pd.DataFrame:
        """
        Clean-up the form and column names of the reports DataFrame.

        Args:
            df (pd.DataFrame): DataFrame containing report data.
            data_type (str): The type of data ('questionnaire' or 'ema').

        Returns:
            pd.DataFrame: DataFrame with cleaned form and column names.
        """
        if data_type == 'questionnaire':
            df = (df
                  .assign(redcap_event_name=lambda df: replace_strings(df.redcap_event_name, {'_arm_1': ''}),
                          output_form=lambda df: np.where(df.redcap_event_name == 'screening', 'Scre', 'Ques')
                          ))
        elif data_type == 'ema':
            df = (df
                  .assign(redcap_repeat_instrument=lambda df: replace_strings(df.redcap_repeat_instrument, FORM_NAMES),
                          output_form=lambda df: df.redcap_repeat_instrument
                          )
                  )
        return (df
                .rename(columns=lambda df: (
                    reduce(lambda s, kv: s.replace(kv[0], kv[1]), FIELD_NAMES.items(), df)
                ))
                .pipe(merge_duplicate_columns)
                )

    def filter_variables_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove unnecessary columns from the DataFrame.

        Args:
            df (pd.DataFrame): DataFrame to be processed.

        Returns:
            pd.DataFrame: DataFrame with unnecessary columns removed.
        """
        keep_cols = ['field_name', 'form_name', 'section_header', 'field_type', 'field_label']
        return df[keep_cols].copy()

    def remove_html_tags(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove HTML tags from all string cells in a DataFrame.

        Args:
            df (pd.DataFrame): DataFrame to be processed.

        Returns:
            pd.DataFrame: DataFrame with HTML tags removed from string cells.
        """
        return df.copy().assign(
            **df.select_dtypes(include=['object'])
            .replace(to_replace=r'<[^>]+>', value='', regex=True)
        )
