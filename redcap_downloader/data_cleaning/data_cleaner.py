import logging
import numpy as np
import pandas as pd
from tabulate import tabulate

from ..redcap_api.dom import Variables, Report
from ..storage.path_resolver import PathResolver
from .helpers import (replace_strings, replace_column_name, merge_duplicate_columns, fill_participant_ids,
                      fix_24h_sleeptimes)
from .replacements import FORM_NAMES, FIELD_NAMES


class DataCleaner:
    """
    Handles the cleaning and saving of data from REDCap.

    Attributes:
        paths (PathResolver): Instance of PathResolver to manage file paths.
        report (Report): Instance of Report containing report data.
        variables (Variables): Instance of Variables containing variable data.
        data_type (str): Type of data ('questionnaire' or 'ema').
        include_identifiers (bool): Whether to include identifier fields in the reports.

    Methods:
        save_cleaned_variables(): Cleans and saves variables.
        save_cleaned_reports(): Cleans and saves reports.
    """
    def __init__(self,
                 paths: PathResolver,
                 report: Report,
                 variables: Variables,
                 data_type: str,
                 include_identifiers: bool = False):
        self._logger = logging.getLogger('DataCleaner')
        self.paths = paths
        self.report = report
        self.variables = variables
        self.data_type = data_type
        self.paths.data_type = data_type
        self.include_identifiers = include_identifiers

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

        self._logger.info(f'Total number of variables: {len(self.variables.data)}')

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
        if not self.include_identifiers:
            self.report = self.remove_identifiers(self.report, self.variables)
        self.report.save_raw_data(paths=self.paths)

        self.report = self.clean_reports(self.report)

        self._logger.info(f'Total number of report entries:\n{self.get_report_entries_table()}')

        self.report.save_cleaned_data(self.paths, by=['participant_id', 'output_form'], remove_empty_columns=True)
        self._logger.info(f'Saved cleaned reports to {self.paths.get_reports_dir()}.')

    def remove_identifiers(self, report: Report, variables: Variables) -> Report:
        """
        Remove identifier fields from the reports DataFrame.

        Args:
            report (Report): Report instance.
            variables (Variables): Variables instance.
        Returns:
            Report: Report instance with identifier fields removed.
        """
        identifier_fields = (variables
                             .raw_data
                             .query('identifier == "y"')
                             ['field_name']
                             .tolist()
                             )
        self._logger.info(f'Removing identifier fields: {identifier_fields}')
        report.data = report.data.drop(columns=identifier_fields, axis='columns', errors='ignore')
        report.raw_data = report.raw_data.drop(columns=identifier_fields, axis='columns', errors='ignore')
        return report

    def clean_variables(self, variables: Variables) -> Variables:
        """
        Clean-up the variables DataFrame.

        Args:
            variables (Variables): Variables instance containing raw data.

        Returns:
            variables: Variables instance with cleaned data added.
        """
        cleaned_var = (variables
                       .data
                       .query('form_name != "participant_information"')
                       .pipe(self.remove_html_tags)
                       .pipe(self.filter_variables_columns)
                       .pipe(self.clean_variables_form_names)
                       .drop_duplicates(ignore_index=True)
                       )
        variables.data = cleaned_var
        return variables

    def clean_reports(self, report: Report) -> Report:
        """
        Clean-up the reports DataFrame.

        Args:
            report (Report): Report instance containing raw data.

        Returns:
            report: Report instance with cleaned data.
        """
        cleaned_report = (report
                          .data
                          .pipe(self.clean_reports_form_names)
                          .drop_duplicates(ignore_index=True)
                          )
        if self.data_type == 'questionnaire':
            report.data = (
                cleaned_report
                .loc[cleaned_report['participant_id'].str.contains('ABD')]
                .query('redcap_event_name != "initial_contact" and\
                            redcap_event_name != "scheduling_emails"'
                       )
            )
        elif self.data_type == 'ema':
            cleaned_report = fill_participant_ids(cleaned_report)
            report.data = (
                cleaned_report
                [~cleaned_report['participant_id'].str.match(r'^ABD9')]  # ABD9xx are test records
                .pipe(self.move_mood_anxiety_ema_p1)
                .pipe(fix_24h_sleeptimes, self._logger)
                .query('redcap_repeat_instrument != "end_period"')
                )
        return report

    def clean_variables_form_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Replace form names by human-readable names and merge researcher and participant forms.

        Args:
            df (pd.DataFrame): DataFrame containing variable data.

        Returns:
            pd.DataFrame: DataFrame with cleaned form names.
        """
        df = (df
              .assign(
                      form_name=lambda df: replace_strings(df.form_name, FORM_NAMES),
                      field_name=lambda df: replace_strings(df.field_name, FIELD_NAMES)
                      ))
        if self.data_type == 'questionnaire':
            df = df.assign(output_form=lambda df: np.where(df.form_name == 'Screening', 'Scre', 'Ques'))
        elif self.data_type == 'ema':
            df = df.assign(output_form=lambda df: df.form_name)
        return (df.pipe(merge_duplicate_columns))

    def clean_reports_form_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean-up the form and column names of the reports DataFrame.

        Args:
            df (pd.DataFrame): DataFrame containing report data.

        Returns:
            pd.DataFrame: DataFrame with cleaned form and column names.
        """
        if self.data_type == 'questionnaire':
            df = (df
                  .assign(redcap_event_name=lambda df: replace_strings(df.redcap_event_name, {'_arm_1': ''}),
                          output_form=lambda df: np.where(df.redcap_event_name == 'screening', 'Scre', 'Ques')
                          ))
        elif self.data_type == 'ema':
            df = (df
                  .assign(redcap_repeat_instrument=lambda df: replace_strings(df.redcap_repeat_instrument, FORM_NAMES),
                          output_form=lambda df: df.redcap_repeat_instrument
                          )
                  )
        return (df
                .rename(columns=replace_column_name)
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

    def get_report_entries_table(self) -> str:
        """
        Generate a formatted table of report entries per form.

        Args:
            None
        Returns:
            str: Formatted table as a string.
        """
        return tabulate(self.report.data
                        .rename(columns={'output_form': 'form'})
                        .groupby('form')
                        .size()
                        .sort_values(ascending=False)
                        .to_frame()
                        .rename(columns={0: 'entries'}),
                        headers='keys',
                        tablefmt='psql'
                        )

    def move_mood_anxiety_ema_p1(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Move mood and anxiety prompts from the EMA period 1 "sleep_diary" form to the "mood_anxiety" form.

        Args:
            df (pd.DataFrame): DataFrame containing report data.

        Returns:
            pd.DataFrame: DataFrame with all mood and anxiety prompts moved to the "mood_anxiety" form.
        """
        mask = (df['output_form'] == 'sleep_diary') & (df['EMA_period_number'] == 1)
        diary_df = df.loc[mask, :]
        non_diary_df = df.loc[~mask, :]

        diary_stay = diary_df.copy()
        diary_move = diary_df.copy()
        cols_to_keep = ['record_id', 'EMA_period_number', 'participant_id', 'redcap_repeat_instrument',
                        'redcap_repeat_instance', 'response_time_diary_ms', 'response_timestamp_diary',
                        'diary_anxiety', 'diary_mood', 'output_form']
        diary_move.loc[:, ~diary_move.columns.isin(cols_to_keep)] = np.nan
        diary_move = (
            diary_move
            .assign(output_form='mood_anxiety',
                    response_time_ms=lambda df: df['response_time_diary_ms'],
                    response_timestamp=lambda df: df['response_timestamp_diary'],
                    current_anxiety=lambda df: df['diary_anxiety'],
                    current_mood=lambda df: df['diary_mood']
                    )
            .drop(['response_time_diary_ms', 'response_timestamp_diary'], axis='columns')
            )

        return (
            pd.concat([non_diary_df, diary_stay, diary_move], ignore_index=True)
            .drop(['diary_anxiety', 'diary_mood'], axis='columns')
            .sort_values(by=['EMA_period_number', 'participant_id', 'output_form', 'response_time_ms'])
            )
