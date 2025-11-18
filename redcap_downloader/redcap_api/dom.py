import logging
import pandas as pd

from ..data_cleaning.helpers import drop_empty_columns
from ..storage.path_resolver import PathResolver


class DataMixin:
    """
    Mixin class providing data handling methods for REDCap data objects.

    Attributes:
        data (pd.DataFrame): The data.
        raw_data (pd.DataFrame): The raw data.

    Methods:
        split(by): Splits the DataFrame into a list of DataFrames based on the specified columns.
    """
    def __init__(self):
        self.data = pd.DataFrame()
        self._logger = logging.getLogger(self.__class__.__name__)

    def split(self, by: list[str] | str) -> list[pd.DataFrame]:
        """Split the DataFrame into a list of DataFrames based on the specified columns.

        Args:
            by (list): List of columns to split the DataFrame by.

        Returns:
            List[pd.DataFrame]: List of DataFrames, one for each unique group defined by 'by'.
        """
        return [group.copy() for _, group in self.data.groupby(by)]


class Report(DataMixin):
    """
    Represents a report containing questionnaire answers, exported from REDCap.

    Attributes:
        raw_data (pd.DataFrame): The raw report data (will not get affected by data cleaning operations).
        data (pd.DataFrame): The report data (will be affected by data cleaning operations).

    Methods:
        save_cleaned_data(paths): Saves cleaned report data to disk.
    """
    def __init__(self, report_data: pd.DataFrame):
        super().__init__()
        self.data = report_data
        self.raw_data = report_data
        self.data_type = self.get_data_type()
        self.subject_list = self.get_subjects()
        self.grouper = 'redcap_event_name' if self.data_type == 'questionnaire' else 'redcap_repeat_instrument'

        self._logger.info(f'Initialised report for {len(self.subject_list)} subjects.')
        self._logger.info(f'Number of questionnaires: \
                          {self.data.groupby(self.grouper).size().sort_values(ascending=False)}')
        self._logger.debug(f'Subject list: {self.subject_list}')

    def __str__(self):
        return f"Report with {self.data.shape[0]} entries and {self.data.shape[1]} columns"

    def save_cleaned_data(self, paths: PathResolver, by: list[str] | str = '', remove_empty_columns: bool = True):
        """
        Save cleaned questionnaire report data after splitting it by the specified columns.

        Args:
            paths (PathResolver): PathResolver instance to get the save paths.
            by (list): List of columns to split the DataFrame by.
            remove_empty_columns (bool): Whether to remove empty columns before saving.

        Returns:
            None
        """
        df_list = [self.data] if by == '' else self.split(by)
        if remove_empty_columns:
            df_list = [drop_empty_columns(df) for df in df_list]

        for df in df_list:
            self._logger.debug(f'Saving report with shape: {df.shape}')
            file_path = paths.get_subject_questionnaire(subject_id=df.participant_id.iloc[0],
                                                        data_type='EMA_' if self.data_type == 'ema' else '',
                                                        event_name=df.output_form.iloc[0])
            df.drop(columns=['output_form'], axis='columns').to_csv(file_path, index=False)
            self._logger.debug(f'Saved cleaned report data to {file_path}')

    def save_raw_data(self, paths: PathResolver):
        """
        Save raw data to a specified path.

        Args:
            raw_data (pd.DataFrame): DataFrame containing the raw data.
            paths (PathResolver): PathResolver instance to get the save paths.

        Returns:
            None
        """
        self.raw_data.to_csv(paths.get_raw_report_file(), index=False)
        self._logger.info(f'Saved raw data to {paths.get_raw_report_file()}')

    def get_data_type(self) -> str:
        """
        Determine the data type of the report based on the column names.

        Args:
            None

        Returns:
            str: The data type ('questionnaire' or 'ema').
        """
        if 'study_id' in self.data.columns and 'redcap_event_name' in self.data.columns:
            return 'questionnaire'
        elif 'field_record_id' in self.data.columns and 'redcap_repeat_instrument' in self.data.columns:
            return 'ema'
        else:
            raise ValueError('Could not infer report data type.')

    def get_subjects(self) -> list[str]:
        """
        Get the list of unique subject identifiers in the report.

        Args:
            None

        Returns:
            list[str]: List of unique subject identifiers.
        """
        if self.data_type == 'questionnaire':
            return self.data['study_id'].unique().tolist()
        elif self.data_type == 'ema':
            subject_ids = self.data['field_7uslb44zkd7bybb6'].dropna().unique().astype('int').tolist()
            return [f"ABD{sid:03d}" for sid in subject_ids]
        else:
            raise ValueError('Could not list subjects: unknown report data type.')


class Variables(DataMixin):
    """
    Represents a set of variables from the questionnaires of a REDCap project.

    Attributes:
        raw_data (pd.DataFrame): The raw variables data (will not get affected by data cleaning operations).
        data (pd.DataFrame): The variables data (will be affected by data cleaning operations).

    Methods:
        save_cleaned_data(paths): Saves cleaned variables data to disk.
    """
    def __init__(self, variables_data: pd.DataFrame):
        super().__init__()
        self.raw_data = variables_data
        self.data = variables_data
        self.data_type = self.get_data_type()

        self._logger.info(f'Initialised list of {len(self.data)} variables.')

    def __str__(self):
        return f"Variables with {self.raw_data.shape[0]} entries"

    def save_cleaned_data(self, paths: PathResolver, by: list[str] | str = '', remove_empty_columns: bool = True):
        """
        Save cleaned variables data.

        Args:
            paths (PathResolver): PathResolver instance to get the save paths.
            by (list or str): List of columns to split the DataFrame by.
            remove_empty_columns (bool): Whether to remove empty columns before saving.

        Returns:
            None
        """
        df_list = [self.data] if by == '' else self.split(by)
        if remove_empty_columns:
            df_list = [drop_empty_columns(df) for df in df_list]

        for df in df_list:
            self._logger.debug(f'Saving {len(df)} variables for form: {df.output_form.iloc[0]}')
            file_path = paths.get_variables_file(form_name=df.output_form.iloc[0])
            df.drop(columns=['output_form']).to_csv(file_path, index=False)
            self._logger.debug(f'Saved cleaned variables data to {file_path}')

    def save_raw_data(self, paths: PathResolver):
        """
        Save raw data to a specified path.

        Args:
            raw_data (pd.DataFrame): DataFrame containing the raw data.
            paths (PathResolver): PathResolver instance to get the save paths.

        Returns:
            None
        """
        self.raw_data.to_csv(paths.get_raw_variables_file(), index=False)
        self._logger.info(f'Saved raw data to {paths.get_raw_variables_file()}')

    def get_data_type(self) -> str:
        """
        Determine the data type of the variables based on the variable names.

        Args:
            None

        Returns:
            str: The data type ('questionnaire' or 'ema').
        """
        self._logger.debug(f'Variable field names: {self.data.field_name.tolist()}')
        if 'study_id' in self.data.field_name.values and 'participant_id' in self.data.field_name.values:
            return 'questionnaire'
        elif 'field_record_id' in self.data.field_name.values:
            return 'ema'
        else:
            raise ValueError('Could not infer variables data type.')
