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
        append(other): Appends data from another DataMixin instance.
    """
    def __init__(self):
        self._logger = logging.getLogger(self.__class__.__name__)

    def split(self, by: list[str] | str) -> list[pd.DataFrame]:
        """Split the DataFrame into a list of DataFrames based on the specified columns.

        Args:
            by (list): List of columns to split the DataFrame by.

        Returns:
            List[pd.DataFrame]: List of DataFrames, one for each unique group defined by 'by'.
        """
        return [group.copy() for _, group in self.data.groupby(by)]

    def append(self, other: 'DataMixin'):
        """
        Append data from another DataMixin instance to this instance.

        Args:
            data (DataMixin): Another DataMixin instance to append data from.
        Returns:
            None
        """
        self.data = pd.concat([self.data, other.data], ignore_index=True)
        self.raw_data = pd.concat([self.raw_data, other.raw_data], ignore_index=True)


class Report(DataMixin):
    """
    Represents a report containing data exported from REDCap.

    Attributes:
        raw_data (pd.DataFrame): The raw report data (will not get affected by data cleaning operations).
        data (pd.DataFrame): The report data (will be affected by data cleaning operations).

    Methods:
        save_cleaned_data(paths): Saves cleaned report data to disk.
    """
    def __init__(self, report_data: pd.DataFrame = pd.DataFrame()):
        super().__init__()
        self.data = report_data
        self.raw_data = report_data

    def __str__(self):
        return f"Report with {self.data.shape[0]} entries and {self.data.shape[1]} columns"

    def save_cleaned_data(self, paths: PathResolver, by: list[str] | str = '', remove_empty_columns: bool = True):
        """
        Save cleaned report data to a csv file after splitting it by the specified columns.

        Args:
            paths (PathResolver): PathResolver instance to get the save paths.
            by (list): List of columns to split the DataFrame by.
            remove_empty_columns (bool): Whether to remove empty columns before saving.

        Returns:
            None
        """
        df_list = [self.data] if by == '' else self.split(['output_form'])
        if remove_empty_columns:
            df_list = [drop_empty_columns(df) for df in df_list]
        for df in df_list:
            all_subjects_path = paths.get_all_subjects_file(event_name=df.output_form.iloc[0])
            df.drop(columns=['output_form'], axis='columns').to_csv(all_subjects_path, index=False)

        df_list = [self.data] if by == '' else self.split(by)
        if remove_empty_columns:
            df_list = [drop_empty_columns(df) for df in df_list]

        for df in df_list:
            self._logger.debug(f'Saving report with shape: {df.shape}')
            file_path = paths.get_subject_questionnaire(subject_id=df.participant_id.iloc[0],
                                                        event_name=df.output_form.iloc[0])
            df.drop(columns=['output_form'], axis='columns').to_csv(file_path, index=False)
            self._logger.debug(f'Saved cleaned report data to {file_path}')

    def save_raw_data(self, paths: PathResolver):
        """
        Save raw data to a csv file.

        Args:
            paths (PathResolver): PathResolver instance to get the save paths.

        Returns:
            None
        """
        self.raw_data.to_csv(paths.get_raw_report_file(), index=False)
        self._logger.info(f'Saved raw data to {paths.get_raw_report_file()}')

    def get_subjects(self, data_type: str) -> list[str]:
        """
        Get the list of unique subject identifiers in the report.

        Args:
            data_type (str): The type of data ('questionnaire' or 'ema').
        Returns:
            list[str]: List of unique subject identifiers.
        Raises:
            ValueError: If the data type is unknown.
        """
        if data_type == 'questionnaire':
            return self.data['study_id'].unique().tolist()
        elif data_type == 'ema':
            subject_ids = self.data['participant_id'].dropna().unique().astype('int').tolist()
            return [f"ABD{sid:03d}" for sid in subject_ids]
        else:
            raise ValueError('Could not list subjects: unknown report data type.')


class Variables(DataMixin):
    """
    Represents a set of variables from a REDCap project.

    Attributes:
        raw_data (pd.DataFrame): The raw variables data (will not get affected by data cleaning operations).
        data (pd.DataFrame): The variables data (will be affected by data cleaning operations).

    Methods:
        save_cleaned_data(paths): Saves cleaned variables data.
    """
    def __init__(self, variables_data: pd.DataFrame = pd.DataFrame()):
        super().__init__()
        self.raw_data = variables_data
        self.data = variables_data

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
        Save raw data to a csv file.

        Args:
            paths (PathResolver): PathResolver instance to get the save paths.

        Returns:
            None
        """
        self.raw_data.to_csv(paths.get_raw_variables_file(), index=False)
        self._logger.info(f'Saved raw data to {paths.get_raw_variables_file()}')
