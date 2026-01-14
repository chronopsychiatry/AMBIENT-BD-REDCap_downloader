import requests
import pandas as pd
from io import StringIO
import logging

from .dom import Variables, Report


class REDCap:
    """
    Represents a connection to a REDCap project.

    Attributes:
        token (str): API token for the REDCap project.
        base_url (str): Base URL for the REDCap API.
        api_access (bool): Indicates if the API access is successful.
        project_title (str): Title of the REDCap project.
        data_type (str): Type of data in the project ('questionnaire' or 'ema').

    Methods:
        has_api_access(): Checks if the REDCap API is accessible with the provided token.
        get_variables(): Fetches the list of questionnaire variables from the REDCap API.
        get_report(): Fetches the questionnaire answers from the REDCap API.
        get_project_title(): Fetches the project title from the REDCap API.
        get_data_type(): Determines the data type of the REDCap project based on its title.
    """
    def __init__(self, token: str):
        self._logger = logging.getLogger('REDCap')
        self.token = token
        self.base_url = 'https://redcap.usher.ed.ac.uk/api/'
        self.api_access = self.has_api_access()
        self.project_title = self.get_project_title()
        self.data_type = self.get_data_type()

    def has_api_access(self) -> bool:
        """
        Check if the REDCap API is accessible with the provided token.

        Args:
            None
        Returns:
            bool: True if API access is successful, False otherwise.
        """
        data = {
            'token': self.token,
            'content': 'project',
            'format': 'json',
            'returnFormat': 'json'
        }
        r = requests.post(self.base_url, data=data)
        if r.status_code != 200:
            self._logger.error(f"Failed to access REDCap API: {r.text}")
            return False
        self._logger.info('Successfully accessed REDCap API.')
        return True

    def get_project_title(self) -> str:
        """
        Fetch the project title from the REDCap API.

        Args:
            None
        Returns:
            str: The title of the REDCap project.
        """
        data = {
            'token': self.token,
            'content': 'project',
            'format': 'json',
            'returnFormat': 'json'
        }
        r = requests.post(self.base_url, data=data)
        if r.status_code != 200:
            self._logger.error(f"Failed to fetch project title: {r.text}")
            raise Exception(f"HTTP Error: {r.status_code}")
        project_info = r.json()
        project_title = project_info.get('project_title', 'Unknown Project')
        self._logger.info(f'Processing REDCap project: {project_title}')
        return project_title

    def get_data_type(self) -> str:
        """
        Determine the data type of the REDCap project based on its title.

        Args:
            None

        Returns:
            str: The data type ('questionnaire' or 'ema').
        """
        if 'EMA' in self.project_title:
            return 'ema'
        elif 'AmbientBD - ambient and passive collection' in self.project_title:
            return 'questionnaire'
        else:
            return None

    def get_variables(self):
        """
        Fetch the list of variables from the REDCap API.

        Args:
            None

        Returns:
            Variables: Variables instance containing the raw data.
        """
        data = {
            'token': self.token,
            'content': 'metadata',
            'format': 'csv',
            'returnFormat': 'json',
        }
        r = requests.post(self.base_url, data=data)
        if r.status_code != 200:
            self._logger.error(f"Failed to fetch variable dictionary: {r.text}")
            raise Exception(f"HTTP Error: {r.status_code}")
        self._logger.info('Accessing variable dictionary through the REDCap API.')
        return Variables(pd.read_csv(StringIO(r.text)))

    def get_report(self):
        """
        Fetch the report (all data) from the REDCap API.

        Args:
            None

        Returns:
            Report: Report instance containing the raw data.
        """
        data = {
            'token': self.token,
            'content': 'record',
            'format': 'csv',
            'type': 'flat',
            'csvDelimiter': '',
            'rawOrLabel': 'raw',
            'rawOrLabelHeaders': 'raw',
            'exportCheckboxLabel': 'true',
            'returnFormat': 'json'
        }

        r = requests.post(self.base_url, data=data)
        if r.status_code != 200:
            self._logger.error(f"Failed to fetch report data: {r.text}")
            raise Exception(f"HTTP Error: {r.status_code}")
        self._logger.info('Fetched report data through the REDCap API.')
        return Report(pd.read_csv(StringIO(r.text)))
