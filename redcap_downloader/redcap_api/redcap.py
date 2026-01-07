import requests
import pandas as pd
from io import StringIO
import logging

from .dom import Variables, Report
from ..config.properties import Properties


class REDCap:
    """
    Represents a connection to a REDCap project.

    Attributes:
        token (str): API token for the REDCap project.
        base_url (str): Base URL for the REDCap API.
        properties (Properties): Values from the properties file for the download.
        api_access (bool): Indicates if the API access is successful.

    Methods:
        get_questionnaire_variables(): Fetches the list of questionnaire variables from the REDCap API.
        get_questionnaire_report(): Fetches the questionnaire answers from the REDCap API.
    """
    def __init__(self, properties: Properties):
        self._logger = logging.getLogger('REDCap')
        self.token = properties.redcap_token
        self.base_url = 'https://redcap.usher.ed.ac.uk/api/'
        self.properties = properties
        self.api_access = self.has_api_access()

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

    def get_questionnaire_variables(self):
        """
        Fetch the list of questionnaire variables from the REDCap API.

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

    def get_questionnaire_report(self):
        """
        Fetch the questionnaire answers from the REDCap API.

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
