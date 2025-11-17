import configparser
from pathlib import Path


class Properties():
    """
    Represents the properties of the REDCap downloader application, read from a configuration file.

    Attributes:
        redcap_token_file (str): Path to the file containing the REDCap API token.
        download_folder (str): Directory where downloaded data will be stored.
        report_id (int): ID of the report to fetch from REDCap.
        log_level (str): Logging level for the application.
    """
    def __init__(self,
                 redcap_token_file: str | Path = None,
                 download_folder: str | Path = '../downloaded_data',
                 report_id: int | None = None,
                 include_identifiers: bool = False,
                 log_level: str = 'INFO'
                 ):

        self.redcap_token_file = Path(redcap_token_file or './redcap_token.txt')
        self.download_folder = Path(download_folder or '../downloaded_data')
        self.report_id = report_id
        self.include_identifiers = include_identifiers
        self.log_level = log_level
        self.redcap_token = read_token(self.redcap_token_file)

    def __str__(self):
        return f"Properties(redcap_token_file={self.redcap_token_file}, " \
               f"download_folder={self.download_folder}, report_id={self.report_id}, " \
               f"log_level={self.log_level})"


def load_application_properties(file_path: str | Path = './REDCap_downloader.properties'):
    """
    Load application properties from a configuration file.

    Args:
        file_path (str): Path to the properties file.

    Returns:
        Properties: An instance of the Properties class containing the loaded properties.

    Raises:
        ValueError: If the properties file does not exist or is not readable.
    """
    file_path = Path(file_path)
    config = configparser.ConfigParser()
    if file_path.exists():
        config.read(file_path)
    else:
        raise ValueError(f"Properties file not found: {file_path}.")
    return Properties(
        redcap_token_file=config['DEFAULT'].get('token-file', None),
        download_folder=config['DEFAULT'].get('download-dir', None),
        report_id=config['DEFAULT'].get('report-id', None),
        include_identifiers=config['DEFAULT'].getboolean('include-identifiers', False),
        log_level=config['DEFAULT'].get('log-level', 'INFO')
    )


def read_token(file_path: str | Path) -> str:
    """
    Read the REDCap API token from a specified file.

    Args:
        file_path (str): Path to the token file.
    Returns:
        str: The REDCap API token.
    """
    file_path = Path(file_path)
    if not file_path.exists():
        raise ValueError(f"Token file not found: {file_path}")
    with file_path.open('r') as f:
        token = f.readline().strip(' \t\n\r')
    return token
