import logging
from pathlib import Path
from importlib.metadata import version
from datetime import datetime

from .config.properties import load_application_properties
from .storage.path_resolver import PathResolver
from .redcap_api.redcap import REDCap
from .data_cleaning.data_cleaner import DataCleaner


def main():
    properties = load_application_properties()

    # Configure the logger
    log_file = Path(properties.download_folder) / f"download_{datetime.now().strftime('%Y%m%d')}.log"
    if not log_file.parent.exists():
        log_file.parent.mkdir(parents=True)
    if log_file.exists():
        log_file.unlink()
    logging.basicConfig(
        level=logging.DEBUG if properties.log_level == 'DEBUG' else logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Log format
        handlers=[
            logging.FileHandler(log_file),  # Log to a file
            logging.StreamHandler()  # Log to console
        ]
    )

    logger = logging.getLogger('main')
    pkg_version = version("redcap_downloader")
    logger.info(f'Running redcap_downloader version {pkg_version}')

    paths = PathResolver(properties.download_folder)

    redcap = REDCap(properties)

    cleaner = DataCleaner(redcap, paths)

    cleaner.save_questionnaire_variables()
    cleaner.save_questionnaire_reports()


if __name__ == '__main__':
    main()
