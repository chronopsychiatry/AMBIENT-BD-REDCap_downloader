import logging
from pathlib import Path
from importlib.metadata import version
from datetime import datetime

from .config.properties import load_application_properties
from .data_cleaning.helpers import get_ema_period_number
from .storage.path_resolver import PathResolver
from .redcap_api.redcap import REDCap
from .redcap_api.dom import Report, Variables
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

    report = Report()
    variables = Variables()
    data_type = None

    for token in properties.redcap_tokens:
        logger.debug(f'Trying to access REDCap with token {token}.')
        redcap = REDCap(token)

        current_report = redcap.get_report()
        current_variables = redcap.get_variables()

        if redcap.data_type == 'ema':
            ema_period = get_ema_period_number(redcap.project_title)
            logger.debug(f'Extracted EMA period number: {ema_period} from project title.')
            current_report.data.insert(1, 'EMA_period_number', ema_period)

        report.append(current_report)
        variables.append(current_variables)

        if data_type and data_type != redcap.data_type:
            logger.error('REDCap projects have different data types. Check your API tokens.')
        logger.debug(f'Report data type: {redcap.data_type}')
        data_type = redcap.data_type

    cleaner = DataCleaner(paths, report, variables, data_type)

    cleaner.save_cleaned_variables()
    cleaner.save_cleaned_reports()


if __name__ == '__main__':
    main()
