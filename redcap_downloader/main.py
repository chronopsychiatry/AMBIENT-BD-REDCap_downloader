import logging
from pathlib import Path
from importlib.metadata import version
from datetime import datetime

from .config.properties import load_application_properties
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

    for token in properties.redcap_tokens:
        logger.debug(f'Trying to access REDCap with token {token}.')
        redcap = REDCap(token)

        project_title = redcap.get_project_title()
        logger.info(f'Processing REDCap project: {project_title}')

        report.append(redcap.get_report())
        variables.append(redcap.get_variables())

        previous_data_type = report.data_type
        report.set_data_type(project_title)
        variables.set_data_type(project_title)
        logger.debug(f'Report data type: {report.data_type}')

        if previous_data_type and previous_data_type != report.data_type:
            logger.warning('REDCap projects have different data types. Check your API tokens.')

        # subject_list = report.get_subjects(data_type)
        # logger.info(f'Downloaded reports for {len(subject_list)} subjects.')
        # logger.debug(f'Subject list: {subject_list}')

    grouper = 'redcap_event_name' if report.data_type == 'questionnaire' else 'redcap_repeat_instrument'
    logger.info(f'Total number of reports: \
                        {report.data.groupby(grouper).size().sort_values(ascending=False)}')
    logger.info(f'Total number of variables: {len(variables.data)}')

    cleaner = DataCleaner(paths, report, variables, properties)

    cleaner.save_cleaned_variables()
    cleaner.save_cleaned_reports()


if __name__ == '__main__':
    main()
