from pathlib import Path
from datetime import datetime
import logging
import sys


class PathResolver:
    """
    Resolves file paths for storing downloaded data.

    Attributes:
        _main_dir (str): Main directory for storing downloaded data.

    Methods:
        set_main_dir(path): Sets the main directory for storing data.
        get_main_dir(): Returns the main directory path.
        get_raw_dir(): Returns the path for raw data storage.
        get_meta_dir(): Returns the path for metadata storage.
        get_reports_dir(): Returns the path for reports storage.
        get_subject_dir(subject_id): Returns the path for a specific subject's data.
        get_raw_variables_file(): Returns the path for raw variables data.
        get_raw_report_file(): Returns the path for raw report data.
        get_variables_file(form_name): Returns the path for a specific form's variables data.
        get_subject_questionnaire(subject_id, event_name): Returns the path for a subject's questionnaire data.
    """
    def __init__(self, path: str | Path = '../downloaded_data'):
        path = Path(path)
        self._logger = logging.getLogger('PathsResolver')
        self.timestamp = datetime.now().strftime('%Y%m%d')
        self._main_dir = None
        self.set_main_dir(path)
        self.data_type = None

    def set_main_dir(self, path: str | Path):
        path = Path(path)
        if not path.exists():
            path.mkdir(parents=True)
        if not path.is_dir():
            raise ValueError(f'Main storage: {str(path)} is not a directory')
        if len(list(path.iterdir())) > 1:
            self._logger.warning(f'Main storage: {str(path)} is not empty.')
            response = input('Continue? (y/n): ').strip().lower()
            if response != 'y':
                self._logger.info('Main storage path is not empty and user chose not to continue. '
                                  'Exiting without downloading data.')
                sys.exit(1)
        self._main_dir = path
        self._logger.info(f'Downloading data to: {self._main_dir.absolute()}')

    def get_main_dir(self) -> Path:
        return self._main_dir

    def get_raw_dir(self) -> Path:
        raw_dir = self._main_dir / 'raw'
        if not raw_dir.exists():
            raw_dir.mkdir(parents=True)
        return raw_dir

    def get_meta_dir(self) -> Path:
        meta_dir = self._main_dir / 'meta'
        if not meta_dir.exists():
            meta_dir.mkdir(parents=True)
        return meta_dir

    def get_reports_dir(self) -> Path:
        reports_dir = self._main_dir / 'reports'
        if not reports_dir.exists():
            reports_dir.mkdir(parents=True)
        return reports_dir

    def get_subject_dir(self, subject_id: str) -> Path:
        subject_dir = self.get_reports_dir() / subject_id
        if not subject_dir.exists():
            subject_dir.mkdir(parents=True)
        return subject_dir

    def get_raw_variables_file(self) -> Path:
        return self.get_raw_dir() / f'Variables_raw_{self.timestamp}.csv'

    def get_raw_report_file(self) -> Path:
        return self.get_raw_dir() / f'Report_raw_{self.timestamp}.csv'

    def get_variables_file(self, form_name: str) -> Path:
        return self.get_meta_dir() / f'{form_name}_variables_{self.timestamp}.csv'

    def get_subject_questionnaire(self, subject_id: str, event_name: str) -> Path:
        data_str = 'EMA_' if self.data_type == 'ema' else ''
        return self.get_subject_dir(subject_id) / f'{subject_id}_PROM-{data_str}{event_name}_{self.timestamp}.csv'
