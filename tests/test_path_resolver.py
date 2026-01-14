import tempfile
import os
from pathlib import Path
import pytest

from redcap_downloader.storage.path_resolver import PathResolver


class TestPathsResolver():

    temp_dir = tempfile.TemporaryDirectory()
    test_dir = Path(temp_dir.name)
    resolver = PathResolver(test_dir)

    def test_initialization(self):
        assert self.resolver._main_dir == self.test_dir

    def test_set_main_dir_creates_directory(self):
        self.resolver.set_main_dir(self.test_dir)
        assert os.path.exists(self.test_dir)
        assert self.resolver.get_main_dir() == self.test_dir

    def test_set_main_dir_raises_exception(self):
        path = os.path.join(self.test_dir, 'file.txt')
        with open(path, 'w') as temp_file:
            temp_file.write('This is a temporary file.')
        with pytest.raises(ValueError):
            self.resolver.set_main_dir(os.path.join(self.test_dir, 'file.txt'))

    def test_get_main_dir(self):
        assert self.resolver.get_main_dir() == self.test_dir

    def test_get_raw_dir(self):
        expected_path = self.test_dir / 'raw'
        assert self.resolver.get_raw_dir() == expected_path
        assert expected_path.exists()

    def test_get_meta_dir(self):
        expected_path = self.test_dir / 'meta'
        assert self.resolver.get_meta_dir() == expected_path
        assert expected_path.exists()

    def test_get_reports_dir(self):
        expected_path = self.test_dir / 'reports'
        assert self.resolver.get_reports_dir() == expected_path
        assert expected_path.exists()

    def test_get_subject_dir(self):
        subject_id = 'subject_123'
        expected_path = self.test_dir / 'reports' / subject_id
        assert self.resolver.get_subject_dir(subject_id) == expected_path
        assert expected_path.exists()

    def test_get_raw_variables_file(self):
        expected_path = self.test_dir / 'raw' / f'Variables_raw_{self.resolver.timestamp}.csv'
        assert self.resolver.get_raw_variables_file() == expected_path

    def test_get_raw_report_file(self):
        expected_path = self.test_dir / 'raw' / f'Report_raw_{self.resolver.timestamp}.csv'
        assert self.resolver.get_raw_report_file() == expected_path

    def test_get_variables_file(self):
        form_name = 'test_form'
        expected_path = self.test_dir / 'meta' / f'{form_name}_variables_{self.resolver.timestamp}.csv'
        assert self.resolver.get_variables_file(form_name) == expected_path

    def test_get_subject_questionnaire(self):
        subject_id = 'subject_123'
        event_name = 'event_456'
        expected_path = (
            self.test_dir
            / 'reports'
            / subject_id
            / f'{subject_id}_PROM-{event_name}_{self.resolver.timestamp}.csv'
        )
        assert self.resolver.get_subject_questionnaire(subject_id, event_name=event_name) == expected_path
        assert not expected_path.exists()

    def test_tearDown(self):
        self.temp_dir.cleanup()
