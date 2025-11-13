import os
import tempfile
import pandas as pd

from redcap_downloader.data_cleaning.data_cleaner import DataCleaner
from redcap_downloader.storage.path_resolver import PathResolver
from redcap_downloader.redcap_api.redcap import REDCap
from redcap_downloader.redcap_api.dom import Report, Variables


class MockREDCap(REDCap):

    def __init__(self):
        self.test_report = pd.read_csv('./tests/data/test_report.csv')
        self.test_variables = pd.read_csv('./tests/data/test_variables.csv')

    def get_questionnaire_variables(self):
        return Variables(self.test_variables)

    def get_questionnaire_report(self):
        return Report(self.test_report)


class TestDataCleaner:

    mock_redcap = MockREDCap()
    test_dir = tempfile.TemporaryDirectory()
    paths = PathResolver(test_dir.name)
    cleaner = DataCleaner(redcap=mock_redcap, paths=paths)
    test_report = pd.read_csv('./tests/data/test_report.csv')
    test_variables = pd.read_csv('./tests/data/test_variables.csv')

    def test_initialisation(self):
        cleaner = DataCleaner(redcap=self.mock_redcap, paths=self.paths)
        assert cleaner.redcap == self.mock_redcap
        assert cleaner.paths == self.paths

    def test_save_questionnaire_variables(self):
        self.cleaner.save_questionnaire_variables()

        expected_path = self.paths.get_variables_file(form_name='Scre')
        assert os.path.exists(expected_path)

        screening_form = pd.read_csv(expected_path)
        assert not screening_form.empty
        assert 'empty_column' not in screening_form.columns
        assert 'non-selected_column' not in screening_form.columns

    def test_save_questionnaire_reports(self):
        self.cleaner.save_questionnaire_reports()

        expected_path = self.paths.get_subject_questionnaire(subject_id='ABD001', data_type='', event_name='Ques')
        assert os.path.exists(expected_path)

        subject1_report = pd.read_csv(expected_path)
        assert not subject1_report.empty
        assert 'empty_column' not in subject1_report.columns
        assert 'consent_contact' in subject1_report.columns

    def test_clean_variables(self):
        variables = self.cleaner.clean_variables(Variables(self.test_variables))

        assert isinstance(variables, Variables)
        assert 'empty_column' not in variables.data.columns
        assert 'non-selected_column' not in variables.data.columns
        assert '<' not in variables.data['section_header'].values

    def test_clean_reports(self):
        reports = self.cleaner.clean_reports(Report(self.test_report))

        assert isinstance(reports, Report)
        assert 'consent_contact' in reports.data.columns
        assert not reports.data['consent_contact'].isna().all()

    def test_clean_variables_form_names(self):
        cleaned_df = self.cleaner.clean_variables_form_names(self.test_variables, data_type='questionnaire')

        assert 'baseline_researcher_cb' not in cleaned_df['form_name'].values
        assert 'Baseline' in cleaned_df['form_name'].values
        assert 'Screening' in cleaned_df['form_name'].values
        assert len(cleaned_df.columns) == len(cleaned_df.columns.unique())

    def test_clean_reports_form_names(self):
        cleaned_df = self.cleaner.clean_reports_form_names(self.test_report, data_type='questionnaire')

        assert 'baseline_researcher_cb' not in cleaned_df['redcap_event_name'].values
        assert 'baseline' in cleaned_df['redcap_event_name'].values
        assert 'screening' in cleaned_df['redcap_event_name'].values
        assert len(cleaned_df.columns) == len(cleaned_df.columns.unique())

    def test_filter_variables_columns(self):
        filtered_df = self.cleaner.filter_variables_columns(self.test_variables)

        assert 'field_name' in filtered_df.columns
        assert 'empty_column' not in filtered_df.columns
        assert 'non-selected_column' not in filtered_df.columns

    def test_remove_html_tags(self):
        df_with_html = pd.DataFrame({
            'section_header': ['<b>Header</b>', '<i>Italic</i>', 'No HTML'],
            'field_label': ['Label <span>1</span>', 'Label 2', 'Label 3']
        })
        cleaned_df = self.cleaner.remove_html_tags(df_with_html)

        assert '<' not in cleaned_df['section_header'].values
        assert '<' not in cleaned_df['field_label'].values
        assert 'No HTML' in cleaned_df['section_header'].values
