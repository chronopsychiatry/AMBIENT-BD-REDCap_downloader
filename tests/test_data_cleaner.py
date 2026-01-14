import os
import tempfile
import pandas as pd

from redcap_downloader.data_cleaning.data_cleaner import DataCleaner
from redcap_downloader.storage.path_resolver import PathResolver
from redcap_downloader.redcap_api.dom import Report, Variables


class TestDataCleaner:

    test_dir = tempfile.TemporaryDirectory()
    paths = PathResolver(test_dir.name)
    report_df = pd.read_csv('./tests/data/test_report.csv')
    variables_df = pd.read_csv('./tests/data/test_variables.csv')
    report = Report(report_df)
    variables = Variables(variables_df)
    cleaner = DataCleaner(
        paths=paths,
        report=report,
        variables=variables,
        data_type='questionnaire',
        include_identifiers=False
    )

    def test_initialisation(self):
        cleaner = DataCleaner(
            paths=self.paths,
            report=self.report,
            variables=self.variables,
            data_type='questionnaire',
            include_identifiers=False
            )
        assert cleaner.paths == self.paths
        assert cleaner.report == self.report
        assert cleaner.variables == self.variables
        assert cleaner.data_type == 'questionnaire'
        assert cleaner.include_identifiers is False

    def test_save_cleaned_variables(self):
        self.cleaner.save_cleaned_variables()

        expected_path = self.paths.get_variables_file(form_name='Scre')
        assert os.path.exists(expected_path)

        screening_form = pd.read_csv(expected_path)
        assert not screening_form.empty
        assert 'empty_column' not in screening_form.columns
        assert 'non-selected_column' not in screening_form.columns

    def test_save_cleaned_reports(self):
        self.cleaner.save_cleaned_reports()

        expected_path = self.paths.get_subject_questionnaire(subject_id='ABD001', event_name='Ques')
        assert os.path.exists(expected_path)

        subject1_report = pd.read_csv(expected_path)
        test_clean = self.cleaner.clean_reports(self.report)
        print(test_clean.data)
        assert not subject1_report.empty
        assert 'empty_column' not in subject1_report.columns
        assert 'consent_contact' in subject1_report.columns

    def test_remove_identifiers(self):
        cleaned_report = self.cleaner.remove_identifiers(Report(self.report_df), self.variables)

        assert 'name' not in cleaned_report.data.columns
        assert 'study_id' in cleaned_report.data.columns
        assert 'name' not in cleaned_report.raw_data.columns
        assert 'study_id' in cleaned_report.raw_data.columns

    def test_clean_variables(self):
        variables = self.cleaner.clean_variables(self.variables)

        assert isinstance(variables, Variables)
        assert 'empty_column' not in variables.data.columns
        assert 'non-selected_column' not in variables.data.columns
        assert '<' not in variables.data['section_header'].values

    def test_clean_reports(self):
        reports = self.cleaner.clean_reports(self.report)

        assert isinstance(reports, Report)
        assert 'consent_contact' in reports.data.columns
        assert not reports.data['consent_contact'].isna().all()

    def test_clean_variables_form_names(self):
        cleaned_df = self.cleaner.clean_variables_form_names(self.variables_df)

        assert 'baseline_researcher_cb' not in cleaned_df['form_name'].values
        assert 'Baseline' in cleaned_df['form_name'].values
        assert 'Screening' in cleaned_df['form_name'].values
        assert len(cleaned_df.columns) == len(cleaned_df.columns.unique())

    def test_clean_reports_form_names(self):
        cleaned_df = self.cleaner.clean_reports_form_names(self.report_df)

        assert 'baseline_researcher_cb' not in cleaned_df['redcap_event_name'].values
        assert 'baseline' in cleaned_df['redcap_event_name'].values
        assert 'screening' in cleaned_df['redcap_event_name'].values
        assert len(cleaned_df.columns) == len(cleaned_df.columns.unique())

    def test_filter_variables_columns(self):
        filtered_df = self.cleaner.filter_variables_columns(self.variables_df)

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

    def test_get_report_entries_table(self):
        self.report.data = (self.report.data
                            .assign(output_form='redcap_event_name')
                            )
        entries = self.cleaner.get_report_entries_table()

        assert "form" in entries
        assert "entries" in entries
        assert "redcap_event_name" in entries
        assert "3" in entries
