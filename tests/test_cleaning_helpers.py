import pandas as pd
import logging

from redcap_downloader.data_cleaning.helpers import (drop_empty_columns,
                                                     merge_duplicate_columns,
                                                     replace_column_name,
                                                     replace_strings,
                                                     fill_participant_ids,
                                                     get_ema_period_number,
                                                     fix_24h_sleeptimes
                                                     )


class TestCleaningHelpers:
    def test_drop_empty_columns(self):
        df = pd.DataFrame({
            'A': [1, 2, 3],
            'B': [None, None, None],
            'C': [4, 5, 6]
        })
        result = drop_empty_columns(df)
        expected = pd.DataFrame({
            'A': [1, 2, 3],
            'C': [4, 5, 6]
        })
        pd.testing.assert_frame_equal(result, expected)

    def test_merge_duplicate_columns(self):
        df = pd.DataFrame({
            'A': ['1', None, '3'],
            'C': [None, '2', None],
            'B': ['4', '5', '6']
        }).rename(columns={'C': 'A'})
        result = merge_duplicate_columns(df)
        expected = pd.DataFrame({
            'A': ['1', '2', '3'],
            'B': ['4', '5', '6']
        })
        pd.testing.assert_frame_equal(result, expected)

    def test_replace_column_name(self):
        col_name = 'field_response_time_ms_p4'
        result = replace_column_name(col_name)
        expected = 'response_time_ms'
        assert result == expected

    def test_replace_strings(self):
        series = pd.Series(['apple', 'banana', 'cherry'])
        replacements = {'apple': 'orange', 'banana': 'grape'}
        result = replace_strings(series, replacements)
        expected = pd.Series(['orange', 'grape', 'cherry'])
        pd.testing.assert_series_equal(result, expected)

    def test_fill_participant_ids(self):
        df = pd.DataFrame({
            'participant_id': [2, None, 4, None, None],
            'EMA_period_number': [1, 1, 2, 2, 2],
            'data': [10, 20, 30, 40, 50]
        })
        result = fill_participant_ids(df)
        expected = pd.DataFrame({
            'participant_id': ['ABD002', 'ABD002', 'ABD004', 'ABD004', 'ABD004'],
            'EMA_period_number': [1, 1, 2, 2, 2],
            'data': [10, 20, 30, 40, 50]
        })
        pd.testing.assert_frame_equal(result, expected)

    def test_get_ema_period_number(self):
        project_title = "Ambient-BD EMA PERIOD 3"
        result = get_ema_period_number(project_title)
        expected = 3
        assert result == expected

    def test_fix_24h_sleeptimes(self):
        df = pd.DataFrame({
            'try_sleep_time': ['23:30:00', '10:15:00', '11:00:00', '22:45:00'],
            'participant_id': ['ABD001', 'ABD001', 'ABD003', 'ABD004'],
            'other_data': [1, 2, 3, 4]
        })
        logger = logging.getLogger('test_logger')
        result = fix_24h_sleeptimes(df, logger)
        expected = pd.DataFrame({
            'try_sleep_time': ['23:30:00',
                               pd.to_datetime('22:15:00', format='%H:%M:%S').time(),
                               pd.to_datetime('23:00:00', format='%H:%M:%S').time(),
                               '22:45:00'],
            'participant_id': ['ABD001', 'ABD001', 'ABD003', 'ABD004'],
            'other_data': [1, 2, 3, 4]
        })
        pd.testing.assert_frame_equal(result, expected)
