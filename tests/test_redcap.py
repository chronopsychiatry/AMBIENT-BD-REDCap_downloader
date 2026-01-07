import pytest
from unittest.mock import patch, MagicMock

from redcap_downloader.redcap_api.redcap import REDCap
from redcap_downloader.redcap_api.dom import Variables, Report


class DummyProperties:
    redcap_token = "dummy_token"


@pytest.fixture
def properties():
    return DummyProperties()


@pytest.fixture
def redcap(properties):
    return REDCap(properties)


def test_get_questionnaire_variables_success(redcap):
    csv_data = "field_name,form_name\nstudy_id,screening\nparticipant_id,baseline"
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = csv_data

    with patch("requests.post", return_value=mock_response) as mock_post:
        variables = redcap.get_questionnaire_variables()
        assert isinstance(variables, Variables)
        assert "field_name" in variables.raw_data.columns
        assert "form_name" in variables.raw_data.columns
        mock_post.assert_called_once()


def test_get_questionnaire_variables_failure(redcap):
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_response.text = "Not Found"

    with patch("requests.post", return_value=mock_response):
        with pytest.raises(Exception) as excinfo:
            redcap.get_questionnaire_variables()
        assert "HTTP Error: 404" in str(excinfo.value)


def test_get_questionnaire_report_success(redcap):
    csv_data = "study_id,redcap_event_name,consent_contact\n1,event1,1\n2,event2,1"
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = csv_data

    with patch("requests.post", return_value=mock_response) as mock_post:
        report = redcap.get_questionnaire_report()
        assert isinstance(report, Report)
        assert "study_id" in report.raw_data.columns
        assert "redcap_event_name" in report.raw_data.columns
        mock_post.assert_called_once()


def test_get_questionnaire_report_failure(redcap):
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.text = "Internal Server Error"

    with patch("requests.post", return_value=mock_response):
        with pytest.raises(Exception) as excinfo:
            redcap.get_questionnaire_report()
        assert "HTTP Error: 500" in str(excinfo.value)
