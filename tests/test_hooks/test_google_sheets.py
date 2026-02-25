"""Tests for GoogleSheetsHook."""

from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from airflow_google_sheets.exceptions import GoogleSheetsAuthError, GoogleSheetsAPIError
from airflow_google_sheets.hooks.google_sheets import GoogleSheetsHook


SPREADSHEET_ID = "test-spreadsheet-id"
SHEET_NAME = "Sheet1"
RANGE = f"{SHEET_NAME}!A1:C10"

FAKE_CREDENTIALS_DICT = {
    "type": "service_account",
    "project_id": "test-project",
    "private_key_id": "key-id",
    "private_key": "-----BEGIN RSA PRIVATE KEY-----\nMIIBogIBAAJBALqz\n-----END RSA PRIVATE KEY-----\n",
    "client_email": "test@test-project.iam.gserviceaccount.com",
    "client_id": "123456789",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
}


@pytest.fixture
def mock_connection():
    """Create a mock Airflow connection with service account credentials."""
    conn = MagicMock()
    conn.extra_dejson = {"keyfile_dict": FAKE_CREDENTIALS_DICT}
    return conn


@pytest.fixture
def mock_service():
    """Create a mock Google Sheets API service."""
    return MagicMock()


@pytest.fixture
def hook(mock_connection, mock_service):
    """Create a GoogleSheetsHook with mocked connection and service."""
    with patch.object(GoogleSheetsHook, "get_connection", return_value=mock_connection), \
         patch("airflow_google_sheets.hooks.google_sheets.Credentials") as mock_creds_cls, \
         patch("airflow_google_sheets.hooks.google_sheets.build", return_value=mock_service):
        mock_creds_cls.from_service_account_info.return_value = MagicMock()
        h = GoogleSheetsHook(gcp_conn_id="test_conn")
        h.get_conn()  # trigger build
        return h


class TestAuthentication:
    @patch("airflow_google_sheets.hooks.google_sheets.build")
    @patch("airflow_google_sheets.hooks.google_sheets.Credentials")
    def test_auth_with_valid_credentials(self, mock_creds_cls, mock_build, mock_connection):
        mock_creds_cls.from_service_account_info.return_value = MagicMock()
        mock_build.return_value = MagicMock()

        with patch.object(GoogleSheetsHook, "get_connection", return_value=mock_connection):
            hook = GoogleSheetsHook(gcp_conn_id="test_conn")
            service = hook.get_conn()

        mock_creds_cls.from_service_account_info.assert_called_once()
        mock_build.assert_called_once()
        assert service is not None

    def test_auth_with_invalid_credentials_raises_auth_error(self):
        conn = MagicMock()
        conn.extra_dejson = {"keyfile_dict": {"invalid": "data"}}

        with patch.object(GoogleSheetsHook, "get_connection", return_value=conn):
            hook = GoogleSheetsHook(gcp_conn_id="bad_conn")
            with pytest.raises(GoogleSheetsAuthError, match="Failed to authenticate"):
                hook.get_conn()

    @patch("airflow_google_sheets.hooks.google_sheets.build")
    @patch("airflow_google_sheets.hooks.google_sheets.Credentials")
    def test_auth_with_raw_json_extra(self, mock_creds_cls, mock_build):
        """When extra IS the keyfile dict directly (no nested keyfile_dict key)."""
        conn = MagicMock()
        conn.extra_dejson = FAKE_CREDENTIALS_DICT.copy()

        mock_creds_cls.from_service_account_info.return_value = MagicMock()
        mock_build.return_value = MagicMock()

        with patch.object(GoogleSheetsHook, "get_connection", return_value=conn):
            hook = GoogleSheetsHook(gcp_conn_id="test_conn")
            hook.get_conn()

        call_args = mock_creds_cls.from_service_account_info.call_args
        assert call_args[0][0]["project_id"] == "test-project"

    @patch("airflow_google_sheets.hooks.google_sheets.build")
    @patch("airflow_google_sheets.hooks.google_sheets.Credentials")
    def test_service_is_cached(self, mock_creds_cls, mock_build, mock_connection):
        mock_creds_cls.from_service_account_info.return_value = MagicMock()
        mock_build.return_value = MagicMock()

        with patch.object(GoogleSheetsHook, "get_connection", return_value=mock_connection):
            hook = GoogleSheetsHook(gcp_conn_id="test_conn")
            s1 = hook.get_conn()
            s2 = hook.get_conn()

        assert s1 is s2
        assert mock_build.call_count == 1


class TestGetValues:
    def test_get_values_returns_data(self, hook, mock_service):
        expected = [["a", "b"], ["c", "d"]]
        mock_service.spreadsheets().values().get().execute.return_value = {"values": expected}

        result = hook.get_values(SPREADSHEET_ID, RANGE)
        assert result == expected

    def test_get_values_empty_range(self, hook, mock_service):
        mock_service.spreadsheets().values().get().execute.return_value = {}

        result = hook.get_values(SPREADSHEET_ID, RANGE)
        assert result == []


class TestBatchGetValues:
    def test_batch_get_returns_multiple_ranges(self, hook, mock_service):
        mock_service.spreadsheets().values().batchGet().execute.return_value = {
            "valueRanges": [
                {"values": [["a"]]},
                {"values": [["b"]]},
            ]
        }

        result = hook.batch_get_values(SPREADSHEET_ID, ["A:A", "B:B"])
        assert result == [[["a"]], [["b"]]]


class TestUpdateValues:
    def test_update_values_calls_api(self, hook, mock_service):
        values = [["x", "y"]]
        mock_service.spreadsheets().values().update().execute.return_value = {"updatedCells": 2}

        result = hook.update_values(SPREADSHEET_ID, RANGE, values)
        assert result == {"updatedCells": 2}


class TestAppendValues:
    def test_append_values_calls_api(self, hook, mock_service):
        values = [["x", "y"]]
        mock_service.spreadsheets().values().append().execute.return_value = {"updates": {"updatedRows": 1}}

        result = hook.append_values(SPREADSHEET_ID, RANGE, values)
        assert result == {"updates": {"updatedRows": 1}}


class TestClearValues:
    def test_clear_values_calls_api(self, hook, mock_service):
        mock_service.spreadsheets().values().clear().execute.return_value = {"clearedRange": RANGE}

        result = hook.clear_values(SPREADSHEET_ID, RANGE)
        assert result == {"clearedRange": RANGE}


class TestBatchUpdate:
    def test_batch_update_calls_api(self, hook, mock_service):
        requests = [{"addSheet": {"properties": {"title": "New"}}}]
        mock_service.spreadsheets().batchUpdate().execute.return_value = {"replies": [{}]}

        result = hook.batch_update(SPREADSHEET_ID, requests)
        assert result == {"replies": [{}]}


class TestCreateSpreadsheet:
    def test_create_spreadsheet_returns_id(self, hook, mock_service):
        mock_service.spreadsheets().create().execute.return_value = {"spreadsheetId": "new-id"}

        result = hook.create_spreadsheet("Test Sheet")
        assert result == "new-id"


class TestGetSpreadsheetMetadata:
    def test_returns_metadata(self, hook, mock_service):
        meta = {"sheets": [{"properties": {"title": "Sheet1", "sheetId": 0}}]}
        mock_service.spreadsheets().get().execute.return_value = meta

        result = hook.get_spreadsheet_metadata(SPREADSHEET_ID)
        assert result == meta


class TestGetSheetId:
    def test_resolves_sheet_name_to_id(self, hook, mock_service):
        meta = {"sheets": [{"properties": {"title": "Data", "sheetId": 42}}]}
        mock_service.spreadsheets().get().execute.return_value = meta

        result = hook.get_sheet_id(SPREADSHEET_ID, "Data")
        assert result == 42

    def test_raises_on_missing_sheet(self, hook, mock_service):
        meta = {"sheets": [{"properties": {"title": "Other", "sheetId": 0}}]}
        mock_service.spreadsheets().get().execute.return_value = meta

        with pytest.raises(GoogleSheetsAPIError, match="not found"):
            hook.get_sheet_id(SPREADSHEET_ID, "Missing")
