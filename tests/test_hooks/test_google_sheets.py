"""Tests for GoogleSheetsHook."""

from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from googleapiclient.errors import HttpError
from httplib2 import Response

from airflow_provider_google_sheets.exceptions import GoogleSheetsAuthError, GoogleSheetsAPIError
from airflow_provider_google_sheets.hooks.google_sheets import GoogleSheetsHook


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
         patch("airflow_provider_google_sheets.hooks.google_sheets.Credentials") as mock_creds_cls, \
         patch("airflow_provider_google_sheets.hooks.google_sheets.build", return_value=mock_service):
        mock_creds_cls.from_service_account_info.return_value = MagicMock()
        h = GoogleSheetsHook(gcp_conn_id="test_conn")
        h.get_conn()  # trigger build
        return h


class TestAuthentication:
    @patch("airflow_provider_google_sheets.hooks.google_sheets.build")
    @patch("airflow_provider_google_sheets.hooks.google_sheets.Credentials")
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

    @patch("airflow_provider_google_sheets.hooks.google_sheets.build")
    @patch("airflow_provider_google_sheets.hooks.google_sheets.Credentials")
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

    @patch("airflow_provider_google_sheets.hooks.google_sheets.build")
    @patch("airflow_provider_google_sheets.hooks.google_sheets.Credentials")
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

        append_mock = mock_service.spreadsheets().values().append
        assert append_mock.call_args.kwargs["insertDataOption"] == "INSERT_ROWS"
        assert append_mock.call_args.kwargs["valueInputOption"] == "USER_ENTERED"


class TestClearValues:
    def test_clear_values_calls_api(self, hook, mock_service):
        mock_service.spreadsheets().values().clear().execute.return_value = {"clearedRange": RANGE}

        result = hook.clear_values(SPREADSHEET_ID, RANGE)
        assert result == {"clearedRange": RANGE}


class TestBatchUpdateValues:
    def test_batch_update_values_calls_api(self, hook, mock_service):
        data = [
            {"range": "A1:B1", "values": [["x", "y"]]},
            {"range": "A2:B2", "values": [["a", "b"]]},
        ]
        mock_service.spreadsheets().values().batchUpdate().execute.return_value = {
            "totalUpdatedCells": 4
        }

        result = hook.batch_update_values(SPREADSHEET_ID, data)
        assert result == {"totalUpdatedCells": 4}


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


class TestHookFailFastOn404:
    """404 is absent from DEFAULT_RETRYABLE_STATUS_CODES=(429,500,503), so the
    real @retry_with_backoff decorator must raise it immediately without any
    backoff. This is a regression guard: read methods stay fail-fast, keeping a
    wrong spreadsheet_id from hanging ~35s before failing.
    """

    def test_get_values_404_does_not_retry(self, hook, mock_service):
        err = HttpError(Response({"status": 404}), b"not found")
        get_execute = mock_service.spreadsheets().values().get().execute
        get_execute.side_effect = err

        with patch(
            "airflow_provider_google_sheets.utils.retry.time.sleep"
        ) as sleep:
            with pytest.raises(HttpError) as ei:
                hook.get_values(SPREADSHEET_ID, RANGE)

        assert ei.value.resp.status == 404
        sleep.assert_not_called()
        # exactly one underlying call — no retry loop
        assert get_execute.call_count == 1

    def test_get_spreadsheet_metadata_404_does_not_retry(self, hook, mock_service):
        err = HttpError(Response({"status": 404}), b"not found")
        get_execute = mock_service.spreadsheets().get().execute
        get_execute.side_effect = err

        with patch(
            "airflow_provider_google_sheets.utils.retry.time.sleep"
        ) as sleep:
            with pytest.raises(HttpError) as ei:
                hook.get_spreadsheet_metadata(SPREADSHEET_ID)

        assert ei.value.resp.status == 404
        sleep.assert_not_called()
        assert get_execute.call_count == 1


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


class TestTrimSheet:
    def test_trim_sheet_deletes_extra_rows(self, hook, mock_service):
        meta = {
            "sheets": [{
                "properties": {
                    "title": SHEET_NAME,
                    "sheetId": 0,
                    "gridProperties": {"rowCount": 20},
                }
            }]
        }
        mock_service.spreadsheets().get().execute.return_value = meta
        mock_service.spreadsheets().batchUpdate().execute.return_value = {"replies": [{}]}

        hook.trim_sheet(SPREADSHEET_ID, SHEET_NAME, keep_rows=10)

        call_args = mock_service.spreadsheets().batchUpdate.call_args
        requests = call_args[1]["body"]["requests"]
        assert len(requests) == 1
        delete_range = requests[0]["deleteDimension"]["range"]
        assert delete_range["sheetId"] == 0
        assert delete_range["dimension"] == "ROWS"
        assert delete_range["startIndex"] == 10
        assert delete_range["endIndex"] == 20

    def test_trim_sheet_noop_when_fewer_rows(self, hook, mock_service):
        meta = {
            "sheets": [{
                "properties": {
                    "title": SHEET_NAME,
                    "sheetId": 0,
                    "gridProperties": {"rowCount": 5},
                }
            }]
        }
        mock_service.spreadsheets().get().execute.return_value = meta

        hook.trim_sheet(SPREADSHEET_ID, SHEET_NAME, keep_rows=10)

        mock_service.spreadsheets().batchUpdate.assert_not_called()

    def test_trim_sheet_noop_when_equal_rows(self, hook, mock_service):
        meta = {
            "sheets": [{
                "properties": {
                    "title": SHEET_NAME,
                    "sheetId": 0,
                    "gridProperties": {"rowCount": 10},
                }
            }]
        }
        mock_service.spreadsheets().get().execute.return_value = meta

        hook.trim_sheet(SPREADSHEET_ID, SHEET_NAME, keep_rows=10)

        mock_service.spreadsheets().batchUpdate.assert_not_called()


class TestRequestTimeout:
    def _build_hook(self, mock_connection, timeout):
        with patch.object(GoogleSheetsHook, "get_connection", return_value=mock_connection), \
             patch("airflow_provider_google_sheets.hooks.google_sheets.Credentials") as mock_creds, \
             patch("airflow_provider_google_sheets.hooks.google_sheets.httplib2.Http") as mock_http_cls, \
             patch("airflow_provider_google_sheets.hooks.google_sheets.AuthorizedHttp") as mock_auth_cls, \
             patch("airflow_provider_google_sheets.hooks.google_sheets.build") as mock_build:
            mock_creds.from_service_account_info.return_value = MagicMock()
            mock_http_cls.return_value = MagicMock()
            mock_auth_cls.return_value = MagicMock()
            mock_build.return_value = MagicMock()
            hook = GoogleSheetsHook(gcp_conn_id="test_conn", request_timeout=timeout)
            hook.get_conn()
            return mock_http_cls, mock_auth_cls, mock_build

    def test_default_timeout_creates_authorized_http(self, mock_connection):
        mock_http_cls, mock_auth_cls, mock_build = self._build_hook(mock_connection, 300)

        mock_http_cls.assert_called_once_with(timeout=300)
        mock_auth_cls.assert_called_once()
        call_kwargs = mock_build.call_args[1]
        assert "http" in call_kwargs
        assert "credentials" not in call_kwargs

    def test_custom_timeout_passed_to_httplib2(self, mock_connection):
        mock_http_cls, mock_auth_cls, mock_build = self._build_hook(mock_connection, 600)

        mock_http_cls.assert_called_once_with(timeout=600)
        call_kwargs = mock_build.call_args[1]
        assert "http" in call_kwargs

    def test_none_timeout_uses_legacy_credentials_path(self, mock_connection):
        mock_http_cls, mock_auth_cls, mock_build = self._build_hook(mock_connection, None)

        mock_http_cls.assert_not_called()
        mock_auth_cls.assert_not_called()
        call_kwargs = mock_build.call_args[1]
        assert "credentials" in call_kwargs
        assert "http" not in call_kwargs
