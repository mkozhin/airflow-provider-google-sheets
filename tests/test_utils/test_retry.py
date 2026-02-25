"""Tests for retry utility."""

from unittest.mock import MagicMock, patch

import pytest
from googleapiclient.errors import HttpError
from httplib2 import Response

from airflow_google_sheets.utils.retry import is_retryable_http_error, retry_with_backoff


def _make_http_error(status: int) -> HttpError:
    resp = Response({"status": status})
    return HttpError(resp, b"error")


class TestIsRetryableHttpError:
    def test_429_is_retryable(self):
        assert is_retryable_http_error(_make_http_error(429)) is True

    def test_500_is_retryable(self):
        assert is_retryable_http_error(_make_http_error(500)) is True

    def test_503_is_retryable(self):
        assert is_retryable_http_error(_make_http_error(503)) is True

    def test_400_is_not_retryable(self):
        assert is_retryable_http_error(_make_http_error(400)) is False

    def test_403_is_not_retryable(self):
        assert is_retryable_http_error(_make_http_error(403)) is False

    def test_404_is_not_retryable(self):
        assert is_retryable_http_error(_make_http_error(404)) is False

    def test_non_http_error_is_not_retryable(self):
        assert is_retryable_http_error(ValueError("test")) is False


class TestRetryWithBackoff:
    @patch("airflow_google_sheets.utils.retry.time.sleep")
    def test_succeeds_on_first_attempt(self, mock_sleep):
        @retry_with_backoff(max_retries=3)
        def func():
            return "ok"

        assert func() == "ok"
        mock_sleep.assert_not_called()

    @patch("airflow_google_sheets.utils.retry.time.sleep")
    def test_retries_on_429_then_succeeds(self, mock_sleep):
        call_count = 0

        @retry_with_backoff(max_retries=3, base_delay=1.0, jitter=False)
        def func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise _make_http_error(429)
            return "ok"

        assert func() == "ok"
        assert call_count == 3
        assert mock_sleep.call_count == 2

    @patch("airflow_google_sheets.utils.retry.time.sleep")
    def test_raises_after_max_retries(self, mock_sleep):
        @retry_with_backoff(max_retries=2, base_delay=1.0, jitter=False)
        def func():
            raise _make_http_error(429)

        with pytest.raises(HttpError):
            func()
        assert mock_sleep.call_count == 2

    @patch("airflow_google_sheets.utils.retry.time.sleep")
    def test_no_retry_on_400(self, mock_sleep):
        @retry_with_backoff(max_retries=3)
        def func():
            raise _make_http_error(400)

        with pytest.raises(HttpError):
            func()
        mock_sleep.assert_not_called()

    @patch("airflow_google_sheets.utils.retry.time.sleep")
    def test_no_retry_on_403(self, mock_sleep):
        @retry_with_backoff(max_retries=3)
        def func():
            raise _make_http_error(403)

        with pytest.raises(HttpError):
            func()
        mock_sleep.assert_not_called()

    @patch("airflow_google_sheets.utils.retry.time.sleep")
    def test_exponential_backoff_delay(self, mock_sleep):
        call_count = 0

        @retry_with_backoff(max_retries=3, base_delay=1.0, jitter=False)
        def func():
            nonlocal call_count
            call_count += 1
            if call_count <= 3:
                raise _make_http_error(500)
            return "ok"

        func()
        delays = [call.args[0] for call in mock_sleep.call_args_list]
        assert delays == [1.0, 2.0, 4.0]

    @patch("airflow_google_sheets.utils.retry.time.sleep")
    def test_delay_capped_at_max_delay(self, mock_sleep):
        call_count = 0

        @retry_with_backoff(max_retries=5, base_delay=10.0, max_delay=30.0, jitter=False)
        def func():
            nonlocal call_count
            call_count += 1
            if call_count <= 5:
                raise _make_http_error(503)
            return "ok"

        func()
        delays = [call.args[0] for call in mock_sleep.call_args_list]
        # base_delay * 2^attempt: 10, 20, 30(capped), 30(capped), 30(capped)
        assert delays == [10.0, 20.0, 30.0, 30.0, 30.0]

    @patch("airflow_google_sheets.utils.retry.time.sleep")
    def test_retries_custom_exception(self, mock_sleep):
        call_count = 0

        @retry_with_backoff(max_retries=2, base_delay=1.0, jitter=False, retryable_exceptions=[ConnectionError])
        def func():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ConnectionError("connection lost")
            return "ok"

        assert func() == "ok"
        assert call_count == 2

    @patch("airflow_google_sheets.utils.retry.time.sleep")
    def test_no_retry_on_non_retryable_exception(self, mock_sleep):
        @retry_with_backoff(max_retries=3)
        def func():
            raise ValueError("bad value")

        with pytest.raises(ValueError):
            func()
        mock_sleep.assert_not_called()
