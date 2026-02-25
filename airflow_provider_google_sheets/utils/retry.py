"""Retry utility with exponential backoff and jitter for Google Sheets API calls."""

from __future__ import annotations

import functools
import logging
import random
import time
from typing import Any, Callable, Sequence, Type

from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)

DEFAULT_RETRYABLE_STATUS_CODES = (429, 500, 503)


def is_retryable_http_error(error: Exception, retryable_status_codes: Sequence[int] = DEFAULT_RETRYABLE_STATUS_CODES) -> bool:
    """Check if an HttpError should be retried based on its status code."""
    if isinstance(error, HttpError):
        return error.resp.status in retryable_status_codes
    return False


def retry_with_backoff(
    max_retries: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    jitter: bool = True,
    retryable_exceptions: Sequence[Type[Exception]] | None = None,
    retryable_status_codes: Sequence[int] = DEFAULT_RETRYABLE_STATUS_CODES,
) -> Callable:
    """Decorator that retries a function with exponential backoff and jitter.

    Args:
        max_retries: Maximum number of retry attempts.
        base_delay: Initial delay in seconds before first retry.
        max_delay: Maximum delay in seconds between retries.
        jitter: Whether to add random jitter to the delay.
        retryable_exceptions: Exception types that should trigger a retry.
            If ``None``, only ``HttpError`` with retryable status codes is retried.
        retryable_status_codes: HTTP status codes that should trigger a retry
            when an ``HttpError`` is raised.
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exception: Exception | None = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e

                    should_retry = False
                    if retryable_exceptions:
                        should_retry = isinstance(e, tuple(retryable_exceptions))
                    if not should_retry:
                        should_retry = is_retryable_http_error(e, retryable_status_codes)

                    if not should_retry or attempt >= max_retries:
                        raise

                    delay = min(base_delay * (2 ** attempt), max_delay)
                    if jitter:
                        delay += random.uniform(0, delay * 0.5)

                    logger.warning(
                        "Attempt %d/%d for %s failed: %s. Retrying in %.1fs...",
                        attempt + 1,
                        max_retries + 1,
                        func.__name__,
                        e,
                        delay,
                    )
                    time.sleep(delay)

            raise last_exception  # type: ignore[misc]

        return wrapper

    return decorator
