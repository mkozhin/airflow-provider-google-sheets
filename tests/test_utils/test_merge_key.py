"""Tests for Merge Key normalization and Inferred Schema resolution."""

import logging

import pytest

from airflow_provider_google_sheets.utils.merge_key import (
    infer_date_key_schema,
    normalize_merge_key,
    resolve_merge_key_schema,
)


class TestNormalizeMergeKey:
    """Tests for normalize_merge_key (serial date conversion and fallback strategies)."""

    # ------------------------------------------------------------------
    # Serial date epoch verification (via normalize_merge_key)
    # ------------------------------------------------------------------

    def test_serial_to_date_epoch_verification(self):
        """Serial 46023 must map to 2026-01-01 (epoch = 1899-12-30)."""
        schema = {"type": "date", "format": "%Y-%m-%d"}
        assert normalize_merge_key("46023", schema) == "2026-01-01"

    def test_serial_to_date_known_value(self):
        """Serial 1 maps to 1899-12-31 (one day after epoch 1899-12-30)."""
        schema = {"type": "date", "format": "%Y-%m-%d"}
        assert normalize_merge_key("1", schema) == "1899-12-31"

    # ------------------------------------------------------------------
    # normalize_merge_key — no schema
    # ------------------------------------------------------------------

    def test_no_schema_returns_raw(self):
        assert normalize_merge_key("anything", None) == "anything"

    def test_no_schema_returns_raw_extended_false(self):
        assert normalize_merge_key("anything", None, extended=False) == "anything"

    # ------------------------------------------------------------------
    # Step 1: key already in format
    # ------------------------------------------------------------------

    def test_key_already_in_format_no_change(self):
        schema = {"type": "date", "format": "%Y-%m-%d"}
        assert normalize_merge_key("2026-01-01", schema) == "2026-01-01"

    def test_key_in_format_extended_false(self):
        schema = {"type": "date", "format": "%Y-%m-%d"}
        assert normalize_merge_key("2026-01-01", schema, extended=False) == "2026-01-01"

    # ------------------------------------------------------------------
    # Step 2: key in input_format → converted to format
    # ------------------------------------------------------------------

    def test_extended_true_key_in_input_format_converted(self):
        """Key stored in regional format (dd.mm.yyyy) → normalised to ISO."""
        schema = {"type": "date", "format": "%Y-%m-%d", "input_format": "%d.%m.%Y"}
        assert normalize_merge_key("01.01.2026", schema) == "2026-01-01"

    def test_extended_false_key_in_input_format_not_converted(self):
        """With extended=False input_format fallback is skipped — raw returned."""
        schema = {"type": "date", "format": "%Y-%m-%d", "input_format": "%d.%m.%Y"}
        result = normalize_merge_key("01.01.2026", schema, extended=False)
        assert result == "01.01.2026"

    def test_extended_false_serial_date_not_converted(self):
        """With extended=False serial date fallback (step 3) is skipped — raw returned."""
        schema = {"type": "date", "format": "%Y-%m-%d"}
        result = normalize_merge_key("46023", schema, extended=False)
        assert result == "46023"

    # ------------------------------------------------------------------
    # Step 3: serial date number
    # ------------------------------------------------------------------

    def test_extended_true_serial_date_converted(self):
        """Serial date string "46023" for a date column → "2026-01-01"."""
        schema = {"type": "date", "format": "%Y-%m-%d"}
        assert normalize_merge_key("46023", schema) == "2026-01-01"

    def test_extended_true_serial_date_non_iso_format(self):
        """Serial → regional format."""
        schema = {"type": "date", "format": "%d.%m.%Y"}
        assert normalize_merge_key("46023", schema) == "01.01.2026"

    def test_extended_true_serial_datetime_converted(self):
        """Serial "46023" for datetime column → midnight datetime."""
        schema = {"type": "datetime", "format": "%Y-%m-%d %H:%M:%S"}
        assert normalize_merge_key("46023", schema) == "2026-01-01 00:00:00"

    def test_serial_date_not_triggered_for_str_type(self):
        """For type=str, numeric value must NOT be treated as serial date."""
        schema = {"type": "str"}
        # "46023" is a valid str, so step 1 returns it unchanged
        assert normalize_merge_key("46023", schema) == "46023"

    def test_serial_date_not_triggered_for_int_type(self):
        """For type=int, numeric ID must NOT be treated as serial date."""
        schema = {"type": "int"}
        assert normalize_merge_key("46023", schema) == "46023"

    # ------------------------------------------------------------------
    # Step 4: nothing works → raw + warning
    # ------------------------------------------------------------------

    def test_unparseable_key_returns_raw_with_warning(self, caplog):
        """Key that matches no parse strategy returns raw and emits a warning."""
        schema = {"type": "date", "format": "%Y-%m-%d"}
        with caplog.at_level(logging.WARNING, logger="airflow_provider_google_sheets.utils.merge_key"):
            result = normalize_merge_key("not-a-date-at-all!", schema)
        assert result == "not-a-date-at-all!"
        assert caplog.records, "Expected a warning to be logged"

    def test_extended_false_unparseable_returns_raw_with_warning(self, caplog):
        """With extended=False, unparseable key also returns raw + warning."""
        schema = {"type": "date", "format": "%Y-%m-%d"}
        with caplog.at_level(logging.WARNING, logger="airflow_provider_google_sheets.utils.merge_key"):
            result = normalize_merge_key("not-a-date!", schema, extended=False)
        assert result == "not-a-date!"
        assert caplog.records, "Expected a warning to be logged"


class TestInferDateKeySchema:
    """Tests for infer_date_key_schema (schema-free ISO-date detection)."""

    def test_iso_dates_detected(self):
        result = infer_date_key_schema(["2026-01-01", "2026-01-02"])
        assert result == {"type": "date", "format": "%Y-%m-%d"}

    def test_datetime_like_strings_not_supported(self):
        assert infer_date_key_schema(["2026-01-01 12:30:00"]) is None

    def test_mixed_values_returns_none(self):
        assert infer_date_key_schema(["2026-01-01", "abc"]) is None

    def test_empty_list_returns_none(self):
        assert infer_date_key_schema([]) is None

    def test_list_of_empty_strings_returns_none(self):
        assert infer_date_key_schema(["", "", ""]) is None

    def test_numeric_ids_not_detected_as_dates(self):
        assert infer_date_key_schema(["1001", "1002"]) is None

    def test_syntactically_valid_but_calendar_invalid_date_returns_none(self):
        """date.fromisoformat(), not just the regex, must reject garbage dates."""
        assert infer_date_key_schema(["9999-99-99"]) is None
        assert infer_date_key_schema(["2026-02-30"]) is None


class TestResolveMergeKeySchema:
    """Tests for resolve_merge_key_schema (priority logic: explicit > inferred > None)."""

    def test_explicit_schema_always_wins(self):
        explicit = {"type": "date", "format": "%Y-%m-%d"}
        result = resolve_merge_key_schema(explicit, ["2026-01-01"], infer=True)
        assert result is explicit

    def test_explicit_non_date_schema_wins_and_skips_inference(self, monkeypatch):
        """Even a "non-date" explicit schema wins; inference must never be called."""
        explicit = {"type": "str"}
        called = False

        def _spy(values):
            nonlocal called
            called = True
            return {"type": "date", "format": "%Y-%m-%d"}

        monkeypatch.setattr(
            "airflow_provider_google_sheets.utils.merge_key.infer_date_key_schema", _spy
        )
        result = resolve_merge_key_schema(explicit, ["2026-01-01"], infer=True)
        assert result is explicit
        assert called is False, "inference must not run when an explicit schema is given"

    def test_infer_false_no_explicit_schema_returns_none(self):
        result = resolve_merge_key_schema(None, ["2026-01-01", "2026-01-02"], infer=False)
        assert result is None

    def test_infer_true_no_explicit_schema_iso_dates_returns_inferred(self):
        result = resolve_merge_key_schema(None, ["2026-01-01", "2026-01-02"], infer=True)
        assert result == {"type": "date", "format": "%Y-%m-%d"}

    def test_infer_true_no_explicit_schema_non_iso_returns_none(self):
        result = resolve_merge_key_schema(None, ["abc", "def"], infer=True)
        assert result is None

    @pytest.mark.parametrize("infer", [True, False])
    def test_explicit_schema_wins_regardless_of_infer_flag(self, infer):
        explicit = {"type": "int"}
        result = resolve_merge_key_schema(explicit, ["123", "456"], infer=infer)
        assert result is explicit
