from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from data_ingest.checkpoints.watermark import WatermarkCheckpoint
from data_ingest.exceptions import ConfigurationError
from data_ingest.sources.snowflake import SnowflakeSource, quote_identifier

# Snowflake type codes (snowflake.connector.constants.FIELD_ID_TO_NAME)
TYPE_FIXED = 0
TYPE_TEXT = 2
TYPE_DATE = 3
TYPE_TIMESTAMP_NTZ = 8
TYPE_VARIANT = 5


def make_source(watermark_type=TYPE_TIMESTAMP_NTZ, **overrides):
    """
    Build a SnowflakeSource against a mocked connection.

    `watermark_type` is what the zero-row type probe reports for the
    watermark column, which drives codec selection.
    """
    with patch("data_ingest.sources.snowflake.snowflake.connector.connect") as connect:
        connection = MagicMock()
        connect.return_value = connection

        credentials = {
            "account": "acct",
            "username": "user",
            "password": "pw",
            "warehouse": "wh",
            "role": "role",
        }

        defaults = dict(
            credentials=credentials,
            database="ACME",
            schema="PUBLIC",
            table="ORDERS",
            watermark_column="UPDATED_AT",
        )
        defaults.update(overrides)

        source = SnowflakeSource(**defaults)
        # The type probe reads cursor.description[0][1].
        connection.cursor.return_value.description = [("UPDATED_AT", watermark_type)]
        return source, connection


def test_quote_identifier_escapes_quotes():
    assert quote_identifier('My"Table') == '"My""Table"'


def test_object_name_quotes_all_parts():
    source, _ = make_source()
    assert source.object_name == '"ACME"."PUBLIC"."ORDERS"'


def test_connection_pins_utc_session_timezone():
    # An unpinned session timezone means the same stored watermark string can
    # denote a different instant after an account default changes.
    with patch("data_ingest.sources.snowflake.snowflake.connector.connect") as connect:
        SnowflakeSource(
            credentials={"account": "a", "username": "u", "password": "p", "warehouse": "w"},
            database="D",
            schema="S",
            table="T",
            watermark_column="UPDATED_AT",
        )
        session_parameters = connect.call_args.kwargs["session_parameters"]
        assert session_parameters["TIMEZONE"] == "UTC"
        assert session_parameters["TIMESTAMP_TYPE_MAPPING"] == "TIMESTAMP_NTZ"


# --------------------------------------------------------------------------
# Watermark fidelity -- the regression tests for the nanosecond-truncation bug
# --------------------------------------------------------------------------


def test_timestamp_watermark_is_read_as_text_at_nanosecond_precision():
    """
    The high watermark must be captured via TO_VARCHAR(..., 'FF9'), NOT by
    str()-ing the connector's Python datetime (which holds only microseconds).
    A truncated ceiling excludes the very row it came from, and the next run
    truncates identically -- so the row is never ingested, in any run.
    """
    source, connection = make_source(watermark_type=TYPE_TIMESTAMP_NTZ)
    cursor = connection.cursor.return_value
    cursor.fetchone.return_value = ("2026-08-24 12:00:00.123456789",)

    checkpoint = source.get_current_checkpoint()

    executed = " ".join(str(c) for c in cursor.execute.call_args_list)
    assert "TO_VARCHAR" in executed
    assert "FF9" in executed
    # Full nanosecond precision preserved -- no truncation to .123456
    assert checkpoint.value == "2026-08-24 12:00:00.123456789"
    assert checkpoint.value_type == "TIMESTAMP_NTZ"


def test_timestamp_watermark_is_bound_with_an_explicit_cast():
    # Relying on Snowflake's implicit VARCHAR->TIMESTAMP coercion makes
    # correctness depend on the session's TIMESTAMP_INPUT_FORMAT.
    source, _ = make_source(watermark_type=TYPE_TIMESTAMP_NTZ)
    high = WatermarkCheckpoint(column="UPDATED_AT", value="2026-08-24 00:00:00.000000000")

    query, _params, _load_type = source._build_query(None, high)

    assert "TO_TIMESTAMP_NTZ(%s" in query


def test_numeric_watermark_uses_number_codec():
    source, connection = make_source(watermark_type=TYPE_FIXED)
    cursor = connection.cursor.return_value
    cursor.fetchone.return_value = ("100",)

    checkpoint = source.get_current_checkpoint()
    assert checkpoint.value_type == "FIXED"

    query, _params, _lt = source._build_query(None, checkpoint)
    assert "TO_NUMBER(%s)" in query


def test_unsupported_watermark_type_raises_configuration_error():
    source, _ = make_source(watermark_type=TYPE_VARIANT)
    with pytest.raises(ConfigurationError, match="unsupported Snowflake type"):
        source.get_current_checkpoint()


def test_lookback_on_non_timestamp_watermark_is_rejected():
    # DATEADD(minute, ...) against a NUMBER is a runtime Snowflake error;
    # catch it at startup with an actionable message instead.
    source, _ = make_source(watermark_type=TYPE_FIXED, lookback_minutes=60)
    with pytest.raises(ConfigurationError, match="minute-based lookback"):
        source.get_current_checkpoint()


def test_lookback_on_date_watermark_is_rejected():
    source, _ = make_source(watermark_type=TYPE_DATE, lookback_minutes=30)
    with pytest.raises(ConfigurationError, match="minute-based lookback"):
        source.get_current_checkpoint()


# --------------------------------------------------------------------------
# Query shapes
# --------------------------------------------------------------------------


def test_get_current_checkpoint_handles_empty_table():
    source, connection = make_source()
    connection.cursor.return_value.fetchone.return_value = (None,)

    checkpoint = source.get_current_checkpoint()
    assert checkpoint.value is None


def test_full_load_query_has_only_an_upper_bound():
    source, _ = make_source()
    high = WatermarkCheckpoint(column="UPDATED_AT", value="2026-08-24")

    query, params, load_type = source._build_query(None, high)

    assert load_type == "full"
    assert params == ("2026-08-24",)
    where_clause = query.split("WHERE")[1]
    assert "<=" in where_clause
    assert ">" not in where_clause


def test_full_load_query_does_not_sort():
    # ORDER BY over a whole table forces Snowflake to materialize the entire
    # result before the first row, and buys no correctness.
    source, _ = make_source()
    high = WatermarkCheckpoint(column="UPDATED_AT", value="2026-08-24")

    query, _params, _load_type = source._build_query(None, high)
    assert "ORDER BY" not in query


def test_incremental_query_used_when_previous_checkpoint_exists():
    source, _ = make_source()
    previous = WatermarkCheckpoint(column="UPDATED_AT", value="2026-08-01")
    high = WatermarkCheckpoint(column="UPDATED_AT", value="2026-08-24")

    query, params, load_type = source._build_query(previous, high)

    assert load_type == "incremental"
    assert params == ("2026-08-01", "2026-08-24")
    assert "ORDER BY" in query


def test_lookback_widens_lower_bound_in_query():
    source, _ = make_source(lookback_minutes=60)
    previous = WatermarkCheckpoint(column="UPDATED_AT", value="2026-08-01 12:00:00.000000000")
    high = WatermarkCheckpoint(column="UPDATED_AT", value="2026-08-24 00:00:00.000000000")

    query, params, load_type = source._build_query(previous, high)

    assert load_type == "incremental"
    assert "DATEADD(minute, -%s," in query
    assert params == (60, "2026-08-01 12:00:00.000000000", "2026-08-24 00:00:00.000000000")


# --------------------------------------------------------------------------
# Batched extraction
# --------------------------------------------------------------------------


def test_extract_batches_via_fetchmany():
    source, connection = make_source(fetch_size=2)
    cursor = connection.cursor.return_value
    cursor.description = [("ID", TYPE_FIXED), ("UPDATED_AT", TYPE_TIMESTAMP_NTZ)]
    cursor.fetchmany.side_effect = [
        [(1, "a"), (2, "b")],
        [(3, "c")],
        [],
    ]

    high = WatermarkCheckpoint(column="UPDATED_AT", value="2026-08-24")
    batches = list(source.extract(None, high))

    assert len(batches) == 2
    assert len(batches[0]) == 2
    assert len(batches[1]) == 1
    assert list(batches[0].columns) == ["ID", "UPDATED_AT"]
    # fetchmany, never fetch_pandas_batches -- the Arrow fetch path is
    # incompatible with the pinned connector under Glue Python Shell.
    cursor.fetch_pandas_batches.assert_not_called()


def test_extract_yields_nothing_when_high_watermark_is_none():
    source, connection = make_source()
    empty = WatermarkCheckpoint(column="UPDATED_AT", value=None)

    assert list(source.extract(None, empty)) == []
    connection.cursor.return_value.execute.assert_not_called()
