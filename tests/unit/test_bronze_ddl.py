import pytest

from data_ingest.bronze import ddl
from data_ingest.exceptions import ConfigurationError


def test_merge_deduplicates_on_primary_key_plus_watermark():
    """
    The dedup identity from the spec. Matching on the primary key alone would
    collapse history -- three versions of one order would become one row --
    and matching on the watermark alone would collide unrelated rows that
    happened to change at the same instant.
    """
    sql = ddl.merge_sql(
        bronze_table="acme_snowflake_order_fact",
        landing_table="landing_acme_snowflake_order_fact",
        ingest_date="2026-08-24",
        run_id="abc-123",
        primary_key=["ORDER_KEY"],
        watermark_column="LAST_UPDATE_DTTM",
    )

    assert 'target."ORDER_KEY" = source."ORDER_KEY"' in sql
    assert 'target."LAST_UPDATE_DTTM" = source."LAST_UPDATE_DTTM"' in sql
    assert "WHEN NOT MATCHED THEN INSERT" in sql
    # No update/delete clause: Bronze retains history rather than mutating it.
    assert "WHEN MATCHED" not in sql


def test_merge_supports_composite_primary_keys():
    sql = ddl.merge_sql(
        bronze_table="t", landing_table="l",
        ingest_date="2026-08-24", run_id="r",
        primary_key=["ORDER_KEY", "LINE_NO"],
        watermark_column="UPDATED_AT",
    )
    for column in ("ORDER_KEY", "LINE_NO", "UPDATED_AT"):
        assert f'target."{column}" = source."{column}"' in sql


def test_merge_scopes_the_source_to_exactly_one_run():
    # Without both partition predicates the merge would rescan the whole
    # table's landing history on every run -- correct but ruinously costly.
    sql = ddl.merge_sql(
        bronze_table="t", landing_table="l",
        ingest_date="2026-08-24", run_id="abc-123",
        primary_key=["ID"], watermark_column="UPDATED_AT",
    )
    assert "ingest_date = '2026-08-24'" in sql
    assert "run_id = 'abc-123'" in sql


def test_merge_without_primary_key_is_rejected():
    with pytest.raises(ConfigurationError, match="no primary_key"):
        ddl.merge_sql(
            bronze_table="t", landing_table="l",
            ingest_date="2026-08-24", run_id="r",
            primary_key=[], watermark_column="UPDATED_AT",
        )


def test_merge_without_watermark_is_rejected():
    with pytest.raises(ConfigurationError, match="no watermark column"):
        ddl.merge_sql(
            bronze_table="t", landing_table="l",
            ingest_date="2026-08-24", run_id="r",
            primary_key=["ID"], watermark_column=None,
        )


@pytest.mark.parametrize(
    "bad_run_id",
    ["abc'; DROP TABLE x --", "abc def", "../../etc", "abc'"],
)
def test_malformed_run_id_is_rejected(bad_run_id):
    """
    Athena DDL has no bind parameters, so partition values are interpolated.
    They are framework-generated (uuid4), so anything not matching that shape
    means the landing layout is not what we think -- reject rather than build
    SQL from it.
    """
    with pytest.raises(ConfigurationError, match="Unexpected run_id"):
        ddl.add_partition_sql("t", "2026-08-24", bad_run_id, "s3://b/p/")


@pytest.mark.parametrize("bad_date", ["2026-8-24", "not-a-date", "2026-08-24' OR '1'='1"])
def test_malformed_ingest_date_is_rejected(bad_date):
    with pytest.raises(ConfigurationError, match="Unexpected ingest_date"):
        ddl.add_partition_sql("t", bad_date, "abc", "s3://b/p/")


def test_malformed_location_is_rejected():
    with pytest.raises(ConfigurationError, match="Unexpected landing location"):
        ddl.add_partition_sql("t", "2026-08-24", "abc", "s3://bucket/p'; DROP --")


def test_add_partition_is_idempotent():
    sql = ddl.add_partition_sql(
        "landing_t", "2026-08-24", "abc-123", "s3://bucket/landing/x/run_id=abc-123/"
    )
    assert "ADD IF NOT EXISTS" in sql
    assert "ingest_date='2026-08-24'" in sql
    assert "run_id='abc-123'" in sql


def test_bronze_table_is_created_as_iceberg():
    # MERGE INTO is available only for Iceberg tables on Athena engine v3;
    # a plain Hive table would fail at merge time, not at create time.
    sql = ddl.create_bronze_table_sql(
        "acme_order_fact",
        [("ORDER_KEY", "bigint"), ("STATUS", "string")],
        "s3://bucket/bronze/acme_order_fact",
    )
    assert "'table_type' = 'ICEBERG'" in sql
    assert "CREATE TABLE IF NOT EXISTS" in sql
    assert '"ORDER_KEY" bigint' in sql


def test_landing_external_table_is_partitioned_to_match_the_physical_layout():
    sql = ddl.create_landing_table_sql(
        "landing_acme_order_fact",
        [("ORDER_KEY", "bigint")],
        "s3://bucket/landing/acme_snowflake/order_fact",
    )
    assert "PARTITIONED BY (ingest_date string, run_id string)" in sql
    assert "STORED AS PARQUET" in sql
    # Not Iceberg: landing is immutable files, not a managed table.
    assert "ICEBERG" not in sql


def test_identifiers_are_quoted_so_mixed_case_survives():
    sql = ddl.merge_sql(
        bronze_table="t", landing_table="l",
        ingest_date="2026-08-24", run_id="r",
        primary_key=["OrderKey"], watermark_column="LastUpdate",
    )
    assert '"OrderKey"' in sql
    assert '"LastUpdate"' in sql
