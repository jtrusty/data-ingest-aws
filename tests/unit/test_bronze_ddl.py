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

    assert 'target."order_key" = source."order_key"' in sql
    assert 'target."last_update_dttm" = source."last_update_dttm"' in sql
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
        assert f'target."{column.lower()}" = source."{column.lower()}"' in sql


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
    assert "`order_key` bigint" in sql


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


def test_identifiers_are_quoted_and_normalized():
    sql = ddl.merge_sql(
        bronze_table="t", landing_table="l",
        ingest_date="2026-08-24", run_id="r",
        primary_key=["OrderKey"], watermark_column="LastUpdate",
    )
    assert '"orderkey"' in sql
    assert '"lastupdate"' in sql


# --------------------------------------------------------------------------
# Partitioning
# --------------------------------------------------------------------------


def test_partition_spec_substitutes_the_tables_own_checkpoint_column():
    # One config entry works across tables that watermark on different column
    # names, without repeating a spec per table.
    assert ddl.resolve_partition_spec(
        ("month({checkpoint_column})",), "LAST_UPDATE_DTTM"
    ) == ["month(LAST_UPDATE_DTTM)"]


def test_transform_partitions_are_not_quoted_as_identifiers():
    """
    month(col) must reach Athena as an expression. Quoting it would make
    Athena read the whole string as one column name and fail -- while a bare
    column still needs quoting to survive mixed case.
    """
    sql = ddl.create_bronze_table_sql(
        "t", [("A", "bigint")], "s3://b/t",
        ddl.resolve_partition_spec(("month({checkpoint_column})", "STORE_ID"), "UPDATED_AT"),
    )
    assert "PARTITIONED BY (month(updated_at), `store_id`)" in sql


def test_no_partition_spec_creates_an_unpartitioned_table():
    sql = ddl.create_bronze_table_sql("t", [("A", "bigint")], "s3://b/t", [])
    assert "PARTITIONED BY" not in sql


def test_table_without_a_checkpoint_column_is_created_unpartitioned():
    # A performance setting must not be able to block table creation.
    assert ddl.resolve_partition_spec(("month({checkpoint_column})",), None) == []


def test_literal_partition_columns_survive_a_missing_checkpoint_column():
    assert ddl.resolve_partition_spec(
        ("month({checkpoint_column})", "REGION"), None
    ) == ["REGION"]


def test_ddl_and_dml_use_different_quoting():
    """
    Athena parses DDL with a Hive grammar and DML with Trino's, and they
    disagree about identifier quoting. A double-quoted identifier inside a
    DDL statement routes the whole statement to the Trino parser, which has
    no EXTERNAL keyword -- producing

        line 1:8: mismatched input 'EXTERNAL'

    which blames the keyword while the actual cause is the quotes further
    along. That cost a real debugging cycle, so it is pinned here.
    """
    cols = [("ORDER_KEY", "bigint")]

    ddl_statements = [
        ddl.create_landing_table_sql("t", cols, "s3://b/t"),
        ddl.create_bronze_table_sql("t", cols, "s3://b/t"),
        ddl.add_partition_sql("t", "2026-08-25", "abc", "s3://b/p/"),
    ]
    for sql in ddl_statements:
        assert '"' not in sql.split("LOCATION")[0], (
            f"DDL must use backticks, not double quotes:\n{sql}"
        )
        assert "`" in sql

    # MERGE INTO is genuinely Trino DML, where double quotes are correct.
    merge = ddl.merge_sql("t", "l", "2026-08-25", "abc", ["ID"], "UPDATED_AT")
    assert '"' in merge
    assert "`" not in merge


def test_external_keyword_is_present_for_the_landing_table():
    # Required for non-Iceberg tables; Athena errors without it.
    sql = ddl.create_landing_table_sql("t", [("A", "bigint")], "s3://b/t")
    assert sql.startswith("CREATE EXTERNAL TABLE IF NOT EXISTS")


def test_iceberg_table_omits_the_external_keyword():
    # The inverse: EXTERNAL is NOT supported for Iceberg tables.
    sql = ddl.create_bronze_table_sql("t", [("A", "bigint")], "s3://b/t")
    assert sql.startswith("CREATE TABLE IF NOT EXISTS")
    assert "EXTERNAL" not in sql


def test_partition_transform_matches_the_declared_column_case():
    """
    Regression for:

        Failed to create bronze table ... Cannot find source column:
        last_update_dttm

    Athena stores column names lowercased and Iceberg then matches them
    case-sensitively, so declaring `LAST_UPDATE_DTTM` while partitioning by
    month(LAST_UPDATE_DTTM) leaves the two sides disagreeing. Snowflake
    returns every identifier uppercase, so this affected every table.
    """
    sql = ddl.create_bronze_table_sql(
        "t",
        [("ORDER_KEY", "bigint"), ("LAST_UPDATE_DTTM", "timestamp")],
        "s3://b/t",
        ddl.resolve_partition_spec(("month({checkpoint_column})",), "LAST_UPDATE_DTTM"),
    )
    assert "`last_update_dttm` timestamp" in sql
    assert "PARTITIONED BY (month(last_update_dttm))" in sql
    assert "LAST_UPDATE_DTTM" not in sql, "no uppercase may survive into the DDL"


def test_merge_references_match_the_declared_column_case():
    # The merge must reference the same lowercased names the DDL declared,
    # or Iceberg cannot resolve the ON clause.
    sql = ddl.merge_sql("t", "l", "2026-08-25", "abc", ["ORDER_KEY"], "LAST_UPDATE_DTTM")
    assert "ORDER_KEY" not in sql
    assert 'target."order_key"' in sql


def test_landing_table_columns_are_normalized_too():
    # Both tables must agree, since MERGE ... INSERT * maps between them.
    sql = ddl.create_landing_table_sql("l", [("ORDER_KEY", "bigint")], "s3://b/l")
    assert "`order_key` bigint" in sql
    assert "ORDER_KEY" not in sql
