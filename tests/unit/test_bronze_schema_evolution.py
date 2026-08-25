"""
Schema evolution between landing and Bronze.

The bug this prevents is silent. CREATE TABLE IF NOT EXISTS is a no-op once
a table exists, so a column added in Snowflake lands in Parquet and is then
invisible to Athena forever -- data present in S3, absent from Bronze, no
error anywhere.
"""

import pytest

from data_ingest.bronze.schema import (
    SchemaChangeError,
    add_columns_sql,
    diff_columns,
    evolve_table,
    get_table_columns,
)


class FakeGlue:
    class exceptions:
        class EntityNotFoundException(Exception):
            pass

    def __init__(self, tables=None):
        self.tables = tables or {}

    def get_table(self, DatabaseName, Name):
        if Name not in self.tables:
            raise self.exceptions.EntityNotFoundException(Name)
        spec = self.tables[Name]
        return {
            "Table": {
                "StorageDescriptor": {
                    "Columns": [{"Name": n, "Type": t} for n, t in spec.get("columns", {}).items()]
                },
                "PartitionKeys": [
                    {"Name": n, "Type": t} for n, t in spec.get("partitions", {}).items()
                ],
            }
        }


class FakeAthena:
    def __init__(self):
        self.statements = []

    def execute(self, sql, description=None):
        self.statements.append(sql)
        return "qid"


# --------------------------------------------------------------------------
# Reading the catalog
# --------------------------------------------------------------------------


def test_missing_table_reports_none_so_the_caller_creates_it():
    assert get_table_columns(FakeGlue(), "db", "absent") is None


def test_partition_columns_count_as_part_of_the_schema():
    # Partition keys live in a separate catalog field. Missing them would make
    # a partition column look "new" and trigger a bogus ALTER on every run.
    glue = FakeGlue({"t": {"columns": {"ID": "bigint"}, "partitions": {"ingest_date": "string"}}})
    assert get_table_columns(glue, "db", "t") == {"id": "bigint", "ingest_date": "string"}


# --------------------------------------------------------------------------
# Diffing
# --------------------------------------------------------------------------


def test_a_new_source_column_is_detected():
    added, changed = diff_columns(
        {"id": "bigint"},
        [("ID", "bigint"), ("NEW_COL", "string")],
    )
    assert added == [("NEW_COL", "string")]
    assert changed == []


def test_column_names_compare_case_insensitively():
    """
    Athena lowercases identifiers in the catalog while Snowflake returns them
    uppercase. A case-sensitive compare would see every column as new and
    ALTER the table on every single run.
    """
    added, changed = diff_columns({"order_key": "bigint"}, [("ORDER_KEY", "bigint")])
    assert added == []
    assert changed == []


def test_types_compare_ignoring_incidental_whitespace():
    added, changed = diff_columns({"amt": "decimal(38,3)"}, [("AMT", "decimal(38, 3)")])
    assert changed == [], "a space after the comma is not a type change"


def test_a_removed_source_column_is_left_alone():
    """
    Dropping it would discard history Bronze exists to retain, and older
    Parquet still contains the values. Newer files simply read NULL.
    """
    added, changed = diff_columns({"id": "bigint", "gone": "string"}, [("ID", "bigint")])
    assert added == []
    assert changed == []


def test_a_type_change_is_detected():
    added, changed = diff_columns({"amt": "decimal(10,2)"}, [("AMT", "decimal(38,3)")])
    assert changed == [("AMT", "decimal(10,2)", "decimal(38,3)")]


# --------------------------------------------------------------------------
# Applying
# --------------------------------------------------------------------------


def test_a_new_column_is_added_to_the_table():
    glue = FakeGlue({"t": {"columns": {"ID": "bigint"}}})
    athena = FakeAthena()

    existed = evolve_table(
        athena, glue, "db", "t", [("ID", "bigint"), ("NEW_COL", "string")], label="bronze table"
    )

    assert existed is True
    assert len(athena.statements) == 1
    assert athena.statements[0] == "ALTER TABLE `t` ADD COLUMNS (`NEW_COL` string)"


def test_several_new_columns_are_added_in_one_statement():
    glue = FakeGlue({"t": {"columns": {"ID": "bigint"}}})
    athena = FakeAthena()

    evolve_table(
        athena, glue, "db", "t",
        [("ID", "bigint"), ("A", "string"), ("B", "double")],
        label="bronze table",
    )

    assert athena.statements == ["ALTER TABLE `t` ADD COLUMNS (`A` string, `B` double)"]


def test_an_unchanged_schema_issues_no_statements():
    # Evolution runs on every load; a no-op schema must cost nothing.
    glue = FakeGlue({"t": {"columns": {"ID": "bigint"}}})
    athena = FakeAthena()

    evolve_table(athena, glue, "db", "t", [("ID", "bigint")], label="bronze table")

    assert athena.statements == []


def test_a_missing_table_is_reported_rather_than_altered():
    athena = FakeAthena()
    existed = evolve_table(
        athena, FakeGlue(), "db", "absent", [("ID", "bigint")], label="bronze table"
    )
    assert existed is False
    assert athena.statements == [], "a table that does not exist must be CREATEd, not ALTERed"


def test_a_type_change_fails_loudly_and_changes_nothing():
    """
    Widening a decimal or turning an int into a string is ambiguous and can
    lose precision. Picking a resolution silently is how a column quietly
    becomes wrong, so this needs a human.
    """
    glue = FakeGlue({"t": {"columns": {"AMT": "decimal(10,2)"}}})
    athena = FakeAthena()

    with pytest.raises(SchemaChangeError) as exc_info:
        evolve_table(athena, glue, "db", "t", [("AMT", "decimal(38,3)")], label="bronze table")

    message = str(exc_info.value)
    assert "AMT" in message
    assert "decimal(10,2)" in message and "decimal(38,3)" in message
    assert athena.statements == [], "nothing may be applied when the change is rejected"


def test_a_type_change_is_reported_even_alongside_valid_additions():
    # The addition must not be applied and then the error raised -- that would
    # leave the table half-evolved.
    glue = FakeGlue({"t": {"columns": {"AMT": "decimal(10,2)"}}})
    athena = FakeAthena()

    with pytest.raises(SchemaChangeError):
        evolve_table(
            athena, glue, "db", "t",
            [("AMT", "decimal(38,3)"), ("NEW_COL", "string")],
            label="bronze table",
        )

    assert athena.statements == []


def test_add_columns_sql_uses_hive_backtick_quoting():
    # ALTER TABLE is DDL, so it must use Athena's Hive grammar. Double quotes
    # would route the statement to the Trino parser -- see test_ddl_and_dml_
    # use_different_quoting in test_bronze_ddl.py.
    sql = add_columns_sql("my_table", [("MixedCase", "string")])
    assert sql == "ALTER TABLE `my_table` ADD COLUMNS (`MixedCase` string)"
