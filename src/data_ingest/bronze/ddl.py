"""
SQL generation for the Landing -> Bronze merge.

Three statements, in the order the loader issues them:

  1. ADD PARTITION  -- point the landing external table at one run's prefix
  2. MERGE INTO     -- insert rows not already present, keyed on the
                       table's own primary_key + watermark column
  3. (table DDL)    -- create the Iceberg target and the landing external
                       table, both idempotent

Identifier quoting note: table and column names come from our own YAML, not
from user input, but they are still quoted so mixed-case Snowflake
identifiers (which is most of them) survive. Values that vary per run --
run_id, ingest_date, the S3 location -- are validated rather than quoted,
because Athena DDL has no bind parameters.
"""

import re

from data_ingest.exceptions import ConfigurationError

# run_id is a uuid4 and ingest_date is an ISO date, both produced by this
# framework. Validated rather than escaped: anything not matching means the
# landing layout is not what we think it is, and building SQL from it would
# be both wrong and unsafe.
_RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")
_INGEST_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_S3_LOCATION_PATTERN = re.compile(r"^s3://[A-Za-z0-9._\-/=]+$")


def quote_identifier(value):
    """Double-quote an identifier for Athena/Trino, escaping embedded quotes."""
    return '"' + str(value).replace('"', '""') + '"'


def _validate_partition_values(ingest_date, run_id, location=None):
    if not _INGEST_DATE_PATTERN.match(str(ingest_date)):
        raise ConfigurationError(f"Unexpected ingest_date in landing path: {ingest_date!r}")
    if not _RUN_ID_PATTERN.match(str(run_id)):
        raise ConfigurationError(f"Unexpected run_id in landing path: {run_id!r}")
    if location is not None and not _S3_LOCATION_PATTERN.match(str(location)):
        raise ConfigurationError(f"Unexpected landing location: {location!r}")


def add_partition_sql(landing_table, ingest_date, run_id, location):
    """
    Register one run's prefix as a partition of the landing external table.

    Deliberately explicit rather than using partition projection. Projection
    would need `injected` type for run_id (uuids cannot be enumerated), and
    Athena engine v3 has a documented defect around injected projections.
    The loader already knows both partition values from the manifest, so
    registering them directly is simpler AND avoids that defect entirely.

    IF NOT EXISTS makes re-processing a run a no-op rather than an error.
    """
    _validate_partition_values(ingest_date, run_id, location)
    return (
        f"ALTER TABLE {quote_identifier(landing_table)} ADD IF NOT EXISTS\n"
        f"  PARTITION (ingest_date='{ingest_date}', run_id='{run_id}')\n"
        f"  LOCATION '{location}'"
    )


def merge_sql(bronze_table, landing_table, ingest_date, run_id, primary_key, watermark_column):
    """
    Merge one landing run into Bronze, deduplicating on primary_key +
    watermark.

    Two properties fall out of `WHEN NOT MATCHED THEN INSERT`:

    * Lookback duplicates collapse. Re-extracting a trailing window
      deliberately re-lands rows already in Bronze; they match on
      primary_key + watermark and are not inserted again.
    * Re-processing a run is a no-op. That makes the whole loader idempotent
      at the SQL level, not just via bookkeeping -- so a crash between
      merging and recording the run as processed is safe.

    History is preserved because the watermark is part of the match key:
    three versions of one primary key at three different watermark values are
    three distinct rows, which is what Bronze is supposed to retain. Silver
    or Redshift is where current-state collapsing belongs.
    """
    _validate_partition_values(ingest_date, run_id)

    if not primary_key:
        raise ConfigurationError(
            f"Cannot merge {bronze_table}: the table has no primary_key, so there is no "
            f"identity to deduplicate on. Add primary_key to its config."
        )
    if not watermark_column:
        raise ConfigurationError(
            f"Cannot merge {bronze_table}: no watermark column. Bronze deduplicates on "
            f"primary_key + watermark; without a watermark, re-landed rows cannot be "
            f"distinguished from new versions."
        )

    match_columns = list(primary_key) + [watermark_column]
    on_clause = "\n   AND ".join(
        f"target.{quote_identifier(c)} = source.{quote_identifier(c)}" for c in match_columns
    )

    return (
        f"MERGE INTO {quote_identifier(bronze_table)} AS target\n"
        f"USING (\n"
        f"  SELECT * FROM {quote_identifier(landing_table)}\n"
        f"  WHERE ingest_date = '{ingest_date}' AND run_id = '{run_id}'\n"
        f") AS source\n"
        f"   ON {on_clause}\n"
        f"WHEN NOT MATCHED THEN INSERT *"
    )


def create_bronze_table_sql(bronze_table, columns, location, partitioned_by=None):
    """
    Create the Iceberg target if absent.

    `table_type = 'ICEBERG'` is what makes MERGE INTO available at all --
    Athena supports it only for Iceberg tables, on engine v3.

    Partitioning defaults to none. Iceberg hidden partitioning can be added
    later without rewriting data, and guessing a partition scheme before
    query patterns are known usually produces the wrong one.
    """
    column_ddl = ",\n  ".join(
        f"{quote_identifier(name)} {sql_type}" for name, sql_type in columns
    )
    partition_ddl = ""
    if partitioned_by:
        spec = ", ".join(f"{quote_identifier(c)}" for c in partitioned_by)
        partition_ddl = f"PARTITIONED BY ({spec})\n"

    return (
        f"CREATE TABLE IF NOT EXISTS {quote_identifier(bronze_table)} (\n"
        f"  {column_ddl}\n"
        f")\n"
        f"{partition_ddl}"
        f"LOCATION '{location.rstrip('/')}/'\n"
        f"TBLPROPERTIES (\n"
        f"  'table_type' = 'ICEBERG',\n"
        f"  'format' = 'parquet',\n"
        f"  'write_compression' = 'snappy'\n"
        f")"
    )


def create_landing_table_sql(landing_table, columns, location):
    """
    External table over one table's landing prefix.

    Partitioned by ingest_date and run_id to match the physical layout; the
    loader registers each run's partition explicitly (see add_partition_sql).
    Plain Hive/Parquet, not Iceberg -- landing is immutable files, not a
    managed table.
    """
    column_ddl = ",\n  ".join(
        f"{quote_identifier(name)} {sql_type}" for name, sql_type in columns
    )
    return (
        f"CREATE EXTERNAL TABLE IF NOT EXISTS {quote_identifier(landing_table)} (\n"
        f"  {column_ddl}\n"
        f")\n"
        f"PARTITIONED BY (ingest_date string, run_id string)\n"
        f"STORED AS PARQUET\n"
        f"LOCATION '{location.rstrip('/')}/'"
    )
