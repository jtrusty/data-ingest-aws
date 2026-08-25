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
from data_ingest.logging import get_logger

logger = get_logger(__name__)

# run_id is a uuid4 and ingest_date is an ISO date, both produced by this
# framework. Validated rather than escaped: anything not matching means the
# landing layout is not what we think it is, and building SQL from it would
# be both wrong and unsafe.
_RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")
_INGEST_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_S3_LOCATION_PATTERN = re.compile(r"^s3://[A-Za-z0-9._\-/=]+$")


def quote_identifier(value):
    """
    Quote an identifier for Athena's Trino engine (DML: SELECT, MERGE INTO).

    Double quotes are ANSI/Trino. Do NOT use these in DDL -- see
    quote_ddl_identifier.
    """
    return '"' + str(value).replace('"', '""') + '"'


def normalize_column(name):
    """
    Lowercase a column name for generated SQL.

    Athena and the Glue catalog store column names lowercased, and Iceberg
    then matches them case-sensitively against whatever a statement says. A
    declaration of `LAST_UPDATE_DTTM` paired with PARTITIONED BY
    (month(LAST_UPDATE_DTTM)) therefore fails with

        Cannot find source column: last_update_dttm

    because one side was normalized and the other was not. Snowflake returns
    identifiers uppercase, so this is every column in every table -- the fix
    is to lowercase consistently on our side rather than fight what Athena
    stores.

    Only generated SQL is affected. The manifest keeps the source's own
    casing, since it records what was actually written to Parquet.
    """
    return str(name).lower()


def quote_ddl_identifier(value):
    """
    Quote an identifier for Athena's Hive DDL parser (CREATE / ALTER TABLE).

    Backticks, not double quotes, and the distinction is not cosmetic: Athena
    parses DDL with a Hive grammar and DML with Trino's. A double-quoted
    identifier in a DDL statement makes Athena route the whole statement to
    the Trino parser, which has no EXTERNAL keyword -- so

        CREATE EXTERNAL TABLE "landing_x" (...)

    fails with `mismatched input 'EXTERNAL'` pointing at column 8, before any
    identifier appears. The error blames the keyword; the cause is the quotes
    further along.
    """
    return "`" + str(value).replace("`", "``") + "`"


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
        f"ALTER TABLE {quote_ddl_identifier(landing_table)} ADD IF NOT EXISTS\n"
        f"  PARTITION (ingest_date='{ingest_date}', run_id='{run_id}')\n"
        f"  LOCATION '{location}'"
    )


def merge_sql(bronze_table, landing_table, ingest_date, run_id, primary_key,
              watermark_column, columns):
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
    if not columns:
        raise ConfigurationError(
            f"Cannot merge {bronze_table}: no column list. Athena requires an explicit "
            f"INSERT (cols) VALUES (...) -- `INSERT *` is Spark syntax, not Trino."
        )

    match_columns = list(primary_key) + [watermark_column]

    # A run that does not carry its own match columns is not mergeable. The
    # ON clause would still parse -- the landing table declares the union of
    # every run's columns, so `source.<col>` resolves -- but it would read
    # NULL, and NULL never equals anything. Every row would look unmatched
    # and be inserted, silently duplicating rows already in Bronze on every
    # single pass. Fail instead.
    landed = {normalize_column(name) for name, _type in columns}
    missing = [c for c in match_columns if normalize_column(c) not in landed]
    if missing:
        raise ConfigurationError(
            f"Cannot merge {bronze_table}: this landing run did not land the "
            f"column(s) {missing}, which Bronze deduplicates on. Matching on a "
            f"column the run lacks compares against NULL, which never matches, so "
            f"every row would be re-inserted on every pass. Check that "
            f"primary_key and the checkpoint column still exist in the source."
        )

    # Lowercased for the same reason as the DDL: the columns exist in the
    # catalog lowercased, and Iceberg matches case-sensitively.
    on_clause = "\n   AND ".join(
        f"target.{quote_identifier(normalize_column(c))} = "
        f"source.{quote_identifier(normalize_column(c))}"
        for c in match_columns
    )

    # Athena requires an explicit column list: `INSERT *` is Spark/Databricks
    # syntax and Trino rejects it with
    #     mismatched input '*'. Expecting: '(', 'VALUES'
    #
    # Per the Athena docs, the target columns in INSERT (...) must NOT be
    # alias-prefixed while the VALUES expressions MUST be -- the two lists
    # look symmetric but are not.
    insert_columns = [normalize_column(name) for name, _type in columns]
    insert_list = ", ".join(quote_identifier(c) for c in insert_columns)
    values_list = ", ".join(f"source.{quote_identifier(c)}" for c in insert_columns)

    return (
        f"MERGE INTO {quote_identifier(bronze_table)} AS target\n"
        f"USING (\n"
        f"  SELECT * FROM {quote_identifier(landing_table)}\n"
        f"  WHERE ingest_date = '{ingest_date}' AND run_id = '{run_id}'\n"
        f") AS source\n"
        f"   ON {on_clause}\n"
        f"WHEN NOT MATCHED THEN INSERT ({insert_list})\n"
        f"  VALUES ({values_list})"
    )


def resolve_partition_spec(partition_by, checkpoint_column):
    """
    Substitute {checkpoint_column} per table.

    One config entry -- e.g. "month({checkpoint_column})" -- therefore works
    across tables that watermark on differently-named columns, without
    repeating a partition spec per table.
    """
    if not partition_by:
        return []

    if not checkpoint_column:
        # The default spec references {checkpoint_column}. A table without one
        # (a future full-load-only source) is landed unpartitioned rather than
        # failing -- an unpartitioned table is merely slower, while refusing to
        # create it stops the pipeline over a performance setting.
        unresolvable = [p for p in partition_by if "{checkpoint_column}" in p]
        if unresolvable:
            logger.warning(
                "Ignoring partition spec %s: this table has no checkpoint column to "
                "substitute. The Bronze table will be created unpartitioned.",
                unresolvable,
            )
        return [p for p in partition_by if "{checkpoint_column}" not in p]

    return [p.replace("{checkpoint_column}", checkpoint_column) for p in partition_by]


def create_bronze_table_sql(bronze_table, columns, location, partitioned_by=None):
    """
    Create the Iceberg target if absent.

    `table_type = 'ICEBERG'` is what makes MERGE INTO available at all --
    Athena supports it only for Iceberg tables, on engine v3.

    Partitioning is whatever bronze.partition_by resolves to, empty by
    default. Unpartitioned is a defensible starting point -- guessing a
    scheme before query patterns are known usually produces the wrong one,
    and Iceberg can evolve partitioning later without rewriting data -- but
    it is not free: MERGE INTO must scan the whole target to evaluate WHEN
    NOT MATCHED, so merge cost grows with Bronze rather than with the run.

    IF NOT EXISTS means this only ever applies to a table that does not yet
    exist. Changing partition_by afterwards has no effect here; that needs an
    explicit Iceberg partition-spec change.
    """
    column_ddl = ",\n  ".join(
        f"{quote_ddl_identifier(normalize_column(name))} {sql_type}"
        for name, sql_type in columns
    )
    partition_ddl = ""
    if partitioned_by:
        # Transforms like month(col) must NOT be quoted as identifiers --
        # quoting would make Athena read the whole expression as a column
        # name. Bare column names are quoted; transforms pass through, having
        # been allowlist-validated at config-parse time.
        # A transform such as month(col) must reference the column exactly as
        # declared above, i.e. lowercased. Bare columns are quoted; transforms
        # are lowercased wholesale, which is safe because the allowlist at
        # config-parse time restricts them to transform(column) forms.
        rendered = [
            normalize_column(c) if "(" in c else quote_ddl_identifier(normalize_column(c))
            for c in partitioned_by
        ]
        partition_ddl = f"PARTITIONED BY ({', '.join(rendered)})\n"

    return (
        f"CREATE TABLE IF NOT EXISTS {quote_ddl_identifier(bronze_table)} (\n"
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
        f"{quote_ddl_identifier(normalize_column(name))} {sql_type}"
        for name, sql_type in columns
    )
    return (
        f"CREATE EXTERNAL TABLE IF NOT EXISTS {quote_ddl_identifier(landing_table)} (\n"
        f"  {column_ddl}\n"
        f")\n"
        f"PARTITIONED BY (ingest_date string, run_id string)\n"
        f"STORED AS PARQUET\n"
        f"LOCATION '{location.rstrip('/')}/'"
    )
