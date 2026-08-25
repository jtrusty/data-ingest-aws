"""
Schema evolution between landing and Bronze.

Source schemas change. Someone adds a column in Snowflake, and every run
after that lands Parquet containing it -- while the Athena tables, created
once with CREATE TABLE IF NOT EXISTS, still describe the old shape. Athena
reads only the columns a table declares, so the new one is invisible: the
data is in S3, Bronze never sees it, and nothing errors. That silent drop is
what this module exists to prevent.

Policy, which is Iceberg's own:

  * ADDED columns are applied automatically. Additive evolution is safe --
    existing rows read NULL for the new column, and no data is rewritten.
  * REMOVED columns are left alone. The table keeps declaring them and older
    Parquet still has them; newer files simply read NULL. Dropping the column
    would discard history Bronze exists to retain.
  * TYPE CHANGES fail loudly. A widened decimal, an int becoming a string --
    these are ambiguous and potentially lossy, and picking a resolution
    silently is how a column quietly becomes wrong.

Renames are deliberately NOT special-cased. At the schema level a rename is
indistinguishable from "drop one column, add another", and guessing wrong
rewrites history in a way nothing downstream would flag.
"""

from data_ingest.exceptions import DataIngestError
from data_ingest.logging import get_logger

logger = get_logger(__name__)


class SchemaChangeError(DataIngestError):
    """A schema change that cannot be applied safely without a human."""


def get_table_columns(glue_client, database, table):
    """
    Current column types for a catalog table, as {name: type}.

    Returns None when the table does not exist -- the caller creates it
    rather than evolving it. Column names are compared case-insensitively
    because Athena lowercases identifiers in the catalog while Snowflake
    hands them back uppercase.
    """
    try:
        response = glue_client.get_table(DatabaseName=database, Name=table)
    except glue_client.exceptions.EntityNotFoundException:
        return None

    storage = response["Table"].get("StorageDescriptor") or {}
    columns = {}
    for column in storage.get("Columns") or []:
        columns[column["Name"].lower()] = column["Type"].lower()
    # Partition columns live separately and are not part of the data schema.
    for column in response["Table"].get("PartitionKeys") or []:
        columns[column["Name"].lower()] = column["Type"].lower()
    return columns


def diff_columns(existing, desired):
    """
    Compare a catalog schema against the schema a landing run actually wrote.

    Returns (added, changed) where `added` is [(name, type), ...] in the
    order they appear in `desired`, and `changed` is
    [(name, existing_type, desired_type), ...].

    Columns present in `existing` but absent from `desired` are ignored on
    purpose -- see the module docstring.
    """
    added = []
    changed = []
    for name, desired_type in desired:
        current = existing.get(name.lower())
        if current is None:
            added.append((name, desired_type))
        elif _normalize(current) != _normalize(desired_type):
            changed.append((name, current, desired_type))
    return added, changed


def _normalize(sql_type):
    """Compare types ignoring case and incidental whitespace."""
    return "".join(str(sql_type).lower().split())


def add_columns_sql(table, columns):
    """
    ALTER TABLE ... ADD COLUMNS, the same syntax for Iceberg and Hive tables.

    Additive only. Athena has no combined add-and-retype statement, which
    suits us -- a retype should not be reachable by accident.
    """
    from data_ingest.bronze.ddl import quote_identifier

    rendered = ", ".join(
        f"{quote_identifier(name)} {sql_type}" for name, sql_type in columns
    )
    return f"ALTER TABLE {quote_identifier(table)} ADD COLUMNS ({rendered})"


def evolve_table(athena, glue_client, database, table, desired_columns, label):
    """
    Bring one catalog table up to date with a landing run's schema.

    Returns True if the table exists (and is now current), False if it does
    not exist and must be created by the caller.
    """
    existing = get_table_columns(glue_client, database, table)
    if existing is None:
        return False

    added, changed = diff_columns(existing, desired_columns)

    if changed:
        details = "; ".join(
            f"{name}: {was} -> {now}" for name, was, now in changed
        )
        raise SchemaChangeError(
            f"{label} `{table}` has incompatible column type change(s): {details}. "
            f"Bronze applies added columns automatically but refuses type changes, "
            f"which are ambiguous and can silently lose precision. Resolve it "
            f"deliberately -- widen the column in Athena, or land the source column "
            f"under a new name -- then re-run."
        )

    if added:
        logger.info(
            "%s `%s`: adding %d new column(s) from the source: %s",
            label, table, len(added), ", ".join(f"{n} {t}" for n, t in added),
        )
        athena.execute(
            add_columns_sql(table, added),
            description=f"add {len(added)} column(s) to {table}",
        )

    return True
