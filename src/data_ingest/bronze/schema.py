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


def check_iceberg_metadata(glue_client, s3_client, database, table):
    """
    Verify that an Iceberg table's metadata file still exists in S3.

    An Iceberg table is two things: a Glue catalog entry, and a metadata file
    in S3 that the entry points at via the `metadata_location` parameter.
    Deleting the S3 prefix removes the second but not the first, leaving a
    catalog entry that looks entirely healthy -- get_table succeeds, the
    columns are all there, so evolve_table reports the table as existing and
    the loader skips CREATE. The failure surfaces much later, at merge time:

        ICEBERG_MISSING_METADATA: Metadata not found in metadata location
        for table <db>.<table>

    which names Athena and the table but not the cause, and not the fix.

    This is a normal consequence of clearing S3 to re-land data, so it is
    worth one HEAD request per table per run to say what actually happened.
    Deliberately NOT self-healing: dropping the catalog entry automatically
    would discard a real table on any transient S3 error.
    """
    try:
        response = glue_client.get_table(DatabaseName=database, Name=table)
    except glue_client.exceptions.EntityNotFoundException:
        return  # Not created yet; the caller will create it.

    parameters = response["Table"].get("Parameters") or {}
    location = parameters.get("metadata_location")
    if not location or not location.startswith("s3://"):
        return  # Not an Iceberg table, or a catalog that does not record it.

    bucket, _, key = location[len("s3://"):].partition("/")
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return
    except Exception as exc:  # noqa: BLE001 - any failure to confirm is fatal
        if "404" not in str(exc) and "Not Found" not in str(exc):
            raise

    raise SchemaChangeError(
        f"Iceberg table `{database}.{table}` is registered in the Glue Data "
        f"Catalog but its metadata file is gone: {location} does not exist. "
        f"This is what clearing the S3 prefix without dropping the table looks "
        f"like -- the catalog entry survives, so nothing recreates the table, "
        f"and the merge fails with ICEBERG_MISSING_METADATA. Drop the stale "
        f"entry and re-run, which will recreate it: "
        f"`aws glue delete-table --database-name {database} --name {table}`."
    )


def diff_columns(existing, desired, catalog_overrides=None):
    """
    Compare a catalog schema against the schema a landing run actually wrote.

    Returns (added, changed) where `added` is [(name, type), ...] in the
    order they appear in `desired`, and `changed` is
    [(name, existing_type, desired_type), ...].

    Columns present in `existing` but absent from `desired` are ignored on
    purpose -- see the module docstring.

    `catalog_overrides` ({column: type}) names columns whose catalog type is
    deliberately different from the landed type, for a consumer that reads
    the catalog rather than Iceberg (see BronzeConfig.catalog_column_types).
    A column already carrying its override is a match, not a change; one
    carrying anything else is judged exactly as before.
    """
    overrides = {k.lower(): _normalize(v) for k, v in (catalog_overrides or {}).items()}
    added = []
    changed = []
    for name, desired_type in desired:
        current = existing.get(name.lower())
        if current is None:
            added.append((name, desired_type))
        elif _normalize(current) == _normalize(desired_type):
            continue
        elif overrides.get(name.lower()) == _normalize(current):
            continue
        else:
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
    from data_ingest.bronze.ddl import normalize_column, quote_ddl_identifier

    rendered = ", ".join(
        f"{quote_ddl_identifier(normalize_column(name))} {sql_type}"
        for name, sql_type in columns
    )
    return f"ALTER TABLE {quote_ddl_identifier(table)} ADD COLUMNS ({rendered})"


def evolve_table(athena, glue_client, database, table, desired_columns, label,
                 catalog_overrides=None):
    """
    Bring one catalog table up to date with a landing run's schema.

    Returns True if the table exists (and is now current), False if it does
    not exist and must be created by the caller.
    """
    existing = get_table_columns(glue_client, database, table)
    if existing is None:
        return False

    added, changed = diff_columns(existing, desired_columns, catalog_overrides)

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


# The fields Glue's UpdateTable accepts in TableInput, per its own error
# message when handed anything else. GetTable returns a superset.
_TABLE_INPUT_FIELDS = (
    "Name", "Description", "Owner", "LastAccessTime", "LastAnalyzedTime", "Retention",
    "StorageDescriptor", "PartitionKeys", "ViewOriginalText", "ViewExpandedText",
    "TableType", "Parameters", "TargetTable", "ViewDefinition",
)


def apply_catalog_overrides(glue_client, database, table, overrides, label):
    """
    Set the declared catalog column types on a table, if they are not
    already set. Idempotent; returns the columns it changed.

    The write goes through Glue's optimistic lock. The same catalog entry
    carries Iceberg's `metadata_location` pointer, which Athena advances on
    every commit; a blind UpdateTable built from a stale read would rewind
    that pointer and corrupt the table. VersionId makes a stale write fail
    instead. Bronze is the only writer and runs one at a time, so in
    practice the retry never fires -- but the guard is what makes that a
    performance fact rather than a correctness assumption.
    """
    if not overrides:
        return []
    wanted = {k.lower(): _normalize(v) for k, v in overrides.items()}

    response = glue_client.get_table(DatabaseName=database, Name=table)
    entry = response["Table"]
    storage = dict(entry.get("StorageDescriptor") or {})
    columns = [dict(c) for c in storage.get("Columns") or []]

    changed = []
    for column in columns:
        want = wanted.get(column["Name"].lower())
        if want is not None and _normalize(column["Type"]) != want:
            changed.append((column["Name"], column["Type"], want))
            column["Type"] = want
    missing = sorted(set(wanted) - {c["Name"].lower() for c in columns})
    if missing:
        logger.warning(
            "%s `%s`: catalog_column_types names column(s) the table does not have: %s",
            label, table, ", ".join(missing),
        )
    if not changed:
        return []

    # TableInput accepts a fixed set of fields, and GetTable returns more --
    # CreateTime, VersionId, IsMaterializedView, and whatever Glue adds next.
    # An allowlist of what UpdateTable takes is the only stable shape; a
    # denylist of what it rejects broke the first time it met a field that
    # was not on it. Parameters, with metadata_location, goes back as read.
    table_input = {k: entry[k] for k in _TABLE_INPUT_FIELDS if k in entry}
    storage["Columns"] = columns
    table_input["StorageDescriptor"] = storage
    kwargs = {"DatabaseName": database, "TableInput": table_input}
    if entry.get("VersionId"):
        kwargs["VersionId"] = entry["VersionId"]
    glue_client.update_table(**kwargs)
    logger.info(
        "%s `%s`: catalog column type(s) set for consumers: %s",
        label, table, ", ".join(f"{n} {was} -> {now}" for n, was, now in changed),
    )
    return changed
