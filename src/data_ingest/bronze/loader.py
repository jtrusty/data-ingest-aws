"""
Bronze load orchestration.

Per table, per committed landing run:

    register the run's partition -> MERGE INTO bronze -> record the run

Nothing here does data work; Athena does. That is what keeps the loader
inside a Glue Python Shell job (1 DPU / 16 GB) regardless of table size --
the same constraint that forced batching on the ingestion side is simply not
in play, because rows never pass through this process.

Failure shape mirrors ingestion. Runs are merged oldest-first and recorded
one at a time, so a failure part-way leaves earlier runs recorded, the failed
run un-recorded, and later runs untouched. The next pass resumes from exactly
there. And because the merge is idempotent, re-merging anything already
merged inserts nothing.
"""

from dataclasses import dataclass
from typing import List, Optional

from data_ingest.bronze import ddl
from data_ingest.bronze.discovery import discover_runs
from data_ingest.exceptions import DataIngestError
from data_ingest.logging import get_logger

logger = get_logger(__name__)


@dataclass
class RunResult:
    run_id: str
    status: str  # MERGED | SKIPPED_ALREADY_PROCESSED | SKIPPED_EMPTY | FAILED
    row_count: Optional[int] = None
    error: Optional[str] = None


@dataclass
class TableResult:
    table_name: str
    status: str  # SUCCESS | FAILED
    runs: List[RunResult] = None
    error: Optional[str] = None

    def __post_init__(self):
        if self.runs is None:
            self.runs = []

    @property
    def merged_count(self):
        return sum(1 for r in self.runs if r.status == "MERGED")


def bronze_table_name(source_key, table_name):
    """
    Bronze table naming: "<source_key>_<table_name>", e.g.
    "acme_snowflake_order_fact".

    Flat rather than nested because the Glue Data Catalog has one level of
    database and Redshift Spectrum surfaces the database as a schema. Keeping
    source_key in the name means two sources with a same-named table coexist
    in one Bronze database without collision -- the same reasoning that put
    source_key in the landing path and the checkpoint key.
    """
    return f"{source_key}_{table_name}"


def landing_table_name(source_key, table_name):
    """External table over landing, namespaced to avoid colliding with Bronze."""
    return f"landing_{source_key}_{table_name}"


ATHENA_TYPE_FOR_ARROW = {
    "int64": "bigint",
    "int32": "int",
    "int16": "smallint",
    "int8": "tinyint",
    "double": "double",
    "float": "float",
    "bool": "boolean",
    "string": "string",
    "large_string": "string",
    "binary": "binary",
    "date32[day]": "date",
}


def athena_type_for(arrow_type):
    """
    Map a recorded Arrow type name to an Athena/Iceberg column type.

    Decimals and timestamps carry parameters, so they are matched by prefix
    rather than looked up. Anything unrecognized falls back to string, which
    is lossy but never fails to create the table -- and the manifest records
    the real Arrow type, so a wrong guess is diagnosable.
    """
    name = str(arrow_type)
    if name.startswith("decimal"):
        # "decimal128(38, 3)" -> "decimal(38,3)"
        inner = name[name.index("(") + 1: name.rindex(")")]
        precision, _, scale = inner.partition(",")
        return f"decimal({precision.strip()},{scale.strip() or '0'})"
    if name.startswith("timestamp"):
        return "timestamp"
    if name.startswith("time"):
        return "string"
    return ATHENA_TYPE_FOR_ARROW.get(name, "string")


def columns_from_manifest_schema(manifest_schema):
    """
    [(name, athena_type), ...] from the schema the landing writer recorded.

    Returns None when the manifest predates schema recording, in which case
    the caller cannot create tables and says so rather than guessing.
    """
    if not manifest_schema:
        return None
    return [(field["name"], athena_type_for(field["type"])) for field in manifest_schema]


def _ensure_tables(athena, bronze_table, landing_table, bronze_location,
                   landing_location, manifest_schema, table_config, partition_by):
    """Create the landing external table and the Iceberg target if absent."""
    columns = columns_from_manifest_schema(manifest_schema)
    if not columns:
        raise DataIngestError(
            f"Cannot create Athena tables for {table_config.name}: the landing "
            f"manifest records no schema. Re-run the ingestion job with a build "
            f"that writes `schema` into _manifest.json, or create the tables by hand."
        )

    athena.execute(
        ddl.create_landing_table_sql(landing_table, columns, landing_location),
        description=f"create landing table {landing_table}",
    )

    resolved_partitions = ddl.resolve_partition_spec(
        partition_by, table_config.checkpoint.column
    )
    athena.execute(
        ddl.create_bronze_table_sql(
            bronze_table, columns, f"{bronze_location.rstrip('/')}/{bronze_table}",
            resolved_partitions,
        ),
        description=f"create bronze table {bronze_table}",
    )


def load_table_runs(
    athena,
    s3_client,
    processed_runs,
    bucket,
    landing_prefix,
    source_key,
    table_config,
    bronze_location=None,
    partition_by=(),
):
    """
    Merge every un-processed committed run for one table.

    Returns a TableResult. Raises nothing for a per-run failure -- the run is
    recorded FAILED and the exception propagates, because continuing past a
    failed merge would record later runs as processed while leaving a gap
    behind them.
    """
    table_name = table_config.name
    bronze_table = bronze_table_name(source_key, table_name)
    landing_table = landing_table_name(source_key, table_name)

    runs = discover_runs(s3_client, bucket, landing_prefix, source_key, table_name)
    if not runs:
        # Deliberately loud. This returns SUCCESS, so a wrong landing path or
        # a source_key mismatch would otherwise show up as a green job that
        # silently did nothing -- the hardest kind of failure to notice.
        logger.warning(
            "[%s] NO COMMITTED LANDING RUNS FOUND under s3://%s/%s/%s/%s -- nothing "
            "to merge. If runs exist, check that landing.location and the table name "
            "match where the ingestion job actually wrote.",
            table_name, bucket, landing_prefix.rstrip("/"), source_key, table_name,
        )
        return TableResult(table_name=table_name, status="SUCCESS")

    # Create both tables before touching partitions. Idempotent (CREATE TABLE
    # IF NOT EXISTS), so this costs two cheap DDL statements per table per
    # run and removes a manual bootstrap step that would otherwise have to
    # happen once per table, by hand, in the Athena console.
    #
    # The column list comes from the manifest's recorded schema -- the
    # landing writer captures the Arrow schema it actually wrote, so the
    # Athena tables are defined from what is really in the Parquet rather
    # than from a hand-maintained DDL that can drift.
    _ensure_tables(
        athena=athena,
        bronze_table=bronze_table,
        landing_table=landing_table,
        bronze_location=bronze_location,
        landing_location=f"s3://{bucket}/{landing_prefix.rstrip('/')}/{source_key}/{table_name}",
        manifest_schema=runs[0].manifest.get("schema"),
        table_config=table_config,
        partition_by=partition_by,
    )

    already_processed = processed_runs.processed_run_ids(source_key, table_name)
    pending = [r for r in runs if r.run_id not in already_processed]

    logger.info(
        "[%s] %s committed run(s), %s already merged, %s pending",
        table_name, len(runs), len(runs) - len(pending), len(pending),
    )

    result = TableResult(table_name=table_name, status="SUCCESS")

    for run in runs:
        if run.run_id in already_processed:
            result.runs.append(
                RunResult(run_id=run.run_id, status="SKIPPED_ALREADY_PROCESSED")
            )
            continue

        # A committed run that landed zero rows is valid -- an incremental
        # window with no changes. Record it so it is not reconsidered, but
        # skip the merge: registering a partition over an empty prefix and
        # merging nothing is pure cost.
        if run.is_empty:
            logger.info("[%s] run %s landed 0 rows; recording without merge.", table_name, run.run_id)
            processed_runs.mark_processed(
                source_key, table_name, run.run_id, run.row_count, run.load_type
            )
            result.runs.append(RunResult(run_id=run.run_id, status="SKIPPED_EMPTY", row_count=0))
            continue

        try:
            athena.execute(
                ddl.add_partition_sql(
                    landing_table, run.ingest_date, run.run_id, run.s3_location
                ),
                description=f"add partition {table_name} run_id={run.run_id}",
            )

            athena.execute(
                ddl.merge_sql(
                    bronze_table=bronze_table,
                    landing_table=landing_table,
                    ingest_date=run.ingest_date,
                    run_id=run.run_id,
                    primary_key=table_config.primary_key,
                    watermark_column=table_config.checkpoint.column,
                ),
                description=f"merge {table_name} run_id={run.run_id} ({run.row_count} rows)",
            )
        except Exception as exc:
            # Deliberately NOT recorded as processed, so the next pass retries
            # it. Re-merging a partially-applied MERGE is safe: Athena's MERGE
            # is transactional on Iceberg, so it either applied or it did not.
            logger.exception("[%s] merge failed for run %s", table_name, run.run_id)
            result.runs.append(
                RunResult(run_id=run.run_id, status="FAILED", error=str(exc))
            )
            result.status = "FAILED"
            result.error = str(exc)
            raise

        # Only after the merge succeeds.
        processed_runs.mark_processed(
            source_key, table_name, run.run_id, run.row_count, run.load_type
        )
        result.runs.append(
            RunResult(run_id=run.run_id, status="MERGED", row_count=run.row_count)
        )

    logger.info(
        "[%s] merged %s run(s) into %s", table_name, result.merged_count, bronze_table
    )
    return result


def load_bronze(
    athena,
    s3_client,
    processed_runs,
    bucket,
    landing_prefix,
    source_key,
    tables,
    fail_fast=True,
    bronze_location=None,
    partition_by=(),
):
    """Run every requested table through load_table_runs."""
    results = []
    failed = False

    for table_config in tables:
        try:
            results.append(
                load_table_runs(
                    athena=athena,
                    s3_client=s3_client,
                    processed_runs=processed_runs,
                    bucket=bucket,
                    landing_prefix=landing_prefix,
                    source_key=source_key,
                    table_config=table_config,
                    bronze_location=bronze_location,
                    partition_by=partition_by,
                )
            )
        except Exception as exc:
            results.append(
                TableResult(table_name=table_config.name, status="FAILED", error=str(exc))
            )
            failed = True
            if fail_fast:
                logger.error("fail_fast enabled; stopping remaining tables.")
                break

    for result in results:
        logger.info(
            "BRONZE SUMMARY table=%s status=%s merged_runs=%s error=%s",
            result.table_name, result.status, result.merged_count, result.error,
        )

    if failed:
        raise DataIngestError("One or more tables failed to load into Bronze.")

    return results
