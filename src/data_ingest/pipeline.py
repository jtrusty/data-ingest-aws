"""
Central orchestration for the ingestion framework.

This module is the one place that knows the full transactional sequence
(read checkpoint -> extract -> land -> manifest -> commit checkpoint) and
enforces it the same way for every source type. Source adapters
(data_ingest.sources.*) are only responsible for talking to the thing
they're extracting from -- they never touch S3, DynamoDB, or manifests
directly. This module also never imports a specific adapter (e.g.
SnowflakeSource) directly; it resolves config.source_type to an adapter via
data_ingest.sources.registry. That split is what lets a new source type
(MySQL, SQL Server, REST, CSV-in-S3, ...) plug in -- new module + one
registry line -- without touching this file or re-implementing the
failure/retry guarantees.

run_job() is the function the thin Glue script (jobs/landing_load_snowflake.py)
calls; run_table() is the reusable per-table transaction it's built on top
of, and is also what the test suite exercises directly with a fake Source.
"""

import argparse
import json
import sys
import uuid
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Optional

import boto3

from data_ingest.config import load_config
from data_ingest.exceptions import CheckpointConflictError, ConfigurationError
from data_ingest.landing import LandingWriter
from data_ingest.logging import configure_logging, get_logger
from data_ingest.sources.registry import build_source, known_source_types
from data_ingest.state import DynamoDBStateStore, StateKey

logger = get_logger(__name__)

# Rows pulled per source batch when neither the CLI arg nor the config YAML
# specifies one. This module is the authority -- run_job() resolves
# CLI > config > this value and hands the result to the adapter, so an
# adapter's own default only applies when it is constructed directly.
#
# 10k, not a larger round number, for an empirical reason: a 1.3M-row full
# load was SIGKILLed (exit 137, OOM) at 50k on a Glue Python Shell job and
# completed at 10k. Python Shell is capped at 1 DPU / 16 GB with no way to
# scale up, so batch size is the main lever. Raise it only with a real run
# behind it.
DEFAULT_FETCH_SIZE = 10_000


@dataclass
class TableResult:
    """Outcome of one table's run_table() call, used for the end-of-job summary log."""

    table_name: str
    status: str  # SUCCESS | SKIPPED | FAILED
    run_id: Optional[str] = None
    row_count: Optional[int] = None
    file_count: Optional[int] = None
    error: Optional[str] = None


def state_key_for(source_type, source_system, table_config):
    """
    Build the StateKey identifying a table's checkpoint.

    Keyed on the CONFIG-LOCAL names (source.name + table.name), the same
    identifiers that form the landing path -- deliberately NOT on
    database.schema.table. Identity is one vocabulary across the framework:

        landing/<source.name>/<table.name>/ingest_date=.../run_id=.../
        DynamoDB (source_name=<source.name>, table_name=<table.name>)

    Two consequences worth understanding:

    1. A source-side rename is free. If Snowflake renames ORDER_FACT_V or
       moves it between schemas, you edit `table:`/`schema:` in the YAML and
       nothing orphans -- the landing path and the checkpoint both key off
       `name`, which didn't change. Keying on database.schema.table would
       instead silently orphan the checkpoint and trigger a full reload.
    2. Renaming `name` moves the landing path and the checkpoint together,
       as one deliberate act, instead of drifting apart silently.

    It also generalizes: `name` exists for every source type, while
    database/schema/table is a relational concept a REST or CSV adapter
    would have to fabricate. Those fields are still recorded in the manifest
    and stamped on every row as _source_database/_source_schema/
    _source_table -- they're lineage, not identity.
    """
    return StateKey(
        source_type=source_type,
        source_name=source_system,
        table_name=table_config.name,
    )


# Watermark value_types whose lossless text form sorts lexicographically the
# same way it sorts natively. All are fixed-width and zero-padded by the
# source adapter's codec (e.g. "YYYY-MM-DD HH24:MI:SS.FF9"), so a plain
# string compare is a correct ordering test.
_LEXICOGRAPHIC_VALUE_TYPES = {
    "TIMESTAMP",
    "TIMESTAMP_NTZ",
    "TIMESTAMP_LTZ",
    "TIMESTAMP_TZ",
    "DATE",
    "TIME",
}

_NUMERIC_VALUE_TYPES = {"FIXED", "REAL"}


def _checkpoint_regressed(previous_checkpoint, current_checkpoint):
    """
    True only when we can positively determine the source's high watermark
    moved backwards.

    Deliberately fails OPEN: if the values can't be confidently ordered
    (unknown/legacy value_type, free-text watermark, unparseable number),
    return False and let the run proceed. This is a safety net against
    silently rewinding a checkpoint, not a correctness primitive -- wrongly
    blocking a legitimate run would be worse than not catching a rare
    regression.
    """
    previous_value = previous_checkpoint.value
    current_value = current_checkpoint.value
    if previous_value is None or current_value is None:
        return False

    # Prefer the current checkpoint's type (freshly read from the source);
    # fall back to whatever the stored record claimed.
    value_type = getattr(current_checkpoint, "value_type", None) or getattr(
        previous_checkpoint, "value_type", None
    )

    if value_type in _NUMERIC_VALUE_TYPES:
        try:
            return Decimal(current_value) < Decimal(previous_value)
        except (InvalidOperation, ValueError, TypeError):
            return False

    if value_type in _LEXICOGRAPHIC_VALUE_TYPES:
        return str(current_value) < str(previous_value)

    return False


def run_table(source, state_store, landing_writer, source_type, source_system, table_config):
    """
    Run one table through the full transactional sequence:

    read checkpoint -> determine high checkpoint -> extract -> write
    parquet -> write manifest -> commit checkpoint.

    Nothing before the final state_store.commit() may advance state. If
    anything raises, the previous checkpoint is left untouched and the same
    window will be retried on the next execution. This is the core
    correctness guarantee of the whole framework -- see README.md
    "Core semantics" for why.
    """
    state_key = state_key_for(source_type, source_system, table_config)
    # A fresh, globally-unique id for this run. Used as the S3 run_id=
    # partition (landing immutability) and as lineage on every row.
    run_id = str(uuid.uuid4())

    logger.info("[%s] run_id=%s starting", table_config.name, run_id)

    # Step 1: read the last committed checkpoint (None => no prior state =>
    # this table has never run before => full load).
    state_record = state_store.get(state_key)
    previous_checkpoint = state_record.checkpoint if state_record else None
    # Carried through to state_store.commit() as the optimistic-concurrency
    # guard: if another execution has committed a newer version since we
    # read it, our commit will be rejected rather than silently clobbering.
    expected_version = state_record.version if state_record else None

    # Step 2: capture the high-water checkpoint BEFORE extracting, so that
    # records written to the source *during* this run are simply left for
    # the next run rather than causing a partially-extracted window.
    current_checkpoint = source.get_current_checkpoint()

    if current_checkpoint.value is None:
        # Source object is empty (e.g. MAX(watermark_column) returned NULL).
        # Nothing to do; explicitly do not touch DynamoDB.
        logger.info("[%s] source has no data; skipping.", table_config.name)
        return TableResult(table_name=table_config.name, status="SKIPPED", run_id=run_id)

    if previous_checkpoint is not None and previous_checkpoint.value is not None:
        # A lookback window means we deliberately re-scan a trailing slice of
        # already-seen time on every run, to pick up rows that were written
        # with a timestamp BEHIND the high watermark we previously recorded
        # (late-committing transactions, backdated writes, clock skew). Those
        # rows by definition do NOT raise MAX(watermark), so skipping when the
        # high watermark is unchanged would make lookback unreachable in
        # exactly the situation it exists for. Only take the skip when
        # lookback is disabled.
        lookback = getattr(current_checkpoint, "lookback_minutes", 0) or 0

        if previous_checkpoint.value == current_checkpoint.value and not lookback:
            logger.info("[%s] no new records since last checkpoint; skipping.", table_config.name)
            return TableResult(table_name=table_config.name, status="SKIPPED", run_id=run_id)

        # Guard against the checkpoint moving BACKWARDS. If the source's max
        # watermark regressed (the max row was hard-deleted, the table was
        # restored from a backup or cloned, a source-side rollback), then
        # `previous != current` would otherwise be read as "there's new data"
        # and produce an empty window -- and, worse, commit the lower value,
        # causing the next run to re-extract everything in between as
        # duplicates. Refuse to move the checkpoint down; a human should
        # decide whether to rewind deliberately.
        if _checkpoint_regressed(previous_checkpoint, current_checkpoint):
            logger.warning(
                "[%s] source high watermark REGRESSED (%s -> %s); refusing to move the "
                "checkpoint backwards. Skipping this table. If the source was intentionally "
                "rolled back or restored, rewind the checkpoint deliberately.",
                table_config.name,
                previous_checkpoint.value,
                current_checkpoint.value,
            )
            return TableResult(table_name=table_config.name, status="SKIPPED", run_id=run_id)

    # A checkpoint value of None means "no prior state" -> first-ever run
    # for this table -> full load. Any other prior value -> incremental.
    load_type = "full" if previous_checkpoint is None or previous_checkpoint.value is None else "incremental"

    # Step 3: open a new, immutable landing run
    # (landing/<source_key>/<table>/ingest_date=.../run_id=<run_id>/).
    # Nothing is ever overwritten -- every run_table() call gets its own
    # prefix. The source segment is state_key.source_key ("<name>_<type>"),
    # the SAME value used as the DynamoDB partition key, so the landing
    # layout and the checkpoint can never drift apart.
    landing_run = landing_writer.start(
        source_system=state_key.source_key,
        table_name=table_config.name,
        run_id=run_id,
        source_database=table_config.database,
        source_schema=table_config.schema,
        source_table=table_config.table,
    )

    # Step 4: stream batches from the source straight to Parquet in S3.
    # source.extract() is a generator, so the full result set is never
    # materialized in memory at once -- batch size is controlled by the
    # source adapter's fetch_size.
    for batch in source.extract(previous_checkpoint, current_checkpoint):
        landing_run.write_batch(batch)

    # Generic checkpoint fields recorded in the manifest. `column` only
    # applies to watermark-style checkpoints; getattr() keeps this generic
    # across future checkpoint types (cursor, full_load, ...) that won't
    # have a `column` attribute at all.
    checkpoint_manifest = {
        "type": current_checkpoint.checkpoint_type,
        "column": getattr(current_checkpoint, "column", None),
        "previous": previous_checkpoint.value if previous_checkpoint else None,
        "high": current_checkpoint.value,
    }

    # Step 5: write _manifest.json. This is the commit marker for the
    # landing run -- until this succeeds, the run is considered incomplete
    # and must be ignored by any downstream reader.
    landing_run.write_manifest(
        source_metadata=source.metadata(),
        primary_key=table_config.primary_key,
        checkpoint_manifest=checkpoint_manifest,
        load_type=load_type,
    )

    # Step 6: THE commit. Only after the manifest is durably written do we
    # advance DynamoDB. If the process dies at any point above this line,
    # the checkpoint is untouched and this exact window gets retried next
    # run -- landing's immutability means the retry just produces a new,
    # separate run_id rather than corrupting anything.
    state_store.commit(
        state_key=state_key,
        checkpoint=current_checkpoint,
        run_id=run_id,
        row_count=landing_run.row_count,
        file_count=landing_run.file_count,
        landing_prefix=landing_run.landing_uri,
        expected_version=expected_version,
    )

    logger.info(
        "[%s] run_id=%s SUCCESS rows=%s files=%s",
        table_config.name,
        run_id,
        landing_run.row_count,
        landing_run.file_count,
    )

    return TableResult(
        table_name=table_config.name,
        status="SUCCESS",
        run_id=run_id,
        row_count=landing_run.row_count,
        file_count=landing_run.file_count,
    )


def get_secret(secret_id, secrets_client=None):
    """Fetch and JSON-decode a Secrets Manager secret (Snowflake credentials, etc)."""
    secrets_client = secrets_client or boto3.client("secretsmanager")
    response = secrets_client.get_secret_value(SecretId=secret_id)
    return json.loads(response["SecretString"])


def parse_job_args(argv=None):
    """
    Parse Glue job arguments. See see "Glue job arguments" in README.md for the full
    list. Only --config-uri is required -- --state-table, --s3-bucket,
    --s3-prefix, --fetch-size, and --fail-fast default to None here (NOT
    their eventual runtime defaults) so run_job() can tell "not passed on
    the CLI" apart from "explicitly passed" and fall back to the config
    YAML's `landing`/`state`/`defaults` sections before finally falling
    back to a hardcoded default. A CLI arg always wins when present -- it's
    for one-off overrides (an ad-hoc retry with a different --fetch-size),
    not something every deployment has to wire up.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-uri", required=True)
    parser.add_argument("--state-table", default=None)
    parser.add_argument("--s3-bucket", default=None)
    parser.add_argument("--s3-prefix", default=None)
    parser.add_argument("--tables", default="all")
    parser.add_argument("--fetch-size", type=int, default=None)
    parser.add_argument("--fail-fast", default=None)

    # parse_known_args(), not parse_args(): Glue injects its own arguments
    # (--JOB_NAME, --JOB_RUN_ID, etc) that this parser doesn't define, and a
    # strict parse_args() would hard-fail on those.
    args, _unknown = parser.parse_known_args(argv)
    return args


# Glue passes every job argument as a string, so an operator disabling a
# flag will type one of these rather than a Python bool. Matching only
# "false" would silently treat `--fail-fast 0` / `no` / `off` as True --
# the exact opposite of what was asked for, with no error.
_FALSE_STRINGS = {"false", "0", "no", "off", "n", "f", ""}
_TRUE_STRINGS = {"true", "1", "yes", "on", "y", "t"}


def _resolve_bool(cli_value, config_value, default, arg_name="flag"):
    """CLI-arg (a string, since argparse) > config (already a bool/None) > default."""
    if cli_value is not None:
        normalized = str(cli_value).strip().lower()
        if normalized in _FALSE_STRINGS:
            return False
        if normalized in _TRUE_STRINGS:
            return True
        raise ConfigurationError(
            f"{arg_name} expects a boolean-ish value "
            f"(one of {sorted(_TRUE_STRINGS)} / {sorted(_FALSE_STRINGS - {''})}), "
            f"got {cli_value!r}."
        )
    if config_value is not None:
        return bool(config_value)
    return default


def _resolve(cli_value, config_value, default):
    """Generic CLI-arg > config > default resolution for the rest."""
    if cli_value is not None:
        return cli_value
    if config_value is not None:
        return config_value
    return default


def run_job(argv=None, expected_source_type=None):
    """
    Thin, source-type-agnostic entry point. Loads config, builds a source
    per configured table type, and runs each requested table through the
    generic pipeline (run_table). This is the function each per-source Glue
    script (e.g. jobs/landing_load_snowflake.py) calls -- the Glue script itself
    stays a couple of lines.

    expected_source_type: optional sanity check. A per-source job script
    (named and deployed for one specific source type) can pass its own type
    here so a config-file mistake -- e.g. `source.type: snowfalke`, or this
    script accidentally pointed at a REST source's config -- fails loudly at
    startup instead of silently no-op'ing (config.source_type not matching
    would otherwise just look like "0 tables ran"). Source-agnostic callers
    (tests, a future generic multi-source runner) can leave this unset.
    """
    # Configure logging here, at the application entry point -- the library
    # modules deliberately don't do it on import. See data_ingest.logging.
    configure_logging()

    args = parse_job_args(argv)

    s3_client = boto3.client("s3")
    dynamodb = boto3.resource("dynamodb")

    # Config (table list, checkpoint columns, primary keys, and optionally
    # the deployment-shape settings below) is separate from credentials:
    # it's loaded from S3/local YAML, not Secrets Manager.
    config = load_config(args.config_uri, s3_client=s3_client)
    tables = config.resolve_tables(args.tables)  # "all" | "t1,t2,..."

    # Resolve deployment-shape settings: CLI arg (explicit override) wins
    # if passed, otherwise fall back to the YAML's landing/state/defaults
    # sections, otherwise a hardcoded default. See parse_job_args().
    s3_bucket = _resolve(args.s3_bucket, config.landing.bucket, None)
    state_table = _resolve(args.state_table, config.landing.checkpoint_table, None)
    s3_prefix = _resolve(args.s3_prefix, config.landing.prefix, "landing")
    fetch_size = _resolve(args.fetch_size, config.defaults.fetch_size, DEFAULT_FETCH_SIZE)
    fail_fast = _resolve_bool(args.fail_fast, config.defaults.fail_fast, True, arg_name="--fail-fast / defaults.fail_fast")

    missing = [
        name
        for name, value in [("--s3-bucket / landing.location", s3_bucket), ("--state-table / landing.checkpoint_table", state_table)]
        if value is None
    ]
    if missing:
        raise ConfigurationError(
            f"Missing required setting(s): {', '.join(missing)}. Pass as a "
            f"CLI arg or set it in the config YAML."
        )

    # fetch_size <= 0 is catastrophic-but-silent: cursor.fetchmany(0) returns
    # [] immediately, so the extraction loop exits on its first iteration, a
    # SUCCESS manifest is written with zero rows, and the checkpoint advances
    # past the entire window -- permanently skipping that data while
    # reporting success.
    try:
        fetch_size = int(fetch_size)
    except (TypeError, ValueError):
        raise ConfigurationError(f"fetch_size must be an integer, got {fetch_size!r}.")
    if fetch_size < 1:
        raise ConfigurationError(
            f"fetch_size must be >= 1, got {fetch_size}. A non-positive fetch size "
            f"would extract zero rows and still advance the checkpoint."
        )

    if expected_source_type is not None and config.source_type != expected_source_type:
        raise ConfigurationError(
            f"This job expects a '{expected_source_type}' source, but "
            f"--config-uri {args.config_uri!r} declares source.type="
            f"'{config.source_type}'."
        )

    # Fail before touching Secrets Manager/DynamoDB/S3 for any table if
    # source.type doesn't map to a registered adapter -- same "fail loud
    # and early" reasoning as the expected_source_type check above.
    if config.source_type not in known_source_types():
        raise ConfigurationError(
            f"No source adapter registered for type '{config.source_type}'. "
            f"Known types: {', '.join(known_source_types()) or '(none)'}"
        )

    # One secret, one connection-shape, shared by every table in this
    # source (per-table DB/schema/table selection lives in config, not
    # in Secrets Manager -- see "Secrets vs config" in README.md).
    credentials = get_secret(config.connection.secret_id)
    state_store = DynamoDBStateStore(dynamodb.Table(state_table))
    landing_writer = LandingWriter(s3_client, s3_bucket, s3_prefix)

    results = []
    failed = False

    for table_config in tables:
        source = None
        try:
            # A fresh source (connection) per table, closed in `finally`
            # below regardless of success/failure. build_source() resolves
            # config.source_type via the plug-in registry (see
            # sources/registry.py) -- this module never imports a specific
            # source adapter directly.
            source = build_source(config.source_type, credentials, table_config, fetch_size)
            result = run_table(
                source=source,
                state_store=state_store,
                landing_writer=landing_writer,
                source_type=config.source_type,
                source_system=config.source_name,
                table_config=table_config,
            )
            results.append(result)
        except CheckpointConflictError as exc:
            # Another concurrent execution already advanced this table's
            # checkpoint past what we expected -- surface it distinctly
            # from a generic extraction failure since the fix is usually
            # "don't run two jobs against the same table concurrently",
            # not "retry this window".
            logger.error("[%s] checkpoint conflict: %s", table_config.name, exc)
            results.append(TableResult(table_name=table_config.name, status="FAILED", error=str(exc)))
            failed = True
        except Exception as exc:
            # Any other failure (connection, extraction, S3 write, manifest
            # write): checkpoint was never touched for this table, so a
            # re-run retries the same window. See "Failure/retry behavior" in README.md.
            logger.exception("[%s] extraction failed", table_config.name)
            results.append(TableResult(table_name=table_config.name, status="FAILED", error=str(exc)))
            failed = True
        finally:
            if source is not None:
                source.close()

        if failed and fail_fast:
            # Default policy (fail_fast=true): stop before
            # starting the next table. Tables that already committed
            # successfully in this run keep their new checkpoint; the
            # failed table and anything after it just wait for the next
            # execution.
            logger.error("fail_fast enabled; stopping remaining tables.")
            break

    # One-line-per-table summary, always logged regardless of outcome, so a
    # partial/failed run's state is visible without digging through the
    # full log.
    for result in results:
        logger.info(
            "SUMMARY table=%s status=%s run_id=%s rows=%s files=%s error=%s",
            result.table_name,
            result.status,
            result.run_id,
            result.row_count,
            result.file_count,
            result.error,
        )

    if failed:
        # Non-zero exit so Glue marks the job run as failed -- callers
        # (schedulers, alerting) must not treat a partial failure as success.
        sys.exit(1)

    return results
