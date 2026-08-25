"""
Glue job entry point for the Bronze load.

Mirrors pipeline.run_job: parse args, load the same config the ingestion job
uses, wire up clients, run the requested tables. Argument resolution follows
the same CLI > config > default precedence, so a Bronze job definition
normally needs only --config-uri.
"""

import argparse
import sys

import boto3

from data_ingest.bronze.athena import AthenaClient
from data_ingest.bronze.loader import load_bronze
from data_ingest.bronze.state import NullProcessedRunStore, ProcessedRunStore
from data_ingest.config import _resolve, _resolve_bool, load_config
from data_ingest.exceptions import ConfigurationError
from data_ingest.logging import configure_logging, get_logger

logger = get_logger(__name__)


def parse_job_args(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-uri", required=True)
    parser.add_argument("--s3-bucket", default=None)
    parser.add_argument("--s3-prefix", default=None)
    parser.add_argument("--tables", default="all")
    parser.add_argument("--bronze-database", default=None)
    parser.add_argument("--athena-output", default=None)
    parser.add_argument("--athena-workgroup", default=None)
    parser.add_argument("--processed-runs-table", default=None)
    parser.add_argument("--fail-fast", default=None)

    # Glue injects its own arguments (--JOB_NAME etc); ignore anything unknown.
    args, _unknown = parser.parse_known_args(argv)
    return args


def run_bronze_job(argv=None):
    configure_logging()
    args = parse_job_args(argv)

    # First line of the job, before any AWS call. When something does not run,
    # the question is always "what did it think it was configured with" -- and
    # answering that from an exit code alone is guesswork.
    logger.info("Bronze job starting: config_uri=%s tables=%s", args.config_uri, args.tables)

    s3_client = boto3.client("s3")
    config = load_config(args.config_uri, s3_client=s3_client)
    tables = config.resolve_tables(args.tables)

    bronze = config.bronze
    if bronze is None and not (args.bronze_database and args.athena_output):
        # Bronze is opt-in per source, so a landing-only config is a valid
        # state, not an error -- a shared Bronze job pointed at several
        # sources should skip the ones that have not opted in rather than
        # failing the whole run.
        #
        # Warned loudly rather than logged at info, because the other reading
        # is a misconfiguration: someone created a Bronze job and expected it
        # to do something. A green job that silently did nothing is the worse
        # outcome of the two.
        logger.warning(
            "SKIPPING BRONZE for %s: the config at %s has no `bronze:` section, so "
            "this source has not opted in. Nothing was merged. If that is "
            "unintended, add a `bronze:` block (database, location, athena_output) "
            "or pass --bronze-database and --athena-output.",
            config.source_key, args.config_uri,
        )
        return []

    s3_bucket = _resolve(args.s3_bucket, config.landing.bucket, None)
    s3_prefix = _resolve(args.s3_prefix, config.landing.prefix, "landing")
    database = _resolve(args.bronze_database, bronze.database if bronze else None, None)
    athena_output = _resolve(args.athena_output, bronze.athena_output if bronze else None, None)
    workgroup = _resolve(
        args.athena_workgroup, bronze.athena_workgroup if bronze else None, "primary"
    )
    processed_runs_table = _resolve(
        args.processed_runs_table, bronze.processed_runs_table if bronze else None, None
    )
    fail_fast = _resolve_bool(
        args.fail_fast, config.defaults.fail_fast, True, arg_name="--fail-fast / defaults.fail_fast"
    )

    missing = [
        name
        for name, value in [
            ("--s3-bucket / landing.location", s3_bucket),
            ("--bronze-database / bronze.database", database),
            ("--athena-output / bronze.athena_output", athena_output),
        ]
        if not value
    ]
    if missing:
        raise ConfigurationError(f"Missing required setting(s): {', '.join(missing)}.")

    if processed_runs_table:
        processed_runs = ProcessedRunStore(
            boto3.resource("dynamodb").Table(processed_runs_table)
        )
    else:
        processed_runs = NullProcessedRunStore()

    athena = AthenaClient(
        client=boto3.client("athena"),
        database=database,
        output_location=athena_output,
        workgroup=workgroup,
    )

    logger.info(
        "Bronze load resolved: source_key=%s tables=%s bronze_db=%s "
        "landing=s3://%s/%s athena_output=%s processed_runs_table=%s",
        config.source_key, [t.name for t in tables], database,
        s3_bucket, s3_prefix, athena_output, processed_runs_table or "(none)",
    )

    try:
        results = load_bronze(
            athena=athena,
            s3_client=s3_client,
            processed_runs=processed_runs,
            bucket=s3_bucket,
            landing_prefix=s3_prefix,
            source_key=config.source_key,
            tables=tables,
            fail_fast=fail_fast,
            bronze_location=bronze.location if bronze else None,
            partition_by=bronze.partition_by if bronze else (),
            table_prefix=bronze.table_prefix if bronze else "source_key",
            # Used to read the CURRENT catalog schema, so a column added in
            # the source is applied to the Athena tables rather than silently
            # dropped. See bronze/schema.py.
            glue_client=boto3.client("glue"),
            database=database,
        )
    except Exception:
        logger.exception("Bronze load failed")
        sys.exit(1)

    return results
