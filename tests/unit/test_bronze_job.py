"""
Bronze Glue entry point.

The ingestion side has test_run_job.py covering argument resolution and the
missing-setting checks; Bronze had no equivalent, so its entry point sat at
23% coverage while being the thing Glue actually invokes.
"""

from contextlib import contextmanager
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws

from data_ingest.bronze.job import run_bronze_job
from data_ingest.bronze.state import NullProcessedRunStore, ProcessedRunStore
from data_ingest.exceptions import ConfigurationError

BASE_CONFIG = """
source:
  name: acme
  type: snowflake

connection:
  secret_id: acme-secret

landing:
  location: s3://landing-bucket/landing
  checkpoint_table: checkpoints

tables:
  - name: order_fact
    database: D
    schema: S
    table: T
    primary_key: [ORDER_KEY]
    checkpoint:
      type: watermark
      column: UPDATED_AT
"""

BRONZE_SECTION = """
bronze:
  database: bronze_acme
  location: s3://landing-bucket/bronze
  athena_output: s3://landing-bucket/athena-results/
  processed_runs_table: processed-runs
"""


@contextmanager
def aws_env():
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="landing-bucket")
        boto3.client("dynamodb", region_name="us-east-1").create_table(
            TableName="processed-runs",
            KeySchema=[
                {"AttributeName": "table_key", "KeyType": "HASH"},
                {"AttributeName": "run_id", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "table_key", "AttributeType": "S"},
                {"AttributeName": "run_id", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        yield


@contextmanager
def captured_load():
    """Intercept load_bronze so the wiring can be asserted without Athena."""
    calls = []

    def fake_load_bronze(**kwargs):
        calls.append(kwargs)
        return []

    with patch("data_ingest.bronze.job.load_bronze", side_effect=fake_load_bronze):
        yield calls


def write_config(tmp_path, text):
    path = tmp_path / "acme_snowflake.yaml"
    path.write_text(text)
    return str(path)


def test_missing_bronze_section_is_reported_clearly(tmp_path):
    # Bronze is opt-in, so a config without the section is a normal state --
    # but pointing the Bronze job at one is a mistake worth naming.
    config = write_config(tmp_path, BASE_CONFIG)
    with aws_env():
        with pytest.raises(ConfigurationError, match="No bronze configuration"):
            run_bronze_job(argv=["--config-uri", config])


def test_settings_resolve_from_the_config(tmp_path):
    config = write_config(tmp_path, BASE_CONFIG + BRONZE_SECTION)
    with aws_env(), captured_load() as calls:
        run_bronze_job(argv=["--config-uri", config])

    (kwargs,) = calls
    assert kwargs["bucket"] == "landing-bucket"
    assert kwargs["landing_prefix"] == "landing"
    assert kwargs["source_key"] == "acme_snowflake"
    assert [t.name for t in kwargs["tables"]] == ["order_fact"]
    assert kwargs["athena"].database == "bronze_acme"
    assert kwargs["athena"].output_location == "s3://landing-bucket/athena-results/"


def test_cli_args_override_the_config(tmp_path):
    config = write_config(tmp_path, BASE_CONFIG + BRONZE_SECTION)
    with aws_env(), captured_load() as calls:
        run_bronze_job(argv=[
            "--config-uri", config,
            "--bronze-database", "override_db",
            "--athena-workgroup", "override_wg",
        ])

    athena = calls[0]["athena"]
    assert athena.database == "override_db"
    assert athena.workgroup == "override_wg"


def test_tables_selector_limits_which_tables_load(tmp_path):
    two_tables = BASE_CONFIG + """
  - name: customers
    database: D
    schema: S
    table: C
    primary_key: [CUSTOMER_ID]
    checkpoint:
      type: watermark
      column: UPDATED_AT
""" + BRONZE_SECTION
    config = write_config(tmp_path, two_tables)

    with aws_env(), captured_load() as calls:
        run_bronze_job(argv=["--config-uri", config, "--tables", "customers"])

    assert [t.name for t in calls[0]["tables"]] == ["customers"]


def test_processed_runs_table_selects_the_real_store(tmp_path):
    config = write_config(tmp_path, BASE_CONFIG + BRONZE_SECTION)
    with aws_env(), captured_load() as calls:
        run_bronze_job(argv=["--config-uri", config])

    assert isinstance(calls[0]["processed_runs"], ProcessedRunStore)


def test_without_a_processed_runs_table_every_run_is_re_merged(tmp_path):
    # Correct, because the merge is idempotent -- just increasingly costly.
    # The null store is the explicit representation of that trade.
    config = write_config(
        tmp_path,
        BASE_CONFIG + BRONZE_SECTION.replace("  processed_runs_table: processed-runs\n", ""),
    )
    with aws_env(), captured_load() as calls:
        run_bronze_job(argv=["--config-uri", config])

    assert isinstance(calls[0]["processed_runs"], NullProcessedRunStore)


def test_missing_required_settings_are_named(tmp_path):
    config = write_config(tmp_path, BASE_CONFIG.replace(
        "  location: s3://landing-bucket/landing\n", ""
    ) + BRONZE_SECTION)

    with aws_env():
        with pytest.raises(ConfigurationError, match="Missing required setting"):
            run_bronze_job(argv=["--config-uri", config])


def test_a_load_failure_exits_nonzero(tmp_path):
    """
    Glue reads the exit code; a Bronze failure that exited 0 would be
    reported as a successful run with data silently missing.
    """
    config = write_config(tmp_path, BASE_CONFIG + BRONZE_SECTION)

    with aws_env():
        with patch("data_ingest.bronze.job.load_bronze", side_effect=RuntimeError("boom")):
            with pytest.raises(SystemExit) as exc_info:
                run_bronze_job(argv=["--config-uri", config])

    assert exc_info.value.code == 1


def test_unknown_glue_arguments_are_ignored(tmp_path):
    # Glue injects --JOB_NAME and friends; a strict parser would reject them.
    config = write_config(tmp_path, BASE_CONFIG + BRONZE_SECTION)
    with aws_env(), captured_load() as calls:
        run_bronze_job(argv=[
            "--config-uri", config,
            "--JOB_NAME", "bronze-load",
            "--JOB_RUN_ID", "jr_123",
        ])
    assert len(calls) == 1
