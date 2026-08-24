from contextlib import contextmanager
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws

from data_ingest.checkpoints.watermark import WatermarkCheckpoint
from data_ingest.exceptions import (
    CheckpointConflictError,
    ConfigurationError,
    ExtractionError,
)
from data_ingest.pipeline import _resolve_bool, run_job
from data_ingest.sources.base import Source

CONFIG_YAML = """
source:
  name: acme
  type: snowflake

connection:
  secret_id: /data-platform/acme/snowflake

tables:
  - name: orders
    database: ACME
    schema: PUBLIC
    table: ORDERS
    primary_key: [ORDER_ID]
    checkpoint:
      type: watermark
      column: UPDATED_AT
"""

TWO_TABLE_YAML = CONFIG_YAML + """
  - name: customers
    database: ACME
    schema: PUBLIC
    table: CUSTOMERS
    primary_key: [CUSTOMER_ID]
    checkpoint:
      type: watermark
      column: UPDATED_AT
"""


@pytest.fixture
def config_path(tmp_path):
    path = tmp_path / "acme.yaml"
    path.write_text(CONFIG_YAML)
    return str(path)


def test_expected_source_type_mismatch_fails_fast(config_path):
    with mock_aws():
        # A Glue script deployed for one source type (e.g. jobs/rest_ingest.py
        # expecting "rest") pointed at this Snowflake config should fail
        # immediately, before touching Secrets Manager/DynamoDB/S3 for any
        # table, rather than silently matching zero tables.
        with pytest.raises(ConfigurationError, match="expects a 'rest' source"):
            run_job(
                argv=[
                    "--config-uri", config_path,
                    "--state-table", "irrelevant-state-table",
                    "--s3-bucket", "irrelevant-bucket",
                    "--tables", "all",
                ],
                expected_source_type="rest",
            )


def test_matching_expected_source_type_passes_the_check(config_path):
    # Secrets Manager has no matching secret, so this fails past the
    # expected_source_type check (proving the check itself did not reject
    # a correctly-typed config) with a boto3 ClientError instead.
    with mock_aws():
        with pytest.raises(Exception) as exc_info:
            run_job(
                argv=[
                    "--config-uri", config_path,
                    "--state-table", "irrelevant-state-table",
                    "--s3-bucket", "irrelevant-bucket",
                    "--tables", "all",
                ],
                expected_source_type="snowflake",
            )
        assert "expects a" not in str(exc_info.value)


def test_missing_bucket_and_state_table_raises_configuration_error(config_path):
    # Neither --s3-bucket/--state-table nor the config YAML define these --
    # should fail loudly with a clear message, not an AttributeError deep
    # inside boto3 from passing None as a table/bucket name.
    with mock_aws():
        with pytest.raises(ConfigurationError, match="Missing required setting"):
            run_job(argv=["--config-uri", config_path, "--tables", "all"])


YAML_DEPLOYMENT_SETTINGS = """
landing:
  location: s3://yaml-landing-bucket/custom-landing-prefix
  checkpoint_table: yaml-state-table

defaults:
  fetch_size: 12345
  fail_fast: false
"""


@contextmanager
def aws_env(secret_name="/data-platform/acme/snowflake",
            bucket="yaml-landing-bucket",
            state_table="yaml-state-table"):
    """Provision the AWS resources run_job() touches before reaching a source."""
    with mock_aws():
        boto3.client("dynamodb", region_name="us-east-1").create_table(
            TableName=state_table,
            KeySchema=[
                {"AttributeName": "source_key", "KeyType": "HASH"},
                {"AttributeName": "table_name", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "source_key", "AttributeType": "S"},
                {"AttributeName": "table_name", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=bucket)
        boto3.client("secretsmanager", region_name="us-east-1").create_secret(
            Name=secret_name,
            SecretString='{"account": "a", "username": "u", "password": "p", "warehouse": "w"}',
        )
        yield s3


class _RecordingSource(Source):
    """Captures what run_job resolved and handed to the source factory."""

    built = []

    def __init__(self, fetch_size):
        self.fetch_size = fetch_size

    def get_current_checkpoint(self):
        return WatermarkCheckpoint(column="UPDATED_AT", value=None)  # -> SKIPPED

    def extract(self, previous_checkpoint, current_checkpoint):
        return iter(())

    def metadata(self):
        return {"database": "D", "schema": "S", "table": "T"}


@contextmanager
def recording_source():
    """Patch the registry so no real Snowflake connection is attempted."""
    _RecordingSource.built = []

    def factory(source_type, credentials, table_config, fetch_size):
        source = _RecordingSource(fetch_size)
        _RecordingSource.built.append(
            {"source_type": source_type, "table": table_config.name, "fetch_size": fetch_size}
        )
        return source

    with patch("data_ingest.pipeline.build_source", side_effect=factory):
        yield _RecordingSource


def test_deployment_settings_resolve_from_config_yaml_when_cli_args_absent(tmp_path):
    """
    bucket/state-table/prefix/fetch-size all come from the YAML rather than
    the CLI. Asserts on the RESOLVED values -- an earlier version of this
    test only checked that some unrelated exception didn't mention certain
    strings, which any failure would have satisfied.
    """
    config_path = tmp_path / "acme.yaml"
    config_path.write_text(CONFIG_YAML + YAML_DEPLOYMENT_SETTINGS)

    with aws_env(), recording_source() as source_cls:
        results = run_job(argv=["--config-uri", str(config_path)])

    assert [r.status for r in results] == ["SKIPPED"]
    assert source_cls.built == [
        {"source_type": "snowflake", "table": "orders", "fetch_size": 12345}
    ]


def test_cli_arg_overrides_config_yaml(tmp_path):
    config_path = tmp_path / "acme.yaml"
    config_path.write_text(CONFIG_YAML + YAML_DEPLOYMENT_SETTINGS)

    with aws_env(), recording_source() as source_cls:
        run_job(argv=["--config-uri", str(config_path), "--fetch-size", "777"])

    assert source_cls.built[0]["fetch_size"] == 777


def test_tables_selector_limits_which_tables_run(tmp_path):
    config_path = tmp_path / "acme.yaml"
    config_path.write_text(TWO_TABLE_YAML + YAML_DEPLOYMENT_SETTINGS)

    with aws_env(), recording_source() as source_cls:
        run_job(argv=["--config-uri", str(config_path), "--tables", "customers"])

    assert [b["table"] for b in source_cls.built] == ["customers"]


def test_config_can_be_loaded_from_an_s3_uri(tmp_path):
    """The only config path production actually uses."""
    with aws_env() as s3:
        s3.put_object(
            Bucket="yaml-landing-bucket",
            Key="ingestion-config/acme.yaml",
            Body=(CONFIG_YAML + YAML_DEPLOYMENT_SETTINGS).encode(),
        )
        with recording_source() as source_cls:
            run_job(argv=["--config-uri", "s3://yaml-landing-bucket/ingestion-config/acme.yaml"])

    assert source_cls.built[0]["table"] == "orders"


@pytest.mark.parametrize("raw", ["false", "False", "0", "no", "off", " FALSE "])
def test_fail_fast_falsey_strings_disable_it(tmp_path, raw):
    # Glue passes every argument as a string. Matching only "false" meant
    # `--fail-fast 0` silently enabled fail-fast instead of disabling it.
    assert _resolve_bool(raw, None, True, arg_name="--fail-fast") is False


@pytest.mark.parametrize("raw", ["true", "TRUE", "1", "yes", "on"])
def test_fail_fast_truthy_strings_enable_it(raw):
    assert _resolve_bool(raw, None, False, arg_name="--fail-fast") is True


def test_fail_fast_rejects_ambiguous_values():
    with pytest.raises(ConfigurationError, match="boolean-ish"):
        _resolve_bool("maybe", None, True, arg_name="--fail-fast")


@pytest.mark.parametrize("bad", ["0", "-1"])
def test_non_positive_fetch_size_is_rejected(tmp_path, bad):
    """
    fetch_size=0 makes cursor.fetchmany(0) return [] immediately: zero rows
    land, a SUCCESS manifest is written, and the checkpoint advances past the
    whole window -- silently skipping data while reporting success.
    """
    config_path = tmp_path / "acme.yaml"
    config_path.write_text(CONFIG_YAML + YAML_DEPLOYMENT_SETTINGS)

    with aws_env():
        with pytest.raises(ConfigurationError, match="fetch_size must be >= 1"):
            run_job(argv=["--config-uri", str(config_path), "--fetch-size", bad])


def test_fail_fast_stops_before_the_next_table_and_exits_nonzero(tmp_path):
    config_path = tmp_path / "acme.yaml"
    config_path.write_text(TWO_TABLE_YAML + YAML_DEPLOYMENT_SETTINGS)

    attempted = []

    def exploding_factory(source_type, credentials, table_config, fetch_size):
        attempted.append(table_config.name)
        raise ExtractionError(f"{table_config.name} boom")

    with aws_env():
        with patch("data_ingest.pipeline.build_source", side_effect=exploding_factory):
            with pytest.raises(SystemExit) as exc_info:
                run_job(argv=["--config-uri", str(config_path), "--fail-fast", "true"])

    # Non-zero exit matters: a scheduler must not read a partial failure as success.
    assert exc_info.value.code == 1
    assert attempted == ["orders"], "fail_fast must not start the second table"


def test_continue_on_failure_runs_remaining_tables_and_still_exits_nonzero(tmp_path):
    config_path = tmp_path / "acme.yaml"
    config_path.write_text(TWO_TABLE_YAML + YAML_DEPLOYMENT_SETTINGS)

    attempted = []

    def exploding_factory(source_type, credentials, table_config, fetch_size):
        attempted.append(table_config.name)
        raise ExtractionError(f"{table_config.name} boom")

    with aws_env():
        with patch("data_ingest.pipeline.build_source", side_effect=exploding_factory):
            with pytest.raises(SystemExit) as exc_info:
                run_job(argv=["--config-uri", str(config_path), "--fail-fast", "false"])

    assert exc_info.value.code == 1
    assert attempted == ["orders", "customers"]


def test_checkpoint_conflict_is_reported_as_a_failed_table(tmp_path):
    """The branch that runs during the exact incident it exists for."""
    config_path = tmp_path / "acme.yaml"
    config_path.write_text(CONFIG_YAML + YAML_DEPLOYMENT_SETTINGS)

    def conflicting_factory(source_type, credentials, table_config, fetch_size):
        raise CheckpointConflictError("another run already committed")

    with aws_env():
        with patch("data_ingest.pipeline.build_source", side_effect=conflicting_factory):
            with pytest.raises(SystemExit) as exc_info:
                run_job(argv=["--config-uri", str(config_path)])

    assert exc_info.value.code == 1
