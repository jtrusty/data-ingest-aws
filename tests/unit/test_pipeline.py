import json
from unittest.mock import patch

import boto3
import pandas as pd
import pytest
from moto import mock_aws

from data_ingest.checkpoints.watermark import WatermarkCheckpoint
from data_ingest.config import CheckpointConfig, TableConfig
from data_ingest.exceptions import ExtractionError, LandingWriteError, ManifestCommitError
from data_ingest.landing import LandingRun, LandingWriter
from data_ingest.pipeline import run_table, state_key_for
from data_ingest.sources.base import Source
from data_ingest.state import DynamoDBStateStore, StateKey

BUCKET = "pipeline-test-bucket"
STATE_TABLE = "pipeline-test-state"

SOURCE_TYPE = "snowflake"
SOURCE_SYSTEM = "acme"


def _manifest_for(s3_client, run_id, table="orders"):
    """Locate and parse the _manifest.json for a run, wherever its ingest_date landed."""
    listing = s3_client.list_objects_v2(Bucket=BUCKET, Prefix="landing/")
    keys = [
        o["Key"]
        for o in listing.get("Contents", [])
        if o["Key"].endswith("_manifest.json") and f"run_id={run_id}" in o["Key"]
    ]
    assert keys, f"no manifest found for run_id={run_id}"
    return json.loads(s3_client.get_object(Bucket=BUCKET, Key=keys[0])["Body"].read())


class FakeSource(Source):
    def __init__(self, high_value, batches=None, fail=False, lookback_minutes=0, value_type="TIMESTAMP_NTZ"):
        self.high_value = high_value
        self.batches = batches or []
        self.fail = fail
        self.lookback_minutes = lookback_minutes
        self.value_type = value_type
        self.closed = False
        # Records the (previous, current) window each extract() call saw, so
        # tests can assert on the actual bounds rather than inferring them.
        self.extract_calls = []

    def get_current_checkpoint(self):
        return WatermarkCheckpoint(
            column="UPDATED_AT",
            value=self.high_value,
            lookback_minutes=self.lookback_minutes,
            value_type=self.value_type,
        )

    def extract(self, previous_checkpoint, current_checkpoint):
        self.extract_calls.append(
            (
                previous_checkpoint.value if previous_checkpoint else None,
                current_checkpoint.value,
            )
        )
        if self.fail:
            raise ExtractionError("boom")
        for batch in self.batches:
            yield batch

    def metadata(self):
        return {"database": "ACME", "schema": "PUBLIC", "table": "ORDERS"}

    def close(self):
        self.closed = True


@pytest.fixture
def env():
    with mock_aws():
        dynamodb = boto3.client("dynamodb", region_name="us-east-1")
        dynamodb.create_table(
            TableName=STATE_TABLE,
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
        s3.create_bucket(Bucket=BUCKET)

        state_store = DynamoDBStateStore(boto3.resource("dynamodb", region_name="us-east-1").Table(STATE_TABLE))
        landing_writer = LandingWriter(s3, BUCKET, "landing")

        yield state_store, landing_writer, s3


def make_table_config():
    return TableConfig(
        name="orders",
        database="ACME",
        schema="PUBLIC",
        table="ORDERS",
        primary_key=["ORDER_ID"],
        checkpoint=CheckpointConfig(type="watermark", column="UPDATED_AT"),
    )


# Derived via the real function rather than restated as a literal, so these
# tests keep pointing at whatever key run_table() actually writes. A
# hardcoded string would silently stop testing the real key the moment the
# derivation changed -- which is exactly what happened when identity moved
# from DATABASE.SCHEMA.TABLE to the config-local `name`.
SOURCE_KEY = state_key_for(SOURCE_TYPE, SOURCE_SYSTEM, make_table_config())


def test_source_key_is_built_from_config_local_names_not_the_source_object():
    """
    Identity is source_type:source.name:table.name -- deliberately NOT
    database.schema.table, so a source-side rename doesn't orphan state.
    """
    assert SOURCE_KEY == StateKey("snowflake", "acme", "orders")

    renamed_in_snowflake = TableConfig(
        name="orders",           # unchanged -> identity unchanged
        database="ACME",
        schema="ANALYTICS",            # moved schema
        table="ORDERS_V2",       # renamed object
        primary_key=["ORDER_ID"],
        checkpoint=CheckpointConfig(type="watermark", column="UPDATED_AT"),
    )
    assert state_key_for(SOURCE_TYPE, SOURCE_SYSTEM, renamed_in_snowflake) == SOURCE_KEY


def test_missing_checkpoint_triggers_full_load(env):
    state_store, landing_writer, s3 = env
    source = FakeSource(high_value="2026-08-24", batches=[pd.DataFrame({"ID": [1]})])

    result = run_table(source, state_store, landing_writer, SOURCE_TYPE, SOURCE_SYSTEM, make_table_config())

    assert result.status == "SUCCESS"
    assert result.row_count == 1

    record = state_store.get(SOURCE_KEY)
    assert record.checkpoint.value == "2026-08-24"


def test_existing_checkpoint_triggers_incremental_load(env):
    state_store, landing_writer, s3 = env
    table_config = make_table_config()

    state_store.commit(
        SOURCE_KEY,
        WatermarkCheckpoint(column="UPDATED_AT", value="2026-08-01"),
        run_id="prior-run",
        row_count=5,
        file_count=1,
        landing_prefix="s3://x",
        expected_version=None,
    )

    source = FakeSource(high_value="2026-08-24", batches=[pd.DataFrame({"ID": [2]})])
    result = run_table(source, state_store, landing_writer, SOURCE_TYPE, SOURCE_SYSTEM, table_config)

    assert result.status == "SUCCESS"
    record = state_store.get(SOURCE_KEY)
    assert record.checkpoint.value == "2026-08-24"
    assert record.version == 2


def test_failed_extraction_does_not_advance_checkpoint(env):
    state_store, landing_writer, s3 = env
    source = FakeSource(high_value="2026-08-24", fail=True)

    with pytest.raises(ExtractionError):
        run_table(source, state_store, landing_writer, SOURCE_TYPE, SOURCE_SYSTEM, make_table_config())

    assert state_store.get(SOURCE_KEY) is None


def test_no_new_data_is_skipped_without_writing_state(env):
    state_store, landing_writer, s3 = env
    table_config = make_table_config()

    state_store.commit(
        SOURCE_KEY,
        WatermarkCheckpoint(column="UPDATED_AT", value="2026-08-24"),
        run_id="prior-run",
        row_count=5,
        file_count=1,
        landing_prefix="s3://x",
        expected_version=None,
    )

    source = FakeSource(high_value="2026-08-24")
    result = run_table(source, state_store, landing_writer, SOURCE_TYPE, SOURCE_SYSTEM, table_config)

    assert result.status == "SKIPPED"
    record = state_store.get(SOURCE_KEY)
    assert record.version == 1


def test_empty_source_table_is_skipped(env):
    state_store, landing_writer, s3 = env
    source = FakeSource(high_value=None)

    result = run_table(source, state_store, landing_writer, SOURCE_TYPE, SOURCE_SYSTEM, make_table_config())

    assert result.status == "SKIPPED"
    assert state_store.get(SOURCE_KEY) is None


# --------------------------------------------------------------------------
# The headline Phase 1 requirement: full load, then incremental picking up
# exactly where the committed checkpoint left off.
# --------------------------------------------------------------------------


def test_full_then_incremental_transition_across_two_runs(env):
    state_store, landing_writer, s3 = env
    table_config = make_table_config()

    first = FakeSource(high_value="2026-08-01 00:00:00.000000000", batches=[pd.DataFrame({"ID": [1]})])
    first_result = run_table(first, state_store, landing_writer, SOURCE_TYPE, SOURCE_SYSTEM, table_config)

    assert first_result.status == "SUCCESS"
    # Run 1 is a full load: no lower bound.
    assert first.extract_calls == [(None, "2026-08-01 00:00:00.000000000")]
    assert _manifest_for(s3, first_result.run_id)["load_type"] == "full"

    second = FakeSource(high_value="2026-08-24 00:00:00.000000000", batches=[pd.DataFrame({"ID": [2]})])
    second_result = run_table(second, state_store, landing_writer, SOURCE_TYPE, SOURCE_SYSTEM, table_config)

    assert second_result.status == "SUCCESS"
    # Run 2's lower bound is exactly run 1's committed high watermark. This
    # is the seam a serialization change would silently break.
    assert second.extract_calls == [
        ("2026-08-01 00:00:00.000000000", "2026-08-24 00:00:00.000000000")
    ]
    manifest = _manifest_for(s3, second_result.run_id)
    assert manifest["load_type"] == "incremental"
    assert manifest["checkpoint"]["previous"] == "2026-08-01 00:00:00.000000000"
    assert manifest["checkpoint"]["high"] == "2026-08-24 00:00:00.000000000"

    record = state_store.get(SOURCE_KEY)
    assert record.checkpoint.value == "2026-08-24 00:00:00.000000000"
    assert record.version == 2


def test_successful_run_writes_manifest_at_the_run_prefix(env):
    state_store, landing_writer, s3 = env
    source = FakeSource(high_value="2026-08-24", batches=[pd.DataFrame({"ID": [1, 2]})])

    result = run_table(source, state_store, landing_writer, SOURCE_TYPE, SOURCE_SYSTEM, make_table_config())

    manifest = _manifest_for(s3, result.run_id)
    assert manifest["status"] == "SUCCESS"
    assert manifest["run_id"] == result.run_id
    assert manifest["row_count"] == 2
    assert manifest["file_count"] == 1
    assert manifest["primary_key"] == ["ORDER_ID"]
    assert manifest["source"] == {"database": "ACME", "schema": "PUBLIC", "table": "ORDERS"}
    assert manifest["schema"], "manifest should record the landed schema"


# --------------------------------------------------------------------------
# Requirement 1: nothing before the commit may advance the checkpoint.
# --------------------------------------------------------------------------


def test_failed_parquet_write_does_not_advance_checkpoint(env):
    state_store, landing_writer, s3 = env
    source = FakeSource(high_value="2026-08-24", batches=[pd.DataFrame({"ID": [1]})])

    with patch.object(LandingRun, "write_batch", side_effect=LandingWriteError("s3 down")):
        with pytest.raises(LandingWriteError):
            run_table(source, state_store, landing_writer, SOURCE_TYPE, SOURCE_SYSTEM, make_table_config())

    assert state_store.get(SOURCE_KEY) is None


def test_failed_manifest_write_does_not_advance_checkpoint(env):
    """
    The manifest is the designated commit marker, so its failure is the most
    important not-committed case: Parquet is already in S3 at this point, and
    the run must still be treated as incomplete.
    """
    state_store, landing_writer, s3 = env
    source = FakeSource(high_value="2026-08-24", batches=[pd.DataFrame({"ID": [1]})])

    with patch.object(LandingRun, "write_manifest", side_effect=ManifestCommitError("no manifest")):
        with pytest.raises(ManifestCommitError):
            run_table(source, state_store, landing_writer, SOURCE_TYPE, SOURCE_SYSTEM, make_table_config())

    assert state_store.get(SOURCE_KEY) is None


def test_commit_happens_strictly_after_the_manifest(env):
    """Explicitly verify ordering, not just the end state."""
    state_store, landing_writer, s3 = env
    source = FakeSource(high_value="2026-08-24", batches=[pd.DataFrame({"ID": [1]})])

    calls = []
    real_manifest = LandingRun.write_manifest
    real_commit = DynamoDBStateStore.commit

    def record_manifest(self, *a, **kw):
        calls.append("manifest")
        return real_manifest(self, *a, **kw)

    def record_commit(self, *a, **kw):
        calls.append("commit")
        return real_commit(self, *a, **kw)

    with patch.object(LandingRun, "write_manifest", record_manifest), \
         patch.object(DynamoDBStateStore, "commit", record_commit):
        run_table(source, state_store, landing_writer, SOURCE_TYPE, SOURCE_SYSTEM, make_table_config())

    assert calls == ["manifest", "commit"]


# --------------------------------------------------------------------------
# Lookback and the regression guard
# --------------------------------------------------------------------------


def test_lookback_still_runs_when_high_watermark_is_unchanged(env):
    """
    Late-arriving rows carry a timestamp BEHIND the recorded high watermark,
    so they never raise MAX(). Skipping on an unchanged high watermark would
    make lookback unreachable in exactly the case it exists for.
    """
    state_store, landing_writer, s3 = env
    table_config = make_table_config()

    state_store.commit(
        SOURCE_KEY,
        WatermarkCheckpoint(column="UPDATED_AT", value="2026-08-24", value_type="TIMESTAMP_NTZ"),
        run_id="prior", row_count=1, file_count=1, landing_prefix="s3://x", expected_version=None,
    )

    source = FakeSource(
        high_value="2026-08-24",           # unchanged
        batches=[pd.DataFrame({"ID": [99]})],
        lookback_minutes=60,
    )
    result = run_table(source, state_store, landing_writer, SOURCE_TYPE, SOURCE_SYSTEM, table_config)

    assert result.status == "SUCCESS", "lookback run must not be skipped"
    assert source.extract_calls == [("2026-08-24", "2026-08-24")]
    assert result.row_count == 1


def test_no_lookback_still_skips_when_high_watermark_is_unchanged(env):
    state_store, landing_writer, s3 = env
    state_store.commit(
        SOURCE_KEY,
        WatermarkCheckpoint(column="UPDATED_AT", value="2026-08-24", value_type="TIMESTAMP_NTZ"),
        run_id="prior", row_count=1, file_count=1, landing_prefix="s3://x", expected_version=None,
    )

    source = FakeSource(high_value="2026-08-24", lookback_minutes=0)
    result = run_table(source, state_store, landing_writer, SOURCE_TYPE, SOURCE_SYSTEM, make_table_config())

    assert result.status == "SKIPPED"
    assert source.extract_calls == []


def test_regressed_high_watermark_does_not_rewind_the_checkpoint(env):
    """
    If the source's max watermark goes backwards (max row deleted, table
    restored/cloned), committing the lower value would make the next run
    re-extract everything in between as duplicates.
    """
    state_store, landing_writer, s3 = env
    state_store.commit(
        SOURCE_KEY,
        WatermarkCheckpoint(column="UPDATED_AT", value="2026-08-24", value_type="TIMESTAMP_NTZ"),
        run_id="prior", row_count=1, file_count=1, landing_prefix="s3://x", expected_version=None,
    )

    source = FakeSource(high_value="2026-08-20", value_type="TIMESTAMP_NTZ")
    result = run_table(source, state_store, landing_writer, SOURCE_TYPE, SOURCE_SYSTEM, make_table_config())

    assert result.status == "SKIPPED"
    assert source.extract_calls == []
    record = state_store.get(SOURCE_KEY)
    assert record.checkpoint.value == "2026-08-24", "checkpoint must not move backwards"
    assert record.version == 1


def test_numeric_watermark_regression_uses_numeric_ordering(env):
    # '9' > '10' lexicographically; a numeric watermark must not be judged
    # by string ordering.
    state_store, landing_writer, s3 = env
    state_store.commit(
        SOURCE_KEY,
        WatermarkCheckpoint(column="UPDATED_AT", value="9", value_type="FIXED"),
        run_id="prior", row_count=1, file_count=1, landing_prefix="s3://x", expected_version=None,
    )

    source = FakeSource(high_value="10", value_type="FIXED", batches=[pd.DataFrame({"ID": [1]})])
    result = run_table(source, state_store, landing_writer, SOURCE_TYPE, SOURCE_SYSTEM, make_table_config())

    assert result.status == "SUCCESS", "10 > 9 numerically; must not be treated as a regression"
