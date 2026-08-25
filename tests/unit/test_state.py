import boto3
import pytest
from moto import mock_aws

from data_ingest.checkpoints.watermark import WatermarkCheckpoint
from data_ingest.exceptions import CheckpointConflictError
from data_ingest.state import DynamoDBStateStore, StateKey

TABLE_NAME = "ingestion-state-test"

# Identity is (source_name, table_name); source_type rides along as a plain
# attribute. See StateKey.
KEY = StateKey("snowflake", "acme", "orders")
KEY_X = StateKey("snowflake", "acme", "never_written")
SEEDED = StateKey("snowflake", "acme", "seeded_by_hand")


@pytest.fixture
def state_table():
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        client.create_table(
            TableName=TABLE_NAME,
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
        resource = boto3.resource("dynamodb", region_name="us-east-1")
        yield resource.Table(TABLE_NAME)


def test_missing_checkpoint_returns_none(state_table):
    store = DynamoDBStateStore(state_table)
    assert store.get(KEY_X) is None


def test_commit_then_get_round_trips(state_table):
    store = DynamoDBStateStore(state_table)
    checkpoint = WatermarkCheckpoint(column="UPDATED_AT", value="2026-08-24T12:00:00Z")

    store.commit(
        state_key=KEY_X,
        checkpoint=checkpoint,
        run_id="run-1",
        row_count=10,
        file_count=1,
        landing_prefix="s3://bucket/landing/x/run_id=run-1",
        expected_version=None,
    )

    record = store.get(KEY_X)
    assert record.checkpoint.value == "2026-08-24T12:00:00Z"
    assert record.version == 1
    assert record.last_successful_run_id == "run-1"
    assert record.last_row_count == 10


def test_concurrent_first_run_is_rejected(state_table):
    """Both runs see no prior state; attribute_not_exists lets exactly one win."""
    store = DynamoDBStateStore(state_table)
    checkpoint_a = WatermarkCheckpoint(column="UPDATED_AT", value="A")
    checkpoint_b = WatermarkCheckpoint(column="UPDATED_AT", value="B")

    store.commit(KEY, checkpoint_a, "run-1", 1, 1, "s3://x", expected_version=None)

    # Job B started with the same expected_version (None means "no state"
    # existed) but is now stale; the table already has version=1.
    with pytest.raises(CheckpointConflictError):
        store.commit(KEY, checkpoint_b, "run-2", 1, 1, "s3://x", expected_version=None)


def test_stale_version_commit_is_rejected_and_leaves_winner_intact(state_table):
    """
    The `version = :expected` guard on an EXISTING record.

    Worth its own test because a case that passes expected_version=None
    exercises only the attribute_not_exists branch, leaving this half of the
    ConditionExpression deletable without anything failing.

    Scenario: runs A and B both read version=1. A commits (-> 2). B then
    commits with its now-stale expected_version=1 and must be rejected
    without clobbering A's value.
    """
    store = DynamoDBStateStore(state_table)
    store.commit(
        KEY, WatermarkCheckpoint(column="UPDATED_AT", value="A"),
        "run-1", 1, 1, "s3://x", expected_version=None,
    )

    stale_version = store.get(KEY).version  # both runs read this
    assert stale_version == 1

    store.commit(
        KEY, WatermarkCheckpoint(column="UPDATED_AT", value="B"),
        "run-2", 1, 1, "s3://b", expected_version=stale_version,
    )

    with pytest.raises(CheckpointConflictError):
        store.commit(
            KEY, WatermarkCheckpoint(column="UPDATED_AT", value="C"),
            "run-3", 1, 1, "s3://c", expected_version=stale_version,
        )

    final = store.get(KEY)
    assert final.checkpoint.value == "B", "loser must not overwrite the winner"
    assert final.last_successful_run_id == "run-2"
    assert final.version == 2


def test_record_without_version_attribute_can_still_commit(state_table):
    """
    A record lacking `version` reads back as version 0. Branching on
    truthiness (`if expected_version:`) sent that down the
    attribute_not_exists path, which can never succeed against an existing
    item -- wedging the table into CheckpointConflictError on every run,
    forever. Hand-seeded and migrated state records hit exactly this.
    """
    state_table.put_item(
        Item={
            "source_key": SEEDED.source_key,
            "table_name": SEEDED.table_name,
            "checkpoint": {"type": "watermark", "column": "UPDATED_AT", "value": "2026-01-01"},
        }
    )
    store = DynamoDBStateStore(state_table)

    record = store.get(SEEDED)
    assert record.version == 0

    store.commit(
        SEEDED, WatermarkCheckpoint(column="UPDATED_AT", value="2026-02-01"),
        "run-1", 5, 1, "s3://x", expected_version=record.version,
    )

    adopted = store.get(SEEDED)
    assert adopted.checkpoint.value == "2026-02-01"
    assert adopted.version == 1


def test_value_type_round_trips_through_dynamodb(state_table):
    store = DynamoDBStateStore(state_table)
    store.commit(
        KEY,
        WatermarkCheckpoint(
            column="UPDATED_AT",
            value="2026-08-24 12:00:00.123456789",
            value_type="TIMESTAMP_NTZ",
            lookback_minutes=60,
        ),
        "run-1", 1, 1, "s3://x", expected_version=None,
    )

    checkpoint = store.get(KEY).checkpoint
    assert checkpoint.value == "2026-08-24 12:00:00.123456789"
    assert checkpoint.value_type == "TIMESTAMP_NTZ"
    # DynamoDB returns numbers as Decimal; must come back as a usable int.
    assert checkpoint.lookback_minutes == 60
    assert isinstance(checkpoint.lookback_minutes, int)


def test_second_commit_with_correct_version_succeeds(state_table):
    store = DynamoDBStateStore(state_table)
    checkpoint_a = WatermarkCheckpoint(column="UPDATED_AT", value="A")
    checkpoint_b = WatermarkCheckpoint(column="UPDATED_AT", value="B")

    store.commit(KEY, checkpoint_a, "run-1", 1, 1, "s3://x", expected_version=None)
    record = store.get(KEY)

    store.commit(KEY, checkpoint_b, "run-2", 2, 1, "s3://x", expected_version=record.version)

    final = store.get(KEY)
    assert final.checkpoint.value == "B"
    assert final.version == 2


def test_list_for_source_queries_every_table_in_one_call(state_table):
    """
    The reason the table is (source_name HASH, table_name RANGE): asking
    "what is the state of every table in this source?" -- for staleness
    monitoring or an on-call check -- must be a Query, not a full Scan.
    """
    store = DynamoDBStateStore(state_table)
    for table_name, value in [("orders", "A"), ("customers", "B"), ("stores", "C")]:
        store.commit(
            StateKey("snowflake", "acme", table_name),
            WatermarkCheckpoint(column="UPDATED_AT", value=value),
            f"run-{table_name}", 1, 1, "s3://x", expected_version=None,
        )
    # A different source must not leak into the results.
    store.commit(
        StateKey("snowflake", "other_system", "orders"),
        WatermarkCheckpoint(column="UPDATED_AT", value="Z"),
        "run-other", 1, 1, "s3://x", expected_version=None,
    )

    records = store.list_for_source(KEY.source_key)

    assert set(records) == {"orders", "customers", "stores"}
    assert records["customers"].checkpoint.value == "B"
    assert records["orders"].last_successful_run_id == "run-orders"


def test_source_type_is_stored_as_an_attribute_not_part_of_the_key(state_table):
    store = DynamoDBStateStore(state_table)
    store.commit(KEY, WatermarkCheckpoint(column="UPDATED_AT", value="A"),
                 "run-1", 1, 1, "s3://x", expected_version=None)

    raw = state_table.get_item(
        Key={"source_key": KEY.source_key, "table_name": KEY.table_name}
    )["Item"]
    # Partition key is the derived identity; name/type are also stored
    # individually so the table stays readable without splitting the key.
    assert raw["source_key"] == "acme_snowflake"
    assert raw["source_name"] == "acme"
    assert raw["source_type"] == "snowflake"
