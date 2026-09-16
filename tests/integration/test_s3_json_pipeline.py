"""Real POS decoding/Parquet/state integration; AWS is moto, Athena is recorded."""

import base64
import gzip
import io
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import patch

import boto3
import pytest
import yaml
from botocore.exceptions import ClientError
from botocore.stub import Stubber
from moto import mock_aws

from data_ingest.landing import LandingWriter
from data_ingest.bronze.loader import load_table_runs
from data_ingest.bronze.state import NullProcessedRunStore
from data_ingest.config import parse_config
from data_ingest.exceptions import ExtractionError, ManifestCommitError
from data_ingest.pipeline import run_job, run_table, state_key_for
from data_ingest.sources.s3_json import S3JsonSource
from data_ingest.state import DynamoDBStateStore

# Import after landing initializes pandas, matching the runtime import order.
import pyarrow.parquet as pq

RAW_BUCKET = "pos-source-test"
LANDING_BUCKET = "pos-landing-test"
STATE_TABLE = "pos-state-test"


@pytest.fixture
def env():
    with mock_aws():
        s3 = boto3.client("s3")
        for bucket in (RAW_BUCKET, LANDING_BUCKET):
            s3.create_bucket(Bucket=bucket)
        table = boto3.resource("dynamodb").create_table(
            TableName=STATE_TABLE,
            KeySchema=[{"AttributeName": "source_key", "KeyType": "HASH"},
                       {"AttributeName": "table_name", "KeyType": "RANGE"}],
            AttributeDefinitions=[{"AttributeName": "source_key", "AttributeType": "S"},
                                  {"AttributeName": "table_name", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        now = datetime.now(timezone.utc)
        config_text = yaml.safe_dump({
            "source": {
                "name": "par_pos", "type": "s3_json",
                "location": f"s3://{RAW_BUCKET}/orders",
                "document": {"preset": "cloudevents"},
            },
            "landing": {"location": f"s3://{LANDING_BUCKET}/landing",
                        "checkpoint_table": STATE_TABLE},
            "tables": [{
                "name": "orders",
                "start_at": (now - timedelta(hours=1)).isoformat(),
                "envelope_fields": {"business_date": "businessdate"},
            }],
        }, sort_keys=False)
        yield {
            "s3": s3, "store": DynamoDBStateStore(table),
            "writer": LandingWriter(s3, LANDING_BUCKET, "landing"),
            "config": parse_config(config_text), "yaml": config_text, "now": now,
        }


def put_orders(env, versions=(1, 2), filename="0001.json.gz"):
    payloads = [
        '{"order":{"id":"order-42"},"businessDate":"2026-09-10",'
        '"version":' + str(version) + ',"amount":1234567890.1234567890123456789,'
        '"lines":[{"sku":"meal","modifiers":["extra"]}]}'
        for version in versions
    ]
    envelopes = [{
        "type": "order.updated", "specversion": "1.0", "source": "store-1",
        "id": f"publication-{version}", "time": "2026-09-10T12:00:00Z",
        "groupid": "group-1", "businessdate": "2026-09-10",
        "historicaldatatype": "order", "datacontenttype": "application/json",
        "data_base64": base64.b64encode(gzip.compress(payload.encode())).decode(),
    } for version, payload in zip(versions, payloads)]
    key = "orders/" + env["now"].strftime("%Y/%m/%d/%H/") + filename
    env["s3"].put_object(Bucket=RAW_BUCKET, Key=key,
                         Body=gzip.compress(json.dumps(envelopes).encode()))
    modified = env["s3"].head_object(Bucket=RAW_BUCKET, Key=key)["LastModified"]
    return key, payloads, modified


def source(env, now, fetch_size=1):
    table = env["config"].tables[0]
    return S3JsonSource(table.s3, lookback_minutes=table.checkpoint.lookback_minutes,
                       fetch_size=fetch_size, s3_client=env["s3"], now=lambda: now)


def run(env, now):
    return run_table(source(env, now), env["store"], env["writer"],
                     "s3_json", "par_pos", env["config"].tables[0])


def state(env):
    key = state_key_for("s3_json", "par_pos", env["config"].tables[0])
    return env["store"].get(key)


def keys(env, suffix):
    return [item["Key"] for item in env["s3"].list_objects_v2(
        Bucket=LANDING_BUCKET, Prefix="landing/").get("Contents", [])
        if item["Key"].endswith(suffix)]


def manifest(env, result):
    key = next(key for key in keys(env, "_manifest.json") if result.run_id in key)
    return json.loads(env["s3"].get_object(Bucket=LANDING_BUCKET, Key=key)["Body"].read())


def rows(env, result):
    records = []
    for uri in manifest(env, result)["files"]:
        key = uri.split("/", 3)[3]
        content = env["s3"].get_object(Bucket=LANDING_BUCKET, Key=key)["Body"].read()
        records += pq.ParquetFile(io.BytesIO(content)).read(use_threads=False).to_pylist()
    return records


def test_versions_and_complete_payload_survive_parquet_and_replay(env):
    key, payloads, modified = put_orders(env)
    now = modified + timedelta(seconds=121)
    first = run(env, now)
    landed = rows(env, first)
    assert first.status == "SUCCESS"
    assert first.row_count == 2 and first.file_count == 2
    assert [row["payload_json"] for row in landed] == payloads
    # Payload values are not columns: both versions of one order survive as
    # separate rows, and the values are read back out of payload_json.
    assert [json.loads(row["payload_json"])["version"] for row in landed] == [1, 2]
    assert {json.loads(row["payload_json"])["order"]["id"] for row in landed} == {"order-42"}
    assert len({row["_source_record_id"] for row in landed}) == 2
    for ordinal, row in enumerate(landed):
        assert row["_s3_key"] == key and row["_s3_bucket"] == RAW_BUCKET
        assert row["_s3_record_index"] == ordinal
        assert row["_s3_last_modified"] == modified.replace(tzinfo=None)
        assert row["_ingest_run_id"] == first.run_id
        assert row["_source_system"] == "par_pos_s3_json"
        envelope = json.loads(row["envelope_json"])
        assert gzip.decompress(base64.b64decode(envelope["data_base64"])).decode() == payloads[ordinal]
        assert json.loads(row["payload_json"], parse_float=Decimal)["amount"] == Decimal(
            "1234567890.1234567890123456789")
    committed = manifest(env, first)
    assert committed["primary_key"] == ["_source_record_id"]
    assert committed["checkpoint"]["column"] == "_s3_last_modified"
    assert committed["schema_drift"] is False
    assert state(env).checkpoint.value == committed["checkpoint"]["high"]

    replay = run(env, now + timedelta(minutes=1))
    assert [r["_source_record_id"] for r in rows(env, replay)] == [
        r["_source_record_id"] for r in landed]
    assert replay.run_id != first.run_id
    assert state(env).version == 2
    assert manifest(env, replay)["load_type"] == "incremental"


def test_corrupt_object_after_landed_batch_cannot_commit(env):
    _, _, modified = put_orders(env)
    now = modified + timedelta(seconds=121)
    run(env, now)
    before_state = state(env)
    before_manifests = keys(env, "_manifest.json")
    before_parts = keys(env, ".parquet")
    corrupt_key = "orders/" + env["now"].strftime("%Y/%m/%d/%H/") + "zzzz.json.gz"
    env["s3"].put_object(Bucket=RAW_BUCKET, Key=corrupt_key, Body=b"corrupt-private-data")
    with pytest.raises(ExtractionError) as error:
        run(env, now + timedelta(minutes=1))
    assert "corrupt-private-data" not in str(error.value)
    assert len(keys(env, ".parquet")) > len(before_parts)
    assert keys(env, "_manifest.json") == before_manifests
    assert state(env) == before_state


def test_failed_manifest_put_leaves_parquet_uncommitted_and_retryable(env):
    _, _, modified = put_orders(env)
    real_put = env["s3"].put_object

    def fail_manifest(**kwargs):
        if kwargs["Key"].endswith("_manifest.json"):
            raise ClientError({"Error": {"Code": "AccessDenied", "Message": "denied"}}, "PutObject")
        return real_put(**kwargs)

    now = modified + timedelta(seconds=121)
    with patch.object(env["s3"], "put_object", side_effect=fail_manifest):
        with pytest.raises(ManifestCommitError):
            run(env, now)
    assert state(env) is None
    assert len(keys(env, ".parquet")) == 2
    assert keys(env, "_manifest.json") == []
    retry = run(env, now)
    assert retry.row_count == 2
    assert state(env).version == 1
    assert len(keys(env, "_manifest.json")) == 1


def test_empty_window_commits_and_new_source_instance_resumes(env):
    idle = run(env, env["now"] - timedelta(minutes=5))
    assert idle.status == "SUCCESS" and idle.row_count == 0
    assert manifest(env, idle)["files"] == []
    idle_high = state(env).checkpoint.value
    _, _, modified = put_orders(env)
    resumed = run(env, modified + timedelta(seconds=121))
    assert resumed.row_count == 2
    assert manifest(env, resumed)["checkpoint"]["previous"] == idle_high
    assert state(env).version == 2


def test_run_job_uses_iam_source_without_secrets_manager(env, tmp_path):
    _, _, modified = put_orders(env)
    path = tmp_path / "pos.yaml"
    path.write_text(env["yaml"])
    with patch("data_ingest.sources.s3_json.datetime", wraps=datetime) as clock, \
         patch("data_ingest.pipeline.get_secret", side_effect=AssertionError("unexpected secret")) as secret:
        clock.now.return_value = modified + timedelta(seconds=121)
        result = run_job(["--config-uri", str(path)], expected_source_type="s3_json")[0]
    secret.assert_not_called()
    assert result.status == "SUCCESS" and result.row_count == 2
    assert state(env).version == 1


class RecordingAthena:
    """No SQL execution: this verifies the integration's emitted statements."""

    def __init__(self):
        self.statements = []

    def execute(self, sql, description=None):
        self.statements = [*self.statements, sql]
        return str(len(self.statements))


def test_real_bronze_loader_uses_source_identity_in_recorded_sql(env):
    _, _, modified = put_orders(env)
    run(env, modified + timedelta(seconds=121))
    glue = boto3.client("glue")
    athena = RecordingAthena()
    # Glue's optional moto backend requires extras absent from this project.
    # Stub the real SDK's missing-table replies, preserving modeled errors.
    with Stubber(glue) as catalog:
        for _ in range(5):
            catalog.add_client_error("get_table", service_error_code="EntityNotFoundException")
        result = load_table_runs(
            athena=athena, s3_client=env["s3"], processed_runs=NullProcessedRunStore(),
            bucket=LANDING_BUCKET, landing_prefix="landing", source_key="par_pos_s3_json",
            table_config=env["config"].tables[0], bronze_location=f"s3://{LANDING_BUCKET}/bronze",
            partition_by=("month({checkpoint_column})",), glue_client=glue, database="bronze_test",
        )
        catalog.assert_no_pending_responses()
    assert result.merged_count == 1
    create = next(sql for sql in athena.statements if sql.startswith("CREATE TABLE"))
    assert "'table_type' = 'ICEBERG'" in create
    assert "month(_s3_last_modified)" in create
    assert "`payload_json` string" in create
    merge = next(sql for sql in athena.statements if sql.startswith("MERGE INTO"))
    assert 'target."_source_record_id" = source."_source_record_id"' in merge
    assert 'target."_s3_last_modified" = source."_s3_last_modified"' in merge
    assert "WHEN NOT MATCHED THEN INSERT" in merge
    assert 'target."order_id" = source."order_id"' not in merge
