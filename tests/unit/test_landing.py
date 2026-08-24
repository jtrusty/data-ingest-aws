import io
import json

import boto3
import pandas as pd
import pyarrow.parquet as pq
import pytest
from moto import mock_aws

from data_ingest.exceptions import LandingWriteError
from data_ingest.landing import LINEAGE_COLUMNS, LandingWriter

BUCKET = "landing-test-bucket"


@pytest.fixture
def s3_client():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


def test_prefix_layout_and_run_id_uniqueness(s3_client):
    writer = LandingWriter(s3_client, BUCKET, "landing")

    run1 = writer.start("acme", "orders", "run-1", "ACME", "PUBLIC", "ORDERS", ingest_date="2026-08-24")
    run2 = writer.start("acme", "orders", "run-2", "ACME", "PUBLIC", "ORDERS", ingest_date="2026-08-24")

    assert run1.prefix == "landing/acme/orders/ingest_date=2026-08-24/run_id=run-1"
    assert run2.prefix == "landing/acme/orders/ingest_date=2026-08-24/run_id=run-2"
    assert run1.prefix != run2.prefix


def _read_parquet_from_s3(s3_client, key):
    body = s3_client.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    return pq.read_table(io.BytesIO(body)).to_pandas()


def test_write_batch_adds_lineage_columns_and_uploads_parquet(s3_client):
    writer = LandingWriter(s3_client, BUCKET, "landing")
    run = writer.start("acme", "orders", "run-1", "ACME", "PUBLIC", "ORDERS", ingest_date="2026-08-24")

    dataframe = pd.DataFrame({"ID": [1, 2], "UPDATED_AT": ["2026-08-24", "2026-08-24"]})
    run.write_batch(dataframe)

    assert run.row_count == 2
    assert run.file_count == 1
    assert len(run.files) == 1

    key = "landing/acme/orders/ingest_date=2026-08-24/run_id=run-1/part-00000.parquet"

    # Read the Parquet back and assert on actual content. Asserting only
    # that the object is non-empty would still pass with _add_lineage_columns
    # replaced by `return dataframe`, leaving the lineage requirement
    # effectively untested.
    landed = _read_parquet_from_s3(s3_client, key)

    for column in LINEAGE_COLUMNS:
        assert column in landed.columns, f"missing lineage column {column}"

    assert set(landed["_ingest_run_id"]) == {"run-1"}
    assert set(landed["_source_system"]) == {"acme"}
    assert set(landed["_source_database"]) == {"ACME"}
    assert set(landed["_source_schema"]) == {"PUBLIC"}
    assert set(landed["_source_table"]) == {"ORDERS"}
    assert landed["_ingested_at"].notna().all()
    # Source columns survive alongside the lineage ones.
    assert list(landed["ID"]) == [1, 2]


def test_write_batch_raises_on_lineage_column_collision(s3_client):
    # A source column named `_source_table` would otherwise be silently
    # overwritten, destroying source data with no error.
    writer = LandingWriter(s3_client, BUCKET, "landing")
    run = writer.start("acme", "orders", "run-1", "ACME", "PUBLIC", "ORDERS", ingest_date="2026-08-24")

    dataframe = pd.DataFrame({"ID": [1], "_source_table": ["something_real"]})

    with pytest.raises(LandingWriteError, match="reserved ingestion"):
        run.write_batch(dataframe)


def test_schema_is_pinned_across_batches(s3_client):
    """
    A column that is all-NULL in batch 0 and populated in batch 1 must not
    produce two part-files with incompatible schemas in the same immutable
    run prefix -- the prefix can never be rewritten to fix it.
    """
    writer = LandingWriter(s3_client, BUCKET, "landing")
    run = writer.start("acme", "orders", "run-1", "ACME", "PUBLIC", "ORDERS", ingest_date="2026-08-24")

    run.write_batch(pd.DataFrame({"ID": [1], "NOTE": [None]}))
    run.write_batch(pd.DataFrame({"ID": [2], "NOTE": ["now populated"]}))

    assert run.file_count == 2

    base = "landing/acme/orders/ingest_date=2026-08-24/run_id=run-1"
    body0 = s3_client.get_object(Bucket=BUCKET, Key=f"{base}/part-00000.parquet")["Body"].read()
    body1 = s3_client.get_object(Bucket=BUCKET, Key=f"{base}/part-00001.parquet")["Body"].read()

    schema0 = pq.read_table(io.BytesIO(body0)).schema
    schema1 = pq.read_table(io.BytesIO(body1)).schema
    assert schema0.equals(schema1), f"schema drift within one run:\n{schema0}\nvs\n{schema1}"


def test_write_batch_skips_empty_dataframe(s3_client):
    writer = LandingWriter(s3_client, BUCKET, "landing")
    run = writer.start("acme", "orders", "run-1", "ACME", "PUBLIC", "ORDERS", ingest_date="2026-08-24")

    run.write_batch(pd.DataFrame())

    assert run.row_count == 0
    assert run.file_count == 0


def test_manifest_written_and_records_counts(s3_client):
    writer = LandingWriter(s3_client, BUCKET, "landing")
    run = writer.start("acme", "orders", "run-1", "ACME", "PUBLIC", "ORDERS", ingest_date="2026-08-24")
    run.write_batch(pd.DataFrame({"ID": [1]}))

    manifest = run.write_manifest(
        source_metadata={"database": "ACME", "schema": "PUBLIC", "table": "ORDERS"},
        primary_key=["ID"],
        checkpoint_manifest={"type": "watermark", "column": "UPDATED_AT", "previous": None, "high": "2026-08-24"},
        load_type="full",
    )

    assert manifest.status == "SUCCESS"
    assert manifest.row_count == 1
    assert manifest.file_count == 1

    key = "landing/acme/orders/ingest_date=2026-08-24/run_id=run-1/_manifest.json"
    obj = s3_client.get_object(Bucket=BUCKET, Key=key)
    body = json.loads(obj["Body"].read())
    assert body["status"] == "SUCCESS"
    assert body["load_type"] == "full"
