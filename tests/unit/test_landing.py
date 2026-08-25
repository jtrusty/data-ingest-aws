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


# --------------------------------------------------------------------------
# Declared schema vs inferred schema
# --------------------------------------------------------------------------


def test_declared_schema_prevents_decimal_precision_drift(s3_client):
    """
    The regression test for a real production warning:

        Schema drift within run ... Decimal type with precision 5 does not
        fit into precision inferred from first array element: 4

    pyarrow sizes a decimal from the first array element it sees. A column
    declared NUMBER(38,3) whose first batch holds only small values infers as
    decimal128(4,3); the first larger value later in the run then cannot
    conform, and the run splits across two incompatible Parquet schemas in
    one immutable prefix that can never be rewritten.

    The source knows the declared precision. Using it removes the guess.
    """
    import pyarrow as pa
    from decimal import Decimal

    declared = pa.schema([pa.field("AMT", pa.decimal128(38, 3))])

    writer = LandingWriter(s3_client, BUCKET, "landing")
    run = writer.start("acme", "orders", "run-1", "ACME", "PUBLIC", "ORDERS",
                       ingest_date="2026-08-25")

    # Batch 0 would infer decimal128(4,3); batch 1 needs more precision.
    run.write_batch(pd.DataFrame({"AMT": [Decimal("1.234")]}), declared_schema=declared)
    run.write_batch(pd.DataFrame({"AMT": [Decimal("12345.678")]}), declared_schema=declared)

    assert run.file_count == 2
    assert run.schema_drift is False, "declared types must remove the drift entirely"

    base = "landing/acme/orders/ingest_date=2026-08-25/run_id=run-1"
    schemas = [
        pq.read_table(io.BytesIO(
            s3_client.get_object(Bucket=BUCKET, Key=f"{base}/part-{i:05d}.parquet")["Body"].read()
        )).schema
        for i in (0, 1)
    ]
    assert schemas[0].equals(schemas[1])
    assert schemas[0].field("AMT").type == pa.decimal128(38, 3)


def test_inference_alone_would_have_drifted(s3_client):
    """Confirms the test above is testing something -- without the declared
    schema, the same two batches DO drift."""
    from decimal import Decimal

    writer = LandingWriter(s3_client, BUCKET, "landing")
    run = writer.start("acme", "orders", "run-2", "ACME", "PUBLIC", "ORDERS",
                       ingest_date="2026-08-25")

    run.write_batch(pd.DataFrame({"AMT": [Decimal("1.234")]}))
    run.write_batch(pd.DataFrame({"AMT": [Decimal("12345.678")]}))

    assert run.schema_drift is True


def test_declared_schema_covers_the_lineage_columns(s3_client):
    """
    The source describes its own columns only. Lineage columns are appended
    by the writer, so a declared schema that omitted them would reject every
    batch.
    """
    import pyarrow as pa

    declared = pa.schema([pa.field("ID", pa.int64())])
    writer = LandingWriter(s3_client, BUCKET, "landing")
    run = writer.start("acme", "orders", "run-3", "ACME", "PUBLIC", "ORDERS",
                       ingest_date="2026-08-25")

    run.write_batch(pd.DataFrame({"ID": [1, 2]}), declared_schema=declared)

    assert run.schema_drift is False
    landed = _read_parquet_from_s3(
        s3_client, "landing/acme/orders/ingest_date=2026-08-25/run_id=run-3/part-00000.parquet"
    )
    for column in LINEAGE_COLUMNS:
        assert column in landed.columns


def test_scale_zero_number_columns_conform_to_the_declared_decimal(s3_client):
    """
    Regression for a second drift warning, on ORDER_ID:

        Got bytestring of length 8 (expected 16)
        Conversion failed for column ORDER_ID with type int64

    Snowflake's default NUMBER(38,0) declares as decimal128(38,0), but the
    connector returns Python ints for scale-0 columns, so pandas types the
    batch int64 -- and pyarrow cannot cast int64 to decimal128. Every ID
    column therefore failed to conform and fell back to per-batch inference.

    The declared type stays authoritative (it preserves precision beyond
    int64); the data is adapted to it.
    """
    import pyarrow as pa

    declared = pa.schema([pa.field("ORDER_ID", pa.decimal128(38, 0))])
    writer = LandingWriter(s3_client, BUCKET, "landing")
    run = writer.start("acme", "orders", "r-int", "ACME", "PUBLIC", "ORDERS",
                       ingest_date="2026-08-25")

    run.write_batch(pd.DataFrame({"ORDER_ID": [1, 2, 3]}), declared_schema=declared)

    assert run.schema_drift is False
    assert run.schema.field("ORDER_ID").type == pa.decimal128(38, 0)


def test_a_null_in_an_id_column_does_not_break_conformance(s3_client):
    """
    pandas has no NaN for int64, so one NULL retypes the whole batch to
    float64. Coercing only integer dtypes would work until the first null and
    then silently stop -- drift appearing partway through a run for no
    visible reason.
    """
    import pyarrow as pa

    declared = pa.schema([pa.field("ORDER_ID", pa.decimal128(38, 0))])
    writer = LandingWriter(s3_client, BUCKET, "landing")
    run = writer.start("acme", "orders", "r-null", "ACME", "PUBLIC", "ORDERS",
                       ingest_date="2026-08-25")

    run.write_batch(pd.DataFrame({"ORDER_ID": [1, 2]}), declared_schema=declared)
    run.write_batch(pd.DataFrame({"ORDER_ID": [3, None]}), declared_schema=declared)

    assert run.schema_drift is False, "a null must not trigger drift"
    assert run.file_count == 2

    base = "landing/acme/orders/ingest_date=2026-08-25/run_id=r-null"
    schemas = [
        pq.read_table(io.BytesIO(
            s3_client.get_object(Bucket=BUCKET, Key=f"{base}/part-{i:05d}.parquet")["Body"].read()
        )).schema
        for i in (0, 1)
    ]
    assert schemas[0].equals(schemas[1])


def test_values_beyond_int64_survive_the_declared_precision(s3_client):
    # Preserving the declared decimal, rather than loosening it to int64, is
    # what keeps a NUMBER(38,0) key that genuinely exceeds int64 intact.
    import pyarrow as pa
    from decimal import Decimal

    big = Decimal("123456789012345678901234567890")
    declared = pa.schema([pa.field("BIG_ID", pa.decimal128(38, 0))])
    writer = LandingWriter(s3_client, BUCKET, "landing")
    run = writer.start("acme", "orders", "r-big", "ACME", "PUBLIC", "ORDERS",
                       ingest_date="2026-08-25")

    run.write_batch(pd.DataFrame({"BIG_ID": [big]}), declared_schema=declared)

    assert run.schema_drift is False
    landed = _read_parquet_from_s3(
        s3_client, "landing/acme/orders/ingest_date=2026-08-25/run_id=r-big/part-00000.parquet"
    )
    assert landed["BIG_ID"].iloc[0] == big
