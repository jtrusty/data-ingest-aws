import json

import boto3
import pytest
from moto import mock_aws

from data_ingest.bronze.discovery import discover_runs
from data_ingest.bronze.loader import (
    bronze_table_name,
    landing_table_name,
    load_table_runs,
)
from data_ingest.bronze.state import NullProcessedRunStore, ProcessedRunStore
from data_ingest.config import CheckpointConfig, TableConfig
from data_ingest.exceptions import DataIngestError

BUCKET = "bronze-test-bucket"
RUNS_TABLE = "bronze-processed-runs-test"
SOURCE_KEY = "acme_snowflake"
TABLE = "order_fact"


def make_table_config():
    return TableConfig(
        name=TABLE,
        database="ACME",
        schema="REPORTING",
        table="ORDER_FACT_V",
        primary_key=["ORDER_KEY"],
        checkpoint=CheckpointConfig(type="watermark", column="LAST_UPDATE_DTTM"),
    )


class FakeGlue:
    """Catalog stand-in. `tables` maps name -> {column: type}; absent = not created."""

    class exceptions:
        class EntityNotFoundException(Exception):
            pass

    def __init__(self, tables=None):
        self.tables = tables or {}

    def get_table(self, DatabaseName, Name):
        if Name not in self.tables:
            raise self.exceptions.EntityNotFoundException(Name)
        return {
            "Table": {
                "StorageDescriptor": {
                    "Columns": [
                        {"Name": n, "Type": t} for n, t in self.tables[Name].items()
                    ]
                }
            }
        }


class FakeAthena:
    """Records statements instead of running them."""

    def __init__(self, fail_on=None):
        self.statements = []
        self.fail_on = fail_on  # substring that triggers a failure

    def execute(self, sql, description=None):
        self.statements.append(sql)
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError("athena boom")
        return "qid-%d" % len(self.statements)

    @property
    def merges(self):
        return [s for s in self.statements if s.startswith("MERGE INTO")]


def _write_run(s3, run_id, ingest_date="2026-08-24", status="SUCCESS",
               row_count=100, file_count=1, with_manifest=True):
    prefix = f"landing/{SOURCE_KEY}/{TABLE}/ingest_date={ingest_date}/run_id={run_id}"
    s3.put_object(Bucket=BUCKET, Key=f"{prefix}/part-00000.parquet", Body=b"parquet-bytes")
    if with_manifest:
        s3.put_object(
            Bucket=BUCKET,
            Key=f"{prefix}/_manifest.json",
            Body=json.dumps({
                "status": status,
                "run_id": run_id,
                "row_count": row_count,
                "file_count": file_count,
                "load_type": "incremental",
                # The landing writer records the Arrow schema it actually
                # wrote; Bronze derives its CREATE TABLE from this rather
                # than from hand-maintained DDL that could drift.
                "schema": [
                    {"name": "ORDER_KEY", "type": "int64"},
                    {"name": "AMOUNT", "type": "decimal128(38, 3)"},
                    {"name": "LAST_UPDATE_DTTM", "type": "timestamp[ns]"},
                ],
            }).encode(),
        )
    return prefix


@pytest.fixture
def env():
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=BUCKET)

        boto3.client("dynamodb", region_name="us-east-1").create_table(
            TableName=RUNS_TABLE,
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
        store = ProcessedRunStore(
            boto3.resource("dynamodb", region_name="us-east-1").Table(RUNS_TABLE)
        )
        yield s3, store


def _load(s3, store, athena, glue=None):
    return load_table_runs(
        athena=athena, s3_client=s3, processed_runs=store,
        bucket=BUCKET, landing_prefix="landing",
        source_key=SOURCE_KEY, table_config=make_table_config(),
        bronze_location="s3://bronze-bucket/bronze",
        partition_by=("month({checkpoint_column})",),
        glue_client=glue if glue is not None else FakeGlue(),
        database="bronze_db",
    )


# --------------------------------------------------------------------------
# The manifest is the commit marker
# --------------------------------------------------------------------------


def test_run_without_a_manifest_is_ignored(env):
    """
    A crashed or OOM-killed extraction leaves Parquet with no manifest. The
    ingestion job deliberately does not clean it up, so Bronze must be the
    thing that refuses to read it -- otherwise a partial extraction silently
    becomes real data.
    """
    s3, store = env
    _write_run(s3, "committed-run")
    _write_run(s3, "orphaned-run", with_manifest=False)

    athena = FakeAthena()
    result = _load(s3, store, athena)

    merged = [r.run_id for r in result.runs if r.status == "MERGED"]
    assert merged == ["committed-run"]
    assert not any("orphaned-run" in sql for sql in athena.statements)


def test_run_with_a_non_success_manifest_is_ignored(env):
    s3, store = env
    _write_run(s3, "bad-run", status="ABANDONED")

    result = _load(s3, store, FakeAthena())
    assert result.runs == []


def test_corrupt_manifest_raises_rather_than_silently_skipping(env):
    # A corrupt manifest is not the same as a missing one -- the run claimed
    # to commit, so skipping it would silently drop data.
    s3, store = env
    prefix = f"landing/{SOURCE_KEY}/{TABLE}/ingest_date=2026-08-24/run_id=x"
    s3.put_object(Bucket=BUCKET, Key=f"{prefix}/_manifest.json", Body=b"{not json")

    with pytest.raises(DataIngestError, match="unreadable manifest"):
        discover_runs(s3, BUCKET, "landing", SOURCE_KEY, TABLE)


# --------------------------------------------------------------------------
# Idempotency
# --------------------------------------------------------------------------


def _existing_tables():
    """A catalog where both tables already exist, as on any re-run."""
    from data_ingest.bronze.loader import bronze_table_name, landing_table_name
    columns = {"order_key": "bigint", "amount": "decimal(38,3)", "last_update_dttm": "timestamp"}
    return FakeGlue({bronze_table_name(SOURCE_KEY, TABLE): dict(columns),
                     landing_table_name(SOURCE_KEY, TABLE): dict(columns)})


def test_reprocessing_skips_already_merged_runs(env):
    s3, store = env
    _write_run(s3, "run-1")

    first = _load(s3, store, FakeAthena())
    assert [r.status for r in first.runs] == ["MERGED"]

    # The table exists on the second pass, as it does on any re-run -- which
    # is what makes the bookkeeping trustworthy.
    second_athena = FakeAthena()
    second = _load(s3, store, second_athena, glue=_existing_tables())

    assert [r.status for r in second.runs] == ["SKIPPED_ALREADY_PROCESSED"]
    assert second_athena.merges == [], "an already-merged run must not be re-scanned"


def test_a_recreated_bronze_table_ignores_stale_processed_runs(env):
    """
    Dropping and recreating a Bronze table is legitimate -- relocating it,
    changing its layout -- and the processed-runs bookkeeping is the one
    thing that does not survive it. Trusting it after a recreate skips those
    runs into an EMPTY table and still reports SUCCESS: data in landing,
    absent from Bronze, nothing failing. Observed in production at 229 of
    234 runs skipped into a table that had just been recreated.
    """
    s3, store = env
    _write_run(s3, "run-1")
    _write_run(s3, "run-2", ingest_date="2026-08-25")

    first = _load(s3, store, FakeAthena(), glue=_existing_tables())
    assert [r.status for r in first.runs] == ["MERGED", "MERGED"]
    assert store.processed_run_ids(SOURCE_KEY, TABLE) == {"run-1", "run-2"}

    # Table dropped: the next pass creates it, so it is empty regardless of
    # what the store says. Every run must be merged again.
    athena = FakeAthena()
    second = _load(s3, store, athena, glue=FakeGlue())

    assert [r.status for r in second.runs] == ["MERGED", "MERGED"]
    assert len(athena.merges) == 2


def test_a_crash_after_merge_before_recording_is_safe(env):
    """
    The loader's fail-safe property. If the process dies between a successful
    MERGE and recording the run, the next pass re-merges it -- which inserts
    nothing, because the merge matches on primary_key + watermark. So the
    window between the two steps is harmless, exactly like the ingestion
    side's window between manifest and checkpoint.
    """
    s3, store = env
    _write_run(s3, "run-1")

    # Merge succeeds, but we never record it (simulating a crash).
    athena = FakeAthena()
    athena.execute("MERGE INTO x", description="simulated prior merge")

    result = _load(s3, store, FakeAthena())

    # Re-merged rather than skipped -- and that is fine.
    assert [r.status for r in result.runs] == ["MERGED"]


def test_merge_failure_leaves_the_run_unrecorded_for_retry(env):
    s3, store = env
    _write_run(s3, "run-1")

    with pytest.raises(RuntimeError):
        _load(s3, store, FakeAthena(fail_on="MERGE INTO"))

    # Not recorded, so the next pass retries it.
    assert store.processed_run_ids(SOURCE_KEY, TABLE) == set()


def test_failure_does_not_record_later_runs(env):
    """
    Ordering guarantee: a failure must not leave a gap with processed runs
    on both sides of it.
    """
    s3, store = env
    _write_run(s3, "run-a", ingest_date="2026-08-01")
    _write_run(s3, "run-b", ingest_date="2026-08-02")
    _write_run(s3, "run-c", ingest_date="2026-08-03")

    # Fail on the second run's merge specifically.
    class FailSecond(FakeAthena):
        def execute(self, sql, description=None):
            self.statements.append(sql)
            if sql.startswith("MERGE INTO") and "run-b" in sql:
                raise RuntimeError("boom")
            return "qid"

    with pytest.raises(RuntimeError):
        _load(s3, store, FailSecond())

    processed = store.processed_run_ids(SOURCE_KEY, TABLE)
    assert processed == {"run-a"}, "only runs before the failure may be recorded"


# --------------------------------------------------------------------------
# Ordering, empties, naming
# --------------------------------------------------------------------------


def test_runs_are_merged_oldest_first(env):
    s3, store = env
    _write_run(s3, "run-c", ingest_date="2026-08-03")
    _write_run(s3, "run-a", ingest_date="2026-08-01")
    _write_run(s3, "run-b", ingest_date="2026-08-02")

    athena = FakeAthena()
    _load(s3, store, athena)

    order = [r.run_id for r in _load(s3, store, FakeAthena()).runs]
    assert order == ["run-a", "run-b", "run-c"]


def test_empty_run_is_recorded_without_merging(env):
    # A committed run with zero rows is valid -- an incremental window with
    # no changes. Recording it without a merge avoids re-examining it forever.
    s3, store = env
    _write_run(s3, "empty-run", row_count=0, file_count=0)

    athena = FakeAthena()
    result = _load(s3, store, athena)

    assert [r.status for r in result.runs] == ["SKIPPED_EMPTY"]
    assert athena.merges == []
    assert store.processed_run_ids(SOURCE_KEY, TABLE) == {"empty-run"}


def test_null_store_re_merges_every_run(env):
    s3, store = env
    _write_run(s3, "run-1")

    athena = FakeAthena()
    _load(s3, NullProcessedRunStore(), athena)
    assert len(athena.merges) == 1

    again = FakeAthena()
    _load(s3, NullProcessedRunStore(), again)
    assert len(again.merges) == 1, "correct, just repeated -- the merge is idempotent"


def test_table_names_are_namespaced_by_source():
    # Two sources with a same-named table must not collide in one Bronze
    # database -- the same reasoning that put source_key in the landing path.
    assert bronze_table_name("acme_snowflake", "orders") == "acme_snowflake_orders"
    assert bronze_table_name("acme_rest", "orders") == "acme_rest_orders"
    assert landing_table_name("acme_snowflake", "orders") == "landing_acme_snowflake_orders"


def test_partition_is_registered_before_the_merge(env):
    # Merging before registering the partition would read zero rows and
    # silently record the run as processed.
    s3, store = env
    _write_run(s3, "run-1")

    athena = FakeAthena()
    _load(s3, store, athena)

    kinds = [s.split()[0] for s in athena.statements]
    assert kinds == ["CREATE", "CREATE", "ALTER", "MERGE"], (
        "tables must be created before the partition is registered, and the "
        "partition before the merge"
    )


# --------------------------------------------------------------------------
# Schema evolution, end to end
# --------------------------------------------------------------------------


def test_a_column_added_in_the_source_reaches_bronze(env):
    """
    The silent-drop bug, end to end. CREATE TABLE IF NOT EXISTS is a no-op
    once the tables exist, so without evolution a column added in Snowflake
    lands in Parquet and is then invisible to Athena forever -- present in
    S3, absent from Bronze, and no error anywhere.
    """
    s3, store = env
    _write_run(s3, "run-with-new-column")

    # Both tables already exist, with the OLD column set.
    old_columns = {
        "ORDER_KEY": "bigint",
        "AMOUNT": "decimal(38,3)",
        "LAST_UPDATE_DTTM": "timestamp",
    }
    glue = FakeGlue({
        "acme_snowflake_order_fact": old_columns,
        "landing_acme_snowflake_order_fact": old_columns,
    })

    # The run's manifest carries a column the tables do not have yet.
    s3.put_object(
        Bucket=BUCKET,
        Key=f"landing/{SOURCE_KEY}/{TABLE}/ingest_date=2026-08-24/run_id=run-with-new-column/_manifest.json",
        Body=json.dumps({
            "status": "SUCCESS", "run_id": "run-with-new-column",
            "row_count": 10, "file_count": 1, "load_type": "incremental",
            "schema": [
                {"name": "ORDER_KEY", "type": "int64"},
                {"name": "AMOUNT", "type": "decimal128(38, 3)"},
                {"name": "LAST_UPDATE_DTTM", "type": "timestamp[ns]"},
                {"name": "PROMO_CODE", "type": "string"},   # <- new
            ],
        }).encode(),
    )

    athena = FakeAthena()
    _load(s3, store, athena, glue=glue)

    alters = [s for s in athena.statements if s.startswith("ALTER TABLE") and "ADD COLUMNS" in s]
    assert len(alters) == 2, "both the landing external table AND bronze must gain it"
    assert all("`promo_code` string" in a for a in alters)

    # And the tables are NOT recreated -- they already exist.
    assert not any(s.startswith("CREATE") for s in athena.statements)


def test_an_unchanged_schema_adds_no_ddl(env):
    # Evolution runs on every load, so a steady-state run must not accumulate
    # pointless DDL statements.
    s3, store = env
    _write_run(s3, "run-1")

    existing = {
        "ORDER_KEY": "bigint",
        "AMOUNT": "decimal(38,3)",
        "LAST_UPDATE_DTTM": "timestamp",
    }
    glue = FakeGlue({
        "acme_snowflake_order_fact": existing,
        "landing_acme_snowflake_order_fact": existing,
    })

    athena = FakeAthena()
    _load(s3, store, athena, glue=glue)

    kinds = [s.split()[0] for s in athena.statements]
    assert kinds == ["ALTER", "MERGE"], "only the partition add and the merge"
    assert "ADD COLUMNS" not in " ".join(athena.statements)


def test_a_source_type_change_stops_the_load(env):
    """
    Fails before merging rather than after. A merge against a mismatched
    column type could truncate silently, and Bronze is append-only -- there
    is no correcting it afterwards.
    """
    from data_ingest.bronze.schema import SchemaChangeError

    s3, store = env
    _write_run(s3, "run-1")

    narrowed = {
        "ORDER_KEY": "bigint",
        "AMOUNT": "decimal(10,2)",          # source now declares (38,3)
        "LAST_UPDATE_DTTM": "timestamp",
    }
    glue = FakeGlue({
        "acme_snowflake_order_fact": narrowed,
        "landing_acme_snowflake_order_fact": narrowed,
    })

    athena = FakeAthena()
    with pytest.raises(SchemaChangeError, match="AMOUNT"):
        _load(s3, store, athena, glue=glue)

    assert not any(s.startswith("MERGE") for s in athena.statements)
    assert store.processed_run_ids(SOURCE_KEY, TABLE) == set(), "run stays retryable"


def test_a_drifted_run_is_refused_before_any_athena_work(env):
    """
    A run whose files disagree with each other cannot be described by one
    table schema. Athena accepts the CREATE and then fails at READ time,
    blaming the file -- far from the extraction that produced it. Refusing up
    front puts the error next to the cause.
    """
    s3, store = env
    prefix = f"landing/{SOURCE_KEY}/{TABLE}/ingest_date=2026-08-25/run_id=drifted"
    s3.put_object(Bucket=BUCKET, Key=f"{prefix}/part-00000.parquet", Body=b"x")
    s3.put_object(
        Bucket=BUCKET, Key=f"{prefix}/_manifest.json",
        Body=json.dumps({
            "status": "SUCCESS", "run_id": "drifted", "row_count": 5,
            "file_count": 2, "load_type": "full",
            "schema_drift": True, "schema": None,
        }).encode(),
    )

    athena = FakeAthena()
    with pytest.raises(DataIngestError, match="schema_drift"):
        _load(s3, store, athena)

    assert athena.statements == [], "nothing may run against inconsistent files"


def test_tables_are_created_from_the_newest_run_schema(env):
    """
    The newest run reflects the current source shape. Creating from the
    oldest would omit every column added since and leave evolution to
    backfill what should have been right at creation.
    """
    s3, store = env
    _write_run(s3, "old-run", ingest_date="2026-08-01")

    newest = f"landing/{SOURCE_KEY}/{TABLE}/ingest_date=2026-08-25/run_id=new-run"
    s3.put_object(Bucket=BUCKET, Key=f"{newest}/part-00000.parquet", Body=b"x")
    s3.put_object(
        Bucket=BUCKET, Key=f"{newest}/_manifest.json",
        Body=json.dumps({
            "status": "SUCCESS", "run_id": "new-run", "row_count": 1,
            "file_count": 1, "load_type": "incremental",
            "schema": [
                {"name": "ORDER_KEY", "type": "int64"},
                {"name": "AMOUNT", "type": "decimal128(38, 3)"},
                {"name": "LAST_UPDATE_DTTM", "type": "timestamp[ns]"},
                {"name": "ADDED_LATER", "type": "string"},
            ],
        }).encode(),
    )

    athena = FakeAthena()
    _load(s3, store, athena)

    creates = [s for s in athena.statements if s.startswith("CREATE")]
    assert creates, "tables should have been created"
    assert all("`added_later` string" in c for c in creates), (
        "the column only the newest run has must be present at creation"
    )


# --------------------------------------------------------------------------
# Runs that disagree about their columns
# --------------------------------------------------------------------------

BASE_SCHEMA = [
    {"name": "ORDER_KEY", "type": "int64"},
    {"name": "LAST_UPDATE_DTTM", "type": "timestamp[ns]"},
]


def _write_run_with_schema(s3, run_id, schema, ingest_date="2026-08-24"):
    prefix = f"landing/{SOURCE_KEY}/{TABLE}/ingest_date={ingest_date}/run_id={run_id}"
    s3.put_object(Bucket=BUCKET, Key=f"{prefix}/part-00000.parquet", Body=b"x")
    s3.put_object(
        Bucket=BUCKET, Key=f"{prefix}/_manifest.json",
        Body=json.dumps({
            "status": "SUCCESS", "run_id": run_id, "row_count": 1,
            "file_count": 1, "load_type": "incremental", "schema": schema,
        }).encode(),
    )


def test_tables_declare_every_column_any_pending_run_has(env):
    """
    Defining the tables from one run breaks the others. Too narrow and the
    merge's `SELECT *` cannot resolve source.<col> for a run that has it --
    Athena fails with "cannot find source column". The union is the only
    column list that works for all of them.
    """
    s3, store = env
    _write_run_with_schema(s3, "run-a", BASE_SCHEMA + [{"name": "OLD_COL", "type": "string"}],
                           ingest_date="2026-08-01")
    _write_run_with_schema(s3, "run-b", BASE_SCHEMA + [{"name": "NEW_COL", "type": "string"}],
                           ingest_date="2026-08-25")

    athena = FakeAthena()
    _load(s3, store, athena)

    creates = [s for s in athena.statements if s.startswith("CREATE")]
    assert len(creates) == 2, "landing external table and bronze table"
    for statement in creates:
        assert "`old_col` string" in statement, "the dropped column must stay declared"
        assert "`new_col` string" in statement, "the added column must be declared"


def test_a_run_missing_a_column_simply_does_not_insert_it(env):
    """
    NULL is the honest value: that run genuinely carried nothing for the
    column. Naming it in the INSERT list would require inventing a value.
    """
    s3, store = env
    _write_run_with_schema(s3, "run-old", BASE_SCHEMA, ingest_date="2026-08-01")
    _write_run_with_schema(s3, "run-new", BASE_SCHEMA + [{"name": "NEW_COL", "type": "string"}],
                           ingest_date="2026-08-25")

    athena = FakeAthena()
    _load(s3, store, athena)

    old_merge, new_merge = athena.merges
    assert "new_col" not in old_merge, "the run that lacks it must leave it NULL"
    assert '"new_col"' in new_merge, "the run that has it must insert it"


def test_a_renamed_column_lands_as_a_new_column(env):
    """
    At the schema level a rename is indistinguishable from a drop plus an
    add, and guessing rewrites history in a way nothing downstream flags. So
    both names exist: rows before the rename carry the old one, rows after
    carry the new one, and each reads NULL for the other.
    """
    s3, store = env
    _write_run_with_schema(s3, "before", BASE_SCHEMA + [{"name": "CUST_ID", "type": "string"}],
                           ingest_date="2026-08-01")
    _write_run_with_schema(s3, "after", BASE_SCHEMA + [{"name": "CUSTOMER_ID", "type": "string"}],
                           ingest_date="2026-08-25")

    athena = FakeAthena()
    _load(s3, store, athena)

    create = next(s for s in athena.statements if s.startswith("CREATE"))
    assert "`cust_id` string" in create and "`customer_id` string" in create

    before_merge, after_merge = athena.merges
    assert '"cust_id"' in before_merge and "customer_id" not in before_merge
    assert '"customer_id"' in after_merge and "cust_id" not in after_merge


def test_runs_that_disagree_on_a_column_type_are_refused(env):
    """
    One Athena table cannot describe both, and picking one silently loses
    precision on the other -- the read-time HIVE_BAD_DATA this whole guard
    exists to pre-empt.
    """
    s3, store = env
    _write_run_with_schema(s3, "narrow", BASE_SCHEMA + [{"name": "AMOUNT", "type": "decimal128(10, 2)"}],
                           ingest_date="2026-08-01")
    _write_run_with_schema(s3, "wide", BASE_SCHEMA + [{"name": "AMOUNT", "type": "decimal128(38, 3)"}],
                           ingest_date="2026-08-25")

    athena = FakeAthena()
    with pytest.raises(DataIngestError, match="disagree on the type"):
        _load(s3, store, athena)

    assert athena.statements == [], "nothing may run against an undecidable schema"


def test_a_run_missing_its_match_columns_is_refused(env):
    """
    The ON clause would still parse -- the landing table declares the union
    -- but compare against NULL, which never matches. Every row would be
    re-inserted on every pass, duplicating Bronze silently.
    """
    s3, store = env
    _write_run_with_schema(s3, "no-pk", [{"name": "LAST_UPDATE_DTTM", "type": "timestamp[ns]"},
                                         {"name": "AMOUNT", "type": "int64"}])

    athena = FakeAthena()
    with pytest.raises(DataIngestError, match="deduplicates on"):
        _load(s3, store, athena)

    assert athena.merges == [], "no merge may be issued"


# --------------------------------------------------------------------------
# bronze.table_prefix
# --------------------------------------------------------------------------


def _load_with_prefix(s3, store, athena, table_prefix, glue=None):
    return load_table_runs(
        athena=athena, s3_client=s3, processed_runs=store,
        bucket=BUCKET, landing_prefix="landing",
        source_key=SOURCE_KEY, table_config=make_table_config(),
        bronze_location="s3://bronze-bucket/bronze",
        partition_by=(),
        glue_client=glue if glue is not None else FakeGlue(),
        database="bronze_db",
        table_prefix=table_prefix,
    )


def test_the_source_key_prefix_is_the_default():
    # Two sources must be able to share one Glue database without colliding.
    assert bronze_table_name("acme_snowflake", "order_fact") == \
        "acme_snowflake_order_fact"
    assert landing_table_name("acme_snowflake", "order_fact") == \
        "landing_acme_snowflake_order_fact"


def test_the_prefix_can_be_dropped_for_a_single_source_database():
    """
    With a Glue database per source the prefix is redundant -- the schema
    already says which source it is, so bronze_olo.olo_snowflake_order_fact
    repeats itself in every query an analyst writes.
    """
    assert bronze_table_name("acme_snowflake", "order_fact", "none") == "order_fact"


def test_the_landing_prefix_survives_dropping_the_source_key():
    # `landing_` is what keeps the external table distinct from the Bronze
    # table of the same name in the same database; only the source key is
    # optional.
    assert landing_table_name("acme_snowflake", "order_fact", "none") == \
        "landing_order_fact"


def test_dropping_the_prefix_creates_unprefixed_tables(env):
    s3, store = env
    _write_run(s3, "run-1")

    athena = FakeAthena()
    _load_with_prefix(s3, store, athena, "none")

    creates = [s for s in athena.statements if s.startswith("CREATE")]
    assert any("`order_fact`" in c for c in creates), "bronze table, unprefixed"
    assert any("`landing_order_fact`" in c for c in creates), "landing table"
    # The source key still appears in the landing LOCATION -- it is the S3
    # layout, which is unaffected. Only the table NAMES lose it.
    assert not any(f"`{SOURCE_KEY}" in c for c in creates)


def test_changing_the_prefix_after_tables_exist_is_refused(env):
    """
    table_prefix is identity, not presentation. The loader cannot rename, so
    proceeding would CREATE a second table beside the first and strand
    everything already merged under a name nothing queries -- present in S3,
    absent from every query, nothing failing.
    """
    s3, store = env
    _write_run(s3, "run-1")

    # The table already exists under the OLD (prefixed) name.
    glue = FakeGlue({f"{SOURCE_KEY}_{TABLE}": {"ORDER_KEY": "bigint"}})
    athena = FakeAthena()

    with pytest.raises(DataIngestError, match="table_prefix"):
        _load_with_prefix(s3, store, athena, "none", glue=glue)

    assert athena.statements == [], "nothing may run before a human decides"


def test_the_reverse_prefix_change_is_refused_too(env):
    # Going from "none" back to "source_key" strands data just as thoroughly.
    s3, store = env
    _write_run(s3, "run-1")

    glue = FakeGlue({TABLE: {"ORDER_KEY": "bigint"}})
    athena = FakeAthena()

    with pytest.raises(DataIngestError, match="already exists"):
        _load_with_prefix(s3, store, athena, "source_key", glue=glue)


def test_a_first_run_chooses_freely(env):
    # Neither name exists, which is exactly when the setting is meant to be
    # decided. It must not be mistaken for a change.
    s3, store = env
    _write_run(s3, "run-1")

    athena = FakeAthena()
    result = _load_with_prefix(s3, store, athena, "none", glue=FakeGlue())

    assert result.status == "SUCCESS"
    assert athena.merges, "the merge must actually run"


def test_discovery_warns_when_most_committed_runs_are_empty():
    """
    Hundreds of empty runs means the landing job is spinning -- a window loop
    that never decides it is caught up. Bronze is where that becomes visible
    as a count, so it says so rather than quietly recording them all.
    """
    import json
    from unittest.mock import MagicMock, patch
    from data_ingest.bronze import discovery
    from data_ingest.bronze.discovery import discover_runs

    s3 = MagicMock()
    keys = [f"landing/src/t/ingest_date=2026-09-17/run_id=r{i:04d}/_manifest.json" for i in range(150)]
    s3.get_paginator.return_value.paginate.return_value = [
        {"Contents": [{"Key": k} for k in keys]}]
    empty = json.dumps({"status": "SUCCESS", "row_count": 0, "file_count": 0,
                        "schema": [{"name": "a", "type": "string"}]}).encode()
    s3.get_object.return_value = {"Body": MagicMock(read=lambda: empty)}

    # Patch the module logger: an earlier test may have configured the
    # package logger with propagate=False, which hides records from caplog.
    with patch.object(discovery, "logger") as log:
        runs = discover_runs(s3, "bucket", "landing", "src", "t")
    assert len(runs) == 150
    warned = [c.args for c in log.warning.call_args_list if "EMPTY" in c.args[0]]
    assert warned and warned[0][1:3] == (150, 150)


# --------------------------------------------------------------------------
# Consumer-facing catalog types survive Athena rewriting the catalog
# --------------------------------------------------------------------------

class AthenaRewritingGlue(FakeGlue):
    """
    Models what Athena actually does: every Iceberg commit -- CREATE, ALTER,
    and MERGE alike -- rewrites the Glue entry's columns from the Iceberg
    schema. So any consumer-facing type the loader sets is gone after the
    next merge, unless the loader puts it back.
    """

    def __init__(self, athena, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.athena = athena
        self.updates = []
        self.iceberg_schema = {}
        athena.on_execute = self._athena_committed

    def _athena_committed(self, sql):
        # A CREATE learns the Iceberg schema from the statement's columns; a
        # MERGE (any later commit) rewrites the catalog from that schema.
        if sql.startswith("CREATE TABLE") and self.athena.bronze_table in sql.split("(")[0]:
            self.iceberg_schema = dict(self.athena.columns)
            self.tables[self.athena.bronze_table] = dict(self.iceberg_schema)
        elif sql.startswith("MERGE INTO"):
            table = sql.split('"')[1]
            self.tables[table] = dict(self.iceberg_schema)

    def get_table(self, DatabaseName, Name):
        out = super().get_table(DatabaseName, Name)
        out["Table"].update({"Name": Name, "TableType": "EXTERNAL_TABLE", "VersionId": "1",
                             "Parameters": {"metadata_location": "s3://x/metadata/00001.metadata.json"}})
        return out

    def update_table(self, DatabaseName, TableInput, **kwargs):
        self.updates.append(TableInput)
        self.tables[TableInput["Name"]] = {
            c["Name"]: c["Type"] for c in TableInput["StorageDescriptor"]["Columns"]}


class ObservableAthena(FakeAthena):
    def __init__(self, bronze_table, columns):
        super().__init__()
        self.bronze_table, self.columns, self.on_execute = bronze_table, columns, None

    def execute(self, sql, description=None):
        out = super().execute(sql, description)
        if self.on_execute:
            self.on_execute(sql)
        return out


def test_declared_catalog_types_are_reasserted_after_every_merge(env):
    """
    Observed on the first production run: the override was applied before
    merging and the columns were `string` again afterwards. Athena rewrites
    the catalog on every commit, so the loader re-applies after each merge.
    A consumer reading the catalog therefore sees `super` except during the
    seconds between a commit and the re-apply.
    """
    from data_ingest.bronze.loader import bronze_table_name
    s3, store = env
    schema = [{"name": "ORDER_KEY", "type": "int64"},
              {"name": "PAYLOAD_JSON", "type": "string"},
              {"name": "LAST_UPDATE_DTTM", "type": "timestamp[ns]"}]
    for i in range(3):
        _write_run_with_schema(s3, f"run-{i}", schema, ingest_date=f"2026-08-2{4 + i}")

    # Catalog types as Athena would record them for that schema.
    catalog = {"order_key": "bigint", "payload_json": "string", "last_update_dttm": "timestamp"}
    bronze_table = bronze_table_name(SOURCE_KEY, TABLE)
    athena = ObservableAthena(bronze_table, catalog)
    glue = AthenaRewritingGlue(athena)
    # The landing external table already exists; only the bronze table is
    # created and merged into here.
    glue.tables[f"landing_{bronze_table}"] = dict(catalog)

    result = load_table_runs(
        athena=athena, s3_client=s3, processed_runs=store,
        bucket=BUCKET, landing_prefix="landing",
        source_key=SOURCE_KEY, table_config=make_table_config(),
        bronze_location="s3://bronze-bucket/bronze",
        partition_by=("month({checkpoint_column})",),
        glue_client=glue, database="bronze_db",
        catalog_column_types={"payload_json": "super"},
    )

    assert result.merged_count == 3
    # After the final merge -- which reset the column -- it is super again.
    assert glue.tables[bronze_table]["payload_json"] == "super"
    # One re-apply after the create, then one after EACH merge that reset it.
    assert len(glue.updates) == 1 + 3
