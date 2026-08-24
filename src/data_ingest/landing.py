"""
S3 landing layout, Parquet writes, and manifest commit.

LandingWriter.start() opens a LandingRun -- one brand-new, immutable
run_id=... prefix per run_table() call (see pipeline.py). LandingRun then
accumulates batches via write_batch() and, once extraction is fully done,
commits the run with write_manifest(). Nothing in this module knows what
Snowflake (or any other source) is -- it only deals in DataFrames, S3 keys,
and the manifest schema.
"""

import io
from datetime import datetime, timezone

# Import pandas before pyarrow. This module doesn't use pandas at import
# time; the import exists purely to fix the ordering, because pipeline.py
# reaches landing.py before anything else pulls pandas in.
#
# Why: pandas and pyarrow both bind to numpy's C API, and on some
# numpy/pandas combinations importing pyarrow.parquet first crashes the
# interpreter at shutdown (exit 139 after all work completes -- which Glue
# would report as a FAILED run and retry, duplicating a landing run that
# actually succeeded). Measured on CPython 3.9 with pyarrow 10.0.1:
#
#   numpy 1.22.3 + pandas 1.4.2  (Glue's own stack)  both orders OK
#   numpy 1.26.4 + pandas 1.5.3  (dev/CI venv)       pyarrow-first CRASHES
#
# The target runtime is safe either way, but the dev/CI stack is not, so
# ordering it here keeps every environment consistent. Note this only
# mitigates ordering -- a genuinely mismatched numpy/pandas build pair
# (e.g. numpy 1.23.5 with pandas 1.4.2) crashes regardless of order; see
# constraints-glue.txt, which exists to keep such a pair from ever being
# installed.
import pandas  # noqa: F401  -- must precede pyarrow; see comment above
import pyarrow as pa
import pyarrow.parquet as pq

from data_ingest.exceptions import LandingWriteError, ManifestCommitError
from data_ingest.logging import get_logger
from data_ingest.manifest import Manifest

logger = get_logger(__name__)

MANIFEST_FILENAME = "_manifest.json"

# Reserved lineage columns stamped onto every landed row. Kept as a module
# constant so both the writer and any downstream/test code agree on the set.
LINEAGE_COLUMNS = (
    "_ingest_run_id",
    "_ingested_at",
    "_source_system",
    "_source_database",
    "_source_schema",
    "_source_table",
)


def utc_now():
    return datetime.now(timezone.utc)


class LandingRun:
    """
    A single immutable extraction run: landing/<source>/<table>/
    ingest_date=YYYY-MM-DD/run_id=<uuid>/. Knows nothing about the source
    that produced the data -- it's handed plain DataFrames and lineage
    identifiers by the pipeline.
    """

    def __init__(self, s3_client, bucket, prefix, run_id, source_system, source_database, source_schema, source_table):
        self.s3 = s3_client
        self.bucket = bucket
        self.prefix = prefix.rstrip("/")
        self.run_id = run_id
        self.source_system = source_system
        self.source_database = source_database
        self.source_schema = source_schema
        self.source_table = source_table

        self.row_count = 0
        self.file_count = 0
        self.files = []
        self.started_at = utc_now().isoformat()
        # Pinned from the first non-empty batch and reused for every
        # subsequent one -- see _to_arrow().
        self.schema = None
        # Set if any batch failed to conform to the pinned schema; surfaced
        # in the manifest so downstream can detect it without opening files.
        self.schema_drift = False

    @property
    def landing_uri(self):
        return f"s3://{self.bucket}/{self.prefix}"

    def _add_lineage_columns(self, dataframe):
        """
        Stamp every row with lineage metadata (see "Ingestion metadata
        columns" in README.md). Uses the `_ingest_`/`_source_` reserved
        prefixes so these never collide with real source column names.
        Copies the DataFrame first so callers aren't surprised by their
        input being mutated.
        """
        # The reserved prefixes are a convention, not a guarantee -- a source
        # table really can have a column called `_source_table`. Assigning
        # over it would silently destroy source data, so refuse instead.
        collisions = [c for c in LINEAGE_COLUMNS if c in dataframe.columns]
        if collisions:
            raise LandingWriteError(
                f"Source {self.source_database}.{self.source_schema}.{self.source_table} "
                f"has column(s) {collisions} that collide with reserved ingestion "
                f"lineage columns. The `_ingest_` and `_source_` prefixes are reserved; "
                f"rename or project out the source column(s) before landing."
            )

        dataframe = dataframe.copy()
        ingested_at = utc_now().isoformat()
        dataframe["_ingest_run_id"] = self.run_id
        dataframe["_ingested_at"] = ingested_at
        dataframe["_source_system"] = self.source_system
        dataframe["_source_database"] = self.source_database
        dataframe["_source_schema"] = self.source_schema
        dataframe["_source_table"] = self.source_table
        return dataframe

    @staticmethod
    def _promote_null_fields(schema):
        """
        Replace `null`-typed fields with `string`.

        A column that happens to be entirely NULL in the first batch infers
        as Arrow `null`, which nothing else can be cast to -- so pinning that
        type would make every later batch containing real values unwritable.
        `string` is the safe floor: source values arrive from the DB-API as
        Python objects and any of them can be rendered as text.
        """
        fields = [
            pa.field(f.name, pa.string()) if pa.types.is_null(f.type) else f
            for f in schema
        ]
        return pa.schema(fields)

    def _to_arrow(self, dataframe):
        """
        Convert a batch to an Arrow table, holding the schema stable across
        the whole run.

        Without a pinned schema, pyarrow re-infers per batch from
        object-dtype columns: a column that is all-NULL in batch 0 and
        populated in batch 1 yields part-00000 with a `null`-typed column
        and part-00001 with `string`. Two files in one immutable run prefix
        with incompatible schemas break strict readers, and the prefix can
        never be rewritten to fix it.
        """
        if self.schema is None:
            inferred = pa.Table.from_pandas(dataframe, preserve_index=False).schema
            self.schema = self._promote_null_fields(inferred)

        try:
            return pa.Table.from_pandas(dataframe, schema=self.schema, preserve_index=False)
        except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as exc:
            # A later batch genuinely doesn't fit the pinned schema (a column
            # the source widened mid-extraction, a mixed-type object column).
            # Hard-failing here would be a poison pill: the run dies at batch
            # N, the checkpoint never advances, and every retry re-reads the
            # same window and dies the same way. Land the batch with its own
            # schema instead, and flag the drift so it's visible in the
            # manifest and the logs rather than silent.
            self.schema_drift = True
            logger.warning(
                "Schema drift within run %s at part-%05d: batch does not conform to the "
                "run's pinned schema (%s). Landing this batch with its own inferred "
                "schema; downstream readers must reconcile. Consider casting the "
                "offending source column explicitly.",
                self.run_id,
                self.file_count,
                exc,
            )
            return pa.Table.from_pandas(dataframe, preserve_index=False)

    def write_batch(self, dataframe):
        """
        Write one batch as a new Snappy-compressed Parquet part-file. Safe
        to call repeatedly per run -- each call gets the next
        part-NNNNN.parquet name from self.file_count. A None/empty
        DataFrame is a deliberate no-op (an empty incremental window
        shouldn't produce a zero-row part file).
        """
        if dataframe is None or dataframe.empty:
            return

        dataframe = self._add_lineage_columns(dataframe)

        key = f"{self.prefix}/part-{self.file_count:05d}.parquet"

        try:
            table = self._to_arrow(dataframe)

            # Buffer in memory rather than writing to local disk first --
            # Glue Python Shell's local disk is small/ephemeral, and these
            # batches are already bounded by fetch_size.
            buffer = io.BytesIO()
            pq.write_table(table, buffer, compression="snappy")
            buffer.seek(0)

            self.s3.upload_fileobj(
                buffer,
                self.bucket,
                key,
                ExtraArgs={"ContentType": "application/octet-stream"},
            )
        except Exception as exc:
            # Wrapped so callers can catch one framework-specific exception
            # type regardless of whether the underlying failure was a
            # pyarrow error or an S3 ClientError.
            raise LandingWriteError(f"Failed writing {key}: {exc}") from exc

        self.row_count += len(dataframe)
        self.file_count += 1
        self.files.append(f"s3://{self.bucket}/{key}")

        logger.info("Wrote %s rows -> s3://%s/%s", len(dataframe), self.bucket, key)

    def _schema_manifest(self):
        """
        The run's pinned Arrow schema as plain JSON-able dicts, or None if
        the run landed no rows (so no schema was ever established).
        """
        if self.schema is None:
            return None
        return [{"name": field.name, "type": str(field.type)} for field in self.schema]

    def write_manifest(self, source_metadata, primary_key, checkpoint_manifest, load_type):
        """
        Write _manifest.json -- the commit marker for this run. Must be
        called exactly once, after all write_batch() calls have succeeded.
        Until this succeeds, the run is incomplete and any downstream
        reader (Bronze loader, a human debugging) must ignore the prefix
        entirely; see "Manifest" in README.md.
        """
        manifest = Manifest(
            version=1,
            status="SUCCESS",
            run_id=self.run_id,
            source_system=self.source_system,
            source=source_metadata,
            primary_key=primary_key,
            checkpoint=checkpoint_manifest,
            load_type=load_type,
            started_at=self.started_at,
            completed_at=utc_now().isoformat(),
            row_count=self.row_count,
            file_count=self.file_count,
            files=self.files,
            # Recording the landed schema makes source-side schema drift
            # auditable after the fact: `SELECT *` means an added/dropped/
            # retyped Snowflake column silently changes the Parquet schema,
            # and without this the only way to find which run introduced a
            # change is to open the Parquet files by hand.
            schema=self._schema_manifest(),
            schema_drift=self.schema_drift,
        )

        key = f"{self.prefix}/{MANIFEST_FILENAME}"
        try:
            self.s3.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=manifest.to_json().encode("utf-8"),
                ContentType="application/json",
            )
        except Exception as exc:
            raise ManifestCommitError(f"Failed writing manifest {key}: {exc}") from exc

        logger.info("Committed manifest: s3://%s/%s", self.bucket, key)
        return manifest


class LandingWriter:
    """Factory for LandingRuns; the only thing in the framework that knows the landing S3 layout."""

    def __init__(self, s3_client, bucket, base_prefix):
        self.s3 = s3_client
        self.bucket = bucket
        self.base_prefix = base_prefix.rstrip("/")

    def start(self, source_system, table_name, run_id, source_database, source_schema, source_table, ingest_date=None):
        """
        Open a new run. `ingest_date` defaults to today (UTC) and only
        exists as a parameter so tests can pin it -- production callers
        should never pass it explicitly, since the point is "the date this
        run actually happened."
        """
        ingest_date = ingest_date or utc_now().date().isoformat()

        # landing/<source>/<table>/ingest_date=YYYY-MM-DD/run_id=<uuid>/
        # -- see "Core semantics" in README.md. Deliberately NOT partitioned
        # by load_type=full|incremental; that's manifest metadata, not a
        # physical path component.
        prefix = (
            f"{self.base_prefix}/{source_system}/{table_name}/"
            f"ingest_date={ingest_date}/run_id={run_id}"
        )

        return LandingRun(
            s3_client=self.s3,
            bucket=self.bucket,
            prefix=prefix,
            run_id=run_id,
            source_system=source_system,
            source_database=source_database,
            source_schema=source_schema,
            source_table=source_table,
        )
