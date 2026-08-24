"""
Finding landing runs that are safe to load.

The rule from the ingestion side is the contract here: a landing prefix
without a successful `_manifest.json` is INCOMPLETE and must be ignored.
Partial Parquet from a crashed or OOM-killed extraction sits in S3 forever
(the ingestion job deliberately does not clean up after itself), so
"directory exists" is never sufficient evidence that a run finished.
"""

import json
from dataclasses import dataclass
from typing import List, Optional

from data_ingest.exceptions import DataIngestError
from data_ingest.logging import get_logger

logger = get_logger(__name__)

MANIFEST_FILENAME = "_manifest.json"


@dataclass(frozen=True)
class LandingRun:
    """One committed landing run, as described by its manifest."""

    run_id: str
    ingest_date: str
    prefix: str          # key prefix within the bucket, no leading/trailing slash
    bucket: str
    manifest: dict

    @property
    def s3_location(self):
        """Trailing slash matters: Athena treats a partition LOCATION as a directory."""
        return f"s3://{self.bucket}/{self.prefix}/"

    @property
    def row_count(self):
        return self.manifest.get("row_count", 0)

    @property
    def load_type(self):
        return self.manifest.get("load_type")

    @property
    def is_empty(self):
        """A committed run that landed no rows -- valid, just nothing to merge."""
        return self.row_count == 0 or self.manifest.get("file_count", 0) == 0


def _parse_partition_values(prefix):
    """
    Pull ingest_date and run_id out of a landing prefix.

    Reads them from the Hive-style path segments rather than trusting the
    manifest body, because the partition we register with Athena has to match
    where the files physically are.
    """
    values = {}
    for segment in prefix.split("/"):
        if "=" in segment:
            key, _, value = segment.partition("=")
            values[key] = value
    return values.get("ingest_date"), values.get("run_id")


def discover_runs(s3_client, bucket, landing_prefix, source_key, table_name):
    """
    Every committed run for one table, oldest first.

    Ordered by ingest_date then run_id so merges apply in roughly the order
    they were extracted. Ordering is not required for correctness -- the
    merge is keyed on primary_key + watermark, so the same rows land the same
    way regardless -- but it keeps Iceberg's snapshot history readable.

    Runs still in flight, or abandoned partway, have no manifest yet and are
    skipped. A run that is mid-write when this lists it is simply picked up
    on the next pass.
    """
    table_root = f"{landing_prefix.rstrip('/')}/{source_key}/{table_name}"
    paginator = s3_client.get_paginator("list_objects_v2")

    runs = []
    skipped_without_manifest = set()

    # List only manifests. Listing every Parquet object would be enormously
    # more requests for a table with hundreds of runs.
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{table_root}/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(f"/{MANIFEST_FILENAME}"):
                continue

            prefix = key[: -len(f"/{MANIFEST_FILENAME}")]
            ingest_date, run_id = _parse_partition_values(prefix)
            if not ingest_date or not run_id:
                logger.warning(
                    "Skipping manifest at an unexpected path (no ingest_date/run_id): s3://%s/%s",
                    bucket, key,
                )
                continue

            body = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
            try:
                manifest = json.loads(body)
            except ValueError as exc:
                # A corrupt manifest is not the same as a missing one: the
                # run claimed to commit. Refuse rather than silently skip,
                # because skipping would silently drop data.
                raise DataIngestError(
                    f"Landing run has an unreadable manifest at s3://{bucket}/{key}: {exc}"
                ) from exc

            if manifest.get("status") != "SUCCESS":
                logger.warning(
                    "Skipping run %s: manifest status is %r, not SUCCESS",
                    run_id, manifest.get("status"),
                )
                skipped_without_manifest.add(run_id)
                continue

            runs.append(
                LandingRun(
                    run_id=run_id,
                    ingest_date=ingest_date,
                    prefix=prefix,
                    bucket=bucket,
                    manifest=manifest,
                )
            )

    runs.sort(key=lambda r: (r.ingest_date, r.run_id))
    logger.info(
        "Discovered %s committed run(s) for %s/%s under s3://%s/%s",
        len(runs), source_key, table_name, bucket, table_root,
    )
    return runs
