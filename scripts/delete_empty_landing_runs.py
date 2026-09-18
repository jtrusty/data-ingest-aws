#!/usr/bin/env python3
"""
Delete landing runs that committed nothing.

A run is deleted only when BOTH are true: its prefix holds exactly one
object, `_manifest.json`, and that manifest reports row_count 0. A run with
any Parquet part, or any manifest claiming rows, is never touched -- so a
real backfill window cannot match even if it sits beside a thousand empties.

Dry run by default. Nothing is deleted without --delete.

    python scripts/delete_empty_landing_runs.py \
        --uri s3://<lake>/landing/par_pos_s3_json/orders/ingest_date=2026-09-18
    python scripts/delete_empty_landing_runs.py --uri s3://... --delete

Needs s3:ListBucket, s3:GetObject and (with --delete) s3:DeleteObject on the
landing prefix. The Glue roles deliberately lack DeleteObject; run this with
your own credentials. Unknown arguments are ignored so it also runs under
Glue as a Python Shell job if that is the only place with access.
"""

import argparse
import functools
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import boto3

# Under Glue, stdout is block-buffered and a long-running script that prints
# only at the end looks hung. Every print here flushes.
print = functools.partial(print, flush=True)


def parse_uri(uri):
    m = re.fullmatch(r"s3://([^/]+)/?(.*)", uri)
    if not m:
        sys.exit(f"not an s3:// URI: {uri}")
    return m.group(1), m.group(2).rstrip("/")


class Sweeper:
    """
    Decide runs as the listing streams past them, and delete as it goes.

    S3 lists in key order, so every object of one run_id is contiguous: the
    moment a key belongs to a different run, the previous run is complete
    and can be judged. Nothing about the listing is retained, so memory is
    flat regardless of how many runs there are -- the version that grouped
    the whole listing first was OOM-killed on 591k objects at 1/16 DPU.
    """

    def __init__(self, s3, bucket, delete, workers=16):
        self.s3, self.bucket, self.delete = s3, bucket, delete
        self.pool = ThreadPoolExecutor(max_workers=workers)
        self.candidates = []          # manifest keys awaiting a read
        self.runs = self.empty = self.kept = self.deleted = self.failed = self.flushes = 0
        self.examples = []

    def run_complete(self, keys):
        self.runs += 1
        if len(keys) == 1 and keys[0].endswith("/_manifest.json"):
            self.candidates.append(keys[0])
            if len(self.candidates) >= 1000:
                self.flush()
        else:
            self.kept += 1

    def _is_empty(self, key):
        manifest = json.loads(self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read())
        return key, manifest.get("row_count", 0) == 0 and manifest.get("file_count", 0) == 0

    def flush(self):
        """Read the buffered manifests in parallel; delete the empty ones."""
        if not self.candidates:
            return
        empties = []
        for key, ok in self.pool.map(self._is_empty, self.candidates):
            if ok:
                empties.append(key)
                if len(self.examples) < 5:
                    self.examples.append(key)
            else:
                self.kept += 1
        self.candidates = []
        self.empty += len(empties)
        if self.delete and empties:
            resp = self.s3.delete_objects(
                Bucket=self.bucket, Delete={"Objects": [{"Key": k} for k in empties], "Quiet": True})
            errors = resp.get("Errors", [])
            for err in errors:
                print("  FAILED", err["Key"], err["Code"], err["Message"])
            self.failed += len(errors)
            self.deleted += len(empties) - len(errors)
        self.flushes += 1
        if self.flushes % 10 == 0:
            print(f"  {self.runs:,} runs seen: {self.empty:,} empty, {self.kept:,} kept"
                  + (f", {self.deleted:,} deleted" if self.delete else ""))

    def close(self):
        self.flush()
        self.pool.shutdown()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--uri", required=True, help="prefix holding run_id=... folders (an ingest_date=, a table, or higher)")
    ap.add_argument("--delete", action="store_true", help="actually delete; default is a dry run")
    args, _unknown = ap.parse_known_args()

    bucket, prefix = parse_uri(args.uri)
    s3 = boto3.client("s3")
    sweeper = Sweeper(s3, bucket, args.delete)
    started = time.time()
    print(f"{'DELETING' if args.delete else 'Dry run:'} empty runs under s3://{bucket}/{prefix}")

    current, keys = None, []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix + "/"):
        for obj in page.get("Contents", []):
            m = re.search(r"^(.*/run_id=[^/]+)/", obj["Key"])
            if not m:
                continue
            if m.group(1) != current:
                if current is not None:
                    sweeper.run_complete(keys)
                current, keys = m.group(1), []
            keys.append(obj["Key"])
    if current is not None:
        sweeper.run_complete(keys)
    sweeper.close()

    print(f"\n{sweeper.runs:,} run(s): {sweeper.empty:,} empty, {sweeper.kept:,} kept "
          f"({time.time() - started:.0f}s)")
    for key in sweeper.examples:
        print("  ", key)
    if args.delete:
        print(f"Deleted {sweeper.deleted:,} empty run(s), {sweeper.failed} failed.")
    elif sweeper.empty:
        print("\nDry run. Re-run with --delete to remove them.")


if __name__ == "__main__":
    main()
