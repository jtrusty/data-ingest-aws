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
from collections import defaultdict
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


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--uri", required=True, help="prefix holding run_id=... folders (an ingest_date=, a table, or higher)")
    ap.add_argument("--delete", action="store_true", help="actually delete; default is a dry run")
    args, _unknown = ap.parse_known_args()

    bucket, prefix = parse_uri(args.uri)
    s3 = boto3.client("s3")

    # One listing; group every object by its run_id folder.
    started = time.time()
    runs = defaultdict(list)
    listed = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix + "/"):
        for obj in page.get("Contents", []):
            listed += 1
            m = re.search(r"^(.*/run_id=[^/]+)/", obj["Key"])
            if m:
                runs[m.group(1)].append(obj["Key"])
        if listed % 10_000 < len(page.get("Contents", [])):
            print(f"  listed {listed:,} objects, {len(runs):,} runs so far ...")
    print(f"Listed {listed:,} objects in {len(runs):,} run(s) under s3://{bucket}/{prefix} "
          f"({time.time() - started:.0f}s)")

    # Only manifest-only runs need their manifest read; runs with parts are
    # kept without a request. Reads are pure I/O, so they go wide.
    candidates = [keys[0] for keys in runs.values()
                  if len(keys) == 1 and keys[0].endswith("/_manifest.json")]
    print(f"{len(candidates):,} run(s) hold only a manifest; reading those to confirm they are empty ...")

    def is_empty(key):
        manifest = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
        return key, manifest.get("row_count", 0) == 0 and manifest.get("file_count", 0) == 0

    empty = []
    with ThreadPoolExecutor(max_workers=16) as pool:
        for i, (key, ok) in enumerate(pool.map(is_empty, candidates), 1):
            if ok:
                empty.append(key)
            if i % 1000 == 0:
                print(f"  checked {i:,}/{len(candidates):,} manifests, {len(empty):,} empty ...")
    empty.sort()
    kept = len(runs) - len(empty)

    print(f"{len(runs):,} run(s): {len(empty):,} empty, {kept:,} kept ({time.time() - started:.0f}s)")
    if not empty:
        return
    for key in empty[:5]:
        print("  ", key)
    if len(empty) > 5:
        print(f"   ... and {len(empty) - 5} more")

    if not args.delete:
        print("\nDry run. Re-run with --delete to remove the empty runs listed above.")
        return

    # Each empty run is exactly one object, so deleting the manifests IS
    # deleting the runs. Batched 1000 at a time, the API's limit.
    failed = 0
    for i in range(0, len(empty), 1000):
        chunk = [{"Key": k} for k in empty[i:i + 1000]]
        resp = s3.delete_objects(Bucket=bucket, Delete={"Objects": chunk, "Quiet": True})
        for err in resp.get("Errors", []):
            failed += 1
            print("  FAILED", err["Key"], err["Code"], err["Message"])
        print(f"  deleted {min(i + 1000, len(empty)):,}/{len(empty):,} ...")
    print(f"Deleted {len(empty) - failed:,} empty run(s), {failed} failed.")


if __name__ == "__main__":
    main()
