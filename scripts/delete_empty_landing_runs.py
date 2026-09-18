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
import json
import re
import sys
from collections import defaultdict

import boto3


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
    runs = defaultdict(list)
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix + "/"):
        for obj in page.get("Contents", []):
            m = re.search(r"^(.*/run_id=[^/]+)/", obj["Key"])
            if m:
                runs[m.group(1)].append(obj["Key"])

    empty, kept = [], 0
    for run_prefix, keys in sorted(runs.items()):
        if len(keys) == 1 and keys[0].endswith("/_manifest.json"):
            manifest = json.loads(s3.get_object(Bucket=bucket, Key=keys[0])["Body"].read())
            if manifest.get("row_count", 0) == 0 and manifest.get("file_count", 0) == 0:
                empty.append(keys[0])
                continue
        kept += 1

    print(f"{len(runs)} run(s) under s3://{bucket}/{prefix}: {len(empty)} empty, {kept} kept")
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
    for i in range(0, len(empty), 1000):
        chunk = [{"Key": k} for k in empty[i:i + 1000]]
        resp = s3.delete_objects(Bucket=bucket, Delete={"Objects": chunk, "Quiet": True})
        for err in resp.get("Errors", []):
            print("  FAILED", err["Key"], err["Code"], err["Message"])
    print(f"Deleted {len(empty)} empty run(s).")


if __name__ == "__main__":
    main()
