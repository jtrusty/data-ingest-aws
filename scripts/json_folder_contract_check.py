#!/usr/bin/env python3
"""
Does the POS producer honour "folder hour == upload hour"?

Read-only: only list_objects_v2, never get_object. Lists every object under
the hourly prefixes in a date range, compares each object's LastModified to
the yyyy/mm/dd/hh folder it sits in, then replays the par_pos_s3 adapter's
discovery rule over that listing -- scheduled runs every --interval minutes,
--lookback minutes of overlap, --safety seconds of cutoff -- and reports the
objects a real schedule would never have found.

    python scripts/json_folder_contract_check.py --uri s3://pos-events/orders --days 14
    python scripts/json_folder_contract_check.py --uri s3://pos-events/orders \
        --days 30 --tz America/Chicago --lookback 30

Runs anywhere with s3:ListBucket and boto3 -- including as an AWS Glue Python
Shell job, which is the easy way to run it against a bucket you cannot reach
from a laptop. Under Glue, pass the same flags as job parameters, give the job
the analytics library set (or --additional-python-modules tzdata, since
zoneinfo has no built-in UTC), and read the verdict in CloudWatch. Unknown
arguments are ignored, so Glue's own injected parameters are harmless.

Prints "VERDICT:" either way. Add --fail-on-miss to exit 1 on a violation,
for gating a schedule from CI; under Glue leave it off, so a run that finds a
violation still reports SUCCEEDED rather than looking like a crash.
"""

import argparse
import re
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import boto3

def parse_uri(uri):
    m = re.fullmatch(r"s3://([^/]+)/?(.*)", uri)
    if not m:
        sys.exit(f"not an s3:// URI: {uri}")
    return m.group(1), m.group(2).rstrip("/")


def hour_prefixes(prefix, path_format, tz, first, last):
    """
    (prefix, folder_hour_utc) for every hour in the range.

    Listing hour by hour rather than parsing the hour back out of each key is
    what makes this work for ANY discovery.path_format: rendering forward is
    well defined, inverting a strftime pattern is not. It costs one list call
    per hour, which at a couple of weeks is a few hundred calls.
    """
    hour = datetime(first.year, first.month, first.day, tzinfo=tz)
    end = datetime(last.year, last.month, last.day, 23, tzinfo=tz)
    while hour <= end:
        rendered = hour.strftime(path_format)
        if not rendered.endswith("/"):
            rendered += "/"
        yield (f"{prefix}/{rendered}" if prefix else rendered), hour.astimezone(timezone.utc)
        hour += timedelta(hours=1)


def list_objects(s3, bucket, prefix, first, last, tz, path_format, suffix):
    """(key, folder_hour_start_utc, last_modified_utc) for every matching object."""
    paginator = s3.get_paginator("list_objects_v2")
    skipped = Counter()
    for hour_prefix, folder in hour_prefixes(prefix, path_format, tz, first, last):
        for page in paginator.paginate(Bucket=bucket, Prefix=hour_prefix):
            for item in page.get("Contents", []):
                if not item["Key"].endswith(suffix):
                    skipped[f"not {suffix}"] += 1
                    continue
                yield item["Key"], folder, item["LastModified"].astimezone(timezone.utc)
    if skipped:
        print("skipped:", dict(skipped))


def simulate(objects, start, interval, lookback, safety):
    """
    Replay the adapter: at each scheduled run, high = now - safety and
    low = previous high - lookback; the hourly prefixes from floor(low) to
    floor(high) are listed, and an object counts as found if its folder is in
    that set and its LastModified falls in [low, high].
    """
    found = set()
    end = max(lm for _, _, lm in objects) + interval + safety
    checkpoint = start
    now = start + interval
    while now <= end:
        high = now - safety
        low = max(start, checkpoint - lookback)
        if high >= low:
            first_hour = low.replace(minute=0, second=0, microsecond=0)
            for key, folder, lm in objects:
                if key not in found and folder >= first_hour and folder <= high and low <= lm <= high:
                    found.add(key)
            checkpoint = high
        now += interval
    return found


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    # --uri, not a positional: AWS Glue can only pass "--flag value" pairs.
    ap.add_argument("--uri", required=True,
                    help="s3://bucket/prefix that ends right before yyyy/mm/dd/hh")
    ap.add_argument("--days", type=int, default=14, help="how many days back to list (default 14)")
    ap.add_argument("--tz", default="UTC", help="discovery.timezone (default UTC)")
    ap.add_argument("--path-format", default="%Y/%m/%d/%H",
                    help="discovery.path_format (default %%Y/%%m/%%d/%%H)")
    ap.add_argument("--suffix", default=".json.gz", help="discovery.suffix (default .json.gz)")
    ap.add_argument("--interval", type=int, default=15, help="schedule minutes (default 15)")
    ap.add_argument("--lookback", type=int, default=15, help="lookback_minutes (default 15)")
    ap.add_argument("--safety", type=int, default=120, help="safety_delay_seconds (default 120)")
    ap.add_argument("--show", type=int, default=25, help="worst offenders to print (default 25)")
    ap.add_argument("--fail-on-miss", action="store_true",
                    help="exit 1 if any object would be missed (off by default: under Glue a "
                         "nonzero exit reads as a broken script, not as a finding)")
    # parse_known_args(), not parse_args(): Glue injects --job-name,
    # --scriptLocation, --continuous-log-logGroup and friends, and a strict
    # parse exits 2 on them before the script ever runs.
    args, _unknown = ap.parse_known_args()

    bucket, prefix = parse_uri(args.uri)
    tz = ZoneInfo(args.tz)
    today = datetime.now(tz).date()
    first, last = today - timedelta(days=args.days), today

    print(f"listing s3://{bucket}/{prefix} {first}..{last} "
          f"(tz {args.tz}, path_format {args.path_format}, suffix {args.suffix})")
    objects = list(list_objects(boto3.client("s3"), bucket, prefix, first, last, tz,
                                args.path_format, args.suffix))
    if not objects:
        sys.exit(f"no {args.suffix} objects found")

    # Lateness = how long after its folder hour ENDED the object was written.
    # <= 0 means it landed inside its own hour, which is the contract.
    late = [(lm - (folder + timedelta(hours=1)), key, folder, lm) for key, folder, lm in objects]
    early = sum(1 for lm_delta, _, folder, lm in late if lm < folder)
    buckets = Counter()
    for delta, *_ in late:
        m = delta.total_seconds() / 60
        buckets[
            "in-hour" if m <= 0 else
            "<=15m late" if m <= 15 else
            "<=30m late" if m <= 30 else
            "<=1h late" if m <= 60 else
            "<=24h late" if m <= 1440 else
            ">24h late"
        ] += 1

    per_hour = Counter(folder for _, folder, _ in objects)
    print(f"\n{len(objects)} objects across {len(per_hour)} folder-hours; "
          f"min/mean/max per hour = {min(per_hour.values())}/"
          f"{sum(per_hour.values()) // len(per_hour)}/{max(per_hour.values())}")
    print("\nlateness past folder end:")
    for label in ("in-hour", "<=15m late", "<=30m late", "<=1h late", "<=24h late", ">24h late"):
        if buckets[label]:
            print(f"  {label:>11}  {buckets[label]:>8}  {100 * buckets[label] / len(objects):5.1f}%")
    if early:
        print(f"\n  {early} objects have LastModified BEFORE their folder hour -- "
              f"folder clock is probably not {args.tz}, or the producer names folders by event time")

    start = min(lm for _, _, lm in objects).replace(minute=0, second=0, microsecond=0)
    found = simulate(objects, start, timedelta(minutes=args.interval),
                     timedelta(minutes=args.lookback), timedelta(seconds=args.safety))
    missed = sorted((d, k, f, lm) for d, k, f, lm in late if k not in found)

    print(f"\nsimulated schedule: every {args.interval}m, lookback {args.lookback}m, safety {args.safety}s")
    print(f"  found  {len(found)}")
    print(f"  MISSED {len(missed)}")
    for delta, key, folder, lm in sorted(missed, reverse=True)[: args.show]:
        print(f"    {delta}  folder={folder:%Y-%m-%d %H}  modified={lm:%Y-%m-%d %H:%M:%S}  {key}")
    if len(missed) > args.show:
        print(f"    ... and {len(missed) - args.show} more")

    worst = max(late)[0]
    if missed:
        need = int(worst.total_seconds() // 60) + args.interval + 1
        print(f"\nworst lateness {worst}; a lookback of ~{need}m would have caught everything listed. "
              f"Re-run with --lookback {need} to confirm.")
        print("\nVERDICT: CONTRACT VIOLATED -- prefix-scan discovery would lose data")
    else:
        print("\nVERDICT: contract holds -- every object would have been found")

    # --fail-on-miss is off by default so a Glue run reporting a violation
    # still finishes SUCCEEDED; the verdict is in the log, and a FAILED run
    # would look like the script broke rather than like an answer.
    if missed and args.fail_on_miss:
        sys.exit(1)


if __name__ == "__main__":
    main()
