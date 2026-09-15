#!/usr/bin/env python3
"""
Does the POS producer honour "folder hour == upload hour"?

Read-only: only list_objects_v2, never get_object. Lists every object under
the hourly prefixes in a date range, compares each object's LastModified to
the yyyy/mm/dd/hh folder it sits in, then replays the par_pos_s3 adapter's
discovery rule over that listing -- scheduled runs every --interval minutes,
--lookback minutes of overlap, --safety seconds of cutoff -- and reports the
objects a real schedule would never have found.

    python scripts/pos_folder_contract_check.py s3://pos-events/orders --days 14
    python scripts/pos_folder_contract_check.py s3://pos-events/orders \
        --days 30 --tz America/Chicago --lookback 30

Exit status is 1 if any object would be missed, so it can gate a schedule.
"""

import argparse
import re
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import boto3

FOLDER = re.compile(r"(\d{4})/(\d{2})/(\d{2})/(\d{2})/[^/]+$")


def parse_uri(uri):
    m = re.fullmatch(r"s3://([^/]+)/?(.*)", uri)
    if not m:
        sys.exit(f"not an s3:// URI: {uri}")
    return m.group(1), m.group(2).rstrip("/")


def day_prefixes(prefix, first, last):
    day = first
    while day <= last:
        p = day.strftime("%Y/%m/%d/")
        yield f"{prefix}/{p}" if prefix else p
        day += timedelta(days=1)


def list_objects(s3, bucket, prefix, first, last, tz):
    """(key, folder_hour_start_utc, last_modified_utc) for every .json.gz."""
    paginator = s3.get_paginator("list_objects_v2")
    skipped = Counter()
    for day_prefix in day_prefixes(prefix, first, last):
        for page in paginator.paginate(Bucket=bucket, Prefix=day_prefix):
            for item in page.get("Contents", []):
                key = item["Key"]
                if not key.endswith(".json.gz"):
                    skipped["not .json.gz"] += 1
                    continue
                m = FOLDER.search(key)
                if not m:
                    skipped["no yyyy/mm/dd/hh in key"] += 1
                    continue
                y, mo, d, h = map(int, m.groups())
                folder = datetime(y, mo, d, h, tzinfo=tz).astimezone(timezone.utc)
                yield key, folder, item["LastModified"].astimezone(timezone.utc)
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
    ap.add_argument("uri", help="s3://bucket/prefix that ends right before yyyy/mm/dd/hh")
    ap.add_argument("--days", type=int, default=14, help="how many days back to list (default 14)")
    ap.add_argument("--tz", default="UTC", help="folder_timezone (default UTC)")
    ap.add_argument("--interval", type=int, default=15, help="schedule minutes (default 15)")
    ap.add_argument("--lookback", type=int, default=15, help="lookback_minutes (default 15)")
    ap.add_argument("--safety", type=int, default=120, help="safety_delay_seconds (default 120)")
    ap.add_argument("--show", type=int, default=25, help="worst offenders to print (default 25)")
    args = ap.parse_args()

    bucket, prefix = parse_uri(args.uri)
    tz = ZoneInfo(args.tz)
    today = datetime.now(tz).date()
    first, last = today - timedelta(days=args.days), today

    print(f"listing s3://{bucket}/{prefix} {first}..{last} (folder tz {args.tz})")
    objects = list(list_objects(boto3.client("s3"), bucket, prefix, first, last, tz))
    if not objects:
        sys.exit("no .json.gz objects found")

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
    sys.exit(1 if missed else 0)


if __name__ == "__main__":
    main()
