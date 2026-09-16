#!/usr/bin/env python3
"""
Does the producer honour "folder hour == upload hour"?

Read-only: only list_objects_v2, never get_object. Lists every object under
the hourly prefixes in a date range, compares each object's LastModified to
the yyyy/mm/dd/hh folder it sits in, then replays the par_pos_s3 adapter's
discovery rule over that listing -- scheduled runs every --interval minutes,
--lookback minutes of overlap, --safety seconds of cutoff -- and reports the
objects a real schedule would never have found.

    python scripts/json_folder_contract_check.py --uri s3://my-events/orders --days 14
    python scripts/json_folder_contract_check.py --uri s3://my-events/orders \
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
    (prefix, (first_utc, last_utc)) for every distinct prefix in the range.

    Listing hour by hour rather than parsing the hour back out of each key is
    what makes this work for ANY discovery.path_format: rendering forward is
    well defined, inverting a strftime pattern is not.

    Steps on the UTC timeline and renders in the folder timezone, exactly as
    the adapter does. A DST fall-back renders two UTC hours to one prefix;
    that prefix is listed once but SPANS both instants, because the adapter
    walks it whenever either UTC hour is in range. Keying it to the first
    instant alone would report objects from the second hour as missed.
    """
    cursor = datetime(first.year, first.month, first.day, tzinfo=tz).astimezone(timezone.utc)
    end = datetime(last.year, last.month, last.day, 23, tzinfo=tz).astimezone(timezone.utc)
    spans = {}
    order = []
    while cursor <= end:
        rendered = cursor.astimezone(tz).strftime(path_format)
        if not rendered.endswith("/"):
            rendered += "/"
        full = f"{prefix}/{rendered}" if prefix else rendered
        if full not in spans:
            spans[full] = [cursor, cursor]
            order.append(full)
        else:
            spans[full][1] = cursor
        cursor += timedelta(hours=1)
    for full in order:
        yield full, tuple(spans[full])


def _span(folder):
    """Accept a bare instant (tests, single hours) or a (first, last) span."""
    return folder if isinstance(folder, tuple) else (folder, folder)


def folder_end(folder):
    """The instant this folder's hour(s) close: the last instant plus one."""
    return _span(folder)[1] + timedelta(hours=1)


def list_objects(s3, bucket, prefix, first, last, tz, path_format, suffix):
    """(key, folder_span_utc, last_modified_utc) for every matching object."""
    paginator = s3.get_paginator("list_objects_v2")
    skipped = Counter()
    for hour_prefix, span in hour_prefixes(prefix, path_format, tz, first, last):
        for page in paginator.paginate(Bucket=bucket, Prefix=hour_prefix):
            for item in page.get("Contents", []):
                if not item["Key"].endswith(suffix):
                    skipped[f"not {suffix}"] += 1
                    continue
                yield item["Key"], span, item["LastModified"].astimezone(timezone.utc)
    if skipped:
        print("skipped:", dict(skipped))


def simulate(objects, start, interval, lookback, safety, lookahead=timedelta(hours=1)):
    """
    Replay the adapter's discovery rule.

    Each run lists the hourly prefixes from floor(previous high - lookback)
    through floor(high + lookahead). Its object window is (previous high,
    high] -- exclusive at the previous high, because anything at or before it
    was inside the previous run's window and, with S3's list-after-put
    consistency, was listed then. The first run's window starts at `start`,
    inclusive. An object is found when its folder is walked AND its
    LastModified is in the window, in the same run.
    """
    found = set()
    end = max(lm for _, _, lm in objects) + interval + safety + lookahead
    checkpoint, first_run = start, True
    now = start + interval
    while now <= end:
        high = now - safety
        low = start if first_run else checkpoint
        if high >= low:
            first_hour = (low - (timedelta(0) if first_run else lookback)).replace(
                minute=0, second=0, microsecond=0)
            last_hour = high + lookahead
            for key, folder, lm in objects:
                span_first, span_last = _span(folder)
                walked = span_first <= last_hour and span_last >= first_hour
                in_window = (low <= lm <= high) if first_run else (low < lm <= high)
                if key not in found and walked and in_window:
                    found.add(key)
            checkpoint, first_run = high, False
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
    late = [(lm - folder_end(folder), key, folder, lm) for key, folder, lm in objects]
    early = sum(1 for _, _, folder, lm in late if lm < _span(folder)[0])
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

    per_hour = Counter(_span(folder)[0] for _, folder, _ in objects)
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
        print(f"    {delta}  folder={_span(folder)[0]:%Y-%m-%d %H}  modified={lm:%Y-%m-%d %H:%M:%S}  {key}")
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
