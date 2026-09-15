#!/usr/bin/env python3
"""
What shapes does the PAR POS feed actually emit?

The existing Lambda consumer defends against four variations the strict
adapter does not accept: a plain `data` dict instead of `data_base64`, a
payload that is not gzip, a decoded payload that is a list, and order fields
nested under `order` or `data.order`. Defensive code does not prove those
shapes occur -- production consumers accumulate branches for things that
happened once in 2019, or never. This samples real objects and reports which
variations are actually present, so the adapter can be relaxed exactly as far
as the data requires and no further.

The last section is the important one: it reports where `id` and `version`
really live, which is what `s3.payload_fields` must be set to. Getting that
wrong does not fail -- it lands NULL and silver silently drops the row.

    python scripts/json_gz_payload_shape_census.py --uri s3://pos-events/orders --sample 50

Needs s3:ListBucket and s3:GetObject. Downloads --sample objects, writes
nothing, and prints no payload values -- only key names, types and counts.
Runs as a Glue Python Shell job; unknown arguments are ignored.
"""

import argparse
import base64
import gzip
import json
import re
import sys
import zlib
from collections import Counter
from datetime import datetime, timedelta, timezone

import boto3

WHITESPACE = re.compile(r"\s+")
# Paths worth probing for order identity, in the order the Lambda tries them.
ID_PATHS = ("id", "order.id", "data.order.id", "orderId", "order.orderId")
VERSION_PATHS = ("version", "order.version", "data.order.version")


def parse_uri(uri):
    m = re.fullmatch(r"s3://([^/]+)/?(.*)", uri)
    if not m:
        sys.exit(f"not an s3:// URI: {uri}")
    return m.group(1), m.group(2).rstrip("/")


def at_path(obj, path):
    for part in path.split("."):
        if not isinstance(obj, dict):
            return None
        obj = obj.get(part)
    return obj


def inner_codec(raw):
    """How is this payload actually compressed, if at all?"""
    if raw[:2] == b"\x1f\x8b":
        # Mirror the adapter's check, not gzip.decompress(): that helper
        # SUCCEEDS on concatenated members by returning them joined, so it
        # cannot see the one gzip variation the adapter rejects.
        d = zlib.decompressobj(31)
        try:
            d.decompress(raw)
        except zlib.error:
            return "gzip(broken)"
        if d.unused_data:
            return "gzip(multi-member)"
        return "gzip" if d.eof else "gzip(truncated)"
    try:
        zlib.decompress(raw)
        return "zlib"
    except Exception:
        pass
    try:
        json.loads(raw.decode("utf-8"))
        return "NONE (plain json)"
    except Exception:
        return "unrecognized"


def decompress(raw):
    for fn in (gzip.decompress, zlib.decompress, lambda b: b):
        try:
            return fn(raw)
        except Exception:
            continue
    return None


def outer_records(body):
    text = gzip.decompress(body).decode("utf-8")
    try:
        value = json.loads(text)
    except ValueError:
        value = [json.loads(line) for line in text.splitlines() if line.strip()]
    return value if isinstance(value, list) else [value]


def to_column(key):
    """
    Derive an Athena-safe column name from a source key.

    Athena lowercases identifiers and Iceberg then matches case-sensitively,
    so the generated block must never propose a name the config layer would
    reject -- the point of generating it is to paste it without editing.
    """
    column = re.sub(r"(?<!^)(?=[A-Z])", "_", key).lower()
    column = re.sub(r"[^a-z0-9_]+", "_", column)
    column = re.sub(r"_+", "_", column).strip("_")
    return column if column[:1].isalpha() or column[:1] == "_" else f"f_{column}"


def emit_config(payload_keys, records, min_presence):
    """
    Print a payload_fields: block covering the keys actually observed.

    Generated rather than inferred at runtime on purpose. The landing writer
    pins one Parquet schema per run from the first batch, so a key that first
    appears midway through a run would not fit it -- the run gets flagged
    schema_drift, and Bronze refuses to load a drifted run at all. An explicit
    block keeps the schema stable, keeps the diff reviewable, and lets Bronze's
    additive evolution add columns deliberately when you extend it later.

    Nested values are still projected: they land as exact JSON text, queryable
    with json_extract_scalar or CAST(json_parse(...) AS ARRAY(ROW(...))).
    """
    print("\n--- generated payload_fields (paste under the table's s3:) ---")
    print("      payload_fields:")
    skipped = 0
    for key, seen in sorted(payload_keys.items()):
        presence = seen / max(records, 1)
        if presence < min_presence:
            skipped += 1
            continue
        column = to_column(key)
        note = "" if presence > 0.999 else f"   # on {100 * presence:.1f}% of records"
        print(f"        {column}: {key}{note}")
    if skipped:
        print(f"      # {skipped} key(s) below --min-presence omitted; "
              f"they remain in payload_json")
    print("      # Review before use: column names are derived from the source keys,")
    print("      # and they are IDENTITY once Bronze has created the table.")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--uri", required=True)
    ap.add_argument("--sample", type=int, default=50, help="objects to download (default 50)")
    ap.add_argument("--hours-back", type=int, default=6, help="spread the sample over the last N hours (default 6)")
    ap.add_argument("--show-keys", type=int, default=30, help="top-level payload keys to list (default 30)")
    ap.add_argument("--emit-config", action="store_true",
                    help="print a payload_fields: block covering every key seen, ready to paste")
    ap.add_argument("--min-presence", type=float, default=0.0,
                    help="with --emit-config, skip keys present on fewer than this fraction of records")
    args, _unknown = ap.parse_known_args()

    bucket, prefix = parse_uri(args.uri)
    s3 = boto3.client("s3")

    # Spread the sample across recent hours rather than taking the first N of
    # one prefix: one hour's files can all come from a single publisher run.
    now = datetime.now(timezone.utc)
    keys = []
    for back in range(args.hours_back):
        hour = now - timedelta(hours=back)
        p = f"{prefix}/{hour:%Y/%m/%d/%H}/" if prefix else f"{hour:%Y/%m/%d/%H}/"
        per_hour = max(1, args.sample // args.hours_back)
        page = s3.list_objects_v2(Bucket=bucket, Prefix=p, MaxKeys=per_hour)
        keys += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".json.gz")]
    keys = keys[: args.sample]
    if not keys:
        sys.exit(f"no .json.gz objects in the last {args.hours_back}h under s3://{bucket}/{prefix}")
    print(f"sampling {len(keys)} objects from s3://{bucket}/{prefix}\n")

    envelope_field = Counter()
    codec = Counter()
    decoded_type = Counter()
    wrapper = Counter()
    payload_keys = Counter()
    id_found = Counter()
    version_found = Counter()
    id_types = Counter()
    envelope_id_matches = Counter()
    records = errors = 0

    for key in keys:
        try:
            body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            for env in outer_records(body):
                records += 1
                b64, inline = env.get("data_base64"), env.get("data")
                if isinstance(b64, str) and b64:
                    envelope_field["data_base64"] += 1
                    raw = base64.b64decode(WHITESPACE.sub("", b64))
                    if WHITESPACE.search(b64):
                        envelope_field["  (base64 was line-wrapped)"] += 1
                    codec[inner_codec(raw)] += 1
                    payload = json.loads(decompress(raw).decode("utf-8"))
                elif isinstance(inline, dict):
                    envelope_field["data (inline dict)"] += 1
                    codec["n/a (inline)"] += 1
                    payload = inline
                else:
                    envelope_field[f"NEITHER (data={type(inline).__name__})"] += 1
                    continue

                decoded_type[type(payload).__name__] += 1
                if isinstance(payload, list):
                    wrapper[f"list[{len(payload)}]"] += 1
                    payload = payload[0] if payload else {}
                if not isinstance(payload, dict):
                    continue

                if "order" in payload:
                    wrapper["{'order': ...}"] += 1
                elif isinstance(payload.get("data"), dict) and "order" in payload["data"]:
                    wrapper["{'data': {'order': ...}}"] += 1
                else:
                    wrapper["bare object"] += 1
                payload_keys.update(payload.keys())

                for path in ID_PATHS:
                    v = at_path(payload, path)
                    if v is not None:
                        id_found[path] += 1
                        id_types[f"{path} -> {type(v).__name__}"] += 1
                        env_id = env.get("id")
                        if isinstance(env_id, str) and ":" in env_id:
                            envelope_id_matches[
                                f"{path} {'==' if env_id.split(':')[-1] == str(v) else '!='} envelope id suffix"
                            ] += 1
                for path in VERSION_PATHS:
                    if at_path(payload, path) is not None:
                        version_found[path] += 1
        except Exception as exc:
            errors += 1
            print(f"  ERROR {key}: {type(exc).__name__}: {str(exc)[:90]}")

    def show(title, counter, total=None):
        print(f"\n{title}")
        if not counter:
            print("  (none)")
            return
        for k, v in counter.most_common():
            pct = f"{100 * v / total:5.1f}%" if total else ""
            print(f"  {v:>7}  {pct}  {k}")

    print(f"\n{records} records in {len(keys)} objects ({errors} objects failed)")
    show("payload carried in:", envelope_field, records)
    show("inner compression:", codec, records)
    show("decoded payload type:", decoded_type, records)
    show("wrapper shape:", wrapper, records)
    show(f"top-level payload keys (top {args.show_keys}):", Counter(dict(payload_keys.most_common(args.show_keys))))
    show("order id found at:", id_found, records)
    show("  id value types:", id_types)
    show("  vs envelope 'guid:<order_id>' suffix:", envelope_id_matches)
    show("version found at:", version_found, records)

    if args.emit_config:
        emit_config(payload_keys, records, args.min_presence)

    print("\n--- what this means for the config ---")
    if id_found:
        best = id_found.most_common(1)[0]
        print(f"  s3.payload_fields.order_id: {best[0]}"
              f"   (present on {100 * best[1] / max(records, 1):.1f}% of records)")
        if len(id_found) > 1:
            print(f"  WARNING: id appears at more than one path {sorted(id_found)} -- "
                  f"a single dotted path cannot cover all of them")
    else:
        print("  no order id found at any probed path; inspect the payload keys above")
    strict_ok = (envelope_field.get("data_base64", 0) == records
                 and set(codec) <= {"gzip"} and set(decoded_type) <= {"dict"})
    print(f"  adapter as written handles this feed: {'YES' if strict_ok else 'NO -- see divergences above'}")


if __name__ == "__main__":
    main()
