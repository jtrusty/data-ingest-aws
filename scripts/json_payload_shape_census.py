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
really live, which Silver needs to know to collapse versions.

    python scripts/json_payload_shape_census.py --uri s3://pos-events/orders --sample 50

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
from zoneinfo import ZoneInfo

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


def emit_config(envelope_field, codec, decoded_type, wrapper, id_found, version_found, records):
    """
    Print the document: and record: blocks this feed implies.

    The payload itself is landed whole as payload_json, so there is no column
    projection to generate. What config actually has to match is the WIRE
    FORMAT -- where the payload lives, how it is encoded and compressed, how
    records are framed -- and that is exactly what the sample just measured.
    """
    def top(counter):
        return counter.most_common(1)[0][0] if counter else None

    carrier, codec_seen, framing = top(envelope_field), top(codec), top(decoded_type)
    inline = carrier == "data (inline dict)"
    path = "data" if inline else "data_base64"
    encoding = "none" if inline else "base64"
    compression = {"gzip": "gzip", "zlib": "zlib", "NONE (plain json)": "none"}.get(
        codec_seen, "none" if inline else "auto")

    preset = ("cloudevents_plain" if inline and compression == "none"
              else "cloudevents" if not inline and compression in ("gzip", "zlib", "auto")
              else None)

    print("\n--- generated config (paste under source:) ---")
    print("  document:")
    if preset:
        print(f"    preset: {preset}")
    print("    compression: gzip          # the OUTER file")
    print("    records: auto")
    print("    payload:")
    print(f"      path: {path}")
    print(f"      encoding: {encoding}")
    print(f"      compression: {compression}")
    print("      format: json")
    if len(codec) > 1:
        print(f"    # WARNING: payloads are not uniformly compressed {dict(codec)} --")
        print("    # 'auto' covers gzip and zlib, but not a mix that includes plain JSON.")
    if len(envelope_field) > 1:
        print(f"    # WARNING: payload carrier varies {dict(envelope_field)} -- CloudEvents")
        print("    # allows either data or data_base64, but one config picks one.")
    if framing and framing != "dict":
        print(f"    # WARNING: decoded payload is {framing}, not an object; the adapter")
        print("    # requires a JSON object per record.")

    print("\n  # For Silver (NOT ingestion config -- Bronze does not know what a record is):")
    if id_found:
        best, seen = id_found.most_common(1)[0]
        note = "" if seen >= records else f"   # on {100 * seen / max(records, 1):.1f}% of records"
        print(f"  #   identity:  json_extract_scalar(payload_json, '$.{best}'){note}")
    else:
        print("  #   no identity found at any probed path; inspect payload keys above")
    if version_found:
        print(f"  #   version:   json_extract_scalar(payload_json, '$.{version_found.most_common(1)[0][0]}')")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--uri", required=True)
    ap.add_argument("--sample", type=int, default=50, help="objects to download (default 50)")
    ap.add_argument("--hours-back", type=int, default=6, help="spread the sample over the last N hours (default 6)")
    ap.add_argument("--show-keys", type=int, default=30, help="top-level payload keys to list (default 30)")
    ap.add_argument("--tz", default="UTC", help="discovery.timezone (default UTC)")
    ap.add_argument("--path-format", default="%Y/%m/%d/%H",
                    help="discovery.path_format (default %%Y/%%m/%%d/%%H)")
    ap.add_argument("--suffix", default=".json.gz", help="discovery.suffix (default .json.gz)")
    ap.add_argument("--emit-config", action="store_true",
                    help="print the document:/record: blocks this feed implies, ready to paste")
    args, _unknown = ap.parse_known_args()

    bucket, prefix = parse_uri(args.uri)
    s3 = boto3.client("s3")

    # Spread the sample across recent hours rather than taking the first N of
    # one prefix: one hour's files can all come from a single publisher run.
    # Prefixes are rendered from the configured layout, in the configured
    # timezone, so a feed with local-time folders or a non-default path_format
    # is sampled from folders that actually exist.
    tz = ZoneInfo(args.tz)
    now = datetime.now(timezone.utc)
    keys = []
    per_hour = max(1, args.sample // args.hours_back)
    for back in range(args.hours_back):
        rendered = (now - timedelta(hours=back)).astimezone(tz).strftime(args.path_format)
        if not rendered.endswith("/"):
            rendered += "/"
        p = f"{prefix}/{rendered}" if prefix else rendered
        page = s3.list_objects_v2(Bucket=bucket, Prefix=p, MaxKeys=per_hour)
        keys += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(args.suffix)]
    keys = keys[: args.sample]
    if not keys:
        sys.exit(f"no {args.suffix} objects in the last {args.hours_back}h under s3://{bucket}/{prefix}")
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
        emit_config(envelope_field, codec, decoded_type, wrapper,
                    id_found, version_found, records)

    print("\n--- what this means for the config ---")
    if id_found:
        best = id_found.most_common(1)[0]
        print(f"  Silver identity path: $.{best[0]}"
              f"   (present on {100 * best[1] / max(records, 1):.1f}% of records)")
        if len(id_found) > 1:
            print(f"  WARNING: id appears at more than one path {sorted(id_found)} -- "
                  f"a single dotted path cannot cover all of them")
    else:
        print("  no order id found at any probed path; inspect the payload keys above")
    # "Can ONE document: block describe this feed?" -- which is what the
    # presets above actually accept, not just the original PAR shape. A feed
    # needs one carrier, one compression family, and object payloads.
    carriers = {k for k in envelope_field if not k.startswith("  (") and not k.startswith("NEITHER")}
    codecs = set(codec) - {"n/a (inline)"}
    one_codec = (codecs <= {"gzip", "zlib"}) or (codecs <= {"NONE (plain json)"}) or not codecs
    describable = (
        len(carriers) == 1 and "NEITHER" not in " ".join(envelope_field)
        and one_codec and set(decoded_type) <= {"dict"}
    )
    print(f"  one document: block can describe this feed: "
          f"{'YES' if describable else 'NO -- see divergences above'}")


if __name__ == "__main__":
    main()
