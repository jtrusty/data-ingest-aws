"""
The census decides how far the adapter has to be relaxed and what
s3.order_id_path must be set to, so its classifications are tested rather
than trusted -- particularly the ones that distinguish a shape the adapter
accepts from one it rejects.
"""

import base64
import gzip
import importlib.util
import json
import sys
import zlib
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

SCRIPT = Path(__file__).parents[2] / "scripts" / "pos_payload_shape_census.py"
spec = importlib.util.spec_from_file_location("pos_payload_shape_census", SCRIPT)
census = importlib.util.module_from_spec(spec)
spec.loader.exec_module(census)


@pytest.mark.parametrize("raw,expected", [
    (gzip.compress(b'{"a":1}'), "gzip"),
    (zlib.compress(b'{"a":1}'), "zlib"),
    (b'{"a":1}', "NONE (plain json)"),
    (gzip.compress(b'{"a":1}') + gzip.compress(b'{"b":2}'), "gzip(multi-member)"),
    (b"\x00\x01\x02", "unrecognized"),
])
def test_inner_codec_distinguishes_what_the_adapter_accepts(raw, expected):
    # gzip is the only value the adapter accepts; every other result here
    # means the feed needs the adapter relaxed before it can be scheduled.
    assert census.inner_codec(raw) == expected


def test_multi_member_gzip_is_not_reported_as_plain_gzip():
    # gzip.decompress() accepts concatenated members and the adapter does
    # not, so conflating them would hide a real incompatibility.
    single = census.inner_codec(gzip.compress(b'{"a":1}'))
    multi = census.inner_codec(gzip.compress(b'{"a":1}') + gzip.compress(b'{"b":2}'))
    assert single == "gzip" and multi != "gzip"


@pytest.mark.parametrize("payload,path,expected", [
    ({"id": 7}, "id", 7),
    ({"order": {"id": 7}}, "order.id", 7),
    ({"data": {"order": {"id": 7}}}, "data.order.id", 7),
    ({"order": {"id": 7}}, "id", None),          # the silent-NULL case
    ({"id": 7}, "order.id", None),
    ({"order": "not-a-dict"}, "order.id", None),
])
def test_at_path(payload, path, expected):
    assert census.at_path(payload, path) == expected


@pytest.mark.parametrize("framing", ["object", "array", "jsonl"])
def test_outer_records_handles_every_framing(framing):
    one = {"id": "guid:1", "data_base64": ""}
    text = {"object": json.dumps(one),
            "array": json.dumps([one, one]),
            "jsonl": json.dumps(one) + "\n" + json.dumps(one)}[framing]
    assert len(census.outer_records(gzip.compress(text.encode()))) == (1 if framing == "object" else 2)


def _object_with(records):
    return gzip.compress(json.dumps(records).encode())


def _run(records, capsys, argv_extra=()):
    s3 = Mock()
    s3.list_objects_v2.return_value = {"Contents": [{"Key": "orders/2026/09/15/10/a.json.gz"}]}
    s3.get_object.return_value = {"Body": Mock(read=lambda: _object_with(records))}
    with patch.object(census.sys, "argv",
                      ["census", "--uri", "s3://pos/orders", "--sample", "1",
                       "--hours-back", "1", *argv_extra]), \
         patch.object(census.boto3, "client", return_value=s3):
        census.main()
    return capsys.readouterr().out


def _envelope(payload, oid="12345678901234"):
    return {"id": f"guid:{oid}",
            "data_base64": base64.b64encode(gzip.compress(json.dumps(payload).encode())).decode()}


def test_a_feed_the_adapter_handles_is_reported_as_such(capsys):
    out = _run([_envelope({"id": 12345678901234, "version": 1})], capsys)
    assert "adapter as written handles this feed: YES" in out
    assert "s3.order_id_path: id" in out


def test_nested_order_is_reported_as_the_path_to_configure(capsys):
    # The dangerous shape: order_id_path 'id' would land NULL here and silver
    # would drop the row, with nothing failing anywhere.
    out = _run([_envelope({"order": {"id": 12345678901234, "version": 1}})], capsys)
    assert "s3.order_id_path: order.id" in out
    assert "{'order': ...}" in out


def test_inline_data_dict_is_flagged_as_unsupported(capsys):
    out = _run([{"id": "guid:1", "data": {"id": 1, "version": 1}}], capsys)
    assert "data (inline dict)" in out
    assert "adapter as written handles this feed: NO" in out


def test_uncompressed_payload_is_flagged_as_unsupported(capsys):
    envelope = {"id": "guid:1",
                "data_base64": base64.b64encode(json.dumps({"id": 1}).encode()).decode()}
    out = _run([envelope], capsys)
    assert "NONE (plain json)" in out
    assert "adapter as written handles this feed: NO" in out


def test_conflicting_id_paths_raise_a_warning(capsys):
    out = _run([_envelope({"id": 1}), _envelope({"order": {"id": 2}})], capsys)
    assert "WARNING: id appears at more than one path" in out


def test_line_wrapped_base64_is_noticed(capsys):
    blob = gzip.compress(json.dumps({"id": 1}).encode())
    out = _run([{"id": "guid:1", "data_base64": base64.encodebytes(blob).decode()}], capsys)
    assert "(base64 was line-wrapped)" in out


def test_a_broken_object_is_counted_not_fatal(capsys):
    s3 = Mock()
    s3.list_objects_v2.return_value = {"Contents": [{"Key": "orders/2026/09/15/10/a.json.gz"}]}
    s3.get_object.return_value = {"Body": Mock(read=lambda: b"not gzip at all")}
    with patch.object(census.sys, "argv",
                      ["census", "--uri", "s3://pos/orders", "--sample", "1", "--hours-back", "1"]), \
         patch.object(census.boto3, "client", return_value=s3):
        census.main()
    assert "1 objects failed" in capsys.readouterr().out


def test_glue_injected_arguments_are_ignored(capsys):
    out = _run([_envelope({"id": 1})], capsys,
               argv_extra=("--job-name", "census", "--scriptLocation", "s3://x/y.py"))
    assert "adapter as written handles this feed" in out
