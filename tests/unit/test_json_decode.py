import base64
import gzip
import json
import zlib
from decimal import Decimal

import pytest

from data_ingest.exceptions import ExtractionError
from data_ingest.config_s3_json import parse_document
from data_ingest.sources.json_decode import decode_records, dumps_json


def document(max_outer_bytes=100_000, max_payload_bytes=1_000, **overrides):
    """The CloudEvents wire format, which is what most of these tests use."""
    payload = {"path": "data_base64", "encoding": "base64",
               "compression": overrides.pop("compression", "auto"), "format": "json"}
    payload.update(overrides.pop("payload", {}))
    return parse_document({
        "compression": overrides.pop("document_compression", "gzip"),
        "records": overrides.pop("records", "auto"),
        "envelope": "cloudevents",
        "payload": payload,
        "max_outer_bytes": max_outer_bytes,
        "max_payload_bytes": max_payload_bytes,
        **overrides,
    })


def envelope(payload=b'{"version": 2}', compressor=gzip.compress):
    return {"id": "event-1", "data_base64": base64.b64encode(compressor(payload)).decode()}


def decode(raw, **kwargs):
    return list(decode_records(gzip.compress(raw), document=document(**kwargs)))


@pytest.mark.parametrize("framing", ["object", "array", "jsonl"])
@pytest.mark.parametrize("compressor", [gzip.compress, zlib.compress])
def test_supported_framing_and_compression(framing, compressor):
    record = envelope(compressor=compressor)
    text = json.dumps(record)
    raw = {"object": text, "array": json.dumps([record, record]),
           "jsonl": text + "\n\n" + text}[framing]
    records = decode(raw.encode())
    assert len(records) == (1 if framing == "object" else 2)
    assert records[0] == (record, {"version": 2}, '{"version": 2}')


def test_numeric_precision_is_preserved():
    payload = b'{"amount": 1234567890.1234567890123456789, "version": 9007199254740993}'
    raw = json.dumps(envelope(payload)).replace('"event-1"', '0.1234567890123456789')
    outer, parsed, original = decode(raw.encode())[0]
    assert original == payload.decode()
    assert parsed["amount"] == Decimal("1234567890.1234567890123456789")
    assert parsed["version"] == 9007199254740993
    assert '0.1234567890123456789' in dumps_json(outer)


@pytest.mark.parametrize("payload", [b'{"x":1,"x":2}', b'{"x":NaN}',
                                    b'{"x":Infinity}', b'[]', b'null', b'private-invalid', b'"secret"'])
def test_rejects_invalid_inner_json_without_leaking_contents(payload):
    with pytest.raises(ExtractionError) as error:
        decode(json.dumps(envelope(payload)).encode())
    assert payload.decode() not in str(error.value)


@pytest.mark.parametrize("raw", [b'[]', b'null', b'[1]', b'{"id":1,"id":2}',
                                b'{"x":NaN}', b'{"x":-Infinity}', b'bad-secret', b'{}\n[]', b''])
def test_rejects_invalid_outer_records(raw):
    if raw == b'[]':
        assert decode(raw) == []
    else:
        with pytest.raises(ExtractionError):
            decode(raw)


@pytest.mark.parametrize("value", [None, 2, "", "not!base64", base64.b64encode(b'{}').decode()])
def test_rejects_missing_or_invalid_base64(value):
    with pytest.raises(ExtractionError):
        decode(json.dumps({"data_base64": value}).encode())


@pytest.mark.parametrize("compression", ["gzip", "zlib"])
def test_explicit_compression(compression):
    compressor = gzip.compress if compression == "gzip" else zlib.compress
    assert decode(json.dumps(envelope(compressor=compressor)).encode(),
                  compression=compression)[0][1] == {"version": 2}
    other = zlib.compress if compression == "gzip" else gzip.compress
    with pytest.raises(ExtractionError):
        decode(json.dumps(envelope(compressor=other)).encode(), compression=compression)


def test_decompression_limits_and_exact_boundary():
    raw = json.dumps(envelope()).encode()
    assert list(decode_records(gzip.compress(raw),
                               document=document(max_outer_bytes=len(raw), max_payload_bytes=14)))
    for outer_limit, inner_limit in [(len(raw) - 1, 14), (len(raw), 13)]:
        with pytest.raises(ExtractionError, match="limit"):
            list(decode_records(gzip.compress(raw), document=document(
                max_outer_bytes=outer_limit, max_payload_bytes=inner_limit)))


@pytest.mark.parametrize("change", [lambda b: b[:-1], lambda b: b + b'junk',
                                    lambda b: b + gzip.compress(b'{}')])
def test_rejects_truncated_or_trailing_outer_stream(change):
    with pytest.raises(ExtractionError):
        list(decode_records(change(gzip.compress(b'[]')),
                            document=document(max_outer_bytes=100, max_payload_bytes=100)))


@pytest.mark.parametrize("compressor", [gzip.compress, zlib.compress])
@pytest.mark.parametrize("change", [lambda b: b[:-1], lambda b: b + b'junk'])
def test_rejects_truncated_or_trailing_inner_stream(compressor, change):
    record = envelope(compressor=lambda b: change(compressor(b)))
    with pytest.raises(ExtractionError):
        decode(json.dumps(record).encode())


@pytest.mark.parametrize("outer,inner,compression", [(0, 1, "auto"), (1, -1, "auto"),
                                                       (1, 1, "raw"), (True, 1, "auto")])
def test_rejects_invalid_configuration(outer, inner, compression):
    # Limits and stage names are validated where config is parsed now, not on
    # every decode call, so the whole run fails before any object is read.
    with pytest.raises(Exception):
        document(max_outer_bytes=outer, max_payload_bytes=inner, compression=compression)


def test_invalid_unicode_is_safe():
    with pytest.raises(ExtractionError):
        decode(b'\xff')
    with pytest.raises(ExtractionError):
        decode(json.dumps(envelope(b'\xff')).encode())


def test_decimal_json_serialization_supports_nested_values():
    value = {"x": [Decimal("1.234567890123456789"), None, True, "text"], "y": {"z": 3}}
    assert json.loads(dumps_json(value), parse_float=Decimal) == value
    with pytest.raises(ValueError):
        dumps_json(Decimal("NaN"))


@pytest.mark.parametrize("encoder", [base64.b64encode, base64.encodebytes])
def test_line_wrapped_base64_is_accepted(encoder):
    """
    RFC 2045 permits base64 wrapped at 76 characters, which Java's MIME
    encoder and OpenSSL both emit. It encodes the same bytes, so rejecting it
    would fail an entire run over a formatting choice by the producer.
    """
    payload = b'{"id": 12345678901234, "version": 1}'
    envelope = {"data_base64": encoder(gzip.compress(payload)).decode()}
    outer = gzip.compress(json.dumps(envelope).encode())

    (_env, decoded, text), = decode_records(
        outer, document=document(max_outer_bytes=1 << 20, max_payload_bytes=1 << 20)
    )
    assert decoded["id"] == 12345678901234
    assert text == payload.decode()


def test_corrupt_base64_still_raises_rather_than_decoding_short():
    # The lenient default would DISCARD the '!' and decode a truncated
    # payload; validate=True must still reject it after whitespace stripping.
    envelope = {"data_base64": "!!!" + base64.b64encode(gzip.compress(b"{}")).decode()}
    outer = gzip.compress(json.dumps(envelope).encode())
    with pytest.raises(ExtractionError, match="Invalid base64 at data_base64"):
        list(decode_records(outer, document=document(
            max_outer_bytes=1 << 20, max_payload_bytes=1 << 20)))


# --- review findings ---------------------------------------------------------

@pytest.mark.parametrize("value", [5, -1, 1.5, True, 100_000_000_000_000])
def test_a_scalar_at_an_unencoded_payload_path_is_refused_not_coerced(value):
    """
    bytes(5) is five NUL bytes, bytes(-1) is ValueError, and bytes(10**14)
    is an attempted 100 TB allocation. None of those is a payload; the
    adapter must refuse it as not-an-object before bytes() is ever reached.
    """
    outer = gzip.compress(json.dumps({"id": "e", "data": value}).encode())
    doc = parse_document({"preset": "cloudevents_plain"})
    with pytest.raises(ExtractionError, match="must be an object"):
        list(decode_records(outer, document=doc))


@pytest.mark.parametrize("value", [5, -1, 100_000_000_000_000, {"not": "bytes"}])
def test_a_non_string_at_a_compressed_payload_path_is_refused_before_bytes(value):
    # encoding none + compression gzip: the value must already be a string
    # of compressed bytes. Anything else used to reach bytes(value).
    outer = gzip.compress(json.dumps({"id": "e", "data": value}).encode())
    doc = parse_document({"payload": {"path": "data", "encoding": "none", "compression": "gzip"}})
    with pytest.raises(ExtractionError, match="Payload at data must be a string"):
        list(decode_records(outer, document=doc))


def test_an_inline_array_payload_is_rejected_exactly_like_an_encoded_one():
    # payload_json's shape must depend on content, never on which wire
    # encoding carried it -- Silver reads $.id and would get NULL from an
    # array, with nothing failing anywhere.
    inline = gzip.compress(json.dumps({"id": "e", "data": [{"id": 1}]}).encode())
    with pytest.raises(ExtractionError, match="must be an object"):
        list(decode_records(inline, document=parse_document({"preset": "cloudevents_plain"})))
    encoded = gzip.compress(json.dumps(envelope(b'[{"id": 1}]')).encode())
    with pytest.raises(ExtractionError, match="must be an object"):
        list(decode_records(encoded, document=parse_document({"preset": "cloudevents"})))


def test_an_inline_payload_is_held_to_the_size_limit_too():
    big = {"id": "e", "data": {"blob": "x" * 200}}
    outer = gzip.compress(json.dumps(big).encode())
    doc = parse_document({"preset": "cloudevents_plain", "max_payload_bytes": 100})
    with pytest.raises(ExtractionError, match="limit"):
        list(decode_records(outer, document=doc))


def test_jsonl_splits_only_on_newline():
    # U+2028 is a legal unescaped character inside a JSON string, and
    # json.dumps(ensure_ascii=False) emits it literally. str.splitlines()
    # would cut the record in two.
    line1 = json.dumps({"a": "x\u2028y"}, ensure_ascii=False)
    line2 = json.dumps({"a": "z"})
    outer = gzip.compress((line1 + "\r\n" + line2 + "\n").encode("utf-8"))
    doc = parse_document({"preset": "records", "records": "jsonl"})
    payloads = [p for _, p, _ in decode_records(outer, document=doc)]
    assert payloads == [{"a": "x\u2028y"}, {"a": "z"}]
