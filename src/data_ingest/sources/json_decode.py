"""Bounded, declaratively-configured decoding of JSON documents from S3.

The pipeline is fixed; each stage is chosen by config from a deliberately tiny
allowlist, so a new feed is a YAML change rather than a new adapter:

    object bytes -> outer decompress -> record framing
                 -> payload at path -> decode encoding -> decompress -> parse

Nothing here knows about CloudEvents, `data_base64`, or any producer. What it
guarantees instead is the same for every feed: every decompression is bounded,
base64 is validated rather than silently discarding bad bytes, JSON numbers
keep full precision as Decimal, duplicate object keys are an error, and a
malformed record fails the run instead of being skipped.
"""

import base64
import binascii
import json
import re
import zlib
from decimal import Decimal
from typing import Any, Dict, Iterator, Tuple

from data_ingest.exceptions import ExtractionError

_WHITESPACE = re.compile(r"\s+")


def _inflate(compressed: bytes, limit: int, compression: str, label: str) -> bytes:
    if compression == "auto":
        compression = "gzip" if compressed.startswith(b"\x1f\x8b") else "zlib"
    inflater = zlib.decompressobj(31 if compression == "gzip" else 15)
    try:
        result = inflater.decompress(compressed, limit + 1)
    except zlib.error:
        raise ExtractionError(f"Invalid {label} compressed stream") from None
    if len(result) > limit or inflater.unconsumed_tail:
        raise ExtractionError(f"{label} decompressed size exceeds configured limit")
    if not inflater.eof or inflater.unused_data:
        raise ExtractionError(f"Truncated or trailing data in {label} compressed stream")
    return result


def _unique_object(pairs):
    result = dict(pairs)
    if len(result) != len(pairs):
        raise ValueError("Duplicate JSON object keys")
    return result


def _reject_constant(_value):
    raise ValueError("Non-finite JSON numbers are unsupported")


def _loads(text: str):
    return json.loads(text, parse_float=Decimal, object_pairs_hook=_unique_object,
                      parse_constant=_reject_constant)


def _utf8(raw: bytes, label: str) -> str:
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        raise ExtractionError(f"Invalid {label} UTF-8 encoding") from None


def _jsonl(text: str):
    # One complete object per nonempty line. Split on '\n' only: JSONL is
    # newline-delimited, and str.splitlines() would also break on U+2028,
    # U+2029, NEL and friends, which are legal unescaped inside a JSON string.
    # A malformed multiline object still fails, since each line parses alone.
    lines = tuple(line for line in (raw.rstrip("\r") for raw in text.split("\n")) if line.strip())
    if not lines:
        raise ExtractionError("Empty outer JSON document")
    try:
        return [_loads(line) for line in lines]
    except (ValueError, RecursionError):
        raise ExtractionError("Invalid outer JSON document") from None


def _outer_records(text: str, framing: str) -> Iterator[Dict[str, Any]]:
    if framing == "jsonl":
        value = _jsonl(text)
    else:
        try:
            value = _loads(text)
        except (ValueError, RecursionError):
            if framing != "auto":
                raise ExtractionError("Invalid outer JSON document") from None
            value = _jsonl(text)
        if framing == "array" and not isinstance(value, list):
            raise ExtractionError("Outer JSON document must be an array")
        if framing == "object" and not isinstance(value, dict):
            raise ExtractionError("Outer JSON document must be an object")
    records = value if isinstance(value, list) else [value]
    for record in records:
        if not isinstance(record, dict):
            raise ExtractionError("Each outer JSON record must be an object")
        yield record


def _at_path(record, path):
    current = record
    for part in path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def _decode_encoding(value, encoding, path):
    """Return payload bytes, or a value already parsed by the outer JSON."""
    if encoding == "none":
        return value
    if not isinstance(value, str) or not value:
        raise ExtractionError(f"Each record requires a nonempty string at {path}")
    try:
        # validate=True rejects any non-alphabet byte, which is the point --
        # the lenient default silently DISCARDS them, so corruption decodes to
        # a shorter payload instead of failing. Whitespace is stripped first
        # because RFC 2045 permits line-wrapped base64 (Java's MIME encoder,
        # OpenSSL) and that is a legitimate encoding of the same bytes, not
        # corruption. Everything else still raises.
        return base64.b64decode(_WHITESPACE.sub("", value), validate=True)
    except (ValueError, binascii.Error):
        raise ExtractionError(f"Invalid base64 at {path}") from None


def _payload(record, payload_config, limit):
    """
    Resolve one record's payload to (value, exact JSON text).

    A payload path of None means the outer record IS the payload -- the shape a
    plain JSON array of business objects has, with no envelope at all.
    """
    path = payload_config.path
    if path is None:
        return record, dumps_json(record)

    raw = _at_path(record, path)
    if raw is None:
        raise ExtractionError(f"Each record requires a payload at {path}")

    # CloudEvents allows `data` (already-parsed JSON) as the alternative to
    # `data_base64`, so an uncompressed, unencoded path may land here as a
    # value the outer parse already produced. It gets the same checks as a
    # decoded payload -- an object, within the size limit -- so payload_json's
    # shape depends on the content, never on which wire encoding carried it.
    if payload_config.encoding == "none" and payload_config.compression == "none" \
            and not isinstance(raw, (str, bytes, bytearray)):
        if not isinstance(raw, dict):
            raise ExtractionError("Decoded payload JSON must be an object")
        text = dumps_json(raw)
        if len(text) > limit:
            raise ExtractionError("payload decompressed size exceeds configured limit")
        return raw, text

    if not isinstance(raw, (str, bytes, bytearray)):
        # bytes(5) is five NUL bytes and bytes(10**14) is an attempted 100 TB
        # allocation; neither is a payload. Refuse before touching it.
        raise ExtractionError(
            f"Payload at {path} must be a string (or an object when unencoded), "
            f"not {type(raw).__name__}"
        )

    raw = _decode_encoding(raw, payload_config.encoding, path)
    if payload_config.compression != "none":
        if not isinstance(raw, (bytes, bytearray)):
            raise ExtractionError(f"Compressed payload at {path} must be bytes")
        raw = _inflate(bytes(raw), limit, payload_config.compression, "payload")
    text = raw if isinstance(raw, str) else _utf8(bytes(raw), "payload")
    if len(text.encode("utf-8")) > limit:
        raise ExtractionError("payload decompressed size exceeds configured limit")
    try:
        value = _loads(text)
    except (ValueError, RecursionError):
        raise ExtractionError("Invalid payload JSON document") from None
    if not isinstance(value, dict):
        raise ExtractionError("Decoded payload JSON must be an object")
    return value, text


def decode_records(body, *, document, max_object_bytes=None):
    """
    Decode one S3 object into (envelope, payload, payload_json) triples.

    `document` supplies the stages: outer compression, record framing, and the
    payload's path/encoding/compression/format. Limits apply to decompressed
    bytes; trailing compressed data is rejected. Payload text is returned
    unchanged so nothing downstream re-serializes and rounds it.
    """
    outer_limit, payload_limit = document.max_outer_bytes, document.max_payload_bytes
    if document.compression == "none":
        outer = _utf8(body, "outer")
        if len(body) > outer_limit:
            raise ExtractionError("outer decompressed size exceeds configured limit")
    else:
        outer = _utf8(_inflate(body, outer_limit, document.compression, "outer"), "outer")
    for envelope in _outer_records(outer, document.records):
        payload, text = _payload(envelope, document.payload, payload_limit)
        yield envelope, payload, text


def dumps_json(value: Any) -> str:
    """Serialize JSON values, retaining Decimal values as exact numeric tokens."""
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("Non-finite JSON numbers are unsupported")
        return str(value)
    if isinstance(value, dict):
        return "{" + ",".join(
            json.dumps(key, ensure_ascii=True) + ":" + dumps_json(item)
            for key, item in value.items()
        ) + "}"
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(dumps_json(item) for item in value) + "]"
    return json.dumps(value, ensure_ascii=True, allow_nan=False)
