"""Bounded decoding of gzip POS exports and their compressed event payloads."""

import base64
import binascii
import json
import zlib
from decimal import Decimal
from typing import Any, Dict, Iterator, Tuple

from data_ingest.exceptions import ExtractionError


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


def _outer_records(text: str) -> Iterator[Dict[str, Any]]:
    try:
        value = _loads(text)
    except (ValueError, RecursionError):
        # JSONL permits one complete object per nonempty line. A malformed
        # multiline object still fails this stricter per-line parser.
        lines = tuple(line for line in text.splitlines() if line.strip())
        if not lines:
            raise ExtractionError("Empty outer JSON document") from None
        try:
            value = [_loads(line) for line in lines]
        except (ValueError, RecursionError):
            raise ExtractionError("Invalid outer JSON document") from None
    records = value if isinstance(value, list) else [value]
    for record in records:
        if not isinstance(record, dict):
            raise ExtractionError("Each outer JSON record must be an object")
        yield record


def _payload(envelope: Dict[str, Any], limit: int, compression: str):
    encoded = envelope.get("data_base64")
    if not isinstance(encoded, str) or not encoded:
        raise ExtractionError("Each outer JSON record requires nonempty data_base64")
    try:
        compressed = base64.b64decode(encoded, validate=True)
    except (ValueError, binascii.Error):
        raise ExtractionError("Invalid data_base64 encoding") from None
    text = _utf8(_inflate(compressed, limit, compression, "payload"), "payload")
    try:
        value = _loads(text)
    except (ValueError, RecursionError):
        raise ExtractionError("Invalid payload JSON document") from None
    if not isinstance(value, dict):
        raise ExtractionError("Decoded payload JSON must be an object")
    return value, text


def decode_records(
    compressed: bytes,
    *,
    max_outer_bytes: int,
    max_payload_bytes: int,
    compression: str = "auto",
) -> Iterator[Tuple[Dict[str, Any], Dict[str, Any], str]]:
    """Decode envelopes and payload objects without losing decimal precision.

    The outer stream must be a single gzip member, containing a JSON object,
    array of objects, or JSONL. Each payload must be gzip or zlib compressed.
    Limits apply to decompressed bytes; trailing compressed data is rejected.
    Payload text is returned unchanged. Parsed fractional numbers are Decimal;
    use :func:`dumps_json` to serialize the complete envelope without rounding.
    """
    for limit in (max_outer_bytes, max_payload_bytes):
        if type(limit) is not int or limit <= 0:
            raise ValueError("Decompression limits must be positive integers")
    if compression not in ("auto", "gzip", "zlib"):
        raise ValueError("Payload compression must be auto, gzip, or zlib")
    outer = _utf8(_inflate(compressed, max_outer_bytes, "gzip", "outer"), "outer")
    for envelope in _outer_records(outer):
        payload, text = _payload(envelope, max_payload_bytes, compression)
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
