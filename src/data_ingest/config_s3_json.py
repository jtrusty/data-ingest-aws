"""
Declarative settings for JSON documents in S3.

Four concepts, each with a deliberately small allowlist, because ".json.gz"
names a compression and not a format. Everything past gzip -- whether the
document is an object, an array or JSONL, whether records carry an envelope,
where the payload lives, and how it is encoded and compressed -- varies per
producer and therefore belongs in configuration:

    discovery   how files are found
    document    how a file is opened and records identified
    payload     where the payload is and how it decodes

What the payload MEANS -- which field identifies a business record, which
one versions it -- is deliberately NOT here. Bronze's contract ends at "I
received this event and decoded its JSON faithfully"; interpretation is
Silver's, where it can change without re-landing anything.

What is NOT configurable is the safety: bounded decompression, validated
base64, exact Decimal preservation, and full retention of both the envelope
and the payload as JSON. Those are framework guarantees, not feed settings.
"""

import re
from dataclasses import dataclass, field, fields, replace
from datetime import datetime
from types import MappingProxyType
from typing import Mapping, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from data_ingest.exceptions import ConfigurationError

# CloudEvents core attributes. Landed as columns only when the document
# declares `preset: cloudevents` -- a generic JSON reader has no business
# manufacturing event_* columns for a feed that is not CloudEvents.
_CLOUDEVENTS_CORE = (
    ("event_type", "type"),
    ("event_specversion", "specversion"),
    ("event_source", "source"),
    ("event_id", "id"),
    ("event_time", "time"),
    ("data_content_type", "datacontenttype"),
)

# A preset is shorthand for a wire format that already has a name. It only
# supplies defaults; anything stated explicitly alongside it still wins.
_PRESETS = {
    "cloudevents": {
        "envelope": "cloudevents",
        "payload": {"path": "data_base64", "encoding": "base64",
                    "compression": "auto", "format": "json"},
    },
    "cloudevents_plain": {
        "envelope": "cloudevents",
        "payload": {"path": "data", "encoding": "none",
                    "compression": "none", "format": "json"},
    },
    # No envelope at all: the outer records are the business objects.
    "records": {
        "envelope": "none",
        "payload": {"path": None, "encoding": "none",
                    "compression": "none", "format": "json"},
    },
}

_ENCODINGS = ("none", "base64")
_PAYLOAD_COMPRESSIONS = ("none", "gzip", "zlib", "auto")
_DOCUMENT_COMPRESSIONS = ("none", "gzip", "zlib", "auto")
_FRAMINGS = ("object", "array", "jsonl", "auto")
_FORMATS = ("json",)
_ENVELOPES = ("none", "cloudevents")
_DISCOVERY_TYPES = ("time_partitioned",)

_COLUMN = re.compile(r"[a-z_][a-z0-9_]*")
# Landed for every row; a configured column colliding with one of these would
# be silently overwritten by the adapter.
_RESERVED_COLUMNS = frozenset({
    "_source_record_id", "_s3_bucket", "_s3_key", "_s3_etag", "_s3_record_index",
    "_s3_last_modified", "envelope_json", "payload_json",
    *(column for column, _ in _CLOUDEVENTS_CORE),
})


@dataclass(frozen=True)
class DiscoveryConfig:
    """How files are found. Only time-partitioned prefixes for now."""

    type: str = "time_partitioned"
    path_format: str = "%Y/%m/%d/%H"
    timezone: str = "UTC"
    suffix: str = ".json.gz"
    safety_delay_seconds: int = 120


@dataclass(frozen=True)
class PayloadConfig:
    """Where a record's payload is, and how to get from bytes to JSON."""

    path: Optional[str] = None
    encoding: str = "none"
    compression: str = "none"
    format: str = "json"


@dataclass(frozen=True)
class DocumentConfig:
    """How one S3 object is opened and split into records."""

    compression: str = "gzip"
    records: str = "auto"
    envelope: str = "none"
    payload: PayloadConfig = field(default_factory=PayloadConfig)
    max_object_bytes: int = 32 * 1024 * 1024
    max_outer_bytes: int = 128 * 1024 * 1024
    max_payload_bytes: int = 16 * 1024 * 1024

    @property
    def core_envelope_fields(self):
        return _CLOUDEVENTS_CORE if self.envelope == "cloudevents" else ()


@dataclass(frozen=True)
class S3JsonConfig:
    """One table's full view: source-level wiring plus its own settings."""

    location: str
    start_at: str
    discovery: DiscoveryConfig = field(default_factory=DiscoveryConfig)
    document: DocumentConfig = field(default_factory=DocumentConfig)
    envelope_fields: Mapping[str, str] = field(default_factory=dict)

    @property
    def envelope_columns(self):
        """Every envelope column: preset core first, then configured extensions."""
        return (*self.document.core_envelope_fields, *self.envelope_fields.items())

    # Kept so callers do not reach through two levels for the common values.
    @property
    def folder_timezone(self):
        return self.discovery.timezone

    @property
    def safety_delay_seconds(self):
        return self.discovery.safety_delay_seconds


def _known(section, data, dataclass_type):
    if not isinstance(data, dict):
        raise ConfigurationError(f"{section} must be a mapping")
    unknown = set(data) - {f.name for f in fields(dataclass_type)}
    if unknown:
        raise ConfigurationError(f"Unknown {section} setting(s): {', '.join(sorted(unknown))}")
    return data


def _choice(section, name, value, allowed):
    if value not in allowed:
        raise ConfigurationError(f"{section}.{name} must be one of: {', '.join(map(str, allowed))}")


def _positive(section, name, value, maximum):
    if type(value) is not int or not 1 <= value <= maximum:
        raise ConfigurationError(f"{section}.{name} must be an integer from 1 to {maximum}")


def _dotted(section, value):
    if not isinstance(value, str) or not value or any(not part for part in value.split(".")):
        raise ConfigurationError(f"{section} must be a nonempty dotted field path")


def parse_discovery(data):
    settings = DiscoveryConfig(**_known("discovery", data or {}, DiscoveryConfig))
    _choice("discovery", "type", settings.type, _DISCOVERY_TYPES)
    try:
        ZoneInfo(settings.timezone)
    except (TypeError, ValueError, ZoneInfoNotFoundError):
        raise ConfigurationError("discovery.timezone must be a valid IANA timezone") from None
    if not isinstance(settings.suffix, str) or not settings.suffix:
        raise ConfigurationError("discovery.suffix must be a nonempty string")
    _validate_path_format(settings.path_format)
    _positive("discovery", "safety_delay_seconds", settings.safety_delay_seconds, 3600)
    return settings


def _validate_path_format(path_format):
    """
    The prefix walk steps an hour at a time, so the format must actually
    change every hour -- otherwise one prefix would be listed repeatedly and
    the hours in between never listed at all.
    """
    if not isinstance(path_format, str) or not path_format:
        raise ConfigurationError("discovery.path_format must be a nonempty strftime format")
    if "%" not in path_format:
        raise ConfigurationError("discovery.path_format must contain strftime directives")
    base = datetime(2026, 3, 1, 0, 0)
    rendered = {base.replace(hour=hour).strftime(path_format) for hour in range(24)}
    if len(rendered) != 24:
        raise ConfigurationError(
            "discovery.path_format must resolve to a distinct prefix per hour "
            "(include an hour directive such as %H)"
        )


def parse_payload(data):
    settings = PayloadConfig(**_known("document.payload", data or {}, PayloadConfig))
    if settings.path is not None:
        _dotted("document.payload.path", settings.path)
    _choice("document.payload", "encoding", settings.encoding, _ENCODINGS)
    _choice("document.payload", "compression", settings.compression, _PAYLOAD_COMPRESSIONS)
    _choice("document.payload", "format", settings.format, _FORMATS)
    if settings.path is None and (settings.encoding != "none" or settings.compression != "none"):
        raise ConfigurationError(
            "document.payload.path is required when the payload is encoded or compressed"
        )
    return settings


def parse_document(data):
    data = dict(data or {})
    preset = data.pop("preset", None)
    if preset is not None:
        if preset not in _PRESETS:
            raise ConfigurationError(
                f"document.preset must be one of: {', '.join(sorted(_PRESETS))}"
            )
        # Explicit keys win over the preset, so a feed that is CloudEvents
        # apart from one detail does not have to abandon the shorthand.
        defaults = _PRESETS[preset]
        data = {**defaults, **data, "payload": {**defaults["payload"], **(data.get("payload") or {})}}
    payload = parse_payload(data.pop("payload", None))
    settings = DocumentConfig(**_known("document", data, DocumentConfig), payload=payload)
    _choice("document", "compression", settings.compression, _DOCUMENT_COMPRESSIONS)
    _choice("document", "records", settings.records, _FRAMINGS)
    _choice("document", "envelope", settings.envelope, _ENVELOPES)
    for name in ("max_object_bytes", "max_outer_bytes", "max_payload_bytes"):
        _positive("document", name, getattr(settings, name), 512 * 1024 * 1024)
    return settings


def parse_envelope_fields(data):
    """Validate a column -> dotted-path mapping, preserving YAML order."""
    raw = data or {}
    if not isinstance(raw, dict):
        raise ConfigurationError("envelope_fields must be a mapping of column name to field path")
    for column, path in raw.items():
        if not isinstance(column, str) or not _COLUMN.fullmatch(column):
            raise ConfigurationError(
                f"envelope_fields column {column!r} must be lowercase letters, digits "
                f"and underscores -- Athena lowercases identifiers, and Iceberg then "
                f"matches them case-sensitively"
            )
        if column in _RESERVED_COLUMNS:
            raise ConfigurationError(
                f"envelope_fields column {column!r} is reserved: the adapter always lands it"
            )
        _dotted(f"envelope_fields.{column}", path)
    return MappingProxyType(dict(raw))


def parse_location(value, section):
    if not isinstance(value, str) or not re.fullmatch(
        r"s3://[a-z0-9][a-z0-9.-]*(?:/[^\x00-\x1f]*)?", value
    ):
        raise ConfigurationError(f"{section} must be an s3://bucket/prefix URI")
    return value.rstrip("/")


def _validate_start(value):
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None or "T" not in value:
            raise ValueError
    except (AttributeError, TypeError, ValueError):
        raise ConfigurationError("start_at must be a quoted ISO timestamp with timezone") from None


def parse_source_s3(source):
    """Source-level wiring shared by every table: location, discovery, document."""
    return {
        "location": parse_location(source.get("location"), "source.location")
        if source.get("location") else None,
        "discovery": parse_discovery(source.get("discovery")),
        "document": parse_document(source.get("document")),
    }


def build_table_config(table, source_s3):
    """Combine source-level wiring with one table's own settings."""
    location = table.get("location")
    location = parse_location(location, "location") if location else source_s3["location"]
    if not location:
        raise ConfigurationError("source.location is required for s3_json")
    start_at = table.get("start_at")
    if not start_at:
        raise ConfigurationError("start_at is required")
    _validate_start(start_at)
    return S3JsonConfig(
        location=location,
        start_at=start_at,
        discovery=source_s3["discovery"],
        document=source_s3["document"],
        envelope_fields=parse_envelope_fields(table.get("envelope_fields")),
    )
