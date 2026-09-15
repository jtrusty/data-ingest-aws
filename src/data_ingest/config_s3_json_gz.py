"""Settings for immutable gzipped-JSON events in hourly S3 prefixes."""

import re
from dataclasses import dataclass, field, fields
from datetime import datetime
from types import MappingProxyType
from typing import Mapping
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from data_ingest.exceptions import ConfigurationError


# CloudEvents core attributes, landed as columns for every feed because the
# spec guarantees them. Anything beyond these is a producer extension and is
# configured per table via envelope_fields.
_CORE_ENVELOPE_FIELDS = (
    ("event_type", "type"),
    ("event_specversion", "specversion"),
    ("event_source", "source"),
    ("event_id", "id"),
    ("event_time", "time"),
    ("data_content_type", "datacontenttype"),
)


@dataclass(frozen=True)
class S3JsonGzConfig:
    location: str
    start_at: str
    folder_timezone: str = "UTC"
    compression: str = "auto"
    safety_delay_seconds: int = 120
    max_object_bytes: int = 32 * 1024 * 1024
    max_outer_bytes: int = 128 * 1024 * 1024
    max_payload_bytes: int = 16 * 1024 * 1024
    # column -> dotted path. envelope_fields reads the outer record and
    # covers producer extensions past the CloudEvents core above;
    # payload_fields reads the decoded data_base64 document. Both are
    # ordered so the Parquet schema is stable across runs.
    envelope_fields: Mapping[str, str] = field(default_factory=dict)
    payload_fields: Mapping[str, str] = field(default_factory=dict)

    @property
    def envelope_columns(self):
        """Every envelope column, CloudEvents core first."""
        return (*_CORE_ENVELOPE_FIELDS, *self.envelope_fields.items())


def _validate_start(value):
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None or "T" not in value:
            raise ValueError
    except (AttributeError, TypeError, ValueError):
        raise ConfigurationError("s3.start_at must be a quoted ISO timestamp with timezone") from None


def _validate_limits(settings):
    for name in ("max_object_bytes", "max_outer_bytes", "max_payload_bytes"):
        value = getattr(settings, name)
        if type(value) is not int or not 1 <= value <= 512 * 1024 * 1024:
            raise ConfigurationError(f"s3.{name} must be an integer from 1 to 536870912")
    delay = settings.safety_delay_seconds
    if type(delay) is not int or not 1 <= delay <= 3600:
        raise ConfigurationError("s3.safety_delay_seconds must be an integer from 1 to 3600")


# Landed for every row regardless of config; a configured column that
# collided with one of these would be silently overwritten by the adapter.
_RESERVED_COLUMNS = frozenset({
    "_source_record_id", "_s3_bucket", "_s3_key", "_s3_etag", "_s3_record_index",
    "_s3_last_modified", "envelope_json", "payload_json",
    *(column for column, _ in _CORE_ENVELOPE_FIELDS),
})
_COLUMN = re.compile(r"[a-z_][a-z0-9_]*")


def _field_map(data, key):
    """Validate a column -> dotted-path mapping, preserving YAML order."""
    raw = data.get(key) or {}
    if not isinstance(raw, dict):
        raise ConfigurationError(f"s3.{key} must be a mapping of column name to field path")
    for column, path in raw.items():
        if not isinstance(column, str) or not _COLUMN.fullmatch(column):
            raise ConfigurationError(
                f"s3.{key} column {column!r} must be lowercase letters, digits and "
                f"underscores -- Athena lowercases identifiers, and Iceberg then "
                f"matches them case-sensitively"
            )
        if column in _RESERVED_COLUMNS:
            raise ConfigurationError(
                f"s3.{key} column {column!r} is reserved: the adapter always lands it"
            )
        if not isinstance(path, str) or not path or any(not part for part in path.split(".")):
            raise ConfigurationError(
                f"s3.{key}.{column} must be a nonempty dotted field path"
            )
    return raw


def parse_s3_config(data, default_location=None):
    if data is None and default_location:
        # Everything this table needs can come from source-level defaults.
        data = {}
    if not isinstance(data, dict):
        raise ConfigurationError("s3_json_gz tables require an s3 settings mapping")
    unknown = set(data) - {f.name for f in fields(S3JsonGzConfig)}
    if unknown:
        raise ConfigurationError(f"Unknown s3 setting(s): {', '.join(sorted(unknown))}")
    data = {**data, "location": data.get("location") or default_location}
    if not data["location"]:
        raise ConfigurationError(
            "source.location is required for s3_json_gz (or s3.location on the table)"
        )
    if not data.get("start_at"):
        raise ConfigurationError("s3.start_at is required")
    location = data["location"]
    if not isinstance(location, str) or not re.fullmatch(r"s3://[a-z0-9][a-z0-9.-]*(?:/[^\x00-\x1f]*)?", location):
        raise ConfigurationError("s3.location must be an s3://bucket/prefix URI")
    settings = S3JsonGzConfig(**{
        **data,
        "location": location.rstrip("/"),
        # Frozen so a shared config object cannot be mutated per table, and
        # so the column order that defines the Parquet schema is fixed.
        "envelope_fields": MappingProxyType(dict(_field_map(data, "envelope_fields"))),
        "payload_fields": MappingProxyType(dict(_field_map(data, "payload_fields"))),
    })
    _validate_start(settings.start_at)
    _validate_limits(settings)
    try:
        ZoneInfo(settings.folder_timezone)
    except (TypeError, ValueError, ZoneInfoNotFoundError):
        raise ConfigurationError("s3.folder_timezone must be a valid IANA timezone") from None
    if settings.compression not in ("auto", "gzip", "zlib"):
        raise ConfigurationError("s3.compression must be auto, gzip, or zlib")
    return settings
