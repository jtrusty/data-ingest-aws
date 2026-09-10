"""Settings for immutable POS events stored in hourly S3 prefixes."""

import re
from dataclasses import dataclass, fields
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from data_ingest.exceptions import ConfigurationError


@dataclass(frozen=True)
class ParPosS3Config:
    location: str
    start_at: str
    folder_timezone: str = "UTC"
    compression: str = "auto"
    safety_delay_seconds: int = 120
    max_object_bytes: int = 32 * 1024 * 1024
    max_outer_bytes: int = 128 * 1024 * 1024
    max_payload_bytes: int = 16 * 1024 * 1024
    order_id_path: Optional[str] = None


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


def parse_s3_config(data):
    if not isinstance(data, dict):
        raise ConfigurationError("par_pos_s3 tables require an s3 settings mapping")
    unknown = set(data) - {f.name for f in fields(ParPosS3Config)}
    if unknown:
        raise ConfigurationError(f"Unknown s3 setting(s): {', '.join(sorted(unknown))}")
    for name in ("location", "start_at"):
        if not data.get(name):
            raise ConfigurationError(f"s3.{name} is required")
    location = data["location"]
    if not isinstance(location, str) or not re.fullmatch(r"s3://[a-z0-9][a-z0-9.-]*(?:/[^\x00-\x1f]*)?", location):
        raise ConfigurationError("s3.location must be an s3://bucket/prefix URI")
    settings = ParPosS3Config(**{**data, "location": location.rstrip("/")})
    _validate_start(settings.start_at)
    _validate_limits(settings)
    try:
        ZoneInfo(settings.folder_timezone)
    except (TypeError, ValueError, ZoneInfoNotFoundError):
        raise ConfigurationError("s3.folder_timezone must be a valid IANA timezone") from None
    if settings.compression not in ("auto", "gzip", "zlib"):
        raise ConfigurationError("s3.compression must be auto, gzip, or zlib")
    path = settings.order_id_path
    if path is not None and (
        not isinstance(path, str) or not path or any(not p for p in path.split("."))
    ):
        raise ConfigurationError("s3.order_id_path must be a nonempty dotted field path")
    return settings
