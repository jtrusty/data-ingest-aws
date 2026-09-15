"""Bounded extraction of immutable, hourly partitioned gzipped-JSON S3 events.

Reads CloudEvents-shaped records whose payload is a compressed, base64-encoded
JSON document. Which fields beyond the CloudEvents core become columns is
configuration (`envelope_fields` / `payload_fields`), so the adapter carries no
vocabulary from any one producer; `envelope_json` and `payload_json` retain
everything the projection does not select.

Folder hours must correspond to object upload time. The configured lookback
replays recent files; bronze deduplicates source-record identities, retaining
separate publications of the same record. No version comparison occurs here.
"""

import hashlib
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

import boto3
import pandas as pd
import pyarrow as pa

from data_ingest.checkpoints.watermark import WatermarkCheckpoint
from data_ingest.config import split_s3_uri
from data_ingest.exceptions import ConfigurationError, ExtractionError
from data_ingest.sources.base import Source
from data_ingest.sources.json_gz_decode import decode_records, dumps_json

_WATERMARK = '_s3_last_modified'
_BATCH_BYTES = 16 * 1024 * 1024
_MAX_ROW_BYTES = 30 * 1024 * 1024
# Landed for every feed: S3 provenance, then the two JSON columns that keep
# the record whole whatever the configured projection happens to select.
_LINEAGE_COLUMNS = ('_source_record_id', '_s3_bucket', '_s3_key', '_s3_etag')
_JSON_COLUMNS = ('envelope_json', 'payload_json')


def _utc(value):
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00')) if isinstance(value, str) else value
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def _scalar(value, field):
    if value is None:
        return None
    if isinstance(value, (str, int, float, Decimal, bool)):
        return str(value)
    raise ExtractionError(f'Field {field} must be a scalar or null')


def _at_path(payload, path):
    current = payload
    for part in path.split('.'):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


class S3JsonGzSource(Source):
    """S3-backed Source using IAM credentials and wall-clock checkpoints."""

    def __init__(self, s3_config, lookback_minutes=15, fetch_size=10_000,
                 s3_client=None, now=None):
        for name, value in (('fetch_size', fetch_size), ('lookback_minutes', lookback_minutes)):
            if type(value) is not int or value <= 0:
                raise ConfigurationError(f'{name} must be a positive integer')
        self.config = s3_config
        self.bucket, self.prefix = split_s3_uri(s3_config.location)
        self.lookback_minutes = lookback_minutes
        self.fetch_size = min(fetch_size, 250)
        self._client = s3_client if s3_client is not None else boto3.client('s3')
        self._now = now or (lambda: datetime.now(timezone.utc))
        self._folder_timezone = ZoneInfo(s3_config.folder_timezone)
        self._start = _utc(s3_config.start_at)
        self._envelope_columns = tuple(s3_config.envelope_columns)
        self._payload_columns = tuple(s3_config.payload_fields.items())
        # Built per table rather than as a module constant: which projected
        # columns exist is now configuration. Order is fixed by the config so
        # the Parquet schema is stable from run to run.
        names = (
            *_LINEAGE_COLUMNS,
            *(column for column, _ in self._envelope_columns),
            *(column for column, _ in self._payload_columns),
            *_JSON_COLUMNS,
        )
        self._schema = pa.schema(
            [(name, pa.string()) for name in names]
            + [('_s3_record_index', pa.int64()), (_WATERMARK, pa.timestamp('us'))]
        )

    def metadata(self):
        return {'bucket': self.bucket, 'prefix': self.prefix,
                'folder_timezone': self.config.folder_timezone,
                'envelope_fields': dict(self.config.envelope_fields),
                'payload_fields': dict(self.config.payload_fields)}

    def arrow_schema(self):
        return self._schema

    def get_current_checkpoint(self):
        high = _utc(self._now()) - timedelta(seconds=self.config.safety_delay_seconds)
        return WatermarkCheckpoint(
            column=_WATERMARK, value=high.strftime('%Y-%m-%d %H:%M:%S.%f'),
            lookback_minutes=self.lookback_minutes, value_type='TIMESTAMP',
        )

    def _prefixes(self, low, high):
        # Round in local time, then step on the UTC timeline. This handles
        # fractional UTC offsets and skips nonexistent daylight-saving hours.
        hour = low.astimezone(self._folder_timezone).replace(minute=0, second=0, microsecond=0)
        cursor = hour.astimezone(timezone.utc)
        previous = None
        while cursor <= high:
            suffix = cursor.astimezone(self._folder_timezone).strftime('%Y/%m/%d/%H/')
            prefix = f'{self.prefix}/{suffix}' if self.prefix else suffix
            # Fall-back repeats a local hour; list its prefix only once.
            if prefix != previous:
                yield prefix
            previous = prefix
            cursor += timedelta(hours=1)
        # A fractional-hour DST shift can leave the final local hour between
        # UTC steps (for example Lord Howe's thirty-minute fall-back).
        suffix = high.astimezone(self._folder_timezone).strftime('%Y/%m/%d/%H/')
        endpoint = f'{self.prefix}/{suffix}' if self.prefix else suffix
        if endpoint != previous:
            yield endpoint

    def _objects(self, low, high):
        paginator = self._client.get_paginator('list_objects_v2')
        for prefix in self._prefixes(low, high):
            try:
                for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                    for item in page.get('Contents', []):
                        if item['Key'].endswith('.json.gz') and low <= _utc(item['LastModified']) <= high:
                            yield item
            except Exception:
                raise ExtractionError(f'Failed to list objects at s3://{self.bucket}/{prefix}') from None

    def _read(self, item):
        limit = self.config.max_object_bytes
        if item.get('Size', 0) > limit:
            raise ExtractionError('S3 object exceeds max_object_bytes')
        response = self._client.get_object(Bucket=self.bucket, Key=item['Key'], IfMatch=item['ETag'])
        body = response['Body']
        try:
            if response.get('ContentLength', 0) > limit:
                raise ExtractionError('S3 object exceeds max_object_bytes')
            compressed = body.read(limit + 1)
            if len(compressed) > limit:
                raise ExtractionError('S3 object exceeds max_object_bytes')
            return compressed
        finally:
            body.close()

    def _row(self, item, ordinal, envelope, payload, payload_json):
        identity = json.dumps((self.bucket, item['Key'], item['ETag'], ordinal),
                              separators=(',', ':'), ensure_ascii=True).encode('utf-8')
        return {
            '_source_record_id': hashlib.sha256(identity).hexdigest(),
            '_s3_bucket': self.bucket, '_s3_key': item['Key'], '_s3_etag': item['ETag'],
            '_s3_record_index': ordinal,
            _WATERMARK: _utc(item['LastModified']).replace(tzinfo=None),
            **{column: _scalar(_at_path(envelope, path), path)
               for column, path in self._envelope_columns},
            **{column: _scalar(_at_path(payload, path), path)
               for column, path in self._payload_columns},
            'envelope_json': dumps_json(envelope), 'payload_json': payload_json,
        }

    def _rows(self, item):
        try:
            compressed = self._read(item)
            records = decode_records(compressed, max_outer_bytes=self.config.max_outer_bytes,
                                     max_payload_bytes=self.config.max_payload_bytes,
                                     compression=self.config.compression)
            for ordinal, (envelope, payload, payload_json) in enumerate(records):
                yield self._row(item, ordinal, envelope, payload, payload_json)
        except ExtractionError as exc:
            raise ExtractionError(
                f'Failed to extract object s3://{self.bucket}/{item["Key"]}: {exc}'
            ) from None
        except Exception:
            # Do not include exception details: SDK/parser errors may contain
            # full response bodies or payload values.
            raise ExtractionError(f'Failed to extract object s3://{self.bucket}/{item["Key"]}') from None

    def extract(self, previous_checkpoint, current_checkpoint):
        if current_checkpoint.value is None:
            return
        high = _utc(current_checkpoint.value)
        low = self._start
        if previous_checkpoint is not None and previous_checkpoint.value is not None:
            low = max(low, _utc(previous_checkpoint.value) - timedelta(minutes=self.lookback_minutes))
        if high < low:
            return
        batch = []
        byte_count = 0
        for item in self._objects(low, high):
            for row in self._rows(item):
                row_bytes = sum(len(value.encode('utf-8')) for value in row.values() if isinstance(value, str))
                if row_bytes > _MAX_ROW_BYTES:
                    raise ExtractionError(
                        f'Row exceeds {_MAX_ROW_BYTES} UTF-8 bytes '
                        f'({row_bytes} bytes) at s3://{self.bucket}/{item["Key"]}'
                    )
                if batch and (len(batch) >= self.fetch_size or byte_count + row_bytes > _BATCH_BYTES):
                    yield pd.DataFrame(batch, columns=self._schema.names)
                    batch = []
                    byte_count = 0
                batch.append(row)
                byte_count += row_bytes
        if batch:
            yield pd.DataFrame(batch, columns=self._schema.names)


def build_source(credentials, table_config, fetch_size):
    """Registry factory: boto3 obtains credentials from the Glue IAM role."""
    return S3JsonGzSource(table_config.s3, lookback_minutes=table_config.checkpoint.lookback_minutes,
                       fetch_size=fetch_size)
