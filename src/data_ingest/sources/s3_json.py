"""Bounded extraction of immutable, time-partitioned JSON documents in S3.

The wire format is declared, not assumed: prefix layout and file suffix come
from `discovery`, and how a file is opened, framed, and where its payload
lives come from `document`. So the adapter carries no vocabulary from any one
producer -- CloudEvents is one preset among others, not a built-in assumption.

Envelope attributes become columns via `envelope_fields`. The payload is
landed whole as `payload_json` and the outer record as `envelope_json`, so
promoting payload fields to their own columns stays a Silver decision that
never requires re-landing.

Folder hours must correspond to object upload time. The configured lookback
replays recent files; bronze deduplicates source-record identities, retaining
separate publications of the same record. No version comparison occurs here.
"""

import hashlib
import json
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

import boto3
import pandas as pd
import pyarrow as pa

from data_ingest.checkpoints.watermark import WatermarkCheckpoint
from data_ingest.config import split_s3_uri
from data_ingest.exceptions import ConfigurationError, ExtractionError
from data_ingest.logging import get_logger
from data_ingest.sources.base import Source
from data_ingest.sources.json_decode import _at_path, decode_records, dumps_json

logger = get_logger(__name__)

_WATERMARK = '_s3_last_modified'
_MAX_ROW_BYTES = 30 * 1024 * 1024
# Rows per Parquet part. fetch_size is a per-source config value shared with
# Snowflake, where 10k is right for a cursor fetch; here rows are whole JSON
# documents, so this cap is what keeps a part inside its byte budget. Part
# count is the cost that matters downstream, so this is deliberately not
# tiny -- see DocumentConfig.batch_bytes.
_MAX_FETCH_SIZE = 5_000
# Landed for every feed: S3 provenance, then the two JSON columns that keep
# the record whole whatever the configured projection happens to select.
_LINEAGE_COLUMNS = ('_source_record_id', '_s3_bucket', '_s3_key', '_s3_etag')
# The payload is landed whole, as one column. Promoting individual payload
# fields to columns is deliberately a Silver concern: the landing writer pins
# one Parquet schema per run from its first batch, so a key that first appears
# midway through a run would not fit it, the run would be flagged schema_drift,
# and Bronze refuses a drifted run outright. Keeping payload_json complete
# means that decision can be revisited without ever re-landing.
_JSON_COLUMNS = ('envelope_json', 'payload_json')


def _utc(value):
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00')) if isinstance(value, str) else value
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def _value(value, _field):
    """
    Project one payload/envelope value to a string column.

    Objects and arrays are serialized as exact JSON rather than rejected: a
    real feed has nested structure, and refusing it would mean either failing
    the run or leaving the field unmapped. As JSON text it stays queryable in
    Athena (`json_extract_scalar`, or `CAST(json_parse(col) AS ARRAY(ROW(...)))`
    to UNNEST line items in Silver) and it round-trips losslessly, because
    dumps_json emits Decimals as exact numeric tokens.

    Everything is a string column. That is deliberate: Bronze fails a load
    outright when a column's type changes between runs, and a feed that sends
    42 one day and "42" the next would otherwise stop ingestion. Typing
    belongs in Silver, where a cast can be fixed without re-landing.
    """
    if value is None:
        return None
    if isinstance(value, (dict, list, tuple)):
        return dumps_json(value)
    if isinstance(value, bool):
        # Before the int branch: bool is a subclass of int, and str(True)
        # would otherwise be reached only by accident of ordering.
        return 'true' if value else 'false'
    if isinstance(value, (str, int, float, Decimal)):
        return str(value)
    raise ExtractionError(f'Unsupported JSON value type {type(value).__name__}')


def _row_bytes(row):
    """
    UTF-8 size of a row's string data, for the Athena row-size guard.

    str.encode() would allocate a full copy of every payload just to measure
    it. For ASCII text -- which all JSON from dumps_json is, and most payloads
    are -- the character count IS the byte count, and isascii() is a scan with
    no allocation. Only genuinely non-ASCII strings pay for the encode.
    """
    total = 0
    for value in row.values():
        if isinstance(value, str):
            total += len(value) if value.isascii() else len(value.encode('utf-8'))
    return total


class S3JsonSource(Source):
    """S3-backed Source using IAM credentials and wall-clock checkpoints."""

    def __init__(self, s3_config, lookback_minutes=15, fetch_size=10_000,
                 s3_client=None, now=None):
        for name, value in (('fetch_size', fetch_size), ('lookback_minutes', lookback_minutes)):
            if type(value) is not int or value <= 0:
                raise ConfigurationError(f'{name} must be a positive integer')
        self.config = s3_config
        self.bucket, self.prefix = split_s3_uri(s3_config.location)
        self.lookback_minutes = lookback_minutes
        self.fetch_size = min(fetch_size, _MAX_FETCH_SIZE)
        self._client = s3_client if s3_client is not None else boto3.client('s3')
        self._now = now or (lambda: datetime.now(timezone.utc))
        self._folder_timezone = ZoneInfo(s3_config.folder_timezone)
        self._start = _utc(s3_config.start_at)
        self._envelope_columns = tuple(s3_config.envelope_columns)
        self._document = s3_config.document
        self._discovery = s3_config.discovery
        self._cap_bound_last_run = False
        # Built per table rather than as a module constant: which envelope
        # columns exist is configuration. Order is fixed by the config so the
        # Parquet schema is stable from run to run.
        names = (
            *_LINEAGE_COLUMNS,
            *(column for column, _ in self._envelope_columns),
            *_JSON_COLUMNS,
        )
        self._schema = pa.schema(
            [(name, pa.string()) for name in names]
            + [('_s3_record_index', pa.int64()), (_WATERMARK, pa.timestamp('us'))]
        )

    def metadata(self):
        # Recorded in the manifest so a landed run can be read back and its
        # wire format recovered without the config file.
        payload = self._document.payload
        return {
            'bucket': self.bucket, 'prefix': self.prefix,
            'folder_timezone': self._discovery.timezone,
            'path_format': self._discovery.path_format,
            'suffix': self._discovery.suffix,
            'document': {'compression': self._document.compression,
                         'records': self._document.records,
                         'envelope': self._document.envelope,
                         'payload': {'path': payload.path, 'encoding': payload.encoding,
                                     'compression': payload.compression,
                                     'format': payload.format}},
            'envelope_fields': dict(self.config.envelope_fields),
        }

    def arrow_schema(self):
        return self._schema

    def _uncapped_high(self):
        return _utc(self._now()) - timedelta(seconds=self.config.safety_delay_seconds)

    def get_current_checkpoint(self, previous_checkpoint=None):
        high = self._uncapped_high()
        cap_hours = self._discovery.max_window_hours
        # Decided HERE, once, and remembered: is_caught_up must not consult
        # the clock again. The bound is "now minus safety"; by the time the
        # run has landed and committed, "now" has moved on, so re-reading it
        # makes the committed bound look stale forever and the job cycles
        # through empty windows a few seconds wide until someone kills it.
        self._cap_bound_last_run = False
        if cap_hours is not None:
            # Bound the run to a window past where the last one stopped. The
            # pipeline commits THIS value after the manifest, so the cap has
            # to live here, not in extract(): a run that fetched a smaller
            # window than it declared would silently skip the remainder.
            resume = (_utc(previous_checkpoint.value)
                      if previous_checkpoint is not None and previous_checkpoint.value is not None
                      else self._start)
            capped = resume + timedelta(hours=cap_hours)
            if capped < high:
                high = capped
                self._cap_bound_last_run = True
        return WatermarkCheckpoint(
            column=_WATERMARK, value=high.strftime('%Y-%m-%d %H:%M:%S.%f'),
            lookback_minutes=self.lookback_minutes, value_type='TIMESTAMP',
        )

    def is_caught_up(self, committed_checkpoint):
        # More is available only if the CAP, not the clock, bounded the run
        # just committed. Instance state rather than a fresh comparison, for
        # the reason in get_current_checkpoint.
        return not self._cap_bound_last_run

    def _path(self, moment):
        """The configured prefix for one instant, in the folder timezone."""
        rendered = moment.astimezone(self._folder_timezone).strftime(self._discovery.path_format)
        return rendered if rendered.endswith('/') else rendered + '/'

    def _prefixes(self, low, high):
        # Round in local time, then step on the UTC timeline. This handles
        # fractional UTC offsets and skips nonexistent daylight-saving hours.
        hour = low.astimezone(self._folder_timezone).replace(minute=0, second=0, microsecond=0)
        cursor = hour.astimezone(timezone.utc)
        # Walk past `high` so a folder named ahead of S3's clock is listed in
        # the run whose LastModified window its objects fall in.
        high = high + timedelta(hours=self._discovery.lookahead_hours)
        previous = None
        while cursor <= high:
            suffix = self._path(cursor)
            prefix = f'{self.prefix}/{suffix}' if self.prefix else suffix
            # Fall-back repeats a local hour; list its prefix only once.
            if prefix != previous:
                yield prefix
            previous = prefix
            cursor += timedelta(hours=1)
        # A fractional-hour DST shift can leave the final local hour between
        # UTC steps (for example Lord Howe's thirty-minute fall-back).
        suffix = self._path(high)
        endpoint = f'{self.prefix}/{suffix}' if self.prefix else suffix
        if endpoint != previous:
            yield endpoint

    def _objects(self, walk_from, low, high, inclusive):
        """
        Objects with LastModified in the run's window, from the folders the
        walk covers.

        The walk starts at `walk_from` (previous high minus the lookback) so a
        folder can still be listed after its hour has closed. The OBJECT window
        starts at `low` = the previous high, exclusive: anything at or before it
        was inside the previous run's window, and with S3's list-after-put
        consistency plus the safety delay, it was listed then. Re-fetching it
        would only produce rows Bronze deduplicates away. The first run has no
        previous high and includes its start instant.
        """
        paginator = self._client.get_paginator('list_objects_v2')
        for prefix in self._prefixes(walk_from, high):
            listed = matched = 0
            try:
                for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                    for item in page.get('Contents', []):
                        listed += 1
                        if not item['Key'].endswith(self._discovery.suffix):
                            continue
                        modified = _utc(item['LastModified'])
                        if modified > high or modified < low or (modified == low and not inclusive):
                            continue
                        matched += 1
                        yield item
            except Exception:
                raise ExtractionError(f'Failed to list objects at s3://{self.bucket}/{prefix}') from None
            # One line per folder walked: where the run is, and how much of
            # the folder fell inside the window. On a backfill this is the
            # progress indicator; on a steady-state run, the last few lines
            # show the lookback and lookahead folders being (mostly) empty.
            logger.info('Listed s3://%s/%s: %s objects, %s in window', self.bucket, prefix, listed, matched)

    def _read(self, item):
        try:
            return self._read_object(item)
        except ExtractionError as exc:
            raise ExtractionError(
                f'Failed to extract object s3://{self.bucket}/{item["Key"]}: {exc}'
            ) from None
        except Exception:
            # No SDK detail: an error body can echo the request or the object.
            raise ExtractionError(f'Failed to extract object s3://{self.bucket}/{item["Key"]}') from None

    def _read_object(self, item):
        limit = self._document.max_object_bytes
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
            **{column: _value(_at_path(envelope, path), path)
               for column, path in self._envelope_columns},
            # With no payload path the record IS the payload, and the decoder
            # already serialized it once; dumps_json is a pure-Python walk, so
            # don't pay for a byte-identical second copy.
            'envelope_json': payload_json if envelope is payload else dumps_json(envelope),
            'payload_json': payload_json,
        }

    def _prefetched(self, items):
        """
        (item, compressed bytes) in listing order, with reads running ahead.

        A sliding window rather than executor.map over the whole listing:
        map would submit every GET at once and hold every body in memory. At
        most `prefetch` bodies are in flight, so memory is bounded and a
        backfill over weeks of prefixes does not balloon.
        """
        depth = self._discovery.prefetch
        pending = deque()
        with ThreadPoolExecutor(max_workers=depth) as pool:
            try:
                for item in items:
                    pending.append((item, pool.submit(self._read, item)))
                    if len(pending) >= depth:
                        head, future = pending.popleft()
                        yield head, future.result()
                while pending:
                    head, future = pending.popleft()
                    yield head, future.result()
            finally:
                # A failure mid-run must not leave the pool finishing GETs
                # whose bodies nothing will read.
                for _, future in pending:
                    future.cancel()

    def _rows(self, item, compressed):
        try:
            records = decode_records(compressed, document=self._document)
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
        if previous_checkpoint is not None and previous_checkpoint.value is not None:
            # Incremental: objects strictly after the previous high; folders
            # walked from `lookback` before it so late arrivals into a closed
            # hour are still listed.
            low, inclusive = _utc(previous_checkpoint.value), False
            walk_from = max(self._start, low - timedelta(minutes=self.lookback_minutes))
        else:
            low, inclusive = self._start, True
            walk_from = self._start
        if high < low:
            return
        logger.info('Window (%s, %s] from %s; folders walked from %s', low.isoformat(),
                    high.isoformat(), 'start_at' if inclusive else 'previous checkpoint',
                    walk_from.isoformat())
        objects = rows = 0
        batch = []
        byte_count = 0
        items = self._objects(walk_from, low, high, inclusive)
        for item, compressed in self._prefetched(items):
            objects += 1
            for row in self._rows(item, compressed):
                rows += 1
                row_bytes = _row_bytes(row)
                if row_bytes > _MAX_ROW_BYTES:
                    raise ExtractionError(
                        f'Row exceeds {_MAX_ROW_BYTES} UTF-8 bytes '
                        f'({row_bytes} bytes) at s3://{self.bucket}/{item["Key"]}'
                    )
                if batch and (len(batch) >= self.fetch_size or byte_count + row_bytes > self._document.batch_bytes):
                    yield pd.DataFrame(batch, columns=self._schema.names)
                    batch = []
                    byte_count = 0
                batch.append(row)
                byte_count += row_bytes
        if batch:
            yield pd.DataFrame(batch, columns=self._schema.names)
        logger.info('Window complete: %s objects, %s rows', objects, rows)


def build_source(credentials, table_config, fetch_size):
    """Registry factory: boto3 obtains credentials from the Glue IAM role."""
    return S3JsonSource(table_config.s3, lookback_minutes=table_config.checkpoint.lookback_minutes,
                       fetch_size=fetch_size)
