import base64
import gzip
import io
import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pyarrow as pa
import pytest

from data_ingest.checkpoints.watermark import WatermarkCheckpoint
from data_ingest.exceptions import ExtractionError
from data_ingest.config_s3_json import build_table_config, parse_source_s3
from data_ingest.sources.s3_json import S3JsonSource, build_source


def instant(value):
    return datetime.fromisoformat(value).replace(tzinfo=timezone.utc)


def checkpoint(value):
    return WatermarkCheckpoint('_s3_last_modified', value, value_type='TIMESTAMP')


ENVELOPE_FIELDS = {'group_id': 'groupid', 'business_date': 'businessdate',
                   'historical_data_type': 'historicaldatatype'}


_DISCOVERY_KEYS = {'timezone', 'path_format', 'suffix', 'safety_delay_seconds', 'prefetch', 'lookahead_hours'}
_DOCUMENT_KEYS = {'compression', 'records', 'envelope', 'preset',
                  'max_object_bytes', 'max_outer_bytes', 'max_payload_bytes'}


def config(**settings):
    """
    Build through the real parser rather than a stand-in. discovery/document
    decide the Arrow schema and the whole decode pipeline, so a hand-rolled
    namespace that drifted from the parser would test a shape config can no
    longer produce.
    """
    discovery = {'timezone': 'UTC', 'safety_delay_seconds': 120}
    document = {'preset': 'cloudevents'}
    table = {'name': 'orders', 'start_at': '2026-09-10T09:00:00Z',
             'envelope_fields': dict(ENVELOPE_FIELDS)}
    table['location'] = settings.pop('location', 's3://pos/orders')
    payload = settings.pop('payload', None)
    for key, value in settings.items():
        target = discovery if key in _DISCOVERY_KEYS else (
            document if key in _DOCUMENT_KEYS else table)
        target[key] = value
    if payload:
        document['payload'] = payload
    source_s3 = parse_source_s3({'discovery': discovery, 'document': document})
    return build_table_config(table, source_s3)


def packed_payload(payload_json):
    """`payload_json` is JSON TEXT, so tests control the exact numeric tokens."""
    if not isinstance(payload_json, str):
        payload_json = json.dumps(payload_json)
    return {'id': 'event-1', 'time': '2026-09-10T09:20:00Z', 'type': 'order',
            'data_base64': base64.b64encode(gzip.compress(payload_json.encode())).decode()}


def packed(version=1):
    payload = {'order': {'id': 'order-1'}, 'version': version, 'businessDate': '2026-09-09', 'nested': [1, 2]}
    return {'id': 'event-1', 'time': '2026-09-10T09:20:00Z', 'businessdate': '2026-09-09',
            'type': 'order', 'data_base64': base64.b64encode(gzip.compress(json.dumps(payload).encode())).decode()}


def setup_source(records=None, **settings):
    client = Mock()
    body = io.BytesIO(gzip.compress(json.dumps(records or [packed()]).encode()))
    obj = dict(Key='orders/2026/09/10/09/file.json.gz', ETag='"abc"', LastModified=instant('2026-09-10T09:20:00'), Size=len(body.getvalue()))
    # Respect Prefix the way S3 does: a key lives under exactly one prefix.
    # Returning the page for every prefix would hand the same object out once
    # per folder walked, which S3 never does.
    client.get_paginator.return_value.paginate.side_effect = (
        lambda **kw: [{'Contents': [obj]}] if obj['Key'].startswith(kw['Prefix']) else [{}])
    client.get_object.return_value = {'Body': body, 'ContentLength': len(body.getvalue())}
    source = S3JsonSource(config(**settings), lookback_minutes=15, s3_client=client,
                         now=lambda: instant('2026-09-10T09:32:00'))
    return source, client, body, obj


def test_checkpoint_is_wall_clock_minus_safety_delay():
    source, _, _, _ = setup_source()
    high = source.get_current_checkpoint()
    assert high.value == '2026-09-10 09:30:00.000000'
    assert high.value_type == 'TIMESTAMP'
    assert high.lookback_minutes == 15


def test_extract_retains_versions_metadata_conditional_read_and_pinned_schema():
    source, client, body, obj = setup_source([packed(1), packed(2)])
    frame = list(source.extract(None, source.get_current_checkpoint()))[0]
    # Payload values are NOT columns: the payload lands whole, so both
    # versions are retained as separate rows distinguished by record identity.
    assert 'order_version' not in frame.columns and 'order_id' not in frame.columns
    assert [json.loads(t)['version'] for t in frame.payload_json] == [1, 2]
    assert frame._source_record_id.nunique() == 2
    assert frame._s3_record_index.tolist() == [0, 1]
    assert frame.business_date.tolist() == ['2026-09-09'] * 2
    assert json.loads(frame.payload_json[0])['nested'] == [1, 2]
    assert json.loads(frame.envelope_json[0]) == packed()
    client.get_object.assert_called_once_with(Bucket='pos', Key=obj['Key'], IfMatch='"abc"')
    assert body.closed
    assert source.arrow_schema().field('payload_json').type == pa.string()
    assert source.arrow_schema().field('_s3_last_modified').type == pa.timestamp('us')
    pa.Table.from_pandas(frame, schema=source.arrow_schema(), preserve_index=False)



def test_producer_shape_keeps_the_fourteen_digit_order_id_exact():
    # The real producer sends an array of envelopes whose `id` is
    # "<guid>:<order_id>", and repeats that 14-digit number, unquoted, as the
    # payload's own `id`. A JSON integer that wide survives only because it is
    # never parsed as a float, and is landed as text rather than an int64.
    def envelope(order_id, version):
        payload = {'id': order_id, 'version': version, 'businessDate': '2026-09-09'}
        return {'id': f'guid:{order_id}', 'type': 'order',
                'data_base64': base64.b64encode(gzip.compress(json.dumps(payload).encode())).decode()}

    source, _, _, _ = setup_source([envelope(98765432109876, 1), envelope(98765432109876, 2)])
    frame = list(source.extract(None, source.get_current_checkpoint()))[0]

    # event_id is a column (CloudEvents core) and keeps the prefixed form;
    # the order id itself lives in payload_json, exact.
    assert list(frame['event_id']) == ['guid:98765432109876'] * 2
    ids = [json.loads(t)['id'] for t in frame['payload_json']]
    assert ids == [98765432109876, 98765432109876]
    assert all(e.split(':')[-1] == str(i) for e, i in zip(frame['event_id'], ids))
    # Both versions of one order are retained as separate rows.
    assert frame['_source_record_id'].nunique() == 2

def test_stable_identity_on_replay_and_distinct_object():
    a, _, _, _ = setup_source()
    b, _, _, _ = setup_source()
    first = list(a.extract(None, a.get_current_checkpoint()))[0]._source_record_id[0]
    assert first == list(b.extract(None, b.get_current_checkpoint()))[0]._source_record_id[0]
    c, _, _, obj = setup_source()
    obj['Key'] = obj['Key'].replace('file', 'next')
    assert first != list(c.extract(None, c.get_current_checkpoint()))[0]._source_record_id[0]


def test_incremental_window_is_exclusive_at_previous_high_inclusive_at_high():
    """
    Objects at or before the previous high were inside the previous run's
    window and, with S3's list-after-put consistency, were listed then.
    Fetching them again only produces rows Bronze deduplicates away. The
    folder walk still reaches back by the lookback so a late arrival into a
    closed hour is listed -- but such an arrival has LastModified AFTER the
    previous high by definition, so it passes the window on its own merits.
    """
    source, client, _, obj = setup_source()
    at = lambda t, key: dict(obj, Key=key, LastModified=instant(t))
    objects = {
        'orders/2026/09/10/09/': [at('2026-09-10T09:29:59', 'orders/2026/09/10/09/a.json.gz'),  # before prev high
                                  at('2026-09-10T09:30:00', 'orders/2026/09/10/09/b.json.gz'),  # == prev high: landed last run
                                  at('2026-09-10T09:45:00', 'orders/2026/09/10/09/c.json.gz'),  # late into a closed hour
                                  dict(obj, Key='orders/2026/09/10/09/ignored.txt')],
        'orders/2026/09/10/10/': [at('2026-09-10T10:00:00', 'orders/2026/09/10/10/d.json.gz'),  # == high: inclusive
                                  at('2026-09-10T10:00:01', 'orders/2026/09/10/10/e.json.gz')], # after high
    }
    client.get_paginator.return_value.paginate.side_effect = (
        lambda **kw: [{'Contents': objects.get(kw['Prefix'], [])}])
    client.get_object.side_effect = lambda **kw: {'Body': io.BytesIO(gzip.compress(json.dumps(packed()).encode()))}
    frames = list(source.extract(checkpoint('2026-09-10 09:30:00.000000'), checkpoint('2026-09-10 10:00:00.000000')))
    landed = [k for f in frames for k in f['_s3_key']]
    assert landed == ['orders/2026/09/10/09/c.json.gz', 'orders/2026/09/10/10/d.json.gz']
    # Walk: floor(09:30 - 15m) = 09, through high = 10, plus one hour of lookahead.
    assert [c.kwargs['Prefix'] for c in client.get_paginator.return_value.paginate.call_args_list] == [
        'orders/2026/09/10/09/', 'orders/2026/09/10/10/', 'orders/2026/09/10/11/']


def test_timezone_folder_and_start_floor():
    source, client, _, _ = setup_source(timezone='America/Chicago')
    client.get_paginator.return_value.paginate.side_effect = None
    client.get_paginator.return_value.paginate.return_value = [{}]
    assert list(source.extract(checkpoint('2026-09-10 09:01:00.000000'), source.get_current_checkpoint())) == []
    prefixes = [c.kwargs['Prefix'] for c in client.get_paginator.return_value.paginate.call_args_list]
    # 09:01 UTC minus the 15m lookback is 08:46, but the walk never starts
    # before start_at (09:00 UTC = 04:00 Chicago): the floor is the FLOOR.
    # Then through high (09:30 UTC, still 04) plus one hour of lookahead.
    assert prefixes == ['orders/2026/09/10/04/', 'orders/2026/09/10/05/']


@pytest.mark.parametrize('version', [None, 123456789012345678901234567890])
def test_payload_json_preserves_nulls_and_unbounded_integers(version):
    # A 30-digit integer exceeds int64; it survives because the payload text
    # is passed through unchanged rather than re-serialized from a parsed value.
    source, _, _, _ = setup_source([packed(version)])
    frame = list(source.extract(None, source.get_current_checkpoint()))[0]
    assert json.loads(frame.payload_json[0])['version'] == version
    if version is not None:
        assert str(version) in frame.payload_json[0]


def test_payload_json_is_exact_including_decimals_and_nesting():
    """
    The payload is landed as the decoded text, unchanged. 0.85 is not
    representable as a float, so a round-trip through one would silently
    change a money value; passing the text through is what prevents it.
    """
    payload = ('{"order":{"id":"order-1"},'
               '"items":[{"sku":"A","qty":2},{"sku":"B","qty":1}],'
               '"totals":{"net":10.1,"tax":0.85},'
               '"void":null,"paid":true}')
    source, _, _, _ = setup_source([packed_payload(payload)])
    frame = list(source.extract(None, source.get_current_checkpoint()))[0]

    assert frame.payload_json[0] == payload
    # Which is what makes the Silver explode possible:
    #   CAST(json_parse(payload_json) AS ...) then UNNEST
    assert [i['sku'] for i in json.loads(frame.payload_json[0])['items']] == ['A', 'B']


def test_an_empty_window_lists_nothing():
    source, client, _, _ = setup_source()
    assert list(source.extract(None, checkpoint('2026-09-10 08:00:00.000000'))) == []
    client.get_paginator.assert_not_called()


def test_batch_bytes_limit(monkeypatch):
    source, _, _, _ = setup_source([packed(1), packed(2)])
    monkeypatch.setattr('data_ingest.sources.s3_json._BATCH_BYTES', 1)
    assert [len(f) for f in source.extract(None, source.get_current_checkpoint())] == [1, 1]


def test_an_envelope_path_that_no_record_has_lands_null_not_an_error():
    # A projection that misses is NOT a failure: producers legitimately omit
    # optional attributes, and envelope_json still holds whatever was there.
    source, _, _, _ = setup_source(envelope_fields={'absent': 'no.such.key'})
    frame = list(source.extract(None, source.get_current_checkpoint()))[0]
    assert frame.absent[0] is None


def test_metadata_records_the_wire_format():
    # The manifest carries this so a landed run can be read back and its
    # decode pipeline recovered without the config file.
    source, _, _, _ = setup_source()
    meta = source.metadata()
    assert meta['bucket'] == 'pos' and meta['prefix'] == 'orders'
    assert meta['path_format'] == '%Y/%m/%d/%H' and meta['suffix'] == '.json.gz'
    assert meta['document']['payload'] == {
        'path': 'data_base64', 'encoding': 'base64', 'compression': 'auto', 'format': 'json'}
    assert meta['envelope_fields'] == ENVELOPE_FIELDS
    # No business identity here: Bronze does not know what an order is.
    assert 'natural_key' not in meta and 'version_field' not in meta


def test_an_empty_projection_still_lands_core_and_json_columns():
    source, _, _, _ = setup_source(envelope_fields={})
    frame = list(source.extract(None, source.get_current_checkpoint()))[0]
    assert 'group_id' not in frame.columns
    assert frame['event_id'][0] == 'event-1'          # CloudEvents preset
    assert json.loads(frame['payload_json'][0])['order']['id'] == 'order-1'


def test_columns_follow_config_order_so_the_schema_is_stable():
    source, _, _, _ = setup_source(envelope_fields={'b_col': 'groupid', 'a_col': 'businessdate'})
    names = source.arrow_schema().names
    assert names.index('b_col') < names.index('a_col')
    # CloudEvents core always precedes the configured extensions.
    assert names.index('event_id') < names.index('b_col')


def test_list_failure_is_safe():
    source, client, _, _ = setup_source()
    client.get_paginator.return_value.paginate.side_effect = RuntimeError('secret')
    with pytest.raises(ExtractionError, match='Failed to list objects') as caught:
        list(source.extract(None, source.get_current_checkpoint()))
    assert 'secret' not in str(caught.value)


def test_response_size_limit_closes_body():
    source, client, body, _ = setup_source()
    client.get_object.return_value['ContentLength'] = source.config.document.max_object_bytes + 1
    with pytest.raises(ExtractionError):
        list(source.extract(None, source.get_current_checkpoint()))
    assert body.closed


def test_none_checkpoint_does_not_list():
    source, client, _, _ = setup_source()
    assert list(source.extract(None, checkpoint(None))) == []
    client.get_paginator.assert_not_called()


def test_local_prefix_fall_back_is_not_listed_twice():
    source, _, _, _ = setup_source(location='s3://pos', timezone='America/Chicago')
    # 06:15 UTC = 01:15 CDT; 08:15 UTC = 02:15 CST after the 07:00 UTC fall-back
    # (the repeated 01:00 local hour is listed once); plus one hour of lookahead.
    assert list(source._prefixes(instant('2026-11-01T06:15:00'), instant('2026-11-01T08:15:00'))) == [
        '2026/11/01/01/', '2026/11/01/02/', '2026/11/01/03/']


def test_factory_uses_iam_client_and_table_settings(monkeypatch):
    client = Mock()
    factory = Mock(return_value=client)
    monkeypatch.setattr('data_ingest.sources.s3_json.boto3.client', factory)
    table = SimpleNamespace(s3=config(), checkpoint=SimpleNamespace(lookback_minutes=20))
    source = build_source({}, table, 17)
    assert source.fetch_size == 17
    assert source.lookback_minutes == 20
    factory.assert_called_once_with('s3')


def test_invalid_fetch_size():
    from data_ingest.exceptions import ConfigurationError
    with pytest.raises(ConfigurationError):
        S3JsonSource(config(), fetch_size=0)


def test_row_size_cap_has_actionable_safe_reason(monkeypatch):
    source, _, _, _ = setup_source()
    monkeypatch.setattr('data_ingest.sources.s3_json._MAX_ROW_BYTES', 1, raising=False)
    with pytest.raises(ExtractionError, match='Row exceeds'):
        list(source.extract(None, source.get_current_checkpoint()))


def test_decode_failure_preserves_safe_reason():
    source, client, _, _ = setup_source()
    client.get_object.return_value = {'Body': io.BytesIO(b'not gzip secret')}
    with pytest.raises(ExtractionError) as caught:
        list(source.extract(None, source.get_current_checkpoint()))
    assert 'Invalid outer compressed stream' in str(caught.value)
    assert 'secret' not in str(caught.value)


def test_half_hour_dst_change_includes_last_local_prefix():
    source, _, _, _ = setup_source(timezone='Australia/Lord_Howe')
    assert list(source._prefixes(instant('2026-04-04T14:00:00'), instant('2026-04-04T15:45:00'))) == [
        'orders/2026/04/05/01/', 'orders/2026/04/05/02/', 'orders/2026/04/05/03/']


@pytest.mark.parametrize('setting,value', [('fetch_size', True), ('fetch_size', 1.5),
                                           ('lookback_minutes', 0), ('lookback_minutes', True),
                                           ('lookback_minutes', 1.5)])
def test_constructor_rejects_invalid_integer_settings(setting, value):
    from data_ingest.exceptions import ConfigurationError
    with pytest.raises(ConfigurationError):
        S3JsonSource(config(), s3_client=Mock(), **{setting: value})


def test_prefetch_preserves_listing_order_and_bounds_in_flight_reads():
    """
    Reads run ahead on a pool; decode stays sequential. Rows must come out in
    listing order regardless of which GET finishes first, and no more than
    `prefetch` bodies may be in flight, or a backfill's memory grows with the
    listing rather than with the depth.

    Deterministic, not sleep-based: every GET blocks on a gate until the
    test has seen `prefetch` of them arrive, which proves the depth is
    reached, then releases them in REVERSE listing order, which proves the
    output order does not come from completion order.
    """
    import threading
    depth = 3
    source, client, _, obj = setup_source(prefetch=depth)
    keys = [f'orders/2026/09/10/09/{i:02d}.json.gz' for i in range(10)]
    client.get_paginator.return_value.paginate.side_effect = (
        lambda **kw: [{'Contents': [dict(obj, Key=k) for k in keys]}] if kw['Prefix'].endswith('/09/') else [{}])

    lock = threading.Lock()
    arrived, gates, peak, in_flight = [], {}, [0], [0]

    def get(**kw):
        gate = threading.Event()
        with lock:
            in_flight[0] += 1
            peak[0] = max(peak[0], in_flight[0])
            arrived.append(kw['Key'])
            gates[kw['Key']] = gate
            # Once the pool is full -- or nothing more can arrive -- release
            # the batch LAST-listed first.
            if len(gates) == depth or len(arrived) == len(keys):
                for key in sorted(gates, reverse=True):
                    gates[key].set()
        gate.wait(timeout=5)
        with lock:
            in_flight[0] -= 1
            gates.pop(kw['Key'], None)
        return {'Body': io.BytesIO(gzip.compress(json.dumps(packed()).encode()))}

    client.get_object.side_effect = get
    frames = list(source.extract(None, source.get_current_checkpoint()))
    assert [k for f in frames for k in f['_s3_key']] == keys
    assert peak[0] == depth
    assert len(arrived) == len(keys)


def test_a_failed_read_is_reported_with_its_key_and_stops_the_run():
    source, client, _, obj = setup_source()
    keys = [f'orders/2026/09/10/09/{i}.json.gz' for i in range(4)]
    client.get_paginator.return_value.paginate.side_effect = (
        lambda **kw: [{'Contents': [dict(obj, Key=k) for k in keys]}] if kw['Prefix'].endswith('/09/') else [{}])

    def get(**kw):
        if kw['Key'].endswith('/2.json.gz'):
            raise RuntimeError('response body with secret contents')
        return {'Body': io.BytesIO(gzip.compress(json.dumps(packed()).encode()))}

    client.get_object.side_effect = get
    with pytest.raises(ExtractionError, match='2.json.gz') as caught:
        list(source.extract(None, source.get_current_checkpoint()))
    assert 'secret' not in str(caught.value)


def test_row_bytes_counts_utf8_bytes_not_characters():
    from data_ingest.sources.s3_json import _row_bytes
    # 'é' is one character and two UTF-8 bytes; the Athena guard is in bytes.
    assert _row_bytes({'a': 'abc', 'b': 'é', 'n': 5, 'z': None}) == 3 + 2
    # ASCII takes the no-allocation path and must agree with a real encode.
    text = '{"id":12345678901234,"items":[{"sku":"A"}]}'
    assert _row_bytes({'payload_json': text}) == len(text.encode('utf-8'))


@pytest.mark.parametrize('setting,value', [
    ('prefetch', 0), ('prefetch', 33), ('prefetch', 2.5),
    ('lookahead_hours', -1), ('lookahead_hours', 25), ('lookahead_hours', True),
])
def test_prefetch_and_lookahead_bounds(setting, value):
    from data_ingest.exceptions import ConfigurationError
    with pytest.raises(ConfigurationError, match=setting):
        config(**{setting: value})


def test_zero_lookahead_walks_only_to_high():
    source, _, _, _ = setup_source(lookahead_hours=0)
    assert list(source._prefixes(instant('2026-09-10T09:00:00'), instant('2026-09-10T09:30:00'))) == [
        'orders/2026/09/10/09/']


def test_records_preset_serializes_each_record_once():
    # With no payload path the record is the payload. The decoder already
    # produced its text; dumps_json is a pure-Python walk, so the row must
    # reuse it rather than build a byte-identical second copy.
    from unittest.mock import patch
    import data_ingest.sources.s3_json as module
    source, client, _, _ = setup_source(preset='records')
    client.get_object.return_value = {
        'Body': io.BytesIO(gzip.compress(b'[{"id": 1}, {"id": 2}]')), 'ContentLength': 30}
    with patch.object(module, 'dumps_json', wraps=module.dumps_json) as spy:
        frame = list(source.extract(None, source.get_current_checkpoint()))[0]
    assert list(frame['envelope_json']) == list(frame['payload_json']) == ['{"id":1}', '{"id":2}']
    assert spy.call_count == 0          # the decoder did it; the row did not repeat it
