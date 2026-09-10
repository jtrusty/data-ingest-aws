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
from data_ingest.sources.par_pos_s3 import ParPosS3Source, build_source


def instant(value):
    return datetime.fromisoformat(value).replace(tzinfo=timezone.utc)


def checkpoint(value):
    return WatermarkCheckpoint('_s3_last_modified', value, value_type='TIMESTAMP')


def config(**overrides):
    return SimpleNamespace(**dict(dict(location='s3://pos/orders', start_at='2026-09-10T09:00:00Z',
        folder_timezone='UTC', compression='auto', safety_delay_seconds=120,
        max_object_bytes=33554432, max_outer_bytes=134217728, max_payload_bytes=16777216,
        order_id_path='order.id'), **overrides))


def packed(version=1):
    payload = {'order': {'id': 'order-1'}, 'version': version, 'businessDate': '2026-09-09', 'nested': [1, 2]}
    return {'id': 'event-1', 'time': '2026-09-10T09:20:00Z', 'businessdate': '2026-09-09',
            'type': 'order', 'data_base64': base64.b64encode(gzip.compress(json.dumps(payload).encode())).decode()}


def setup_source(records=None, **settings):
    client = Mock()
    body = io.BytesIO(gzip.compress(json.dumps(records or [packed()]).encode()))
    obj = dict(Key='orders/2026/09/10/09/file.json.gz', ETag='"abc"', LastModified=instant('2026-09-10T09:20:00'), Size=len(body.getvalue()))
    client.get_paginator.return_value.paginate.return_value = [{'Contents': [obj]}]
    client.get_object.return_value = {'Body': body, 'ContentLength': len(body.getvalue())}
    source = ParPosS3Source(config(**settings), lookback_minutes=15, s3_client=client,
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
    assert frame.order_version.tolist() == ['1', '2']
    assert frame.order_id.tolist() == ['order-1', 'order-1']
    assert frame._source_record_id.nunique() == 2
    assert frame._s3_record_index.tolist() == [0, 1]
    assert frame.business_date.tolist() == ['2026-09-09'] * 2
    assert json.loads(frame.payload_json[0])['nested'] == [1, 2]
    assert json.loads(frame.envelope_json[0]) == packed()
    client.get_object.assert_called_once_with(Bucket='pos', Key=obj['Key'], IfMatch='"abc"')
    assert body.closed
    assert source.arrow_schema().field('order_version').type == pa.string()
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

    source, _, _, _ = setup_source([envelope(98765432109876, 1), envelope(98765432109876, 2)],
                                   order_id_path='id')
    frame = list(source.extract(None, source.get_current_checkpoint()))[0]

    assert list(frame['order_id']) == ['98765432109876', '98765432109876']
    assert list(frame['order_version']) == ['1', '2']
    # event_id keeps the full prefixed form, so the two can be reconciled.
    assert list(frame['event_id']) == ['guid:98765432109876'] * 2
    assert all(row.split(':')[-1] == order for row, order in zip(frame['event_id'], frame['order_id']))
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


def test_incremental_prefixes_pages_and_inclusive_bounds():
    source, client, _, obj = setup_source()
    objects = [dict(obj, Key='orders/2026/09/10/09/' + str(i) + '.json.gz', LastModified=instant(t))
               for i, t in enumerate(['2026-09-10T09:14:59', '2026-09-10T09:15:00', '2026-09-10T10:00:00', '2026-09-10T10:00:01'])]
    client.get_paginator.return_value.paginate.side_effect = [
        [{'Contents': objects[:2]}, {'Contents': [dict(obj, Key='ignored.txt')]}], [{'Contents': objects[2:]}]]
    client.get_object.side_effect = lambda **kw: {'Body': io.BytesIO(gzip.compress(json.dumps(packed()).encode()))}
    frames = list(source.extract(checkpoint('2026-09-10 09:30:00.000000'), checkpoint('2026-09-10 10:00:00.000000')))
    assert sum(len(f) for f in frames) == 2
    assert [c.kwargs['Prefix'] for c in client.get_paginator.return_value.paginate.call_args_list] == [
        'orders/2026/09/10/09/', 'orders/2026/09/10/10/']


def test_timezone_folder_and_start_floor():
    source, client, _, _ = setup_source(folder_timezone='America/Chicago')
    client.get_paginator.return_value.paginate.return_value = [{}]
    assert list(source.extract(checkpoint('2026-09-10 09:01:00.000000'), source.get_current_checkpoint())) == []
    assert client.get_paginator.return_value.paginate.call_args.kwargs['Prefix'] == 'orders/2026/09/10/04/'


@pytest.mark.parametrize('version', [None, 123456789012345678901234567890])
def test_nullable_and_large_versions(version):
    source, _, _, _ = setup_source([packed(version)])
    frame = list(source.extract(None, source.get_current_checkpoint()))[0]
    assert frame.order_version[0] == (None if version is None else str(version))


def test_non_scalar_order_identity_fails_without_payload_in_error():
    source, _, _, _ = setup_source([packed({'sensitive': 'secret'})])
    with pytest.raises(ExtractionError) as caught:
        list(source.extract(None, source.get_current_checkpoint()))
    assert 'secret' not in str(caught.value)


@pytest.mark.parametrize('failure', ['listed_size', 'body_size', 'conditional', 'corrupt'])
def test_read_failures_do_not_leak_data_and_close_body(failure):
    source, client, body, obj = setup_source(max_object_bytes=1000)
    if failure == 'listed_size':
        obj['Size'] = 1001
    elif failure == 'body_size':
        body = io.BytesIO(b'x' * 1001)
        client.get_object.return_value = {'Body': body}
    elif failure == 'conditional':
        client.get_object.side_effect = RuntimeError('secret')
    else:
        body = io.BytesIO(b'corrupt secret')
        client.get_object.return_value = {'Body': body}
    with pytest.raises(ExtractionError) as caught:
        list(source.extract(None, source.get_current_checkpoint()))
    assert 'secret' not in str(caught.value)
    if failure in ('body_size', 'corrupt'):
        assert body.closed


def test_batches_have_record_ceiling():
    source, _, _, _ = setup_source([packed(i) for i in range(501)])
    frames = list(source.extract(None, source.get_current_checkpoint()))
    assert [len(f) for f in frames] == [250, 250, 1]


def test_empty_or_future_window_does_not_list():
    source, client, _, _ = setup_source()
    assert list(source.extract(None, checkpoint('2026-09-10 08:00:00.000000'))) == []
    client.get_paginator.assert_not_called()


def test_batch_bytes_limit(monkeypatch):
    source, _, _, _ = setup_source([packed(1), packed(2)])
    monkeypatch.setattr('data_ingest.sources.par_pos_s3._BATCH_BYTES', 1)
    assert [len(f) for f in source.extract(None, source.get_current_checkpoint())] == [1, 1]


def test_missing_order_path_and_metadata():
    source, _, _, _ = setup_source(order_id_path='order.id.missing')
    frame = list(source.extract(None, source.get_current_checkpoint()))[0]
    assert frame.order_id[0] is None
    assert source.metadata() == {'bucket': 'pos', 'prefix': 'orders', 'folder_timezone': 'UTC'}


def test_unconfigured_order_id():
    source, _, _, _ = setup_source(order_id_path=None)
    assert list(source.extract(None, source.get_current_checkpoint()))[0].order_id[0] is None


def test_list_failure_is_safe():
    source, client, _, _ = setup_source()
    client.get_paginator.return_value.paginate.side_effect = RuntimeError('secret')
    with pytest.raises(ExtractionError, match='Failed to list POS') as caught:
        list(source.extract(None, source.get_current_checkpoint()))
    assert 'secret' not in str(caught.value)


def test_response_size_limit_closes_body():
    source, client, body, _ = setup_source()
    client.get_object.return_value['ContentLength'] = source.config.max_object_bytes + 1
    with pytest.raises(ExtractionError):
        list(source.extract(None, source.get_current_checkpoint()))
    assert body.closed


def test_none_checkpoint_does_not_list():
    source, client, _, _ = setup_source()
    assert list(source.extract(None, checkpoint(None))) == []
    client.get_paginator.assert_not_called()


def test_local_prefix_fall_back_is_not_listed_twice():
    source, _, _, _ = setup_source(location='s3://pos', folder_timezone='America/Chicago')
    assert list(source._prefixes(instant('2026-11-01T06:15:00'), instant('2026-11-01T08:15:00'))) == [
        '2026/11/01/01/', '2026/11/01/02/']


def test_factory_uses_iam_client_and_table_settings(monkeypatch):
    client = Mock()
    factory = Mock(return_value=client)
    monkeypatch.setattr('data_ingest.sources.par_pos_s3.boto3.client', factory)
    table = SimpleNamespace(s3=config(), checkpoint=SimpleNamespace(lookback_minutes=20))
    source = build_source({}, table, 17)
    assert source.fetch_size == 17
    assert source.lookback_minutes == 20
    factory.assert_called_once_with('s3')


def test_invalid_fetch_size():
    from data_ingest.exceptions import ConfigurationError
    with pytest.raises(ConfigurationError):
        ParPosS3Source(config(), fetch_size=0)


def test_row_size_cap_has_actionable_safe_reason(monkeypatch):
    source, _, _, _ = setup_source()
    monkeypatch.setattr('data_ingest.sources.par_pos_s3._MAX_ROW_BYTES', 1, raising=False)
    with pytest.raises(ExtractionError, match='row exceeds'):
        list(source.extract(None, source.get_current_checkpoint()))


def test_decode_failure_preserves_safe_reason():
    source, client, _, _ = setup_source()
    client.get_object.return_value = {'Body': io.BytesIO(b'not gzip secret')}
    with pytest.raises(ExtractionError) as caught:
        list(source.extract(None, source.get_current_checkpoint()))
    assert 'Invalid outer compressed stream' in str(caught.value)
    assert 'secret' not in str(caught.value)


def test_half_hour_dst_change_includes_last_local_prefix():
    source, _, _, _ = setup_source(folder_timezone='Australia/Lord_Howe')
    assert list(source._prefixes(instant('2026-04-04T14:00:00'), instant('2026-04-04T15:45:00'))) == [
        'orders/2026/04/05/01/', 'orders/2026/04/05/02/']


@pytest.mark.parametrize('setting,value', [('fetch_size', True), ('fetch_size', 1.5),
                                           ('lookback_minutes', 0), ('lookback_minutes', True),
                                           ('lookback_minutes', 1.5)])
def test_constructor_rejects_invalid_integer_settings(setting, value):
    from data_ingest.exceptions import ConfigurationError
    with pytest.raises(ConfigurationError):
        ParPosS3Source(config(), s3_client=Mock(), **{setting: value})
