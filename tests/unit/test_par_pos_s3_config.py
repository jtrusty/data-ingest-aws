from copy import deepcopy
from pathlib import Path

import pytest
import yaml

from data_ingest.config import parse_config
from data_ingest.exceptions import ConfigurationError


CONFIG = {
    "source": {"name": "restaurant", "type": "par_pos_s3"},
    "tables": [{
        "name": "orders",
        "s3": {
            "location": "s3://pos-events/orders/",
            "start_at": "2026-09-01T00:00:00Z",
        },
    }],
}


def parse(data):
    return parse_config(yaml.safe_dump(data))


def test_s3_source_uses_iam_and_stable_record_identity_without_relational_config():
    config = parse(CONFIG)
    table = config.tables[0]
    assert config.connection.secret_id is None
    assert config.source_key == "restaurant_par_pos_s3"
    assert table.source_object == "s3://pos-events/orders"
    assert table.primary_key == ["_source_record_id"]
    assert table.checkpoint.column == "_s3_last_modified"
    assert table.checkpoint.lookback_minutes == 15
    assert table.s3.folder_timezone == "UTC"
    assert table.s3.compression == "auto"
    assert table.s3.safety_delay_seconds == 120


@pytest.mark.parametrize("field,value", [
    ("location", "https://bucket/orders"),
    ("location", "s3://"),
    ("start_at", "2026-09-01"),
    ("start_at", "2026-09-01T00:00:00"),
    ("start_at", "invalid"),
    ("folder_timezone", "Invalid/Zone"),
    ("compression", "zip"),
    ("max_object_bytes", 0),
    ("max_outer_bytes", True),
    ("max_payload_bytes", 1.5),
    ("safety_delay_seconds", -1),
    ("order_id_path", "order..id"),
    ("order_id_path", ["order", "id"]),
    ("typo", 12),
])
def test_invalid_s3_settings_rejected(field, value):
    data = deepcopy(CONFIG)
    data["tables"][0]["s3"][field] = value
    with pytest.raises(ConfigurationError):
        parse(data)


@pytest.mark.parametrize("field", ["location", "start_at"])
def test_required_s3_settings_rejected(field):
    data = deepcopy(CONFIG)
    del data["tables"][0]["s3"][field]
    with pytest.raises(ConfigurationError, match=field):
        parse(data)


@pytest.mark.parametrize("override", [
    {"primary_key": ["event_id"]},
    {"checkpoint": {"type": "watermark", "column": "order_version"}},
    {"checkpoint": {"type": "watermark", "column": "_s3_last_modified", "lookback_minutes": 0}},
])
def test_s3_discovery_and_replay_identity_cannot_be_replaced_by_business_fields(override):
    data = deepcopy(CONFIG)
    data["tables"][0].update(override)
    with pytest.raises(ConfigurationError):
        parse(data)


def test_s3_explicit_options():
    data = deepcopy(CONFIG)
    data["tables"][0]["s3"].update({
        "folder_timezone": "America/Chicago", "order_id_path": "order.id",
        "compression": "zlib", "max_object_bytes": 4096,
    })
    settings = parse(data).tables[0].s3
    assert settings.folder_timezone == "America/Chicago"
    assert settings.order_id_path == "order.id"
    assert settings.max_object_bytes == 4096


def test_s3_does_not_accept_a_secret():
    data = {**CONFIG, "connection": {"secret_id": "unneeded"}}
    with pytest.raises(ConfigurationError, match="IAM"):
        parse(data)


def test_snowflake_still_requires_connection():
    data = {**CONFIG, "source": {"name": "x", "type": "snowflake"}}
    with pytest.raises(ConfigurationError, match="connection"):
        parse(data)


@pytest.mark.parametrize("value", [None, "s3://pos-events/orders", []])
def test_s3_settings_must_be_a_mapping(value):
    data = deepcopy(CONFIG)
    data["tables"][0]["s3"] = value
    with pytest.raises(ConfigurationError, match="s3 settings mapping"):
        parse(data)


def test_example_is_a_valid_bronze_enabled_config():
    path = Path(__file__).parents[2] / "config" / "par_pos_s3.example.yaml"
    config = parse_config(path.read_text())
    assert config.bronze.database == "bronze_par_pos"
    # One database per source, so the source_key prefix would only be noise.
    # Identity, applied once at CREATE TABLE: pinned so it cannot drift.
    assert config.bronze.table_prefix == "none"
    assert config.tables[0].s3 is not None
    # The producer repeats the envelope's "guid:<order_id>" suffix as the
    # payload's own `id`. Mapping it at ingestion is not cosmetic: Bronze
    # rows already inserted are never revisited if this changes later.
    assert config.tables[0].s3.order_id_path == "id"
