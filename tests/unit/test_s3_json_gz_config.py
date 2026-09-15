from copy import deepcopy
from pathlib import Path

import pytest
import yaml

from data_ingest.config import parse_config
from data_ingest.exceptions import ConfigurationError


CONFIG = {
    "source": {"name": "par_pos", "type": "s3_json_gz"},
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
    assert config.source_key == "par_pos_s3_json_gz"
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
    ("payload_fields", {"order_id": "order..id"}),
    ("payload_fields", {"order_id": ["order", "id"]}),
    ("payload_fields", {"Order_Id": "id"}),          # uppercase: Athena lowercases
    ("payload_fields", {"event_id": "id"}),          # reserved: always landed
    ("payload_fields", {"payload_json": "id"}),      # reserved
    ("payload_fields", ["order_id", "id"]),          # not a mapping
    ("envelope_fields", {"group_id": ""}),
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
        "folder_timezone": "America/Chicago", "payload_fields": {"order_id": "order.id"},
        "compression": "zlib", "max_object_bytes": 4096,
    })
    settings = parse(data).tables[0].s3
    assert settings.folder_timezone == "America/Chicago"
    assert dict(settings.payload_fields) == {"order_id": "order.id"}
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
    path = Path(__file__).parents[2] / "config" / "s3_json_gz.example.yaml"
    config = parse_config(path.read_text())
    assert config.bronze.database == "bronze_par_pos"
    # One database per source, so the source_key prefix would only be noise.
    # Identity, applied once at CREATE TABLE: pinned so it cannot drift.
    assert config.bronze.table_prefix == "none"
    assert config.tables[0].s3 is not None
    # The producer repeats the envelope's "guid:<order_id>" suffix as the
    # payload's own `id`. Mapping it at ingestion is not cosmetic: Bronze
    # rows already inserted are never revisited if this changes later.
    assert dict(config.tables[0].s3.payload_fields) == {
        "order_id": "id", "order_version": "version",
        "payload_business_date": "businessDate",
    }
    assert dict(config.tables[0].s3.envelope_fields) == {
        "group_id": "groupid", "business_date": "businessdate",
        "historical_data_type": "historicaldatatype",
    }


SOURCE_LOCATION = {
    "source": {"name": "par_pos", "type": "s3_json_gz", "location": "s3://pos-events/orders"},
    "tables": [{"name": "orders", "s3": {"start_at": "2026-09-01T00:00:00Z"}}],
}


def test_location_is_inherited_from_the_source():
    # The common shape: stated once on source:, table block never mentions it.
    table = parse(deepcopy(SOURCE_LOCATION)).tables[0]
    assert table.s3.location == "s3://pos-events/orders"
    assert table.source_object == "s3://pos-events/orders"


def test_a_table_may_override_the_source_location():
    # One source spanning sibling prefixes.
    data = deepcopy(SOURCE_LOCATION)
    data["tables"][0]["s3"]["location"] = "s3://pos-events/payments/"
    assert parse(data).tables[0].s3.location == "s3://pos-events/payments"


def test_a_table_needs_no_s3_block_when_start_at_is_not_required_separately():
    # start_at is still per table, so an absent s3 block is an error that
    # names start_at rather than failing obscurely inside the dataclass.
    data = deepcopy(SOURCE_LOCATION)
    del data["tables"][0]["s3"]
    with pytest.raises(ConfigurationError, match="start_at"):
        parse(data)


def test_location_missing_everywhere_names_the_source_key():
    data = deepcopy(SOURCE_LOCATION)
    del data["source"]["location"]
    with pytest.raises(ConfigurationError, match="source.location is required"):
        parse(data)


def test_source_location_is_rejected_for_a_relational_source():
    data = {"source": {"name": "acme", "type": "snowflake", "location": "s3://nope"},
            "connection": {"secret_id": "x"}}
    with pytest.raises(ConfigurationError, match="location"):
        parse(data)


def test_relational_defaults_are_rejected_on_an_s3_source():
    data = deepcopy(SOURCE_LOCATION)
    data["source"]["database"] = "MY_DB"
    with pytest.raises(ConfigurationError, match="database"):
        parse(data)
