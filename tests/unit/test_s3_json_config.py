from copy import deepcopy
from pathlib import Path

import pytest
import yaml

from data_ingest.config import parse_config
from data_ingest.exceptions import ConfigurationError


CONFIG = {
    "source": {
        "name": "events",
        "type": "s3_json",
        "document": {"preset": "cloudevents"},
    },
    "tables": [{"name": "orders", "location": "s3://pos-events/orders/",
                "start_at": "2026-09-01T00:00:00Z"}],
}


def parse(data):
    # sort_keys=False matters: column order in the YAML fixes column order in
    # the Parquet schema, and the default dump would silently alphabetize it,
    # so an ordering assertion would be testing yaml rather than the parser.
    return parse_config(yaml.safe_dump(data, sort_keys=False))


def table(**overrides):
    data = deepcopy(CONFIG)
    data["tables"][0].update(overrides)
    return parse(data).tables[0]


def source(**overrides):
    data = deepcopy(CONFIG)
    data["source"].update(overrides)
    return parse(data).tables[0]


# --- identity -------------------------------------------------------------

def test_name_identifies_the_producer_and_type_selects_the_adapter():
    config = parse(deepcopy(CONFIG))
    assert config.source_key == "events_s3_json"
    assert config.connection.secret_id is None
    assert config.tables[0].source_object == "s3://pos-events/orders"


def test_replay_identity_is_pinned_and_cannot_be_traded_for_business_identity():
    # Deduplicating on the business key would collapse an order's versions and
    # destroy the history Bronze exists to keep. The error says where that
    # decision actually lives.
    data = deepcopy(CONFIG)
    data["tables"][0]["primary_key"] = ["order_id"]
    with pytest.raises(ConfigurationError, match="Silver"):
        parse(data)


@pytest.mark.parametrize("override", [
    {"checkpoint": {"type": "watermark", "column": "order_version"}},
    {"checkpoint": {"type": "watermark", "column": "_s3_last_modified", "lookback_minutes": 0}},
])
def test_checkpoint_identity_is_pinned(override):
    data = deepcopy(CONFIG)
    data["tables"][0].update(override)
    with pytest.raises(ConfigurationError):
        parse(data)


# --- discovery ------------------------------------------------------------

def test_discovery_defaults_to_hourly_utc_json_gz():
    discovery = table().s3.discovery
    assert (discovery.path_format, discovery.timezone, discovery.suffix) == \
        ("%Y/%m/%d/%H", "UTC", ".json.gz")
    assert discovery.safety_delay_seconds == 120


def test_discovery_layout_is_configurable():
    discovery = source(discovery={
        "path_format": "year=%Y/month=%m/day=%d/hour=%H",
        "timezone": "America/Chicago", "suffix": ".jsonl.gz",
    }).s3.discovery
    assert discovery.path_format == "year=%Y/month=%m/day=%d/hour=%H"
    assert discovery.suffix == ".jsonl.gz"


@pytest.mark.parametrize("path_format", [
    "%Y/%m/%d",          # no hour: one prefix would cover 24 hours
    "static/prefix",     # no directives at all
    "",
])
def test_a_path_format_without_an_hour_is_refused(path_format):
    # The walk steps an hour at a time. A format that does not change hourly
    # would list one prefix repeatedly and never list the hours in between --
    # silent data loss, so it is refused at parse time.
    with pytest.raises(ConfigurationError, match="path_format"):
        source(discovery={"path_format": path_format})


@pytest.mark.parametrize("override", [
    {"timezone": "Invalid/Zone"}, {"suffix": ""}, {"type": "sqs"},
    {"safety_delay_seconds": 0}, {"safety_delay_seconds": 99999},
])
def test_invalid_discovery_rejected(override):
    with pytest.raises(ConfigurationError):
        source(discovery=override)


# --- document / payload ---------------------------------------------------

def test_the_cloudevents_preset_expands_to_the_wire_format():
    document = table().s3.document
    assert document.envelope == "cloudevents"
    assert (document.payload.path, document.payload.encoding,
            document.payload.compression) == ("data_base64", "base64", "auto")
    # The preset is what puts the CloudEvents core in the schema.
    assert [c for c, _ in table().s3.envelope_columns][:2] == ["event_type", "event_specversion"]


def test_an_explicit_key_overrides_the_preset():
    # A feed that is CloudEvents apart from one detail keeps the shorthand.
    document = source(document={"preset": "cloudevents",
                                "payload": {"compression": "zlib"}}).s3.document
    assert document.payload.compression == "zlib"
    assert document.payload.path == "data_base64"      # still from the preset


def test_the_records_preset_has_no_envelope_and_no_payload_path():
    # A plain array of business objects: the outer record IS the payload, and
    # no event_* columns are manufactured for a feed that is not CloudEvents.
    config = source(document={"preset": "records"}).s3
    assert config.document.payload.path is None
    assert config.document.envelope == "none"
    assert config.envelope_columns == ()


def test_a_generic_feed_gets_no_cloudevents_columns():
    config = source(document={"compression": "gzip", "records": "jsonl",
                              "payload": {"path": "payload"}}).s3
    assert [c for c, _ in config.envelope_columns] == []


def test_an_encoded_payload_needs_a_path():
    with pytest.raises(ConfigurationError, match="payload.path is required"):
        source(document={"payload": {"path": None, "encoding": "base64"}})


@pytest.mark.parametrize("override", [
    {"compression": "brotli"}, {"records": "csv"}, {"envelope": "avro"},
    {"preset": "nope"}, {"max_outer_bytes": 0}, {"max_payload_bytes": True},
    {"payload": {"encoding": "hex"}}, {"payload": {"compression": "lz4"}},
    {"payload": {"format": "xml"}}, {"payload": {"path": "a..b"}},
])
def test_invalid_document_rejected(override):
    with pytest.raises(ConfigurationError):
        source(document={"preset": "cloudevents", **override})


# --- projection and record ------------------------------------------------

def test_envelope_fields_become_columns_in_config_order():
    config = table(envelope_fields={"b_col": "groupid", "a_col": "businessdate"}).s3
    assert [c for c, _ in config.envelope_columns][-2:] == ["b_col", "a_col"]


@pytest.mark.parametrize("fields", [
    {"Group_Id": "groupid"},      # uppercase: Athena lowercases identifiers
    {"event_id": "groupid"},      # reserved: the preset already lands it
    {"payload_json": "x"},        # reserved: always landed
    {"group_id": ""},
    {"group_id": "a..b"},
    ["group_id"],
])
def test_invalid_envelope_fields_rejected(fields):
    with pytest.raises(ConfigurationError):
        table(envelope_fields=fields)


def test_business_identity_is_not_ingestion_config():
    # What identifies an order and what versions it is Silver's decision,
    # resolved from payload_json. A key for it here would be a second home for
    # the same fact, and two homes drift.
    with pytest.raises(ConfigurationError, match="record"):
        table(record={"natural_key": ["id"], "version": "version"})


# --- location and wiring --------------------------------------------------

def test_each_table_is_one_path():
    # One S3 path holds one kind of JSON, so a second feed is a second table
    # under the same source, each with its own location and checkpoint.
    data = deepcopy(CONFIG)
    data["tables"].append({"name": "timecards", "location": "s3://pos-events/timecards",
                           "start_at": "2026-09-10T00:00:00Z"})
    config = parse(data)
    assert [t.s3.location for t in config.tables] == [
        "s3://pos-events/orders", "s3://pos-events/timecards"]
    assert config.source_key == "events_s3_json"       # one source, one key


def test_location_is_required_per_table_not_inherited():
    data = deepcopy(CONFIG)
    del data["tables"][0]["location"]
    with pytest.raises(ConfigurationError, match=r"tables\[orders\].location is required"):
        parse(data)
    data["source"]["location"] = "s3://pos-events/orders"   # not a source setting
    with pytest.raises(ConfigurationError, match="location"):
        parse(data)


@pytest.mark.parametrize("location", ["https://bucket/orders", "s3://", 12])
def test_invalid_location_rejected(location):
    with pytest.raises(ConfigurationError, match="location"):
        table(location=location)


def test_start_at_is_required_per_table():
    data = deepcopy(CONFIG)
    del data["tables"][0]["start_at"]
    with pytest.raises(ConfigurationError, match=r"tables\[orders\].start_at is required"):
        parse(data)


@pytest.mark.parametrize("start_at", ["2026-09-01", "2026-09-01T00:00:00", "invalid"])
def test_invalid_start_at_rejected(start_at):
    with pytest.raises(ConfigurationError, match="start_at"):
        table(start_at=start_at)


def test_s3_does_not_accept_a_secret():
    data = {**deepcopy(CONFIG), "connection": {"secret_id": "unneeded"}}
    with pytest.raises(ConfigurationError, match="IAM"):
        parse(data)


def test_snowflake_still_requires_connection():
    data = {**deepcopy(CONFIG), "source": {"name": "x", "type": "snowflake"}}
    with pytest.raises(ConfigurationError, match="connection"):
        parse(data)


def test_relational_defaults_are_rejected_on_an_s3_source():
    with pytest.raises(ConfigurationError, match="database"):
        source(database="MY_DB")


def test_source_level_wiring_is_rejected_for_a_relational_source():
    data = {"source": {"name": "acme", "type": "snowflake", "document": {}},
            "connection": {"secret_id": "x"}}
    with pytest.raises(ConfigurationError, match="document"):
        parse(data)


def test_example_is_a_valid_bronze_enabled_config():
    path = Path(__file__).parents[2] / "config" / "s3_json.example.yaml"
    config = parse_config(path.read_text())
    assert config.source_key == "events_s3_json"
    assert config.bronze.database == "bronze_events"
    # One database per source, so the source_key prefix would only be noise.
    # Identity, applied once at CREATE TABLE: pinned so it cannot drift.
    assert config.bronze.table_prefix == "none"
    settings = config.tables[0].s3
    assert settings.document.payload.path == "data_base64"
    assert dict(settings.envelope_fields) == {
        "group_id": "groupid", "business_date": "businessdate",
        "historical_data_type": "historicaldatatype",
    }


def test_reserved_columns_include_everything_the_landing_writer_stamps():
    """
    The writer refuses a batch carrying one of its lineage columns, so a
    config naming one would pass parsing and then fail on EVERY run. The
    parser mirrors the list rather than importing it (landing.py pulls
    pyarrow at import, and this module is on Bronze's light path); this pins
    the mirror to the real thing.
    """
    from data_ingest.config_s3_json import _LANDING_LINEAGE_COLUMNS, _RESERVED_COLUMNS
    from data_ingest.landing import LINEAGE_COLUMNS

    assert set(_LANDING_LINEAGE_COLUMNS) == set(LINEAGE_COLUMNS)
    assert set(LINEAGE_COLUMNS) <= _RESERVED_COLUMNS
    for column in LINEAGE_COLUMNS:
        with pytest.raises(ConfigurationError, match="reserved"):
            table(envelope_fields={column: "groupid"})


# --- full_prefix discovery --------------------------------------------------

def test_full_prefix_discovery_is_accepted():
    discovery = source(discovery={"type": "full_prefix", "suffix": ".json"}).s3.discovery
    assert discovery.type == "full_prefix"
    assert discovery.suffix == ".json"


@pytest.mark.parametrize("override", [
    {"path_format": "year=%Y/month=%m"},
    {"timezone": "America/Chicago"},
    {"lookahead_hours": 2},
])
def test_full_prefix_rejects_time_partitioned_only_settings(override):
    # These exist to configure a folder walk full_prefix does not have.
    # Leaving them set (a copy-paste from a time_partitioned source) would
    # silently do nothing, so they are refused instead.
    with pytest.raises(ConfigurationError, match="only apply to discovery.type: time_partitioned"):
        source(discovery={"type": "full_prefix", **override})


def test_full_prefix_defaults_for_those_settings_are_fine():
    # The defaults happen to equal what time_partitioned would also use, so
    # they are not "set" in any meaningful sense -- only an explicit,
    # non-default value is refused.
    discovery = source(discovery={"type": "full_prefix"}).s3.discovery
    assert discovery.type == "full_prefix"


def test_full_prefix_does_not_require_positive_lookback():
    # There is no folder to keep in the walk after its hour closes -- the
    # whole location is listed every run regardless of lateness.
    data = deepcopy(CONFIG)
    data["source"]["discovery"] = {"type": "full_prefix"}
    data["tables"][0]["checkpoint"] = {"type": "watermark", "column": "_s3_last_modified",
                                       "lookback_minutes": 0}
    cfg = parse(data)
    assert cfg.tables[0].checkpoint.lookback_minutes == 0


def test_time_partitioned_still_requires_positive_lookback():
    data = deepcopy(CONFIG)
    data["tables"][0]["checkpoint"] = {"type": "watermark", "column": "_s3_last_modified",
                                       "lookback_minutes": 0}
    with pytest.raises(ConfigurationError, match="positive lookback_minutes"):
        parse(data)


def test_a_source_can_have_multiple_full_prefix_tables_with_different_locations():
    # The dynamic-store scenario: rti_shifts and rti_cash_sheet_info are two
    # tables under one source, each its own location, no time partitioning,
    # and no config change needed when a new store-XXX prefix appears under
    # either location.
    data = deepcopy(CONFIG)
    data["source"]["discovery"] = {"type": "full_prefix", "suffix": ".json"}
    data["tables"] = [
        {"name": "rti_shifts", "location": "s3://bucket/rti-data-export/rti_shifts",
         "start_at": "2026-01-01T00:00:00Z"},
        {"name": "rti_cash_sheet_info", "location": "s3://bucket/rti-data-export/rti_cash_sheet_info",
         "start_at": "2026-01-01T00:00:00Z"},
    ]
    cfg = parse(data)
    assert [t.s3.location for t in cfg.tables] == [
        "s3://bucket/rti-data-export/rti_shifts", "s3://bucket/rti-data-export/rti_cash_sheet_info"]
    assert all(t.s3.discovery.type == "full_prefix" for t in cfg.tables)


def test_full_prefix_example_is_a_valid_bronze_enabled_config():
    path = Path(__file__).parents[2] / "config" / "s3_json_full_prefix.example.yaml"
    config = parse_config(path.read_text())
    assert config.source_key == "rti_s3_json"
    assert len(config.tables) == 2
    shifts, cash = config.tables
    assert shifts.name == "rti_shifts" and shifts.s3.location == "s3://my-exports/rti_shifts"
    assert cash.name == "rti_cash_sheet_info" and cash.s3.location == "s3://my-exports/rti_cash_sheet_info"
    assert shifts.s3.discovery.type == "full_prefix"
    assert shifts.checkpoint.lookback_minutes == 0
    # The second table omits checkpoint entirely -- the (unused) default
    # still parses, proving it really is optional for this discovery type.
    assert cash.checkpoint.lookback_minutes == 15
    assert shifts.s3.document.records == "array" and shifts.s3.document.compression == "none"
