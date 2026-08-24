import pytest

from data_ingest.config import parse_config
from data_ingest.exceptions import ConfigurationError

VALID_YAML = """
source:
  name: acme
  type: snowflake

connection:
  secret_id: /data-platform/acme/snowflake

tables:
  - name: orders
    database: ACME
    schema: PUBLIC
    table: ORDERS
    primary_key:
      - ORDER_ID
    checkpoint:
      type: watermark
      column: UPDATED_AT

  - name: customers
    database: ACME
    schema: PUBLIC
    table: CUSTOMERS
    primary_key:
      - CUSTOMER_ID
    checkpoint:
      type: watermark
      column: UPDATED_AT
      lookback_minutes: 60
"""


def test_parse_config_returns_all_tables():
    config = parse_config(VALID_YAML)
    assert config.source_name == "acme"
    assert len(config.tables) == 2
    assert config.tables[1].checkpoint.lookback_minutes == 60


def test_default_lookback_is_zero():
    config = parse_config(VALID_YAML)
    assert config.tables[0].checkpoint.lookback_minutes == 0


def test_resolve_tables_all():
    config = parse_config(VALID_YAML)
    assert {t.name for t in config.resolve_tables("all")} == {"orders", "customers"}


def test_resolve_tables_subset():
    config = parse_config(VALID_YAML)
    assert [t.name for t in config.resolve_tables("customers")] == ["customers"]


def test_resolve_tables_unknown_raises():
    config = parse_config(VALID_YAML)
    with pytest.raises(ConfigurationError):
        config.resolve_tables("nope")


def test_missing_source_raises():
    with pytest.raises(ConfigurationError):
        parse_config("connection:\n  secret_id: x\ntables: []")


def test_empty_tables_raises():
    with pytest.raises(ConfigurationError):
        parse_config("source:\n  name: x\n  type: snowflake\nconnection:\n  secret_id: x\ntables: []")


def test_deployment_settings_default_to_none_when_absent():
    config = parse_config(VALID_YAML)
    assert config.landing.bucket is None
    assert config.landing.prefix == "landing"  # sensible default, not None
    assert config.state.table is None
    assert config.defaults.fetch_size is None
    assert config.defaults.fail_fast is None


def test_unknown_checkpoint_type_is_rejected():
    # Without validation, `type: cursor` parses fine and is then treated as a
    # watermark anyway, failing much later with an opaque AttributeError.
    bad = VALID_YAML.replace("      type: watermark\n      column: UPDATED_AT", "      type: cursor", 1)
    with pytest.raises(ConfigurationError, match="unknown checkpoint type"):
        parse_config(bad)


def test_watermark_checkpoint_without_column_is_rejected():
    bad = VALID_YAML.replace("      type: watermark\n      column: UPDATED_AT", "      type: watermark", 1)
    with pytest.raises(ConfigurationError, match="does not specify 'column'"):
        parse_config(bad)


def test_negative_lookback_is_rejected():
    bad = VALID_YAML.replace("lookback_minutes: 60", "lookback_minutes: -5")
    with pytest.raises(ConfigurationError, match="negative lookback_minutes"):
        parse_config(bad)


def test_duplicate_table_names_are_rejected():
    with pytest.raises(ConfigurationError, match="Duplicate table name"):
        parse_config(VALID_YAML + """
  - name: orders
    database: ACME
    schema: PUBLIC
    table: SOMETHING_ELSE
    primary_key: [ID]
    checkpoint:
      type: watermark
      column: UPDATED_AT
""")


def test_two_entries_targeting_the_same_object_are_rejected():
    # They would share one DynamoDB checkpoint (source_key derives from the
    # object name) while landing to different paths -- each run would advance
    # the checkpoint past the other's window, starving both.
    with pytest.raises(ConfigurationError, match="same source object"):
        parse_config(VALID_YAML + """
  - name: orders_copy
    database: ACME
    schema: PUBLIC
    table: ORDERS
    primary_key: [ORDER_ID]
    checkpoint:
      type: watermark
      column: UPDATED_AT
""")


def test_deployment_settings_parsed_when_present():
    config = parse_config(
        VALID_YAML
        + """
landing:
  bucket: my-landing-bucket
  prefix: custom-prefix

state:
  table: my-state-table

defaults:
  fetch_size: 12345
  fail_fast: false
"""
    )
    assert config.landing.bucket == "my-landing-bucket"
    assert config.landing.prefix == "custom-prefix"
    assert config.state.table == "my-state-table"
    assert config.defaults.fetch_size == 12345
    assert config.defaults.fail_fast is False


def test_yaml_section_names_are_the_public_contract():
    """
    The YAML shape is what lives in S3 and is edited by hand -- renaming a
    section silently breaks every deployed config, and the failure mode is a
    setting reverting to its default rather than an error. The internal
    dataclass layout may change freely; these key names may not.
    """
    config = parse_config(VALID_YAML + """
landing:
  bucket: b
  prefix: p

state:
  table: t

defaults:
  fetch_size: 123
  fail_fast: false

bronze:
  database: d
  location: s3://x/bronze
  athena_output: s3://x/results/
""")
    assert config.landing.bucket == "b"
    assert config.landing.prefix == "p"
    assert config.state.table == "t"
    assert config.defaults.fetch_size == 123
    assert config.defaults.fail_fast is False
    assert config.bronze.database == "d"


def test_landing_prefix_defaults_without_a_landing_section():
    # An absent section must not mean an absent default -- prefix has a
    # sensible one, and a None here would build "None/<source>/..." paths.
    config = parse_config(VALID_YAML)
    assert config.landing.prefix == "landing"
    assert config.landing.bucket is None
    assert config.state.table is None
