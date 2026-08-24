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
    assert config.landing.checkpoint_table is None
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
  location: s3://my-landing-bucket/custom-prefix
  checkpoint_table: my-state-table

defaults:
  fetch_size: 12345
  fail_fast: false
"""
    )
    assert config.landing.bucket == "my-landing-bucket"
    assert config.landing.prefix == "custom-prefix"
    assert config.landing.checkpoint_table == "my-state-table"
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
  location: s3://b/p
  checkpoint_table: t

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
    assert config.landing.checkpoint_table == "t"
    assert config.defaults.fetch_size == 123
    assert config.defaults.fail_fast is False
    assert config.bronze.database == "d"


def test_landing_prefix_defaults_without_a_landing_section():
    # An absent section must not mean an absent default -- prefix has a
    # sensible one, and a None here would build "None/<source>/..." paths.
    config = parse_config(VALID_YAML)
    assert config.landing.prefix == "landing"
    assert config.landing.bucket is None
    assert config.landing.checkpoint_table is None


def test_checkpoint_table_lives_under_landing():
    """
    Structural symmetry: each pipeline layer owns its destination AND its
    state table. landing.checkpoint_table mirrors bronze.processed_runs_table.
    """
    config = parse_config(VALID_YAML + """
landing:
  location: s3://b/landing
  checkpoint_table: my-checkpoints

bronze:
  database: d
  location: s3://x/bronze
  athena_output: s3://x/results/
  processed_runs_table: my-processed-runs
""")
    assert config.landing.checkpoint_table == "my-checkpoints"
    assert config.bronze.processed_runs_table == "my-processed-runs"


def test_shipped_example_config_parses_and_exercises_every_section():
    """
    The example is what people copy, so a mistake in it propagates into every
    new source. Parsing it here means a typo fails the build instead of
    silently shipping -- which is exactly the gap that existed while the
    bronze block was commented out and therefore never validated by anything.
    """
    import pathlib

    from data_ingest.config import load_config

    example = pathlib.Path(__file__).parents[2] / "config" / "snowflake.example.yaml"
    assert example.exists(), f"example config missing at {example}"

    config = load_config(str(example))

    # Both layers present, so the symmetry the file demonstrates is real
    # rather than aspirational.
    assert config.landing.bucket and config.landing.checkpoint_table
    assert config.bronze is not None
    assert config.bronze.database and config.bronze.processed_runs_table

    assert config.defaults.fetch_size == 10000, (
        "the example must not advertise a fetch_size that has been observed to OOM"
    )
    assert config.source_key == "acme_snowflake"
    assert [t.name for t in config.tables] == ["fact_order"]


def test_landing_location_is_split_into_bucket_and_prefix():
    config = parse_config(VALID_YAML + """
landing:
  location: s3://my-bucket/some/nested/landing
""")
    assert config.landing.bucket == "my-bucket"
    assert config.landing.prefix == "some/nested/landing"


def test_landing_and_bronze_can_live_in_different_buckets():
    """
    The reason each layer carries its own location rather than sharing a
    top-level bucket: landing gets an S3 Lifecycle policy and bronze must
    never get one, because expiring files under Iceberg metadata corrupts
    the table. Separate buckets must stay expressible.
    """
    config = parse_config(VALID_YAML + """
landing:
  location: s3://raw-zone/landing

bronze:
  database: d
  location: s3://curated-zone/bronze
  athena_output: s3://scratch/results/
""")
    assert config.landing.bucket == "raw-zone"
    assert config.bronze.location_root == "s3://curated-zone/bronze"


def test_landing_location_must_be_an_s3_uri():
    with pytest.raises(ConfigurationError, match="must be an s3:// URI"):
        parse_config(VALID_YAML + """
landing:
  location: /local/path
""")


def test_trailing_slashes_do_not_produce_double_slashes():
    # "s3://b/landing/" + "/" + "source" would yield "landing//source",
    # which S3 treats as a real empty path segment.
    config = parse_config(VALID_YAML + """
landing:
  location: s3://my-bucket/landing/
""")
    assert config.landing.prefix == "landing"


def test_bronze_partitions_by_watermark_month_by_default():
    """
    The default has to both apply to every table and actually reduce cost.
    Partitioning on the watermark does: the merge predicates on it, so Iceberg
    prunes to the months a run touches. Ingest date would not -- it cannot
    appear in the merge's ON clause without breaking dedup, so its partitions
    would never be pruned.
    """
    config = parse_config(VALID_YAML + """
bronze:
  database: d
  location: s3://x/bronze
  athena_output: s3://x/results/
""")
    assert config.bronze.partition_by == ("month({checkpoint_column})",)


def test_partitioning_can_be_explicitly_disabled():
    # Absent means "use the default"; an explicit empty list means "none".
    # Those must stay distinguishable.
    config = parse_config(VALID_YAML + """
bronze:
  database: d
  location: s3://x/bronze
  athena_output: s3://x/results/
  partition_by: []
""")
    assert config.bronze.partition_by == ()


def test_partition_spec_is_validated_against_known_transforms():
    with pytest.raises(ConfigurationError, match="not a bare column name"):
        parse_config(VALID_YAML + """
bronze:
  database: d
  location: s3://x/bronze
  athena_output: s3://x/results/
  partition_by: ["month(x); DROP TABLE y"]
""")


# --------------------------------------------------------------------------
# Unknown keys
#
# Silently ignoring an unrecognized key is a bad trade for a config that is
# hand-edited and deployed to S3: a typo, or a key left behind after a
# rename, reverts that setting to its default. The job then behaves subtly
# differently, or -- as happened -- fails much later complaining about a
# DIFFERENT setting being missing, sending you looking in the wrong place.
# --------------------------------------------------------------------------


def test_renamed_landing_keys_name_their_replacement():
    with pytest.raises(ConfigurationError) as exc_info:
        parse_config(VALID_YAML + """
landing:
  bucket: b
  prefix: landing
""")
    message = str(exc_info.value)
    assert "landing.bucket" in message
    assert "landing.location" in message, "must say what to write instead"


def test_removed_top_level_state_section_names_its_replacement():
    with pytest.raises(ConfigurationError, match="landing.checkpoint_table"):
        parse_config(VALID_YAML + """
state:
  table: my-watermarks
""")


@pytest.mark.parametrize(
    "extra, expected",
    [
        ("defaults:\n  fetch_sizee: 10\n", "defaults.fetch_sizee"),
        ("landing:\n  locaiton: s3://b/p\n", "landing.locaiton"),
        ("bronze:\n  database: d\n  location: s3://x/b\n"
         "  athena_output: s3://x/r/\n  partitoin_by: []\n", "bronze.partitoin_by"),
    ],
)
def test_typos_are_rejected_rather_than_ignored(extra, expected):
    with pytest.raises(ConfigurationError, match=r"not a recognized setting"):
        parse_config(VALID_YAML + extra)


def test_typo_inside_a_table_entry_is_rejected():
    bad = VALID_YAML.replace("      - RESERVATION_ID", "      - ORDER_ID").replace(
        "    primary_key:", "    primary_keys:", 1
    )
    with pytest.raises(ConfigurationError):
        parse_config(bad)


def test_typo_inside_a_checkpoint_block_is_rejected():
    bad = VALID_YAML.replace("      column: UPDATED_AT", "      colunm: UPDATED_AT", 1)
    with pytest.raises(ConfigurationError, match="colunm"):
        parse_config(bad)


def test_the_error_lists_the_keys_that_are_valid():
    # Naming the alternatives is what turns the error into a fix.
    with pytest.raises(ConfigurationError, match="fetch_size"):
        parse_config(VALID_YAML + "defaults:\n  fetch_sizee: 10\n")
