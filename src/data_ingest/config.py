"""
YAML source/table configuration: parsing, validation, and table selection.

This is what makes onboarding a new table a config-only change (README
"Developer Experience") -- IngestionConfig/TableConfig are plain,
source-type-agnostic dataclasses. Nothing here knows Snowflake exists;
`source.type` is just a string looked up later in
data_ingest.sources.registry.
"""

from dataclasses import dataclass, field
from typing import List, Optional

import yaml

from data_ingest.checkpoints import _TYPE_REGISTRY
from data_ingest.exceptions import ConfigurationError


@dataclass(frozen=True)
class CheckpointConfig:
    """
    A table's checkpoint declaration from YAML, e.g.
    `{type: watermark, column: UPDATED_AT, lookback_minutes: 60}`.
    `column` is only meaningful for watermark-style checkpoints; future
    checkpoint types (cursor, full_load) may leave it unset.
    """

    type: str
    column: Optional[str] = None
    lookback_minutes: int = 0  # widens the effective lower bound on the next incremental extraction; 0 = disabled


@dataclass(frozen=True)
class TableConfig:
    # IDENTITY. `name` is the single identifier this table is known by: it
    # forms the landing path segment AND the DynamoDB source_key, and it's
    # what --tables selects on. Renaming it re-partitions landing and
    # re-keys state together, so treat it as permanent once a table has run.
    name: str

    # WHERE TO READ FROM (not identity). These locate the object in the
    # source system and are recorded as lineage -- in the manifest, and as
    # _source_database/_source_schema/_source_table on every landed row --
    # but nothing keys off them. That means a source-side rename or schema
    # move is a config edit that orphans nothing.
    database: str
    schema: str
    table: str

    primary_key: List[str]
    checkpoint: CheckpointConfig

    @property
    def source_object(self):
        """"DATABASE.SCHEMA.TABLE" -- lineage/logging only; never an identity key."""
        return f"{self.database}.{self.schema}.{self.table}"


@dataclass(frozen=True)
class BronzeConfig:
    """
    Landing -> Iceberg Bronze settings. Optional: a config without a
    `bronze:` section ingests to landing and stops there.

    Note what is NOT here. The merge identity -- primary_key plus the
    checkpoint column -- already exists on each TableConfig because
    ingestion needs it, and it is exactly the dedup key Bronze needs. So
    onboarding a table to Bronze requires no new per-table configuration.

    This section is source-agnostic on purpose: once a run is Parquet plus
    a valid _manifest.json under the landing layout, Bronze does not care
    whether Snowflake, a REST API, or a CSV drop produced it.
    """

    database: str          # Glue Data Catalog database holding the Iceberg tables
    location: str          # s3://<bucket>/bronze -- Iceberg table root
    athena_output: str     # s3://... -- where Athena writes query results
    athena_workgroup: str = "primary"
    processed_runs_table: Optional[str] = None  # DynamoDB; None = re-merge every run

    @property
    def location_root(self):
        return self.location.rstrip("/")


@dataclass(frozen=True)
class ConnectionConfig:
    # Only a Secrets Manager pointer -- actual credentials never live in
    # this config (see "Secrets vs config" in README.md). Database/schema/table mappings are
    # config, not secrets, and live on TableConfig instead.
    secret_id: str


@dataclass(frozen=True)
class IngestionConfig:
    source_name: str  # e.g. "acme"       -- the system being extracted from
    source_type: str  # e.g. "snowflake" -- resolved via data_ingest.sources.registry
    connection: ConnectionConfig
    tables: List[TableConfig] = field(default_factory=list)

    @property
    def source_key(self):
        """
        "<name>_<type>", e.g. "acme_snowflake" -- the source's identity.

        Derived rather than configured so the type is always present: if the
        same system is later also ingested over REST, that config gets
        `acme_rest` automatically and cannot collide with `acme_snowflake`,
        whether or not anyone remembered to disambiguate the name by hand.

        This single value is the source segment of the landing path AND the
        DynamoDB partition key, so identity stays one vocabulary end to end
        (see "Identity" in README.md).
        """
        return f"{self.source_name}_{self.source_type}"

    # Optional deployment-shape settings. These may ALSO be passed as Glue
    # job CLI args (--state-table, --s3-bucket, ...); pipeline.run_job()
    # resolves CLI-arg > this config > built-in default, so a CLI arg is
    # only needed for a one-off override (e.g. an ad-hoc retry), not as
    # boilerplate on every job/environment. Keeping these here (rather than
    # requiring them on every `terraform apply`'s default_arguments) is
    # deliberate: they're config for THIS source, and each environment
    # already gets its own --config-uri (different table sets, lookback,
    # etc), so there's no new dev/prod coupling introduced by having the
    # bucket/state-table live alongside it.
    s3_bucket: Optional[str] = None
    s3_prefix: Optional[str] = None
    state_table: Optional[str] = None
    fetch_size: Optional[int] = None
    fail_fast: Optional[bool] = None

    # None when the config has no `bronze:` section -- ingestion to landing
    # works standalone, and Bronze is opt-in per source.
    bronze: Optional[BronzeConfig] = None

    def get_table(self, name):
        for table in self.tables:
            if table.name == name:
                return table
        raise ConfigurationError(f"Table '{name}' not found in configuration")

    def resolve_tables(self, selector):
        """
        selector: 'all' | comma-separated table names (the Glue job's
        --tables argument). Used for targeted retries/debugging without
        re-running every table -- see "Glue job arguments" in README.md.
        """
        if selector == "all":
            return list(self.tables)

        requested = [t.strip() for t in selector.split(",") if t.strip()]
        known = {t.name for t in self.tables}
        unknown = [t for t in requested if t not in known]
        if unknown:
            raise ConfigurationError(f"Unknown tables requested: {', '.join(unknown)}")
        return [self.get_table(name) for name in requested]


def _duplicates(values):
    """Return the sorted set of values appearing more than once."""
    seen = set()
    dupes = set()
    for value in values:
        if value in seen:
            dupes.add(value)
        seen.add(value)
    return sorted(dupes)


def _parse_checkpoint(data):
    table_name = data.get("name")
    if "checkpoint" not in data:
        raise ConfigurationError(f"Table '{table_name}' is missing 'checkpoint'")
    checkpoint = data["checkpoint"]

    if "type" not in checkpoint:
        raise ConfigurationError(f"Table '{table_name}' checkpoint is missing 'type'")

    checkpoint_type = checkpoint["type"]
    # Validate against the registry rather than accepting any string. Without
    # this, `type: cursor` parses fine and is then treated as a watermark
    # anyway, failing much later with an opaque AttributeError from
    # quote_identifier(None) deep inside the first query build.
    known_types = sorted(_TYPE_REGISTRY)
    if checkpoint_type not in known_types:
        raise ConfigurationError(
            f"Table '{table_name}' has unknown checkpoint type "
            f"'{checkpoint_type}'. Known types: {', '.join(known_types)}."
        )

    column = checkpoint.get("column")
    if checkpoint_type == "watermark" and not column:
        raise ConfigurationError(
            f"Table '{table_name}' uses checkpoint type 'watermark' but does not "
            f"specify 'column'."
        )

    lookback_minutes = checkpoint.get("lookback_minutes", 0)
    try:
        lookback_minutes = int(lookback_minutes)
    except (TypeError, ValueError):
        raise ConfigurationError(
            f"Table '{table_name}' has non-integer lookback_minutes: {lookback_minutes!r}"
        )
    if lookback_minutes < 0:
        raise ConfigurationError(
            f"Table '{table_name}' has negative lookback_minutes: {lookback_minutes}"
        )

    return CheckpointConfig(
        type=checkpoint_type,
        column=column,
        lookback_minutes=lookback_minutes,
    )


def _parse_bronze(data):
    """Parse the optional `bronze:` section. None when absent."""
    if not data:
        return None

    required = ["database", "location", "athena_output"]
    missing = [field_name for field_name in required if not data.get(field_name)]
    if missing:
        raise ConfigurationError(
            f"bronze section is missing required field(s): {', '.join(missing)}"
        )

    for uri_field in ("location", "athena_output"):
        if not str(data[uri_field]).startswith("s3://"):
            raise ConfigurationError(
                f"bronze.{uri_field} must be an s3:// URI, got {data[uri_field]!r}"
            )

    return BronzeConfig(
        database=data["database"],
        location=data["location"],
        athena_output=data["athena_output"],
        athena_workgroup=data.get("athena_workgroup", "primary"),
        processed_runs_table=data.get("processed_runs_table"),
    )


def _parse_table(data):
    required = ["name", "database", "schema", "table", "primary_key"]
    missing = [field_name for field_name in required if field_name not in data]
    if missing:
        raise ConfigurationError(f"Table config missing required fields: {missing}")

    return TableConfig(
        name=data["name"],
        database=data["database"],
        schema=data["schema"],
        table=data["table"],
        primary_key=list(data["primary_key"]),
        checkpoint=_parse_checkpoint(data),
    )


def parse_config(raw_text):
    """Parse and validate a config YAML document's text into an IngestionConfig."""
    try:
        data = yaml.safe_load(raw_text)
    except yaml.YAMLError as exc:
        raise ConfigurationError(f"Invalid YAML configuration: {exc}") from exc

    if not data or "source" not in data or "connection" not in data:
        raise ConfigurationError("Configuration must define 'source' and 'connection'")

    source = data["source"]
    connection = data["connection"]

    tables = [_parse_table(t) for t in data.get("tables", [])]
    if not tables:
        raise ConfigurationError("Configuration must define at least one table")

    # `name` is the identity key: it forms both the landing path and the
    # DynamoDB source_key. Two entries sharing a name would write to the same
    # landing prefix and fight over one checkpoint -- each run advancing it
    # past the other's window, permanently starving both.
    duplicate_names = _duplicates(t.name for t in tables)
    if duplicate_names:
        raise ConfigurationError(
            f"Duplicate table name(s) in configuration: {', '.join(duplicate_names)}. "
            f"`name` is the identity key -- it selects the table via --tables, names its "
            f"landing path, and forms its DynamoDB source_key -- so it must be unique."
        )

    # Two entries reading the same source object is no longer a correctness
    # problem (they'd have distinct names, so distinct checkpoints and
    # distinct landing paths), but it does mean extracting the same data
    # twice and paying for it twice -- almost always a copy/paste mistake.
    duplicate_objects = _duplicates(t.source_object for t in tables)
    if duplicate_objects:
        raise ConfigurationError(
            f"Multiple table entries read the same source object(s): "
            f"{', '.join(duplicate_objects)}. Each would extract the same rows into a "
            f"separate landing path. If that is genuinely intended, say so explicitly by "
            f"giving them different source objects; otherwise define each object once."
        )

    # All optional -- see IngestionConfig for why these live here instead
    # of being required Glue job arguments.
    landing = data.get("landing") or {}
    state = data.get("state") or {}
    defaults = data.get("defaults") or {}
    bronze = _parse_bronze(data.get("bronze"))

    return IngestionConfig(
        source_name=source["name"],
        source_type=source["type"],
        connection=ConnectionConfig(secret_id=connection["secret_id"]),
        tables=tables,
        s3_bucket=landing.get("bucket"),
        s3_prefix=landing.get("prefix"),
        state_table=state.get("table"),
        fetch_size=defaults.get("fetch_size"),
        fail_fast=defaults.get("fail_fast"),
        bronze=bronze,
    )


def load_config(config_uri, s3_client=None):
    """
    Load configuration from a local path or an s3:// URI. The Glue job
    passes --config-uri as s3://...; local paths exist mainly for tests
    and local development (see "Local development" in README.md).
    """
    if config_uri.startswith("s3://"):
        if s3_client is None:
            # Lazily constructed rather than a required parameter, so
            # callers that already have a client (run_job) can pass it
            # through and avoid creating a second one.
            import boto3

            s3_client = boto3.client("s3")

        without_scheme = config_uri[len("s3://"):]
        bucket, _, key = without_scheme.partition("/")
        if not bucket or not key:
            raise ConfigurationError(f"Invalid S3 config URI: {config_uri}")

        response = s3_client.get_object(Bucket=bucket, Key=key)
        raw_text = response["Body"].read().decode("utf-8")
    else:
        with open(config_uri, "r") as f:
            raw_text = f.read()

    return parse_config(raw_text)
