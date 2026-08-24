"""
YAML source/table configuration: parsing, validation, and table selection.

This is what makes onboarding a new table a config-only change (README
"Developer Experience") -- IngestionConfig/TableConfig are plain,
source-type-agnostic dataclasses. Nothing here knows Snowflake exists;
`source.type` is just a string looked up later in
data_ingest.sources.registry.
"""

import re
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
class LandingConfig:
    """
    Where extracted data lands, how it is written, and where its extraction
    state lives.

    A cohesive block rather than loose fields on IngestionConfig, matching
    BronzeConfig -- each layer of the pipeline owns its own settings, so
    landing-scoped options (output file sizing, S3 encryption) have an
    obvious home instead of accumulating on the top-level config.

    `checkpoint_table` lives here for the same reason bronze keeps
    `processed_runs_table` in its own block: the checkpoint is the LANDING
    layer's state -- how far extraction has progressed into landing -- not a
    free-floating concern. Each layer owns its resources and its state
    tracking together.

    Named checkpoint_table, not watermark_table: the framework's abstraction
    is a Checkpoint, and a watermark is only one kind. A REST source uses a
    cursor and a full-load source has none, so baking "watermark" into the
    config key would be a lie for every non-timestamp source. See
    data_ingest.checkpoints.
    """

    # A single s3:// URI rather than separate bucket/prefix fields, matching
    # bronze.location. Beyond being one field instead of two that can be
    # half-set, it keeps each layer's location independent -- which matters,
    # because landing and bronze must NOT share a lifecycle policy. Landing
    # ages into Glacier and expires; Iceberg metadata references physical
    # objects, so expiring files under bronze corrupts the table. A shared
    # top-level bucket would structurally encourage one rule spanning both.
    location: Optional[str] = None
    checkpoint_table: Optional[str] = None

    @property
    def bucket(self):
        """Derived from location; boto3 needs bucket and key separately."""
        return split_s3_uri(self.location)[0] if self.location else None

    @property
    def prefix(self):
        return split_s3_uri(self.location)[1] if self.location else "landing"


@dataclass(frozen=True)
class DefaultsConfig:
    """
    Execution policy. Every field here is also overridable per-run via a CLI
    argument, since these are the knobs an operator reaches for during an
    ad-hoc retry.
    """

    fetch_size: Optional[int] = None
    fail_fast: Optional[bool] = None


# Partition Bronze by the month of each table's watermark column, unless the
# config says otherwise. Chosen as the default because it is the one spec
# that both applies to every table (they all have a checkpoint column) and
# actually reduces cost: the merge predicates on the watermark, so Iceberg
# prunes to the months an incoming run touches instead of scanning all of
# Bronze. Coarse enough that partition count stays trivial.
DEFAULT_PARTITION_BY = ("month({checkpoint_column})",)


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

    # Iceberg partition spec applied when a Bronze table is CREATED.
    #
    # Empty (the default) means unpartitioned, which is correct until query
    # patterns are known -- but it has a real cost: MERGE INTO must scan the
    # whole target to evaluate WHEN NOT MATCHED, so merge cost grows with
    # Bronze rather than with the size of the incoming run.
    #
    # `{checkpoint_column}` is substituted with each table's own watermark
    # column, so one setting works across tables that watermark on different
    # column names. Partitioning on a time transform of the watermark is the
    # high-value choice: each run covers a narrow watermark range, so Iceberg
    # prunes to a few partitions instead of scanning everything.
    #
    #     partition_by: ["month({checkpoint_column})"]     # the default
    #     partition_by: ["day({checkpoint_column})"]       # higher volume
    #     partition_by: []                                 # explicitly none
    #
    # Use a TRANSFORM, not a bare timestamp column. month(ts) yields ~12
    # partitions a year; identity partitioning on a second-resolution
    # timestamp would yield one per distinct value -- thousands of tiny
    # partitions, which costs more than it saves.
    #
    # Partitioning on the INGEST date instead would not help: pruning only
    # happens on columns the query predicates on, and the merge's ON clause
    # matches primary_key + watermark. Ingest date cannot join that clause --
    # a row re-landed later must still match its earlier copy or dedup breaks
    # -- so those partitions would never be pruned, making it strictly worse
    # than unpartitioned.
    #
    # IMPORTANT: applies at CREATE TABLE time only. Setting it after a table
    # exists changes nothing, because the loader issues CREATE TABLE IF NOT
    # EXISTS. Iceberg supports partition evolution, but that is a separate
    # ALTER, not a re-CREATE.
    partition_by: tuple = DEFAULT_PARTITION_BY

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

    # Deployment-shape settings, grouped per pipeline layer so each layer
    # owns its own knobs. Every one of these may ALSO be passed as a Glue job
    # CLI arg; run_job() resolves CLI-arg > this config > built-in default, so
    # a CLI arg is only needed for a one-off override (an ad-hoc retry), not
    # as boilerplate on every job definition. Keeping them here is deliberate:
    # they're config for THIS source, and each environment already gets its
    # own --config-uri, so nothing new couples dev and prod.
    landing: LandingConfig = field(default_factory=LandingConfig)
    defaults: DefaultsConfig = field(default_factory=DefaultsConfig)

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


# Glue passes every job argument as a string, so an operator disabling a
# flag will type one of these rather than a Python bool. Matching only
# "false" would silently treat `--fail-fast 0` / `no` / `off` as True --
# the exact opposite of what was asked for, with no error.
_FALSE_STRINGS = {"false", "0", "no", "off", "n", "f", ""}
_TRUE_STRINGS = {"true", "1", "yes", "on", "y", "t"}


def _resolve_bool(cli_value, config_value, default, arg_name="flag"):
    """CLI-arg (a string, since argparse) > config (already a bool/None) > default."""
    if cli_value is not None:
        normalized = str(cli_value).strip().lower()
        if normalized in _FALSE_STRINGS:
            return False
        if normalized in _TRUE_STRINGS:
            return True
        raise ConfigurationError(
            f"{arg_name} expects a boolean-ish value "
            f"(one of {sorted(_TRUE_STRINGS)} / {sorted(_FALSE_STRINGS - {''})}), "
            f"got {cli_value!r}."
        )
    if config_value is not None:
        return bool(config_value)
    return default


def _resolve(cli_value, config_value, default):
    """Generic CLI-arg > config > default resolution for the rest."""
    if cli_value is not None:
        return cli_value
    if config_value is not None:
        return config_value
    return default


def split_s3_uri(uri):
    """
    "s3://bucket/some/prefix" -> ("bucket", "some/prefix").

    Trailing slashes are stripped so callers can join with "/" without
    producing a double slash, which S3 treats as a real (empty) path segment.
    """
    if not str(uri).startswith("s3://"):
        raise ConfigurationError(f"Expected an s3:// URI, got {uri!r}")
    without_scheme = str(uri)[len("s3://"):]
    bucket, _, prefix = without_scheme.partition("/")
    if not bucket:
        raise ConfigurationError(f"S3 URI has no bucket: {uri!r}")
    return bucket, prefix.strip("/")


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


# Iceberg partition transforms Athena accepts. An allowlist rather than a
# passthrough: these land in DDL that has no bind parameters, and a typo
# should fail at config-parse time with a clear message rather than as an
# opaque Athena syntax error on the first Bronze run.
_PARTITION_TRANSFORMS = ("year", "month", "day", "hour", "bucket", "truncate")

_PARTITION_SPEC_PATTERN = re.compile(
    r"^(?:"
    r"[A-Za-z_][A-Za-z0-9_]*"                                  # bare column (identity)
    r"|(?:%s)\(\s*[^()]*\s*\)"                                 # transform(args)
    r")$" % "|".join(_PARTITION_TRANSFORMS)
)


def _validate_partition_spec(spec):
    if not _PARTITION_SPEC_PATTERN.match(str(spec).strip()):
        raise ConfigurationError(
            f"bronze.partition_by entry {spec!r} is not a bare column name or a "
            f"supported Iceberg transform "
            f"({', '.join(t + '(...)' for t in _PARTITION_TRANSFORMS)}). "
            f"Use e.g. \"month({{checkpoint_column}})\" or \"STORE_ID\"."
        )


def _resolve_landing_location(landing_data):
    """Validate landing.location. Required; run_job names the CLI override."""
    location = landing_data.get("location")
    if location is None:
        # Absent is legal here -- run_job reports it as a missing required
        # setting alongside the --s3-bucket override that can supply it.
        return None
    if not str(location).startswith("s3://"):
        raise ConfigurationError(
            f"landing.location must be an s3:// URI, got {location!r}"
        )
    return location


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

    # Absent -> the default. Explicitly empty (`partition_by: []`) -> no
    # partitioning. Those must stay distinguishable, so this checks for the
    # key rather than for a falsy value.
    if "partition_by" in data:
        partition_by = data.get("partition_by") or []
        if isinstance(partition_by, str):
            partition_by = [partition_by]
    else:
        partition_by = list(DEFAULT_PARTITION_BY)

    for spec in partition_by:
        _validate_partition_spec(spec)

    return BronzeConfig(
        partition_by=tuple(partition_by),
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
    landing_data = data.get("landing") or {}
    defaults_data = data.get("defaults") or {}

    landing_location = _resolve_landing_location(landing_data)

    return IngestionConfig(
        source_name=source["name"],
        source_type=source["type"],
        connection=ConnectionConfig(secret_id=connection["secret_id"]),
        tables=tables,
        landing=LandingConfig(
            location=landing_location,
            checkpoint_table=landing_data.get("checkpoint_table"),
        ),
        defaults=DefaultsConfig(
            fetch_size=defaults_data.get("fetch_size"),
            fail_fast=defaults_data.get("fail_fast"),
        ),
        bronze=_parse_bronze(data.get("bronze")),
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
