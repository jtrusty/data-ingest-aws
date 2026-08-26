# data_ingest

Reusable AWS Glue ingestion framework: source -> immutable S3 Landing, with
a generic checkpoint/state model and transactional commit semantics.

First adapter: **Snowflake -> S3 Landing**. Built to be extended
with REST/JDBC/S3/cursor-based sources later without touching the core
pipeline.

## Architecture

```
Source (Snowflake, ...)
  |
  |   Glue Python Shell: jobs/landing_load_snowflake.py   (one per source type)
  v
S3 Landing        immutable Parquet, one run_id prefix per run,
  |               committed by _manifest.json
  |               checkpoint -> DynamoDB
  |
  |   Glue Python Shell: jobs/bronze_load.py              (one, for every source)
  v
Bronze            Apache Iceberg via Athena MERGE INTO,
  |               deduplicated on primary_key + watermark
  v
Redshift Spectrum / Athena
```

Both jobs ship in the same wheel and read the same config file. Landing is
the normalization boundary: once a run is Parquet plus a valid manifest in
the standard layout, Bronze does not care which source produced it, so one
Bronze job serves Snowflake, REST, and CSV sources alike.

Infrastructure (S3, Glue jobs, IAM, DynamoDB, Secrets Manager, the Glue
catalog database) is owned elsewhere. This package owns extraction and
transaction logic. Neither should know the other's internals.

## Package layout

```
src/data_ingest/
  pipeline.py       generic orchestration: checkpoint -> extract -> land -> manifest -> commit
  config.py         YAML config loading/parsing (local path or s3://)
  state.py          StateStore abstraction + DynamoDB implementation (optimistic concurrency)
  landing.py        S3 landing writer: prefix layout, Parquet writes, manifest commit
  manifest.py       _manifest.json schema
  exceptions.py     framework-specific exceptions
  logging.py        stdlib logging setup (does NOT configure on import)
  sources/
    base.py         Source interface (get_current_checkpoint / extract / metadata)
    registry.py     source.type -> adapter module; add a source with one line here
    snowflake.py    Snowflake adapter (fetchmany batching; lossless watermark codecs)
  checkpoints/
    base.py         Checkpoint interface
    watermark.py    single-column watermark checkpoint (+ optional lookback_minutes)
  bronze/
    loader.py       orchestration: discover runs -> merge -> record processed
    discovery.py    committed landing runs (skips anything without a manifest)
    ddl.py          MERGE / ADD PARTITION / CREATE TABLE generation
    schema.py       catalog-vs-run schema diff, additive evolution, and the
                    checks that refuse an unmergeable state
    athena.py       submit a statement, poll to a terminal state, fail loudly
    state.py        which runs have been merged (a cost optimization, not
                    correctness -- the merge is idempotent)
    job.py          Glue entry point

jobs/                      thin Glue entry points, named <layer>_load[_<source>]:
  landing_load_snowflake.py  source -> landing. One per source type, because
                             each asserts its own source.type and imports its
                             adapter eagerly to fail fast on a bad runtime.
  bronze_load.py             landing -> bronze. No source suffix: landing is
                             the normalization boundary, so one script serves
                             every source.
config/snowflake.example.yaml  example config; real ones are gitignored
constraints-glue.txt       frozen dependency set matching the Glue runtime
tests/unit/                pytest suite (moto-mocked AWS)
tests/conftest.py          pins AWS region/credentials so tests don't inherit
                           ambient AWS config
```

## Core semantics (do not break these)

- **Landing is immutable.** Every run writes to a brand-new
  `landing/<source_key>/<table>/ingest_date=YYYY-MM-DD/run_id=<uuid>/` prefix.
  Nothing is ever overwritten.
- **The manifest is the commit marker.** A run directory without a
  successful `_manifest.json` is incomplete and must be ignored downstream.
- **DynamoDB only advances after the manifest is written.** If extraction,
  the Parquet writes, or the manifest write fail, the checkpoint is left
  untouched and the next execution retries the same window.
- **Optimistic concurrency.** Checkpoint commits are conditioned on the
  version read at the start of the run; a concurrent commit from another
  execution raises `CheckpointConflictError` instead of silently
  overwriting.
- **A table with no checkpoint does a full load.** A table with no prior
  DynamoDB state automatically performs `WHERE watermark <= high` as its
  first run; every run after that is incremental. No separate backfill job.
- **The checkpoint never moves backwards.** If the source's max watermark
  regresses (max row hard-deleted, table restored or cloned), the run is
  skipped rather than committing the lower value — which would otherwise
  make the next run re-extract everything in between as duplicates.
- **Watermarks round-trip losslessly.** The high watermark is read as text
  at full source precision (`TO_VARCHAR(..., 'FF9')`) and bound back with an
  explicit cast, never via `str()` of a Python object. The connector
  materializes `TIMESTAMP(9)` as `datetime`, which holds only microseconds —
  a truncated ceiling would exclude the very row it came from, and the next
  run would truncate identically, silently losing that row forever.

## Identity

Three config values identify a table — `source.name`, `source.type`, and the
table's `name` — and they're used consistently everywhere:

```
source_key = "<source.name>_<source.type>"        e.g. "acme_snowflake"

landing/<source_key>/<table.name>/ingest_date=YYYY-MM-DD/run_id=<uuid>/

DynamoDB:  source_key  (partition key)   "acme_snowflake"
           table_name  (sort key)        "order_fct"
           source_name, source_type      also stored, for readability
```

`source_key` is **derived**, not configured. Writing it by hand would let two
ingestions of the same system collide; deriving it from name AND type means
ingesting the same system over REST later is automatically `acme_rest` and
distinct from `acme_snowflake`, whether or not anyone remembered to disambiguate.

`database` / `schema` / `table` say **where to read from**. They are
recorded as lineage — in `_manifest.json`, and on every landed row as
`_source_database` / `_source_schema` / `_source_table` — but nothing keys
off them. Two consequences:

- **A source-side rename is free.** If Snowflake renames a view or moves it
  between schemas, edit the YAML and nothing orphans; landing path and
  checkpoint both key off `name`, which didn't change. Keying on
  `DATABASE.SCHEMA.TABLE` would instead silently orphan the checkpoint and
  trigger a full reload of the whole table.
- **Renaming `name` is a deliberate re-partition.** Landing path and
  checkpoint move together rather than drifting apart silently. Treat
  `name` as permanent once a table has run.

This also generalizes past relational sources: `name` exists for every
source type, while `database`/`schema`/`table` is a relational concept a
REST or CSV adapter would have to fabricate. Path depth stays constant at
`landing/<source>/<table>/…` no matter what the source is.

## Bronze

Optional, and opt-in per source: a config with no `bronze:` section ingests
to landing and stops there.

Per table, per committed landing run:

```
ALTER TABLE ... ADD PARTITION   register the run's prefix
MERGE INTO bronze ...           insert rows not already present
record run_id processed         DynamoDB
```

Athena does the compute, so rows never pass through the job process. The
1 DPU / 16 GB ceiling that forces batching on the ingestion side does not
apply here at any table size — which is also why the Bronze job can run on
Glue's **smallest** Python Shell size (0.0625 DPU) rather than 1.

The merge:

```sql
MERGE INTO "acme_snowflake_order_fact" AS target
USING (
  SELECT * FROM "landing_acme_snowflake_order_fact"
  WHERE ingest_date = '2026-08-25' AND run_id = '3f2a9c1e'
) AS source
   ON target."order_key" = source."order_key"
   AND target."last_update_dttm" = source."last_update_dttm"
WHEN NOT MATCHED THEN INSERT ("order_key", "amount", "last_update_dttm")
  VALUES (source."order_key", source."amount", source."last_update_dttm")
```

**The dedup keys are already in your config.** `primary_key` and
`checkpoint.column` exist because ingestion needs them, and they are exactly
the identity Bronze deduplicates on. Onboarding a table to Bronze requires no
new per-table configuration.

Two details of that statement are not stylistic. Identifiers are lowercased
because Athena stores them that way and Iceberg then matches
case-sensitively; and the `INSERT` column list is explicit because `INSERT *`
is Spark syntax that Trino rejects. The `INSERT` targets are unprefixed while
the `VALUES` expressions are `source.`-prefixed — the two lists look
symmetric but are not.

Three properties follow from `WHEN NOT MATCHED THEN INSERT`:

- **Lookback duplicates collapse.** Re-extracting a trailing window
  deliberately re-lands rows already in Bronze; they match and are not
  inserted again.
- **Re-merging a run is a no-op**, so idempotency is a property of the SQL
  rather than the bookkeeping. A crash between merging and recording a run is
  therefore harmless — the same fail-safe shape as a crash between manifest
  and checkpoint on the ingestion side.
- **History is retained.** The watermark is part of the match key, so three
  versions of one primary key at three watermark values stay three rows.
  There is deliberately no `WHEN MATCHED` clause; collapsing to current state
  belongs downstream in Redshift.

A landing run without a `_manifest.json` is ignored — a crashed or
OOM-killed extraction leaves Parquet behind, and the ingestion job
deliberately does not clean up after itself. A run with an *unreadable*
manifest raises instead: it claimed to commit, so skipping it would silently
drop data.

### Partitioning

`bronze.partition_by` defaults to `["month({checkpoint_column})"]`, with
`{checkpoint_column}` substituted per table so one entry covers tables that
watermark on different column names.

Unpartitioned Bronze is correct but not free: `MERGE INTO` must scan the
whole target to evaluate `WHEN NOT MATCHED`, so merge cost grows with Bronze
rather than with the incoming run. Partitioning on the watermark fixes that,
because the merge predicates on it and Iceberg can prune to the months a run
touches.

Two things that look reasonable and are not:

- **Partitioning by ingest date does not help.** Pruning only happens on
  columns a query predicates on, and ingest date cannot join the merge's `ON`
  clause without breaking dedup — a row re-landed later must still match its
  earlier copy. Those partitions would never be pruned, making it strictly
  worse than none.
- **Never partition on a bare timestamp.** `month(ts)` yields ~12 partitions
  a year; identity partitioning a second-resolution column yields one per
  distinct value. Specs are validated against the transforms Athena supports
  (`year`, `month`, `day`, `hour`, `bucket`, `truncate`).

**Set this before the first Bronze run.** The loader issues `CREATE TABLE IF
NOT EXISTS`, so the partition spec is applied exactly once, when the table is
first created. Changing it afterwards does nothing — the setting is read and
then ignored, *silently*, because the table already exists. Repartitioning
after the fact means an explicit Iceberg partition-spec change in Athena,
outside this config.

`partition_by` is override-only; omitting it uses the default above. The
example config leaves it commented out for that reason.

### Schema evolution

Source schemas change, and Athena tables created with `CREATE TABLE IF NOT
EXISTS` do not follow. Before each merge, the loader compares the run's
manifest schema against the live catalog schema and applies the difference,
using Iceberg's own policy:

| source change | Bronze |
|---|---|
| column **added** | `ALTER TABLE ADD COLUMNS` on both tables, automatically. Existing rows read NULL for it. |
| column **removed** | left alone. Older Parquet still has the values; newer files read NULL. Dropping it would discard history Bronze exists to retain. |
| type **changed** | **fails before merging**, naming the column and both types. |

Type changes stop the load rather than resolving themselves. Widening a
decimal or turning an int into a string is ambiguous and can lose precision,
and Bronze is append-only — a silently truncated column cannot be corrected
afterwards. Resolve it deliberately (widen the column in Athena, or land the
source column under a new name) and re-run.

Renames are deliberately not special-cased: at the schema level a rename is
indistinguishable from "drop one column, add another", and guessing wrong
would rewrite history in a way nothing downstream would flag.

This matters because the failure it prevents is silent. Athena returns only
the columns a table declares, so without evolution a column added upstream
lands in Parquet and is then invisible — data in S3, absent from Bronze, no
error anywhere.

The comparison uses the **union** of every pending run's manifest schema,
not one run's. Runs merged in a single pass legitimately disagree — a newer
one has a column the source just gained, an older one predates it — and
defining the tables from any single run breaks the rest: too narrow, and the
merge's `SELECT *` cannot resolve `source.<col>` for a run that has it. A run
lacking a column simply omits it from its `INSERT` list, and Iceberg writes
NULL, which is the honest value.

A rename therefore yields **both** columns, each NULL outside its own era.

The Bronze role needs `glue:GetTable` for this.

### What Bronze refuses

Each of these stops the load with a message naming the cause and the fix,
rather than letting Athena fail later with an error that points at the symptom.

| Condition | Why it cannot proceed |
|---|---|
| Landing run marked `schema_drift` | Its Parquet files disagree with each other, so no single table schema describes them. Athena would accept the `CREATE` and fail at read time with `HIVE_BAD_DATA`, blaming the file rather than the extraction. |
| Runs disagreeing on a column's **type** | One table cannot describe both, and picking one silently loses precision on the other. |
| A run missing its own primary key or watermark | The `ON` clause would parse — the landing table declares the union — but compare against NULL, which never matches. Every row would be re-inserted on every pass. |
| Iceberg metadata file missing | A catalog entry whose S3 metadata was deleted looks healthy to `GetTable`, so `CREATE TABLE IF NOT EXISTS` is skipped and the merge fails with `ICEBERG_MISSING_METADATA`. |
| `table_prefix` changed after tables exist | See above — it would create a second set of tables and strand the first. |

None of these self-heal. Dropping a catalog entry or deleting a prefix can
destroy hours of landed data, so the job prints the `aws glue delete-table`
command and stops. That is also why neither Glue role holds
`glue:DeleteTable` or `s3:DeleteObject`.

### Table naming  (`bronze.table_prefix`)

| Setting | Table name | Use when |
|---|---|---|
| `source_key` (default) | `acme_snowflake_order_fact` | Several sources share one Bronze database |
| `none` | `order_fact` | One Glue database per source |

The prefix exists to stop two sources colliding in one Glue database. With a
database per source that can't happen, and the prefix is noise an analyst
types in every query — the schema already says which source it is.

**This is identity.** It decides what the tables are *called*, and the loader
cannot rename. Changing it after tables exist would create a second set
beside the first and strand everything already merged under a name nothing
queries — present in S3, absent from every query, nothing failing. The loader
detects the mismatch and refuses, but the only real fix is to drop the old
tables, clear their rows from the processed-runs table, and re-merge.

Set it when onboarding a source, then leave it alone.

### Consuming from Redshift

Bronze needs no Redshift-side DDL: the Glue Data Catalog already describes
the Iceberg tables, and Spectrum reads them in place — no copy, no second
storage location.

```sql
CREATE EXTERNAL SCHEMA bronze_acme
FROM DATA CATALOG DATABASE 'bronze_acme'
IAM_ROLE 'arn:aws:iam::<account>:role/<redshift-role>';

-- USAGE is the read grant for a Spectrum external schema. It covers every
-- table in it, including ones created later, so onboarding a table needs no
-- Redshift change. There is no GRANT SELECT here: per-table grants on
-- external tables only work when the schema is registered with Lake
-- Formation, and against a plain Glue catalog the statement fails.
-- Authorization to the data itself is the IAM role above, not this grant.
GRANT USAGE ON SCHEMA bronze_acme TO GROUP analysts;

SELECT * FROM bronze_acme.order_fact;      -- with table_prefix: none
```

New tables appear in the external schema automatically. A new *source* needs
one more `CREATE EXTERNAL SCHEMA` only if it gets its own Glue database.

The Redshift IAM role needs `glue:GetDatabase*` / `glue:GetTable*` /
`glue:GetPartition*`, plus `s3:GetObject` and `s3:ListBucket` on the Bronze
location. If the catalog is Lake Formation-managed, IAM alone is not enough —
grant the role `SELECT` on the database in Lake Formation as well. The symptom
otherwise is a permissions error naming the table while the S3 grants look
correct.

Two things to know before pointing analysts at it:

- **Spectrum is read-only on Iceberg.** The Glue job owns all writes, which
  is what keeps Bronze's history intact.
- **Bronze retains every version of a row** — the merge identity is primary
  key + watermark, so an order edited three times is three rows. Collapsing
  to current state belongs in a view or in Silver, not here.

## Configuring a source

See `config/snowflake.example.yaml` for the fully commented template — it
is parsed by the test suite, so it cannot drift from the code. The shape:

```yaml
source:
  name: acme         # -> source_key "acme_snowflake"
  type: snowflake
  database: ACME_ANALYTICS   # optional defaults for every table below
  schema: REPORTING

connection:
  secret_id: acme-snowflake-ro  # Secrets Manager ID, not the credentials

landing:
  location: s3://my-data-lake-bucket/landing
  checkpoint_table: my-ingestion-checkpoints

bronze:                          # omit entirely to stop at landing
  database: bronze_acme
  location: s3://my-data-lake-bucket/bronze
  athena_output: s3://my-data-lake-bucket/athena-results/
  processed_runs_table: bronze-processed-runs
  # table_prefix: none                          # identity -- see Table naming
  # partition_by: ["month({checkpoint_column})"]  # the default; CREATE-time only

defaults:
  fetch_size: 10000
  fail_fast: true

tables:
  - name: order_fct             # identity: landing segment + DynamoDB sort key
    table: ORDER_FACT_V         # lineage only -- safe to change
    # database / schema inherited from source: above; override per table
    primary_key: [ORDER_KEY]
    checkpoint:
      type: watermark
      column: LAST_UPDATE_DTTM
      lookback_minutes: 0       # optional, defaults to 0
```

Name real configs `<system>_<type>.yaml` to match the derived `source_key`.
They stay gitignored (they carry account-specific bucket/secret/object
names) and are uploaded to S3, where the job reads them via `--config-uri`.

Adding table #13 is a YAML change only — no Python, no new DynamoDB setup
(the first run for a new table creates its own state record).

## Secrets vs config

Snowflake credentials (`account`, `username`, `password`, `warehouse`,
`role`) live in Secrets Manager, referenced by `connection.secret_id`.
Database/schema/table mappings are configuration, not secrets, and live in
the YAML.

## AWS resources you must create

None of these are created by the code; it fails loudly if they are missing.

### DynamoDB: extraction checkpoints  (`landing.checkpoint_table`)

```
partition key   source_key (String)   "acme_snowflake"   (= source.name_source.type)
sort key        table_name (String)   "order_fact"
TTL             DISABLED
PITR            recommended
```

TTL must stay off. A TTL here silently deletes checkpoints, and every
affected table then does a full reload — expensive and, on a large table,
not obviously distinguishable from a first run.

PITR matters more than it looks: this table is the only thing preventing a
full re-extraction of every table. Lose it and everything reloads.

```bash
aws dynamodb create-table \
  --table-name data-platform-checkpoints --region us-east-2 \
  --billing-mode PAY_PER_REQUEST \
  --attribute-definitions \
      AttributeName=source_key,AttributeType=S \
      AttributeName=table_name,AttributeType=S \
  --key-schema \
      AttributeName=source_key,KeyType=HASH \
      AttributeName=table_name,KeyType=RANGE

aws dynamodb update-continuous-backups \
  --table-name data-platform-checkpoints --region us-east-2 \
  --point-in-time-recovery-specification PointInTimeRecoveryEnabled=true
```

### DynamoDB: merged Bronze runs  (`bronze.processed_runs_table`)

Only needed if you use Bronze. Optional even then — without it every
committed run is re-merged on every pass, which is *correct* (the merge
deduplicates, so re-merging inserts nothing) but costs more as history
accumulates.

```
partition key   table_key (String)   "acme_snowflake:order_fact"
sort key        run_id    (String)
```

```bash
aws dynamodb create-table \
  --table-name bronze-processed-runs --region us-east-2 \
  --billing-mode PAY_PER_REQUEST \
  --attribute-definitions \
      AttributeName=table_key,AttributeType=S \
      AttributeName=run_id,AttributeType=S \
  --key-schema \
      AttributeName=table_key,KeyType=HASH \
      AttributeName=run_id,KeyType=RANGE
```

### Glue Data Catalog database  (`bronze.database`)

```bash
aws glue create-database --region us-east-2 \
  --database-input '{"Name":"bronze_acme"}'
```

One database per source, or one shared across all of them — both work,
because every key already carries `source_key`. Pick on **access isolation**:
Lake Formation grants are per database, so a source whose data shouldn't be
visible to everyone wants its own. Otherwise share one and Redshift needs a
single external schema forever. See `bronze.table_prefix` above, which should
match the choice.

### Secrets Manager

One secret per source, holding the connection credentials only — never
database/schema/table mappings, which are configuration:

```json
{"account": "...", "username": "...", "password": "...",
 "warehouse": "...", "role": "..."}
```

## IAM roles

Four roles, one per trust boundary. They are deliberately **not** one shared
role: the landing job holds Snowflake credentials and never touches Athena;
the Bronze job creates catalog tables and never sees a secret; Redshift reads
and writes nothing. A single role would give every one of those the union.

| Role | Trusted by | Holds |
|---|---|---|
| `data-ingest-landing-glue` | `glue.amazonaws.com` | Snowflake secret, write to landing, checkpoints |
| `data-ingest-bronze-glue` | `glue.amazonaws.com` | Read landing, write Bronze, Athena, Glue Catalog |
| `data-ingest-redshift-spectrum` | `redshift.amazonaws.com` | Read-only: Glue Catalog + Bronze S3 |
| `data-ingest-gha-publisher` | GitHub OIDC | Write the wheel to the artifact bucket (optional) |

Substitute `<account>`, `<region>`, `<data-lake-bucket>`, `<glue-assets-bucket>`,
`<bronze-db>`, and the secret name throughout.

### Trust policies

Both Glue roles:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "Service": "glue.amazonaws.com" },
    "Action": "sts:AssumeRole"
  }]
}
```

Redshift: the same with `redshift.amazonaws.com`.

### 1. Landing job role

Every statement maps to a call the job actually makes: `GetSecretValue` for
the Snowflake credentials, `PutObject` for Parquet and the manifest,
`GetItem`/`PutItem`/`Query` for checkpoints, and `GetObject` for the config
and the wheel.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "SnowflakeCredentials",
      "Effect": "Allow",
      "Action": "secretsmanager:GetSecretValue",
      "Resource": "arn:aws:secretsmanager:<region>:<account>:secret:<secret-name>-*"
    },
    {
      "Sid": "WriteLanding",
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:AbortMultipartUpload"],
      "Resource": "arn:aws:s3:::<data-lake-bucket>/landing/*"
    },
    {
      "Sid": "ReadConfigAndWheel",
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": [
        "arn:aws:s3:::<data-lake-bucket>/config/*",
        "arn:aws:s3:::<glue-assets-bucket>/scripts/*",
        "arn:aws:s3:::<glue-assets-bucket>/python/data_ingest/*"
      ]
    },
    {
      "Sid": "Checkpoints",
      "Effect": "Allow",
      "Action": ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:Query"],
      "Resource": "arn:aws:dynamodb:<region>:<account>:table/<checkpoint-table>"
    },
    {
      "Sid": "Logs",
      "Effect": "Allow",
      "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
      "Resource": "arn:aws:logs:<region>:<account>:log-group:/aws-glue/*"
    }
  ]
}
```

**No `s3:DeleteObject`.** Landing is immutable and lifecycle owns retention,
so the job has no business deleting a landed run — and a bug therefore
cannot. Clearing a prefix to re-land is a deliberate act by a human with
their own credentials.

**No `dynamodb:DeleteItem`.** Deleting a checkpoint triggers a full reload of
that table. Same reasoning.

### 2. Bronze job role

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ListLandingForRunDiscovery",
      "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
      "Resource": "arn:aws:s3:::<data-lake-bucket>",
      "Condition": {
        "StringLike": { "s3:prefix": ["landing/*", "bronze/*", "athena-results/*"] }
      }
    },
    {
      "Sid": "ReadLanding",
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::<data-lake-bucket>/landing/*"
    },
    {
      "Sid": "WriteBronzeAndAthenaResults",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject", "s3:PutObject",
        "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"
      ],
      "Resource": [
        "arn:aws:s3:::<data-lake-bucket>/bronze/*",
        "arn:aws:s3:::<data-lake-bucket>/athena-results/*"
      ]
    },
    {
      "Sid": "ReadConfigAndWheel",
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": [
        "arn:aws:s3:::<data-lake-bucket>/config/*",
        "arn:aws:s3:::<glue-assets-bucket>/scripts/*",
        "arn:aws:s3:::<glue-assets-bucket>/python/data_ingest/*"
      ]
    },
    {
      "Sid": "RunAthenaStatements",
      "Effect": "Allow",
      "Action": [
        "athena:StartQueryExecution",
        "athena:GetQueryExecution",
        "athena:StopQueryExecution",
        "athena:GetWorkGroup"
      ],
      "Resource": "arn:aws:athena:<region>:<account>:workgroup/primary"
    },
    {
      "Sid": "GlueCatalog",
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase", "glue:GetDatabases",
        "glue:GetTable", "glue:GetTables",
        "glue:CreateTable", "glue:UpdateTable",
        "glue:GetPartition", "glue:GetPartitions",
        "glue:BatchGetPartition", "glue:BatchCreatePartition", "glue:CreatePartition"
      ],
      "Resource": [
        "arn:aws:glue:<region>:<account>:catalog",
        "arn:aws:glue:<region>:<account>:database/<bronze-db>",
        "arn:aws:glue:<region>:<account>:table/<bronze-db>/*"
      ]
    },
    {
      "Sid": "ProcessedRuns",
      "Effect": "Allow",
      "Action": ["dynamodb:Query", "dynamodb:PutItem"],
      "Resource": "arn:aws:dynamodb:<region>:<account>:table/<processed-runs-table>"
    },
    {
      "Sid": "Logs",
      "Effect": "Allow",
      "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
      "Resource": "arn:aws:logs:<region>:<account>:log-group:/aws-glue/*"
    }
  ]
}
```

Why each of the less obvious ones:

- **`s3:ListBucket`** — run discovery paginates `list_objects_v2` over the
  landing tree. Without it Bronze reports "no committed landing runs found"
  rather than an access error, which reads as a config mistake.
- **`glue:UpdateTable`** — every Iceberg commit moves the table's
  `metadata_location` pointer. Without it the MERGE fails *after* writing
  data files.
- **`glue:BatchCreatePartition`** — the landing external table is Hive, and
  each run is registered with `ALTER TABLE ... ADD PARTITION`.
- **`glue:GetTable`** — read before write: schema evolution diffs the live
  catalog schema, and the orphaned-metadata check reads
  `metadata_location`.
- **`athena:GetWorkGroup`** — `StartQueryExecution` resolves the workgroup's
  result configuration before running.

**No `glue:DeleteTable` or `glue:DeleteDatabase`.** Nothing in the loader
drops a table, and the recovery procedures that do (a stale Iceberg catalog
entry, a `table_prefix` change) are deliberately manual. The error messages
print the `aws glue delete-table` command precisely because the job cannot
run it itself.

**No `s3:DeleteObject`.** The merge only ever inserts, so Iceberg writes new
files and never rewrites or deletes. If an `UPDATE`/`DELETE` clause is ever
added to the merge, this becomes required — treat needing it as a signal that
Bronze's retention model changed.

### 3. Redshift Spectrum role

Read-only. Spectrum cannot write Iceberg, and the Glue job owning all writes
is what keeps Bronze's history intact.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ReadCatalog",
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase", "glue:GetDatabases",
        "glue:GetTable", "glue:GetTables",
        "glue:GetPartition", "glue:GetPartitions", "glue:BatchGetPartition"
      ],
      "Resource": [
        "arn:aws:glue:<region>:<account>:catalog",
        "arn:aws:glue:<region>:<account>:database/<bronze-db>",
        "arn:aws:glue:<region>:<account>:table/<bronze-db>/*"
      ]
    },
    {
      "Sid": "ReadBronzeData",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"],
      "Resource": [
        "arn:aws:s3:::<data-lake-bucket>",
        "arn:aws:s3:::<data-lake-bucket>/bronze/*"
      ]
    }
  ]
}
```

Attach it to the cluster or Serverless namespace under **Associated IAM
roles**, then reference the ARN in `CREATE EXTERNAL SCHEMA`.

Scope `Resource` to one database per source if you run a database per source
and the isolation is meant to bind the Redshift role too, not just analysts.

### 4. GitHub Actions publisher (optional)

Only needed if CI uploads the wheel to S3 rather than a human doing it.

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["s3:PutObject"],
    "Resource": "arn:aws:s3:::<glue-assets-bucket>/python/data_ingest/*"
  }]
}
```

Trusted through GitHub's OIDC provider, with the subject condition pinned to
this repository so another repo cannot assume it:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {
      "Federated": "arn:aws:iam::<account>:oidc-provider/token.actions.githubusercontent.com"
    },
    "Action": "sts:AssumeRoleWithWebIdentity",
    "Condition": {
      "StringEquals": {
        "token.actions.githubusercontent.com:aud": "sts.amazonaws.com"
      },
      "StringLike": {
        "token.actions.githubusercontent.com:sub": "repo:<org>/data-ingest-aws:ref:refs/tags/v*"
      }
    }
  }]
}
```

Pinning `sub` to tag refs means only a released tag can publish — a pull
request from a fork cannot. No long-lived AWS keys in GitHub secrets.

### Two things these policies assume

**SSE-KMS.** If any bucket, the DynamoDB tables, or the secret use a customer
managed key, IAM alone is not enough — every role touching them also needs
`kms:Decrypt`, and the writing roles `kms:GenerateDataKey`, on that key ARN,
plus a matching key policy. Symptom is `AccessDenied` on an object whose
bucket policy plainly allows the call.

**Lake Formation.** If the Glue Data Catalog is LF-managed, IAM is only half
the check. The Bronze role needs LF `CREATE_TABLE`/`ALTER`/`DESCRIBE` on the
database, and the Redshift role `SELECT` (plus `lakeformation:GetDataAccess`).
Symptom is a permissions error naming the table while the S3 and Glue grants
look correct.

## Glue job arguments

Only `--config-uri` is required. `--state-table`, `--s3-bucket`,
`--s3-prefix`, `--fetch-size`, and `--fail-fast` can instead be set once in
the config YAML's `landing`/`defaults` sections (see
`config/snowflake.example.yaml`) — a CLI arg always wins if passed, so these exist
for one-off overrides (an ad-hoc retry with a different `--fetch-size`),
not as required boilerplate on every job definition.

```
--config-uri     s3://<bucket>/ingestion-config/acme_snowflake.yaml (required)
--state-table    data-platform-checkpoints                       (optional -- falls back to config YAML's landing.checkpoint_table)
--s3-bucket      <data-bucket>                                   (optional -- falls back to the bucket in config YAML's landing.location)
--s3-prefix      landing                                         (optional -- falls back to the prefix in config YAML's landing.location)
--tables         all | orders | orders,customers        (default "all")
--fetch-size     10000                                           (optional -- falls back to config YAML's defaults.fetch_size, default 10000)
--fail-fast      true | false                                    (optional -- falls back to config YAML's defaults.fail_fast, default true)
```

`--state-table` / `--s3-bucket` (from either source) are the only ones that
are actually required — the job fails fast with a clear `ConfigurationError`
at startup if neither the CLI arg nor the config YAML provides them.

`--extra-py-files` points at an exact, immutable wheel version — never a
mutable `latest` path:

```
--extra-py-files s3://<artifact-bucket>/python/data_ingest/<version>/data_ingest-<version>-py3-none-any.whl
--additional-python-modules snowflake-connector-python[pandas]==3.0.4
```

## Local development

This repo uses [mise](https://mise.jdx.dev/) to pin Python 3.9 (matching
the Glue Python Shell runtime) and manage a project-local venv.

```bash
mise trust
mise install          # installs Python 3.9, creates .venv/
mise run install       # pip install -e .[dev,snowflake]
mise run test           # pytest
mise run build          # builds dist/data_ingest-<version>-py3-none-any.whl
```

Without mise, the equivalent is:

```bash
python3.9 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev,snowflake]"
pytest
```

`from data_ingest import run_job` works immediately against source under
`src/` thanks to the editable install — no wheel rebuild needed while
iterating.

## Building and shipping the wheel

```bash
mise run build
# dist/data_ingest-<version>-py3-none-any.whl
```

Upload to a **versioned** S3 path (`python/data_ingest/<version>/...`),
never to a mutable `latest/` path, and point Terraform's
`--extra-py-files` at that exact version. Bump the version deliberately
before shipping a new artifact — see "CI/CD" below for how this repo
automates that.

## CI/CD

- **`.github/workflows/ci.yml`** — every PR and push to `main`, on Python
  3.9 (the Glue runtime version), in two jobs.

  **`test`** runs the suite twice, against two dependency sets, because a
  green run on either alone is misleading:

  | leg | install | catches |
  |---|---|---|
  | `glue-pinned` | `-c constraints-glue.txt` | behaviour that differs on the versions Glue actually ships |
  | `floating` | whatever pip resolves today | our code breaking on modern pandas/numpy |

  The pinned leg matters because this package's behaviour is coupled to
  pandas and pyarrow semantics that move between versions — int64 →
  decimal128 coercion, `from_pandas` being stricter than `Table.cast`,
  all-NULL columns inferring as Arrow `null`. The floating leg is the early
  warning for Glue shipping a newer runtime. Both block; the coverage floor
  is enforced once, on the pinned leg.

  Only `-c`-listed packages are pinned — `pytest`, `moto`, and `werkzeug`
  float on both legs, so each records its resolved versions in the log. moto
  against Glue's botocore 1.24.21 is a pairing nobody upstream tests; if moto
  drops support for it, pin moto in the `dev` extra rather than loosening
  `constraints-glue.txt`.

  **`package`** builds and validates the artifact that actually ships:

  1. build the wheel and sdist;
  2. install *the built wheel* into a throwaway venv and import the Glue
     entry point's full chain (including `SnowflakeSource`, which is what
     transitively pulls `snowflake.connector`);
  3. `pip check` for internal consistency;
  4. install the wheel against `constraints-glue.txt` and assert the
     interpreter **exits 0** — a shutdown segfault (exit 139) would make
     Glue mark a successful run FAILED and retry it, duplicating a landing
     run;
  5. install the wheel on top of Glue's exact preinstalled versions and
     fail if pip would replace any of them.

  Step 5 is the highest-value check here: this package's whole risk profile
  is "does it coexist with Glue's preinstalled stack," and it catches a bad
  pin before it can reach a job definition. `package` is a separate job
  because it builds its own venvs and does not depend on how `test`
  installed anything — running it per matrix leg would double the work for
  an identical result.
- **`.github/workflows/release-please.yml`** — on push to `main`,
  [Release Please](https://github.com/googleapis/release-please) maintains
  a standing release PR derived from
  [Conventional Commits](https://www.conventionalcommits.org/):
  - `fix: ...` -> patch (`0.1.0` -> `0.1.1`)
  - `feat: ...` -> minor (`0.1.1` -> `0.2.0`)
  - `feat!: ...` / `BREAKING CHANGE:` -> major (`0.2.0` -> `1.0.0`)

  **Versioning posture:** 1.0.0 was reached while Bronze was still
  unexercised against Athena, so treat 1.x as "landing is stable, Bronze may
  still move" rather than a strict API-stability guarantee. Breaking changes
  during that period are marked honestly and bump the major -- the changelog
  staying truthful matters more than the number staying small. Tighten to
  strict semver once Bronze has run in production.

  Release Please also bumps `__version__` in `src/data_ingest/__init__.py`
  (via the `extra-files` entry in `release-please-config.json` and the
  `x-release-please-version` marker), so the version reported at runtime
  can't drift from the tag.

  Merging that PR creates a `vX.Y.Z` tag and GitHub release, which triggers
  the *same* workflow run (release-please's own commit) to build the wheel,
  run the test suite **against the built wheel** (not an editable source
  install — that's the artifact that ships), and attach the wheel/sdist to
  the GitHub release (`gh release upload`). Build+test are kept in the same
  workflow as the release step because a release created with the default
  `GITHUB_TOKEN` does not itself trigger a separate `on: release` workflow.

  **S3 publish is intentionally not wired up.** Glue's `--extra-py-files`
  needs an S3 URI, not a GitHub release asset, so getting the wheel from
  "attached to a GitHub release" to "at a versioned S3 path" is still a
  manual step (or a follow-up you add later) — download the release asset
  and `aws s3 cp` it to `s3://<artifact-bucket>/python/data_ingest/<version>/`.
  When you're ready to automate that: add a step to `release-please.yml`
  using an OIDC-trusted IAM role (trust policy scoped to this repo via
  GitHub's OIDC provider, `s3:PutObject` on the artifact bucket's
  `python/data_ingest/*` prefix) and `aws-actions/configure-aws-credentials`
  — no long-lived AWS keys in GitHub secrets.

- Terraform then pins the exact version deliberately
  (`data_ingest_version = "0.2.1"`), so a Glue behavior change always shows
  up as a reviewable Terraform diff, and rollback is just pinning the prior
  version.

## Failure/retry behavior

If a table extraction raises at any point before the DynamoDB commit, the
job logs the failure, does **not** advance that table's checkpoint, and
(by default, `fail_fast=true`) stops before starting the next table. Tables
that already committed successfully in this run keep their new checkpoint.
Re-running the job retries the failed table's window from where it left
off; already-succeeded tables perform their next incremental load as
normal.

## Adding a future source adapter

Source types are plugged in through a registry
(`data_ingest.sources.registry`), not by editing `pipeline.py`. To add a
new one (MySQL, SQL Server, a REST API, CSV files landing in S3, ...):

1. Create `data_ingest/sources/<type>.py` implementing
   `data_ingest.sources.base.Source` (`get_current_checkpoint`, `extract`,
   `metadata`) and, if it needs a checkpoint shape other than a watermark,
   `data_ingest.checkpoints.base.Checkpoint`.
2. Add a module-level `build_source(credentials, table_config, fetch_size)
   -> Source` factory function to that same module (see
   `sources/snowflake.py` for the reference implementation).
3. Register it in `sources/registry.py`'s `_SOURCE_MODULES` dict:
   `"mysql": "data_ingest.sources.mysql"`.

That's it -- `pipeline.py` never imports a specific adapter and doesn't
change. Adapter modules are imported lazily (only when a config actually
declares that `source.type`), so a Snowflake-only deployment never needs a
MySQL/REST/etc adapter's dependencies installed. The transactional sequence
(checkpoint read -> extract -> land -> manifest -> commit), landing layout,
manifest schema, and state store are all reused unchanged regardless of
source type.

## Known Glue constraints

These exist because of the specific runtime this deploys to. Read
`constraints-glue.txt` and the RUNTIME CONTRACT comment in `pyproject.toml`
before touching any dependency pin.

### IAM the Glue roles need

See [IAM roles](#iam-roles) above for the full policies.

### The two Glue job definitions

Both use the same wheel and the same config file; they differ in script,
sizing, and which extras they need.

**Landing job** — `jobs/landing_load_snowflake.py`, one per source type:

```
Python version         3.9
Max capacity           1              # smallest that fits a 10k-row batch
MaxConcurrentRuns      1              # cheapest defense against a checkpoint race
--library-set          analytics      # REQUIRED: supplies pandas/numpy/boto3
--additional-python-modules  snowflake-connector-python[pandas]==3.0.4
--extra-py-files       s3://<artifact-bucket>/python/data_ingest/<version>/data_ingest-<version>-py3-none-any.whl
--config-uri           s3://<bucket>/ingestion-config/<source>_<type>.yaml
```

The `[pandas]` extra is **load-bearing**: Glue's analytics library-set does
not ship pyarrow, and that extra is what supplies it (pinned by the
connector to `>=10.0.1,<10.1.0`). Dropping `[pandas]` breaks Parquet writing.

**Bronze job** — `jobs/bronze_load.py`, one total, whatever the source:

```
Python version         3.9
Max capacity           0.0625         # 16x cheaper; Athena does the work
--extra-py-files       s3://<artifact-bucket>/python/data_ingest/<version>/data_ingest-<version>-py3-none-any.whl
--config-uri           s3://<bucket>/ingestion-config/<source>_<type>.yaml
```

Note what the Bronze job does **not** need: no `--library-set`, no
`--additional-python-modules`, no Snowflake connector. It imports no pandas
or pyarrow (asserted in tests), which is what lets it run at the smallest
capacity. It never opens a source connection — it reads the landing layout
and drives Athena.

### Dependency pins

- `snowflake-connector-python[pandas]==3.0.4` is the version proven to work
  under Glue Python Shell **3.9**. Newer connector versions caused
  urllib3/OpenSSL incompatibilities on that runtime.

  **When can this be bumped?** The pin is tied to the Python version, not to
  Snowflake. If Glue ships a Python Shell runtime newer than 3.9, revisit
  it: the urllib3/OpenSSL conflict stems from the old runtime's system
  OpenSSL and the vendored urllib3 that connector generation shipped with. On
  a newer Python, a current connector is likely both possible and preferable
  (security fixes, `TIMESTAMP` handling, performance). Re-derive
  `constraints-glue.txt` from the new image's actual preinstalled versions
  and let CI's Glue-compatibility job verify the combination — don't
  hand-bump individual lines.

- **`--extra-py-files` with a `.whl` does invoke pip**, and pip resolves this
  wheel's `Requires-Dist` against what the Glue image already has. Pins that
  disagree with the runtime are not inert — they make pip swap a library out
  from under a running job. CI has a job that installs the wheel over Glue's
  exact preinstalled versions and fails if anything would be replaced.

- **numpy and pandas must be a matched build pair.** A mismatched pair (e.g.
  numpy 1.23.5 with pandas 1.4.2) segfaults the interpreter *at shutdown* —
  exit 139 after all work completes. Glue reads that as a FAILED run and
  retries, duplicating a landing run that actually succeeded. This is why
  `constraints-glue.txt` pins them together rather than as independent
  ranges.

### Memory: the initial full load is the hard case

Glue Python Shell is capped at **1 DPU / 16 GB and cannot be scaled up**
(`max_capacity` accepts only `0.0625` or `1`), so memory is a fixed ceiling
rather than something to provision around. The first full load of a table is
where that bites — every later run is an increment.

Measured: a 1.3M-row full load of a wide fact table was SIGKILLed
(**exit 137**, the container OOM) at `fetch_size: 50000`, and completed at
`10000`. Two things consume the budget:

- **Batch size.** Rows arrive from the DB-API as tuples of Python objects, so
  every column lands as pandas `object` dtype — individually boxed values
  rather than packed arrays. That is far heavier per row than a typed frame,
  and wide tables or `VARIANT`/large `VARCHAR` columns push it up further.
- **Connector read-ahead.** `fetchmany()` bounds how many rows *we* hold, not
  how much the connector buffers behind it: it downloads result chunks in
  background threads and queues them ahead of the cursor. The library default
  of 4 assumes headroom we don't have, so the adapter pins
  `client_prefetch_threads` to 2.

If a run dies with exit 137, lower `defaults.fetch_size` in the config YAML
and re-upload — no rebuild or redeploy, since the job reads it from S3 at
runtime. Failed runs cost nothing in correctness: no manifest was written, so
the checkpoint never advanced and the retry starts clean. The orphaned
Parquet under that `run_id` is inert and ages out via lifecycle.

Still open: bounded/chunked initial load, so a large first load is resumable
in windows rather than all-or-nothing. Until then, a table big enough to
exhaust 16 GB even at a small `fetch_size` needs its first load seeded
another way.

### Exit codes worth recognizing

| Code | Meaning |
|---|---|
| `2` | argparse rejected the job arguments — usually a missing `--config-uri`. The explanation goes to stderr, which lands in `/aws-glue/python-jobs/error`, a *different* log group from the output one. |
| `137` | SIGKILL, i.e. the container ran out of memory. See above. |
| `1` | A table failed; the checkpoint did not advance. See "Failure/retry behavior". |

### Extraction behavior

- Extraction deliberately uses `cursor.fetchmany()` + `pd.DataFrame(rows,
  columns=columns)`, not `cursor.fetch_pandas_batches()`. Don't reintroduce
  the Arrow-based fetch path without proving compatibility first.
- Glue supplies extra CLI args beyond the ones this job defines; argument
  parsing uses `parse_known_args()`, not `parse_args()`.
- The Snowflake session pins `TIMEZONE=UTC` and
  `TIMESTAMP_TYPE_MAPPING=TIMESTAMP_NTZ` at connect time, so a change to an
  account or role default can't silently reinterpret a stored watermark.

