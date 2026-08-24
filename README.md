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
  v
Glue Python Shell job (jobs/landing_load_snowflake.py)
  |
  +-- data_ingest wheel (this package)
  |
  +-- source/table YAML config
  |
  v
S3 Landing (immutable, run_id-partitioned Parquet + _manifest.json)
  |
  v
Bronze Loader (separate project, not part of this repo) -> Iceberg -> Redshift/Athena
```

Something else should own infrastructure (S3, Glue job, IAM, DynamoDB, Secrets Manager
references). This package owns extraction/transaction logic. Neither should
know about the other's internals.

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

## Configuring a source

See `config/snowflake.example.yaml` for a fully commented template. The
shape:

```yaml
source:
  name: acme         # -> source_key "acme_snowflake"
  type: snowflake

connection:
  secret_id: acme-snowflake-ro  # Secrets Manager ID, not the credentials

tables:
  - name: order_fct             # identity: landing segment + DynamoDB sort key
    database: ACME_ANALYTICS
    schema: REPORTING             # lineage only -- safe to change
    table: ORDER_FACT_V          # lineage only -- safe to change
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
--extra-py-files s3://<artifact-bucket>/python/data_ingest/0.1.0/data_ingest-0.1.0-py3-none-any.whl
--additional-python-modules snowflake-connector-python==3.0.4
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
# dist/data_ingest-0.1.0-py3-none-any.whl
```

Upload to a **versioned** S3 path (`python/data_ingest/<version>/...`),
never to a mutable `latest/` path, and point Terraform's
`--extra-py-files` at that exact version. Bump the version deliberately
before shipping a new artifact — see "CI/CD" below for how this repo
automates that.

## CI/CD

- **`.github/workflows/ci.yml`** — every PR and push to `main`, on Python
  3.9 (the Glue runtime version):
  1. run pytest;
  2. build the wheel;
  3. install *the built wheel* into a throwaway venv and import the Glue
     entry point's full chain (including `SnowflakeSource`, which is what
     transitively pulls `snowflake.connector`);
  4. `pip check` for internal consistency;
  5. install the wheel against `constraints-glue.txt` and assert the
     interpreter **exits 0** — a shutdown segfault (exit 139) would make
     Glue mark a successful run FAILED and retry it, duplicating a landing
     run;
  6. install the wheel on top of Glue's exact preinstalled versions and
     fail if pip would replace any of them.

  Step 6 is the highest-value job here: this package's whole risk profile is
  "does it coexist with Glue's preinstalled stack," and it catches a bad pin
  before it can reach a job definition.
- **`.github/workflows/release-please.yml`** — on push to `main`,
  [Release Please](https://github.com/googleapis/release-please) maintains
  a standing release PR derived from
  [Conventional Commits](https://www.conventionalcommits.org/):
  - `fix: ...` -> patch (`0.1.0` -> `0.1.1`)
  - `feat: ...` -> minor (`0.1.1` -> `0.2.0`)
  - `feat!: ...` / `BREAKING CHANGE:` -> major (`0.2.0` -> `1.0.0`)

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

### IAM the Glue role needs

Beyond S3/DynamoDB/Secrets Manager access, the role must be able to write
logs, or the job runs blind:

```
logs:CreateLogGroup
logs:CreateLogStream
logs:PutLogEvents      on arn:aws:logs:*:*:log-group:/aws-glue/*
```

These come with the managed `AWSGlueServiceRole` policy, so this only bites
on a hand-rolled least-privilege role. The log groups are created lazily on
first write, so a missing permission shows up as "log group does not exist"
rather than an access error — indistinguishable at a glance from a job that
never ran.

### The Glue job definition

The job's correctness and durability depend on four settings that live in
the job definition, not in this repo:

```
--library-set          analytics    # REQUIRED: supplies pandas/numpy/boto3
--additional-python-modules  snowflake-connector-python[pandas]==3.0.4
--extra-py-files       s3://<artifact-bucket>/python/data_ingest/<version>/data_ingest-<version>-py3-none-any.whl
MaxConcurrentRuns      1            # cheapest defense against a checkpoint race
```

The `[pandas]` extra is **load-bearing**: Glue's analytics library-set does
not ship pyarrow, and that extra is what supplies it (pinned by the
connector to `>=10.0.1,<10.1.0`). Dropping `[pandas]` breaks Parquet writing.

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

### DynamoDB state table contract

```
partition key   source_key (String)   e.g. "acme_snowflake"  (= name_type)
sort key        table_name (String)   e.g. "order_fct"
TTL             DISABLED  -- a TTL here silently deletes checkpoints, and
                             every affected table then does a full reload
PITR            recommended
```

Create it with:

```bash
aws dynamodb create-table \
  --table-name data-platform-watermarks \
  --region us-east-2 \
  --billing-mode PAY_PER_REQUEST \
  --attribute-definitions \
      AttributeName=source_key,AttributeType=S \
      AttributeName=table_name,AttributeType=S \
  --key-schema \
      AttributeName=source_key,KeyType=HASH \
      AttributeName=table_name,KeyType=RANGE

aws dynamodb update-continuous-backups \
  --table-name data-platform-watermarks --region us-east-2 \
  --point-in-time-recovery-specification PointInTimeRecoveryEnabled=true
```

The composite key is deliberate: it makes "every checkpoint for this source"
a single `Query` (`DynamoDBStateStore.list_for_source`), which is what
staleness monitoring and an on-call "did everything run last night?" check
need. A single opaque partition key would force a full table `Scan`.

`source_name` and `source_type` are also written as their own attributes.
They're redundant with `source_key` by construction, but they keep the table
readable in the console and let a filter target either without
string-splitting the key.

Note the key is **not** `DATABASE.SCHEMA.TABLE` — see "Identity". A table
still using the original script's `table_name`-only partition key must be
recreated; DynamoDB cannot change a key schema in place.

Point-in-time recovery matters more than it looks: this table is the only
thing preventing a full re-extraction of every table. If it's lost, every
table sees no prior checkpoint and does a full load on the next run.
