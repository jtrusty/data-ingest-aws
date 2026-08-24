#!/usr/bin/env python3
"""
Glue Python Shell entry point for Snowflake -> S3 Landing ingestion.

Intentionally boring: all extraction/transaction logic lives in the
data_ingest wheel (--extra-py-files). This script just runs it.

Why the SnowflakeSource import below, if run_job() picks the source
adapter from config.yaml's `source.type` on its own? Two reasons:

1. Fail fast at job startup, not mid-run. If snowflake-connector-python
   isn't on the Glue runtime's path (missing from
   --additional-python-modules, wrong version, etc), we want that error
   the moment this script starts -- not buried inside the first table's
   extraction after DynamoDB/S3 clients are already spun up.
2. This file is deployed and named as THE Snowflake job. Passing
   expected_source_type="snowflake" to run_job() below means a config
   mistake (wrong --config-uri, a typo'd `source.type` in the YAML) fails
   loudly with a clear error instead of silently matching zero tables.
"""

from data_ingest import run_job
from data_ingest.sources.snowflake import SnowflakeSource  # noqa: F401 -- see module docstring

if __name__ == "__main__":
    run_job(expected_source_type="snowflake")
