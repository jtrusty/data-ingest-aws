#!/usr/bin/env python3
"""
Glue Python Shell entry point for Landing -> Iceberg Bronze.

Same shape as jobs/landing_load_snowflake.py: all logic lives in the data_ingest
wheel (--extra-py-files), and this script just runs it.

Unlike the ingestion job, this one is source-agnostic -- there is no
expected_source_type assertion, because Bronze reads the landing layout
rather than any source. The same script loads a Snowflake source, a REST
source, or a CSV source; only --config-uri changes.

It also has no meaningful memory profile: Athena does the merging, so rows
never pass through this process. The 1 DPU / 16 GB ceiling that constrains
ingestion is not in play here regardless of table size.
"""

from data_ingest.bronze import run_bronze_job

if __name__ == "__main__":
    run_bronze_job()
