#!/usr/bin/env python3
"""Glue Python Shell entry point for hourly S3 POS events -> S3 landing."""

from data_ingest import run_job
from data_ingest.sources.s3_json_gz import S3JsonGzSource  # noqa: F401 -- fail fast on runtime dependencies


if __name__ == "__main__":
    run_job(expected_source_type="s3_json_gz")
