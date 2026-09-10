#!/usr/bin/env python3
"""Glue Python Shell entry point for hourly S3 POS events -> S3 landing."""

from data_ingest import run_job
from data_ingest.sources.par_pos_s3 import ParPosS3Source  # noqa: F401 -- fail fast on runtime dependencies


if __name__ == "__main__":
    run_job(expected_source_type="par_pos_s3")
