"""
Landing -> Iceberg Bronze.

Deliberately source-agnostic. Landing is the normalization boundary: once a
run is Parquet plus a valid _manifest.json under the standard layout, this
package does not care whether Snowflake, a REST API, or a CSV drop produced
it. That is why Bronze lives in the same repo as ingestion rather than being
re-implemented per source -- it is the *more* reusable half.

Compute is Athena's, not ours. The loader is orchestration only: find runs,
skip processed ones, register the partition, issue a MERGE, record the run.
That keeps it inside a Glue Python Shell job (1 DPU / 16 GB) no matter how
large the table, because the merge never runs in this process.
"""

from data_ingest.bronze.job import run_bronze_job
from data_ingest.bronze.loader import load_bronze, load_table_runs

__all__ = ["run_bronze_job", "load_bronze", "load_table_runs"]

# Nothing here imports pandas/pyarrow, and a test asserts it stays that way:
# this job is pure orchestration (Athena does the work), so it can run on
# Glue's smallest Python Shell size rather than paying for 1 DPU.
