"""
Which landing runs have already been merged into Bronze.

This is a COST optimization, not a correctness mechanism, and the difference
matters when reasoning about failure. The merge itself is idempotent --
`WHEN NOT MATCHED THEN INSERT` re-inserts nothing -- so re-processing a run
produces identical Bronze content. What this table buys is not having to
re-scan every historical run on every load, which for a table with hundreds
of runs is the entire cost of the job.

Consequence worth stating: a crash between the merge succeeding and the run
being recorded is harmless. The next pass merges it again and inserts zero
rows. That is the same fail-safe shape as the ingestion side, where a crash
before the checkpoint commit just replays the window.

Table layout:

    partition key   table_key (String)   "<source_key>:<table_name>"
    sort key        run_id    (String)

Composite so "which runs have been processed for this table" is one Query.
"""

from datetime import datetime, timezone

from data_ingest.logging import get_logger

logger = get_logger(__name__)


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


class ProcessedRunStore:
    """DynamoDB-backed record of merged runs."""

    def __init__(self, table):
        self._table = table

    @staticmethod
    def _table_key(source_key, table_name):
        return f"{source_key}:{table_name}"

    def processed_run_ids(self, source_key, table_name):
        """
        Every run_id already merged for this table, as a set.

        Fetched in one Query up front rather than a GetItem per run: a table
        with 500 historical runs would otherwise mean 500 round trips before
        any work started.
        """
        from boto3.dynamodb.conditions import Key as _Key

        table_key = self._table_key(source_key, table_name)
        run_ids = set()
        kwargs = {
            "KeyConditionExpression": _Key("table_key").eq(table_key),
            # Only the sort key is needed; skip hauling back manifests.
            "ProjectionExpression": "run_id",
        }
        while True:
            response = self._table.query(**kwargs)
            run_ids.update(item["run_id"] for item in response.get("Items", []))
            if "LastEvaluatedKey" not in response:
                return run_ids
            kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]

    def mark_processed(self, source_key, table_name, run_id, row_count, load_type):
        """
        Record a run as merged. Called only AFTER the merge succeeds.

        No conditional write: re-marking an already-marked run is harmless,
        and the merge it records is idempotent anyway.
        """
        self._table.put_item(
            Item={
                "table_key": self._table_key(source_key, table_name),
                "run_id": run_id,
                "bronze_status": "SUCCESS",
                "bronze_processed_at": utc_now_iso(),
                "source_row_count": row_count,
                "load_type": load_type,
            }
        )
        logger.info("Recorded run %s as merged for %s/%s", run_id, source_key, table_name)


class NullProcessedRunStore:
    """
    Used when no processed_runs_table is configured.

    Every run is re-merged on every pass. Correct, because the merge is
    idempotent -- but the cost grows with history, so this suits a first
    trial or a table with few runs, not steady state.
    """

    def processed_run_ids(self, source_key, table_name):
        logger.warning(
            "No processed_runs_table configured: every committed run for %s/%s will be "
            "re-merged. Correct but increasingly expensive as runs accumulate.",
            source_key, table_name,
        )
        return set()

    def mark_processed(self, source_key, table_name, run_id, row_count, load_type):
        return None
