"""
Minimal Athena client: submit a statement, wait for it, fail loudly.

Athena is asynchronous -- StartQueryExecution returns immediately and the
work happens elsewhere -- so every call here polls to a terminal state. That
matters more than it looks: without waiting, the loader would record a run as
processed while its MERGE was still running, or had already failed.
"""

import time

from data_ingest.exceptions import DataIngestError
from data_ingest.logging import get_logger

logger = get_logger(__name__)

TERMINAL_STATES = {"SUCCEEDED", "FAILED", "CANCELLED"}

DEFAULT_POLL_SECONDS = 2
DEFAULT_TIMEOUT_SECONDS = 3600


class AthenaError(DataIngestError):
    """An Athena statement failed, was cancelled, or timed out."""


class AthenaClient:
    def __init__(
        self,
        client,
        database,
        output_location,
        workgroup="primary",
        poll_seconds=DEFAULT_POLL_SECONDS,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    ):
        self._client = client
        self.database = database
        self.output_location = output_location
        self.workgroup = workgroup
        self.poll_seconds = poll_seconds
        self.timeout_seconds = timeout_seconds

    def execute(self, sql, description=None):
        """
        Run one statement to completion. Returns the query execution id.

        Raises AthenaError on FAILED/CANCELLED/timeout -- never returns
        having quietly not done the work.
        """
        label = description or sql.strip().split("\n", 1)[0][:80]
        logger.info("Athena: %s", label)

        response = self._client.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": self.database},
            ResultConfiguration={"OutputLocation": self.output_location},
            WorkGroup=self.workgroup,
        )
        execution_id = response["QueryExecutionId"]

        state, reason, stats = self._wait(execution_id, label)

        if state != "SUCCEEDED":
            raise AthenaError(
                f"Athena statement {state} ({label}): {reason or 'no reason given'} "
                f"[QueryExecutionId={execution_id}]"
            )

        scanned = stats.get("DataScannedInBytes")
        logger.info(
            "Athena SUCCEEDED (%s) in %sms, scanned %s bytes [%s]",
            label,
            stats.get("TotalExecutionTimeInMillis"),
            scanned if scanned is not None else "n/a",
            execution_id,
        )
        return execution_id

    def _wait(self, execution_id, label):
        deadline = time.monotonic() + self.timeout_seconds

        while True:
            execution = self._client.get_query_execution(QueryExecutionId=execution_id)[
                "QueryExecution"
            ]
            status = execution["Status"]
            state = status["State"]

            if state in TERMINAL_STATES:
                return (
                    state,
                    status.get("StateChangeReason"),
                    execution.get("Statistics", {}),
                )

            if time.monotonic() >= deadline:
                # Stop the statement rather than abandoning it: an orphaned
                # MERGE would keep consuming and could still commit after we
                # have given up and reported failure.
                try:
                    self._client.stop_query_execution(QueryExecutionId=execution_id)
                except Exception:
                    logger.warning("Could not cancel timed-out query %s", execution_id, exc_info=True)
                raise AthenaError(
                    f"Athena statement exceeded {self.timeout_seconds}s and was cancelled "
                    f"({label}) [QueryExecutionId={execution_id}]"
                )

            time.sleep(self.poll_seconds)
