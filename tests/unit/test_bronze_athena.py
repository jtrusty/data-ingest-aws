"""
Athena client behaviour.

The stakes here are higher than the module's size suggests. Athena is
asynchronous: StartQueryExecution returns immediately and the work happens
elsewhere. If this client mistakes "still running" or "failed" for "done",
the loader records the run as merged and moves on -- a silent data gap, in a
pipeline whose entire design is about making silent gaps impossible.
"""

import pytest

from data_ingest.bronze.athena import AthenaClient, AthenaError


class FakeAthenaBackend:
    """Minimal stand-in for the boto3 athena client."""

    def __init__(self, states, statistics=None, reason=None):
        # One entry per get_query_execution call, so a test can walk a query
        # through RUNNING -> RUNNING -> SUCCEEDED.
        self.states = list(states)
        self.statistics = statistics or {}
        self.reason = reason
        self.started = []
        self.stopped = []
        self.poll_count = 0

    def start_query_execution(self, **kwargs):
        self.started.append(kwargs)
        return {"QueryExecutionId": "qid-1"}

    def get_query_execution(self, QueryExecutionId):
        self.poll_count += 1
        state = self.states.pop(0) if len(self.states) > 1 else self.states[0]
        status = {"State": state}
        if self.reason:
            status["StateChangeReason"] = self.reason
        return {"QueryExecution": {"Status": status, "Statistics": self.statistics}}

    def stop_query_execution(self, QueryExecutionId):
        self.stopped.append(QueryExecutionId)


def make_client(backend, **overrides):
    kwargs = dict(
        client=backend,
        database="bronze_db",
        output_location="s3://bucket/athena-results/",
        workgroup="primary",
        poll_seconds=0,          # keep tests fast
        timeout_seconds=10,
    )
    kwargs.update(overrides)
    return AthenaClient(**kwargs)


def test_succeeded_statement_returns_the_execution_id():
    client = make_client(FakeAthenaBackend(["SUCCEEDED"]))
    assert client.execute("SELECT 1") == "qid-1"


def test_polls_until_a_terminal_state():
    backend = FakeAthenaBackend(["QUEUED", "RUNNING", "RUNNING", "SUCCEEDED"])
    make_client(backend).execute("SELECT 1")
    assert backend.poll_count == 4, "must keep polling while non-terminal"


def test_failed_statement_raises_with_the_reason():
    """
    The critical case. A FAILED merge that returned normally would let the
    loader record the run as processed, leaving a permanent gap in Bronze
    that nothing downstream would flag.
    """
    backend = FakeAthenaBackend(["FAILED"], reason="SYNTAX_ERROR: line 1:1")
    with pytest.raises(AthenaError) as exc_info:
        make_client(backend).execute("MERGE INTO x", description="merge x")

    message = str(exc_info.value)
    assert "FAILED" in message
    assert "SYNTAX_ERROR" in message, "the reason is the whole diagnostic value"
    assert "qid-1" in message, "the execution id is how you find it in the console"


def test_cancelled_statement_raises():
    # A human cancelling in the console must not read as success.
    with pytest.raises(AthenaError, match="CANCELLED"):
        make_client(FakeAthenaBackend(["CANCELLED"])).execute("MERGE INTO x")


def test_failure_without_a_reason_still_raises_readably():
    with pytest.raises(AthenaError, match="no reason given"):
        make_client(FakeAthenaBackend(["FAILED"])).execute("SELECT 1")


def test_timeout_cancels_the_query_rather_than_abandoning_it():
    """
    An abandoned MERGE keeps consuming, and could still COMMIT after we have
    given up and reported failure -- so the loader would retry a run that
    then succeeds twice. Stopping it makes the outcome definite.
    """
    backend = FakeAthenaBackend(["RUNNING"])
    with pytest.raises(AthenaError, match="exceeded"):
        make_client(backend, timeout_seconds=0).execute("MERGE INTO x")

    assert backend.stopped == ["qid-1"], "the timed-out query must be cancelled"


def test_a_failure_to_cancel_does_not_mask_the_timeout():
    class Stubborn(FakeAthenaBackend):
        def stop_query_execution(self, QueryExecutionId):
            raise RuntimeError("cannot stop")

    # The timeout is the real problem; a failed cancel must not replace it
    # with a confusing secondary error.
    with pytest.raises(AthenaError, match="exceeded"):
        make_client(Stubborn(["RUNNING"]), timeout_seconds=0).execute("SELECT 1")


def test_statement_is_submitted_with_the_configured_context():
    backend = FakeAthenaBackend(["SUCCEEDED"])
    make_client(backend).execute("SELECT 1")

    submitted = backend.started[0]
    assert submitted["QueryString"] == "SELECT 1"
    assert submitted["QueryExecutionContext"] == {"Database": "bronze_db"}
    assert submitted["ResultConfiguration"] == {
        "OutputLocation": "s3://bucket/athena-results/"
    }
    assert submitted["WorkGroup"] == "primary"


def test_scan_statistics_are_tolerated_when_absent():
    # DDL statements scan nothing, so Statistics can come back sparse --
    # logging must not blow up on the happy path.
    backend = FakeAthenaBackend(["SUCCEEDED"], statistics={})
    assert make_client(backend).execute("ALTER TABLE x ADD PARTITION") == "qid-1"
