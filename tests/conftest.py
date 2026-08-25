"""
Shared test setup.

Pins the AWS region and installs dummy credentials for the whole suite.
Without this, boto3 clients created inside the code under test (which pass
no explicit region) pick up the developer's ambient AWS config -- so tests
that provision moto resources in one region silently fail to find them in
another, and results differ between a laptop and CI. The dummy credentials
also guarantee nothing can reach real AWS if a moto mock is ever missing.
"""

import os

import pytest

AWS_REGION = "us-east-1"


@pytest.fixture(autouse=True)
def aws_test_environment(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", AWS_REGION)
    monkeypatch.setenv("AWS_REGION", AWS_REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    # Never let a stray profile from the developer's environment apply.
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    yield


@pytest.fixture
def package_logs():
    """
    Capture records from the `data_ingest` logger.

    pytest's caplog attaches to the ROOT logger, but configure_logging()
    deliberately sets propagate=False on the package logger so framework
    records cannot double-log through whatever the host (Glue) attached to
    root. That means caplog sees nothing, and a test asserting on a warning
    would silently pass for the wrong reason. This attaches to the package
    logger directly.
    """
    import logging

    records = []

    class _Collector(logging.Handler):
        def emit(self, record):
            records.append(record)

    logger = logging.getLogger("data_ingest")
    handler = _Collector(level=logging.DEBUG)
    previous_level = logger.level
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    try:
        yield records
    finally:
        logger.removeHandler(handler)
        logger.setLevel(previous_level)
