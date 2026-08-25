import io
import logging

import pytest

from data_ingest import logging as di_logging


@pytest.fixture(autouse=True)
def reset_logging_state():
    """
    configure_logging() is process-global and idempotent, so each test has to
    start from a clean slate or ordering decides the result.
    """
    package_logger = logging.getLogger(di_logging.PACKAGE_LOGGER_NAME)
    original_handlers = list(package_logger.handlers)
    original_level = package_logger.level
    original_propagate = package_logger.propagate

    root = logging.getLogger()
    root_handlers = list(root.handlers)
    root_level = root.level

    di_logging._CONFIGURED = False
    package_logger.handlers = []
    yield

    di_logging._CONFIGURED = False
    package_logger.handlers = original_handlers
    package_logger.setLevel(original_level)
    package_logger.propagate = original_propagate
    root.handlers = root_handlers
    root.setLevel(root_level)


def test_emits_when_the_host_already_configured_root_logging():
    """
    The case that decides whether the job can be diagnosed at all.

    AWS Glue configures root logging before the job script runs and leaves
    the root level above INFO. A configure_logging() that calls basicConfig()
    only `if not logging.getLogger().handlers` therefore does nothing under
    Glue: root filters every INFO record and the framework emits no logs.

    A test process has no pre-existing root handlers, so no ordinary test
    reaches that state. This one recreates Glue's condition explicitly.
    """
    root = logging.getLogger()
    root.handlers = [logging.NullHandler()]
    root.setLevel(logging.WARNING)  # what Glue leaves behind

    di_logging.configure_logging()

    stream = io.StringIO()
    package_logger = logging.getLogger(di_logging.PACKAGE_LOGGER_NAME)
    package_logger.handlers = [logging.StreamHandler(stream)]

    di_logging.get_logger("data_ingest.pipeline").info("checkpoint committed")

    assert "checkpoint committed" in stream.getvalue()


def test_emits_when_no_logging_is_configured_at_all():
    root = logging.getLogger()
    root.handlers = []
    root.setLevel(logging.WARNING)

    di_logging.configure_logging()

    stream = io.StringIO()
    package_logger = logging.getLogger(di_logging.PACKAGE_LOGGER_NAME)
    package_logger.handlers = [logging.StreamHandler(stream)]

    di_logging.get_logger("data_ingest.state").info("hello")

    assert "hello" in stream.getvalue()


def test_does_not_modify_the_root_logger():
    """A library must not reconfigure its host's root logger."""
    root = logging.getLogger()
    sentinel = logging.NullHandler()
    root.handlers = [sentinel]
    root.setLevel(logging.WARNING)

    di_logging.configure_logging()

    assert root.handlers == [sentinel], "root handlers must be untouched"
    assert root.level == logging.WARNING, "root level must be untouched"


def test_does_not_double_log_through_root():
    root = logging.getLogger()
    root_stream = io.StringIO()
    root.handlers = [logging.StreamHandler(root_stream)]
    root.setLevel(logging.DEBUG)

    di_logging.configure_logging()
    di_logging.get_logger("data_ingest.pipeline").info("only once")

    # propagate=False keeps our records out of the host's handlers.
    assert "only once" not in root_stream.getvalue()


def test_is_idempotent():
    di_logging.configure_logging()
    di_logging.configure_logging()
    di_logging.configure_logging()

    package_logger = logging.getLogger(di_logging.PACKAGE_LOGGER_NAME)
    stdout_handlers = [
        h for h in package_logger.handlers if isinstance(h, logging.StreamHandler)
    ]
    assert len(stdout_handlers) == 1, "repeated calls must not stack handlers"
