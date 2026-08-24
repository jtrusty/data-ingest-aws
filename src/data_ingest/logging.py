"""
Logging helpers.

Two constraints pull against each other here:

1. A library must not clobber the root logger. Calling basicConfig() at
   import time would stomp on handlers and formatting the host process
   already set up.
2. The output has to actually appear. Under AWS Glue, the runtime configures
   root logging BEFORE the job script runs, and leaves the root level above
   INFO -- so a library that politely defers to the host emits nothing at
   all, which is exactly what happened here: every framework log line was
   silently filtered, and the first real failure had to be diagnosed with no
   logs whatsoever.

Deferring to the host is only safe when the host is guaranteed to route our
records somewhere visible, and Glue isn't. So instead of touching root, we
configure OUR OWN package logger: level, a stdout handler, and
propagate=False. That is self-contained, cannot double-log through root, and
works identically whether or not the host configured logging.

Glue captures the job process's stdout into CloudWatch, so a StreamHandler
on sys.stdout is what reaches the log group.
"""

import logging
import sys

PACKAGE_LOGGER_NAME = "data_ingest"
LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"

_CONFIGURED = False


def configure_logging(level=logging.INFO):
    """
    Attach a stdout handler to the `data_ingest` logger.

    Idempotent. Deliberately does NOT call basicConfig() or modify the root
    logger -- see the module docstring for why deferring to the host's root
    configuration silently produced zero output under Glue.
    """
    global _CONFIGURED
    if _CONFIGURED:
        return

    package_logger = logging.getLogger(PACKAGE_LOGGER_NAME)
    package_logger.setLevel(level)

    already_attached = any(
        isinstance(h, logging.StreamHandler) and getattr(h, "stream", None) is sys.stdout
        for h in package_logger.handlers
    )
    if not already_attached:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)
        handler.setFormatter(logging.Formatter(LOG_FORMAT))
        package_logger.addHandler(handler)

    # Our handler is the whole story for this package; propagating as well
    # would duplicate every line through whatever the host attached to root.
    package_logger.propagate = False

    _CONFIGURED = True


def get_logger(name):
    """
    Module-level logger. Does not configure logging -- see module docstring.

    Callers use __name__, which lands under the `data_ingest.*` hierarchy and
    therefore inherits the level and handler configure_logging() installs.
    """
    return logging.getLogger(name)
