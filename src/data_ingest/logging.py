"""
Logging helpers.

Deliberately does NOT configure logging at import time. Modules call
get_logger() at import to create their module logger, and a library that
called logging.basicConfig() there would mutate the root logger of whatever
process imported it -- clobbering handlers/formatting that the host app (or
a test harness) had set up. Configuration is an application decision, so
run_job() calls configure_logging() explicitly at job startup.
"""

import logging
import sys

_CONFIGURED = False


def configure_logging(level=logging.INFO):
    """
    Install a stdout handler and formatter. Idempotent, and a no-op if the
    root logger already has handlers (i.e. the host application configured
    logging itself and we should not second-guess it).
    """
    global _CONFIGURED
    if _CONFIGURED:
        return
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=level,
            format="%(asctime)s %(levelname)s %(name)s %(message)s",
            stream=sys.stdout,
        )
    _CONFIGURED = True


def get_logger(name):
    """Module-level logger. Does not configure logging -- see module docstring."""
    return logging.getLogger(name)
