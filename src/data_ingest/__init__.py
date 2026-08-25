"""
Reusable ingestion framework: source -> immutable S3 landing -> Iceberg Bronze.

Nothing heavy is imported at package import time. A direct
`from data_ingest import run_job` would pull pipeline -> landing -> pandas +
pyarrow, dragging the whole data stack into the Bronze job's memory even
though it never touches a DataFrame -- Athena does all of its work. That
would cost Bronze the smallest Python Shell size (0.0625 DPU, ~1 GB), a 16x
price difference for a job that is pure orchestration.

The module-level __getattr__ below (PEP 562) keeps the import spelling
unchanged while deferring the work until the name is actually used. Import a
name, pay for that name.
"""

# x-release-please-version
__version__ = "1.2.0"

__all__ = ["run_job", "run_table", "run_bronze_job"]

# Public name -> the module that defines it. Kept explicit rather than
# scanning, so a typo here surfaces as an ImportError naming the attribute
# instead of silently shadowing something.
_LAZY_ATTRIBUTES = {
    "run_job": "data_ingest.pipeline",
    "run_table": "data_ingest.pipeline",
    "run_bronze_job": "data_ingest.bronze.job",
}


def __getattr__(name):
    """Resolve a public name on first use (PEP 562)."""
    module_path = _LAZY_ATTRIBUTES.get(name)
    if module_path is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    import importlib

    return getattr(importlib.import_module(module_path), name)


def __dir__():
    """Keep tab-completion and dir() honest despite the lazy attributes."""
    return sorted(set(globals()) | set(_LAZY_ATTRIBUTES))
