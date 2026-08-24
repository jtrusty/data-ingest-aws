"""
Reusable ingestion framework: source -> immutable S3 landing -> Iceberg Bronze.

Nothing heavy is imported at package import time. `from data_ingest import
run_job` used to pull pipeline -> landing -> pandas + pyarrow, which meant
the Bronze job dragged the whole data stack into memory despite never
touching a DataFrame -- Athena does all of its work. That cost it the ability
to run on Glue's smallest Python Shell size (0.0625 DPU, ~1 GB), a 16x price
difference for a job that is pure orchestration.

The module-level __getattr__ below (PEP 562) keeps `from data_ingest import
run_job` working exactly as before, while deferring the import until the name
is actually used. Import a name, pay for that name.
"""

# x-release-please-version
__version__ = "0.1.2"

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
