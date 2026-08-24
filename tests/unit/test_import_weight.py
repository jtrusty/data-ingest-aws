"""
Import-cost guards.

The Bronze job does no data work -- Athena does all of it -- so it can run on
Glue's smallest Python Shell size (0.0625 DPU, ~1 GB) instead of 1 DPU, a 16x
price difference. That only holds while it avoids loading the data stack.

pandas/pyarrow/numpy are tens of MB of native extensions; importing them in a
job that never touches a DataFrame is pure cost. It happened by accident once
(data_ingest/__init__ imported pipeline -> landing -> pandas), which is why
these are asserted rather than assumed.
"""

import subprocess
import sys
import textwrap

HEAVY = ("pandas", "pyarrow", "numpy")


def _loaded_heavy_modules(source):
    """Import in a FRESH interpreter -- pytest has already imported everything."""
    script = textwrap.dedent(source) + textwrap.dedent(
        """
        import sys, json
        print(json.dumps(sorted(
            m for m in sys.modules if m.split('.')[0] in %r
        )))
        """ % (HEAVY,)
    )
    out = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, check=True
    )
    import json

    return json.loads(out.stdout.strip().splitlines()[-1])


def test_importing_the_package_does_not_load_the_data_stack():
    # `import data_ingest` is what every entry point starts with; it must stay
    # cheap so the lazy attributes below are actually reachable.
    assert _loaded_heavy_modules("import data_ingest") == []


def test_bronze_job_does_not_load_the_data_stack():
    assert _loaded_heavy_modules("import data_ingest.bronze.job") == []


def test_bronze_entry_point_via_the_package_stays_light():
    assert _loaded_heavy_modules("from data_ingest import run_bronze_job") == []


def test_ingestion_job_does_load_the_data_stack():
    """
    The counterpart. Landing genuinely needs pandas and pyarrow -- it writes
    Parquet -- so this asserts the laziness did not accidentally break the
    ingestion path's imports.
    """
    loaded = _loaded_heavy_modules("from data_ingest import run_job")
    assert "pandas" in loaded
    assert "pyarrow" in loaded


def test_lazy_attributes_resolve_to_the_real_callables():
    import data_ingest

    assert callable(data_ingest.run_job)
    assert callable(data_ingest.run_table)
    assert callable(data_ingest.run_bronze_job)
    # dir() must still advertise them despite the lazy resolution.
    for name in ("run_job", "run_table", "run_bronze_job"):
        assert name in dir(data_ingest)


def test_unknown_attribute_still_raises_attribute_error():
    import data_ingest

    try:
        data_ingest.does_not_exist
    except AttributeError as exc:
        assert "does_not_exist" in str(exc)
    else:
        raise AssertionError("expected AttributeError")
