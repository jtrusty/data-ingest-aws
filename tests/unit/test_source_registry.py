import sys
import types

import pytest

from data_ingest.exceptions import ConfigurationError
from data_ingest.sources import registry


@pytest.fixture(autouse=True)
def restore_registry():
    """Don't let a test's fake registrations leak into other tests."""
    original = dict(registry._SOURCE_MODULES)
    yield
    registry._SOURCE_MODULES.clear()
    registry._SOURCE_MODULES.update(original)


def _install_fake_module(module_name, build_source_fn=None):
    module = types.ModuleType(module_name)
    if build_source_fn is not None:
        module.build_source = build_source_fn
    sys.modules[module_name] = module
    return module


def test_snowflake_is_registered_by_default():
    assert "snowflake" in registry.known_source_types()


def test_unknown_source_type_raises_configuration_error():
    with pytest.raises(ConfigurationError, match="No source adapter registered"):
        registry.build_source("carrier_pigeon", credentials={}, table_config=None, fetch_size=10)


def test_new_source_type_plugs_in_via_one_registry_line():
    # Proves the plug-in contract: a brand-new source type needs nothing
    # more than a module exposing build_source(...) plus one registration
    # call -- pipeline.py is not involved at all.
    calls = []

    def fake_build_source(credentials, table_config, fetch_size):
        calls.append((credentials, table_config, fetch_size))
        return "a-fake-source-instance"

    _install_fake_module("tests.fake_mysql_adapter", fake_build_source)
    registry.register_source_module("mysql", "tests.fake_mysql_adapter")

    assert "mysql" in registry.known_source_types()

    result = registry.build_source("mysql", credentials={"user": "x"}, table_config="a-table-config", fetch_size=100)

    assert result == "a-fake-source-instance"
    assert calls == [({"user": "x"}, "a-table-config", 100)]


def test_adapter_module_missing_build_source_raises_configuration_error():
    _install_fake_module("tests.fake_broken_adapter")  # no build_source attribute
    registry.register_source_module("broken", "tests.fake_broken_adapter")

    with pytest.raises(ConfigurationError, match="must define a build_source"):
        registry.build_source("broken", credentials={}, table_config=None, fetch_size=10)


def test_fetch_size_defaults_do_not_drift():
    """
    pipeline.DEFAULT_FETCH_SIZE is the authority (run_job resolves
    CLI > config > it), while the adapter's own default applies when it is
    constructed directly. They are separate constants in separate modules,
    so nothing but a test stops them drifting apart -- and a mismatch would
    mean the memory ceiling proven in one path silently differs in the other.
    """
    from data_ingest import pipeline
    from data_ingest.sources import snowflake

    assert pipeline.DEFAULT_FETCH_SIZE == snowflake.DEFAULT_FETCH_SIZE


def test_prefetch_threads_are_capped_below_the_library_default():
    """
    The connector buffers result chunks ahead of the cursor; its default (4)
    assumes a machine with headroom. Glue Python Shell is 1 DPU / 16 GB and
    cannot be scaled, so we cap it. Guards against someone raising it back
    without knowing why it was lowered.
    """
    from data_ingest.sources import snowflake

    assert 1 <= snowflake.DEFAULT_PREFETCH_THREADS <= 2
