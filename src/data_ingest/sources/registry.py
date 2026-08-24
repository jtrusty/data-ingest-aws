"""
Source-type registry: maps a config file's `source.type` string (e.g.
"snowflake") to the adapter module responsible for it.

Plugging in a new source type -- MySQL, SQL Server, a REST API, a CSV drop
in S3, whatever -- means exactly two things:

  1. Create data_ingest/sources/<type>.py with a Source subclass and a
     module-level `build_source(credentials, table_config, fetch_size) ->
     Source` factory function.
  2. Add one line to _SOURCE_MODULES below.

Nothing in pipeline.py changes. Adapter modules are imported lazily -- only
the module for whichever source.type a given config actually declares gets
imported -- so a Snowflake-only deployment never needs a mysql/api/etc
adapter's dependencies installed, and adding a tenth source type doesn't
grow pipeline.py by a function every time.
"""

import importlib

from data_ingest.exceptions import ConfigurationError

# source.type string -> dotted module path. Each module must expose:
#   build_source(credentials, table_config, fetch_size) -> Source
_SOURCE_MODULES = {
    "snowflake": "data_ingest.sources.snowflake",
}


def register_source_module(source_type, module_path):
    """
    Register (or override) a source type's adapter module path. Exists
    mainly for tests that want to point a type at a fake adapter module
    without touching _SOURCE_MODULES directly.
    """
    _SOURCE_MODULES[source_type] = module_path


def known_source_types():
    return sorted(_SOURCE_MODULES)


def build_source(source_type, credentials, table_config, fetch_size):
    """Look up, import, and invoke the registered factory for `source_type`."""
    module_path = _SOURCE_MODULES.get(source_type)
    if module_path is None:
        raise ConfigurationError(
            f"No source adapter registered for type '{source_type}'. "
            f"Known types: {', '.join(known_source_types()) or '(none)'}"
        )

    module = importlib.import_module(module_path)
    try:
        factory = module.build_source
    except AttributeError as exc:
        raise ConfigurationError(
            f"Source adapter module '{module_path}' must define a "
            f"build_source(credentials, table_config, fetch_size) function"
        ) from exc

    return factory(credentials, table_config, fetch_size)
