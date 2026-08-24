"""
Snowflake source adapter: connects, quotes identifiers, determines the high
watermark, builds full/incremental SQL, and streams batches out.

This module owns everything Snowflake-specific and nothing else (see
"Snowflake source adapter" in README.md) -- it never touches S3, DynamoDB,
or manifests. That split is what run_table() in pipeline.py relies on to
apply the same transactional guarantees to any future source type.

Watermark fidelity is handled carefully here; see WATERMARK_CODECS below.
"""

import pandas as pd
import snowflake.connector

from data_ingest.checkpoints.watermark import WatermarkCheckpoint
from data_ingest.exceptions import ConfigurationError, ExtractionError, SourceConnectionError
from data_ingest.logging import get_logger
from data_ingest.sources.base import Source

logger = get_logger(__name__)

DEFAULT_FETCH_SIZE = 50_000

# Snowflake numeric type codes (cursor.description[i].type_code), from
# snowflake.connector.constants.FIELD_ID_TO_NAME.
_TYPE_FIXED = 0
_TYPE_REAL = 1
_TYPE_TEXT = 2
_TYPE_DATE = 3
_TYPE_TIMESTAMP = 4
_TYPE_TIMESTAMP_LTZ = 6
_TYPE_TIMESTAMP_TZ = 7
_TYPE_TIMESTAMP_NTZ = 8
_TYPE_TIME = 12


class _Codec:
    """
    How to read a watermark column out of Snowflake losslessly, and how to
    bind that text back in as a typed bound parameter.

    `select_expr` wraps MAX(col) so the value comes back as *text at full
    source precision*, and `bind_expr` casts the stored text back to the
    original type in the extraction query.

    This exists because the obvious implementation is subtly wrong: the
    connector materializes TIMESTAMP(9) as Python `datetime`, which holds
    only microseconds. Doing str(MAX(col)) therefore truncates the last
    three digits of a nanosecond timestamp, and a truncated `<=` ceiling
    excludes the very row it came from. On the next run MAX() truncates
    identically, so the checkpoint never advances past it -- the row is
    never ingested, in any run, with no error and no log line. Round-tripping
    as TO_VARCHAR(..., 'FF9') text avoids the lossy Python type entirely.
    """

    __slots__ = ("name", "select_expr", "bind_expr", "supports_lookback")

    def __init__(self, name, select_expr, bind_expr, supports_lookback):
        self.name = name
        self.select_expr = select_expr  # format string taking the quoted column
        self.bind_expr = bind_expr  # SQL fragment casting a %s placeholder
        self.supports_lookback = supports_lookback


_TS_FORMAT_NTZ = "YYYY-MM-DD HH24:MI:SS.FF9"
_TS_FORMAT_TZ = "YYYY-MM-DD HH24:MI:SS.FF9 TZHTZM"

WATERMARK_CODECS = {
    _TYPE_TIMESTAMP_NTZ: _Codec(
        "TIMESTAMP_NTZ",
        "TO_VARCHAR(MAX({col}), '%s')" % _TS_FORMAT_NTZ,
        "TO_TIMESTAMP_NTZ(%%s, '%s')" % _TS_FORMAT_NTZ,
        supports_lookback=True,
    ),
    # Snowflake's bare TIMESTAMP is an alias controlled by
    # TIMESTAMP_TYPE_MAPPING; it defaults to NTZ and we pin that session
    # parameter at connect time, so treat it as NTZ.
    _TYPE_TIMESTAMP: _Codec(
        "TIMESTAMP",
        "TO_VARCHAR(MAX({col}), '%s')" % _TS_FORMAT_NTZ,
        "TO_TIMESTAMP_NTZ(%%s, '%s')" % _TS_FORMAT_NTZ,
        supports_lookback=True,
    ),
    _TYPE_TIMESTAMP_LTZ: _Codec(
        "TIMESTAMP_LTZ",
        "TO_VARCHAR(MAX({col}), '%s')" % _TS_FORMAT_TZ,
        "TO_TIMESTAMP_LTZ(%%s, '%s')" % _TS_FORMAT_TZ,
        supports_lookback=True,
    ),
    _TYPE_TIMESTAMP_TZ: _Codec(
        "TIMESTAMP_TZ",
        "TO_VARCHAR(MAX({col}), '%s')" % _TS_FORMAT_TZ,
        "TO_TIMESTAMP_TZ(%%s, '%s')" % _TS_FORMAT_TZ,
        supports_lookback=True,
    ),
    _TYPE_DATE: _Codec(
        "DATE",
        "TO_VARCHAR(MAX({col}), 'YYYY-MM-DD')",
        "TO_DATE(%s, 'YYYY-MM-DD')",
        supports_lookback=False,  # DATEADD(minute, ...) on a DATE is meaningless
    ),
    _TYPE_TIME: _Codec(
        "TIME",
        "TO_VARCHAR(MAX({col}), 'HH24:MI:SS.FF9')",
        "TO_TIME(%s, 'HH24:MI:SS.FF9')",
        supports_lookback=False,
    ),
    _TYPE_FIXED: _Codec(
        "FIXED",
        # TO_VARCHAR on a NUMBER renders full precision with no exponent
        # form, so a sequence/ID watermark round-trips exactly.
        "TO_VARCHAR(MAX({col}))",
        "TO_NUMBER(%s)",
        supports_lookback=False,  # minutes are meaningless on a numeric sequence
    ),
    _TYPE_TEXT: _Codec(
        "TEXT",
        "MAX({col})",
        "%s",
        supports_lookback=False,
    ),
}


def quote_identifier(value):
    """
    Quote a Snowflake identifier. Values come from our own configuration,
    not end users, but quoting keeps schema/table names safe and supports
    mixed-case identifiers.
    """
    return '"' + value.replace('"', '""') + '"'


class SnowflakeSource(Source):
    """
    Snowflake -> Landing extraction adapter.

    Deliberately uses cursor.fetchmany() batching instead of
    fetch_pandas_batches(): the pandas/Arrow fetch path has caused
    urllib3/OpenSSL incompatibilities under the Glue Python Shell runtime
    with snowflake-connector-python==3.0.4. Do not switch back without
    proving compatibility in that runtime first see "Known Glue constraints" in README.md.
    """

    def __init__(
        self,
        credentials,
        database,
        schema,
        table,
        watermark_column,
        lookback_minutes=0,
        fetch_size=DEFAULT_FETCH_SIZE,
    ):
        self.database = database
        self.schema = schema
        self.table = table
        self.watermark_column = watermark_column
        self.lookback_minutes = lookback_minutes
        self.fetch_size = fetch_size

        self._codec = None  # resolved lazily on first watermark read

        try:
            connection_args = {
                "account": credentials["account"],
                "user": credentials["username"],
                "password": credentials["password"],
                "warehouse": credentials["warehouse"],
                # Pin the session's temporal semantics rather than inheriting
                # whatever the account/user default happens to be. Without
                # this, the same stored watermark string can mean a different
                # instant after an account default changes or the job runs
                # under a different role -- rows then get silently skipped or
                # re-read with no error.
                "session_parameters": {
                    "TIMEZONE": "UTC",
                    "TIMESTAMP_TYPE_MAPPING": "TIMESTAMP_NTZ",
                },
            }
            # `role` is optional in the Secrets Manager secret -- Snowflake
            # falls back to the user's default role if omitted.
            if credentials.get("role"):
                connection_args["role"] = credentials["role"]

            self._connection = snowflake.connector.connect(**connection_args)
        except Exception as exc:
            raise SourceConnectionError(f"Failed to connect to Snowflake: {exc}") from exc

    @property
    def object_name(self):
        """Fully-qualified, quoted "DATABASE"."SCHEMA"."TABLE" for use in SQL."""
        return ".".join(
            [
                quote_identifier(self.database),
                quote_identifier(self.schema),
                quote_identifier(self.table),
            ]
        )

    def metadata(self):
        return {
            "database": self.database,
            "schema": self.schema,
            "table": self.table,
        }

    def _resolve_codec(self):
        """
        Determine the watermark column's Snowflake type once per source, so
        the value can be read and bound losslessly. Uses a zero-row probe
        (`WHERE 1 = 0`) -- cursor.description is populated from result
        metadata, so this costs no table scan.
        """
        if self._codec is not None:
            return self._codec

        column = quote_identifier(self.watermark_column)
        probe = f"SELECT {column} FROM {self.object_name} WHERE 1 = 0"

        cursor = self._connection.cursor()
        try:
            cursor.execute(probe)
            type_code = cursor.description[0][1]
        except Exception as exc:
            raise ExtractionError(
                f"Failed to determine the type of watermark column "
                f"{self.watermark_column!r} on {self.object_name}: {exc}"
            ) from exc
        finally:
            cursor.close()

        codec = WATERMARK_CODECS.get(type_code)
        if codec is None:
            raise ConfigurationError(
                f"Watermark column {self.watermark_column!r} on {self.object_name} "
                f"has an unsupported Snowflake type (type_code={type_code}). "
                f"Supported: {', '.join(sorted(c.name for c in WATERMARK_CODECS.values()))}."
            )

        if self.lookback_minutes and not codec.supports_lookback:
            raise ConfigurationError(
                f"lookback_minutes={self.lookback_minutes} is configured for "
                f"{self.object_name}.{self.watermark_column}, but that column is "
                f"{codec.name} -- a minute-based lookback only applies to "
                f"timestamp watermarks. Set lookback_minutes: 0."
            )

        logger.info(
            "Watermark column %s.%s resolved as %s",
            self.object_name,
            self.watermark_column,
            codec.name,
        )
        self._codec = codec
        return codec

    def get_current_checkpoint(self):
        """
        Capture MAX(watermark_column) as the extraction's upper bound
        BEFORE any extraction happens. Doing this up front (rather than
        just extracting "everything newer than last time" with no ceiling)
        is what keeps a run's window well-defined even if new rows land in
        Snowflake mid-extraction -- those just wait for the next run.

        The value is read as TEXT at full source precision (see
        WATERMARK_CODECS) rather than as a Python object, because the
        connector's Python types are lossy for TIMESTAMP(9).
        """
        codec = self._resolve_codec()
        select_expr = codec.select_expr.format(col=quote_identifier(self.watermark_column))
        query = f"SELECT {select_expr} FROM {self.object_name}"

        logger.info("Determining high watermark for %s", self.object_name)

        cursor = self._connection.cursor()
        try:
            cursor.execute(query)
            value = cursor.fetchone()[0]
        except Exception as exc:
            raise ExtractionError(f"Failed to determine high watermark: {exc}") from exc
        finally:
            cursor.close()

        return WatermarkCheckpoint(
            column=self.watermark_column,
            # value=None here means "the table is empty" (MAX of zero rows
            # is NULL) -- the pipeline treats that as SKIPPED, not an error.
            # Everything else is already text straight from Snowflake; no
            # str() coercion of a lossy Python object happens anywhere.
            value=None if value is None else value,
            lookback_minutes=self.lookback_minutes,
            value_type=codec.name,
        )

    def _build_query(self, previous_checkpoint, current_checkpoint):
        """
        Build the SELECT for this run. Three shapes, chosen by
        previous_checkpoint/lookback_minutes:

        1. No previous checkpoint -> full load: `<= high`.
        2. Previous checkpoint, no lookback -> incremental: `> low AND <= high`.
        3. Previous checkpoint, lookback configured -> incremental with a
           widened lower bound: `> DATEADD(minute, -lookback, low) AND <= high`.
           This deliberately re-extracts a window of already-seen rows
           (see lookback_minutes in "Configuring a source" in README.md) -- Landing keeps the
           duplicates, Bronze is responsible for deduping them.

        All three use bind parameters (%s / pyformat), not string
        interpolation, for the watermark values themselves -- only
        identifiers (already quoted via quote_identifier) and the SQL
        structure are built with f-strings. Each bound watermark is wrapped
        in an explicit cast (codec.bind_expr) matching the column's real
        type, rather than relying on Snowflake's implicit VARCHAR coercion
        and whatever TIMESTAMP_INPUT_FORMAT the session happens to have.
        """
        column = quote_identifier(self.watermark_column)
        bind = self._resolve_codec().bind_expr
        high = current_checkpoint.value

        if previous_checkpoint is None or previous_checkpoint.value is None:
            # No ORDER BY on the full-load path: the window ceiling is what
            # makes the run well-defined, ordering contributes nothing to
            # correctness, and sorting an entire table forces Snowflake to
            # materialize the whole result before returning the first row.
            query = f"""
                SELECT *
                FROM {self.object_name}
                WHERE {column} <= {bind}
            """
            params = (high,)
            load_type = "full"
            return query, params, load_type

        low = previous_checkpoint.value
        if self.lookback_minutes:
            # Widen the lower bound backwards in time. Only reachable for
            # timestamp codecs -- _resolve_codec() rejects lookback on
            # non-temporal watermarks at startup.
            query = f"""
                SELECT *
                FROM {self.object_name}
                WHERE {column} > DATEADD(minute, -%s, {bind})
                  AND {column} <= {bind}
                ORDER BY {column}
            """
            params = (self.lookback_minutes, low, high)
        else:
            query = f"""
                SELECT *
                FROM {self.object_name}
                WHERE {column} > {bind}
                  AND {column} <= {bind}
                ORDER BY {column}
            """
            params = (low, high)

        load_type = "incremental"
        return query, params, load_type

    def extract(self, previous_checkpoint, current_checkpoint):
        """
        Stream the extraction window as a sequence of DataFrames, one per
        fetchmany() batch. A generator, not a list -- callers (pipeline.py)
        write each batch to S3 as it arrives, so the full result set is
        never held in memory at once (never load a whole table into memory: don't
        load an entire large Snowflake table into memory).
        """
        if current_checkpoint.value is None:
            # Empty source table -- nothing to query at all.
            logger.info("%s has no watermark values; nothing to extract.", self.object_name)
            return

        query, params, load_type = self._build_query(previous_checkpoint, current_checkpoint)

        logger.info(
            "%s load for %s: watermark window %s -> %s",
            load_type.upper(),
            self.object_name,
            previous_checkpoint.value if previous_checkpoint else None,
            current_checkpoint.value,
        )

        cursor = self._connection.cursor()
        try:
            cursor.execute(query, params)
            # cursor.description gives column names in query order; used
            # to build each batch's DataFrame with the correct headers
            # since fetchmany() returns bare row tuples.
            columns = [col[0] for col in cursor.description]

            batch_number = 0
            while True:
                # fetchmany(), not fetch_pandas_batches() -- see class
                # docstring. This is the DB-API standard batching call;
                # each call pulls up to `fetch_size` rows already buffered
                # server-side by Snowflake, not re-querying.
                rows = cursor.fetchmany(self.fetch_size)
                if not rows:
                    break

                dataframe = pd.DataFrame(rows, columns=columns)
                logger.info(
                    "Fetched batch %s (%s rows) from %s",
                    batch_number,
                    len(dataframe),
                    self.object_name,
                )
                yield dataframe
                batch_number += 1
        except Exception as exc:
            raise ExtractionError(f"Extraction failed for {self.object_name}: {exc}") from exc
        finally:
            cursor.close()

    def close(self):
        try:
            self._connection.close()
        except Exception:
            # Don't let a failure to close mask the run's actual outcome --
            # log and move on rather than raising during cleanup.
            logger.warning("Error closing Snowflake connection", exc_info=True)


def build_source(credentials, table_config, fetch_size):
    """
    The factory data_ingest.sources.registry looks up for source.type ==
    "snowflake". Every source adapter module defines one of these with this
    exact name/signature -- see registry.py for the plug-in convention.
    """
    return SnowflakeSource(
        credentials=credentials,
        database=table_config.database,
        schema=table_config.schema,
        table=table_config.table,
        watermark_column=table_config.checkpoint.column,
        lookback_minutes=table_config.checkpoint.lookback_minutes,
        fetch_size=fetch_size,
    )
