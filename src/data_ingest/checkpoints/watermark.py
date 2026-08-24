from dataclasses import dataclass
from typing import Optional

from data_ingest.checkpoints.base import Checkpoint


@dataclass(frozen=True)
class WatermarkCheckpoint(Checkpoint):
    """
    A single-column monotonic watermark checkpoint (e.g. UPDATED_AT).

    `value=None` means "no prior checkpoint" -> triggers a full load.

    `value` is always the LOSSLESS text form of the source value, not
    `str(python_object)`. That distinction matters: the Snowflake connector
    materializes TIMESTAMP(9) columns as Python `datetime`, which only holds
    microseconds, so `str()` silently truncates nanoseconds -- and a
    truncated ceiling excludes the very row it was derived from, producing a
    permanent, silent gap. Source adapters are responsible for producing a
    full-fidelity string and for binding it back with an explicit cast.

    `value_type` records the source-side type the value was captured from
    (e.g. "TIMESTAMP_NTZ", "FIXED") so the adapter can choose the matching
    cast when binding it into the next query. None means "unknown/legacy"
    -- state records written before this field existed -- in which case
    adapters fall back to an implicit cast.

    `lookback_minutes` widens the effective lower bound on the next
    extraction; it deliberately produces overlap/duplicates which downstream
    Bronze must dedupe.
    """

    checkpoint_type = "watermark"

    column: str
    value: Optional[str] = None
    lookback_minutes: int = 0
    value_type: Optional[str] = None

    def to_dict(self):
        return {
            "type": self.checkpoint_type,
            "column": self.column,
            "value": self.value,
            "value_type": self.value_type,
            # NOTE: lookback_minutes is config, not state -- it's persisted
            # only so a human reading the state record can see what the run
            # used. The value actually applied at query time always comes
            # from the table's YAML config via the source adapter, so
            # editing the YAML takes effect immediately and this stored copy
            # is never read back into behavior.
            "lookback_minutes": self.lookback_minutes,
        }

    @classmethod
    def from_dict(cls, data):
        lookback = data.get("lookback_minutes", 0)
        return cls(
            column=data["column"],
            value=data.get("value"),
            # int(): DynamoDB hands numbers back as Decimal, and this value
            # flows into SQL/int arithmetic downstream.
            lookback_minutes=int(lookback) if lookback is not None else 0,
            value_type=data.get("value_type"),
        )
