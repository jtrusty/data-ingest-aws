"""
_manifest.json schema -- the commit marker for a landing run.

Deliberately a plain, flat, versioned schema (version=1) rather than
anything source-specific: `checkpoint` and `source` are opaque dicts filled
in by the pipeline/source adapter, so this schema doesn't need to change
when a future source type (cursor-based, full-load-only, ...) is added.
Bump `version` if the shape ever changes incompatibly, so a Bronze loader
can branch on it.
"""

import json
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class Manifest:
    """
    The commit marker for a landing run. A landing directory without a
    successful manifest must be treated as incomplete/orphaned by any
    downstream reader (see "Core semantics" in README.md --
    cleanup is an S3 Lifecycle concern, not something this framework does).
    """

    version: int
    status: str  # always "SUCCESS" today -- a manifest is only ever written after a fully successful run
    run_id: str
    source_system: str
    source: dict  # e.g. {"database": ..., "schema": ..., "table": ...} -- shape from Source.metadata()
    primary_key: List[str]
    checkpoint: dict  # e.g. {"type": "watermark", "column": ..., "previous": ..., "high": ...}
    load_type: str  # "full" | "incremental"
    started_at: str
    completed_at: str
    row_count: int
    file_count: int
    files: List[str] = field(default_factory=list)
    # [{"name": ..., "type": ...}] for the Parquet files in this run, or None
    # if the run landed zero rows. Lets downstream detect source schema drift
    # without opening the data files.
    schema: Optional[List[dict]] = None
    # True if some batch in this run could not conform to the run's pinned
    # schema and was landed with its own. Downstream must reconcile.
    schema_drift: bool = False

    def to_dict(self):
        return {
            "version": self.version,
            "status": self.status,
            "run_id": self.run_id,
            "source_system": self.source_system,
            "source": self.source,
            "primary_key": self.primary_key,
            "checkpoint": self.checkpoint,
            "load_type": self.load_type,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "row_count": self.row_count,
            "file_count": self.file_count,
            "files": self.files,
            "schema": self.schema,
            "schema_drift": self.schema_drift,
        }

    def to_json(self):
        return json.dumps(self.to_dict(), indent=2, default=str)
