from abc import ABC, abstractmethod


class Source(ABC):
    """
    Generic extraction source. Implementations must not write landing data,
    update DynamoDB, or build manifests -- that's the pipeline/landing writer's job.
    An S3 source may read its own upstream bucket.
    A source only knows how to talk to the thing it's extracting from.
    """

    @abstractmethod
    def get_current_checkpoint(self, previous_checkpoint=None):
        """
        Return a Checkpoint representing the current upper extraction bound.

        `previous_checkpoint` is the last committed one (None on a first run).
        Most sources ignore it -- the bound is whatever the source holds now.
        A source that must bound how much ONE run may cover uses it to cap
        the bound relative to where the last run stopped, since the pipeline
        commits this value verbatim after the manifest: a source that fetched
        less than it declared would silently skip the difference.
        """

    def is_caught_up(self, committed_checkpoint):
        """
        After a run commits `committed_checkpoint`, is there nothing more the
        source could have offered right now? True for every source whose
        bound is simply "now"; a source that caps its window answers False
        while the cap is what limited the last run, and the pipeline then
        runs the table again immediately rather than waiting for the next
        scheduled execution.
        """
        return True

    @abstractmethod
    def extract(self, previous_checkpoint, current_checkpoint):
        """
        Yield pandas DataFrames of records between previous_checkpoint
        (exclusive, or None for a full load) and current_checkpoint
        (inclusive). Must not load the full result set into memory at once.
        """

    @abstractmethod
    def metadata(self):
        """Return a dict describing the source object (database/schema/table etc)."""

    def arrow_schema(self):
        """
        Declared Arrow schema for the extraction, or None if the adapter
        cannot supply one.

        Optional. When present, the landing writer pins Parquet types from it
        instead of inferring them from the first batch -- which matters for
        types whose width is not visible in a sample, notably decimals: a
        NUMBER(38,3) column whose first batch holds only small values infers
        as decimal128(4,3), and the first larger value then cannot conform.
        """
        return None

    def close(self):
        """Release any held resources. Default no-op."""
