from abc import ABC, abstractmethod


class Source(ABC):
    """
    Generic extraction source. Implementations must not write to S3, touch
    DynamoDB, or build manifests -- that's the pipeline/landing writer's job.
    A source only knows how to talk to the thing it's extracting from.
    """

    @abstractmethod
    def get_current_checkpoint(self):
        """Return a Checkpoint representing the current upper extraction bound."""

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
