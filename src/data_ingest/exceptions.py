class DataIngestError(Exception):
    """Base exception for all framework errors."""


class ConfigurationError(DataIngestError):
    """Raised for invalid or missing configuration."""


class SourceConnectionError(DataIngestError):
    """Raised when a source connection cannot be established."""


class ExtractionError(DataIngestError):
    """Raised when extracting data from a source fails."""


class LandingWriteError(DataIngestError):
    """Raised when writing landing output to S3 fails."""


class ManifestCommitError(DataIngestError):
    """Raised when writing the run manifest fails."""


class CheckpointConflictError(DataIngestError):
    """Raised when a checkpoint commit loses an optimistic-concurrency race."""
