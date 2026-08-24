from abc import ABC, abstractmethod


class Checkpoint(ABC):
    """
    A generic extraction checkpoint.

    Not every source uses a timestamp watermark: future sources may use an
    API cursor, a pagination token, a sequence number, or no checkpoint at
    all (full-load-only). The pipeline and state store only ever deal with
    the dict form, so new checkpoint types can be added without touching
    either.
    """

    checkpoint_type = None

    @abstractmethod
    def to_dict(self):
        ...

    @classmethod
    @abstractmethod
    def from_dict(cls, data):
        ...
