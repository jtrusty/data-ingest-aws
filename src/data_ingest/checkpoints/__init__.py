from data_ingest.checkpoints.base import Checkpoint
from data_ingest.checkpoints.watermark import WatermarkCheckpoint

# Maps the "type" field stored in DynamoDB/the manifest back to a concrete
# Checkpoint subclass. Adding a new checkpoint type (cursor, sequence
# number, full_load) means implementing Checkpoint and registering it here
# -- state.py's DynamoDBStateStore never needs to change.
_TYPE_REGISTRY = {
    WatermarkCheckpoint.checkpoint_type: WatermarkCheckpoint,
}


def checkpoint_from_dict(data):
    """Reconstruct the right Checkpoint subclass from its persisted dict form."""
    if data is None:
        return None
    checkpoint_type = data.get("type")
    checkpoint_cls = _TYPE_REGISTRY.get(checkpoint_type)
    if checkpoint_cls is None:
        raise ValueError(f"Unknown checkpoint type: {checkpoint_type}")
    return checkpoint_cls.from_dict(data)


__all__ = ["Checkpoint", "WatermarkCheckpoint", "checkpoint_from_dict"]
