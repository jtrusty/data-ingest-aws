"""
State/checkpoint persistence.

StateStore is the abstraction the pipeline talks to; DynamoDBStateStore is
the only implementation today. It intentionally does NOT know anything
about watermarks specifically -- it stores whatever dict a Checkpoint
subclass produces (see data_ingest.checkpoints), so a future cursor- or
sequence-number-based source reuses this unchanged.

Concurrency: commits are conditioned on a `version` attribute (optimistic
locking), not on the checkpoint value itself, so two concurrent executions
racing to extend the same table's checkpoint can't silently clobber each
other -- the loser gets CheckpointConflictError. See "Core semantics" in README.md.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

from data_ingest.checkpoints import checkpoint_from_dict
from data_ingest.exceptions import CheckpointConflictError
from data_ingest.logging import get_logger

logger = get_logger(__name__)


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class StateKey:
    """
    Identifies one table's checkpoint.

    Built from the config-local names only -- deliberately not
    DATABASE.SCHEMA.TABLE, so a source-side rename doesn't orphan state.
    See "Identity" in README.md.

    `source_key` ("<name>_<type>", e.g. "acme_snowflake") is the DynamoDB
    partition key and also the source segment of the landing path, so the
    two never drift. `table_name` is the sort key. Deriving the partition
    key from name AND type means a second ingestion of the same system over
    a different protocol (`acme_rest`) is automatically distinct, rather than
    relying on someone hand-disambiguating the name.

    source_name and source_type are also persisted individually so the table
    stays readable in the console and queryable by either field.
    """

    source_type: str
    source_name: str
    table_name: str

    @property
    def source_key(self):
        return f"{self.source_name}_{self.source_type}"

    def __str__(self):
        return f"{self.source_key}:{self.table_name}"


@dataclass
class StateRecord:
    """What StateStore.get() returns for a table that has run at least once."""

    checkpoint: object  # Checkpoint or None (no prior state)
    version: int
    last_successful_run_id: Optional[str] = None
    last_row_count: Optional[int] = None
    last_file_count: Optional[int] = None
    last_landing_prefix: Optional[str] = None
    updated_at: Optional[str] = None


class StateStore(ABC):
    """Tracks the last successfully committed checkpoint per source table."""

    @abstractmethod
    def get(self, state_key):
        """Return a StateRecord for a StateKey, or None if no state exists yet."""

    @abstractmethod
    def commit(self, state_key, checkpoint, run_id, row_count, file_count, landing_prefix, expected_version):
        """
        Advance the checkpoint. Must be the LAST step of a successful run.
        Raises CheckpointConflictError if expected_version no longer matches
        (another execution already committed a newer checkpoint).
        """


class DynamoDBStateStore(StateStore):
    """
    DynamoDB-backed state store.

    Table layout:

        partition key   source_key (String)   e.g. "acme_snowflake"
        sort key        table_name (String)   e.g. "fact_order"
        TTL             DISABLED -- a TTL here silently deletes checkpoints
                        and every affected table then does a full reload

    The composite key is deliberate: it makes "every checkpoint for this
    source" a single Query (see list_for_source), which is what staleness
    monitoring and an on-call "did everything run last night?" check need. A
    single opaque partition key would force a full table Scan for the same
    question.

    source_name and source_type are ALSO written as their own attributes.
    They're redundant with source_key by construction, but they keep the
    table readable in the console and let a filter target either field
    without string-splitting the key.

    The checkpoint is stored as a nested map so it isn't hard-coded to a
    single watermark field; future checkpoint types (cursor, full_load, ...)
    fit the same schema unchanged.
    """

    def __init__(self, table):
        # `table` is a boto3 DynamoDB Table resource (dynamodb.Table(name)),
        # injected rather than constructed here so tests can point it at a
        # moto-mocked table.
        self._table = table

    @staticmethod
    def _key(state_key):
        return {"source_key": state_key.source_key, "table_name": state_key.table_name}

    def get(self, state_key):
        # ConsistentRead=True: the pipeline's optimistic-concurrency check
        # depends on reading the true latest version, not an eventually-
        # consistent replica that might be behind.
        response = self._table.get_item(
            Key=self._key(state_key),
            ConsistentRead=True,
        )
        item = response.get("Item")
        if not item:
            logger.info("No prior state for %s", state_key)
            return None

        checkpoint_data = item.get("checkpoint")
        # checkpoint_from_dict() dispatches on the stored "type" field, so
        # this line doesn't need to change as new checkpoint types are added.
        checkpoint = checkpoint_from_dict(dict(checkpoint_data)) if checkpoint_data else None

        return StateRecord(
            checkpoint=checkpoint,
            version=int(item.get("version", 0)),
            last_successful_run_id=item.get("last_successful_run_id"),
            last_row_count=int(item["last_row_count"]) if "last_row_count" in item else None,
            last_file_count=int(item["last_file_count"]) if "last_file_count" in item else None,
            last_landing_prefix=item.get("last_landing_prefix"),
            updated_at=item.get("updated_at"),
        )

    def commit(self, state_key, checkpoint, run_id, row_count, file_count, landing_prefix, expected_version):
        new_version = (expected_version if expected_version is not None else 0) + 1

        item = {
            "source_key": state_key.source_key,   # partition key
            "table_name": state_key.table_name,   # sort key
            # Redundant with source_key by construction, but keeps the table
            # readable and filterable without string-splitting the key.
            "source_name": state_key.source_name,
            "source_type": state_key.source_type,
            "checkpoint": checkpoint.to_dict(),
            "version": Decimal(new_version),
            "last_successful_run_id": run_id,
            "last_row_count": Decimal(row_count),
            "last_file_count": Decimal(file_count),
            "last_landing_prefix": landing_prefix,
            "updated_at": utc_now_iso(),
        }

        try:
            # `is not None`, NOT truthiness: a record that exists but has no
            # `version` attribute reads back as version 0 (see get()), and
            # `if expected_version:` would send it down the
            # attribute_not_exists branch -- which can never succeed against
            # an existing item, wedging that table into a permanent
            # CheckpointConflictError on every run. Hand-seeded and migrated
            # state records hit exactly that case.
            if expected_version is not None:
                # Normal case: we read an existing record at `expected_version`
                # and are advancing it. The condition fails (and raises
                # ConditionalCheckFailedException) if someone else already
                # bumped the version since our read -- i.e. two concurrent
                # runs racing to extend the same table's checkpoint.
                #
                # attribute_not_exists(version) is OR'd in so a pre-existing
                # record that predates versioning can still be adopted once,
                # rather than being permanently un-committable.
                self._table.put_item(
                    Item=item,
                    ConditionExpression=(
                        "version = :expected OR attribute_not_exists(version)"
                        if expected_version == 0
                        else "version = :expected"
                    ),
                    ExpressionAttributeValues={":expected": Decimal(expected_version)},
                )
            else:
                # First-ever commit for this key (get() returned no record at
                # all). Condition on the item not existing, so a concurrent
                # "first run" from another execution can't silently overwrite
                # this one either. Testing the partition-key attribute is how
                # DynamoDB expresses "this item does not exist".
                self._table.put_item(
                    Item=item,
                    ConditionExpression="attribute_not_exists(source_key)",
                )
        except self._table.meta.client.exceptions.ConditionalCheckFailedException as exc:
            raise CheckpointConflictError(
                f"Checkpoint for {state_key} was modified concurrently; refusing to overwrite."
            ) from exc

        logger.info("Committed checkpoint for %s: %s", state_key, checkpoint.to_dict())
        return new_version

    def list_for_source(self, source_key):
        """
        Every checkpoint for one source, as {table_name: StateRecord}.

        `source_key` is the derived "<name>_<type>" identity, e.g.
        "acme_snowflake" -- see StateKey.source_key.

        A single Query against the partition key -- the reason the table uses
        (source_key HASH, table_name RANGE). Use
        for staleness monitoring and "did everything run last night?" checks;
        the equivalent against a single-key table would be a full Scan.

        Not used by the ingestion path itself, which only ever needs one
        table's state.
        """
        from boto3.dynamodb.conditions import Key as _Key

        records = {}
        kwargs = {"KeyConditionExpression": _Key("source_key").eq(source_key)}
        while True:
            response = self._table.query(**kwargs)
            for item in response.get("Items", []):
                checkpoint_data = item.get("checkpoint")
                records[item["table_name"]] = StateRecord(
                    checkpoint=checkpoint_from_dict(dict(checkpoint_data)) if checkpoint_data else None,
                    version=int(item.get("version", 0)),
                    last_successful_run_id=item.get("last_successful_run_id"),
                    last_row_count=int(item["last_row_count"]) if "last_row_count" in item else None,
                    last_file_count=int(item["last_file_count"]) if "last_file_count" in item else None,
                    last_landing_prefix=item.get("last_landing_prefix"),
                    updated_at=item.get("updated_at"),
                )
            # Paginate: a source with many tables can exceed one page.
            if "LastEvaluatedKey" not in response:
                return records
            kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]
