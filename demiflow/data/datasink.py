"""Backend-neutral datasink contract."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Optional
import json


@dataclass(frozen=True)
class WriteContext:
    task_index: int = 0
    run_id: str | None = None
    action_id: str | None = None
    commit_id: str | None = None
    attempt: int | None = None
    fence: str | None = None


@dataclass(frozen=True)
class WriteResult:
    written_rows: Optional[int]
    failed_rows: Optional[int] = 0
    blocks_written: Optional[int] = 0
    target: str = ""
    metadata: Mapping[str, Any] = field(default_factory=dict)


def validate_write_result(result: WriteResult) -> WriteResult:
    counts = (result.written_rows, result.failed_rows, result.blocks_written)
    if any(
        value is not None
        and (isinstance(value, bool) or not isinstance(value, int) or value < 0)
        for value in counts
    ):
        raise ValueError("WriteResult counts must be non-negative integers or None")
    if not isinstance(result.target, str):
        raise TypeError("WriteResult target must be a string")
    if not isinstance(result.metadata, Mapping):
        raise TypeError("WriteResult metadata must be a mapping")
    try:
        json.dumps(dict(result.metadata), allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise TypeError("WriteResult metadata must contain strict JSON values") from exc
    return result


class Datasink(ABC):
    def recovery_identity(self):
        """Immutable destination/config declaration, without live credentials."""
        raise NotImplementedError('Durable sinks must declare native recovery identity')

    def prepare(self, blocks: Iterable[Any], context: WriteContext) -> Mapping[str, Any]:
        """Stage bounded data without publishing it; return durable references.

        Recovery-capable adapters implement all four methods below. The driver
        provides identity/ownership. A legacy write callback is never taken as
        proof that it can inspect or safely repeat a publication.
        """
        raise NotImplementedError("Datasink has no durable prepare contract")

    def commit(self, prepared: Mapping[str, Any], context: WriteContext) -> Mapping[str, Any]:
        """Publish prepared data idempotently for context.commit_id."""
        raise NotImplementedError("Datasink has no durable commit contract")

    def inspect(self, prepared: Mapping[str, Any], context: WriteContext) -> Mapping[str, Any] | None:
        """Return a verified receipt, or None only when definitely uncommitted.

        Unknown outcomes raise UnresolvedCommit. A transport error, missing
        permission, or incomplete history is never proof of non-publication.
        """
        raise NotImplementedError("Datasink has no durable inspect contract")

    def abort(self, prepared: Mapping[str, Any], context: WriteContext) -> None:
        """Remove owned unpublished staging only; never undo committed data."""
        raise NotImplementedError("Datasink has no durable abort contract")

    def on_write_start(self, schema: Any = None) -> None:
        """Receive the input schema once before worker write calls begin."""
        pass

    def write(self, blocks: Iterable[Any], context: WriteContext) -> Any:
        """Write one iterable of backend-native blocks and return a backend-supported task result."""
        raise NotImplementedError

    def on_write_complete(self, result: WriteResult) -> None:
        """Receive the validated aggregate WriteResult after all write tasks commit."""
        pass

    def on_write_failed(self, error: Exception) -> None:
        """Receive the original write exception when the managed write fails."""
        pass
