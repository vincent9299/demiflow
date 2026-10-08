"""Dataset graph contracts and opt-in parallel Python task execution.

The Dataset graph is independent of worker transport. Arrow source batches cross
the task boundary intact; Python row callbacks are lowered inside workers. Keyed
operators are explicit graph nodes, never closures that launch nested executors.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

from ..data.sources import SourcePlan


@dataclass(frozen=True)
class LocalKernelOptions:
    workers: int = 4
    worker_mode: str = "process"
    partitions: int = 16
    batch_rows: int = 8192
    memory_bytes: int = 256 * 1024 * 1024
    temp_directory: str | None = None

    def __post_init__(self):
        for name in ("workers", "partitions", "batch_rows", "memory_bytes"):
            value = getattr(self, name)
            if type(value) is not int or value < 1:
                raise ValueError(f"local_execution {name} must be a positive integer")
        if self.worker_mode not in {"thread", "process"}:
            raise ValueError("worker_mode must be thread or process")
        if self.partitions > 256:
            raise ValueError("local_execution supports at most 256 shuffle partitions")
        if self.memory_bytes < self.workers * 64 * 1024:
            raise ValueError("memory_bytes must provide at least 64 KiB per worker")


@dataclass(frozen=True)
class PlanInput:
    source: SourcePlan
    plan: Any
    executor: Any = None


@dataclass(frozen=True)
class UnionSource(SourcePlan):
    """Concatenation within one local task engine; never nested Dataset actions."""
    inputs: tuple[PlanInput, ...]


@dataclass(frozen=True)
class KeyedSource(SourcePlan):
    kind: str
    left: PlanInput
    on: tuple[str, ...]
    right: PlanInput | None = None
    right_on: tuple[str, ...] = ()
    how: str = "inner"
    suffix: str = "_right"
    reducer: Any = None
    initial: Any = None
    chunk_bytes: int = 32 * 1024 * 1024
    max_rows: int = 32
    output: str = "items"


def keyed_dataset(left, kind, on, *, right=None, right_on=None, how="inner",
                  suffix="_right", reducer=None, initial=None,
                  chunk_bytes=32 * 1024 * 1024, max_rows=32, output="items"):
    from ..data.dataset import Dataset
    from ..data.plan import LogicalPlan, reject_streaming_map_options
    from ..data.local_relational import keys
    reject_streaming_map_options(left._plan, 'local relational')
    if right is not None:
        reject_streaming_map_options(right._plan, 'local relational')
    fields = tuple(keys(on))
    rfields = tuple(keys(right_on or on))
    if not fields or any(not isinstance(k, str) or not k for k in fields):
        raise ValueError("nonempty key field names are required")
    if chunk_bytes < 1:
        raise ValueError("chunk_bytes must be positive")
    if right is not None:
        if getattr(right._executor, 'NAME', None) != 'local':
            raise NotImplementedError('join supports local execution only')
        if left._executor._local_kernel is not None and right._executor is not left._executor:
            raise ValueError("parallel join inputs must belong to the same local_execution context")
        if how not in {"inner", "left", "semi", "anti"} or len(fields) != len(rfields):
            raise ValueError("invalid join type or key arity")
    if kind == "groups" and (max_rows < 1 or output in fields or output in {"group_index", "group_last"}):
        raise ValueError("invalid grouping fields or max_rows")
    if kind == "reduce" and not callable(reducer):
        raise TypeError("reducer must be callable")
    source = KeyedSource(kind, PlanInput(left._source, left._plan, left._executor), fields,
        PlanInput(right._source, right._plan, right._executor) if right is not None else None,
        rfields, how, suffix, reducer, initial, chunk_bytes, max_rows, output)
    return Dataset(source, LogicalPlan(), left._executor)


class LocalExecutionSession:
    """Inspection handle; statistics reflect the latest action, not an estimate."""
    def __init__(self, executor):
        self._executor = executor

    @property
    def stats(self):
        import copy
        return copy.deepcopy(self._executor._local_kernel_stats)


@contextmanager
def local_execution(*, workers=4, worker_mode="process", partitions=None,
                    batch_rows=8192, memory_bytes=256 * 1024 * 1024,
                    temp_directory=None):
    """Bind existing Dataset readers to a single-machine partition executor.

    Callbacks must be partition-local: no driver side effects or dependence on
    invocation order across keys. Process mode uses spawn and cloudpickle for
    notebook functions; thread mode is for I/O or code releasing the GIL. Row
    order and within-key fold order are preserved. No retry of arbitrary UDFs.
    Actions must run inside this context; it closes all owned worker resources.
    """
    from ..data.api import _current_executor, _use_executor
    from .executors.local import LocalDatasetExecutor
    if _current_executor.get() is not None:
        raise ValueError("local_execution cannot replace a bound Pipeline or nested executor")
    options = LocalKernelOptions(workers, worker_mode, workers * 4 if partitions is None else partitions,
                                 batch_rows, memory_bytes, temp_directory)
    try:
        import cloudpickle  # noqa: F401
    except ImportError as exc:
        raise ImportError("local_execution requires the demiflow cloudpickle dependency") from exc
    executor = LocalDatasetExecutor(workers=workers, local_kernel=options)
    try:
        with _use_executor(executor):
            yield LocalExecutionSession(executor)
    finally:
        executor._local_kernel_closed = True
        executor.close()
