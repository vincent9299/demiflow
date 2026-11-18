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
    native_resources: Any = None
    worker_mode_explicit: bool = True

    def __post_init__(self):
        for name in ("workers", "partitions", "batch_rows", "memory_bytes"):
            value = getattr(self, name)
            if type(value) is not int or value < 1:
                raise ValueError(f"local_execution {name} must be a positive integer")
        if type(self.worker_mode_explicit) is not bool:
            raise ValueError("worker_mode_explicit must be boolean")
        if self.worker_mode not in {"thread", "process"}:
            raise ValueError("worker_mode must be thread or process")
        if self.partitions > 256:
            raise ValueError("local_execution supports at most 256 shuffle partitions")
        if self.memory_bytes < self.workers * 64 * 1024:
            raise ValueError("memory_bytes must provide at least 64 KiB per worker")
        from .native_resources import parse_native_resources
        object.__setattr__(self, 'native_resources', parse_native_resources(self.native_resources))


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
    reducer_contract: dict | None = None
    reducer_kwargs: dict | None = None


def keyed_dataset(left, kind, on, *, right=None, right_on=None, how="inner",
                  suffix="_right", reducer=None, initial=None,
                  chunk_bytes=32 * 1024 * 1024, max_rows=32, output="items", reducer_kwargs=None):
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
        if how not in {"inner", "left", "semi", "anti"} or len(fields) != len(rfields):
            raise ValueError("invalid join type or key arity")
    if kind == "groups" and (max_rows < 1 or output in fields or output in {"group_index", "group_last"}):
        raise ValueError("invalid grouping fields or max_rows")
    if kind == "reduce" and not callable(reducer):
        raise TypeError("reducer must be callable")
    source = KeyedSource(kind, PlanInput(left._source, left._plan, left._executor), fields,
        PlanInput(right._source, right._plan, right._executor) if right is not None else None,
        rfields, how, suffix, reducer, initial, chunk_bytes, max_rows, output,
        reducer_kwargs=dict(reducer_kwargs or {}))
    from ..planning.dataflow_compile import expand_relational_inputs
    from ..planning.dataflow_policy import resolve_runtime_policy
    from .contracts.dataflow_errors import DataflowError
    _, bounds = resolve_runtime_policy({}, {}, provider="local")
    bindings = expand_relational_inputs(source, path=(), bounds=bounds)
    if len(bindings) != (2 if right is not None else 1):
        raise DataflowError("INVALID_CONTRACT", "relational input port multiplicity was not preserved")
    return Dataset(source, LogicalPlan(), left._executor)


class LocalExecutionSession:
    """Inspection handle; statistics reflect the latest action, not an estimate."""
    def __init__(self, executor):
        self._executor = executor

    @property
    def run_ref(self):
        return getattr(self._executor, "_run_ref", None)

    @property
    def stats(self):
        import copy
        return copy.deepcopy(self._executor._local_kernel_stats)


@contextmanager
def local_execution(*, workers=4, worker_mode=None, partitions=None,
                    batch_rows=8192, memory_bytes=256 * 1024 * 1024,
                    temp_directory=None, native_resources=None, unified_chain=True,
                    unified_queue_chain=True, planning=None):
    """Bind existing Dataset readers to a single-machine partition executor.

    Callbacks must be partition-local: no driver side effects or dependence on
    invocation order across keys. Process mode uses spawn and cloudpickle for
    notebook functions; thread mode is for I/O or code releasing the GIL. Row
    order and within-key fold order are preserved. No retry of arbitrary UDFs.
    Actions must run inside this context; it closes all owned worker resources.
    planning optionally supplies the same validated platform planning policy as
    a Pipeline driver. Explicit worker limits remain bounded by that policy;
    omitting planning retains the platform defaults, including 32 workers.
    native_resources optionally tightens native query limits separately from
    Python workers/memory_bytes; shared admission and sampled guards remain.
    unified_chain selects the platform-side unified Runtime for stream-op
    materialize (fixed-source chain in one managed worker, single native commit
    authority). It defaults to True for the workbench Local stream-materialize
    slice; pass unified_chain=False for the byte-unchanged legacy in-driver
    fallback. Queue/incremental flows use run_stream (untouched by this seam) and
    open queues fail closed before any capture.
    unified_queue_chain selects the unified queue settlement carrier (MVP-2 A1):
    the driver keeps the queue lifecycle, RawFetch acquisition and finalize_task
    byte-unchanged, and only the by-value pure-maps suffix runs in a managed
    worker. It defaults to True for the workbench Local queue settlement slice;
    the S8.5 narrow gate routes a by-reference, fetch-only or non-serializable
    suffix to the byte-unchanged legacy in-driver write_queue_datasink fallback.
    Pass unified_queue_chain=False for the explicit legacy in-driver path.
    """
    from ..data.api import _current_executor, _use_executor
    from .executors.local import LocalDatasetExecutor
    from ..planning.policy import parse_platform_planning_policy
    planning_policy = parse_platform_planning_policy(planning)
    if _current_executor.get() is not None:
        raise ValueError("local_execution cannot replace a bound Pipeline or nested executor")
    options = LocalKernelOptions(workers, "process" if worker_mode is None else worker_mode,
                                 workers * 4 if partitions is None else partitions,
                                 batch_rows, memory_bytes, temp_directory, native_resources,
                                 worker_mode_explicit=worker_mode is not None)
    try:
        import cloudpickle  # noqa: F401
    except ImportError as exc:
        raise ImportError("local_execution requires the demiflow cloudpickle dependency") from exc
    executor = LocalDatasetExecutor(workers=workers, local_kernel=options,
                                    planning_policy=planning_policy,
                                    unified_chain=unified_chain,
                                    unified_queue_chain=unified_queue_chain)
    from .run_context import execution_scope
    try:
        with execution_scope() as run, _use_executor(executor):
            executor._run_ref = run.ref
            yield LocalExecutionSession(executor)
    finally:
        executor._local_kernel_closed = True
        executor.close()
