"""Dataset graph contracts and opt-in parallel Python task execution.

The Dataset graph is independent of worker transport. Arrow source batches cross
the task boundary intact; Python row callbacks are lowered inside workers. Keyed
operators are explicit graph nodes, never closures that launch nested executors.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

# Historical programs may import the relational declarations by these names.
from ..data.sources import KeyedSource, PlanInput, SourcePlan, UnionSource


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
    native_resources_inherited: bool = False

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
        from demiflow.planning.dataflow_policy import parse_native_resources
        object.__setattr__(self, 'native_resources', parse_native_resources(self.native_resources))


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
                    temp_directory=None, native_resources=None, inherited_native_resources=None,
                    planning=None, resource_root=None,
                    diagnostics_root=None, block_size=256):
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
    A host can instead forward its admitted envelope using inherited_native_resources.
    This preserves the original budget declarations in its RunContext.
    resource_root resolves relative Dataset/Prompt resources (default: cwd).
    diagnostics_root overrides native diagnostics placement; None retains the
    current run's diagnostics fallback. Neither option creates a directory here.
    block_size controls the executor's source blocks independently of batch_rows;
    its ordinary default remains 256. Managed hosts may explicitly bind both.
    """
    from ..data.execution_contract import _current_executor, _use_executor
    from .executors.local import LocalDatasetExecutor
    from ..planning.policy import parse_platform_planning_policy
    planning_policy = parse_platform_planning_policy(planning)
    if type(block_size) is not int or block_size < 1:
        raise ValueError("local_execution block_size must be a positive integer")
    if _current_executor.get() is not None:
        raise ValueError("local_execution cannot replace a bound Pipeline or nested executor")
    if native_resources is not None and inherited_native_resources is not None:
        raise ValueError("Choose declared or inherited native resources for this executor")
    inherited = inherited_native_resources is not None
    options = LocalKernelOptions(workers, "process" if worker_mode is None else worker_mode,
                                 workers * 4 if partitions is None else partitions,
                                 batch_rows, memory_bytes, temp_directory,
                                 inherited_native_resources if inherited else native_resources,
                                 worker_mode_explicit=worker_mode is not None, native_resources_inherited=inherited)
    try:
        import cloudpickle  # noqa: F401
    except ImportError as exc:
        raise ImportError("local_execution requires the demiflow cloudpickle dependency") from exc
    executor = LocalDatasetExecutor(workers=workers, local_kernel=options,
                                    resource_root=resource_root, diagnostics_root=diagnostics_root,
                                    block_size=block_size,
                                    planning_policy=planning_policy)
    from .run_context import execution_scope
    try:
        with execution_scope() as run, _use_executor(executor):
            executor._run_ref = run.ref
            yield LocalExecutionSession(executor)
    finally:
        executor.close()
