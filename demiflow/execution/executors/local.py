"""Local provider for Dataset actions and published materialized results."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from ...data.plan import LogicalPlan, reject_streaming_map_options
from ...data.sources import MaterializedSource, SourcePlan
from ...data.stats import ExecutionMetadata
from ...errors import UnsupportedSourceError
from .base import DatasetExecutor


# Published result pickles and their lazy checkpoint pages resolve these names.
@dataclass
class _LocalMaterializedHandle:
    blocks: Iterable[Any]
    row_count: int
    released: bool = False
    recovery_reference: Any = None

    def check_open(self):
        if self.released:
            raise RuntimeError('Materialized Dataset was released')

    def rows(self):
        """Read the published block representations lazily, in archive order."""
        self.check_open()
        import pickle
        for block in self.blocks:
            if isinstance(block, _SpilledBlock):
                with open(block.path, 'rb') as stream:
                    while True:
                        try:
                            row = pickle.load(stream)
                        except EOFError:
                            break
                        yield row
            elif isinstance(block, _EncodedBlock):
                for raw in block.rows:
                    yield pickle.loads(raw)
            else:
                yield from block

    def num_blocks(self):
        self.check_open()
        return sum(1 for _ in self.blocks)

    def release(self):
        if self.released:
            return
        for block in self.blocks:
            if isinstance(block, _SpilledBlock) and not block.durable:
                Path(block.path).unlink(missing_ok=True)
        self.blocks = ()
        self.released = True


class _CheckpointBlocks:
    """Repeatable lazy block paths backed by immutable checkpoint pages."""
    def __init__(self, terminal):
        self.run, self.action_id = terminal.run, terminal.action.action_id
        self.sequence, self.reader, self.has_objects = terminal.sequence, terminal.row_sink, terminal.has_objects
        self.sink, self.coordinator = terminal.sink, terminal.coordinator

    def __iter__(self):
        for fact in self.run.journal.iter_execution_commits(self.action_id, through_sequence=self.sequence):
            # Historical rows also own object publications. Verify the complete
            # native receipt lazily before exposing this generation's blocks.
            receipt = self.sink.inspect(fact['prepared']['sinks']['output'],
                                        self.coordinator._context(fact['commit_id']))
            if receipt != fact['receipts']['output']:
                from ..recovery_contract import RecoveryContractError
                raise RecoveryContractError('Completed receipt differs from artifact evidence')
            value = receipt['data']
            if self.has_objects:
                value = value['rows']
            for block in self.reader.block_entries(value):
                yield _SpilledBlock(str(self.run.directory / block['path']), durable=True)

    def __len__(self):
        return sum(1 for _ in self)


@dataclass(frozen=True)
class _SpilledBlock:
    path: str
    durable: bool = False


@dataclass(frozen=True)
class _EncodedBlock:
    rows: tuple[bytes, ...]


class LocalDatasetExecutor(DatasetExecutor):
    NAME = "local"

    def __init__(
        self,
        workers: int = 4,
        *,
        block_size: int = 256,
        resource_root=None,
        diagnostics_root=None,
        planning_policy=None,
        candidate_execution=None,
        local_kernel=None,
    ) -> None:
        self._workers = max(1, int(workers))
        from ...planning.policy import parse_platform_planning_policy
        self._planning_policy = planning_policy or parse_platform_planning_policy(None)
        self._candidate_execution = candidate_execution
        self._local_kernel = local_kernel
        self._local_kernel_closed = False
        self._local_kernel_stats = {}
        if local_kernel is not None and local_kernel.workers > self._planning_policy.max_parallelism:
            raise ValueError('local kernel workers exceed the platform max_parallelism')
        self._block_size = max(1, int(block_size))
        self._last_metadata: ExecutionMetadata | None = None
        self._run_ref = None
        self.resource_root = Path(resource_root or '.').resolve()
        self.diagnostics_root = Path(diagnostics_root).resolve() if diagnostics_root is not None else None

    def operator_llm_usage(self) -> dict[str, int]:
        from demiflow.execution.journal.dataflow_usage import executor_prompt_usage
        return executor_prompt_usage(self)

    def plan(
        self, source, plan, action_kind, *, terminal_category="action",
        terminal_native_options=None, source_parallelism_cap=None,
        terminal_parallelism_cap=None,
    ):
        reject_streaming_map_options(plan, 'local')
        from demiflow.execution.executors.dataflow_api import explain_dataset
        options: dict[str, Any] = {
            'action_kind': action_kind,
            'terminal_category': terminal_category,
        }
        if terminal_native_options is not None:
            options['terminal_native_options'] = terminal_native_options
        if source_parallelism_cap is not None:
            options['source_parallelism_cap'] = source_parallelism_cap
        if terminal_parallelism_cap is not None:
            options['terminal_parallelism_cap'] = terminal_parallelism_cap
        return explain_dataset(source, plan, provider=self.NAME, options=options)

    def materialize_graph(self, graph_spec, *, host_id='local', resource_root=None, control_timeout_s=120):
        from .graph import materialize_graph
        return materialize_graph(self, graph_spec, host_id=host_id, resource_root=resource_root,
                                 control_timeout_s=control_timeout_s)

    def schema(self, source: SourcePlan, plan: LogicalPlan, *, fetch_if_missing: bool = True) -> Any:
        if not fetch_if_missing:
            return None
        from demiflow.execution.executors.dataflow_api import make_action_request, run_dataset_action
        request = make_action_request(self, source, plan, kind='schema',
            arguments={'fetch_if_missing': fetch_if_missing})
        return run_dataset_action(request)

    def columns(
        self, source: SourcePlan, plan: LogicalPlan, *, fetch_if_missing: bool,
    ) -> list[str] | None:
        if not fetch_if_missing:
            return None
        from demiflow.execution.executors.dataflow_api import make_action_request, run_dataset_action
        request = make_action_request(self, source, plan, kind='columns',
            arguments={'fetch_if_missing': fetch_if_missing})
        return run_dataset_action(request)

    def num_blocks(self, source: SourcePlan, plan: LogicalPlan) -> int:
        if not isinstance(source, MaterializedSource) or not plan.is_empty:
            raise TypeError("num_blocks is only available on MaterializedDataset")
        handle = source.handle
        num_blocks = getattr(handle, 'num_blocks', None)
        if num_blocks is None:
            raise UnsupportedSourceError("materialized source belongs to another backend")
        return int(num_blocks())

    def release(self, source):
        handle = source.handle
        release = getattr(handle, 'release', None)
        if release is None:
            raise UnsupportedSourceError('materialized source belongs to another backend')
        release()

    def stats(self, source: SourcePlan, plan: LogicalPlan) -> str:
        if self._last_metadata is None:
            return f"Dataset({type(source).__name__}, operations={len(plan.operations)}, not executed)"
        return str(self._last_metadata)

    def execution_metadata(self, source: SourcePlan, plan: LogicalPlan) -> ExecutionMetadata | None:
        from dataclasses import replace
        if self._last_metadata is not None:
            return replace(self._last_metadata, run_ref=self._run_ref)
        return None

    def close(self) -> None:
        self._local_kernel_closed = True
