"""The single concrete Kernel state holder and lifecycle (lane K, K016-K022).

OperatorKernel owns the resolved compute instance, per-input-port fronts, bounded
batch/spool buffers, the output confirmation cursor and the lifecycle. It never
executes in ``__init__``; ``start`` constructs the instance once, ``push``/``finish``
delegate to the unique compute function for the kind, ``snapshot``/``restore``
persist a recovery image (never a live DataFusion session or instance object), and
``close`` releases every owner idempotently while preserving the business error.

Finite-complete ports spool records to bounded durable segments so external
relational/ordering/aggregate kernels can rebuild input after a crash and skip
already-confirmed output. Incremental ports compute per record; batch ports
assemble a bounded batch and flush the remainder on EOI.
"""
from __future__ import annotations

import asyncio
import pickle
from . import state_codec
from .async_instance import AsyncInstance
from .block_partition import preserve_block
from collections.abc import Iterator, Mapping
from typing import Any

from demiflow.execution.contracts.dataflow import (
    AggregateState,
    ArtifactRef,
    KernelSpec,
    Record,
    RuntimeServices,
)

from .dataflow import (
    KERNEL_KINDS,
    RECORD_MEDIA_TYPE,
    _AGGREGATE_KINDS,
    _BATCH_KINDS,
    _canonical,
    _cfg,
    _check_stop,
    _err,
    _hash_id,
    _input_row_bytes,
    _MODEL_KINDS,
    _occurrence,
    _ORDERING_KINDS,
    _RELATIONAL_KINDS,
    _STREAM_GROUP_KINDS,
    _UNARY_KINDS,
    invoke_batch,
    invoke_unary,
)

IMAGE_VERSION = "demiflow-kernel-image-v3"
IMAGE_MEDIA_TYPE = "application/x-demiflow-kernel-image-v3"
_SPOOL_OWNER = "kernel-spool"
_INSTANCE_KINDS = frozenset({"MapOp", "FlatMapOp", "BoundMapOp", "FilterOp", "AddColumnOp"}) | _BATCH_KINDS | _MODEL_KINDS
_BATCH_INSTANCE_KINDS = _BATCH_KINDS | {"embedding", "EmbeddingMapOp", "RegisterDocumentsBatchOp", "SaveLanceOp"}


def _segment_rows(services: RuntimeServices) -> int:
    # A maximum-sized row is not a prediction of each row's actual size.
    # The port independently admits encoded bytes before growing the segment.
    return 8192


def _resolve_instance(spec: KernelSpec, services: RuntimeServices, *, declaration=None) -> Any:
    key = spec.configuration.get("program")
    if key is None and spec.implementation_ref is not None:
        key = spec.implementation_ref.key
    if key is None:
        raise _err("INVALID_CONTRACT", "callable kernel requires a staged program")
    program = services.programs.get(key)
    if program is None or program.kind not in ("callable", "native"):
        raise _err("INVALID_CONTRACT", "callable program not staged", program=str(key))
    from demiflow.data.plan import CallableSpec
    declaration = program.value if declaration is None else declaration
    if isinstance(declaration, CallableSpec):
        if spec.kind == "BoundMapOp":
            from demiflow.data.plan import BoundMapOp, MapStreamOptions
            op = BoundMapOp(callable=declaration, inputs=dict(spec.configuration["inputs"]),
                            output=spec.configuration.get("output"),
                            outputs=spec.configuration.get("outputs"), stream_options=MapStreamOptions())
            return _FieldCallable(op, shared=bool(spec.configuration.get('shared_actor')))
        instance = declaration.target if spec.configuration.get('shared_actor') else declaration.instantiate()
        if declaration.call_args or declaration.call_kwargs:
            return _BoundCallable(instance, declaration.call_args, dict(declaration.call_kwargs))
        return instance
    return declaration


from demiflow.data.plan import BoundCallable


class _FieldCallable(BoundCallable):
    def __init__(self, operation, *, shared=False):
        if not shared:
            super().__init__(operation)
            return
        from ...data.runtime_contract import validator
        self._operation = operation
        self._fn = operation.callable.target
        self._validation = validator(self._fn, operation.callable.runtime_contract)

    def __getattr__(self, name):
        return getattr(self._fn, name)


class _BoundCallable:
    """Keep lifecycle hooks on the actor when binding call-time arguments."""
    def __init__(self, instance, args, kwargs):
        self._instance, self._args, self._kwargs = instance, args, kwargs

    def __call__(self, value):
        return self._instance(value, *self._args, **self._kwargs)

    def __getattr__(self, name):
        return getattr(self._instance, name)


def _run_astart(instance: Any) -> None:
    if isinstance(instance, AsyncInstance):
        instance.start()
        return
    astart = getattr(instance, "astart", None)
    if astart is None:
        return
    result = astart()
    if asyncio.iscoroutine(result):
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            asyncio.run(result)
        else:
            raise _err("INVALID_CONTRACT", "astart must not nest a running user event loop")


def _close_instance(instance: Any) -> None:
    if instance is None:
        return
    if isinstance(instance, AsyncInstance):
        instance.close()
        return
    aclose = getattr(instance, "aclose", None)
    if aclose is not None:
        result = aclose()
        if asyncio.iscoroutine(result):
            asyncio.run(result)
        return
    close = getattr(instance, "close", None)
    if close is not None:
        close()


class _SpoolPort:
    """Bounded durable segment spool for one finite-complete input port."""

    def __init__(self, name: str, services: RuntimeServices, segment_rows: int) -> None:
        self.name = name
        self._services = services
        self._segment_rows = segment_rows
        self._refs: list[ArtifactRef] = []
        self._borrowed = []
        self._borrowed_proofs = {}
        self._pending: list[bytes] = []
        self._empty_bytes = len(state_codec.dumps([], max_bytes=services.bounds.max_batch_bytes))
        self._pending_bytes = self._empty_bytes
        # Retained records coexist with their encoded segment during a flush.
        self._segment_bytes = min(services.bounds.max_batch_bytes,
            (services.bounds.max_batch_bytes + services.bounds.max_row_bytes) // 2)
        self._eoi = False
        self._count = 0
        self._bytes = 0

    @property
    def eoi(self) -> bool:
        return self._eoi

    def push(self, record: Record) -> None:
        if self._eoi:
            raise _err("INVALID_CONTRACT", "push after port EOI", port=self.name)
        if _input_row_bytes(record) > self._services.bounds.max_row_bytes:
            raise _err("BUDGET_EXCEEDED", "input spool row exceeds max_row_bytes", port=self.name)
        raw = state_codec.record_fragment(record, max_bytes=self._services.bounds.max_batch_bytes)
        contribution = len(raw)
        workspace = 0
        if getattr(self, '_shuffle_write_window', None) is not None:
            from .shuffle_order import spool_fragment_workspace
            workspace = spool_fragment_workspace(raw, self._shuffle_write_window)
            if self._pending and self._pending_workspace + workspace > self._shuffle_write_window:
                self._flush()
        projected = self._pending_bytes + contribution + bool(self._pending)
        if self._pending and (projected > self._segment_bytes or len(self._pending) >= self._segment_rows):
            self._flush()
        self._pending_bytes += contribution + bool(self._pending)
        self._pending.append(raw)
        if workspace:
            self._pending_workspace += workspace
        self._count += 1
        if len(self._pending) >= self._segment_rows or self._pending_bytes >= self._segment_bytes:
            self._flush()

    def _flush(self) -> None:
        if not self._pending:
            return
        if self._bytes + self._pending_bytes > self._services.bounds.max_spill_bytes:
            raise _err("BUDGET_EXCEEDED", "input spool exceeds max_spill_bytes", port=self.name)
        # Keep the original list encoding, but stream already admitted fragments
        # into the store. No second payload encoding or full-batch copy exists.
        ref = self._services.store.put(state_codec.record_list_chunks(self._pending),
            max_bytes=self._pending_bytes, media_type=RECORD_MEDIA_TYPE)
        self._refs.append(ref)
        self._bytes += self._pending_bytes
        self._pending = []
        self._pending_bytes = self._empty_bytes
        if getattr(self, '_shuffle_write_window', None) is not None:
            self._pending_workspace = 4096

    def finish(self) -> None:
        self._flush()
        self._eoi = True

    def adopt(self, entry):
        if self._eoi:
            raise _err('INVALID_CONTRACT', 'finite reference after port EOI', port=self.name)
        from ..dataflow_finite_input import validate_borrowed_entry
        validate_borrowed_entry(self._services, entry)
        if len(self._borrowed) >= self._services.bounds.max_partitions:
            raise _err('BUDGET_EXCEEDED', 'borrowed port membership exceeds max_partitions')
        if any((item['input']['edge_id'], item['input']['producer'], item['input']['target']) ==
               (entry['edge_id'], entry['producer'], entry['target']) for item in self._borrowed):
            raise _err('CORRUPT_ARTIFACT', 'duplicate borrowed input member')
        self._flush()  # Preserve any historical/local owned prefix before a ref.
        candidate = {'position':len(self._refs),'input':dict(entry)}
        state_codec.dumps(self._borrowed + [candidate], max_bytes=self._services.bounds.max_state_bytes)
        self._borrowed.append(candidate)
        from ..dataflow_finite_input import _ValidatedFiniteEntry
        if type(entry) is _ValidatedFiniteEntry:
            self._borrowed_proofs[len(self._borrowed)-1] = entry
        if not self._projection_metadata_fits():
            self._borrowed_proofs.clear()
        self._count += entry['rows']

    def records(self) -> Iterator[Record]:
        return _SpoolRecords(self)

    def _projection_metadata_fits(self):
        raw = state_codec.dumps(self._borrowed, max_bytes=self._services.bounds.max_state_bytes)
        return 3 * len(raw) <= self._services.bounds.max_state_bytes

    def _join_records(self, fields):
        from ..dataflow_finite_input import validate_finite_members, validate_borrowed_entry, _validated_entry
        if not self._projection_metadata_fits():
            self._borrowed_proofs.clear()
            yield from self._records()
            return
        # Restored mappings carry no live capability. Revalidate each complete
        # original scope before returning any optimized join input.
        for index, item in enumerate(self._borrowed):
            entry = item['input']
            proof = self._borrowed_proofs.get(index)
            if proof is not None:
                _validated_entry(self._services, entry, proof)
                continue
            spec = validate_borrowed_entry(self._services, entry)
            verified = validate_finite_members(self._services, entry['seal_ref'], spec, target=entry['target'])
            from ..dataflow_finite_input import _ValidatedFiniteEntry
            if any(type(fresh) is not _ValidatedFiniteEntry for fresh in verified):
                self._borrowed_proofs.clear()
                yield from self._records()
                return
            for other, candidate in enumerate(self._borrowed):
                for fresh in verified:
                    if dict(fresh) == candidate['input']:
                        self._borrowed_proofs[other] = fresh
            _validated_entry(self._services, entry, self._borrowed_proofs.get(index))
        yield from self._records(join_keys=fields)

    def _records(self, *, join_keys=None):
        from ..dataflow_finite_input import borrowed_records, borrowed_join_records
        borrowed = iter(self._borrowed)
        pending = next(borrowed, None)
        member_index = 0
        for index in range(len(self._refs) + 1):
            _check_stop(self._services)
            while pending is not None and pending['position'] == index:
                if join_keys is None:
                    yield from borrowed_records(self._services, pending['input'],
                        frame_call=getattr(self, '_shuffle_frame_call', None))
                else:
                    from .join_spool import _resident_limit
                    yield from borrowed_join_records(self._services, pending['input'],
                        self._borrowed_proofs[member_index], fields=join_keys,
                        row_window=max(1, _resident_limit(self._services)//4))
                member_index += 1
                pending = next(borrowed, None)
            if index < len(self._refs):
                ref = self._refs[index]
                window = getattr(self, '_shuffle_input_window', None)
                options = {}
                if window is not None:
                    if 2 * ref.size_bytes + 4096 > window:
                        raise _err('BUDGET_EXCEEDED', 'shuffle input segment exceeds its reader subwindow')
                    options['chunk_bytes'] = min(4096, max(1, window // 32))
                data = b"".join(self._services.store.read(ref, max_bytes=ref.size_bytes, **options))
                if window is not None:
                    from .shuffle_order import admit_spool_image
                    admit_spool_image(data, window)
                yield from state_codec.loads(data, max_bytes=self._services.bounds.max_batch_bytes)
        for fragment in self._pending:
            _check_stop(self._services)
            raw = b''.join(state_codec.record_list_chunks((fragment,)))
            if getattr(self, '_shuffle_input_window', None) is not None:
                from .shuffle_order import admit_spool_image
                admit_spool_image(raw, self._shuffle_input_window)
            yield from state_codec.loads(raw, max_bytes=self._services.bounds.max_batch_bytes)

    def release(self) -> None:
        for ref in self._refs:
            self._services.store.release(ref, owner=_SPOOL_OWNER)
        self._refs = []
        # A borrower never releases or deletes the producer's immutable refs.
        self._borrowed = []
        self._borrowed_proofs = {}
        self._pending = []
        self._pending_bytes = self._empty_bytes

    def snapshot(self) -> dict[str, Any]:
        # Persist each bounded pending segment, keeping the recovery image a
        # small set of original artifact references even with multiple ports.
        self._flush()
        return {
            "refs": list(self._refs), "pending": list(self._pending),
            "eoi": self._eoi, "count": self._count, "bytes": self._bytes,
            **({'borrowed':[{'position':x['position'],'input':dict(x['input'])}
                            for x in self._borrowed]} if self._borrowed else {}),
        }

    def restore(self, snap: Mapping[str, Any]) -> None:
        self._refs = list(snap["refs"])
        borrowed = snap.get('borrowed', [])
        if type(borrowed) is not list or len(borrowed) > self._services.bounds.max_partitions:
            raise _err('CORRUPT_ARTIFACT', 'invalid borrowed input membership')
        from ..dataflow_finite_input import validate_borrowed_entry
        previous = 0
        members = set()
        for item in borrowed:
            if (type(item) is not dict or set(item) != {'position','input'}
                    or type(item['position']) is not int
                    or not previous <= item['position'] <= len(self._refs)):
                raise _err('CORRUPT_ARTIFACT', 'borrowed input order differs from owned prefix')
            validate_borrowed_entry(self._services, item['input'])
            entry = item['input']
            member = (entry['edge_id'], entry['producer'], entry['target'])
            if member in members:
                raise _err('CORRUPT_ARTIFACT', 'duplicate borrowed input member')
            members.add(member)
            previous = item['position']
        state_codec.dumps(borrowed, max_bytes=self._services.bounds.max_state_bytes)
        self._borrowed = list(borrowed)
        self._borrowed_proofs = {}
        pending = list(snap["pending"])
        # Old images may contain in-memory pending records. Retain that format
        # and derive byte admission from the actual restored data, never a new
        # claimed counter or a row-size estimate.
        if len(pending) > self._segment_rows:
            raise _err("BUDGET_EXCEEDED", "restored spool exceeds segment row bound")
        if any(_input_row_bytes(record) > self._services.bounds.max_row_bytes for record in pending):
            raise _err("BUDGET_EXCEEDED", "restored spool row exceeds max_row_bytes")
        self._pending = [state_codec.record_fragment(record,
            max_bytes=self._services.bounds.max_batch_bytes) for record in pending]
        self._pending_bytes = self._empty_bytes + sum(map(len, self._pending)) + max(0, len(self._pending) - 1)
        if getattr(self, '_shuffle_write_window', None) is not None:
            from .shuffle_order import spool_fragment_workspace
            self._pending_workspace = 4096 + sum(spool_fragment_workspace(raw,
                self._shuffle_write_window) for raw in self._pending)
            if self._pending_workspace > self._shuffle_write_window:
                raise _err('BUDGET_EXCEEDED', 'restored shuffle pending rows exceed their reader subwindow')
        if self._pending_bytes > self._services.bounds.max_batch_bytes:
            raise _err('BUDGET_EXCEEDED', 'restored spool exceeds batch byte bound')
        self._eoi = bool(snap["eoi"])
        if (type(snap['count']) is not int or type(snap['bytes']) is not int
                or snap['count'] < sum(item['input']['rows'] for item in borrowed)
                or snap['bytes'] < 0):
            raise _err('CORRUPT_ARTIFACT', 'invalid restored spool row or byte count')
        self._count = snap['count']
        self._bytes = snap['bytes']


class _SpoolRecords:
    """Ordinary Record iterator with an explicit kernel-only join projection."""
    def __init__(self, port):
        self.port = port
        self._iterator = None

    def __iter__(self):
        return self

    def __next__(self):
        if self._iterator is None:
            self._iterator = self.port._records()
        return next(self._iterator)

    def for_join(self, fields):
        if self._iterator is not None:
            raise _err('INVALID_CONTRACT', 'cannot project a partially consumed spool')
        return self.port._join_records(fields)

    def close(self):
        if self._iterator is not None:
            self._iterator.close()


class OperatorKernel:
    """K016: the only concrete Kernel; construction performs no execution."""

    def __init__(self, spec: KernelSpec, *, services: RuntimeServices, partition_id: str,
                 finite_services: RuntimeServices | None = None) -> None:
        if spec.kind not in KERNEL_KINDS:
            raise _err("INVALID_CONTRACT", "unknown kernel kind", kind=spec.kind)
        self._spec = spec
        from ..dataflow_operator_observations import CallbackObservations
        self.observations = CallbackObservations()
        self._services = services
        self._finite_services = services if finite_services is None else finite_services
        from ..record_schema import RecordSchemas
        self._schemas = RecordSchemas(services)
        self._partition_id = partition_id
        self._occurrence = _occurrence(spec)
        self._instance: Any = None
        self._shared_actor = None
        self._recovery_rows = None
        self._recovery_buffering = False
        self._started = False
        self._reducer_retained_bytes = 0
        self._shuffle_retained_bytes = 0
        self._closed = False
        self._restored = False
        self._image: dict[str, Any] | None = None
        self._segment_rows = _segment_rows(services)
        self._ports: dict[str, _SpoolPort] = {
            port.name: _SpoolPort(port.name, self._finite_services, self._segment_rows)
            for port in spec.inputs
            if port.requirement == "finite_complete" or spec.kind in ("union", "StreamPrependOp")
        }
        if spec.kind == 'RandomShuffleOp' and 'shuffle_order_version' in spec.configuration:
            from .shuffle_order import FIELD, VERSION, shuffle_windows
            if spec.configuration[FIELD] != VERSION:
                raise _err('INVALID_CONTRACT', 'unknown seeded shuffle ordering version')
            windows = shuffle_windows(self._finite_services)
            window = windows.input_bytes - windows.metadata_bytes
            for port in self._ports.values():
                # New plans write original-format segments that fit the same
                # reader subwindow. Never reject a large segment made only of
                # otherwise fitting small rows at finish time.
                port._shuffle_write_window = window
                port._pending_workspace = 4096
                port._segment_bytes = min(port._segment_bytes, max(1, (window - 4096) // 2))
        self._port_order = tuple(port.name for port in spec.inputs)
        self._batch: list[Record] = []
        self._batch_deadline = None
        self._batch_ordinal = 0
        self._batch_membership = None
        self._max_batch = int(spec.configuration.get("batch_size") or spec.configuration.get("max_batch") or spec.configuration.get("default_batch_size") or 256)
        self._groups: dict[str, list[dict[str, Any]]] = {}
        self._group_base: dict[str, dict[str, Any]] = {}
        self._timer_deadline: int | None = None
        self._group_bytes = {}
        self._group_buffer_bytes = 0
        self._emitted = 0
        self._skip = 0
        self._finish_cursor = None
        self.aggregate_state: AggregateState | None = None
        self._spool_owner_refs: list[ArtifactRef] = []
        self._instances = []
        self._calls = None
        self._concurrency = int(spec.configuration.get('concurrency') or 1)
        self._worker_scope = spec.configuration.get('callable_scope') == 'worker'
        self.recovery_restriction = None
        self._task_rows = spec.configuration.get('task_batch_rows')
        if self._task_rows is not None and (type(self._task_rows) is not int or self._task_rows < 1
                or spec.configuration.get('streaming_map') or self._concurrency != 1):
            raise _err('INVALID_CONTRACT', 'invalid logical callback task policy')
        self._task_frontier = 0
        self._task_ordinal = None
        self._task_group = spec.configuration.get('task_group', self._occurrence)
        self._task_origin = spec.configuration.get('task_origin', True)
        if type(self._task_origin) is not bool:
            raise _err('INVALID_CONTRACT', 'invalid logical task origin')
        self._task_declaration = None

    # -- K017 start ---------------------------------------------------------
    def start(self) -> None:
        if self._closed:
            raise _err("INVALID_CONTRACT", "cannot start a closed kernel")
        if self._started:
            return
        self._validate_profile()
        try:
            if self._spec.kind in _INSTANCE_KINDS:
                count = self._concurrency if self._worker_scope else 1
                self._instances = [None] * count
                key = self._spec.configuration.get('program')
                if key is None and self._spec.implementation_ref is not None:
                    key = self._spec.implementation_ref.key
                program = self._services.programs.get(key)
                from ...data.plan import CallableSpec
                factory = (program is not None and isinstance(program.value, CallableSpec)
                           and program.value.is_class)
                # Existing instances own lifecycle even for empty input. Class
                # declarations acquire resources only for an invoked/restored slot.
                if self._task_rows:
                    # LoadedProgram is the sealed declaration, never an active
                    # instance. Retain only its existing reference, not a second
                    # resident serialized copy of the whole program.
                    self._task_declaration = program.value
                # A restored task constructs only if its snapshot has a live
                # partial batch. Completed task boundaries do not rerun hooks.
                if not factory and not (self._task_rows and self._image is not None):
                    for slot in range(count):
                        self._create_instance(slot)
                if self._concurrency > 1:
                    from .concurrent_calls import ConcurrentCalls
                    self._calls = ConcurrentCalls(self._concurrency, check_stop=self._services.check_stop)
            from ..dataflow_batch_membership import KINDS, NativeBatchMembership
            if self._spec.kind in KINDS:
                self._batch_membership = NativeBatchMembership(self._services,self._occurrence,self._partition_id)
            if self._image is not None:
                self._apply_image(self._image)
            self._started = True
        except BaseException:
            self._cleanup_on_failure()
            raise

    def _create_instance(self, slot):
        declaration = None
        if self._task_declaration is not None and self._shared_actor is None:
            import cloudpickle
            raw = cloudpickle.dumps(self._task_declaration)
            if len(raw) > self._services.bounds.max_state_bytes:
                raise _err('BUDGET_EXCEEDED', 'task declaration exceeds max_state_bytes')
            declaration = cloudpickle.loads(raw)
        if self._spec.state == 'adapter':
            # LoadedProgram is a frozen declaration, never the active function
            # owner. Each occurrence/worker slot gets its own admitted data.
            from ...data.plan import CallableSpec
            from dataclasses import replace
            from .task_scope import function_residency_kind, clone_resident_callable
            key = self._spec.configuration.get('program')
            if key is None and self._spec.implementation_ref is not None:
                key = self._spec.implementation_ref.key
            program = self._services.programs.get(key)
            original = declaration if declaration is not None else program.value if program is not None else None
            if isinstance(original, CallableSpec) and function_residency_kind(original.target,
                    call_args=original.call_args, call_kwargs=original.call_kwargs) == 'adapter':
                target, args, kwargs = clone_resident_callable(original.target,
                    max_bytes=self._services.bounds.max_state_bytes,
                    call_args=original.call_args, call_kwargs=original.call_kwargs)
                declaration = replace(original, target=target, call_args=args, call_kwargs=kwargs)
        instance = _resolve_instance(self._spec, self._services, declaration=declaration)
        if self._spec.kind in {'prompt', 'embedding', 'agent', 'fetch', 'document'}:
            from ..dataflow_models import bind_model_services
            instance = bind_model_services(instance, self._spec, services=self._services)
        from ..dataflow_acquisition_call import AcquisitionCall
        from ..dataflow_producer import ProducerCall
        from ..dataflow_search import NativeSearchCall
        # A real native writer declaration already imported its canonical
        # class while the sealed program was loaded. Merely recognizing an
        # ordinary callable must not initialize Lance and its driver stack.
        # Inspect only the known module table, never user class attributes.
        import sys
        stream_lance = sys.modules.get('demiflow.execution.dataflow_stream_lance')
        stream_lance_type = (vars(stream_lance).get('StreamLanceCall')
                             if stream_lance is not None else None)
        if any(type(instance) is native for native in
               (AcquisitionCall, ProducerCall, NativeSearchCall, stream_lance_type)):
            instance.bind(self._services, self._occurrence)
        from ...data.result_cache import CacheCall
        if isinstance(instance, CacheCall):
            instance.validate_runtime_state(1 if self._worker_scope else self._concurrency)
            state_owner = (self._shared_actor.binding['state_owner']
                           if self._shared_actor is not None else self._occurrence)
            instance.bind_checkpoint_cache(_hash_id(['cache-state-owner-v1',
                self._services.assignment.run_id, self._services.assignment.action_id,
                state_owner, self._partition_id, slot if self._worker_scope else 0,
                (self._task_ordinal or 0) if self._task_rows and self._shared_actor is None else None]))
            from ...operator_llm.runtime import PromptActor
            if type(instance.fn) is PromptActor:
                from ..dataflow_model_journal import bind_prompt_adapter, bind_agent_adapter
                bind = bind_prompt_adapter if instance.fn.environment is None else bind_agent_adapter
                bind(instance.fn, services=self._services, node_id=self._occurrence)
        import inspect
        owner = self._state_owner(instance)
        lifecycle = (None if inspect.isroutine(owner) or inspect.isclass(owner)
                     else self.observations.lifecycle)
        if self._shared_actor is not None:
            if owner is not self._shared_actor.owner:
                raise _err('CORRUPT_ARTIFACT', 'Shared actor call resolved to a different owner')
            instance = self._shared_actor.acquire(self._occurrence, instance, self._services,
                lifecycle=lifecycle, timeout=self._spec.configuration.get('hard_timeout'),
                await_calls=self._spec.kind not in {'MapOp', 'BoundMapOp'}
                    and self._spec.configuration.get('execution') != 'thread')
        else:
            instance = AsyncInstance(instance, self._services,
                                     owner=owner, lifecycle=lifecycle,
                                     timeout=self._spec.configuration.get('hard_timeout'),
                                     await_calls=self._spec.kind not in {'MapOp', 'BoundMapOp'}
                    and self._spec.configuration.get('execution') != 'thread')
        instance.native_scope = (self._occurrence, self._partition_id) + ((slot,) if self._worker_scope else ())
        self._instances[slot] = instance
        if slot == 0:
            self._instance = instance
        # Field-bound calls already validate at their original keyword boundary.
        # Other checked contracts belong to this call, not the actor state owner.
        if self._spec.kind != 'BoundMapOp':
            from ...data.plan import CallableSpec
            from ...data.runtime_contract import validator
            key = self._spec.configuration.get('program')
            if key is None and self._spec.implementation_ref is not None:
                key = self._spec.implementation_ref.key
            program = declaration if declaration is not None else self._services.programs[key].value
            if isinstance(program, CallableSpec) and program.runtime_contract is not None:
                target = instance.target
                if type(target) is _BoundCallable:
                    instance.validation_args = target._args
                    instance.validation_kwargs = target._kwargs
                    target = target._instance
                instance.validation = validator(target, program.runtime_contract)
        _run_astart(instance)
        if self._spec.state != 'none':
            from ..operator_policy import classify_operator_state
            owner = self._state_owner(instance)
            policy = classify_operator_state(owner, phase='instance')
            if policy.mode == 'function':
                state = self._snapshot_state(instance)
                if state['mode'] == 'nonrecoverable':
                    self.recovery_restriction = state['reason']
                    self._services.journal.restrict_recovery(self._occurrence)
            concurrency = 1 if self._worker_scope else self._concurrency
            if policy.mode == 'auto_json' and vars(owner) and concurrency != 1:
                raise _err('INVALID_CONTRACT', 'auto_json state requires concurrency 1; declare snapshot_state/restore_state for higher concurrency')
            if policy.mode == 'unsupported':
                self.recovery_restriction = policy.reason
                self._services.journal.restrict_recovery(self._occurrence)

    def _slot_instance(self, slot):
        slot = slot if self._worker_scope else 0
        if self._instances[slot] is None:
            self._create_instance(slot)
        return self._instances[slot]

    def _call_scope(self, slot):
        instance = self._slot_instance(slot)
        result = _hash_id(['dataflow-call-v3', *instance.native_scope, instance.native_call_ordinal])
        instance.native_call_ordinal += 1
        return result

    @property
    def pending_calls(self):
        return self._calls is not None and self._calls.pending

    @property
    def can_snapshot_pending(self):
        # The native journal owns request effects for these row-local actors.
        # Arbitrary mutable UDFs/agents still drain before a state snapshot.
        if self._spec.kind not in {'prompt', 'embedding'} or not self._instances:
            return False
        for instance in self._instances:
            if instance is None:
                continue
            owner = self._state_owner(instance)
            recovery = getattr(owner, '_native_recovery', None)
            if recovery is None:
                recovery = getattr(getattr(owner, '_runtime', None), 'native_recovery', None)
            if recovery is None or not recovery.row_local:
                return False
        return True

    @property
    def cooperative_finish(self):
        from .recovery_rows import hook_owner
        return (self._recovery_rows is not None or hook_owner(self) is not None
                or bool(self._ports) and self._spec.kind != 'StreamPrependOp')

    @property
    def recovery_waits_for_live_drain(self):
        from .recovery_rows import hook_owner
        return self._shared_actor is not None and self._recovery_rows is None and hook_owner(self) is not None

    @property
    def finish_replayable(self):
        # Structural finite kernels are deterministic from the original sealed
        # inputs. A user reducer may mutate hidden state; do not manufacture a
        # replay promise for its active Python fold iterator.
        return self.cooperative_finish and self._spec.kind != 'reduce'

    @property
    def checkpoint_ready(self):
        if self._recovery_buffering or self._recovery_rows is not None and not self._recovery_rows.prepared:
            return False
        if self._finish_cursor is not None and not self.finish_replayable:
            return False
        for instance in self._instances:
            if instance is not None:
                instance.check_lifecycle()
        return not self.pending_calls or self.can_snapshot_pending

    def poll(self):
        for instance in self._instances:
            if instance is not None:
                instance.check_lifecycle()
        if self._batch and self._batch_deadline is not None and self._services.clock_ms() >= self._batch_deadline:
            expected = self._batch_membership.expected(self._batch_ordinal) if self._batch_membership else None
            if expected is None:
                batch, self._batch = tuple(self._batch), []
                self._batch_deadline = None
                yield from self._invoke_batch(batch)
        if self._spec.kind in _STREAM_GROUP_KINDS:
            yield from self._emit(self._flush_expired_groups())
        if self._calls is not None:
            yield from self._emit(self._calls.poll(max_records=1))

    def _validate_profile(self) -> None:
        profile = self._spec.profile
        requests = getattr(profile, "requests", None)
        if not isinstance(requests, Mapping):
            raise _err("INVALID_CONTRACT", "kernel profile must declare requests")
        role = self._spec.resource_role
        if role == "gpu" and not any(a.get("type") == "gpu" and a.get("count", 0) > 0
                                    for a in requests.get("accelerators", ())):
            raise _err("RESOURCE_INFEASIBLE", "gpu role requires a gpu resource request")

    def _cleanup_on_failure(self) -> None:
        # Startup restoration may already have launched native invocations.
        # Their threads must finish before partial actor startup is unwound.
        for instance in self._instances:
            if instance is not None:
                instance.request_stop()
        if self._calls is not None:
            try:
                self._calls.close(timeout=min(30, self._services.bounds.timeout_ms / 1000))
            except BaseException:
                return  # Provider cleanup owns a worker that failed to drain.
        for instance in self._instances:
            if instance is None:
                continue
            try:
                _close_instance(instance)
            except BaseException:
                pass
        self._instances.clear()
        self._instance = None
        if self._recovery_rows is not None:
            try:
                self._recovery_rows.close()
            except BaseException:
                pass
        for port in self._ports.values():
            try:
                port.release()
            except BaseException:
                pass

    def accepts_finite_reference(self, port):
        return (self._spec.kind != 'StreamPrependOp' and port in self._ports
                and any(p.name == port and p.requirement == 'finite_complete' for p in self._spec.inputs))

    def adopt_finite_reference(self, port, entry):
        self._require_started()
        _check_stop(self._finite_services)
        if not self.accepts_finite_reference(port):
            raise _err('INVALID_CONTRACT', 'input port cannot borrow finite producer records')
        self._ports[port].adopt(entry)

    # -- K018 push ----------------------------------------------------------
    def push(self, port: str, record: Record) -> Iterator[Record]:
        self._require_started()
        _check_stop(self._services)
        kind = self._spec.kind
        if kind == 'StreamPrependOp':
            if port == 'prefix':
                if self._ports[port]._count >= self._spec.configuration['max_rows']:
                    raise _err('BUDGET_EXCEEDED', 'prepend exceeds max_rows')
                if record.encoded_bytes > self._spec.configuration['max_row_bytes']:
                    raise _err('BUDGET_EXCEEDED', 'prepend exceeds max_row_bytes')
                self._ports[port].push(record)
                return
            if port != 'main':
                raise _err('INVALID_CONTRACT', 'prepend has only prefix and main inputs')
            if not self._ports['prefix'].eoi:
                # Shared source branches must progress to the prefix's EOI.
                # Persist in the existing bounded port before returning an ACK;
                # its original snapshot restores this exact un-emitted prefix.
                self._ports['main'].push(record)
                self._ports['main']._flush()
                return
            yield from self._emit(iter((record,)))
            return
        if port in self._ports:
            self._ports[port].push(record)
            return iter(())
        if kind == "LimitOp" and self._emitted >= self._spec.configuration["limit"]:
            return
        if kind in _UNARY_KINDS or (kind in _MODEL_KINDS and kind not in _BATCH_INSTANCE_KINDS):
            if self._task_rows:
                record = self._task_record(record)
            if self._calls is None:
                yield from self._emit(preserve_block(invoke_unary(self._spec, record,
                    instance=self._slot_instance(0) if kind in _INSTANCE_KINDS else self._instance,
                    services=self._services, schemas=self._schemas, observe=self.observations.observe), record.block_partition))
            else:
                yield from self._emit(self._calls.submit(lambda slot: preserve_block(invoke_unary(self._spec, record,
                    instance=self._slot_instance(slot), services=self._services, schemas=self._schemas,
                    observe=self.observations.observe), record.block_partition), scope=self._call_scope, input=record))
        elif kind in _BATCH_INSTANCE_KINDS:
            yield from self._push_batch(record)
        elif kind in _STREAM_GROUP_KINDS:
            yield from self._emit(self._push_stream_group(record))
        elif kind == "RepartitionOp":
            yield from self._emit(iter((record,)))
        else:
            raise _err("INVALID_CONTRACT", "kind does not accept incremental push", kind=kind)

    def _task_record(self, record):
        # The first narrow operator numbers logical INPUT batches. Filters and
        # expansions forward that provenance, so later callbacks do not regroup
        # survivors or multiply the task count by the number of emitted rows.
        from ..dataflow_records import clone_record
        from ..contracts.dataflow_codec import _thaw_json
        if self._task_origin:
            ordinal = self._task_frontier // self._task_rows
            record = clone_record(record, source_position={**_thaw_json(record.source_position),
                'dataflow_task': {'group': self._task_group, 'ordinal': ordinal}})
        else:
            position = record.source_position.get('dataflow_task')
            ordinal = position.get('ordinal') if isinstance(position, Mapping) else None
            if not isinstance(position, Mapping) or set(position) != {'group', 'ordinal'} or position.get('group') != self._task_group or type(ordinal) is not int or ordinal < 0:
                raise _err('INVALID_CONTRACT', 'logical task provenance is absent or invalid')
        if self._task_ordinal is not None and ordinal < self._task_ordinal:
            raise _err('INVALID_CONTRACT', 'logical callback tasks arrived out of order')
        if self._task_ordinal is not None and ordinal != self._task_ordinal and self._shared_actor is None:
            _close_instance(self._instance)
            self._instances[0] = self._instance = None
        self._task_ordinal = ordinal
        self._task_frontier += 1
        return record

    def _push_batch(self, record: Record) -> Iterator[Record]:
        expected = self._batch_membership.expected(self._batch_ordinal) if self._batch_membership else None
        if self._batch and (self._batch[0].schema_ref != record.schema_ref
                            or self._batch[0].block_partition != record.block_partition):
            if expected is not None:
                raise _err('INCOMPATIBLE_HISTORY', 'Native replay batch schema or block changed')
            batch, self._batch = tuple(self._batch), []
            self._batch_deadline = None
            yield from self._invoke_batch(batch)
            expected = self._batch_membership.expected(self._batch_ordinal) if self._batch_membership else None
        if expected is not None:
            from ..dataflow_batch_membership import member
            index=len(self._batch)
            if index >= len(expected) or member(record,self._services.bounds) != expected[index]:
                raise _err('INCOMPATIBLE_HISTORY','Native replay batch changed original record membership or payload')
        if sum(r.encoded_bytes for r in self._batch) + record.encoded_bytes > self._services.bounds.max_batch_bytes:
            raise _err("BUDGET_EXCEEDED", "declared business batch exceeds max_batch_bytes")
        if not self._batch:
            interval=self._spec.configuration.get('flush_interval')
            self._batch_deadline=(self._services.clock_ms()+max(1,int(float(interval)*1000))) if interval else None
        self._batch.append(record)
        if len(self._batch) >= (len(expected) if expected is not None else self._max_batch):
            batch, self._batch = tuple(self._batch), []
            self._batch_deadline = None
            yield from self._invoke_batch(batch)

    def _invoke_batch(self, batch):
        if self._batch_membership is not None:
            self._batch_membership.bind(self._batch_ordinal,batch)
        self._batch_ordinal += 1
        if self._calls is None:
            yield from self._emit(preserve_block(invoke_batch(self._spec, batch,
                instance=self._slot_instance(0), services=self._services, schemas=self._schemas,
                observe=self.observations.observe, observe_dead=self.observations.dead_batch), batch[0].block_partition))
        else:
            yield from self._emit(self._calls.submit(lambda slot: preserve_block(invoke_batch(self._spec, batch,
                instance=self._slot_instance(slot), services=self._services, schemas=self._schemas,
                observe=self.observations.observe, observe_dead=self.observations.dead_batch), batch[0].block_partition), scope=self._call_scope, input=batch))

    def _group_limits(self):
        # Retained groups occupy the original batch/state grants; one emitted
        # row and its encoding use the original row grant. No extra allocator.
        bounds=self._services.bounds
        buffer=min(int(_cfg(self._spec,'buffer_bytes')),bounds.max_batch_bytes,
                   max(1,bounds.max_state_bytes//2))
        chunk=min(int(_cfg(self._spec,'chunk_bytes')),buffer)
        return chunk,buffer

    def _group_row_bytes(self,row):
        from ..stream_grouping import row_size
        chunk,_=self._group_limits()
        return row_size(row,chunk)

    def _push_stream_group(self, record: Record) -> Iterator[Record]:
        from .dataflow import _group_key, _materialize_row
        on=tuple(_cfg(self._spec,'on'))
        max_batch=int(self._spec.configuration.get('max_batch',self._max_batch))
        row=dict(_materialize_row(record,self._services))
        key=_group_key(row,on)
        size=self._group_row_bytes(row)
        chunk,buffer=self._group_limits()
        if key in self._groups and self._group_bytes[key]+size>chunk:
            yield from self._emit_group(key,last=False)
        while self._groups and self._group_buffer_bytes+size>buffer:
            yield from self._flush_oldest_group()
        if key not in self._groups:
            max_groups=int(_cfg(self._spec,'max_groups'))
            if len(self._groups)>=max_groups:
                yield from self._flush_oldest_group()
            self._groups[key]=[]
            self._group_bytes[key]=0
            self._group_base[key]={field:row[field] for field in on}
        self._groups[key].append(row)
        self._group_bytes[key]+=size
        self._group_buffer_bytes+=size
        self._reset_timer()
        if len(self._groups[key])>=max_batch:
            yield from self._emit_group(key,last=False)
        yield from self._flush_expired_groups()

    def _reset_timer(self) -> None:
        interval = self._spec.configuration.get("flush_interval")
        if interval is not None and self._timer_deadline is None:
            self._timer_deadline = self._services.clock_ms() + max(1,int(float(interval) * 1000))

    def _flush_expired_groups(self) -> Iterator[Record]:
        if self._timer_deadline is None:
            return
        if self._services.clock_ms() >= self._timer_deadline:
            for key in list(self._groups):
                yield from self._emit_group(key, last=False)
            self._timer_deadline = None

    def _flush_oldest_group(self) -> Iterator[Record]:
        key = next(iter(self._groups))
        yield from self._emit_group(key, last=False)

    def _emit_group(self, key: str, *, last: bool) -> Iterator[Record]:
        from .dataflow import _output_record, _OutputBudget

        batch = self._groups.pop(key, [])
        base = self._group_base.pop(key, {})
        self._group_buffer_bytes -= self._group_bytes.pop(key,0)
        if not self._groups:
            self._timer_deadline = None
        if not batch:
            return
        output = self._spec.configuration.get("output", "items")
        # An online microbatch cannot assert a final complete group.
        row = {**base, output: batch}
        record_id = _hash_id(["dataflow-record-v3", self._occurrence, "stream-group", self._emitted])
        out = _output_record(record_id, {"group_key": key}, row, self._emitted, self._services)
        _OutputBudget(self._spec, self._services, streaming=True).charge(out)
        yield out

    # -- K019 finish --------------------------------------------------------
    def finish(self, port: str, *, wait_for_calls: bool = True) -> Iterator[Record]:
        from .recovery_rows import hook_owner, RecoveryRows
        if self._recovery_rows is None and hook_owner(self) is not None:
            self._recovery_rows = RecoveryRows(self)
        if self._recovery_rows is not None:
            yield from self._recovery_rows.finish(port)
        else:
            yield from self._finish_normal(port, wait_for_calls=wait_for_calls)

    def _finish_normal(self, port: str, *, wait_for_calls: bool = True) -> Iterator[Record]:
        self._require_started()
        _check_stop(self._services)
        kind = self._spec.kind
        if kind == 'StreamPrependOp':
            if port == 'prefix':
                if self._ports[port].eoi:
                    return
                self._ports[port].finish()
                yield from self._emit(self._ports[port].records())
                yield from self._emit(self._ports['main'].records())
                self._ports['main'].release()
            elif port == 'main':
                self._ports['main'].finish()
            return
        if port in self._ports:
            self._ports[port].finish()
            if not all(p.eoi for p in self._ports.values()):
                return iter(())
            if self._finish_cursor is None:
                self._finish_cursor = {'port': port, 'emitted': 0}
            elif self._finish_cursor['port'] != port:
                raise _err('INCOMPATIBLE_HISTORY', 'finite finish resumed on a different input port')
            yield from self._finish_complete()
            if self._skip:
                raise _err('INCOMPATIBLE_HISTORY', 'finite replay ended before the committed output frontier')
            self._finish_cursor = None
            return
        if kind in _BATCH_INSTANCE_KINDS:
            if self._batch:
                batch = tuple(self._batch)
                self._batch = []
                self._batch_deadline = None
                yield from self._invoke_batch(batch)
        elif kind in _STREAM_GROUP_KINDS:
            for key in list(self._groups):
                yield from self._emit(self._emit_group(key, last=True))
            self._timer_deadline = None
        if self._calls is not None:
            # The graph runner must keep polling downstream timers while an
            # upstream invocation drains. Direct kernel callers retain the
            # synchronous finish contract; the runner records the port EOI
            # and finishes the node only after pending_calls becomes false.
            outputs = self._calls.finish() if wait_for_calls else self._calls.poll(max_records=1)
            yield from self._emit(outputs)
        if kind == 'SaveLanceOp':
            # Empty native output still publishes its declared empty table at
            # real EOI; this creates only the passive driver RPC proxy.
            instance = self._slot_instance(0)
            instance.finish(instance.native_call_ordinal)

    @property
    def retained_resident_bytes(self) -> int:
        """Yield-suspended peers, excluding the current output Record itself.

        Reducer initial/current state and live merge/next-group windows are
        measured under the original grant. This is not a budget authority or
        checkpoint field; the frame codec adds it to its own output allocation.
        """
        return self._reducer_retained_bytes + self._shuffle_retained_bytes

    def _retain_reducer_peers(self, size: int) -> None:
        self._reducer_retained_bytes = size

    def _retain_shuffle_peers(self, size: int) -> None:
        self._shuffle_retained_bytes = size

    def _finish_complete(self) -> Iterator[Record]:
        # This view stops pure reads/merges. Callable/actor construction and
        # its NativeRunView still use self._services for admitted-effect drain.
        services = self._finite_services
        _check_stop(services)
        region = getattr(self, '_relational_region', None)
        if region is not None:
            if self._occurrence != region.plan.exit_occurrence:
                raise _err('INVALID_CONTRACT', 'internal region member cannot execute independently')
            yield from self._emit(region.records())
            return
        kind = self._spec.kind
        if kind in ("join", "exclude_keys"):
            from .relational import join_partitions

            left = self._ports[self._port_order[0]].records()
            right = self._ports[self._port_order[1]].records()
            yield from self._emit(preserve_block(join_partitions(left, right, spec=self._spec,
                services=services, retain=self._retain_reducer_peers), None))
        elif kind == "reduce":
            from .relational import reduce_partition

            yield from self._emit(reduce_partition(self._ports[self._port_order[0]].records(), spec=self._spec,
                services=services,retain=self._retain_reducer_peers))
        elif kind == "groups":
            from .relational import emit_groups

            yield from self._emit(emit_groups(self._ports[self._port_order[0]].records(), spec=self._spec, services=services))
        elif kind == "union":
            from .relational import union_inputs

            inputs = tuple(self._ports[name].records() for name in self._port_order)
            yield from self._emit(preserve_block(union_inputs(inputs, spec=self._spec, services=services), None))
        elif kind in _ORDERING_KINDS:
            from .ordering import build_sort_runs, merge_sort_runs
            from .block_partition import partition_records, block_count
            from dataclasses import replace
            port = self._ports[self._port_order[0]]
            records = port.records()
            shuffle_iterator = None
            layout = None
            if kind == 'RandomizeBlockOrderOp':
                from .block_partition import shuffled_block_layout
                layout = shuffled_block_layout(port.records(), spec=self._spec, services=services)
            if kind == 'RandomShuffleOp' and 'shuffle_order_version' in self._spec.configuration:
                from .shuffle_order import seeded_shuffle, shuffle_windows, input_records
                windows = shuffle_windows(services)
                records.close()
                records, windows = input_records(port, services, windows)
                shuffle_iterator = seeded_shuffle(records, spec=self._spec, services=services,
                    windows=windows, retain=self._retain_shuffle_peers)
                records = shuffle_iterator
            elif kind != 'RepartitionOp' or self._spec.configuration.get('keys'):
                sort_spec = self._spec
                if kind == 'RepartitionOp':
                    sort_spec = replace(sort_spec, configuration={**dict(sort_spec.configuration),
                        'keys': list(sort_spec.configuration.get('keys') or ()),
                        'num_blocks': block_count(sort_spec, rows=port._count, bounds=self._services.bounds)})
                runs = build_sort_runs(records, spec=sort_spec, services=services)
                records = merge_sort_runs(runs, spec=sort_spec, services=services)
            if kind in ('RepartitionOp', 'RandomShuffleOp'):
                records = partition_records(records, spec=self._spec, rows=port._count, services=services)
            elif layout is not None:
                from .block_partition import reordered_records
                records = reordered_records(records, layout=layout, spec=self._spec)
            else:
                records = preserve_block(records, None)
            try:
                yield from self._emit(records)
            finally:
                if shuffle_iterator is not None:
                    try:
                        records.close()
                    finally:
                        shuffle_iterator.close()
        elif kind in _AGGREGATE_KINDS:
            from .aggregate import accumulate_partial

            records = self._ports[self._port_order[0]].records()
            self.aggregate_state = accumulate_partial(records, spec=self._spec, services=services)
        else:
            raise _err("INVALID_CONTRACT", "kind has no finite-complete finish", kind=kind)

    def _emit(self, records: Iterator[Record]) -> Iterator[Record]:
        if self._recovery_buffering:
            yield from records
            return
        for record in records:
            if self._skip > 0:
                self._skip -= 1
                continue
            self._emitted += 1
            if self._finish_cursor is not None:
                self._finish_cursor['emitted'] += 1
            self.observations.hold_output(record.encoded_bytes)
            try:
                yield record
            finally:
                self.observations.release_output()

    # -- K020 snapshot ------------------------------------------------------
    def snapshot(self) -> ArtifactRef | None:
        raw = self._snapshot_bytes()
        return self._services.store.put(iter([raw]), max_bytes=len(raw), media_type=IMAGE_MEDIA_TYPE)

    def _pack_snapshot_ready(self):
        """Only fixed pure metadata can be encoded twice without user hooks."""
        if (self._spec.kind not in {'MapOp', 'FlatMapOp', 'BoundMapOp', 'FilterOp', 'AddColumnOp'}
                or self._spec.state != 'none' or self._spec.effect != 'pure'
                or self._ports or self._batch or self._groups or self._group_base
                or self._calls is not None or self._worker_scope or self._finish_cursor is not None
                or self.aggregate_state is not None or self._recovery_rows is not None
                or isinstance(self._instance, AsyncInstance) and self._instance.loop is not None
                or self._batch_deadline is not None or self._timer_deadline is not None):
            return False
        # These platform counters and bounded miss labels give the encoder a
        # conservative fixed workspace. Decline packing before constructing an
        # image if its metadata lies outside that envelope; ordinary snapshot
        # still applies its original contract.
        observed = self.observations.snapshot()
        counts = [self._emitted, self._batch_ordinal, self._task_rows or 0,
                  self._task_frontier, self._task_ordinal or 0,
                  self._instance.native_call_ordinal if isinstance(self._instance, AsyncInstance) else 0]
        counts.extend(observed[k] for k in ('count', 'total_ns', 'min_ns', 'max_ns'))
        counts.extend(observed['miss'].values())
        return (type(self._occurrence) is str and 1 <= len(self._occurrence) <= 64
                and type(self._partition_id) is str and 1 <= len(self._partition_id) <= 128
                and all(type(n) is int and 0 <= n < 2**64 for n in counts)
                and len(observed['miss']) <= 33
                and all(type(k) is str and len(k) <= 128 for k in observed['miss']))

    def _snapshot_bytes(self):
        self._require_started()
        if not self.checkpoint_ready:
            raise _err('INVALID_CONTRACT', 'Checkpoint requires drained non-native invocations')
        worker_states = [self._snapshot_state(instance) if instance is not None else None for instance in self._instances] if self._worker_scope else None
        completed_task = bool(self._shared_actor is None and self._task_rows and self._task_origin and self._task_frontier and self._task_frontier % self._task_rows == 0)
        image = {
            "schema_version": IMAGE_VERSION,
            "kind": self._spec.kind,
            "occurrence_id": self._occurrence,
            "partition_id": self._partition_id,
            "config_digest": _hash_id([_canonical(self._spec.configuration)]),
            "state": None if completed_task else (worker_states[0] if worker_states else self._snapshot_state()),
            "ports": {name: port.snapshot() for name, port in self._ports.items()},
            "batch": list(self._batch),
            "batch_ordinal": self._batch_ordinal,
            "batch_remaining_ms": (max(0,self._batch_deadline-self._services.clock_ms())
                if self._batch_deadline is not None else None),
            "groups": {key: list(value) for key, value in self._groups.items()},
            "group_base": {key: dict(value) for key, value in self._group_base.items()},
            "timer_deadline": self._timer_deadline,
            "output_cursor": self._emitted,
            "call_ordinal": self._instance.native_call_ordinal if isinstance(self._instance, AsyncInstance) else 0,
            "worker_states": worker_states,
            "worker_calls": [instance.native_call_ordinal if instance is not None else 0 for instance in self._instances] if self._worker_scope else None,
            "call_slots": list(self._calls.slots) if self._calls is not None else None,
            "inflight_calls": self._calls.snapshot() if self._calls is not None else [],
            "aggregate_state": self.aggregate_state,
        }
        region = getattr(self, '_relational_region', None)
        if region is not None and self._occurrence == region.plan.exit_occurrence:
            image['relational_region'] = region.snapshot()
        if self._recovery_rows is not None:
            image['recovery_rows'] = self._recovery_rows.snapshot()
        if self._finish_cursor is not None:
            image['finish_cursor'] = dict(self._finish_cursor)
        if self._spec.kind in _STREAM_GROUP_KINDS:
            image['group_remaining_ms']=(max(0,self._timer_deadline-self._services.clock_ms())
                if self._timer_deadline is not None else None)
            image['timer_deadline']=None  # Process monotonic time is not portable history.
        if self._task_rows:
            image['task'] = {'batch_rows': self._task_rows, 'input_frontier': self._task_frontier,
                             'ordinal': self._task_ordinal, 'origin': self._task_origin}
        observed = self.observations.snapshot()
        if observed['count']:
            image['callback_observations'] = observed
        raw = state_codec.dumps(image, max_bytes=self._services.bounds.max_state_bytes)
        limit = self._services.bounds.max_state_bytes
        if limit > 0 and len(raw) > limit:
            raise _err("BUDGET_EXCEEDED", "kernel recovery image exceeds max_state_bytes")
        return raw

    def _state_owner(self, instance=None, *, retain_partial=False):
        import functools
        import inspect
        instance = instance if instance is not None else self._instance
        owner = instance.target if isinstance(instance, AsyncInstance) else instance
        while True:
            if isinstance(owner, _BoundCallable):
                owner = owner._instance
            elif isinstance(owner, _FieldCallable):
                owner = owner._fn
            elif isinstance(owner, functools.partial):
                if retain_partial:
                    return owner
                owner = owner.func
            elif inspect.ismethod(owner) or inspect.ismethodwrapper(owner):
                owner = owner.__self__
            else:
                return owner

    def _function_slots(self, instance):
        """Exact platform wrappers retain the explicit call binding state owner."""
        target = instance.target if isinstance(instance, AsyncInstance) else instance
        if isinstance(target, _BoundCallable):
            return target._instance, target._args, target._kwargs
        if isinstance(target, _FieldCallable):
            declaration = target._operation.callable
            return target._fn, declaration.call_args, declaration.call_kwargs
        return self._state_owner(instance, retain_partial=True), (), {}

    def _snapshot_state(self, instance=None) -> Any:
        if self._shared_actor is not None and self._occurrence != self._shared_actor.binding['state_owner']:
            return {'mode': 'shared_owner', 'owner': self._shared_actor.binding['state_owner']}
        instance = instance if instance is not None else self._instance
        if instance is None or self._spec.state == "none":
            return None
        owner = self._state_owner(instance)
        from ..operator_policy import classify_operator_state, snapshot_auto_state
        policy = classify_operator_state(owner, phase="instance")
        snapshot = getattr(owner, "__demiflow_snapshot__", None)
        if snapshot is not None:
            return {"mode": "adapter", "value": instance.resolve(snapshot())}
        if policy.mode == "function":
            from .task_scope import function_residency_kind, snapshot_function, snapshot_resident_function
            target, args, kwargs = self._function_slots(instance)
            kind = function_residency_kind(target, call_args=args, call_kwargs=kwargs)
            bound_state = any(type(value) in (list, dict) for value in (*args, *kwargs.values()))
            if kind != 'nonrecoverable' and self._task_rows and target is owner and not bound_state:
                state = snapshot_function(owner, max_bytes=self._services.bounds.max_state_bytes)
                if state['mode'] == 'task_function':
                    return state  # Preserve every already-readable legacy task image.
            state = snapshot_resident_function(target, call_args=args, call_kwargs=kwargs,
                                               max_bytes=self._services.bounds.max_state_bytes)
            if state['mode'] == 'nonrecoverable':
                self._services.journal.restrict_recovery(self._occurrence)
            return state
        if policy.mode == "hooks":
            return {"mode": "hooks", "value": instance.resolve(owner.snapshot_state())}
        if policy.mode == "auto_json":
            return {"mode": "plain_json", "value": snapshot_auto_state(
                owner, max_bytes=self._services.bounds.max_state_bytes)}
        # Runtime resources are allowed during the current attempt; the image
        # explicitly forbids restoring a lost cross-row state. No fields vanish.
        return {"mode": "nonrecoverable", "reason": policy.reason or "state adapter missing"}

    # -- K021 restore -------------------------------------------------------
    def restore(self, ref: ArtifactRef | None) -> None:
        if self._closed:
            raise _err("INVALID_CONTRACT", "cannot restore a closed kernel")
        if ref is None:
            if self._spec.state != "none":
                raise _err("INCOMPATIBLE_HISTORY", "stateful kernel cannot restore from an empty ref")
            return
        limit = self._services.bounds.max_state_bytes
        from ..dataflow_kernel_pack import read_kernel_image
        data = read_kernel_image(self._services.store, ref, self._services.bounds,
                                 occurrence=self._occurrence, partition=self._partition_id)
        image = state_codec.loads(data, max_bytes=limit)
        if image.get("schema_version") != IMAGE_VERSION:
            raise _err("INCOMPATIBLE_HISTORY", "kernel image schema mismatch")
        if image.get("kind") != self._spec.kind or image.get("occurrence_id") != self._occurrence:
            raise _err("INCOMPATIBLE_HISTORY", "kernel identity mismatch on restore")
        if image.get("partition_id") != self._partition_id:
            raise _err("INCOMPATIBLE_HISTORY", "partition/route changed on restore")
        if image.get("config_digest") != _hash_id([_canonical(self._spec.configuration)]):
            raise _err("INCOMPATIBLE_HISTORY", "kernel configuration changed on restore")
        self._image = image
        self._restored = True
        if self._started:
            self._apply_image(image)

    def _apply_image(self, image: Mapping[str, Any]) -> None:
        if 'recovery_rows' in image:
            from .recovery_rows import RecoveryRows
            self._recovery_rows = RecoveryRows(self)
            self._recovery_rows.restore(image['recovery_rows'])
        if 'callback_observations' in image:
            self.observations.restore(image['callback_observations'])
        if self._task_rows:
            task = image.get('task')
            if not isinstance(task, Mapping) or set(task) != {'batch_rows', 'input_frontier', 'ordinal', 'origin'}:
                raise _err('INCOMPATIBLE_HISTORY', 'logical task frontier is absent')
            frontier = task['input_frontier']
            ordinal = task['ordinal']
            if (type(frontier) is not int or frontier < 0 or type(task['batch_rows']) is not int or task['batch_rows'] != self._task_rows
                    or type(task['origin']) is not bool or task['origin'] != self._task_origin
                    or (frontier == 0 and ordinal is not None)
                    or (frontier > 0 and (type(ordinal) is not int or ordinal < 0))
                    or (self._task_origin and frontier > 0 and ordinal != (frontier - 1) // self._task_rows)):
                raise _err('CORRUPT_ARTIFACT', 'invalid logical task frontier')
            self._task_frontier = frontier
            self._task_ordinal = ordinal
        for name, snap in image["ports"].items():
            if name in self._ports:
                self._ports[name].restore(snap)
        self._batch = list(image["batch"])
        self._batch_ordinal = image.get('batch_ordinal',image.get('call_ordinal',0))
        if type(self._batch_ordinal) is not int or self._batch_ordinal < 0:
            raise _err('CORRUPT_ARTIFACT','Invalid native batch cursor')
        remaining=image.get('batch_remaining_ms')
        interval=self._spec.configuration.get('flush_interval')
        maximum=max(1,int(float(interval)*1000)) if interval else None
        if self._batch and maximum is not None and remaining is None:
            raise _err('INCOMPATIBLE_HISTORY','Pending timed batch has no original relative timer evidence')
        if remaining is not None and (type(remaining) is not int or maximum is None or not 0<=remaining<=maximum):
            raise _err('CORRUPT_ARTIFACT','Invalid relative batch timer')
        self._batch_deadline=(self._services.clock_ms()+remaining) if remaining is not None else None
        self._groups = {key: list(value) for key, value in image["groups"].items()}
        self._group_base = {key: dict(value) for key, value in image["group_base"].items()}
        self._timer_deadline = image["timer_deadline"]
        if self._spec.kind in _STREAM_GROUP_KINDS:
            remaining=image.get('group_remaining_ms')
            maximum=max(1,int(float(_cfg(self._spec,'flush_interval'))*1000))
            if self._groups and remaining is None:
                raise _err('INCOMPATIBLE_HISTORY','Pending stream groups have no portable timer evidence')
            if remaining is not None and (type(remaining) is not int or not self._groups or not 0<=remaining<=maximum):
                raise _err('CORRUPT_ARTIFACT','Invalid relative stream group timer')
            if set(self._groups)!=set(self._group_base):
                raise _err('CORRUPT_ARTIFACT','Stream group base differs from pending groups')
            self._timer_deadline=self._services.clock_ms()+remaining if remaining is not None else None
            self._group_bytes={key:sum(self._group_row_bytes(row) for row in rows) for key,rows in self._groups.items()}
            self._group_buffer_bytes=sum(self._group_bytes.values())
            chunk,buffer=self._group_limits()
            if (self._group_buffer_bytes>buffer or any(v>chunk for v in self._group_bytes.values())
                    or len(self._groups)>int(_cfg(self._spec,'max_groups'))):
                raise _err('CORRUPT_ARTIFACT','Recovered stream groups exceed original buffer grants')
        self._emitted = int(image["output_cursor"])
        if not self._worker_scope and (image["state"] is not None or image.get('call_ordinal', 0)):
            self._slot_instance(0)
        if isinstance(self._instance, AsyncInstance):
            ordinal = image.get('call_ordinal', 0)
            if type(ordinal) is not int or ordinal < 0:
                raise _err('CORRUPT_ARTIFACT', 'Invalid native invocation cursor')
            self._instance.native_call_ordinal = ordinal
        self._skip = 0  # incremental inputs resume after the committed prefix
        self._finish_cursor = None
        cursor = image.get('finish_cursor')
        if cursor is not None:
            if (not self.finish_replayable or type(cursor) is not dict
                    or set(cursor) != {'port', 'emitted'} or cursor['port'] not in self._ports
                    or type(cursor['emitted']) is not int or not 0 <= cursor['emitted'] <= self._emitted
                    or not all(port.eoi for port in self._ports.values())):
                raise _err('INCOMPATIBLE_HISTORY', 'invalid finite output recovery frontier')
            self._finish_cursor = dict(cursor)
            self._skip = cursor['emitted']
        self.aggregate_state = image["aggregate_state"]
        if not self._worker_scope:
            self._restore_state(image["state"])
        if self._worker_scope:
            states, calls = image.get('worker_states'), image.get('worker_calls')
            if not isinstance(states, list) or not isinstance(calls, list) or len(states) != len(self._instances) or len(calls) != len(states):
                raise _err('INCOMPATIBLE_HISTORY', 'Worker callable slots differ from checkpoint')
            for slot, (state, ordinal) in enumerate(zip(states, calls)):
                if type(ordinal) is not int or ordinal < 0:
                    raise _err('CORRUPT_ARTIFACT', 'Invalid worker invocation cursor')
                if state is not None or ordinal or self._instances[slot] is not None:
                    instance = self._slot_instance(slot)
                    self._restore_state(state, instance)
                    instance.native_call_ordinal = ordinal

        inflight = image.get('inflight_calls', [])
        if inflight and not self.can_snapshot_pending:
            raise _err('INCOMPATIBLE_HISTORY', 'Pending calls need row-local native recovery')
        if self._calls is not None:
            def compute(slot, input):
                instance = self._slot_instance(slot)
                invoke = invoke_batch if type(input) is tuple else invoke_unary
                options = {'observe': self.observations.observe}
                if type(input) is tuple:
                    options['observe_dead'] = self.observations.dead_batch
                return invoke(self._spec, input, instance=instance, services=self._services, schemas=self._schemas, **options)
            self._calls.restore(inflight, slots=image.get('call_slots'), compute=compute)
        elif inflight:
            raise _err('INCOMPATIBLE_HISTORY', 'Pending calls have no invocation slots')

    def _restore_state(self, state: Any, instance=None) -> None:
        if self._shared_actor is not None and not self._shared_actor.restoring(self._occurrence, state):
            return
        if state is None:
            return
        mode = state["mode"]
        instance = instance if instance is not None else self._instance
        owner = self._state_owner(instance)
        if mode == "nonrecoverable":
            raise _err("INCOMPATIBLE_HISTORY", "actor state cannot resume: " + state["reason"])
        if mode == "plain_json":
            from ..operator_policy import restore_auto_state
            restore_auto_state(owner, state["value"], max_bytes=self._services.bounds.max_state_bytes)
        elif mode == 'function_slots_v1':
            from .task_scope import restore_resident_function
            target, args, kwargs = self._function_slots(instance)
            restore_resident_function(target, state, call_args=args, call_kwargs=kwargs,
                                      max_bytes=self._services.bounds.max_state_bytes)
        elif mode == 'task_function' and self._task_rows:
            from .task_scope import restore_function
            restore_function(owner, state, max_bytes=self._services.bounds.max_state_bytes)
        elif mode in ("adapter", "hooks", "auto_json"):
            name = "restore_state" if mode == "hooks" else "__demiflow_restore__"
            restore = getattr(owner, name, None)
            if restore is None:
                raise _err("INCOMPATIBLE_HISTORY", "actor restore adapter is absent")
            instance.resolve(restore(state["value"]))
        else:
            raise _err("INCOMPATIBLE_HISTORY", "unknown actor state image mode")

    def request_stop(self, reason=None):
        for instance in self._instances:
            if instance is not None:
                instance.request_stop(reason)

    # -- K022 close ---------------------------------------------------------
    def close(self) -> None:
        self._reducer_retained_bytes = 0
        self._shuffle_retained_bytes = 0
        if self._closed:
            return
        for instance in self._instances:
            if instance is not None:
                instance.request_stop()
        if self._calls is not None:
            self._calls.close(timeout=min(30, self._services.bounds.timeout_ms / 1000))
        self._closed = True
        errors = []
        for instance in self._instances:
            if instance is None:
                continue
            try:
                _close_instance(instance)
            except BaseException as exc:
                errors.append(exc)
        if not errors:
            from ..dataflow_model_observations import worker_model_observations
            try:
                worker_model_observations(self, final=True)
            except BaseException as exc:
                errors.append(exc)
        self._instances.clear()
        self._instance = None
        if self._recovery_rows is not None:
            try:
                self._recovery_rows.close()
            except BaseException as exc:
                errors.append(exc)
        for port in self._ports.values():
            try:
                port.release()
            except BaseException as exc:
                errors.append(exc)
        for ref in self._spool_owner_refs:
            try:
                self._services.store.release(ref, owner=_SPOOL_OWNER)
            except BaseException as exc:
                errors.append(exc)
        self._spool_owner_refs = []
        if len(errors) == 1:
            raise errors[0]
        if errors:
            raise BaseExceptionGroup("operator cleanup failed", errors)

    def _require_started(self) -> None:
        if self._closed:
            raise _err("INVALID_CONTRACT", "kernel is closed")
        if not self._started:
            raise _err("INVALID_CONTRACT", "kernel must start before use")
