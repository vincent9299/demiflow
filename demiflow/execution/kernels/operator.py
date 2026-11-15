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
_INSTANCE_KINDS = _UNARY_KINDS | _BATCH_KINDS | _MODEL_KINDS | _STREAM_GROUP_KINDS
_BATCH_INSTANCE_KINDS = _BATCH_KINDS | {"EmbeddingMapOp", "RegisterDocumentsBatchOp", "SaveLanceOp"}


def _segment_rows(services: RuntimeServices) -> int:
    bounds = services.bounds
    return min(8192, max(1, bounds.max_batch_bytes // max(1, bounds.max_row_bytes)))


def _resolve_instance(spec: KernelSpec, services: RuntimeServices) -> Any:
    key = spec.configuration.get("program")
    if key is None and spec.implementation_ref is not None:
        key = spec.implementation_ref.key
    if key is None:
        raise _err("INVALID_CONTRACT", "callable kernel requires a staged program")
    program = services.programs.get(key)
    if program is None or program.kind not in ("callable", "native"):
        raise _err("INVALID_CONTRACT", "callable program not staged", program=str(key))
    from demiflow.data.plan import CallableSpec
    declaration = program.value
    if isinstance(declaration, CallableSpec):
        instance = declaration.instantiate()
        if declaration.call_args or declaration.call_kwargs:
            return _BoundCallable(instance, declaration.call_args, dict(declaration.call_kwargs))
        return instance
    return declaration


class _BoundCallable:
    """Keep lifecycle hooks on the actor when binding call-time arguments."""
    def __init__(self, instance, args, kwargs):
        self._instance, self._args, self._kwargs = instance, args, kwargs

    def __call__(self, value):
        return self._instance(value, *self._args, **self._kwargs)

    def __getattr__(self, name):
        return getattr(self._instance, name)


def _run_astart(instance: Any) -> None:
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
        self._pending: list[Record] = []
        self._eoi = False
        self._count = 0
        self._bytes = 0

    @property
    def eoi(self) -> bool:
        return self._eoi

    def push(self, record: Record) -> None:
        if self._eoi:
            raise _err("INVALID_CONTRACT", "push after port EOI", port=self.name)
        self._pending.append(record)
        self._count += 1
        if len(self._pending) >= self._segment_rows:
            self._flush()

    def _flush(self) -> None:
        if not self._pending:
            return
        raw = pickle.dumps(self._pending, protocol=pickle.HIGHEST_PROTOCOL)
        if self._bytes + len(raw) > self._services.bounds.max_spill_bytes:
            raise _err("BUDGET_EXCEEDED", "input spool exceeds max_spill_bytes", port=self.name)
        ref = self._services.store.put(iter([raw]), max_bytes=len(raw), media_type=RECORD_MEDIA_TYPE)
        self._refs.append(ref)
        self._bytes += len(raw)
        self._pending = []

    def finish(self) -> None:
        self._flush()
        self._eoi = True

    def records(self) -> Iterator[Record]:
        for ref in self._refs:
            data = b"".join(self._services.store.read(ref, max_bytes=ref.size_bytes))
            yield from pickle.loads(data)
        yield from self._pending

    def release(self) -> None:
        for ref in self._refs:
            self._services.store.release(ref, owner=_SPOOL_OWNER)
        self._refs = []
        self._pending = []

    def snapshot(self) -> dict[str, Any]:
        return {
            "refs": list(self._refs), "pending": list(self._pending),
            "eoi": self._eoi, "count": self._count, "bytes": self._bytes,
        }

    def restore(self, snap: Mapping[str, Any]) -> None:
        self._refs = list(snap["refs"])
        self._pending = list(snap["pending"])
        self._eoi = bool(snap["eoi"])
        self._count = int(snap["count"])
        self._bytes = int(snap["bytes"])


class OperatorKernel:
    """K016: the only concrete Kernel; construction performs no execution."""

    def __init__(self, spec: KernelSpec, *, services: RuntimeServices, partition_id: str) -> None:
        if spec.kind not in KERNEL_KINDS:
            raise _err("INVALID_CONTRACT", "unknown kernel kind", kind=spec.kind)
        self._spec = spec
        self._services = services
        self._partition_id = partition_id
        self._occurrence = _occurrence(spec)
        self._instance: Any = None
        self._started = False
        self._closed = False
        self._restored = False
        self._image: dict[str, Any] | None = None
        self._segment_rows = _segment_rows(services)
        self._ports: dict[str, _SpoolPort] = {
            port.name: _SpoolPort(port.name, services, self._segment_rows)
            for port in spec.inputs
            if port.requirement == "finite_complete"
        }
        self._port_order = tuple(port.name for port in spec.inputs)
        self._batch: list[Record] = []
        self._max_batch = int(spec.configuration.get("max_batch", self._segment_rows))
        self._groups: dict[str, list[dict[str, Any]]] = {}
        self._group_base: dict[str, dict[str, Any]] = {}
        self._timer_deadline: int | None = None
        self._emitted = 0
        self._skip = 0
        self.aggregate_state: AggregateState | None = None
        self._spool_owner_refs: list[ArtifactRef] = []

    # -- K017 start ---------------------------------------------------------
    def start(self) -> None:
        if self._closed:
            raise _err("INVALID_CONTRACT", "cannot start a closed kernel")
        if self._started:
            return
        self._validate_profile()
        try:
            if self._spec.kind in _INSTANCE_KINDS:
                self._instance = _resolve_instance(self._spec, self._services)
                _run_astart(self._instance)
            if self._image is not None:
                self._apply_image(self._image)
            self._started = True
        except BaseException:
            self._cleanup_on_failure()
            raise

    def _validate_profile(self) -> None:
        profile = self._spec.profile
        requests = getattr(profile, "requests", None)
        if not isinstance(requests, Mapping):
            raise _err("INVALID_CONTRACT", "kernel profile must declare requests")
        role = self._spec.resource_role
        if role == "gpu" and not any("gpu" in str(key).lower() for key in requests):
            raise _err("RESOURCE_INFEASIBLE", "gpu role requires a gpu resource request")

    def _cleanup_on_failure(self) -> None:
        try:
            _close_instance(self._instance)
        except BaseException:
            pass
        self._instance = None
        for port in self._ports.values():
            try:
                port.release()
            except BaseException:
                pass

    # -- K018 push ----------------------------------------------------------
    def push(self, port: str, record: Record) -> Iterator[Record]:
        self._require_started()
        _check_stop(self._services)
        kind = self._spec.kind
        if port in self._ports:
            self._ports[port].push(record)
            return iter(())
        if kind in _UNARY_KINDS or (kind in _MODEL_KINDS and kind not in _BATCH_INSTANCE_KINDS):
            yield from self._emit(invoke_unary(self._spec, record, instance=self._instance, services=self._services))
        elif kind in _BATCH_INSTANCE_KINDS:
            yield from self._push_batch(record)
        elif kind in _STREAM_GROUP_KINDS:
            yield from self._push_stream_group(record)
        elif kind == "RepartitionOp":
            yield from self._emit(iter((record,)))
        else:
            raise _err("INVALID_CONTRACT", "kind does not accept incremental push", kind=kind)

    def _push_batch(self, record: Record) -> Iterator[Record]:
        self._batch.append(record)
        if len(self._batch) >= self._max_batch:
            batch = tuple(self._batch)
            self._batch = []
            yield from self._emit(invoke_batch(self._spec, batch, instance=self._instance, services=self._services))

    def _push_stream_group(self, record: Record) -> Iterator[Record]:
        from .dataflow import _group_key, _materialize_row

        on = tuple(_cfg(self._spec, "on"))
        max_batch = int(self._spec.configuration.get("max_batch", self._max_batch))
        row = _materialize_row(record, self._services)
        key = _group_key(row, on)
        if key not in self._groups:
            max_groups = int(self._spec.configuration.get("max_groups", 0))
            if max_groups > 0 and len(self._groups) >= max_groups:
                yield from self._flush_oldest_group()
            self._groups[key] = []
            self._group_base[key] = {field: row[field] for field in on}
        self._groups[key].append(dict(row))
        self._reset_timer()
        if len(self._groups[key]) >= max_batch:
            yield from self._emit_group(key, last=False)
        yield from self._flush_expired_groups()

    def _reset_timer(self) -> None:
        interval = self._spec.configuration.get("flush_interval")
        if interval is not None:
            self._timer_deadline = self._services.clock_ms() + int(float(interval) * 1000)

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
        if not batch:
            return
        output = self._spec.configuration.get("output", "items")
        row = {**base, output: batch, "group_index": 0, "group_last": last}
        record_id = _hash_id(["dataflow-record-v3", self._occurrence, "stream-group", self._emitted])
        out = _output_record(record_id, {"group_key": key}, row, self._emitted, self._services)
        _OutputBudget(self._spec, self._services).charge(out)
        yield out

    # -- K019 finish --------------------------------------------------------
    def finish(self, port: str) -> Iterator[Record]:
        self._require_started()
        _check_stop(self._services)
        kind = self._spec.kind
        if port in self._ports:
            self._ports[port].finish()
            if not all(p.eoi for p in self._ports.values()):
                return iter(())
            yield from self._finish_complete()
            return
        if kind in _BATCH_INSTANCE_KINDS:
            if self._batch:
                batch = tuple(self._batch)
                self._batch = []
                yield from self._emit(invoke_batch(self._spec, batch, instance=self._instance, services=self._services))
        elif kind in _STREAM_GROUP_KINDS:
            for key in list(self._groups):
                yield from self._emit(self._emit_group(key, last=True))
            self._timer_deadline = None

    def _finish_complete(self) -> Iterator[Record]:
        kind = self._spec.kind
        if kind in ("join", "exclude_keys"):
            from .relational import join_partitions

            left = self._ports[self._port_order[0]].records()
            right = self._ports[self._port_order[1]].records()
            yield from self._emit(join_partitions(left, right, spec=self._spec, services=self._services))
        elif kind == "reduce":
            from .relational import reduce_partition

            yield from self._emit(reduce_partition(self._ports[self._port_order[0]].records(), spec=self._spec, services=self._services))
        elif kind == "groups":
            from .relational import emit_groups

            yield from self._emit(emit_groups(self._ports[self._port_order[0]].records(), spec=self._spec, services=self._services))
        elif kind == "union":
            from .relational import union_inputs

            inputs = tuple(self._ports[name].records() for name in self._port_order)
            yield from self._emit(union_inputs(inputs, spec=self._spec, services=self._services))
        elif kind in _ORDERING_KINDS and kind != "RepartitionOp":
            from .ordering import build_sort_runs, merge_sort_runs

            records = self._ports[self._port_order[0]].records()
            runs = build_sort_runs(records, spec=self._spec, services=self._services)
            yield from self._emit(merge_sort_runs(runs, spec=self._spec, services=self._services))
        elif kind in _AGGREGATE_KINDS:
            from .aggregate import accumulate_partial

            records = self._ports[self._port_order[0]].records()
            self.aggregate_state = accumulate_partial(records, spec=self._spec, services=self._services)
        else:
            raise _err("INVALID_CONTRACT", "kind has no finite-complete finish", kind=kind)

    def _emit(self, records: Iterator[Record]) -> Iterator[Record]:
        for record in records:
            if self._skip > 0:
                self._skip -= 1
                continue
            self._emitted += 1
            yield record

    # -- K020 snapshot ------------------------------------------------------
    def snapshot(self) -> ArtifactRef | None:
        self._require_started()
        if self._spec.state == "nonrecoverable":
            raise _err("UNKNOWN_EFFECT", "nonrecoverable kernel cannot snapshot state")
        image = {
            "schema_version": IMAGE_VERSION,
            "kind": self._spec.kind,
            "occurrence_id": self._occurrence,
            "partition_id": self._partition_id,
            "config_digest": _hash_id([_canonical(self._spec.configuration)]),
            "state": self._snapshot_state(),
            "ports": {name: port.snapshot() for name, port in self._ports.items()},
            "batch": list(self._batch),
            "groups": {key: list(value) for key, value in self._groups.items()},
            "group_base": {key: dict(value) for key, value in self._group_base.items()},
            "timer_deadline": self._timer_deadline,
            "output_cursor": self._emitted,
            "aggregate_state": self.aggregate_state,
        }
        raw = pickle.dumps(image, protocol=pickle.HIGHEST_PROTOCOL)
        limit = self._services.bounds.max_state_bytes
        if limit > 0 and len(raw) > limit:
            raise _err("BUDGET_EXCEEDED", "kernel recovery image exceeds max_state_bytes")
        return self._services.store.put(iter([raw]), max_bytes=len(raw), media_type=IMAGE_MEDIA_TYPE)

    def _snapshot_state(self) -> Any:
        state = self._spec.state
        if state == "none":
            return None
        if state == "auto_json":
            getter = getattr(self._instance, "__demiflow_state__", None)
            value = getter() if getter is not None else getattr(self._instance, "state", None)
            _canonical(value)
            return {"mode": "auto_json", "value": value}
        if state == "adapter":
            snapshot = getattr(self._instance, "__demiflow_snapshot__", None)
            if snapshot is None:
                raise _err("INVALID_CONTRACT", "adapter instance lacks __demiflow_snapshot__")
            return {"mode": "adapter", "value": snapshot()}
        raise _err("INVALID_CONTRACT", "unknown state mode", state=state)

    # -- K021 restore -------------------------------------------------------
    def restore(self, ref: ArtifactRef | None) -> None:
        if self._closed:
            raise _err("INVALID_CONTRACT", "cannot restore a closed kernel")
        if ref is None:
            if self._spec.state != "none":
                raise _err("INCOMPATIBLE_HISTORY", "stateful kernel cannot restore from an empty ref")
            return
        limit = self._services.bounds.max_state_bytes
        data = b"".join(self._services.store.read(ref, max_bytes=limit if limit > 0 else ref.size_bytes))
        image = pickle.loads(data)
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
        for name, snap in image["ports"].items():
            if name in self._ports:
                self._ports[name].restore(snap)
        self._batch = list(image["batch"])
        self._groups = {key: list(value) for key, value in image["groups"].items()}
        self._group_base = {key: dict(value) for key, value in image["group_base"].items()}
        self._timer_deadline = image["timer_deadline"]
        self._emitted = int(image["output_cursor"])
        self._skip = int(image["output_cursor"])
        self.aggregate_state = image["aggregate_state"]
        self._restore_state(image["state"])

    def _restore_state(self, state: Any) -> None:
        if state is None:
            return
        mode = state["mode"]
        if mode == "auto_json":
            setter = getattr(self._instance, "__demiflow_restore__", None)
            if setter is not None:
                setter(state["value"])
            elif self._instance is not None:
                self._instance.state = state["value"]
        elif mode == "adapter":
            restore = getattr(self._instance, "__demiflow_restore__", None)
            if restore is None:
                raise _err("INVALID_CONTRACT", "adapter instance lacks __demiflow_restore__")
            restore(state["value"])

    # -- K022 close ---------------------------------------------------------
    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        first_error: BaseException | None = None
        try:
            _close_instance(self._instance)
        except BaseException as exc:
            first_error = exc
        self._instance = None
        for port in self._ports.values():
            try:
                port.release()
            except BaseException as exc:
                if first_error is None:
                    first_error = exc
        for ref in self._spool_owner_refs:
            try:
                self._services.store.release(ref, owner=_SPOOL_OWNER)
            except BaseException as exc:
                if first_error is None:
                    first_error = exc
        self._spool_owner_refs = []
        if first_error is not None:
            raise first_error

    def _require_started(self) -> None:
        if self._closed:
            raise _err("INVALID_CONTRACT", "kernel is closed")
        if not self._started:
            raise _err("INVALID_CONTRACT", "kernel must start before use")
