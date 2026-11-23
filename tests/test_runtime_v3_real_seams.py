"""Integration regression tests: real C/G/P/D/H values, no lane doubles.

These tests certify compilation and sealing, not a successful hosted run.
All run state is isolated beneath pytest's /tmp/demiflow-* basetemp.
"""
from contextlib import contextmanager
from dataclasses import replace
import math
import pickle
import json
import time

import pytest

from demiflow import data
from demiflow.data.plan import LogicalPlan
from demiflow.data.sources import ItemsSource
from demiflow.execution.contracts.dataflow import Record, thaw_record_value
from demiflow.execution.contracts.dataflow import WorkerLease, RuntimeServices
from demiflow.execution.contracts.dataflow_codec import compute_graph_digest, compute_physical_digest
from demiflow.execution.action_driver.dataflow import DataflowExecution
from demiflow.execution.executors.dataflow_api import make_action_request
from demiflow.execution.sources.materialized_input import build_input_handle
from demiflow.execution.seal.plan import load_dataflow
from demiflow.execution.seal.plan import verify_dataflow_program
from demiflow.execution.action_driver.services import make_driver_services
from demiflow.execution.sources.snapshots import spool_record
from demiflow.execution.sources.snapshots import open_source_reader
from demiflow.execution.recovery_identity import source_identity
from demiflow.execution.run_context import RunContext
from demiflow.planning.dataflow_compile import lower_action
from demiflow.planning.dataflow_policy import resolve_runtime_policy


class PreparationOnly(Exception):
    pass


@contextmanager
def prepared(tmp_path, dataset, *, kind="take", arguments=None):
    # An intentionally preparation-only run must never be marked succeeded.
    with pytest.raises(PreparationOnly):
        with RunContext.start(tmp_path / "run") as run:
            request = make_action_request(dataset._executor, dataset._source, dataset._plan,
                                          kind=kind, arguments=arguments or {"limit": 1})
            policy, bounds = resolve_runtime_policy({}, run.limits, provider="local")
            services = make_driver_services(request.action, policy=policy, bounds=bounds)
            execution = DataflowExecution(request, services=services)
            try:
                execution.prepare()
                yield execution, services
            finally:
                execution.close()
            raise PreparationOnly()


def test_real_compiler_seal_roundtrip(tmp_path):
    with prepared(tmp_path, data.from_iter(lambda: iter([{"x": 1}])).map(lambda row: row)) as (ex, services):
        graph, physical, sealed = load_dataflow(services.action.context.directory,
            services.action.action_id, store=services.store, bounds=services.bounds)
        assert graph.dag_id == compute_graph_digest(graph)
        assert physical.plan_id == compute_physical_digest(physical)
        assert len(graph.nodes) == 3
        assert len(sealed.programs) == 2  # source + map; terminal is a platform implementation
        assert ex._acquisition is None


def test_two_sources_bound_to_their_graph_occurrences(tmp_path):
    from demiflow.data.api import DataAPI
    from demiflow.execution.executors.local import LocalDatasetExecutor
    executor = LocalDatasetExecutor(resource_root=tmp_path)
    source = DataAPI(executor)
    left = source.from_iter(lambda: iter([{"x": 1}]))
    right = source.from_iter(lambda: iter([{"x": 2}]))
    try:
        with prepared(tmp_path, left.union(right)) as (ex, services):
            assert len(ex._physical.sources) == 2
            assert {s.source_id for s in ex._physical.sources} == set(ex._graph.sources)
            assert {s.source_id for s in ex._physical.splits} == set(ex._graph.sources)
    finally:
        executor.close()


def test_take_zero_never_reads_or_serializes_original_source(tmp_path):
    import threading
    unpicklable_lock = threading.Lock()
    def forbidden():
        with unpicklable_lock:
            raise AssertionError("take(0) read the original input")
        yield {}
    with prepared(tmp_path, data.from_iter(forbidden), arguments={"limit": 0}) as (ex, services):
        assert len(ex._physical.sources) == 1
        assert ex._physical.sources[0].immutable_identity["count"] == 0


def test_literal_input_identity_preserves_historical_projection():
    source = data.from_items([{"x": 1}])._source
    other = data.from_items([{"x": 1}])._source
    changed = data.from_items([{"x": 2}])._source
    assert source_identity(source) == source_identity(other)
    assert source_identity(source) != source_identity(changed)
    assert source.handle.row_count == 1
    from demiflow.execution.recovery_contract import identity
    from demiflow.execution.recovery_identity import describe
    assert source_identity(source) == {
        "literal_materialization": identity(describe((({"x": 1},),))), "rows": 1}
    assert not hasattr(source.handle, "recovery_reference")


def test_record_fidelity_through_real_store(tmp_path):
    original = {"list": [-0.0, 0.0], "tuple": (b"x", {"nested": [1]}), "none": None}
    record = Record("a" * 64, {"index": 0}, None, original, 100, 0)
    original["list"].append(99)
    with prepared(tmp_path, data.from_iter(lambda: iter([]))) as (ex, services):
        ref = spool_record(record, split_id="b" * 64, sequence=0,
                           store=services.store, bounds=services.bounds)
        from demiflow.execution.contracts.record_codec import _decode_payload
        value = _decode_payload(b"".join(services.store.read(ref, max_bytes=services.bounds.max_row_bytes)),
                                max_bytes=services.bounds.max_row_bytes)
        assert type(value["list"]) is list and len(value["list"]) == 2
        assert type(value["tuple"]) is tuple
        assert type(value["tuple"][1]["nested"]) is list
        assert value["tuple"][0] == b"x"
        assert math.copysign(1, value["list"][0]) == -1
        assert math.copysign(1, value["list"][1]) == 1


def test_nested_literal_input_budget_is_checked_before_copy():
    from demiflow.execution.sources.materialized_input import InputMaterializedHandle
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    with pytest.raises(DataflowError, match="BUDGET_EXCEEDED"):
        InputMaterializedHandle([{"nested": {"wide": "x" * 10000}}], format="items", max_bytes=1024)


def reader_services(ex, services):
    # This test exercises loading/reader/kernel contracts directly. It does not
    # claim this lease was allocated by a host or verified by a worker transport.
    stage = ex._physical.stages[0]
    lease = WorkerLease("reader-test", "reader-boot", stage.stage_id, stage.partitions,
                        "local", "reader-test", ex._sealed.code_pin_ref, 2**62)
    assignment = ex._assignment(lease, stage)
    pin = json.loads(b"".join(services.store.read(ex._sealed.code_pin_ref,
                                              max_bytes=services.bounds.max_control_bytes)))
    programs = {node.occurrence_id: verify_dataflow_program(node.program_ref,
                expected_pin=pin["programs"][node.occurrence_id], store=services.store,
                bounds=services.bounds) for node in ex._graph.nodes if node.program_ref}
    return RuntimeServices(services.store, services.journal_service, services.bounds,
                           assignment, lambda: int(time.time() * 1000), lambda: None, {}, programs)


def test_real_reader_and_callable_kernel_receive_sealed_parameters(tmp_path):
    from demiflow.execution.kernels.dataflow import make_kernel
    from demiflow.execution.storage.streams import materialize_row as _materialize_row
    dataset = data.from_iter(lambda: iter([{"x": 2}])).map(
        lambda row, delta: {"x": row["x"] + delta}, fn_kwargs={"delta": 3})
    with prepared(tmp_path, dataset) as (ex, services):
        runtime = reader_services(ex, services)
        reader = open_source_reader(ex._physical.sources[0], ex._physical.splits[0],
                                    cursor=None, services=runtime)
        node = next(node for node in ex._graph.nodes if node.kind == "transform")
        kernel = make_kernel(node.kernel, services=runtime, partition_id="test-partition")
        try:
            kernel.start()
            poll = reader.next_record()
            assert poll.status == "record"
            output = list(kernel.push("main", poll.record))
            assert [_materialize_row(record, store=runtime.store, bounds=runtime.bounds, assignment=getattr(runtime, 'assignment', None)) for record in output] == [{"x": 5}]
            assert reader.next_record().status == "eoi"
        finally:
            kernel.close()
            reader.close()


def test_assignment_only_contains_sources_owned_by_its_stage(tmp_path):
    with prepared(tmp_path, data.from_iter(lambda: iter([{"x": 1}])).map(lambda r: r)) as (ex, services):
        for stage in ex._physical.stages:
            lease = WorkerLease("lease", "boot", stage.stage_id, stage.partitions,
                                "local", "endpoint", ex._sealed.code_pin_ref, 2**62)
            assignment = ex._assignment(lease, stage)
            assert all(split.source_id in stage.occurrences for split in assignment.source_splits)
            if not (set(stage.occurrences) & set(ex._graph.sources)):
                assert assignment.source_splits == ()


def test_worker_reads_original_sealed_refs_without_opening_journal(tmp_path, monkeypatch):
    from demiflow.execution.seal.plan import load_sealed_dataflow
    from demiflow.execution.seal.plan import load_worker_programs as _load_programs
    from demiflow.execution.worker.dataflow_execution import DataflowWorkerExecutor
    from demiflow.execution.dataflow_hosts import ControlMailbox
    with prepared(tmp_path, data.from_items([{"x": 1}]).map(lambda r: r)) as (ex, services):
        runtime = reader_services(ex, services)
        def forbidden(*args, **kwargs):
            raise AssertionError("worker opened the driver database")
        with monkeypatch.context() as patch:
            patch.setattr("demiflow.execution.seal.plan.RunJournal", forbidden)
            graph, physical, pin = load_sealed_dataflow(ex._sealed,
                action_id=ex._action.action_id, store=services.store, bounds=services.bounds)
            programs = _load_programs(runtime.assignment, store=services.store, bounds=services.bounds)
            owned = {occurrence for stage in runtime.assignment.stages for occurrence in stage.occurrences}
            unassigned_programs = set(pin["programs"]) - owned
            assert unassigned_programs, "fixture must include a program owned by another stage"
            assert set(programs) == set(pin["programs"]).intersection(owned)
            assert set(programs).isdisjoint(unassigned_programs)
            runtime = replace(runtime, programs=programs)
            worker = DataflowWorkerExecutor(services=runtime,
                mailbox=ControlMailbox(bounds=services.bounds, check_stop=lambda: None))
            try:
                worker.load(runtime.assignment)
                assert worker._graph == graph and worker._physical == physical
            finally:
                worker.close()


def test_real_source_kernel_exchange_codec_roundtrip(tmp_path):
    from demiflow.execution.exchange.dataflow_channels import encode_record, resolve_record
    from demiflow.execution.kernels.dataflow import make_kernel
    from demiflow.execution.storage.streams import materialize_row as _materialize_row
    value = {"negative": [-0.0], "positive": [0.0], "tuple": (b"data", [None])}
    with prepared(tmp_path, data.from_items([value]).map(lambda r: r)) as (ex, services):
        runtime = reader_services(ex, services)
        reader = open_source_reader(ex._physical.sources[0], ex._physical.splits[0],
                                    cursor=None, services=runtime)
        node = next(node for node in ex._graph.nodes if node.kind == "transform")
        kernel = make_kernel(node.kernel, services=runtime, partition_id="partition")
        try:
            kernel.start()
            source = reader.next_record().record
            source_wire = encode_record(source, bounds=services.bounds)
            decoded = resolve_record(source_wire, store=services.store, bounds=services.bounds)
            output, = kernel.push("main", decoded)
            wire = encode_record(output, bounds=services.bounds)
            actual = _materialize_row(resolve_record(wire, store=services.store, bounds=services.bounds), store=runtime.store, bounds=runtime.bounds, assignment=getattr(runtime, 'assignment', None))
            assert actual == value
            assert type(actual["tuple"]) is tuple and type(actual["tuple"][1]) is list
            assert math.copysign(1, actual["negative"][0]) == -1
            assert math.copysign(1, actual["positive"][0]) == 1
        finally:
            kernel.close()
            reader.close()


def test_two_real_local_allocations_share_manager_and_close_confirmed(tmp_path):
    from demiflow.resources.contracts import ResourceScope
    with prepared(tmp_path, data.from_items([{"x": 1}]).map(lambda r: r)) as (ex, services):
        scope = ex._scheduler.scope_id
        services.resources.state.open_scope(ex._fence(), ResourceScope(
            scope, ex._run_id(), {"provider": "local"}, ex._fence(),
            {"cpu_millis": 8000, "memory_bytes": services.bounds.max_resident_bytes, "max_workers": 16}))
        ex._scope_open = True
        try:
            for stage in ex._physical.stages[:2]:
                work = ex._initial_work(stage, stage.partitions[0])
                lease = ex._scheduler.place(work, prepare=ex._prepare_launch, host_id="local")
                assert ex._endpoints[lease.allocation_id]._manager is services.resources
            snapshot = services.resources.get(scope)
            assert len(snapshot["allocations"]) == 2
            assert all(a["phase"] == "RUNNING" for a in snapshot["allocations"])
        finally:
            ex.close()
        snapshot = services.resources.get(scope)
        assert all(a["cleanup"] == "CONFIRMED" for a in snapshot["allocations"])


def test_source_resume_replays_pending_record_with_original_provenance(tmp_path):
    with prepared(tmp_path, data.from_items([{"x": 1}, {"x": 2}])) as (ex, services):
        runtime = reader_services(ex, services)
        reader = open_source_reader(ex._physical.sources[0], ex._physical.splits[0],
                                    cursor=None, services=runtime)
        first = reader.next_record().record
        cursor = reader.snapshot()  # simulate death after read, before ordered publish
        reader.close()
        resumed = open_source_reader(ex._physical.sources[0], ex._physical.splits[0],
                                     cursor=cursor, services=runtime)
        try:
            replay = resumed.next_record().record
            assert replay == first
            second = resumed.next_record().record
            assert second.sequence == first.sequence + 1 and second.record_id != first.record_id
            assert resumed.next_record().status == "eoi"
        finally:
            resumed.close()


def test_native_operation_has_one_type_authority():
    from demiflow.data.dataflow_operation import NativeOperation as public
    from demiflow.execution.contracts.dataflow import NativeOperation as contract
    assert public is contract


@pytest.mark.parametrize("override", [None, 0, -1, 1, 2, 4, 12])
def test_input_block_boundaries_and_history_match_pre_v3(override):
    from demiflow.execution.sources.materialized_input import build_input_handle
    from demiflow.execution.recovery_identity import describe
    from demiflow.execution.recovery_contract import identity
    from demiflow.data.sources import MaterializedSource
    rows = [{"x": i} for i in range(5)]
    count = max(1, int(override or 1))
    size = max(1, (len(rows) + count - 1) // count)
    expected = tuple(tuple(rows[i:i + size]) for i in range(0, len(rows), size))
    # The formula is copied verbatim from 267b914's LocalDatasetExecutor.from_items;
    # no new projection may silently replace the historical materialization ID.
    handle = build_input_handle(rows, format="items", override_num_blocks=override)
    try:
        assert handle.blocks == expected
        assert handle.num_blocks() == len(expected)
        assert source_identity(MaterializedSource(handle, len(rows))) == {
            "literal_materialization": identity(describe(expected)), "rows": len(rows)}
    finally:
        handle.release()


def test_real_native_cut_commit_and_recovery_with_typed_sink_record(tmp_path):
    from demiflow.execution.contracts.dataflow import CutContribution, SourceCursor
    from demiflow.execution.dataflow_sinks import make_sink_adapter, prepare_sink_partition
    from demiflow.execution.kernels.dataflow import _output_record
    with prepared(tmp_path, data.from_items([{"value": (b"a", [-0.0])}])) as (ex, services):
        runtime = reader_services(ex, services)
        terminal = next(node for node in ex._graph.nodes if node.kind == "terminal")
        sink = make_sink_adapter(terminal.kernel, services=services)
        record = _output_record("a" * 64, {"index": 0}, {"value": (b"a", [-0.0])}, 0, runtime)
        staged = prepare_sink_partition(iter((record,)), spec=terminal.kernel, services=runtime)
        cut = ex._checkpoint.begin(final=False)
        for index, participant in enumerate(cut.participants):
            cursors = tuple(SourceCursor(split.split_id, {"index": 1}, 1, True, None, ())
                            for split in ex._physical.splits) if index == 0 else ()
            contribution = CutContribution(cut.cut_id, participant, "boot", cursors,
                            {}, {}, (), (), {terminal.kernel.kind: staged} if index == 0 else {}, False)
            ex._checkpoint.contribute(contribution)
        image = ex._checkpoint.commit(cut.cut_id, sinks={terminal.kernel.kind: sink})
        assert image.commit_id != "0" * 64
        assert services.action.context.journal.execution_commit(image.commit_id)["status"] == "acked"
        restored = ex._checkpoint.recover(sinks={terminal.kernel.kind: sink})
        assert restored.commit_id == image.commit_id and restored.source_vector == image.source_vector
        # EOI becomes evidence only after a complete native cut, not from one
        # worker's optimistic progress counter.
        assert ex._checkpoint.begin(final=True).final


def test_real_local_host_verifies_loads_and_activates_map_program(tmp_path):
    from demiflow.resources.contracts import ResourceScope
    with prepared(tmp_path, data.from_items([{"x": 1}]).map(lambda r: r)) as (ex, services):
        scope = ex._scheduler.scope_id
        services.resources.state.open_scope(ex._fence(), ResourceScope(
            scope, ex._run_id(), {"provider": "local"}, ex._fence(),
            {"cpu_millis": 8000, "memory_bytes": services.bounds.max_resident_bytes, "max_workers": 16}))
        ex._scope_open = True
        transform = next(node for node in ex._graph.nodes if node.kind == "transform")
        stage = next(stage for stage in ex._physical.stages if transform.occurrence_id in stage.occurrences)
        lease = ex._scheduler.place(ex._initial_work(stage, stage.partitions[0]),
                                   prepare=ex._prepare_launch, host_id="local")
        endpoint = ex._endpoints[lease.allocation_id]
        endpoint.activate(ex._assignment(lease, stage))
        assert endpoint._activated
        endpoint.cancel("activation verification complete", deadline_ms=5000)


def test_driver_control_repeated_reads_and_unread_reply_backpressure(tmp_path):
    from demiflow.execution.dataflow_authority import DriverJournalService
    from demiflow.execution.contracts.dataflow import WorkerEvent
    from demiflow.execution.recovery_contract import identity
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    with prepared(tmp_path, data.from_items([])) as (ex, services):
        service = DriverJournalService(ex._action, store=services.store,
                                      bounds=replace(services.bounds, max_pending_events=1))
        event = WorkerEvent("demiflow.dataflow_event.v3", "check_grant", identity(["grant", 1]),
            ex._run_id(), ex._attempt(), ex._fence(), "allocation", "boot", 0, "scope", 1, None, {})
        stage = ex._physical.stages[0]
        service.register_worker(WorkerLease('allocation', 'boot', stage.stage_id,
            stage.partitions, 'local', 'endpoint', ex._sealed.code_pin_ref,
            int(time.time() * 1000) + 60000), stage.occurrences)
        other = replace(event, operation_id=identity(["grant", 2]))
        try:
            assert service.submit(event)
            assert service.submit(event)  # identical in-flight delivery
            with pytest.raises(DataflowError, match="Operation id reused"):
                service.submit(replace(event, payload={"different": True}))
            assert not service.submit(other)
            for _ in range(2):
                deadline = time.monotonic() + 3
                while not (replies := service.poll_replies(max_replies=1)):
                    assert time.monotonic() < deadline
                    time.sleep(0.005)
                assert len(replies) == 1 and replies[0].reply.status == "ok"
                if _ == 0:
                    assert service.submit(event)  # repeat after a consumed reply must answer again
            assert service.submit(other)
        finally:
            service.close(deadline_ms=3000)


def test_seal_never_manufactures_report_identity_from_execution_ids(tmp_path):
    with prepared(tmp_path, data.from_items([])) as (ex, services):
        raw = b"".join(services.store.read(ex._sealed.identity_bridge_ref,
                                         max_bytes=services.bounds.max_control_bytes))
        bridge = json.loads(raw)
        assert bridge["available"] is False
        assert bridge["reason"] == "checked_report_binding_missing"
        assert "report_digest" not in bridge and "entries" not in bridge


def test_input_literal_history_blocks_are_read_only():
    handle = build_input_handle([{"x": 1}], format="items")
    with pytest.raises(AttributeError):
        handle.blocks = ()


def test_snapshot_kind_is_the_registered_semantic_kind(tmp_path):
    from demiflow.planning.dataflow_registry import source_registry
    with prepared(tmp_path, data.from_items([{"x": 1}])) as (ex, services):
        snapshot, = ex._physical.sources
        assert snapshot.kind == "materialized"
        assert snapshot.kind in source_registry()


def test_real_workbench_and_history_read_original_v3_store(tmp_path):
    from demiflow.workbench.runs.facts import read_sealed_plan
    from demiflow.execution.dataflow_history import load_execution_version
    with prepared(tmp_path, data.from_items([{"x": 1}]).map(lambda r: r)) as (ex, services):
        root = services.action.context.directory
        before = sorted(str(p.relative_to(root)) for p in root.rglob("*"))
        entry = read_sealed_plan(root, ex._action.action_id)
        assert entry["available"], entry
        assert entry["dag_id"] == ex._graph.dag_id
        assert entry["plan_id"] == ex._physical.plan_id
        binding = load_execution_version(root, ex._action.action_id,
                                         max_bytes=services.bounds.max_control_bytes)
        assert binding.original_plan_ref == ex._sealed.physical_ref
        assert binding.original_code_pin_ref == ex._sealed.code_pin_ref
        assert binding.compatible
        assert sorted(str(p.relative_to(root)) for p in root.rglob("*")) == before


def test_real_sort_spill_preserves_typed_records_and_provenance(tmp_path):
    from demiflow.execution.kernels.ordering import build_sort_runs, merge_sort_runs
    from demiflow.execution.storage.streams import materialize_row as _materialize_row
    original = [{"x": 2, "typed": (-0.0, b"two")}, {"x": 1, "typed": [0.0, b"one"]}]
    with prepared(tmp_path, data.from_items(original).sort("x")) as (ex, services):
        runtime = reader_services(ex, services)
        reader = open_source_reader(ex._physical.sources[0], ex._physical.splits[0],
                                    cursor=None, services=runtime)
        try:
            records = [reader.next_record().record, reader.next_record().record]
            spec = next(n.kernel for n in ex._graph.nodes if n.kernel.kind == "SortOp")
            runs = build_sort_runs(iter(records), spec=spec, services=runtime)
            ordered = list(merge_sort_runs(runs, spec=spec, services=runtime))
            assert [r.record_id for r in ordered] == [records[1].record_id, records[0].record_id]
            assert [r.source_position for r in ordered] == [records[1].source_position, records[0].source_position]
            rows = [_materialize_row(r, store=runtime.store, bounds=runtime.bounds, assignment=getattr(runtime, 'assignment', None)) for r in ordered]
            assert rows == [original[1], original[0]]
            assert type(rows[0]["typed"]) is list
            assert type(rows[1]["typed"]) is tuple
            assert math.copysign(1, rows[1]["typed"][0]) == -1
            # A single frame cannot use the oversized-row exception to bypass
            # the resident ceiling. The input is still within max_row_bytes.
            small = replace(runtime, bounds=replace(runtime.bounds, max_resident_bytes=64))
            from demiflow.execution.contracts.dataflow_errors import DataflowError
            with pytest.raises(DataflowError, match="single sort frame"):
                build_sort_runs(iter(records), spec=spec, services=small)
            with pytest.raises(DataflowError, match="two input file slots"):
                list(merge_sort_runs(runs + runs, spec=spec,
                     services=replace(runtime, bounds=replace(runtime.bounds, max_open_files=1))))
        finally:
            reader.close()


def test_resume_validation_reads_wire_plan_and_checks_platform_pin(tmp_path):
    from demiflow.execution.dataflow_history import load_execution_version, validate_resume_compatibility
    from demiflow.execution.recovery_identity import platform_identity
    with prepared(tmp_path, data.from_items([{"x": 1}])) as (ex, services):
        binding = load_execution_version(services.action.context.directory,
                         services.action.action_id, max_bytes=services.bounds.max_control_bytes)
        same = validate_resume_compatibility(binding, current_runtime=platform_identity(), store=services.store)
        changed = validate_resume_compatibility(binding, current_runtime="different-runtime", store=services.store)
        assert same.recovery == ex._physical.recovery
        assert same.required_profiles == tuple(s.profile for s in ex._physical.stages)
        assert not same.supported  # Preparation alone is not committed recovery proof.
        assert "historical binding has no journal-proven recovery image" in same.reasons
        mismatch = "original platform code differs from the current runtime"
        assert mismatch not in same.reasons
        assert mismatch not in changed.reasons
        assert not changed.supported
        # Platform provenance and the legacy keyword cannot alter actual
        # recovery-image, wire-plan or resource-profile validation.
        assert changed == same == validate_resume_compatibility(binding, store=services.store)


def test_real_join_kernel_uses_same_row_codec_on_write_and_read(tmp_path):
    from demiflow.execution.kernels.relational import join_partitions
    from demiflow.execution.storage.streams import materialize_row as _materialize_row
    left = data.from_items([{"key": 1, "left": (b"one", -0.0)}, {"key": None, "left": ()}])
    right = data.from_items([{"key": 1, "right": [b"a"]}, {"key": 1, "right": [b"b"]}])
    with prepared(tmp_path, left.join(right, on="key")) as (ex, services):
        runtime = reader_services(ex, services)
        sources = []
        readers = []
        try:
            for snap in ex._physical.sources:
                split = next(s for s in ex._physical.splits if s.source_id == snap.source_id)
                reader = open_source_reader(snap, split, cursor=None, services=runtime)
                readers.append(reader)
                rows = []
                while True:
                    poll = reader.next_record()
                    if poll.status == "eoi": break
                    assert poll.status == "record"
                    rows.append(poll.record)
                sources.append(iter(rows))
            spec = next(n.kernel for n in ex._graph.nodes if n.kind == "relational")
            rows = [_materialize_row(r, store=runtime.store, bounds=runtime.bounds, assignment=getattr(runtime, 'assignment', None)) for r in join_partitions(
                sources[0], sources[1], spec=spec, services=runtime)]
            assert len(rows) == 2
            assert {r["right"][0] for r in rows} == {b"a", b"b"}
            assert all(type(r["left"]) is tuple and type(r["right"]) is list for r in rows)
            assert all(math.copysign(1, r["left"][1]) == -1 for r in rows)
        finally:
            for reader in readers: reader.close()


def test_control_cancel_wakes_waiter_without_confirming_unknown_effect(tmp_path):
    import threading
    from demiflow.execution.dataflow_hosts import ControlMailbox
    from demiflow.execution.contracts.dataflow import WorkerEvent, ControlReply
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    with prepared(tmp_path, data.from_items([])) as (ex, services):
        mailbox = ControlMailbox(bounds=services.bounds, check_stop=lambda: None)
        event = WorkerEvent('demiflow.dataflow_event.v3', 'check_grant', 'a' * 64,
            ex._run_id(), ex._attempt(), ex._fence(), 'allocation', 'boot', 0, 'scope', 1, None, {})
        outcomes = []
        def request(value):
            try:
                outcomes.append(mailbox.request(value, deadline_ms=3000))
            except Exception as exc:
                outcomes.append(exc)
        def take_request():
            deadline = time.monotonic() + 2
            while not (events := mailbox.drain(max_events=1)):
                assert time.monotonic() < deadline
                time.sleep(.005)
            return events[0]
        thread = threading.Thread(target=request, args=(event,))
        thread.start()
        assert take_request() == event
        mailbox.cancel_pending()
        thread.join(timeout=1)
        assert not thread.is_alive()
        assert isinstance(outcomes[0], DataflowError) and outcomes[0].code == 'UNKNOWN_EFFECT'
        mailbox.deliver(ControlReply(event.operation_id, 'ok', {}, None, None))
        # Cleanup can still use this port, with a fresh operation identity.
        cleanup = replace(event, operation_id='b' * 64)
        thread = threading.Thread(target=request, args=(cleanup,))
        thread.start()
        assert take_request() == cleanup
        reply = ControlReply(cleanup.operation_id, 'ok', {}, None, None)
        mailbox.deliver(reply)
        thread.join(timeout=1)
        assert not thread.is_alive() and outcomes[1] == reply
        mailbox.close()


class CountingTask:
    def __call__(self, row):
        return row


def test_explicit_worker_grant_includes_exact_private_load_buffer(tmp_path):
    from demiflow.execution.contracts.dataflow_errors import DataflowError

    def task(grant):
        return data.from_items([{'k': 1}]).map(CountingTask, backend_options={
            'ray': {'worker_resources': {'memory_bytes': grant}}})

    def measured(ex):
        node = next(n for n in ex._graph.nodes if n.kind == 'transform')
        stage = next(s for s in ex._physical.stages if node.occurrence_id in s.occurrences)
        return node.kernel.configuration, stage.profile.requests['memory_bytes']

    grant = 128 * 1024**2
    with prepared(tmp_path / 'initial', task(grant)) as (execution, _):
        config, selected = measured(execution)
        assert selected == grant
        exact = config['fixed_worker_min_bytes'] + config['task_load_bytes']
    with prepared(tmp_path / 'exact', task(exact)) as (execution, _):
        assert measured(execution)[1] == exact
    with pytest.raises(DataflowError, match='private task load') as rejected, prepared(
            tmp_path / 'short', task(exact - 1)):
        pytest.fail('an undersized explicit grant was accepted')
    # The workbench persists this message for a failed run.
    assert f'memory_bytes required={exact}, available={exact - 1}' in str(rejected.value)
    assert rejected.value.context['stage_id'] in str(rejected.value)
