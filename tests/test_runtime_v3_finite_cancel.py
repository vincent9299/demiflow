"""Executor cancellation reaches real finite authentication and borrowed reads.

Only the control transport is in-process. Seals, assignments, codecs, kernels,
artifact grants and cleanup use their original implementation.
"""
from demiflow.execution.contracts.dataflow import stage_resident_bytes
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from contextvars import copy_context
from dataclasses import asdict, replace
from pathlib import Path
from types import SimpleNamespace
import threading
import time

import pytest

from demiflow import data
from demiflow.execution import dataflow_finite_input as finite
from demiflow.execution.contracts.dataflow import WorkerLease
from demiflow.execution.contracts.dataflow_codec import runtime_scope_id
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.dataflow_hosts import ControlMailbox
from demiflow.execution.operator_bindings.dataflow_model_journal import make_native_action_view
from demiflow.execution.worker.runtime import make_runtime_services
from demiflow.execution.recovery_contract import document, RecoveryContractError
from demiflow.execution.run_context import RunContext
from demiflow.execution.worker.dataflow_execution import DataflowWorkerExecutor
from test_runtime_v3_b import DirectTransport, _events
from test_runtime_v3_finite_borrow import make_seal, record
from test_runtime_v3_real_seams import prepared


@contextmanager
def _join_worker(tmp_path):
    import demiflow
    assert Path(demiflow.__file__).resolve().is_relative_to(Path(__file__).resolve().parents[1])
    rows = [{'key': n, 'value': str(n)} for n in range(4)]
    ds = data.from_items(rows).join(data.from_items(rows), on='key')
    with prepared(tmp_path, ds) as (execution, driver):
        nodes = {node.occurrence_id: node for node in execution._graph.nodes}
        stage = next(stage for stage in execution._physical.stages
                     if any(nodes[occ].kernel.kind == 'join' for occ in stage.occurrences))
        grant = {'grant_id': 'finite-cancel-grant',
            'run_id': execution._action.context.ref.run_id,
            'attempt': execution._action.context.attempt, 'fence': execution._action.context.fence,
            'scope_id': runtime_scope_id(execution._physical.plan_id, execution._action.context.attempt),
            'allocation_id': 'finite-cancel-worker', 'boot_id': 'finite-cancel-boot',
            'controller_epoch': execution._action.context.attempt,
            'plan_revision': execution._physical.plan_revision,
            'expires_at_ms': int(time.time() * 1000) + 60000}
        raw = document(grant)
        grant_ref = driver.store.put(iter((raw,)), max_bytes=len(raw), media_type='application/json')
        lease = WorkerLease(grant['allocation_id'], grant['boot_id'], stage.stage_id,
            stage.partitions, 'local', 'in-process-test-boundary', grant_ref, grant['expires_at_ms'])
        assignment = execution._assignment(lease, stage)
        driver.journal_service.register_worker(lease, stage.occurrences)
        file_slots_before = driver.store.open_file_slots
        services = make_runtime_services(assignment, transport=DirectTransport(driver), cache=tmp_path/'cache')
        mailbox = ControlMailbox(bounds=services.bounds, check_stop=services.check_stop)
        worker = DataflowWorkerExecutor(services=services, mailbox=mailbox)
        try:
            worker.load(assignment)
            worker.start()
            occurrence, kernel = next((occ, kernel) for (occ, _), kernel in worker._kernels.items()
                                      if kernel._spec.kind == 'join')
            spec = next(spec for spec in assignment.inputs
                        if any(edge.edge_id == spec.edge_id and edge.consumer == occurrence
                               for edge in worker._graph.edges))
            assert spec.mode == 'finite_sealed'
            edge = next(edge for edge in worker._graph.edges if edge.edge_id == spec.edge_id)
            yield SimpleNamespace(worker=worker, services=services, assignment=assignment,
                                  mailbox=mailbox, spec=spec, kernel=kernel, port=edge.input_port)
        finally:
            worker.close()
            assert worker._closed and not worker._kernels and not worker._readers
            assert services.store.open_file_slots == file_slots_before
            if hasattr(worker, '_runner'):
                peer = worker._runner._peer
                assert not peer._active and not peer._sending
                assert not any(t.is_alive() for t in (peer._thread, *peer._threads))


def _handoff(item, seal):
    item.worker.receive({'kind': 'finite_input', 'edge_id': item.spec.edge_id,
                         'seal_ref': asdict(seal), 'delivery_id': 'finite-cancel-delivery'})
    return item.worker.step(max_records=1, deadline_ms=1000)


def test_cancel_during_original_full_scan_never_adopts_or_acks(tmp_path, monkeypatch):
    with _join_worker(tmp_path) as item:
        seal, _, expected = make_seal(item.services, item.spec)
        assert len(expected) > 1
        entered, release = threading.Event(), threading.Event()
        decoded, errors = [], []
        original = finite._decode_record_document
        def observed(*args, **kwargs):
            record = original(*args, **kwargs)
            decoded.append(record.record_id)
            if len(decoded) == 1:
                entered.set()
                assert release.wait(5)
            return record
        monkeypatch.setattr(finite, '_decode_record_document', observed)
        def scan():
            try:
                _handoff(item, seal)
            except BaseException as error:
                errors.append(error)
        thread = threading.Thread(target=scan)
        thread.start()
        try:
            assert entered.wait(3)
            item.worker.cancel('test finite scan cancellation', deadline_ms=100)
        finally:
            release.set()
            thread.join(5)
        assert not thread.is_alive()
        assert len(errors) == 1 and isinstance(errors[0], DataflowError)
        assert errors[0].code == 'CANCELLED'
        assert len(decoded) == 1
        assert not item.kernel._ports[item.port]._borrowed
        assert not item.worker._runner._finished_inputs
        assert not [event for event in _events(item) if event.kind == 'ack']


@pytest.mark.parametrize('projected', [False, True])
def test_cancel_reaches_original_borrowed_second_scan(tmp_path, projected):
    with _join_worker(tmp_path) as item:
        seal, _, expected = make_seal(item.services, item.spec)
        assert len(expected) > 1
        _handoff(item, seal)
        acks = [event for event in _events(item) if event.kind == 'ack']
        assert len(acks) == 1  # The completed original adoption remains valid.
        port = item.kernel._ports[item.port]
        records = port.records()
        iterator = iter(records.for_join(('key',)) if projected else records)
        try:
            first = next(iterator)
            assert first.inline['key'] == expected[0]['key']
            item.worker.cancel('test borrowed scan cancellation', deadline_ms=100)
            with pytest.raises(DataflowError, match='CANCELLED'):
                next(iterator)
            assert not [event for event in _events(item) if event.kind == 'ack']
        finally:
            iterator.close()


def test_finite_checker_leaves_original_native_persistence_and_first_cancel_reason(tmp_path):
    with _join_worker(tmp_path) as item:
        kernel = item.kernel
        action = make_native_action_view(kernel._services, node_id=kernel._occurrence)
        assert kernel._services is item.services
        for name in ('store', 'journal', 'bounds', 'assignment', 'constants', 'programs'):
            assert getattr(kernel._ports[item.port]._services, name) is getattr(item.services, name)
        assert action.context.check_stop is item.services.check_stop
        assert item.worker._source_run.check_stop is item.services.check_stop
        item.worker.cancel('cooperative:run_cancelled', deadline_ms=100)
        action.context.check_stop()  # A pending native response retains its port.
        with pytest.raises(DataflowError, match='CANCELLED'):
            kernel._ports[item.port]._services.check_stop()
        with pytest.raises(DataflowError, match='CANCELLED'):
            next(kernel._finish_complete())
        assert item.worker._cancel_reason == 'cooperative:run_cancelled'
        item.worker.cancel('execution_close', deadline_ms=100)
        assert item.worker._cancel_reason == 'cooperative:run_cancelled'


def test_real_dataset_cancel_interrupts_finite_authentication_and_releases_workers(tmp_path, monkeypatch):
    entered, cancelled = threading.Event(), threading.Event()
    decoded = []
    original_decode, original_cancel = finite._decode_record_document, DataflowWorkerExecutor.cancel
    def observe_decode(*args, **kwargs):
        record = original_decode(*args, **kwargs)
        decoded.append(record.record_id)
        if len(decoded) == 1:
            entered.set()
            assert cancelled.wait(10), 'driver did not dispatch original executor cancellation'
        return record
    def observe_cancel(worker, *args, **kwargs):
        result = original_cancel(worker, *args, **kwargs)
        if any(kernel._spec.kind == 'join' for kernel in worker._kernels.values()):
            cancelled.set()
        return result
    monkeypatch.setattr(finite, '_decode_record_document', observe_decode)
    monkeypatch.setattr(DataflowWorkerExecutor, 'cancel', observe_cancel)
    with pytest.raises(RecoveryContractError, match='Execution cancellation requested'):
        with RunContext.start(tmp_path/'run', limits={'timeout_s': 30}) as run, \
                data.local_execution(workers=4, worker_mode='thread'):
            rows = [{'key': n, 'value': str(n)} for n in range(32)]
            dataset = data.from_items(rows).join(data.from_items(rows), on='key')
            with ThreadPoolExecutor(1) as pool:
                future = pool.submit(copy_context().run, dataset.take_all)
                try:
                    assert entered.wait(15), 'real finite authentication did not start'
                    RunContext.cancel(run.ref)
                    future.result(15)
                finally:
                    # If the original control path failed, release the observer
                    # so its test-owned waiter cannot hide the real failure.
                    cancelled.set()
    assert decoded and len(decoded) == 1
    from demiflow.execution.run_journal import RunJournal
    from demiflow.execution.hosts.capacity import local_capacity
    from demiflow.resources.capacity import LocalCapacityPool
    with RunJournal(tmp_path/'run')._connection() as db:
        allocations = db.execute('SELECT allocation_id,cleanup FROM resource_allocations').fetchall()
        scopes = db.execute('SELECT state FROM resource_scopes').fetchall()
    assert allocations and all(cleanup == 'CONFIRMED' for _, cleanup in allocations)
    assert scopes and all(state == 'CLOSED' for state, in scopes)
    domain, capacity = local_capacity()
    assert str(domain).startswith('/tmp/demiflow-')
    snapshot = LocalCapacityPool(domain/'capacity', capacity=capacity).snapshot()
    assert not snapshot['active'] and snapshot['charged'] == {'cpu_millis': 0, 'memory_bytes': 0}


def test_cancel_inside_intermediate_sort_merge_stops_before_next_spill_frame(tmp_path, monkeypatch):
    from demiflow.execution.kernels import ordering
    with _join_worker(tmp_path) as item:
        services = item.kernel._ports[item.port]._services
        spec = replace(item.kernel._spec, kind='SortOp', configuration={'keys': ['key']})
        # Three original durable sorted runs force an intermediate merge with
        # fan-in two. The observer delegates every write to the original store.
        runs = tuple(ref for offset in range(3) for ref in ordering.build_sort_runs(
            (record({'key': i}, i) for i in range(offset, 12, 3)), spec=spec, services=services))
        assert len(runs) == 3
        monkeypatch.setattr(ordering, '_MERGE_FAN_IN', 2)
        original_put = services.store.put
        slots_before_merge = services.store.open_file_slots
        written = []
        def put(chunks, **kwargs):
            def observe():
                for chunk in chunks:
                    written.append(chunk)
                    yield chunk
                    if len(written) == 1:
                        item.worker.cancel('cancel intermediate merge', deadline_ms=100)
            return original_put(observe(), **kwargs)
        monkeypatch.setattr(services.store, 'put', put)
        with pytest.raises(DataflowError, match='CANCELLED'):
            list(ordering.merge_sort_runs(runs, spec=spec, services=services))
        assert len(written) == 1
        assert services.store.open_file_slots == slots_before_merge
        assert all(services.store.inspect(ref) for ref in runs)


def test_finite_credit_retry_checks_cancel_but_native_port_keeps_original_guard(tmp_path, monkeypatch):
    from demiflow.execution.contracts.record_codec import admit_workspace, resident_limit
    with _join_worker(tmp_path) as item:
        runner = item.worker._runner
        held = runner._peer.put(b'x' * 4096)
        requests, sizes = [], []
        def measure(bounds, *, resident_bytes, retained):
            requests.append(retained)
            if not sizes:
                sizes.append(resident_limit(bounds, stage_resident_bytes(getattr(services, 'assignment', None))) - retained + 2048)
            return admit_workspace(sizes[0], bounds, resident_bytes=stage_resident_bytes(getattr(services, 'assignment', None)), retained=retained)
        original_wait = runner._condition.wait
        def cancel_at_original_credit_wait(timeout):
            item.worker.cancel('cancel finite peer-credit wait', deadline_ms=100)
            return original_wait(0)
        monkeypatch.setattr(runner._condition, 'wait', cancel_at_original_credit_wait)
        try:
            with pytest.raises(DataflowError, match='CANCELLED'):
                runner._finite_frame_call(measure, runner.bounds)
            assert len(requests) == 1
        finally:
            runner._peer.release(held['key'])
        # Once original peer ownership is released, the default port still
        # obeys only the original guard, allowing admitted effects to persist.
        runner._frame_call(measure, runner.bounds)
        assert len(requests) == 2 and requests[0] - requests[1] == 4096
