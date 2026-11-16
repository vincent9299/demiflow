"""B001–B020 against real sealed DTOs, journal RPC and the unified graph runner.

Only the network/provider boundary is in-process: leases are registered with the
real driver service and worker commands go through its real dispatch. No mirror
DTOs, sys.modules replacement, fake compiler/store/kernel or private second DB.
Actual host allocation/Local/Ray end-to-end coverage lives in the integration gates.
"""
from contextlib import contextmanager
from dataclasses import asdict, replace
import json
from pathlib import Path
import socket
import threading
import time
from types import SimpleNamespace
import uuid

import pytest

from demiflow import data
from demiflow.execution.contracts.dataflow import (
    CutRequest, RecoveryImage, WorkerLease, thaw_record_value,
)
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.contracts.dataflow_codec import runtime_scope_id, encode_wire
from demiflow.execution.dataflow_hosts import ControlMailbox
from demiflow.execution.dataflow_services import make_runtime_services
from demiflow.execution.recovery_contract import document, identity
from demiflow.execution.worker.dataflow_execution import DataflowWorkerExecutor
from demiflow.execution.worker.runtime import WorkerRuntime
from test_runtime_v3_real_seams import prepared

TREE = Path(__file__).resolve().parents[1]


def _assert_tree_binding():
    import demiflow
    assert Path(demiflow.__file__).resolve().is_relative_to(TREE)


def _increment(row):
    return {'x': row['x'] + 1}


class DirectTransport:
    """A network boundary adapter; every authority decision stays native."""
    def __init__(self, driver):
        self.store, self.service = driver.store, driver.journal_service
        self.requests = []

    def request(self, event, *, deadline_ms):
        self.requests.append(event)
        return self.service.dispatch(event)

    def check_stop(self):
        self.service._action.context.check_stop()


@contextmanager
def worker_env(tmp_path, *, rows=None):
    _assert_tree_binding()
    ds = data.from_items([{'x': 1}, {'x': 2}] if rows is None else rows).map(_increment)
    workers = []
    with prepared(tmp_path, ds) as (execution, driver):
        def make(kind='source', *, load=True):
            nodes = {n.occurrence_id: n for n in execution._graph.nodes}
            stage = next(s for s in execution._physical.stages
                         if any(nodes[o].kind == kind for o in s.occurrences))
            serial = len(workers)
            grant = {'grant_id': 'b-grant-' + str(serial),
                'run_id': execution._action.context.ref.run_id,
                'attempt': execution._action.context.attempt, 'fence': execution._action.context.fence,
                'scope_id': runtime_scope_id(execution._physical.plan_id, execution._action.context.attempt),
                'allocation_id': 'b-alloc-' + str(serial), 'boot_id': 'b-boot-' + str(serial),
                'controller_epoch': execution._action.context.attempt,
                'plan_revision': execution._physical.plan_revision,
                'expires_at_ms': int(time.time() * 1000) + 60000}
            raw = document(grant)
            grant_ref = driver.store.put(iter([raw]), max_bytes=len(raw), media_type='application/json')
            lease = WorkerLease(grant['allocation_id'], grant['boot_id'], stage.stage_id,
                stage.partitions, 'local', 'in-process-test-boundary', grant_ref, grant['expires_at_ms'])
            assignment = execution._assignment(lease, stage)
            driver.journal_service.register_worker(lease, stage.occurrences)
            transport = DirectTransport(driver)
            services = make_runtime_services(assignment, transport=transport, cache=tmp_path / 'services-cache')
            mailbox = ControlMailbox(bounds=services.bounds, check_stop=services.check_stop)
            worker = DataflowWorkerExecutor(services=services, mailbox=mailbox)
            item = SimpleNamespace(worker=worker, services=services, mailbox=mailbox,
                                   assignment=assignment, transport=transport, grant=grant)
            workers.append(item)
            if load:
                worker.load(assignment)
            return item
        try:
            yield SimpleNamespace(make=make, execution=execution, driver=driver, workers=workers)
        finally:
            for item in workers:
                item.worker.close()


def _start_with_source_credit(worker):
    # This harness is the driver boundary: activation alone cannot spend a
    # per-worker copy of the whole run's unconfirmed source allowance.
    worker.start()
    worker.receive({'kind': 'resume', 'source_credit': {
        'rows': 64, 'bytes': worker.assignment.bounds.max_batch_bytes}})


def _cut(item, *, final=False, sequence=0):
    return CutRequest(identity(['B-cut', sequence]), sequence, item.assignment.plan_revision,
                      0, item.worker._participant_ids(), final)


def _events(item):
    return item.mailbox.drain(max_events=item.services.bounds.max_pending_events)


def _launch(item, tmp_path, *, cache=True):
    config = {'assignment': item.assignment, 'transport': item.transport,
              'mailbox': ControlMailbox(bounds=item.services.bounds, check_stop=item.services.check_stop),
              'entrypoint': 'dataflow_worker_v3', 'code_pin': item.assignment.sealed.code_pin_ref.digest,
              'limits': {'max_row_bytes': item.services.bounds.max_row_bytes,
                         'max_resident_bytes': item.services.bounds.max_resident_bytes},
              'bounds': asdict(item.services.bounds)}
    if cache:
        config['cache'] = str(tmp_path / 'runtime-cache')
    return {'directory': str(tmp_path), 'run_id': item.assignment.run_id,
            'scope_id': item.grant['scope_id'], 'allocation_id': item.grant['allocation_id'],
            'boot_id': item.grant['boot_id'], 'controller_epoch': item.grant['controller_epoch'],
            'capabilities': ['cpu'], 'grant': {k: v for k, v in item.grant.items() if k != 'plan_revision'},
            'artifact_digest_observed': item.assignment.sealed.code_pin_ref.digest, 'dataflow_v3': config}


def _activate(runtime, item):
    pin = item.assignment.sealed.code_pin_ref.digest
    runtime._execute('VerifyArtifactAndCapabilities',
        {'artifact_manifest_ref': {'manifest_id': pin, 'sha256': pin}, 'required_capabilities': ['cpu']},
        int(time.time() * 1000))
    runtime._execute('Restore', {'state_refs': []}, int(time.time() * 1000))
    runtime._execute('Activate', {'grant_ref': item.grant['grant_id']}, int(time.time() * 1000))


def test_b001(tmp_path):
    with worker_env(tmp_path) as env:
        item = env.make(load=False)
        assert item.worker.assignment is None and item.worker._state == 'constructed'
        assert not item.worker._kernels and not item.worker._readers
        assert not hasattr(item.services.journal, '_connection')
        item.worker.cancel('before-load', deadline_ms=100)
        item.worker.close()
        item.worker.close()
        with pytest.raises(DataflowError, match='FENCED'):
            item.worker.step(max_records=1, deadline_ms=100)


def test_b002(tmp_path, monkeypatch):
    with worker_env(tmp_path) as env:
        item = env.make(load=False)
        def forbidden(*args, **kwargs):
            raise AssertionError('worker opened the driver DB')
        with monkeypatch.context() as patch:
            patch.setattr('demiflow.execution.dataflow_seal.RunJournal', forbidden)
            item.worker.load(item.assignment)
        assert item.worker._state == 'loaded' and not item.worker._readers
        stale = env.make(load=False)
        with pytest.raises(DataflowError, match='FENCED'):
            stale.worker.load(replace(stale.assignment, lease=replace(stale.assignment.lease, boot_id='wrong-boot')))
        wrong_plan = env.make(load=False)
        with pytest.raises(DataflowError, match='FENCED'):
            wrong_plan.worker.load(replace(wrong_plan.assignment, plan_id='f' * 64))
        bad = env.make(load=False)
        pin_ref = bad.assignment.sealed.code_pin_ref
        pin = json.loads(b''.join(env.driver.store.read(pin_ref, max_bytes=env.driver.bounds.max_control_bytes)))
        pin['programs'] = {}
        raw = document(pin)
        wrong_pin = env.driver.store.put(iter([raw]), max_bytes=len(raw), media_type=pin_ref.media_type)
        assignment = replace(bad.assignment, sealed=replace(bad.assignment.sealed, code_pin_ref=wrong_pin))
        with pytest.raises(DataflowError, match='CODE_MISMATCH'):
            bad.worker.load(assignment)
        assert not bad.worker._readers and not bad.worker._kernels


def test_b003(tmp_path):
    with worker_env(tmp_path) as env:
        old = env.make()
        _start_with_source_credit(old.worker)
        cut = _cut(old)
        contribution = old.worker.snapshot(cut)
        image = RecoveryImage(identity('B-commit'), cut, (contribution,), {}, contribution.sources)
        resumed = env.make()
        resumed.worker.restore(image)
        assert resumed.worker._state == 'restored'
        assert set(resumed.worker._restored_cursors) == {s.split_id for s in contribution.sources}
        assert resumed.worker._restored_states == dict(contribution.operator_states)
        _start_with_source_credit(resumed.worker)
        resumed.worker.run()
        resumed.worker.cancel('restored', deadline_ms=100)
        with pytest.raises(DataflowError, match='INVALID_CONTRACT'):
            replace(contribution, buffers=(None,))
        with pytest.raises(DataflowError, match='INVALID_CONTRACT'):
            replace(contribution, channels={'invalid-channel': None})


def test_b004(tmp_path, monkeypatch):
    with worker_env(tmp_path) as env:
        item = env.make()
        with pytest.raises(DataflowError, match='FENCED'):
            item.worker.step(max_records=1, deadline_ms=100)
        _start_with_source_credit(item.worker)
        reader = next(iter(item.worker._readers.values()))
        entered, release = threading.Event(), threading.Event()
        original = reader.next_record
        def slow_read():
            entered.set()
            assert release.wait(3)
            return original()
        monkeypatch.setattr(reader, 'next_record', slow_read)
        errors = []
        def step():
            try:
                item.worker.step(max_records=1, deadline_ms=1000)
            except DataflowError as exc:
                errors.append(exc)
        thread = threading.Thread(target=step)
        thread.start()
        try:
            assert entered.wait(2)
            start = time.monotonic()
            item.worker.cancel('during-source-read', deadline_ms=100)
            assert time.monotonic() - start < .15
        finally:
            release.set()
            thread.join(3)
        assert not thread.is_alive()
        assert errors and errors[0].code == 'CANCELLED'
        before = reader.snapshot()
        assert item.worker.step(max_records=1, deadline_ms=100).end_of_input
        assert reader.snapshot() == before


def test_b005(tmp_path, monkeypatch):
    from demiflow.execution.exchange.dataflow_channels import reserve_credit
    with worker_env(tmp_path) as env:
        producer, consumer = env.make(), env.make('transform')
        _start_with_source_credit(producer.worker)
        _start_with_source_credit(consumer.worker)
        spec = producer.assignment.outputs[0]
        grant = reserve_credit(spec.channel_id, spec.consumer_partitions[0], rows=spec.credit_rows,
            size_bytes=0, operation_id=identity('B-exhaust-credit'), state=producer.worker._channels)
        errors, finished = [], threading.Event()
        def step():
            try:
                producer.worker.step(max_records=1, deadline_ms=1000)
            except BaseException as exc:
                errors.append(exc)
            finally:
                finished.set()
        thread = threading.Thread(target=step)
        thread.start()
        try:
            assert not finished.wait(.1)
            assert not [e for e in _events(producer) if e.kind == 'data']
            assert all(not k.pending_calls for k in consumer.worker._kernels.values())
        finally:
            producer.worker._channels.release(grant)
            thread.join(3)
        assert not thread.is_alive() and not errors
        event = next(e for e in _events(producer) if e.kind == 'data')
        payload = thaw_record_value(event.payload)
        import demiflow.execution.worker.dataflow_graph_runner as runner_module
        original_read = runner_module.read_peer
        decoded = []
        def read_peer(*args, **kwargs):
            decoded.append(True)
            return original_read(*args, **kwargs)
        monkeypatch.setattr(runner_module, 'read_peer', read_peer)
        with pytest.raises(DataflowError, match='FENCED'):
            consumer.worker._runner._receive_data(dict(payload, producer_partition='foreign'))
        assert not decoded
        consumer.worker.receive(dict(payload, kind='data'))
        assert consumer.worker.step(max_records=1, deadline_ms=1000).processed >= 1
        assert decoded
        ack = next(e for e in _events(consumer) if e.kind == 'ack')
        producer.worker.receive(dict(thaw_record_value(ack.payload), kind='ack'))
        assert not producer.worker._runner._credits


def test_b006(tmp_path):
    with worker_env(tmp_path, rows=[]) as env:
        item = env.make()
        _start_with_source_credit(item.worker)
        item.worker.step(max_records=1, deadline_ms=100)
        assert not [e for e in _events(item) if e.kind == 'cut']
        cut = _cut(item, final=True)  # final is a driver request, not a worker decision.
        with pytest.raises(DataflowError, match='FENCED'):
            item.worker.request_cut(replace(cut, plan_revision=cut.plan_revision + 1))
        with pytest.raises(DataflowError, match='INVALID_CONTRACT'):
            item.worker.request_cut(replace(cut, participants=()))
        item.worker.request_cut(cut)
        item.worker.step(max_records=1, deadline_ms=100)
        assert any(e.kind == 'cut' for e in _events(item))
        with pytest.raises(DataflowError, match='INVALID_CONTRACT'):
            replace(cut, cut_id='')


def test_b007(tmp_path):
    with worker_env(tmp_path, rows=[]) as env:
        item = env.make()
        _start_with_source_credit(item.worker)
        cut = _cut(item)
        contribution = item.worker.snapshot(cut)
        assert not contribution.final and contribution.participant_id in cut.participants
        assert contribution.boot_id == item.assignment.lease.boot_id
        assert contribution.buffers and contribution.operator_states
        assert contribution.sources and all(s.next_sequence == 0 for s in contribution.sources)
        for ref in contribution.buffers:
            assert env.driver.store.inspect(ref)


def test_b008(tmp_path):
    with worker_env(tmp_path) as env:
        item = env.make()
        _start_with_source_credit(item.worker)
        cut = _cut(item)
        item.worker.snapshot(cut)
        item.worker.accept_receipt(identity('B-receipt'), cut_id=cut.cut_id)
        item.worker.accept_receipt(identity('B-receipt'), cut_id=cut.cut_id)
        assert cut.cut_id not in item.worker._snapshot_contributions
        with pytest.raises(DataflowError, match='conflicting receipt'):
            item.worker.accept_receipt(identity('B-conflict'), cut_id=cut.cut_id)
        with pytest.raises(DataflowError, match='unknown cut'):
            item.worker.accept_receipt(identity('B-receipt'), cut_id=identity('unknown'))


def test_b009(tmp_path):
    with worker_env(tmp_path) as env:
        item = env.make()
        _start_with_source_credit(item.worker)
        ref = env.driver.store.put(iter([b'unknown-effect']), max_bytes=14, media_type='application/octet-stream')
        item.worker._effect_refs.append(ref)
        before = {k: r.snapshot() for k, r in item.worker._readers.items()}
        item.worker.cancel('user', deadline_ms=100)
        assert item.worker._state == 'draining' and item.worker._cancel_event.is_set()
        assert item.worker.step(max_records=1, deadline_ms=100).end_of_input
        assert item.worker._effect_refs == [ref]
        assert {k: r.snapshot() for k, r in item.worker._readers.items()} == before


def test_b010(tmp_path, monkeypatch):
    with worker_env(tmp_path) as env:
        source, item = env.make(), env.make('transform')
        _start_with_source_credit(source.worker)
        _start_with_source_credit(item.worker)
        kernel = next(iter(item.worker._kernels.values()))
        # A real acquired reader is moved into this executor solely to exercise
        # close fan-out after another owned resource fails; no business execution.
        item.worker._readers.update(source.worker._readers)
        source.worker._readers.clear()
        readers = list(item.worker._readers.values())
        closed = []
        for reader in readers:
            close = reader.close
            def observed(close=close):
                closed.append(True)
                close()
            monkeypatch.setattr(reader, 'close', observed)
        original = kernel.close
        cleanup_error = RuntimeError('owned kernel close failed')
        def broken_close():
            original()
            raise cleanup_error
        monkeypatch.setattr(kernel, 'close', broken_close)
        with pytest.raises(ExceptionGroup, match='cleanup failed') as raised:
            item.worker.close()
        assert raised.value.exceptions == (cleanup_error,)
        assert len(closed) == len(readers) > 0 and item.mailbox._closed
        assert not item.worker._readers and not item.worker._kernels
        item.worker.close()


def test_b011(tmp_path):
    with worker_env(tmp_path) as env:
        item = env.make(load=False)
        launch = _launch(item, tmp_path)
        runtime = WorkerRuntime(launch)
        assert runtime._v3 and runtime.state == 'REGISTERED' and runtime.authority is None
        assert runtime._v3_executor is None and runtime._limits
        no_limits = dict(launch, dataflow_v3=dict(launch['dataflow_v3']))
        del no_limits['dataflow_v3']['limits']
        with pytest.raises(DataflowError, match='INVALID_CONTRACT'):
            WorkerRuntime(no_limits)
        no_pin = dict(launch, dataflow_v3=dict(launch['dataflow_v3']))
        del no_pin['artifact_digest_observed']
        del no_pin['dataflow_v3']['code_pin']
        with pytest.raises(DataflowError, match='CODE_MISMATCH'):
            WorkerRuntime(no_pin)
        runtime.close()


def test_b012(tmp_path, monkeypatch):
    from demiflow.execution.contracts.errors import ProtocolError
    with worker_env(tmp_path) as env:
        item = env.make(load=False)
        runtime = WorkerRuntime(_launch(item, tmp_path))
        pin = item.assignment.sealed.code_pin_ref.digest
        try:
            with pytest.raises(DataflowError, match='CODE_MISMATCH'):
                runtime._verify({'manifest_id': pin, 'sha256': '0' * 64})
            assert runtime._v3_executor is None
            def forbidden(*args, **kwargs):
                raise AssertionError('worker constructed a driver journal')
            with monkeypatch.context() as patch:
                patch.setattr('demiflow.execution.dataflow_seal.RunJournal', forbidden)
                runtime._verify({'manifest_id': pin, 'sha256': pin})
            assert runtime._v3_executor._state == 'loaded' and runtime.authority is None
            runtime._v3 = False
            with pytest.raises(ProtocolError) as exc:
                runtime._verify({'manifest_id': pin, 'sha256': pin})
            assert exc.value.code == 'INVALID_SPEC'
        finally:
            runtime._v3 = True
            runtime.close()


def test_b013(tmp_path):
    with worker_env(tmp_path) as env:
        item = env.make(load=False)
        runtime = WorkerRuntime(_launch(item, tmp_path))
        try:
            checksums = runtime._restore([{'sha256': 'a' * 64}])
            assert checksums == ['a' * 64]
            assert runtime._v3_executor._state == 'restored' and not runtime._v3_executor._readers
        finally:
            runtime.close()


def test_b014(tmp_path):
    from demiflow.execution.contracts.errors import ProtocolError
    with worker_env(tmp_path) as env:
        item = env.make(load=False)
        runtime = WorkerRuntime(_launch(item, tmp_path))
        try:
            with pytest.raises(ProtocolError) as error:
                runtime._activate({'grant_ref': 'wrong-grant'}, int(time.time() * 1000))
            assert error.value.code == 'GRANT_REVOKED' and runtime._v3_executor is None
            _activate(runtime, item)
            assert runtime.state == 'ACTIVATED' and runtime._v3_executor._state == 'started'
            assert runtime.authority is None and runtime._v3_executor._runner._thread is None
        finally:
            runtime.close()


def test_b015(tmp_path):
    with worker_env(tmp_path) as env:
        item = env.make(load=False)
        launch = _launch(item, tmp_path)
        launch['dataflow_v3']['assignment'] = {'wire': encode_wire(item.assignment,
            type_name='WorkerAssignment', bounds=item.services.bounds).decode()}
        runtime = WorkerRuntime(launch)
        try:
            pin = item.assignment.sealed.code_pin_ref.digest
            receipt = runtime._execute('VerifyArtifactAndCapabilities',
                {'artifact_manifest_ref': {'manifest_id': pin, 'sha256': pin},
                 'required_capabilities': ['cpu']}, int(time.time() * 1000))
            assert receipt['kind'] == 'Verified' and runtime.state == 'VERIFIED'
            assert runtime._v3_executor.assignment == item.assignment and runtime.authority is None
        finally:
            runtime.close()


def test_b016(tmp_path):
    with worker_env(tmp_path) as env:
        item = env.make(load=False)
        runtime = WorkerRuntime(_launch(item, tmp_path))
        try:
            _activate(runtime, item)
            runtime._drain({'reason': 'done', 'deadline_ms': 100})
            assert not runtime._admit_open and runtime._v3_executor._state == 'draining'
            assert runtime._v3_executor.step(max_records=1, deadline_ms=100).end_of_input
        finally:
            runtime.close()


def test_b017(tmp_path):
    from demiflow.execution.contracts.errors import ProtocolError
    with worker_env(tmp_path) as env:
        item = env.make(load=False)
        fresh = WorkerRuntime(_launch(item, tmp_path))
        fresh._execute('Cancel', {'reason': 'before-activation', 'deadline_ms': 100}, int(time.time() * 1000))
        assert fresh.state == 'RETIRED' and fresh._v3_executor is None and not fresh._admit_open
        with pytest.raises(ProtocolError):
            fresh._execute('Activate', {'grant_ref': item.grant['grant_id']}, int(time.time() * 1000))
        fresh.close()
        runtime = WorkerRuntime(_launch(item, tmp_path))
        try:
            _activate(runtime, item)
            runtime._cancel({'reason': 'stop', 'deadline_ms': 100})
            assert runtime._v3_executor._cancel_event.is_set()
            assert runtime._v3_executor.step(max_records=1, deadline_ms=100).processed == 0
        finally:
            runtime.close()


def test_b018(tmp_path):
    with worker_env(tmp_path) as env:
        item = env.make(load=False)
        for cache in (True, False):
            launch = _launch(item, tmp_path, cache=cache)
            if cache:
                # A by-value source may never need to create a code cache. Seed
                # this assignment's existing private cache to test real cleanup.
                configured = Path(launch['dataflow_v3']['cache'])
                prior = configured / item.assignment.lease.allocation_id
                prior.mkdir(parents=True)
                (prior / 'owned').write_text('this assignment')
                sibling = configured / 'another-allocation'
                sibling.mkdir()
                (sibling / 'protected').write_text('another owner')
            runtime = WorkerRuntime(launch)
            try:
                _activate(runtime, item)
                owned = runtime._v3_executor._owned_services_cache
                assert owned is not None and owned.exists()
                if cache:
                    assert owned == configured / item.assignment.lease.allocation_id
            finally:
                runtime.close()
            assert runtime._v3_executor is None and not owned.exists()
            if cache:
                assert (sibling / 'protected').read_text() == 'another owner'
            runtime.close()


def test_b019():
    from demiflow.execution.worker.bootstrap import _validate_v3_launch
    good = {'dataflow_v3': {'entrypoint': 'dataflow_worker_v3', 'code_pin': 'd' * 64, 'assignment': {}}}
    _validate_v3_launch(good)
    with pytest.raises(DataflowError, match='INVALID_CONTRACT'):
        _validate_v3_launch({'dataflow_v3': dict(good['dataflow_v3'], entrypoint='wrong')})
    with pytest.raises(DataflowError, match='CODE_MISMATCH'):
        _validate_v3_launch({'dataflow_v3': dict(good['dataflow_v3'], code_pin='short')})


def _bootstrap_launch(tmp_path):
    from demiflow.execution.runtime_limits import execution_limits
    from demiflow.planning.dataflow_policy import resolve_runtime_policy
    _, bounds = resolve_runtime_policy({}, execution_limits(), provider='local')
    # No assignment is decoded for Register -> Cancel before Verify/Activate.
    grant = {'grant_id': 'g', 'run_id': 'run', 'attempt': 1, 'fence': 'fence',
             'scope_id': 'action', 'allocation_id': 'alloc', 'boot_id': 'boot',
             'controller_epoch': 1, 'expires_at_ms': int(time.time() * 1000) + 60000}
    return {'directory': str(tmp_path), 'run_id': 'run', 'scope_id': 'action',
        'allocation_id': 'alloc', 'boot_id': 'boot', 'controller_epoch': 1,
        'capabilities': ['cpu'], 'grant': grant,
        'dataflow_v3': {'assignment': None, 'limits': {'max_row_bytes': 1024},
            'code_pin': 'd' * 64, 'bounds': asdict(bounds), 'store_root': str(tmp_path / 'store'), 'store_id': 'test-store'}}


def test_b020(tmp_path):
    from demiflow.execution.worker.bootstrap import _run_v3_worker
    launch = _bootstrap_launch(tmp_path)
    driver_sock, child_sock = socket.socketpair()
    driver_sock.settimeout(5)  # bootstrap failure must fail, never hang the suite
    result = {}

    def worker():
        try:
            result["runtime"] = _run_v3_worker(launch, child_sock)
        except BaseException as exc:
            result["error"] = exc

    thread = threading.Thread(target=worker)
    thread.start()
    # driver reads the Register envelope then sends Cancel
    from demiflow.execution.worker.bootstrap import recv_message, send_message
    register = recv_message(driver_sock)
    assert register["kind"] == "Register"
    cancel = {
        "protocol_version": {"major": 1, "minor": 0}, "kind": "Cancel",
        "scope_id": "action", "run_id": "run", "operation_id": uuid.uuid4().hex,
        "controller_epoch": 1, "authority_ref": "g", "allocation_id": "alloc",
        "boot_id": "boot", "payload": {"operation_id": uuid.uuid4().hex, "reason": "test"},
    }
    send_message(driver_sock, cancel)
    reply = recv_message(driver_sock)
    assert reply["response_to"] == cancel["operation_id"]
    thread.join(timeout=5)
    driver_sock.close()
    assert not thread.is_alive()
    assert "error" not in result, result.get("error")
    assert result["runtime"].state == "RETIRED"

def test_source_activation_does_not_pull_before_driver_credit(tmp_path):
    with worker_env(tmp_path) as env:
        item = env.make()
        item.worker.start()
        reader = next(iter(item.worker._readers.values()))
        before = reader.snapshot()
        item.worker.step(max_records=1, deadline_ms=100)
        assert reader.snapshot() == before
        assert not [event for event in _events(item) if event.kind == 'data']
        item.worker.receive({'kind': 'resume', 'source_credit': {
            'rows': 1, 'bytes': item.assignment.bounds.max_batch_bytes}})
        item.worker.step(max_records=1, deadline_ms=100)
        assert any(event.kind == 'data' for event in _events(item))
        assert reader.snapshot().next_sequence == before.next_sequence + 1
