"""Control deadlines, participating-thread CPU, and durable boundary facts."""
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import sys
import threading
import time
from types import SimpleNamespace

import pytest
from test_forge_runs import base, root

from demiflow.execution.run_journal import RunJournal
from demiflow.forge import operation_budget as budget
from demiflow.forge import platform_worker
from demiflow.workbench.runs import domain as run_domain
from demiflow.workbench.runs import requests as run_requests
from demiflow.workbench.runs import manager as runs
from demiflow.forge.platform import content_digest
from demiflow.forge.schema import ForgeError


class Clock:
    def __init__(self, now=100.):
        self.now = now

    def monotonic(self):
        return self.now


@pytest.fixture(autouse=True)
def private_budget(tmp_path, monkeypatch):
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN', str(tmp_path / 'domain'))
    monkeypatch.delenv(budget.SCOPE_ENV, raising=False)
    monkeypatch.delenv(budget.DEADLINE_ENV, raising=False)
    monkeypatch.setattr(budget, '_worker_bound', None)
    token = budget._CURRENT.set(None)
    try:
        yield
    finally:
        budget._CURRENT.reset(token)


def test_nested_remaining_independent_calls_and_finally_reset(monkeypatch):
    clock = Clock()
    monkeypatch.setattr(budget, 'time', clock)
    with budget.scope() as outer:
        assert outer == 160
        clock.now = 130
        with budget.scope() as inner:
            assert inner == outer
            assert budget.child_deadline(40) == 160
            assert budget.child_deadline(5) == 135
        with pytest.raises(RuntimeError):
            with budget.scope(deadline=140):
                raise RuntimeError('injected')
        assert budget.current_deadline() == outer
        clock.now = 161  # A committed result can still return after its bound.
    assert budget.current_deadline() is None
    with budget.scope() as fresh:
        assert fresh == 221
    with pytest.raises(ForgeError, match='deadline exceeded'):
        with budget.scope(deadline=160):
            pytest.fail('Expired scope entered')
    assert budget.current_deadline() is None


@pytest.mark.parametrize('deadline', [float('nan'), float('inf'), True, '160'])
def test_invalid_scope_refuses_without_context_leak(deadline):
    with pytest.raises(ForgeError, match='Invalid control deadline'):
        with budget.scope(deadline=deadline):
            pass
    assert budget.current_deadline() is None


def test_resident_absence_does_not_install_startup_deadline(monkeypatch):
    clock = Clock()
    monkeypatch.setattr(budget, 'time', clock)
    budget.bind_worker()
    assert budget._worker_bound is None
    with budget.scope():
        clock.now += 61
    with budget.scope() as second:
        assert second == 221
    assert budget.current_deadline() is None


@pytest.mark.parametrize('marker,raw', [
    ('other', '150000000000'), ('oneshot-v1', 'nan'),
    ('oneshot-v1', '١٥٠'), ('oneshot-v1', '0'),
    ('oneshot-v1', '161000000000'), ('oneshot-v1', '1' * 21),
    (None, '150000000000'), ('oneshot-v1', None),
])
def test_untrusted_or_expanded_worker_transport_refuses(monkeypatch, marker, raw):
    monkeypatch.setattr(budget, 'time', Clock())
    if marker is not None:
        monkeypatch.setenv(budget.SCOPE_ENV, marker)
    if raw is not None:
        monkeypatch.setenv(budget.DEADLINE_ENV, raw)
    with pytest.raises(ForgeError):
        budget.bind_worker()
    assert budget._worker_bound is None


def test_validated_worker_bound_is_retained_and_transport_removed(monkeypatch):
    clock = Clock()
    monkeypatch.setattr(budget, 'time', clock)
    for name, value in budget.worker_environment(150).items():
        monkeypatch.setenv(name, value)
    budget.bind_worker()
    assert budget.current_deadline() == 150
    assert budget.SCOPE_ENV not in os.environ and budget.DEADLINE_ENV not in os.environ
    clock.now = 145
    with budget.scope() as inherited:
        assert inherited == budget.child_deadline(60) == 150
    clock.now = 151
    with pytest.raises(ForgeError, match='deadline exceeded'):
        budget.check()
    for name, value in budget.worker_environment(155).items():
        monkeypatch.setenv(name, value)
    with pytest.raises(ForgeError, match='cannot rebind'):
        budget.bind_worker()


def test_one_shot_parent_binding_transports_remaining_without_global_env():
    source = str(Path(runs.__file__).resolve().parents[3])
    program = f'''import sys, json, os
sys.path.insert(0, {source!r})
from demiflow.forge.platform_worker import _bind_parent
from demiflow.forge import operation_budget as b
_bind_parent()
with b.scope() as bound:
    print(json.dumps(dict(format='budget_test', deadline=bound,
        transport_cleared=b.SCOPE_ENV not in os.environ and b.DEADLINE_ENV not in os.environ)))
'''
    before = dict(os.environ)
    with budget.scope(deadline=time.monotonic() + 10) as parent:
        result = platform_worker.capture([sys.executable, '-I', '-B', '-c', program], {},
            dict(timeout_seconds=30, catalog_bytes=4096, stderr_bytes=4096),
            response_formats={'budget_test'})
        assert abs(result['deadline'] - parent) < .000001
        assert budget.current_deadline() == parent
    assert result['transport_cleared'] and dict(os.environ) == before
    assert budget.current_deadline() is None


def test_exited_ready_response_is_drained_after_deadline(monkeypatch):
    clock = Clock()
    monkeypatch.setattr(budget, 'time', clock)
    monkeypatch.setattr(platform_worker, 'time', clock)
    killed = []

    def launch(*args, **kwargs):
        assert kwargs['env'][budget.DEADLINE_ENV] == str(160 * 10**9)
        pipes = []
        for payload in (b'{"format":"budget_test","status":"ready"}', b''):
            reader, writer = os.pipe()
            os.write(writer, payload)
            os.close(writer)
            pipes.append(os.fdopen(reader, 'rb'))
        clock.now = 161
        return SimpleNamespace(pid=987654321, stdout=pipes[0], stderr=pipes[1],
            returncode=0, poll=lambda: 0, wait=lambda **kw: 0)

    monkeypatch.setattr(platform_worker.subprocess, 'Popen', launch)
    monkeypatch.setattr(platform_worker.os, 'killpg', lambda pid, sig: killed.append(pid))
    result = platform_worker.capture(['private-test'], {},
        dict(timeout_seconds=60, catalog_bytes=4096, stderr_bytes=4096),
        response_formats={'budget_test'})
    assert result['status'] == 'ready' and killed == [987654321]
    assert budget.current_deadline() is None


def test_exited_worker_with_open_reply_pipe_does_not_wait_past_deadline(monkeypatch):
    clock = Clock()
    monkeypatch.setattr(budget, 'time', clock)
    monkeypatch.setattr(platform_worker, 'time', clock)
    writers = []

    def launch(*args, **kwargs):
        pipes = []
        for _ in range(2):
            reader, writer = os.pipe()
            writers.append(writer)  # A descendant still owns these pipes.
            pipes.append(os.fdopen(reader, 'rb'))
        clock.now = 161
        return SimpleNamespace(pid=987654321, stdout=pipes[0], stderr=pipes[1],
            returncode=0, poll=lambda: 0, wait=lambda **kw: 0)

    monkeypatch.setattr(platform_worker.subprocess, 'Popen', launch)
    monkeypatch.setattr(platform_worker.os, 'killpg', lambda *a: None)
    try:
        with pytest.raises(ForgeError, match='response incomplete at deadline'):
            platform_worker.capture(['private-test'], {},
                dict(timeout_seconds=60, catalog_bytes=4096, stderr_bytes=4096),
                response_formats={'budget_test'})
    finally:
        for writer in writers:
            os.close(writer)
    assert budget.current_deadline() is None


def test_expired_copy_does_not_begin_a_new_file(tmp_path, monkeypatch):
    clock = Clock()
    monkeypatch.setattr(budget, 'time', clock)

    def read(*args):
        clock.now = 161
        return b'checked-source'

    monkeypatch.setattr(runs, 'read_bounded', read)
    target = tmp_path / 'new-seal'
    with budget.scope(), pytest.raises(ForgeError, match='deadline exceeded'):
        runs._copy_checked(tmp_path / 'source', target, 'unused', 4096)
    assert not target.exists()


def test_cpu_retains_all_four_exited_thread_totals(monkeypatch):
    main = threading.get_ident()
    clocks = {}

    def cpu_time():
        if threading.get_ident() == main:
            return 100
        ident = threading.get_ident()
        clocks[ident] = clocks.get(ident, 0) + 1
        return min(clocks[ident], 2) * 10**9

    monkeypatch.setattr(run_domain, 'time', SimpleNamespace(thread_time_ns=cpu_time))
    meter = run_domain._HistoryCPU(100)
    barrier = threading.Barrier(4, timeout=3)
    with ThreadPoolExecutor(max_workers=4, initializer=meter.start_worker) as pool:
        futures = [pool.submit(barrier.wait) for _ in range(4)]
        for future in futures:
            future.result()
    meter.finish_main()
    assert len(meter._finished) == len(meter._clocks) == 5
    assert meter.used_ns == 8 * 10**9
    with pytest.raises(ForgeError, match='used 8.000000s'):
        meter.require_complete()


def test_unrelated_web_thread_cpu_is_excluded():
    meter = run_domain._HistoryCPU(time.thread_time_ns())
    other = []

    def unrelated_request():
        started = time.thread_time_ns()
        while time.thread_time_ns() - started < 50_000_000:
            pass
        other.append(time.thread_time_ns() - started)

    thread = threading.Thread(target=unrelated_request)
    thread.start()
    thread.join()
    meter.finish_main()
    meter.require_complete()
    assert len(meter._clocks) == 1 and other[0] >= 50_000_000
    assert meter.used_ns < other[0] // 2


def test_missing_final_cpu_coverage_refuses():
    meter = run_domain._HistoryCPU(time.thread_time_ns())
    with pytest.raises(ForgeError, match='final CPU coverage'):
        meter.require_complete()


def test_history_slow_marker_and_wait_limit_are_separate(monkeypatch):
    clock = Clock()
    monkeypatch.setattr(run_domain, 'time', SimpleNamespace(
        monotonic=clock.monotonic, thread_time_ns=time.thread_time_ns))
    scan = run_domain._HistoryScan()
    clock.now += 6
    scan.check()
    assert scan.slow
    clock.now = 130
    with pytest.raises(ForgeError, match='wait budget'):
        scan.check()


def test_history_inherits_shorter_operation_deadline(monkeypatch):
    clock = Clock()
    monkeypatch.setattr(budget, 'time', clock)
    monkeypatch.setattr(run_domain, 'time', SimpleNamespace(
        monotonic=clock.monotonic, thread_time_ns=time.thread_time_ns))
    with budget.scope(deadline=108):
        scan = run_domain._HistoryScan()
        assert scan.deadline == 108
        clock.now = 108
        with pytest.raises(ForgeError, match='operation deadline'):
            scan.check()


def test_history_cpu_refusal_drains_and_next_request_is_independent(tmp_path, monkeypatch):
    target = tmp_path / '.forge/runs/one'
    target.mkdir(parents=True)
    (target / 'data').write_bytes(b'x')
    original = set(os.sched_getaffinity(0))
    with monkeypatch.context() as patch:
        patch.setattr(run_domain, '_HISTORY_CPU_SECONDS', .000001)
        with pytest.raises(ForgeError, match='History CPU budget exceeded; used'):
            run_domain.history_admission(tmp_path, reserve=0)
    assert not [t for t in threading.enumerate() if t.name.startswith('forge-history')]
    assert set(os.sched_getaffinity(0)) == original
    assert budget.current_deadline() is None
    assert run_domain.history_admission(tmp_path, reserve=0) == 1


def test_native_worker_environment_never_inherits_control_bound(tmp_path, monkeypatch):
    for name, value in budget.worker_environment(time.monotonic() + 10).items():
        monkeypatch.setenv(name, value)
    with budget.scope():
        env = runs.worker_environment(tmp_path)
    assert budget.SCOPE_ENV not in env and budget.DEADLINE_ENV not in env
    assert env['TOKIO_WORKER_THREADS'] == env['LANCE_CPU_THREADS'] == '2'
    with pytest.raises(ForgeError, match='Credential name conflicts'):
        runs.worker_environment(tmp_path, credential_names=[budget.DEADLINE_ENV])


@pytest.mark.parametrize('late_outcome', ['ready', 'unknown'])
def test_request_receipt_keeps_late_ready_or_unknown_seal(tmp_path, monkeypatch, late_outcome):
    (tmp_path / '.forge').mkdir()
    clock = Clock()
    monkeypatch.setattr(budget, 'time', clock)
    identity = run_requests.request_id()
    created = []

    def prepare_operation(name, root, **arguments):
        target = runs.directory(root, arguments['reserved_run_id'])
        journal = RunJournal(target)
        journal.initialize(dict(run_id=arguments['reserved_run_id']), attempts=True)
        journal = RunJournal(target, attempt=1)
        journal.transition({None}, 'prepared')
        created.append(target)
        clock.now = 161
        if late_outcome == 'unknown':
            raise ForgeError('Worker stopped without a response', code='resource_limit')
        return dict(run_id=arguments['reserved_run_id'], digest='late-ready-digest')

    with budget.scope():
        if late_outcome == 'unknown':
            with pytest.raises(ForgeError, match='without a response'):
                run_requests.prepare(tmp_path, 'flows/private.py:run', {}, prepare_operation, identity)
        else:
            assert run_requests.prepare(tmp_path, 'flows/private.py:run', {}, prepare_operation, identity)['digest'] == 'late-ready-digest'
    receipt = run_requests.read(tmp_path, identity)
    assert receipt['status'] == ('ready' if late_outcome == 'ready' else 'pending')
    assert len(created) == 1 and created[0].is_dir()
    state = RunJournal(created[0], attempt=1).execution_status()
    assert state['status'] == 'prepared'
    assert state.get('preparation_ready', False) == (late_outcome == 'ready')


def minimal_run(root, monkeypatch, status='prepared'):
    """Real SQLite/unit/lease state; omit unrelated inspection and seal copies."""
    from demiflow.workbench.projects import index
    from demiflow.workbench import worker as workspace_worker
    run_id = 'r' + 'a' * 24
    target = runs.directory(root, run_id)
    manifest = dict(format=runs.FORMAT, root=str(root), run_id=run_id, outputs={},
        platform={'files': [{'path': 'workbench/runs/worker.py'}]},
        entry='flows/private.py:run', created_at=time.time(), parameter_path='flows/private.json', report_digest='b' * 64,
        observation_instances=[], object_locations=[], limits=dict(runs.DEFAULT_LIMITS))
    manifest['digest'] = content_digest(manifest)
    journal = RunJournal(target)
    journal.initialize(manifest, attempts=True)
    journal = RunJournal(target, attempt=1)
    journal.transition({None}, status, cleanup='complete', execution_stopped=True, recoverable=True)
    journal.record_usage(elapsed_ms=0, output_bytes=0, complete=True)

    def show(*args, **kwargs):
        state = RunJournal(target).execution_status()
        return dict(run_id=run_id, digest=manifest['digest'], attempt=state['attempt'], status=state['status'], state=state)

    monkeypatch.setattr(runs, 'show', show)
    monkeypatch.setattr(index, 'begin', lambda *a, **kw: None)
    monkeypatch.setattr(index, 'publish', lambda *a, **kw: None)
    monkeypatch.setattr(workspace_worker, 'operation', lambda *a, **kw: dict(status='checked'))
    monkeypatch.setattr(run_domain, 'history_admission', lambda *a, **kw: 0)
    return target, journal, manifest


@pytest.mark.parametrize('boundary', ['reservation_overrun', 'before_fork', 'possible_fork', 'returned_fork'])
def test_launch_deadline_preserves_real_journal_and_ownership(tmp_path, monkeypatch, boundary):
    target, journal, manifest = minimal_run(tmp_path, monkeypatch)
    clock = Clock()
    monkeypatch.setattr(budget, 'time', clock)
    reservations, releases, launches = [], [], []

    def reserve(*args):
        result = dict(slot=0)
        reservations.append(result)
        if boundary == 'reservation_overrun':
            clock.now = 161
        return result

    def environment(*args, **kwargs):
        if boundary == 'before_fork':
            clock.now = 161
        return {}

    def launch(*args, **kwargs):
        launches.append(kwargs)
        clock.now = 161
        if boundary == 'possible_fork':
            raise OSError('Fork outcome unavailable')
        return SimpleNamespace(pid=987654321)

    monkeypatch.setattr(run_domain, 'reserve', reserve)
    monkeypatch.setattr(run_domain, 'abandon', lambda *a, **kw: releases.append(kw))
    monkeypatch.setattr(runs, 'worker_environment', environment)
    monkeypatch.setattr(runs.subprocess, 'Popen', launch)
    monkeypatch.setattr(runs, 'process_identity', lambda pid: dict(pid=pid, starttime=1))
    if boundary == 'returned_fork':
        assert runs.start(tmp_path, manifest['run_id'], expected=manifest['digest'])['status'] == 'starting'
    else:
        with pytest.raises((ForgeError, OSError)):
            runs.start(tmp_path, manifest['run_id'], expected=manifest['digest'])
    state = journal.execution_status()
    assert len(reservations) == 1
    assert (target / 'unit-1.lock').is_file()
    assert budget.current_deadline() is None
    if boundary in {'reservation_overrun', 'before_fork'}:
        assert not launches and len(releases) == 1
        assert state['status'] == 'failed' and state['execution_stopped'] and state['usage_complete']
    else:
        assert len(launches) == 1 and not releases
        assert state['status'] == 'starting' and state['launch_phase'] == 'spawning'
        assert not state['execution_stopped'] and not state['usage_complete']
        if boundary == 'returned_fork':
            assert state['supervisor']['pid'] == 987654321


def test_cancel_remains_independent_of_expired_control_and_admission_locks(tmp_path, monkeypatch):
    target, journal, manifest = minimal_run(tmp_path, monkeypatch, status='running')
    clock = Clock()
    monkeypatch.setattr(budget, 'time', clock)
    from demiflow.workbench.projects.workspace import control_lock
    with run_domain.lock(), control_lock(tmp_path), budget.scope():
        clock.now = 161
        value = runs.cancel(tmp_path, manifest['run_id'], expected_attempt=1)
        assert value['status'] == 'stopping'
    assert journal.execution_status()['stop_phase'] == 'requested'


def test_resume_attempt_commit_survives_expiry_before_nested_start(root):
    from test_forge_runs import execute
    from test_workbench_native_submission import native_flow
    from test_forge_resume import stopped
    from _p12_bounded_test import bounded_test
    value = execute(root, native_flow(root))
    assert value['status'] == 'succeeded', value
    stopped(root, value['run_id'])
    journal = runs.journal(root, value['run_id'])
    assert journal.recovery_identity() is not None
    # A private fault state keeps the real execution identity/unit/publications.
    journal.transition({'succeeded'}, 'cancelled', execution_stopped=True, cleanup='complete')
    bounded_test(_expired_after_actual_resume_commit, root=root, value=value)


def _expired_after_actual_resume_commit(root, value, monkeypatch):
    from demiflow.workbench.projects import index
    target = runs.directory(root, value['run_id'])
    previous = runs.show(root, value['run_id'], attempt=1)
    original = RunJournal.begin_attempt
    current_deadline = budget.current_deadline
    deadlines = []
    def commit(self, **kwargs):
        deadlines.append(current_deadline())
        result = original(self, **kwargs)
        monkeypatch.setattr(budget, 'current_deadline', lambda: time.monotonic() - 1)
        return result
    monkeypatch.setattr(RunJournal, 'begin_attempt', commit)
    with pytest.raises(ForgeError) as caught, budget.scope():
        runs._resume(root, value['run_id'], expected=value['digest'], expected_attempt=1)
    assert caught.value.code == 'resource_limit'
    monkeypatch.setattr(budget, 'current_deadline', current_deadline)
    assert len(deadlines) == 1 and deadlines[0] is not None
    state = RunJournal(target).execution_status()
    assert state['attempt'] == 2 and state['status'] == 'prepared'
    assert index.status(root)['pending'] == 1
    assert runs.show(root, value['run_id'], attempt=1) == previous
    assert budget.current_deadline() is None
