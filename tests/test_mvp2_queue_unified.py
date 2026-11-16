"""MVP-2 segment 1 (A1) unified queue settlement carrier acceptance.

The driver keeps the legacy queue lifecycle byte-unchanged (claim/lease/heartbeat,
the single RawFetch acquisition and finalize_task); only the pure stateless maps
suffix after RawFetch runs in an independent managed worker, correlated per task
by its native delivery task_id. These tests drive the genuine workbench entry
(RunContext + local_execution(unified_queue_chain=...) + bound public data API),
prove the unified settlement equals the byte-unchanged legacy in-driver path,
prove the worker constructs zero CommitCoordinator (guardrail 2, absolute) and
that both commit scopes settle in the driver's single journal, and bind the
default-OFF seam's eligibility fallbacks.
"""
import ast
import json
import os
import signal
import sys
from dataclasses import asdict
from pathlib import Path

import pytest
from runtime_v3_owned_crash import kill_owned_worker

import demiflow.data as data
from demiflow.collect import FetchPolicy, HttpSource
from demiflow.execution.queue import QueueChannel, QueueConfig, ProducerSourceSpec, task_message
from demiflow.execution.queue_sinks import QueueResultSink
from demiflow.execution.recovery_contract import RecoveryContractError, RunRef
from demiflow.execution.run_context import RunContext
from demiflow.objects import LocalObjectStore
from test_queue_v2_boundaries import seed
from test_raw_fetch import origin_server, BODY

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"),
                                reason="Local managed units require Linux")

LIMITS = {"checkpoint_rows": 2, "checkpoint_interval_ms": 60000}


@pytest.fixture(autouse=True)
def pidfds_available():
    from demiflow.resources.process_observation import require_pidfds
    try:
        require_pidfds()
    except (OSError, RuntimeError) as exc:
        pytest.skip("pidfd exit evidence unavailable: " + str(exc))


def _config(tmp_path, name):
    return QueueConfig(backend='sqlite', queue_id=name, path=str(tmp_path / (name + '.sqlite')))


def _settle(cfg, root, *, url, **exec_kwargs):
    # Local closures: cloudpickle serializes the maps suffix by value so the
    # independent managed worker need not import this test module (a module-level
    # callable would serialize by reference and hang the worker's graph cut).
    class PureAnnotation:
        def __call__(self, row):
            return {**row, 'accepted': False, 'temporary': 'remove-me'}

    def receipt_status(result):
        return result['status']

    def annotate(status):
        return 'downloaded' if status == 'ok' else 'failed'

    with RunContext.start(root, limits=LIMITS):
        with data.local_execution(workers=1, worker_mode="thread", partitions=1, **exec_kwargs):
            ds = data.read_queue(cfg, pool='p', max_tasks=1, max_inflight=1, idle_timeout_s=.2).fetch(
                store=LocalObjectStore(root / 'objects'), concurrency=1,
                policy=FetchPolicy(max_object_bytes=131072, http=HttpSource(allow_private=True)))
            ds = (ds.map(PureAnnotation).drop_columns(['request', 'temporary'])
                  .rename_columns({'accepted': 'business_accepted'})
                  .select_columns(['result', 'business_accepted', '_queue_delivery'])
                  .map(receipt_status, inputs={'result': 'result'}, output='status')
                  .map(annotate, inputs={'status': 'status'}, output='label'))
            assert ds.write_datasink(QueueResultSink(cfg, pool='p')) is None


def _business(row):
    # Strip only the run/path-specific identifiers (object-store URI, timestamp,
    # native commit refs, storage domain); every business and acquisition fact
    # must be identical between the legacy and the unified carrier settlement.
    result = {k: v for k, v in row['result'].items()
              if k not in {'retrieved_at', 'acquisition_ref', 'storage_domain'}}
    if result.get('raw_ref'):
        result['raw_ref'] = {'sha256': result['raw_ref']['sha256']}
    return dict(result=result, business_accepted=row['business_accepted'],
                label=row['label'], status=row['status'], verification=row['verification'])


def _managed_worker_pids(resource_root):
    import json
    units = resource_root / "units"
    if not units.exists():
        return set()
    return {json.loads(p.read_text())["identity"]["pid"]
            for p in units.glob("*/*/process.json")}


def _resources(root):
    return root.with_name(root.name + "-resources")


def test_unified_queue_carrier_matches_legacy_settlement(tmp_path):
    legacy_cfg, unified_cfg = _config(tmp_path, 'legacy'), _config(tmp_path, 'unified')
    legacy_root, unified_root = tmp_path / 'legacy', tmp_path / 'unified'
    with origin_server() as (url, calls):
        seed(QueueChannel(legacy_cfg), tmp_path / 'legacy-seed', url)
        _settle(legacy_cfg, legacy_root, url=url, unified_queue_chain=False)
        legacy_fetches = calls['/file']
        seed(QueueChannel(unified_cfg), tmp_path / 'unified-seed', url)
        _settle(unified_cfg, unified_root, url=url, unified_queue_chain=True)
        unified_fetches = calls['/file'] - legacy_fetches
    # Exactly one acquisition per run: the carrier never re-fetches the object.
    assert legacy_fetches == 1 and unified_fetches == 1

    legacy_channel, unified_channel = QueueChannel(legacy_cfg), QueueChannel(unified_cfg)
    legacy_row, = data.read_queue_records(legacy_cfg, pool='p',
                                          through=legacy_channel.snapshot('p'), results=True).take_all()
    unified_row, = data.read_queue_records(unified_cfg, pool='p',
                                           through=unified_channel.snapshot('p'), results=True).take_all()
    assert _business(legacy_row) == _business(unified_row)
    assert unified_row['result']['status'] == 'ok'
    assert unified_row['business_accepted'] is False and unified_row['label'] == 'downloaded'
    assert legacy_channel.state('p')['queue_drained'] and unified_channel.state('p')['queue_drained']
    assert legacy_channel.state('p')['counts'] == unified_channel.state('p')['counts'] == {'done_ok': 1}

    # The unified path supervised a genuine independent managed worker process;
    # the byte-unchanged legacy in-driver path provisions no managed worker host.
    unified_pids = _managed_worker_pids(_resources(unified_root))
    assert unified_pids and os.getpid() not in unified_pids
    assert _managed_worker_pids(_resources(legacy_root)) == set()


def test_seam_explicit_off_uses_legacy_in_driver(tmp_path):
    cfg = _config(tmp_path, 'off')
    root = tmp_path / 'run'
    with origin_server() as (url, calls):
        seed(QueueChannel(cfg), tmp_path / 'seed', url)
        # Explicit unified_queue_chain=False: the byte-unchanged legacy path.
        _settle(cfg, root, url=url, unified_queue_chain=False)
    assert calls['/file'] == 1
    assert QueueChannel(cfg).state('p')['queue_drained']
    assert _managed_worker_pids(_resources(root)) == set()


def test_seam_default_on_uses_unified_carrier(tmp_path):
    cfg = _config(tmp_path, 'default')
    root = tmp_path / 'run'
    with origin_server() as (url, calls):
        seed(QueueChannel(cfg), tmp_path / 'seed', url)
        # No unified_queue_chain kwarg: the workbench default is now ON, so the
        # by-value maps suffix runs on a genuine independent managed worker.
        _settle(cfg, root, url=url)
    assert calls['/file'] == 1
    assert QueueChannel(cfg).state('p')['queue_drained']
    assert QueueChannel(cfg).state('p')['counts'] == {'done_ok': 1}
    pids = _managed_worker_pids(_resources(root))
    assert pids and os.getpid() not in pids


def test_worker_carrier_constructs_zero_commit_coordinator():
    # Guardrail 2 (absolute), grep proof: the worker carrier module neither
    # imports nor constructs CommitCoordinator. AST ignores the docstring, so
    # this binds actual code references, not prose.
    import demiflow.execution.worker.queue_transform as carrier
    tree = ast.parse(Path(carrier.__file__).read_text())
    referenced = set()
    imported_modules = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            referenced.add(node.id)
        elif isinstance(node, ast.Attribute):
            referenced.add(node.attr)
        elif isinstance(node, ast.ImportFrom):
            imported_modules.add(node.module or '')
            referenced.update(alias.name for alias in node.names)
        elif isinstance(node, ast.Import):
            referenced.update(alias.name for alias in node.names)
    assert 'CommitCoordinator' not in referenced
    assert not any(module == 'commit' or module.endswith('.commit') for module in imported_modules)


def test_both_commit_scopes_settle_in_driver_single_journal(tmp_path, monkeypatch):
    import demiflow.execution.commit as commits
    records = []
    original = commits.CommitCoordinator.__init__

    def spy(self, action):
        declaration = getattr(action, 'declaration', None)
        phase = declaration.get('phase') if isinstance(declaration, dict) else None
        directory = str(getattr(action.context, 'directory', ''))
        records.append((os.getpid(), phase, directory))
        original(self, action)

    monkeypatch.setattr(commits.CommitCoordinator, '__init__', spy)
    cfg = _config(tmp_path, 'scopes')
    root = tmp_path / 'run'
    with origin_server() as (url, calls):
        seed(QueueChannel(cfg), tmp_path / 'seed', url)
        _settle(cfg, root, url=url, unified_queue_chain=True)
    assert calls['/file'] == 1
    # Every CommitCoordinator construction happened in the driver process; the
    # managed worker (a separate pid) constructed none.
    assert records and all(pid == os.getpid() for pid, _, _ in records)
    settlement = [(pid, phase, directory) for pid, phase, directory in records
                  if phase in {'acquisition', 'final'}]
    assert {phase for _, phase, _ in settlement} == {'acquisition', 'final'}
    # Both scopes share one driver run directory == the single native journal.
    assert len({directory for _, _, directory in settlement}) == 1
    channel = QueueChannel(cfg)
    with channel.database.connection() as db:
        task = db.execute('SELECT acquisition_scope,final_scope,status FROM queue_tasks '
                          'WHERE generation=? AND task_id=?',
                          (channel.snapshot('p').generation, 'x')).fetchone()
    assert task[0] and task[1] and task[2] == 'done_ok'


def test_eligibility_requires_serializable_maps_suffix(tmp_path):
    from demiflow.execution.queue_chain import unified_queue_chain_eligible
    cfg = _config(tmp_path, 'elig')
    store = LocalObjectStore(tmp_path / 'objects')
    policy = FetchPolicy(max_object_bytes=131072, http=HttpSource(allow_private=True))

    def fetched():
        return data.read_queue(cfg, pool='p').fetch(store=store, policy=policy, concurrency=1)

    # No RawFetch node -> ineligible.
    assert unified_queue_chain_eligible(data.read_queue(cfg, pool='p')._plan) is False
    # Fetch-only (empty suffix) -> byte-unchanged legacy in-driver path.
    assert unified_queue_chain_eligible(fetched()._plan) is False

    def pure(row):
        return row

    # Fetch + pure serializable maps suffix -> unified carrier.
    assert unified_queue_chain_eligible(fetched().map(pure)._plan) is True

    # Fetch + a suffix capturing a live process-bound lock -> not serializable,
    # so the narrow probe routes it to the legacy in-driver fallback.
    import threading
    lock = threading.Lock()

    def captured(row):
        with lock:
            return row

    assert unified_queue_chain_eligible(fetched().map(captured)._plan) is False


# --- Segment 2: multi-task settlement equality + R4 kill/recover ------------


def _seed_tasks(cfg, root, url, n):
    channel = QueueChannel(cfg)
    p = channel.open('p', producer_id='seed', source=ProducerSourceSpec(
        kind='manifest', source_id='fixed-request', uri='memory:request', version=1))
    with RunContext.start(root / 'producer'):
        data.from_items([{'messages': [task_message('t' + str(i), {'id': i, 'request': {'uri': url + '/file'}}, pool='p')]}
                         for i in range(n)]).enqueue(cfg, producer=p).run_stream()
    channel.seal(p)
    return channel, p


def _make_builder(n, *, label_fn=None):
    # Local closures captured once; the returned build() reuses the same callable
    # objects so a resumed run keeps a stable plan semantic identity (the native
    # recovery contract, mirroring the single-closure S6 chain evidence). A
    # module-level callable would serialize by reference and hang the worker.
    class PureAnnotation:
        def __call__(self, row):
            return {**row, 'accepted': False, 'temporary': 'remove-me'}

    def receipt_status(result):
        return result['status']

    if label_fn is None:
        def label_fn(row):
            return {**row, 'label': 'downloaded' if row['status'] == 'ok' else 'failed'}

    def build(cfg, root, url):
        ds = data.read_queue(cfg, pool='p', max_tasks=n, max_inflight=1, idle_timeout_s=.5).fetch(
            store=LocalObjectStore(root / 'objects'), concurrency=1,
            policy=FetchPolicy(max_object_bytes=131072, http=HttpSource(allow_private=True)))
        return (ds.map(PureAnnotation).drop_columns(['request', 'temporary'])
                .rename_columns({'accepted': 'business_accepted'})
                .select_columns(['result', 'business_accepted', '_queue_delivery'])
                .map(receipt_status, inputs={'result': 'result'}, output='status')
                .map(label_fn))
    return build


def _records_by_task(cfg, channel):
    rows = data.read_queue_records(cfg, pool='p', through=channel.snapshot('p'), results=True).take_all()
    return {r['queue_record']['task_id']: r for r in rows}


def _settle_many(cfg, root, url, n, **exec_kwargs):
    with RunContext.start(root, limits=LIMITS):
        with data.local_execution(workers=1, worker_mode="thread", partitions=1, **exec_kwargs):
            _make_builder(n)(cfg, root, url).write_datasink(QueueResultSink(cfg, pool='p'))


def test_unified_queue_carrier_matches_legacy_multi_task_settlement(tmp_path):
    # Settlement slice (M06->M02->M07), not the full queue flow: N pre-populated
    # tasks, each claimed -> validated -> exactly one RawFetch -> atomic finalize.
    n = 4
    legacy_cfg, unified_cfg = _config(tmp_path, 'mlegacy'), _config(tmp_path, 'munified')
    legacy_root, unified_root = tmp_path / 'mlegacy', tmp_path / 'munified'
    with origin_server() as (url, calls):
        legacy_channel, _ = _seed_tasks(legacy_cfg, tmp_path / 'mlegacy-seed', url, n)
        _settle_many(legacy_cfg, legacy_root, url, n, unified_queue_chain=False)
        legacy_fetches = calls['/file']
        unified_channel, up = _seed_tasks(unified_cfg, tmp_path / 'munified-seed', url, n)
        _settle_many(unified_cfg, unified_root, url, n, unified_queue_chain=True)
        unified_fetches = calls['/file'] - legacy_fetches
    # Exactly one acquisition per task per run; the carrier never re-fetches.
    assert legacy_fetches == n and unified_fetches == n
    legacy_rows, unified_rows = _records_by_task(legacy_cfg, legacy_channel), _records_by_task(unified_cfg, unified_channel)
    # Per-record identity + business multiplicity: same N distinct ids, no dup/loss.
    assert sorted(legacy_rows) == sorted(unified_rows) == ['t' + str(i) for i in range(n)]
    for tid in legacy_rows:
        assert _business(legacy_rows[tid]) == _business(unified_rows[tid])
        assert unified_rows[tid]['result']['status'] == 'ok'
        assert unified_rows[tid]['verification']['status'] == 'verified'
    assert legacy_channel.state('p')['queue_drained'] and unified_channel.state('p')['queue_drained']
    assert legacy_channel.state('p')['counts'] == unified_channel.state('p')['counts'] == {'done_ok': n}
    # Each task's atomic final commit is acked (per-record sink commit).
    with unified_channel.database.connection() as db:
        for tid in unified_rows:
            proof = unified_channel.completion(up.generation, tid)
            assert db.execute('SELECT status FROM runtime_commits WHERE commit_id=?',
                              (proof['final_commit'],)).fetchone()[0] == 'acked'
    unified_pids = _managed_worker_pids(_resources(unified_root))
    assert unified_pids and os.getpid() not in unified_pids
    assert _managed_worker_pids(_resources(legacy_root)) == set()


def test_unified_queue_carrier_kill_recover_per_record_equivalence(tmp_path):
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from runtime_v3_owned_crash import (assert_worker_killed, assert_released,
                                        materialize_action_id, process_records)
    # R4: a genuine worker SIGKILL mid-transform, then resume the SAME run. The
    # byte-unchanged legacy queue lifecycle re-claims the interrupted task; the
    # acked RawFetch acquisition is reused (unknown paid effect never replayed);
    # a fresh stateless worker completes the suffix. Per-record equivalence is
    # checked against an uninterrupted unified run, not just total row count.
    n = 3
    cfg, clean_cfg = _config(tmp_path, 'r4'), _config(tmp_path, 'r4clean')
    root, clean_root = tmp_path / 'r4run', tmp_path / 'r4clean'
    driver_pid = os.getpid()
    crash_marker, ref_file = tmp_path / 'crashed-once', tmp_path / 'run-ref.json'

    def fault_label(row):
        if row['_queue_delivery']['task_id'] == 't1' and not crash_marker.exists():
            kill_owned_worker(crash_marker, driver_pid)
        return {**row, 'label': 'downloaded' if row['status'] == 'ok' else 'failed'}

    build = _make_builder(n, label_fn=fault_label)
    with origin_server() as (url, calls):
        channel, p = _seed_tasks(cfg, tmp_path / 'r4-seed', url, n)
        crashed = False
        try:
            with RunContext.start(root, limits=LIMITS) as run:
                ref_file.write_text(json.dumps(asdict(run.ref)))
                with data.local_execution(workers=1, worker_mode="process", partitions=1, unified_queue_chain=True):
                    build(cfg, root, url).write_datasink(QueueResultSink(cfg, pool='p'))
        except DataflowError as exc:
            assert exc.code == "UNKNOWN_EFFECT"
            crashed = True
        assert crashed
        assert_worker_killed(crash_marker, root)
        assert_released(root)
        # t0 settled; t1 acquired (acked) then interrupted in the worker; t2 pending.
        assert calls['/file'] == 2
        before = channel.state('p')
        assert before['counts']['done_ok'] == 1 and not before['queue_drained']
        t0_proof_before = channel.completion(p.generation, 't0')
        ref = RunRef(**json.loads(ref_file.read_text()))
        # Genuine SIGKILL ran no callback cleanup; accelerate only the lease clock.
        with channel.database.connection(write=True) as db:
            db.execute('UPDATE queue_tasks SET lease_until=0 WHERE generation=?', (p.generation,))
        with RunContext.resume(ref):
            with data.local_execution(workers=1, worker_mode="process", partitions=1, unified_queue_chain=True):
                build(cfg, root, url).write_datasink(QueueResultSink(cfg, pool='p'))
        # Unknown paid effect never replayed: t1's acked acquisition reused, only
        # the never-acquired t2 fetched on resume (2 -> 3, not 2 -> 4).
        assert calls['/file'] == n
        # known_completed reuse: the pre-crash settled task is not re-committed.
        assert channel.completion(p.generation, 't0') == t0_proof_before
        clean_channel, _ = _seed_tasks(clean_cfg, tmp_path / 'r4clean-seed', url, n)
        _settle_many(clean_cfg, clean_root, url, n, unified_queue_chain=True)
    recovered, clean = _records_by_task(cfg, channel), _records_by_task(clean_cfg, clean_channel)
    assert sorted(recovered) == sorted(clean) == ['t0', 't1', 't2']
    for tid in recovered:
        assert _business(recovered[tid]) == _business(clean[tid])
        assert recovered[tid]['verification']['status'] == 'verified'
    assert channel.state('p')['queue_drained'] and channel.state('p')['counts'] == {'done_ok': n}
    # Per-record sink commit + confirmed effect charged exactly once (no replay).
    with channel.database.connection() as db:
        for tid in recovered:
            proof = channel.completion(p.generation, tid)
            assert db.execute('SELECT status FROM runtime_commits WHERE commit_id=?',
                              (proof['final_commit'],)).fetchone()[0] == 'acked'
        consumed = db.execute('SELECT consumed FROM runtime_budgets WHERE name=?',
                              ('pool-' + p.generation + '-object_bytes',)).fetchone()[0]
    assert consumed == n * len(BODY)

    assert_released(root)



# --- Segment 2: finite-resource release (worker quota + claim/lease) ---------

_CAPACITY = {"cpu_millis": 4000, "memory_bytes": 4 * 1024 ** 3}


def _capacity(resource_root):
    from demiflow.resources.capacity import LocalCapacityPool
    return LocalCapacityPool(resource_root / "capacity", capacity=_CAPACITY)


def _pid_dead(pid):
    # A reaped or zombie child is not a live worker; only a running process leaks.
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False
    try:
        state = Path('/proc/%d/stat' % pid).read_text().rsplit(')', 1)[1].split()[0]
    except FileNotFoundError:
        return True
    return state == 'Z'


def _release_orphans(resource_root):
    # Canonical exit-evidence release (the provider's ensure_absent path): a dead
    # allocation stays BOUND until unit-lock + birth-identity evidence confirms
    # the host gone. Cross-run shared-tree reconcile is post-MVP (S8.3); here we
    # only prove this-task quota is reclaimable, never permanently leaked.
    from demiflow.resources.process_unit import ManagedProcessUnit
    pool = _capacity(resource_root)
    unit = ManagedProcessUnit(resource_root / "units")
    for record in pool.snapshot()["active"]:
        allocation_id = record["allocation_id"]
        process_json = next((resource_root / "units").glob("*/" + allocation_id + "/process.json"))
        evidence = unit.stop(process_json.parent.parent.name, allocation_id, terminate_deadline_ms=2000)
        assert evidence["confirmed"] is True
        pool.release(allocation_id, evidence)
        try:
            os.waitpid(evidence["identity"]["pid"], os.WNOHANG)
        except (ChildProcessError, OSError):
            pass
    return pool.snapshot()["charged"]


def test_unified_queue_carrier_releases_worker_quota_on_success_and_cancel(tmp_path, monkeypatch):
    import demiflow.execution.queue_chain as qc
    n = 3
    # Success: this-task worker quota fully released, no live worker, every claim
    # consumed (no leased remainder), queue drained, exactly one fetch per task.
    cfg, root = _config(tmp_path, 'rok'), tmp_path / 'rokrun'
    with origin_server() as (url, calls):
        channel, p = _seed_tasks(cfg, tmp_path / 'rok-seed', url, n)
        _settle_many(cfg, root, url, n, unified_queue_chain=True)
    res = _resources(root)
    assert _capacity(res).snapshot()["charged"] == {"cpu_millis": 0, "memory_bytes": 0}
    assert _capacity(res).snapshot()["active"] == []
    pids = _managed_worker_pids(res)
    assert pids and all(_pid_dead(pid) for pid in pids)
    assert channel.state('p')['queue_drained'] and calls['/file'] == n
    with channel.database.connection() as db:
        statuses = sorted(r[0] for r in db.execute(
            'SELECT status FROM queue_tasks WHERE generation=?', (p.generation,)))
    assert statuses == ['done_ok'] * n

    # Cancel/parent-exit: a driver-side carrier failure on t1 leaves the worker
    # LIVE (it already transformed t0); the finally block must kill it. The queue
    # is NOT drained (cancel never manufactures ACK) and the dead allocation is
    # canonically releasable.
    ccfg, croot = _config(tmp_path, 'rcancel'), tmp_path / 'rcancelrun'
    original = qc._TransformClient.transform

    def faulty(self, row):
        if row['_queue_delivery']['task_id'] == 't1':
            raise RuntimeError('driver carrier cancelled')
        return original(self, row)

    monkeypatch.setattr(qc._TransformClient, 'transform', faulty)
    with origin_server() as (url, calls):
        cchannel, cp = _seed_tasks(ccfg, tmp_path / 'rcancel-seed', url, n)
        with pytest.raises(RuntimeError, match='driver carrier cancelled'):
            _settle_many(ccfg, croot, url, n, unified_queue_chain=True)
    cres = _resources(croot)
    cpids = _managed_worker_pids(cres)
    assert cpids and all(_pid_dead(pid) for pid in cpids)
    assert not cchannel.state('p')['queue_drained']
    with cchannel.database.connection() as db:
        t0 = db.execute('SELECT status FROM queue_tasks WHERE generation=? AND task_id=?',
                        (cp.generation, 't0')).fetchone()[0]
    assert t0 == 'done_ok'  # the worker did real work, so it was live before the kill
    assert _release_orphans(cres) == {"cpu_millis": 0, "memory_bytes": 0}


def test_unified_queue_carrier_crash_resume_reclaims_quota_no_live_worker(tmp_path):
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from runtime_v3_owned_crash import (assert_worker_killed, assert_released,
                                        materialize_action_id, process_records)
    n = 3
    cfg, root = _config(tmp_path, 'rq'), tmp_path / 'rqrun'
    driver_pid = os.getpid()
    crash_marker, ref_file = tmp_path / 'rq-crashed', tmp_path / 'rq-ref.json'

    def fault_label(row):
        if row['_queue_delivery']['task_id'] == 't1' and not crash_marker.exists():
            kill_owned_worker(crash_marker, driver_pid)
        return {**row, 'label': 'downloaded' if row['status'] == 'ok' else 'failed'}

    build = _make_builder(n, label_fn=fault_label)
    with origin_server() as (url, calls):
        channel, p = _seed_tasks(cfg, tmp_path / 'rq-seed', url, n)
        crashed = False
        try:
            with RunContext.start(root, limits=LIMITS) as run:
                ref_file.write_text(json.dumps(asdict(run.ref)))
                with data.local_execution(workers=1, worker_mode="process", partitions=1, unified_queue_chain=True):
                    build(cfg, root, url).write_datasink(QueueResultSink(cfg, pool='p'))
        except DataflowError as exc:
            assert exc.code == "UNKNOWN_EFFECT"
            crashed = True
        assert crashed
        assert_worker_killed(crash_marker, root)
        assert_released(root)
        before_processes = process_records(root)
        charged_after_kill = assert_released(root)
        ref = RunRef(**json.loads(ref_file.read_text()))
        with channel.database.connection(write=True) as db:
            db.execute('UPDATE queue_tasks SET lease_until=0 WHERE generation=?', (p.generation,))
        with RunContext.resume(ref):
            with data.local_execution(workers=1, worker_mode="process", partitions=1, unified_queue_chain=True):
                build(cfg, root, url).write_datasink(QueueResultSink(cfg, pool='p'))
        # Original allocator confirms both attempts released their own allocations.
        assert before_processes.keys() < process_records(root).keys()
        assert assert_released(root) == charged_after_kill
        assert channel.state('p')['queue_drained'] and calls['/file'] == n



# --- Segment 3: S8.5 artifact immutability narrow gate ----------------------


def _module_label(status):
    # Module-level UDF: under pytest its __module__ is this test module (not
    # '__main__'), so standard pickle succeeds and cloudpickle carries it BY
    # REFERENCE. The managed worker cannot import the producer module, so S8.5
    # must reject the whole suffix to the byte-unchanged legacy in-driver path.
    return 'downloaded' if status == 'ok' else 'failed'


def _settle_label_variant(cfg, root, *, url, label_fn, **exec_kwargs):
    # Same business chain as _settle, but the final label map uses a supplied
    # callable so a by-reference (module-level) and a by-value (closure) suffix
    # can be driven through the identical settlement and compared.
    class PureAnnotation:
        def __call__(self, row):
            return {**row, 'accepted': False, 'temporary': 'remove-me'}

    def receipt_status(result):
        return result['status']

    with RunContext.start(root, limits=LIMITS):
        with data.local_execution(workers=1, worker_mode="thread", partitions=1, **exec_kwargs):
            ds = data.read_queue(cfg, pool='p', max_tasks=1, max_inflight=1, idle_timeout_s=.2).fetch(
                store=LocalObjectStore(root / 'objects'), concurrency=1,
                policy=FetchPolicy(max_object_bytes=131072, http=HttpSource(allow_private=True)))
            ds = (ds.map(PureAnnotation).drop_columns(['request', 'temporary'])
                  .rename_columns({'accepted': 'business_accepted'})
                  .select_columns(['result', 'business_accepted', '_queue_delivery'])
                  .map(receipt_status, inputs={'result': 'result'}, output='status')
                  .map(label_fn, inputs={'status': 'status'}, output='label'))
            assert ds.write_datasink(QueueResultSink(cfg, pool='p')) is None


def test_eligibility_discriminates_by_reference_udfs(tmp_path, monkeypatch):
    import pickle
    from demiflow.execution.queue_chain import unified_queue_chain_eligible
    cfg = _config(tmp_path, 'disc')
    store = LocalObjectStore(tmp_path / 'objects')
    policy = FetchPolicy(max_object_bytes=131072, http=HttpSource(allow_private=True))

    def fetched():
        return data.read_queue(cfg, pool='p').fetch(store=store, policy=policy, concurrency=1)

    def suffix(label_fn):
        return fetched().map(label_fn, inputs={'status': 'status'}, output='label')._plan

    # By-reference: a module-level UDF (standard pickle succeeds, __module__ is
    # not '__main__') -> cloudpickle carries it by reference -> rejected.
    assert _module_label.__module__ != '__main__'
    assert unified_queue_chain_eligible(suffix(_module_label)) is False

    # By-value: a local closure and a lambda fail standard pickle, so cloudpickle
    # carries their code bytes by value -> admitted.
    def closure_label(status):
        return 'downloaded' if status == 'ok' else 'failed'

    assert unified_queue_chain_eligible(suffix(closure_label)) is True
    assert unified_queue_chain_eligible(suffix(lambda status: status)) is True

    # __main__ nuance: standard pickle ALSO succeeds for a genuine __main__ UDF
    # (script/notebook top level), but cloudpickle special-cases __main__ BY
    # VALUE. Make it findable as __main__.main_label so pickle.dumps succeeds,
    # then prove the __module__ guard admits it (a naive pickle-success-only rule
    # would wrongly reject this primary workbench case).
    def main_label(status):
        return status

    main_label.__module__ = '__main__'
    main_label.__qualname__ = 'main_label'
    monkeypatch.setattr(sys.modules['__main__'], 'main_label', main_label, raising=False)
    assert pickle.dumps(main_label)
    assert unified_queue_chain_eligible(suffix(main_label)) is True


def test_by_reference_suffix_falls_back_to_legacy_without_hang(tmp_path):
    # S8.5 empirical proof (intent-critical: artifact immutability/correctness).
    # With the seam ON, a suffix holding a by-reference UDF is NOT admitted to the
    # managed worker (which would hang on the graph cut unable to import the
    # producer module); it falls back to the byte-unchanged legacy in-driver path,
    # completes without hanging, and settles the SAME business facts as the
    # by-value carrier. The test completing is itself the no-hang evidence.
    ref_cfg, val_cfg = _config(tmp_path, 'byref'), _config(tmp_path, 'byval')
    ref_root, val_root = tmp_path / 'byref', tmp_path / 'byval'

    def by_value_label(status):
        return 'downloaded' if status == 'ok' else 'failed'

    with origin_server() as (url, calls):
        seed(QueueChannel(ref_cfg), tmp_path / 'byref-seed', url)
        _settle_label_variant(ref_cfg, ref_root, url=url, label_fn=_module_label,
                              unified_queue_chain=True)
        ref_fetches = calls['/file']
        seed(QueueChannel(val_cfg), tmp_path / 'byval-seed', url)
        _settle_label_variant(val_cfg, val_root, url=url, label_fn=by_value_label,
                              unified_queue_chain=True)
        val_fetches = calls['/file'] - ref_fetches
    # Exactly one acquisition each: neither path re-fetches the object.
    assert ref_fetches == 1 and val_fetches == 1
    # by-reference -> legacy in-driver: NO managed worker was provisioned.
    assert _managed_worker_pids(_resources(ref_root)) == set()
    # by-value -> unified carrier: a genuine independent managed worker ran.
    val_pids = _managed_worker_pids(_resources(val_root))
    assert val_pids and os.getpid() not in val_pids
    # The fallback is not a degraded result: identical business facts.
    ref_row, = data.read_queue_records(ref_cfg, pool='p',
                                       through=QueueChannel(ref_cfg).snapshot('p'), results=True).take_all()
    val_row, = data.read_queue_records(val_cfg, pool='p',
                                       through=QueueChannel(val_cfg).snapshot('p'), results=True).take_all()
    assert _business(ref_row) == _business(val_row)
    assert ref_row['label'] == 'downloaded' and ref_row['result']['status'] == 'ok'
    assert QueueChannel(ref_cfg).state('p')['queue_drained']
    assert QueueChannel(ref_cfg).state('p')['counts'] == {'done_ok': 1}


# --- Stage 1: N1/N2/N3 default-path regression counterexamples ---------------
# Precise reproductions of the codex 2026-11-12 independent review counterexamples
# (docs/demiflow升级代码复审-20261112.md §2), driven through the genuine public API
# (RunContext + local_execution + bound data API), a local HTTP origin and a real
# Local managed worker. A driver-side prefix map adds the intermediate value and
# the worker-side suffix map consumes it, so the value crosses the carrier
# boundary exactly as in the review probe.


def _intermediate_settle(cfg, root, *, url, kind, **exec_kwargs):
    def prefix(row):
        if kind == 'tuple':
            return {**row, 'temporary': (1, 2)}
        if kind == 'bytes':
            return {**row, 'temporary': b'valid binary intermediate'}
        if kind == 'wide':
            return {**row, 'temporary': 'x' * (32 * 1024**2)}
        return row

    def suffix(row):
        if kind == 'error':
            raise ValueError('review-business-invalid-input')
        out = {k: v for k, v in row.items() if k not in ('temporary', 'request')}
        out['intermediate_type'] = type(row.get('temporary')).__name__
        out['label'] = 'downloaded'
        return out

    with RunContext.start(root, limits=LIMITS):
        with data.local_execution(workers=1, worker_mode='thread', partitions=1, **exec_kwargs):
            ds = data.read_queue(cfg, pool='p', max_tasks=1, max_inflight=1, idle_timeout_s=.2)
            ds = ds.map(prefix).fetch(
                store=LocalObjectStore(root / 'objects'), concurrency=1,
                policy=FetchPolicy(max_object_bytes=131072, http=HttpSource(allow_private=True)))
            ds = ds.map(suffix)
            ds.write_datasink(QueueResultSink(cfg, pool='p'))


def _intermediate_row(cfg):
    row, = data.read_queue_records(cfg, pool='p',
                                   through=QueueChannel(cfg).snapshot('p'), results=True).take_all()
    return row


def test_unified_queue_carrier_preserves_tuple_intermediate(tmp_path):
    # N1: a legal tuple intermediate must stay a tuple across the worker boundary
    # (not silently coerced to list) and the final business result must equal the
    # byte-unchanged legacy in-driver path.
    legacy_cfg, unified_cfg = _config(tmp_path, 'tlegacy'), _config(tmp_path, 'tunified')
    legacy_root, unified_root = tmp_path / 'tlegacy', tmp_path / 'tunified'
    with origin_server() as (url, calls):
        seed(QueueChannel(legacy_cfg), tmp_path / 'tlegacy-seed', url)
        _intermediate_settle(legacy_cfg, legacy_root, url=url, kind='tuple', unified_queue_chain=False)
        legacy_fetches = calls['/file']
        seed(QueueChannel(unified_cfg), tmp_path / 'tunified-seed', url)
        _intermediate_settle(unified_cfg, unified_root, url=url, kind='tuple', unified_queue_chain=True)
        unified_fetches = calls['/file'] - legacy_fetches
    assert legacy_fetches == 1 and unified_fetches == 1
    legacy_row, unified_row = _intermediate_row(legacy_cfg), _intermediate_row(unified_cfg)
    assert legacy_row['intermediate_type'] == 'tuple'
    assert unified_row['intermediate_type'] == 'tuple'
    assert unified_row['label'] == legacy_row['label'] == 'downloaded'
    assert unified_row['result']['status'] == legacy_row['result']['status'] == 'ok'
    # The unified path really ran an independent managed worker (not a fallback).
    pids = _managed_worker_pids(_resources(unified_root))
    assert pids and os.getpid() not in pids
    assert _managed_worker_pids(_resources(legacy_root)) == set()
    assert QueueChannel(unified_cfg).state('p')['counts'] == {'done_ok': 1}


def test_unified_queue_carrier_preserves_bytes_intermediate(tmp_path):
    # N1: a legal bytes intermediate must succeed (no strict-JSON rejection after
    # the acquisition already completed) and equal the legacy result.
    legacy_cfg, unified_cfg = _config(tmp_path, 'blegacy'), _config(tmp_path, 'bunified')
    legacy_root, unified_root = tmp_path / 'blegacy', tmp_path / 'bunified'
    with origin_server() as (url, calls):
        seed(QueueChannel(legacy_cfg), tmp_path / 'blegacy-seed', url)
        _intermediate_settle(legacy_cfg, legacy_root, url=url, kind='bytes', unified_queue_chain=False)
        legacy_fetches = calls['/file']
        seed(QueueChannel(unified_cfg), tmp_path / 'bunified-seed', url)
        _intermediate_settle(unified_cfg, unified_root, url=url, kind='bytes', unified_queue_chain=True)
        unified_fetches = calls['/file'] - legacy_fetches
    assert legacy_fetches == 1 and unified_fetches == 1
    legacy_row, unified_row = _intermediate_row(legacy_cfg), _intermediate_row(unified_cfg)
    assert legacy_row['intermediate_type'] == 'bytes'
    assert unified_row['intermediate_type'] == 'bytes'
    assert unified_row['label'] == legacy_row['label'] == 'downloaded'
    pids = _managed_worker_pids(_resources(unified_root))
    assert pids and os.getpid() not in pids
    assert QueueChannel(unified_cfg).state('p')['counts'] == {'done_ok': 1}
    # N3 success path: this-task quota fully released, no manual helper.
    assert _capacity(_resources(unified_root)).snapshot()["charged"] == {"cpu_millis": 0, "memory_bytes": 0}


def test_unified_queue_carrier_business_error_raises_original_promptly(tmp_path, monkeypatch):
    # N2: a worker business exception must reach the driver as the ORIGINAL
    # ValueError immediately, not as a control TimeoutError after the production
    # 120s wait. A bounded 30s control wait caps regression runtime; the fix
    # returns in seconds, so elapsed must stay well below the wait.
    import time
    import demiflow.execution.queue_chain as qc
    original = qc.run_local_queue_chain
    monkeypatch.setattr(qc, 'run_local_queue_chain',
                        lambda *a, **k: original(*a, **k, control_timeout_s=30))
    cfg, root = _config(tmp_path, 'err'), tmp_path / 'errrun'
    with origin_server() as (url, calls):
        seed(QueueChannel(cfg), tmp_path / 'err-seed', url)
        start = time.monotonic()
        with pytest.raises(ValueError, match='review-business-invalid-input') as excinfo:
            _intermediate_settle(cfg, root, url=url, kind='error', unified_queue_chain=True)
        elapsed = time.monotonic() - start
    # Exact original business type preserved across the worker boundary — not a
    # wrapper, not a control TimeoutError (guardrail 3).
    assert type(excinfo.value) is ValueError
    assert elapsed < 30  # prompt: did not wait out the control timeout
    assert calls['/file'] == 1  # the acquisition completed before the business error
    # The legacy in-driver path raises the identical original ValueError.
    lcfg, lroot = _config(tmp_path, 'errl'), tmp_path / 'errlrun'
    with origin_server() as (url, calls):
        seed(QueueChannel(lcfg), tmp_path / 'errl-seed', url)
        with pytest.raises(ValueError, match='review-business-invalid-input'):
            _intermediate_settle(lcfg, lroot, url=url, kind='error', unified_queue_chain=False)


def test_unified_queue_carrier_releases_quota_on_business_failure_no_helper(tmp_path, monkeypatch):
    # N3: after a business failure returns, this-task capacity is auto-released
    # through the unified close+reconcile terminal — NO manual _release_orphans.
    import demiflow.execution.queue_chain as qc
    original = qc.run_local_queue_chain
    monkeypatch.setattr(qc, 'run_local_queue_chain',
                        lambda *a, **k: original(*a, **k, control_timeout_s=30))
    cfg, root = _config(tmp_path, 'fail'), tmp_path / 'failrun'
    with origin_server() as (url, calls):
        seed(QueueChannel(cfg), tmp_path / 'fail-seed', url)
        with pytest.raises(ValueError, match='review-business-invalid-input'):
            _intermediate_settle(cfg, root, url=url, kind='error', unified_queue_chain=True)
    res = _resources(root)
    assert _capacity(res).snapshot()["charged"] == {"cpu_millis": 0, "memory_bytes": 0}
    assert _capacity(res).snapshot()["active"] == []
    pids = _managed_worker_pids(res)
    assert pids and all(_pid_dead(pid) for pid in pids)
    assert not QueueChannel(cfg).state('p')['queue_drained']  # failure never manufactures ACK


def test_unified_queue_carrier_releases_quota_on_cancel_no_helper(tmp_path, monkeypatch):
    # N3: a driver-side carrier cancel (failure before the worker drains) also
    # auto-releases this-task capacity through the same terminal — no helper.
    import demiflow.execution.queue_chain as qc
    cfg, root = _config(tmp_path, 'cancel'), tmp_path / 'cancelrun'

    def faulty(self, row):
        raise RuntimeError('driver carrier cancelled')

    monkeypatch.setattr(qc._TransformClient, 'transform', faulty)
    with origin_server() as (url, calls):
        seed(QueueChannel(cfg), tmp_path / 'cancel-seed', url)
        with pytest.raises(RuntimeError, match='driver carrier cancelled'):
            _intermediate_settle(cfg, root, url=url, kind='tuple', unified_queue_chain=True)
    res = _resources(root)
    assert _capacity(res).snapshot()["charged"] == {"cpu_millis": 0, "memory_bytes": 0}
    assert _capacity(res).snapshot()["active"] == []
    pids = _managed_worker_pids(res)
    assert pids and all(_pid_dead(pid) for pid in pids)
    assert not QueueChannel(cfg).state('p')['queue_drained']


def test_unified_queue_carrier_scratch_bounded_across_many_rows(tmp_path):
    # Guardrail 2 (finite-resource premise): the per-row DataRef scratch never
    # accumulates. One row is in flight end-to-end (driver concurrency=1), so at
    # most 2 live blocks ever coexist, and the unified terminal reclaims the whole
    # scratch dir after the run regardless of row count (inodes do not grow with N).
    import threading
    import time
    n = 6
    cfg, root = _config(tmp_path, 'bounded'), tmp_path / 'boundedrun'
    segment_dir = root / 'queue-transform-segments'
    scratch = segment_dir / 'segments'
    peak = [0]
    stop = threading.Event()

    def sample():
        while not stop.is_set():
            try:
                live = len(list(scratch.glob('*/*.pkl')))
            except OSError:
                live = 0
            if live > peak[0]:
                peak[0] = live
            time.sleep(0.001)

    build = _make_builder(n)
    with origin_server() as (url, calls):
        channel, _ = _seed_tasks(cfg, tmp_path / 'bounded-seed', url, n)
        sampler = threading.Thread(target=sample, daemon=True)
        sampler.start()
        try:
            with RunContext.start(root, limits=LIMITS):
                with data.local_execution(workers=1, worker_mode='thread', partitions=1,
                                          unified_queue_chain=True):
                    build(cfg, root, url).write_datasink(QueueResultSink(cfg, pool='p'))
        finally:
            stop.set()
            sampler.join(timeout=5)
    assert calls['/file'] == n
    assert channel.state('p')['counts'] == {'done_ok': n}
    # Bounded carriage: never more than 2 live blocks; scratch fully reclaimed.
    assert peak[0] <= 2
    assert not segment_dir.exists()
    # A genuine independent managed worker ran the carrier (the data plane was used).
    res = _resources(root)
    pids = _managed_worker_pids(res)
    assert pids and os.getpid() not in pids and all(_pid_dead(pid) for pid in pids)
    assert _capacity(res).snapshot()['charged'] == {'cpu_millis': 0, 'memory_bytes': 0}


def test_unified_queue_carrier_wide_row_hits_existing_byte_budget_both_paths(tmp_path):
    # Guardrail 2 negative control: an over-budget intermediate trips the
    # PRE-EXISTING native row byte ceiling (check_size, default 16 MiB) in the
    # driver-side prefix map on BOTH paths, before acquisition and before the
    # carrier stage — not a new carrier/strict-JSON failure. The legacy in-driver
    # path raises the identical error type/message.
    legacy_cfg, unified_cfg = _config(tmp_path, 'wlegacy'), _config(tmp_path, 'wunified')
    legacy_root, unified_root = tmp_path / 'wlegacy', tmp_path / 'wunified'

    def capture(cfg, root, url, **kw):
        try:
            _intermediate_settle(cfg, root, url=url, kind='wide', **kw)
        except BaseException as exc:
            return exc
        raise AssertionError('over-budget wide row did not trip the native byte ceiling')

    with origin_server() as (url, calls):
        seed(QueueChannel(legacy_cfg), tmp_path / 'wlegacy-seed', url)
        legacy_exc = capture(legacy_cfg, legacy_root, url, unified_queue_chain=False)
        legacy_fetches = calls['/file']
        seed(QueueChannel(unified_cfg), tmp_path / 'wunified-seed', url)
        unified_exc = capture(unified_cfg, unified_root, url, unified_queue_chain=True)
        unified_fetches = calls['/file'] - legacy_fetches
    # Same pre-existing ceiling error on both paths; trips before RawFetch.
    assert 'Value byte budget exceeded' in str(legacy_exc)
    assert 'Value byte budget exceeded' in str(unified_exc)
    assert type(legacy_exc) is type(unified_exc)
    assert legacy_fetches == 0 and unified_fetches == 0
    # The unified terminal still releases the idle worker it had provisioned.
    res = _resources(unified_root)
    assert _capacity(res).snapshot()['charged'] == {'cpu_millis': 0, 'memory_bytes': 0}
    pids = _managed_worker_pids(res)
    assert pids and all(_pid_dead(pid) for pid in pids)


def test_unified_queue_carrier_unpicklable_business_error_fallback_conveys_type(tmp_path, monkeypatch):
    # Guardrail 3: when the original business exception cannot be pickled onto the
    # data plane, the worker falls back to a control-frame error carrying the
    # original type/message; the driver surfaces that promptly (NOT a control
    # TimeoutError) and still releases capacity through the unified terminal.
    import time
    import threading
    import demiflow.execution.queue_chain as qc
    original = qc.run_local_queue_chain
    monkeypatch.setattr(qc, 'run_local_queue_chain',
                        lambda *a, **k: original(*a, **k, control_timeout_s=30))
    cfg, root = _config(tmp_path, 'unpick'), tmp_path / 'unpickrun'

    def suffix(row):
        exc = ValueError('review-unpicklable-business')
        exc.payload = threading.Lock()  # the instance cannot be pickled
        raise exc

    with origin_server() as (url, calls):
        seed(QueueChannel(cfg), tmp_path / 'unpick-seed', url)
        start = time.monotonic()
        with pytest.raises(RecoveryContractError, match='ValueError: review-unpicklable-business') as excinfo:
            with RunContext.start(root, limits=LIMITS):
                with data.local_execution(workers=1, worker_mode='thread', partitions=1,
                                          unified_queue_chain=True):
                    ds = data.read_queue(cfg, pool='p', max_tasks=1, max_inflight=1, idle_timeout_s=.2).fetch(
                        store=LocalObjectStore(root / 'objects'), concurrency=1,
                        policy=FetchPolicy(max_object_bytes=131072, http=HttpSource(allow_private=True)))
                    ds = ds.map(suffix)
                    ds.write_datasink(QueueResultSink(cfg, pool='p'))
        elapsed = time.monotonic() - start
    assert elapsed < 30  # prompt fallback, did not wait out the control timeout
    assert not isinstance(excinfo.value, TimeoutError)
    assert calls['/file'] == 1  # acquisition completed before the business error
    res = _resources(root)
    assert _capacity(res).snapshot()['charged'] == {'cpu_millis': 0, 'memory_bytes': 0}
    pids = _managed_worker_pids(res)
    assert pids and all(_pid_dead(pid) for pid in pids)


def test_unified_queue_carrier_close_failure_does_not_mask_business_error(tmp_path, monkeypatch):
    # Guardrail 4: if the resource terminal itself fails (close/reconcile raises),
    # it is logged but must NOT mask the original business exception; the backstop
    # kill still reaps the worker. The driver surfaces the original ValueError.
    import demiflow.execution.queue_chain as qc
    from demiflow.resources.manager import ResourceManager
    original = qc.run_local_queue_chain
    monkeypatch.setattr(qc, 'run_local_queue_chain',
                        lambda *a, **k: original(*a, **k, control_timeout_s=30))

    def broken_close(self, *a, **k):
        raise RuntimeError('injected resource terminal failure')

    monkeypatch.setattr(ResourceManager, 'close', broken_close)
    cfg, root = _config(tmp_path, 'closefail'), tmp_path / 'closefailrun'
    with origin_server() as (url, calls):
        seed(QueueChannel(cfg), tmp_path / 'closefail-seed', url)
        with pytest.raises(ValueError, match='review-business-invalid-input'):
            _intermediate_settle(cfg, root, url=url, kind='error', unified_queue_chain=True)
    assert calls['/file'] == 1
    # Backstop kill reaped the worker even though the protocol terminal failed.
    pids = _managed_worker_pids(_resources(root))
    assert pids and all(_pid_dead(pid) for pid in pids)


def test_local_segment_store_discard_is_safe_and_idempotent(tmp_path):
    # Guardrail 1: discard reuses the read-path _resolve security (path-escape /
    # symlink / size) before unlink, removes only within-store blocks and their
    # now-empty generation dir, never deletes outside the store, and the finally
    # rmtree backstop is idempotent. write/read behavior is unchanged.
    import hashlib
    import shutil
    import uuid
    from demiflow.execution.exchange.carriage_local import LocalSegmentStore
    from demiflow.execution.contracts.exchange import build_data_ref
    store_dir = tmp_path / 'store'
    store = LocalSegmentStore(store_dir, host_id='local')
    scope = uuid.uuid4().hex
    _, refs = store.write([{'v': (1, 2), 'b': b'bytes'}], scope_ref=scope, durability='ephemeral')
    block = store_dir / refs[0]['location']
    assert block.is_file()
    # read unchanged: type-preserving round-trip before discard.
    assert store.read(refs) == [{'v': (1, 2), 'b': b'bytes'}]
    # discard removes the block and its now-empty generation dir (within store).
    store.discard(refs)
    assert not block.exists() and not block.parent.exists()
    # A properly-built escaping ref is rejected by _resolve; nothing outside is deleted.
    outside = tmp_path / 'outside.pkl'
    payload = b'precious-outside-data'
    outside.write_bytes(payload)
    escape = build_data_ref(kind='file', read_domain=store.domain, location='../outside.pkl',
                            payload_bytes=len(payload), sha256=hashlib.sha256(payload).hexdigest(),
                            durability='ephemeral', scope_ref=scope)
    with pytest.raises(RecoveryContractError, match='escapes its store'):
        store.discard([escape])
    assert outside.read_bytes() == payload
    # A symlinked block path is rejected; the target and the link are intact.
    gen = store_dir / 'segments' / 'symgen'
    gen.mkdir(parents=True)
    link = gen / '0.pkl'
    link.symlink_to(outside)
    sym = build_data_ref(kind='file', read_domain=store.domain, location='segments/symgen/0.pkl',
                         payload_bytes=len(payload), sha256=hashlib.sha256(payload).hexdigest(),
                         durability='ephemeral', scope_ref=scope)
    with pytest.raises(RecoveryContractError, match='symlink'):
        store.discard([sym])
    assert outside.read_bytes() == payload and link.is_symlink()
    # System-level idempotent cleanup backstop (finally rmtree ignore_errors).
    shutil.rmtree(store_dir, ignore_errors=True)
    shutil.rmtree(store_dir, ignore_errors=True)
    assert not store_dir.exists()