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
    # R4: a genuine worker SIGKILL mid-transform, then resume the SAME run. The
    # byte-unchanged legacy queue lifecycle re-claims the interrupted task; the
    # acked RawFetch acquisition is reused (unknown paid effect never replayed);
    # a fresh stateless worker completes the suffix. Per-record equivalence is
    # checked against an uninterrupted unified run, not just total row count.
    n = 3
    cfg, clean_cfg = _config(tmp_path, 'r4'), _config(tmp_path, 'r4clean')
    root, clean_root = tmp_path / 'r4run', tmp_path / 'r4clean'
    crash_marker, ref_file = tmp_path / 'crashed-once', tmp_path / 'run-ref.json'

    def fault_label(row):
        if row['_queue_delivery']['task_id'] == 't1' and not crash_marker.exists():
            crash_marker.touch()
            os.kill(os.getpid(), signal.SIGKILL)
        return {**row, 'label': 'downloaded' if row['status'] == 'ok' else 'failed'}

    build = _make_builder(n, label_fn=fault_label)
    with origin_server() as (url, calls):
        channel, p = _seed_tasks(cfg, tmp_path / 'r4-seed', url, n)
        crashed = False
        try:
            with RunContext.start(root, limits=LIMITS) as run:
                ref_file.write_text(json.dumps(asdict(run.ref)))
                with data.local_execution(workers=1, worker_mode="thread", partitions=1, unified_queue_chain=True):
                    build(cfg, root, url).write_datasink(QueueResultSink(cfg, pool='p'))
        except (RecoveryContractError, OSError):
            crashed = True
        assert crashed and crash_marker.exists()
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
            with data.local_execution(workers=1, worker_mode="thread", partitions=1, unified_queue_chain=True):
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
    n = 3
    cfg, root = _config(tmp_path, 'rq'), tmp_path / 'rqrun'
    crash_marker, ref_file = tmp_path / 'rq-crashed', tmp_path / 'rq-ref.json'

    def fault_label(row):
        if row['_queue_delivery']['task_id'] == 't1' and not crash_marker.exists():
            crash_marker.touch()
            os.kill(os.getpid(), signal.SIGKILL)
        return {**row, 'label': 'downloaded' if row['status'] == 'ok' else 'failed'}

    build = _make_builder(n, label_fn=fault_label)
    with origin_server() as (url, calls):
        channel, p = _seed_tasks(cfg, tmp_path / 'rq-seed', url, n)
        crashed = False
        try:
            with RunContext.start(root, limits=LIMITS) as run:
                ref_file.write_text(json.dumps(asdict(run.ref)))
                with data.local_execution(workers=1, worker_mode="thread", partitions=1, unified_queue_chain=True):
                    build(cfg, root, url).write_datasink(QueueResultSink(cfg, pool='p'))
        except (RecoveryContractError, OSError):
            crashed = True
        assert crashed and crash_marker.exists()
        res = _resources(root)
        # SIGKILL ran no callback cleanup; the finally still left no live worker.
        assert all(_pid_dead(pid) for pid in _managed_worker_pids(res))
        charged_after_kill = _capacity(res).snapshot()["charged"]
        ref = RunRef(**json.loads(ref_file.read_text()))
        with channel.database.connection(write=True) as db:
            db.execute('UPDATE queue_tasks SET lease_until=0 WHERE generation=?', (p.generation,))
        with RunContext.resume(ref):
            with data.local_execution(workers=1, worker_mode="thread", partitions=1, unified_queue_chain=True):
                build(cfg, root, url).write_datasink(QueueResultSink(cfg, pool='p'))
        # The resumed run released its OWN allocation on its success close: the
        # only charged remainder is the pre-crash orphan, not a doubled charge.
        assert _capacity(res).snapshot()["charged"] == charged_after_kill
        assert all(_pid_dead(pid) for pid in _managed_worker_pids(res))
        assert channel.state('p')['queue_drained'] and calls['/file'] == n
        assert _release_orphans(res) == {"cpu_millis": 0, "memory_bytes": 0}


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