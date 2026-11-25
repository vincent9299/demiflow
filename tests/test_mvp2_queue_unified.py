"""Public queue claim, acquisition, settlement and worker resource recovery."""
import json
import os
import sys
from dataclasses import asdict
from pathlib import Path

import pytest
from runtime_v3_owned_crash import assert_released, kill_owned_worker, process_records

import demiflow.data as data
from demiflow.collect import FetchPolicy, HttpSource
from demiflow.execution.native_queue.queue import QueueChannel, QueueConfig, ProducerSourceSpec, task_message
from demiflow.execution.native_queue.queue_sinks import QueueResultSink
from demiflow.execution.contracts.recovery_contract import RecoveryContractError, RunRef
from demiflow.execution.pipeline.run_context import RunContext
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


def _settle(cfg, root, *, url):
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
        with data.local_execution(workers=1, partitions=1):
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
    # must be identical across interrupted and uninterrupted settlement.
    result = {k: v for k, v in row['result'].items()
              if k not in {'retrieved_at', 'acquisition_ref', 'storage_domain'}}
    if result.get('raw_ref'):
        result['raw_ref'] = {'sha256': result['raw_ref']['sha256']}
    return dict(result=result, business_accepted=row['business_accepted'],
                label=row['label'], status=row['status'], verification=row['verification'])


def _managed_worker_pids(root):
    return {record['identity']['pid'] for _, record in process_records(root).values()}


def test_public_queue_settles_acquired_business_rows(tmp_path):
    cfg, root = _config(tmp_path, 'one'), tmp_path/'run'
    channel = QueueChannel(cfg)
    with origin_server() as (url, calls):
        seed(channel, tmp_path/'seed', url)
        _settle(cfg, root, url=url)
        assert calls['/file'] == 1
    row, = data.read_queue_records(cfg, pool='p', through=channel.snapshot('p'), results=True).take_all()
    assert row['result']['status'] == 'ok'
    assert row['business_accepted'] is False and row['label'] == 'downloaded'
    assert row['verification']['status'] == 'verified'
    assert channel.state('p')['queue_drained'] and channel.state('p')['counts'] == {'done_ok': 1}


def test_old_queue_driver_selector_is_rejected_before_work():
    with pytest.raises(TypeError, match='unified_queue_chain'):
        with data.local_execution(unified_queue_chain=False):
            pytest.fail('removed queue selector started a second runtime')


def test_public_queue_runs_in_managed_worker(tmp_path):
    cfg = _config(tmp_path, 'default')
    root = tmp_path / 'run'
    with origin_server() as (url, calls):
        seed(QueueChannel(cfg), tmp_path / 'seed', url)
        _settle(cfg, root, url=url)
    assert calls['/file'] == 1
    assert QueueChannel(cfg).state('p')['queue_drained']
    assert QueueChannel(cfg).state('p')['counts'] == {'done_ok': 1}
    pids = _managed_worker_pids(root)
    assert pids and os.getpid() not in pids


def test_both_commit_scopes_settle_in_driver_single_journal(tmp_path, monkeypatch):
    import demiflow.execution.checkpoint.commit as commits
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
        _settle(cfg, root, url=url)
    assert calls['/file'] == 1
    # The spy only sees the driver; tests/test_stage2_unified_plan.py keeps the
    # rule that worker modules never construct a CommitCoordinator.
    assert records
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


def _settle_many(cfg, root, url, n):
    with RunContext.start(root, limits=LIMITS):
        with data.local_execution(workers=1, partitions=1):
            _make_builder(n)(cfg, root, url).write_datasink(QueueResultSink(cfg, pool='p'))


def test_public_queue_settles_each_task_once(tmp_path):
    n = 4
    cfg, root = _config(tmp_path, 'many'), tmp_path/'run'
    with origin_server() as (url, calls):
        channel, producer = _seed_tasks(cfg, tmp_path/'seed', url, n)
        _settle_many(cfg, root, url, n)
        assert calls['/file'] == n
    rows = _records_by_task(cfg, channel)
    assert sorted(rows) == ['t' + str(i) for i in range(n)]
    with channel.database.connection() as db:
        for task, row in rows.items():
            assert row['result']['status'] == 'ok'
            assert row['verification']['status'] == 'verified'
            proof = channel.completion(producer.generation, task)
            assert db.execute('SELECT status FROM runtime_commits WHERE commit_id=?',
                              (proof['final_commit'],)).fetchone()[0] == 'acked'
    assert channel.state('p')['queue_drained'] and channel.state('p')['counts'] == {'done_ok': n}


def test_unified_queue_carrier_kill_recover_per_record_equivalence(tmp_path):
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from runtime_v3_owned_crash import assert_worker_killed
    # R4: a genuine worker SIGKILL mid-transform, then resume the SAME run. The
    # queue lifecycle re-claims the interrupted task; the
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
                with data.local_execution(workers=1, worker_mode="process", partitions=1):
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
            with data.local_execution(workers=1, worker_mode="process", partitions=1):
                build(cfg, root, url).write_datasink(QueueResultSink(cfg, pool='p'))
        # Unknown paid effect never replayed: t1's acked acquisition reused, only
        # the never-acquired t2 fetched on resume (2 -> 3, not 2 -> 4).
        assert calls['/file'] == n
        # known_completed reuse: the pre-crash settled task is not re-committed.
        assert channel.completion(p.generation, 't0') == t0_proof_before
        clean_channel, _ = _seed_tasks(clean_cfg, tmp_path / 'r4clean-seed', url, n)
        _settle_many(clean_cfg, clean_root, url, n)
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


def test_unified_queue_carrier_crash_resume_reclaims_quota_no_live_worker(tmp_path):
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from runtime_v3_owned_crash import assert_worker_killed
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
                with data.local_execution(workers=1, worker_mode="process", partitions=1):
                    build(cfg, root, url).write_datasink(QueueResultSink(cfg, pool='p'))
        except DataflowError as exc:
            assert exc.code == "UNKNOWN_EFFECT"
            crashed = True
        assert crashed
        assert_worker_killed(crash_marker, root)
        before_processes = process_records(root)
        released_after_kill = assert_released(root)
        ref = RunRef(**json.loads(ref_file.read_text()))
        with channel.database.connection(write=True) as db:
            db.execute('UPDATE queue_tasks SET lease_until=0 WHERE generation=?', (p.generation,))
        with RunContext.resume(ref):
            with data.local_execution(workers=1, worker_mode="process", partitions=1):
                build(cfg, root, url).write_datasink(QueueResultSink(cfg, pool='p'))
        # Original allocator confirms both attempts released their own allocations.
        assert before_processes.keys() < process_records(root).keys()
        assert released_after_kill < assert_released(root)
        assert channel.state('p')['queue_drained'] and calls['/file'] == n


# --- Segment 3: S8.5 artifact immutability narrow gate ----------------------


def _module_label(status):
    # Module-level UDF: under pytest its __module__ is this test module (not
    # '__main__'), so standard pickle succeeds and cloudpickle carries it BY
    # REFERENCE. The worker must run the imported function on the public path.
    return 'downloaded' if status == 'ok' else 'failed'


def _settle_label_variant(cfg, root, *, url, label_fn):
    # Same business chain as _settle, but the final label map uses a supplied
    # callable so a by-reference (module-level) and a by-value (closure) suffix
    # can be driven through the identical settlement and compared.
    class PureAnnotation:
        def __call__(self, row):
            return {**row, 'accepted': False, 'temporary': 'remove-me'}

    def receipt_status(result):
        return result['status']

    with RunContext.start(root, limits=LIMITS):
        with data.local_execution(workers=1, partitions=1):
            ds = data.read_queue(cfg, pool='p', max_tasks=1, max_inflight=1, idle_timeout_s=.2).fetch(
                store=LocalObjectStore(root / 'objects'), concurrency=1,
                policy=FetchPolicy(max_object_bytes=131072, http=HttpSource(allow_private=True)))
            ds = (ds.map(PureAnnotation).drop_columns(['request', 'temporary'])
                  .rename_columns({'accepted': 'business_accepted'})
                  .select_columns(['result', 'business_accepted', '_queue_delivery'])
                  .map(receipt_status, inputs={'result': 'result'}, output='status')
                  .map(label_fn, inputs={'status': 'status'}, output='label'))
            assert ds.write_datasink(QueueResultSink(cfg, pool='p')) is None


def test_module_function_and_closure_settle_through_managed_workers(tmp_path):
    ref_cfg, val_cfg = _config(tmp_path, 'byref'), _config(tmp_path, 'byval')
    ref_root, val_root = tmp_path / 'byref', tmp_path / 'byval'

    def by_value_label(status):
        return 'downloaded' if status == 'ok' else 'failed'

    with origin_server() as (url, calls):
        seed(QueueChannel(ref_cfg), tmp_path / 'byref-seed', url)
        _settle_label_variant(ref_cfg, ref_root, url=url, label_fn=_module_label)
        ref_fetches = calls['/file']
        seed(QueueChannel(val_cfg), tmp_path / 'byval-seed', url)
        _settle_label_variant(val_cfg, val_root, url=url, label_fn=by_value_label)
        val_fetches = calls['/file'] - ref_fetches
    # Exactly one acquisition each: neither path re-fetches the object.
    assert ref_fetches == 1 and val_fetches == 1
    ref_pids = _managed_worker_pids(ref_root)
    assert ref_pids and os.getpid() not in ref_pids
    val_pids = _managed_worker_pids(val_root)
    assert val_pids and os.getpid() not in val_pids
    # Both supported callable forms must deliver identical business facts.
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


def _intermediate_settle(cfg, root, *, url, kind):
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
        with data.local_execution(workers=1, partitions=1):
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


def _assert_intermediate_type(tmp_path, kind):
    cfg, root = _config(tmp_path, kind), tmp_path/'run'
    with origin_server() as (url, calls):
        seed(QueueChannel(cfg), tmp_path/'seed', url)
        _intermediate_settle(cfg, root, url=url, kind=kind)
        assert calls['/file'] == 1
    row = _intermediate_row(cfg)
    assert row['intermediate_type'] == kind
    assert row['label'] == 'downloaded' and row['result']['status'] == 'ok'
    assert QueueChannel(cfg).state('p')['counts'] == {'done_ok': 1}
    assert_released(root)


def test_unified_queue_carrier_preserves_tuple_intermediate(tmp_path):
    _assert_intermediate_type(tmp_path, 'tuple')


def test_unified_queue_carrier_preserves_bytes_intermediate(tmp_path):
    _assert_intermediate_type(tmp_path, 'bytes')


def test_typed_intermediate_survives_worker_loss_after_acquisition(tmp_path):
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    n = 3
    cfg, root = _config(tmp_path, 'typed'), tmp_path / 'typed-run'
    driver_pid = os.getpid()
    crash_marker, ref_file = tmp_path / 'crashed-once', tmp_path / 'run-ref.json'

    def prefix(row):
        return {**row, 'temporary': (1, b'\x00')}

    def suffix(row):
        if row['_queue_delivery']['task_id'] == 't1' and not crash_marker.exists():
            kill_owned_worker(crash_marker, driver_pid)
        value = row['temporary']
        out = {k: v for k, v in row.items() if k not in ('temporary', 'request')}
        out['intermediate_type'] = [type(value).__name__, type(value[1]).__name__]
        return out

    def build(url):
        return (data.read_queue(cfg, pool='p', max_tasks=n, max_inflight=1, idle_timeout_s=.5).map(prefix)
                .fetch(store=LocalObjectStore(root / 'objects'), concurrency=1,
                       policy=FetchPolicy(max_object_bytes=131072, http=HttpSource(allow_private=True)))
                .map(suffix))

    with origin_server() as (url, calls):
        channel, p = _seed_tasks(cfg, tmp_path / 'typed-seed', url, n)
        with pytest.raises(DataflowError, match='UNKNOWN_EFFECT'):
            with RunContext.start(root, limits=LIMITS) as run:
                ref_file.write_text(json.dumps(asdict(run.ref)))
                with data.local_execution(workers=1, worker_mode='process', partitions=1):
                    build(url).write_datasink(QueueResultSink(cfg, pool='p'))
        assert crash_marker.exists() and calls['/file'] == 2
        with channel.database.connection(write=True) as db:
            db.execute('UPDATE queue_tasks SET lease_until=0 WHERE generation=?', (p.generation,))
        with RunContext.resume(RunRef(**json.loads(ref_file.read_text()))):
            with data.local_execution(workers=1, worker_mode='process', partitions=1):
                build(url).write_datasink(QueueResultSink(cfg, pool='p'))
        # The acquired task is not fetched again; its stored receipt keeps the row's types.
        assert calls['/file'] == n
    records = _records_by_task(cfg, channel)
    assert sorted(records) == ['t0', 't1', 't2']
    assert all(r['intermediate_type'] == ['tuple', 'bytes'] for r in records.values())
    assert channel.state('p')['counts'] == {'done_ok': n}


def test_rows_stored_as_json_by_an_earlier_attempt_remain_readable():
    from demiflow.execution.contracts.record_codec import RECORD_MEDIA_TYPE, dumps_row, loads_stored_row
    assert loads_stored_row(b'{"a":[1,2]}', 'application/json') == {'a': [1, 2]}
    row = {'t': (1, b'\x00'), 'n': None}
    assert loads_stored_row(dumps_row(row, max_bytes=4096), RECORD_MEDIA_TYPE) == row


def test_unified_queue_carrier_business_error_raises_original_promptly(tmp_path, monkeypatch):
    # N2: a worker business exception must reach the driver as the ORIGINAL
    # ValueError immediately, not as a control TimeoutError after the production
    # 120s wait. A bounded 30s control wait caps regression runtime; the fix
    # returns in seconds, so elapsed must stay well below the wait.
    import time
    cfg, root = _config(tmp_path, 'err'), tmp_path / 'errrun'
    with origin_server() as (url, calls):
        seed(QueueChannel(cfg), tmp_path / 'err-seed', url)
        start = time.monotonic()
        with pytest.raises(ValueError, match='review-business-invalid-input') as excinfo:
            _intermediate_settle(cfg, root, url=url, kind='error')
        elapsed = time.monotonic() - start
    # Exact original business type preserved across the worker boundary — not a
    # wrapper, not a control TimeoutError (guardrail 3).
    assert type(excinfo.value) is ValueError
    assert elapsed < 30  # prompt: did not wait out the control timeout
    assert calls['/file'] == 1  # the acquisition completed before the business error


def test_unified_queue_carrier_releases_quota_on_business_failure_no_helper(tmp_path, monkeypatch):
    # N3: after a business failure returns, this-task capacity is auto-released
    # through the unified close+reconcile terminal — NO manual _release_orphans.
    cfg, root = _config(tmp_path, 'fail'), tmp_path / 'failrun'
    with origin_server() as (url, calls):
        seed(QueueChannel(cfg), tmp_path / 'fail-seed', url)
        with pytest.raises(ValueError, match='review-business-invalid-input'):
            _intermediate_settle(cfg, root, url=url, kind='error')
    res = root
    assert_released(res)
    pids = _managed_worker_pids(res)
    assert pids and all(_pid_dead(pid) for pid in pids)
    assert not QueueChannel(cfg).state('p')['queue_drained']  # failure never manufactures ACK


def test_public_queue_releases_staging_throughout_settlement(tmp_path, monkeypatch):
    from demiflow.execution.journal.run_journal import RunJournal

    # Inspect the owning store, both while the real acquisition is staging and
    # after its last settlement. A defunct carrier directory proves no bound.
    n = 6
    cfg, root = _config(tmp_path, 'bounded'), tmp_path/'run'
    reservations = []
    begin = RunJournal.begin_dataflow_staging

    def observe(self, fence, reservation_id, store_id, inode, size, *, ceiling):
        charged = begin(self, fence, reservation_id, store_id, inode, size, ceiling=ceiling)
        with self._connection() as db:
            total, = db.execute(
                "SELECT consumed FROM runtime_budgets WHERE name='artifact_staging_bytes'").fetchone()
        reservations.append((charged, total, ceiling))
        return charged

    monkeypatch.setattr(RunJournal, 'begin_dataflow_staging', observe)
    with origin_server() as (url, calls):
        channel, _ = _seed_tasks(cfg, tmp_path/'seed', url, n)
        _settle_many(cfg, root, url, n)
        assert calls['/file'] == n
    assert channel.state('p')['counts'] == {'done_ok': n}
    assert reservations and all(0 < charged <= total <= ceiling for charged, total, ceiling in reservations)
    with RunJournal(root)._connection() as db:
        pending = db.execute(
            'SELECT count(*) FROM runtime_dataflow_staging s LEFT JOIN runtime_dataflow_settlements d '
            'ON d.reservation_id=s.reservation_id WHERE d.reservation_id IS NULL').fetchone()[0]
        assert pending == 0
    assert not list((root/'.dataflow-store').glob('*/staging/.pending-*'))
    assert_released(root)


def test_public_queue_wide_row_hits_byte_budget_before_fetch(tmp_path):
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    cfg, root = _config(tmp_path, 'wide'), tmp_path/'run'
    with origin_server() as (url, calls):
        seed(QueueChannel(cfg), tmp_path/'seed', url)
        with pytest.raises(DataflowError, match='record string exceeds byte budget') as rejected:
            _intermediate_settle(cfg, root, url=url, kind='wide')
        assert rejected.value.code == 'BUDGET_EXCEEDED'
        assert calls['/file'] == 0
    assert_released(root)


def test_unified_queue_carrier_unpicklable_business_error_fallback_conveys_type(tmp_path, monkeypatch):
    # Guardrail 3: when the original business exception cannot be pickled onto the
    # data plane, the worker falls back to a control-frame error carrying the
    # original type/message; the driver surfaces that promptly (NOT a control
    # TimeoutError) and still releases capacity through the unified terminal.
    import time
    import threading
    cfg, root = _config(tmp_path, 'unpick'), tmp_path / 'unpickrun'

    def suffix(row):
        exc = ValueError('review-unpicklable-business')
        exc.payload = threading.Lock()  # the instance cannot be pickled
        raise exc

    with origin_server() as (url, calls):
        seed(QueueChannel(cfg), tmp_path / 'unpick-seed', url)
        start = time.monotonic()
        with pytest.raises(ValueError, match='review-unpicklable-business') as excinfo:
            with RunContext.start(root, limits=LIMITS):
                with data.local_execution(workers=1, partitions=1):
                    ds = data.read_queue(cfg, pool='p', max_tasks=1, max_inflight=1, idle_timeout_s=.2).fetch(
                        store=LocalObjectStore(root / 'objects'), concurrency=1,
                        policy=FetchPolicy(max_object_bytes=131072, http=HttpSource(allow_private=True)))
                    ds = ds.map(suffix)
                    ds.write_datasink(QueueResultSink(cfg, pool='p'))
        elapsed = time.monotonic() - start
    assert elapsed < 30  # prompt fallback, did not wait out the control timeout
    assert not isinstance(excinfo.value, TimeoutError)
    assert calls['/file'] == 1  # acquisition completed before the business error
    res = root
    assert_released(res)
    pids = _managed_worker_pids(res)
    assert pids and all(_pid_dead(pid) for pid in pids)


def test_unified_queue_carrier_close_failure_does_not_mask_business_error(tmp_path, monkeypatch):
    # Guardrail 4: if the resource terminal itself fails (close/reconcile raises),
    # it is logged but must NOT mask the original business exception; the backstop
    # kill still reaps the worker. The driver surfaces the original ValueError.
    from demiflow.resources.manager import ResourceManager

    def broken_close(self, *a, **k):
        raise RuntimeError('injected resource terminal failure')

    monkeypatch.setattr(ResourceManager, 'close', broken_close)
    cfg, root = _config(tmp_path, 'closefail'), tmp_path / 'closefailrun'
    with origin_server() as (url, calls):
        seed(QueueChannel(cfg), tmp_path / 'closefail-seed', url)
        with pytest.raises(ValueError, match='review-business-invalid-input'):
            _intermediate_settle(cfg, root, url=url, kind='error')
    assert calls['/file'] == 1
    # Backstop kill reaped the worker even though the protocol terminal failed.
    pids = _managed_worker_pids(root)
    assert pids and all(_pid_dead(pid) for pid in pids)
