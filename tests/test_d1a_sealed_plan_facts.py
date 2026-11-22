"""Published v1 carriers remain readable and immutable alongside fresh native v3 runs."""
import hashlib
import json
import sqlite3
import pytest

from demiflow.execution.sealed_plans import PlanContractError, load_sealed_plan
from demiflow.execution.run_context import RunContext
from demiflow.workbench.runs.facts import read_sealed_plan
from test_stage2_unified_plan import LIMITS
from test_u2_chain_commit import original_chain


def test_original_f1_matches_old_writer_and_workbench_reader(original_chain):
    root, original = original_chain
    journal_mtime = (root / 'run.sqlite').stat().st_mtime_ns
    sealed = load_sealed_plan(root, 'main-action', version='v2')
    assert sealed == {'facts': original['facts'], 'logical_dag': original['dag'],
                      'physical_plan': original['physical']}
    for field, expected in (('logical_dag_ref', original['dag']), ('physical_plan_ref', original['physical'])):
        ref = sealed['facts'][field]
        assert set(ref) == {'sha256', 'path', 'bytes'}
        path = root / ref['path']
        assert path.parent == root / 'scratch' / 'chain-sealed-plans'
        assert not path.is_symlink()
        raw = path.read_bytes()
        assert len(raw) == ref['bytes'] and hashlib.sha256(raw).hexdigest() == ref['sha256']
        assert json.loads(raw) == expected
    assert read_sealed_plan(root, 'main-action')['dag_id'] == original['dag']['dag_id']
    assert load_sealed_plan(root, 'main-action', version='v2') == sealed
    assert (root / 'run.sqlite').stat().st_mtime_ns == journal_mtime


@pytest.mark.parametrize('damage', ['bytes', 'symlink', 'budget', 'binding'])
def test_current_reader_refuses_invalid_old_carriers(original_chain, tmp_path, damage, monkeypatch):
    root, original = original_chain
    path = root / original['facts']['logical_dag_ref']['path']
    if damage == 'bytes':
        path.write_bytes(path.read_bytes() + b' ')
    elif damage == 'symlink':
        new = tmp_path / 'other-carrier'
        path.rename(new)
        path.symlink_to(new)
    elif damage == 'budget':
        from demiflow.execution import sealed_plans
        monkeypatch.setattr(sealed_plans, 'MAX_SEALED_PLAN_BYTES', 16)
    else:
        with sqlite3.connect(root / 'run.sqlite') as db:
            db.execute('UPDATE runtime_sources SET document=? WHERE action_id=?',
                       (json.dumps({'schema': 'bogus'}), 'main-action-sealed-plan'))
    with pytest.raises(PlanContractError):
        load_sealed_plan(root, 'main-action', version='v2')


def test_v4_workbench_scratch_bytes_counts_carrier_and_staging_released(original_chain, tmp_path):
    from demiflow.workbench.runs.storage import scratch_bytes
    target, original = original_chain
    f1 = original["facts"]
    carriers = [target / f1["logical_dag_ref"]["path"],
                target / f1["physical_plan_ref"]["path"]]
    for path in carriers:
        assert path.is_file()
        # atomic_bytes finally-unlinks its staging on every path (success/failure/
        # cancel/parent-exit, state_artifacts.py:30); none may leak.
        assert not path.with_name(path.name + ".staging").exists(), "staging not released"
    # Carriers are unregistered (mirror manifest), so the workbench filesystem scan
    # -- not journal aggregate admission -- is what charges them (verdict §7.4).
    work = tmp_path / "work"
    work.mkdir()
    total_with = scratch_bytes(target, work, [])
    sizes = [p.stat().st_size for p in carriers]
    for path in carriers:
        path.unlink()
    total_without = scratch_bytes(target, work, [])
    assert total_with - total_without == sum(sizes), \
        "workbench scratch_bytes did not charge the sealed-plan carriers"


def test_v3_local_caller_seals_and_persists(tmp_path):
    import demiflow.data as data
    root = tmp_path / "run"

    def double(row):
        return {**row, "x": row["x"] * 2}
    with RunContext.start(root, limits=LIMITS):
        with data.local_execution(workers=1, worker_mode="thread", partitions=1,
                                  unified_chain=True):
            data.from_items([{"x": i} for i in range(6)]).map_async(
                double, concurrency=1).materialize().take_all()
    assert load_sealed_plan(root, "action-1", version="v3")["version"] == "v3"


def test_v3_queue_caller_seals_and_persists(tmp_path):
    import demiflow.data as data
    from demiflow.collect import FetchPolicy, HttpSource
    from demiflow.execution.queue import QueueChannel, QueueConfig
    from demiflow.execution.queue_sinks import QueueResultSink
    from demiflow.objects import LocalObjectStore
    from test_queue_v2_boundaries import seed
    from test_raw_fetch import origin_server
    cfg = QueueConfig(backend='sqlite', queue_id='q', path=str(tmp_path / 'q.sqlite'))
    root = tmp_path / "run"

    class PureAnnotation:
        def __call__(self, row):
            return {**row, 'accepted': False, 'temporary': 'remove-me'}

    def receipt_status(result):
        return result['status']

    def annotate(status):
        return 'downloaded' if status == 'ok' else 'failed'
    with origin_server() as (url, calls):
        seed(QueueChannel(cfg), tmp_path / 'seed', url)
        with RunContext.start(root, limits=LIMITS):
            with data.local_execution(workers=1, worker_mode="thread", partitions=1,
                                      unified_queue_chain=True):
                ds = data.read_queue(cfg, pool='p', max_tasks=1, max_inflight=1,
                                     idle_timeout_s=.2).fetch(
                    store=LocalObjectStore(root / 'objects'), concurrency=1,
                    policy=FetchPolicy(max_object_bytes=131072,
                                       http=HttpSource(allow_private=True)))
                ds = (ds.map(PureAnnotation).drop_columns(['request', 'temporary'])
                      .rename_columns({'accepted': 'business_accepted'})
                      .select_columns(['result', 'business_accepted', '_queue_delivery'])
                      .map(receipt_status, inputs={'result': 'result'}, output='status')
                      .map(annotate, inputs={'status': 'status'}, output='label'))
                assert ds.write_datasink(QueueResultSink(cfg, pool='p')) is None
    assert load_sealed_plan(root, "action-1", version="v3")["version"] == "v3"


def test_v3_ray_caller_routes_through_same_seam():
    from demiflow.execution.executors.base import DatasetExecutor
    from demiflow.execution.executors.local import LocalDatasetExecutor
    from demiflow.execution.executors.ray import RayDatasetExecutor
    assert LocalDatasetExecutor.execute is RayDatasetExecutor.execute is DatasetExecutor.execute
