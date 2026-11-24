"""Actual published-object lifecycle for the disk GC acceptance gate.

Only fixture objects and observations are added around a real Dataset action.
Collection, readers, pins, scratch admission and refunds use their native owners.
"""
from __future__ import annotations

from dataclasses import asdict
from contextlib import nullcontext


def disk_gc(fx, fault):
    from pytest import MonkeyPatch, raises
    from demiflow import data
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    from demiflow.execution.storage.gc import collect_published_orphans
    from demiflow.execution.pipeline.run_context import current_run

    def consumed(run):
        with run.journal._connection() as db:
            return db.execute("SELECT consumed FROM runtime_budgets "
                              "WHERE name='artifact_staging_bytes'").fetchone()[0]

    def put(store, value):
        return store.put(iter((value,)), max_bytes=len(value),
                         media_type='application/octet-stream')

    original_prepare = DataflowExecution.prepare
    executions, readers = [], []
    fixture = {}

    def prepare(execution):
        sealed = original_prepare(execution)
        # Later result reads use the original published blocks. Their own
        # Dataset actions may add legitimate native records and reservations.
        assert not executions
        executions.append(execution)
        store = execution._store
        fixture['orphan'] = put(store, b'gc-fixture-unreferenced-by-history')
        fixture['pinned'] = put(store, b'gc-fixture-live-explicit-owner')
        store.retain(fixture['pinned'], owner='disk-gc-fixture-owner')
        reader = store.read(fixture['orphan'], max_bytes=fixture['orphan'].size_bytes)
        assert next(reader) == b'gc-fixture-unreferenced-by-history'
        readers.append(reader)
        # A resident reader prevents physical collection even when the driver
        # writer closes. Failed scratch growth must retain neither bytes nor a
        # native reservation; its original spill ceiling is not enlarged.
        before = consumed(execution._action.context)
        with store.scratch(max_bytes=4, max_io_bytes=8) as scratch:
            scratch.append(b'abcd')
            with raises(DataflowError, match='BUDGET_EXCEEDED'):
                scratch.append(b'e')
            assert scratch.size == 4 and scratch.read_at(0, 4) == b'abcd'
        fixture['scratch_released'] = consumed(execution._action.context) == before
        assert fixture['scratch_released']
        return sealed

    expected = [{'i': i * 2} for i in range(12)]
    handle = None
    # RuntimeV3Fixture owns the original run and the selected Local/Ray provider.
    with nullcontext(current_run()) as run:
        try:
            with MonkeyPatch.context() as patch:
                patch.setattr(DataflowExecution, 'prepare', prepare)
                handle = data.from_items([{'i': i} for i in range(12)]).map(
                    lambda row: {'i': row['i'] * 2}).materialize()
            execution, = executions
            store, action = execution._store, execution._action
            collect = lambda: collect_published_orphans(action, bounds=execution._bounds)
            commits = run.journal.execution_commits(action.action_id)
            assert commits and all(item['status'] == 'acked' for item in commits)
            fx.assert_semantics(expected, handle.take_all(), ordering='ordered')
            before = consumed(run)
            held = collect()
            assert held['blocked'] and held['removed'] == held['bytes'] == 0
            assert consumed(run) == before and store.inspect(fixture['orphan'])
            readers.pop().close()
            first = collect()
            after = consumed(run)
            assert first['blocked'] is None and first['bytes'] >= fixture['orphan'].size_bytes
            assert before - after == first['bytes']
            assert not store.inspect(fixture['orphan']) and store.inspect(fixture['pinned'])
            # Both stored results and the actual commit documents survive GC.
            fx.assert_semantics(expected, handle.take_all(), ordering='ordered')
            assert run.journal.execution_commits(action.action_id) == commits
            before_repeat = consumed(run)
            repeated = collect()
            assert repeated == {'removed': 0, 'bytes': 0, 'blocked': None}
            assert consumed(run) == before_repeat
            store.release(fixture['pinned'], owner='disk-gc-fixture-owner')
            last = collect()
            assert last == {'removed': 1, 'bytes': fixture['pinned'].size_bytes, 'blocked': None}
            released = (not store.inspect(fixture['pinned'])
                        and consumed(run) == before_repeat - fixture['pinned'].size_bytes)
            assert released
            fx.assert_semantics(expected, handle.take_all(), ordering='ordered')
            assert run.journal.execution_commits(action.action_id) == commits
            return {'released': released, 'live_reader': held, 'first_collection': first,
                    'repeat_collection': repeated, 'released_owner': last,
                    'scratch_released': fixture['scratch_released'],
                    'original_commits': [item['commit_id'] for item in commits],
                    'fixture_refs': {key: asdict(fixture[key]) for key in ('orphan', 'pinned')}}
        finally:
            for reader in readers:
                reader.close()
            if handle is not None:
                handle.release()
