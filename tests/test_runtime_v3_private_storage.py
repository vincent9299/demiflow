"""Private model paths share the original artifact byte authority."""
from dataclasses import replace
from contextlib import contextmanager
import sqlite3
from types import SimpleNamespace

import pytest

from demiflow import data
from demiflow.execution.journal.dataflow_authority import bind_private_storage, private_storage
from demiflow.execution.contracts.recovery_contract import RecoveryContractError, identity
from demiflow.execution.pipeline.run_context import RunContext, RunAction
from demiflow.execution.contracts.errors import ExecutionBudgetExceeded
from demiflow.execution.contracts.dataflow_errors import DataflowError
from test_runtime_v3_native_rpc_replay import IntentionalInterruption, service
from test_runtime_v3_private_prompt_journal import _pipeline, _charged, native_server
from test_embeddings import server

MiB = 1024**2


@contextmanager
def _bounded_run(location, maximum=32*MiB):
    try:
        with RunContext.start(location, limits={'max_scratch_bytes': maximum}) as run:
            yield run
            raise IntentionalInterruption()
    except IntentionalInterruption:
        pass


def _configure(authority, paths, *, maximum=32*MiB):
    assert authority._bounds.max_spill_bytes == maximum
    authority._private_embedding_nodes = {'embedding': {'private_journal': {'path': str(paths[0])}}}
    authority._private_prompt_nodes = {('prompt', str(i)): {'private_journal': {'path': str(p)}}
                                     for i, p in enumerate(paths[1:])}


def _private_reservations(run):
    with run.journal._connection() as db:
        return db.execute("SELECT reservation_id,amount FROM runtime_reservations "
                          "WHERE name='artifact_staging_bytes' AND reservation_id LIKE 'private-sqlite-%' ORDER BY reservation_id").fetchall()


def test_alias_nodes_routes_and_actions_keep_one_original_path_cap(tmp_path):
    folder = tmp_path/'cache'
    folder.mkdir()
    alias = tmp_path/'alias'
    alias.symlink_to(folder, target_is_directory=True)
    path = folder/'calls.sqlite'
    with _bounded_run(tmp_path/'run') as run, service(run) as (authority, store, _):
        _configure(authority, [path, alias/'calls.sqlite', folder/'..'/'cache'/'calls.sqlite'])
        bind_private_storage(authority)
        expected = [('private-sqlite-'+identity(str(path)), 16*MiB)]
        assert _private_reservations(run) == expected
        assert private_storage(authority, alias/'calls.sqlite') == (path, 16*MiB)
        run.journal.bind_execution_action(run.fence, 'action-2', {'kind': 'take_all'})
        authority._action = RunAction(run, 'action-2', {'kind': 'take_all'})
        bind_private_storage(authority)
        assert _private_reservations(run) == expected
        assert run.journal.source_binding('action-1-private-model-storage') == run.journal.source_binding('action-2-private-model-storage')
        # New paths cannot silently shrink existing caps or refill the reserved
        # half. They require available original run admission.
        _configure(authority, [path, tmp_path/'new.sqlite'])
        with pytest.raises(DataflowError, match='4 MiB'):
            bind_private_storage(authority)
        assert _private_reservations(run) == expected


def test_private_models_and_artifacts_cannot_each_spend_whole_ceiling(tmp_path):
    with _bounded_run(tmp_path/'run') as run, service(run) as (authority, store, _):
        _configure(authority, [tmp_path/'embed.sqlite', tmp_path/'prompt.sqlite'])
        run.journal.reserve_budgets(run.fence, 'original-artifact-proof', {'artifact_staging_bytes': (17*MiB, 32*MiB)})
        with pytest.raises(ExecutionBudgetExceeded):
            bind_private_storage(authority)
        assert not _private_reservations(run)  # Both path reservations rolled back.
        assert not (tmp_path/'embed.sqlite').exists() and not (tmp_path/'prompt.sqlite').exists()


@pytest.mark.parametrize('problem', ['pages', 'wal'])
def test_existing_history_incompatible_with_cap_is_not_rewritten(tmp_path, problem):
    path = tmp_path/'history.sqlite'
    db = sqlite3.connect(path)
    db.execute('CREATE TABLE original (value BLOB)')
    db.execute('INSERT INTO original VALUES (zeroblob(?))', (3*MiB if problem == 'pages' else 1024,))
    db.commit()
    if problem == 'wal':
        assert db.execute('PRAGMA journal_mode=WAL').fetchone()[0] == 'wal'
        db.execute('INSERT INTO original VALUES (?)', (b'pending-wal-history',))
        db.commit()
    before = {p.name: p.read_bytes() for p in tmp_path.glob('history.sqlite*') if p.is_file()}
    try:
        with _bounded_run(tmp_path/'run', 16*MiB) as run, service(run) as (authority, store, _):
            _configure(authority, [path], maximum=16*MiB)
            with pytest.raises((DataflowError, RecoveryContractError), match='pages|DELETE'):
                bind_private_storage(authority)
            assert not _private_reservations(run)
        assert {p.name: p.read_bytes() for p in tmp_path.glob('history.sqlite*') if p.is_file()} == before
    finally:
        db.close()


def test_real_prompt_then_embedding_share_path_and_replay_without_extra_charge(tmp_path, native_server, server):
    path = tmp_path/'shared.sqlite'
    folder = tmp_path/'alias'
    folder.symlink_to(tmp_path, target_is_directory=True)
    with RunContext.start(tmp_path/'run', limits={'max_scratch_bytes': 64*MiB, 'max_requests': 2}) as run, data.local_execution():
        for action in range(2):
            ds = _pipeline(path, items=('a',)).map_embeddings(model=server['model'], inputs={'text': 'answer'},
                batch_size=1, concurrency=1, call_output='embedding_call',
                options={'sqlite_journal': {'path': str(folder/'shared.sqlite')}})
            rows = ds.take_all()
            assert len(rows) == 1 and rows[0]['answer'] == 'ok' and len(rows[0]['embedding']) == 3
            assert rows[0]['embedding_call']['reused'] == bool(action)
            assert _charged(run) == 2
            reservations = _private_reservations(run)
            assert len(reservations) == 1 and reservations[0][1] == 32*MiB
            with run.journal._connection() as db:
                consumed = db.execute("SELECT consumed FROM runtime_budgets WHERE name='artifact_staging_bytes'").fetchone()[0]
            assert 32*MiB < consumed <= 64*MiB  # real original artifact refs coexist
        assert len(native_server) == len(server['bodies']) == 1
        assert all(c['status'] == 'acked' for action in ('action-1', 'action-2') for c in run.journal.execution_commits(action))


def test_real_history_exceeds_private_share_before_any_http(tmp_path, native_server, server):
    path = tmp_path/'history.sqlite'
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE original (payload BLOB)')
        db.execute('INSERT INTO original VALUES (zeroblob(?))', (5*MiB,))
    before = path.read_bytes()
    with pytest.raises(DataflowError, match='pages'), RunContext.start(tmp_path/'run', limits={'max_scratch_bytes': 32*MiB}), data.local_execution():
        _pipeline(path, items=('a',)).map_embeddings(model=server['model'], inputs={'text': 'answer'}, batch_size=1,
            options={'sqlite_journal': {'path': str(tmp_path/'embed.sqlite')}}).take_all()
    assert path.read_bytes() == before and not native_server and not server['bodies']
    assert not (tmp_path/'embed.sqlite').exists()


def test_real_existing_artifact_prevents_mixed_cache_overcommit_before_http(tmp_path, native_server, server):
    with pytest.raises(ExecutionBudgetExceeded), RunContext.start(tmp_path/'run', limits={'max_scratch_bytes': 32*MiB}) as run, data.local_execution():
        run.journal.allocate_artifact(run.fence, 'scratch/owned-before-models.bin', size=17*MiB, atomic=False)
        artifact = run.directory/'scratch/owned-before-models.bin'
        artifact.parent.mkdir(exist_ok=True)
        with artifact.open('wb') as file:
            file.truncate(17*MiB)
        _pipeline(tmp_path/'prompt.sqlite', items=('a',)).map_embeddings(model=server['model'],
            inputs={'text': 'answer'}, batch_size=1,
            options={'sqlite_journal': {'path': str(tmp_path/'embed.sqlite')}}).take_all()
    assert not native_server and not server['bodies'] and not _private_reservations(run)
    assert not (tmp_path/'prompt.sqlite').exists() and not (tmp_path/'embed.sqlite').exists()


def test_old_lease_history_is_count_bounded_and_untouched(tmp_path, monkeypatch):
    from demiflow.execution.storage import private_journals as storage
    path = tmp_path/'history.sqlite'
    leases = tmp_path/'history.sqlite.leases'
    leases.mkdir()
    for index in range(3):
        (leases/identity(['old', index])).touch()
    before = {p.name: p.stat().st_ino for p in leases.iterdir()}
    monkeypatch.setattr(storage, '_MAX_HISTORICAL_LEASES', 2)
    with _bounded_run(tmp_path/'run') as run, service(run) as (authority, store, _):
        _configure(authority, [path])
        with pytest.raises(DataflowError, match='lease count'):
            bind_private_storage(authority)
        assert not _private_reservations(run)
    assert {p.name: p.stat().st_ino for p in leases.iterdir()} == before
