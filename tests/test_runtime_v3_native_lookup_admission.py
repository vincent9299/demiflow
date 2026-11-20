"""Public native operations use real worker lifecycle and sealed declarations."""
import sqlite3

import lance
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.execution.run_context import RunContext


def table(tmp_path):
    uri = str(tmp_path / 'lookup.lance')
    schema = pa.schema([('id', pa.string()), ('value', pa.int64()),
                        ('vector', pa.list_(pa.float32(), 2))])
    rows = [{'id': 'a', 'value': 1, 'vector': [1.0, 0.0]},
            {'id': 'b', 'value': 2, 'vector': [0.0, 1.0]}]
    lance.write_dataset(pa.Table.from_pylist(rows, schema=schema), uri)
    return uri


@pytest.mark.parametrize('kind', ['admit', 'deduplicate'])
def test_public_admission_starts_worker_and_preserves_original_reservations(tmp_path, kind):
    path = tmp_path / 'admission.sqlite'
    rows = [{'id': 'a', 'group': 'x'}, {'id': 'b', 'group': 'x'}, {'id': 'c', 'group': 'y'}]
    def build():
        ds = data.from_items(rows)
        if kind == 'admit':
            return ds.admit_rows(path=path, key='id', unique_on=['group'],
                quotas=[{'on': [], 'limit': 1}])
        return ds.deduplicate('group', path=path, key='id', keep_duplicates=True)
    with RunContext.start(tmp_path / 'run') as run, data.local_execution():
        actual = build().take_all()
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
    output = 'admission' if kind == 'admit' else 'deduplication'
    expected = ['admitted', 'duplicate', 'limited' if kind == 'admit' else 'admitted']
    assert [r[output]['status'] for r in actual] == expected
    with sqlite3.connect(path) as db:
        before = db.execute('SELECT count(*) FROM accepted').fetchone()[0]
    with data.local_execution():
        replay = build().take_all()
    assert [r[output]['status'] for r in replay] == expected
    assert [r['id'] for r in replay] == [r['id'] for r in actual]
    assert all(r[output]['reason'] == 'reused' for r in replay if r[output]['status'] == 'admitted')
    with sqlite3.connect(path) as db:
        assert db.execute('SELECT count(*) FROM accepted').fetchone()[0] == before


@pytest.mark.parametrize('kind', ['lookup', 'vector'])
def test_public_pinned_lance_operation_runs_and_replays_original_commit(tmp_path, kind):
    uri = table(tmp_path)
    def build():
        if kind == 'lookup':
            return data.from_items([{'id': 'a'}, {'id': 'missing'}]).lookup_lance(
                {'uri': uri, 'version': 1}, on='id', columns=['value'], output='hits')
        return data.from_items([{'query': [1.0, 0.0]}]).search_vectors(query='query', output='hits',
            uri=uri, vector_column='vector', columns=['id', 'value'], version=1, top_k=1,
            concurrency=1, options={'use_index': False})
    class Interrupted(Exception):
        pass
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path / 'run') as run, data.local_execution():
            ref = run.ref
            actual = build().take_all()
            commits = run.journal.execution_commits('action-1')
            assert commits[-1]['status'] == 'acked'
            raise Interrupted()
    if kind == 'lookup':
        assert actual == [{'id': 'a', 'hits': [{'value': 1}]}, {'id': 'missing', 'hits': []}]
    else:
        assert len(actual) == 1 and actual[0]['hits'] == [{'id': 'a', 'value': 1, '_distance': 0.0}]
    # Moving the table head cannot alter the fixed-version read or the original receipt.
    lance.write_dataset(pa.table({'id': ['a'], 'value': [99], 'vector': pa.array([[0., 1.]],
        type=pa.list_(pa.float32(), 2))}), uri, mode='overwrite')
    with RunContext.resume(ref) as run, data.local_execution():
        assert build().take_all() == actual
        assert run.journal.execution_commits('action-1') == commits


@pytest.mark.parametrize('kind', ['lookup', 'vector'])
def test_native_query_factory_is_passive_binds_policy_and_rejects_live_handles(tmp_path, monkeypatch, kind):
    from dataclasses import replace
    import cloudpickle
    from demiflow.execution.dataflow_model_factory import native_model_declaration
    from demiflow.execution.seal.plan import _base_code_identity_from_value
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from demiflow.planning.dataflow_compile import _callable_code_identity
    from demiflow.execution.stream_lookup import LanceLookup
    from demiflow.lance.search import VectorSearch
    if kind == 'lookup':
        ds = data.from_items([{'id': 'a'}]).lookup_lance({'uri': str(tmp_path / 'absent'), 'version': 1},
            on='id', columns=['value'])
    else:
        ds = data.from_items([{'query': [1., 0.]}]).search_vectors(query='query', output='hits',
            uri=str(tmp_path / 'absent'), vector_column='vector', columns=['id'], version=1,
            top_k=1, concurrency=1, options={'use_index': False})
    op = ds._plan.operations[-1]
    actor = op.callable.target.__self__
    def must_not_construct(*args, **kwargs):
        raise AssertionError('compiler constructed a native actor')
    monkeypatch.setattr(type(actor), '__init__', must_not_construct)
    first = native_model_declaration(op)
    assert first.target is (LanceLookup if kind == 'lookup' else VectorSearch)
    assert not (tmp_path / 'absent').exists()
    assert _base_code_identity_from_value('callable', first) == _base_code_identity_from_value(
        'callable', cloudpickle.loads(cloudpickle.dumps(first)))
    original = _callable_code_identity(first)
    if kind == 'lookup':
        actor.cache[(str, 'previous')] = ([{'value': 7}], 50)
        actor.cache_bytes = 50
        assert _callable_code_identity(native_model_declaration(op)) == original
        actor.source = {**actor.source, 'version': 2}
        assert _callable_code_identity(native_model_declaration(op)) != original
        actor.table = object()
    else:
        actor.spec = {**actor.spec, 'top_k': 2}
        with pytest.raises(Exception, match='differs'):
            native_model_declaration(op)
        changed = replace(op, search_spec=dict(actor.spec))
        assert _callable_code_identity(native_model_declaration(changed)) != original
        op = changed
        actor._dataset = object()
    with pytest.raises(DataflowError, match='unstarted'):
        native_model_declaration(op)


def test_unregistered_actor_cache_is_still_rejected_by_data_only_identity():
    from collections import OrderedDict
    from demiflow.data.plan import CallableSpec
    from demiflow.execution.recovery_contract import RecoveryContractError
    from demiflow.planning.dataflow_compile import _callable_code_identity
    class UserActor:
        def __init__(self):
            self.cache = OrderedDict()
        def __call__(self, row):
            return row
    with pytest.raises(RecoveryContractError, match='OrderedDict'):
        _callable_code_identity(CallableSpec.create(UserActor()))
