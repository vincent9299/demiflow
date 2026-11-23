"""Historical checkpoint guarantees on the original managed run protocol."""
import json
from pathlib import Path

import lance
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.execution.run_journal import RunJournal
from demiflow.execution.recovery_contract import RecoveryContractError, RunRef
from demiflow.execution.contracts.dataflow_errors import DataflowError


def declared(tmp_path):
    schema = data.schema({'fields': [{'name': 'id', 'type': 'string', 'nullable': False}]})
    return schema, {'out': {'uri': str(tmp_path / 'out.lance'), 'schema': schema}}


def save(rows, outputs, schema, **options):
    source = data.from_items(rows) if rows else data.from_arrow(pa.Table.from_pylist([], schema=schema))
    return source.save_lance(outputs['out']['uri'], schema=schema, key='id',
        stage='out', mode='append', max_batch=1, **options).run_stream()


def native_scopes(journal):
    with journal._connection() as db:
        rows = db.execute('SELECT scope_id,declaration FROM runtime_commit_scopes ORDER BY rowid').fetchall()
    return [(key, json.loads(raw)) for key, raw in rows
            if json.loads(raw).get('schema') == 'demiflow.stream_lance.v3']


def interrupt_after_success(directory, execute, *, entry_identity=None):
    with pytest.raises(OSError, match='stop after original action receipt'):
        with RunContext.start(directory, entry_identity=entry_identity) as run:
            reference = run.ref
            execute()
            raise OSError('stop after original action receipt')
    return reference


def test_empty_checkpoint_read_and_fixed_completed_read(tmp_path):
    schema, outputs = declared(tmp_path)
    # A typed empty native publication supplies an actual fixed read receipt.
    # There is no independently writable checkpoint descriptor.
    empty = save([], outputs, schema).outputs['out']
    assert data.read_lance(**empty).take_all() == []
    first = save([{'id': 'a'}], outputs, schema).outputs['out']
    fixed = data.read_lance(**first)
    assert fixed.take_all() == [{'id': 'a'}]
    second = save([{'id': 'b'}], outputs, schema).outputs['out']
    assert fixed.take_all() == [{'id': 'a'}]
    assert data.read_lance(**empty).count() == 0
    assert data.read_lance(**second).count() == 2


def test_checkpoint_empty_stream_really_commits_schema(tmp_path):
    schema, outputs = declared(tmp_path)
    with RunContext.start(tmp_path / 'run') as run:
        ref = save([], outputs, schema).outputs['out']
    assert ref['version'] == 1 and lance.dataset(**ref).count_rows() == 0
    assert lance.dataset(**ref).schema.equals(schema, check_metadata=True)
    facts = [f for key, _ in native_scopes(run.journal) for f in run.journal.execution_commits(key)]
    assert facts and all(f['status'] == 'acked' for f in facts)
    assert any(f['receipts']['lance']['data']['reference'] == ref for f in facts)


def test_checkpoint_rejects_redirect_before_opening_table(tmp_path, monkeypatch):
    _assert_redirect_rejected(tmp_path, monkeypatch, change='prepared')


@pytest.mark.parametrize('change', ['declared_reference', 'declared_spec'])
def test_native_scope_cannot_redirect_its_sealed_writer(tmp_path, monkeypatch, change):
    _assert_redirect_rejected(tmp_path, monkeypatch, change=change)


def _assert_redirect_rejected(tmp_path, monkeypatch, *, change):
    schema, outputs = declared(tmp_path)
    def execute():
        return save([{'id': 'a'}, {'id': 'a'}], outputs, schema)
    reference = interrupt_after_success(tmp_path / 'run', execute)
    journal = RunJournal(reference.directory)
    scopes = [(key, declaration) for key, declaration in native_scopes(journal)
              if (declaration['spec'] is not None if change == 'declared_spec' else
                  declaration['spec'] is None and declaration['reference'] is not None)]
    assert scopes
    key, declaration = scopes[0]
    fact = journal.execution_commits(key)[0]
    prepared = fact['prepared']
    foreign = tmp_path / 'foreign.lance'
    if change != 'declared_spec':
        prepared['sinks']['lance']['reference'] = {'uri': str(foreign), 'version': 1}
    if change == 'declared_reference':
        declaration['reference'] = dict(prepared['sinks']['lance']['reference'])
    elif change == 'declared_spec':
        declaration['spec']['fields']['uri'] = str(foreign)
    with journal._transaction() as db:
        db.execute('UPDATE runtime_commits SET prepared=? WHERE commit_id=?',
                   (json.dumps(prepared), fact['commit_id']))
        db.execute('UPDATE runtime_commit_scopes SET declaration=? WHERE scope_id=?',
                   (json.dumps(declaration), key))
    opened = []
    from demiflow.lance import storage
    original = storage.open_lance_dataset
    def guarded(uri=None, *args, **kwargs):
        target = uri or kwargs.get('uri')
        if target == str(foreign):
            opened.append(target)
            pytest.fail('unowned URI was opened before native reference ownership validation')
        return original(uri, *args, **kwargs)
    monkeypatch.setattr(storage, 'open_lance_dataset', guarded)
    with pytest.raises(DataflowError, match='reference.*declaration|declared.*reference|sealed writer') as failure:
        with RunContext.resume(reference):
            execute()
    assert failure.value.code == 'CORRUPT_ARTIFACT'
    assert opened == [] and not foreign.exists()
    assert lance.dataset(outputs['out']['uri']).to_table().to_pylist() == [{'id': 'a'}]


def test_checkpoint_schema_change_and_symlink_are_rejected(tmp_path):
    schema, outputs = declared(tmp_path)
    def execute():
        return save([{'id': 'a'}], outputs, schema)
    reference = interrupt_after_success(tmp_path / 'run', execute)
    journal = RunJournal(reference.directory)
    original = journal.execution_commits('action-1')
    changed = pa.schema([('id', pa.int64())])
    with pytest.raises(RecoveryContractError, match='identity|schema|declaration'):
        with RunContext.resume(reference):
            save([{'id': 'a'}], outputs, changed)
    alias = tmp_path / 'alias-run'
    alias.symlink_to(Path(reference.directory), target_is_directory=True)
    with pytest.raises(RecoveryContractError, match='symlink|symbolic'):
        with RunContext.resume(RunRef(str(alias), reference.run_id)):
            execute()
    assert journal.execution_commits('action-1') == original
    assert lance.dataset(outputs['out']['uri']).to_table().to_pylist() == [{'id': 'a'}]


def test_declared_checkpoint_rejects_wrong_sink_before_write(tmp_path):
    schema, outputs = declared(tmp_path)
    def execute():
        return save([{'id': 'a'}], outputs, schema)
    reference = interrupt_after_success(tmp_path / 'run', execute)
    other = tmp_path / 'other.lance'
    original = lance.dataset(outputs['out']['uri']).version
    with pytest.raises(RecoveryContractError, match='identity|declaration|changed'):
        with RunContext.resume(reference):
            save([{'id': 'a'}], {'out': {'uri': str(other)}}, schema)
    assert not other.exists()
    assert lance.dataset(outputs['out']['uri']).version == original
    assert lance.dataset(outputs['out']['uri']).to_table().to_pylist() == [{'id': 'a'}]


def test_legacy_checkpoint_cannot_supply_declared_resource_read(tmp_path):
    from types import SimpleNamespace
    # Read-only legacy fixture is evidence, never authority for a current read.
    path = tmp_path / 'old.json'
    path.write_text(json.dumps({'format': 1, 'identity': 'historical', 'outputs': {}}))
    before = path.read_bytes()
    with pytest.raises(ValueError, match='declared native checkpoint'):
        data.read_checkpoint(SimpleNamespace(path=path, state=json.loads(before)), 'out')
    assert path.read_bytes() == before


@pytest.mark.parametrize('commit_number', [1, 2])
def test_recovery_after_table_commit_before_checkpoint_publish(tmp_path, monkeypatch, commit_number):
    from demiflow.execution import commit
    schema, outputs = declared(tmp_path)
    rows = [{'id': 'a'}, {'id': 'b'}]
    calls = []
    def execute():
        return save(rows, outputs, schema)
    def fault(name, commit_id):
        if name != 'after_sink_commit_before_receipt':
            return
        fact = run.journal.execution_commit(commit_id)
        if fact['action_id'] not in {key for key, _ in native_scopes(run.journal)}:
            return
        calls.append(commit_id)
        if len(calls) == commit_number:
            raise OSError('injected original native receipt interruption')
    with monkeypatch.context() as patch:
        patch.setattr(commit, 'fault_point', fault)
        with pytest.raises(OSError, match='injected original native receipt'):
            with RunContext.start(tmp_path / 'run') as run:
                reference = run.ref
                execute()
    assert len(calls) == commit_number
    assert lance.dataset(outputs['out']['uri']).count_rows() == commit_number
    before = run.journal.execution_commit(calls[-1])
    assert before['status'] == 'prepared' and before['receipts'] == {}
    with RunContext.resume(reference) as recovered:
        stats = execute()
    assert stats.outputs['out']['version'] == 2
    assert data.read_lance(**stats.outputs['out']).take_all() == rows
    assert recovered.journal.execution_commit(calls[-1])['status'] == 'acked'
    facts = [f for key, _ in native_scopes(recovered.journal) for f in recovered.journal.execution_commits(key)]
    assert all(f['status'] == 'acked' for f in facts)
    with recovered.journal._connection() as db:
        assert db.execute("SELECT consumed FROM runtime_budgets WHERE name='output_rows'").fetchone()[0] == 2


def test_disk_key_budget_rejects_before_batch_commit_and_releases_files(tmp_path):
    schema, outputs = declared(tmp_path)
    with pytest.raises(DataflowError, match='key index') as failure:
        with RunContext.start(tmp_path / 'run') as run:
            data.from_items([{'id': str(i)} for i in range(1000)]).save_lance(outputs['out']['uri'],
                schema=schema, key='id', stage='out', mode='append', max_batch=1000,
                max_index_bytes=8192).run_stream()
    assert failure.value.code == 'RESOURCE_INFEASIBLE'
    assert not Path(outputs['out']['uri']).exists()
    assert not list((tmp_path / 'out.stream-writer').glob('keys-*'))
    with run.journal._connection() as db:
        assert db.execute('SELECT count(*) FROM runtime_receipts').fetchone()[0] == 0
