"""Public fixed-source merge uses its original worker cut and one external publication."""
import lance
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.execution import RunContext, UnresolvedCommit
from demiflow.execution.journal.run_journal import RunJournal
from demiflow.execution.journal.publication_facts import inspect_write_receipt


def _inputs(tmp_path, n):
    patch, target = str(tmp_path/'patch.lance'), str(tmp_path/'target.lance')
    lance.write_dataset(pa.table({'key': pa.array(range(n), type=pa.int64()),
                                 'value': pa.array(range(n), type=pa.int64())}), patch)
    lance.write_dataset(pa.table({'key': range(max(n, 1)), 'value': [-1] * max(n, 1),
                                 'keep': list(range(max(n, 1)))}), target)
    return patch, target


def _merge(source, target):
    return source.write_lance(target, mode='merge', on='key', update_columns=['value'], return_receipt=True)


def test_fixed_source_cut_resume_and_pin(tmp_path, monkeypatch):
    from demiflow.execution import commit as protocol
    patch, target = _inputs(tmp_path, 13)
    directory = tmp_path/'run'
    def interrupt(name, commit_id):
        if name == 'after_sink_commit_before_receipt':
            raise RuntimeError('before receipt')
    source = lambda: data.read_lance(patch, version=1, batch_size=6)
    with pytest.raises(RuntimeError, match='before receipt'), RunContext.start(directory,
            limits={'checkpoint_rows': 4,'checkpoint_unconfirmed_rows': 4}) as run:
        ref = run.ref
        with monkeypatch.context() as mp:
            mp.setattr(protocol, 'fault_point', interrupt)
            _merge(source(), target)
    journal = RunJournal(directory)
    cuts = journal.execution_commits('action-1')
    assert len(cuts) == 1
    first = cuts[0]['prepared']['sinks']['write_lance']
    assert first['native'] is None and first['manifest']['final'] is False
    assert first['rows'] == 4 and inspect_write_receipt(journal, 'action-1') is None
    assert lance.dataset(target).version == 1
    lance.write_dataset(pa.table({'key': [99], 'value': [99]}), patch, mode='append')
    with RunContext.resume(ref):
        receipt = _merge(source(), target)
    assert receipt.committed_version == 2
    assert lance.dataset(target).to_table().to_pydict() == {
        'key': list(range(13)), 'value': list(range(13)), 'keep': list(range(13))}
    facts = journal.execution_commits('action-1')
    assert [c['prepared']['sinks']['write_lance']['rows'] for c in facts] == [4, 8, 12, 13]
    final = facts[-1]['prepared']['sinks']['write_lance']
    assert final['native']['publication'] == final['ref']
    assert not list(directory.rglob('*.arrow'))
    assert inspect_write_receipt(journal,'action-1')['committed_version'] == 2


def test_empty_fixed_merge_records_actual_terminal_receipt(tmp_path):
    patch, target = _inputs(tmp_path, 0)
    with RunContext.start(tmp_path/'run') as run:
        receipt = _merge(data.read_lance(patch, version=1), target)
        projected = inspect_write_receipt(run.journal, 'action-1')
        assert receipt.input_rows == 0
        assert projected['committed_version'] == receipt.committed_version == lance.dataset(target).version


def test_python_callback_uses_original_record_path(tmp_path):
    patch, target = _inputs(tmp_path, 4)
    def change(row):
        return {**row, 'value': row['value'] + 10}
    with RunContext.start(tmp_path/'run') as run:
        receipt = _merge(data.read_lance(patch, version=1).map(change), target)
        final = run.journal.execution_commits('action-1')[-1]['prepared']['sinks']['write_lance']
        assert final['native'].get('path') and 'publication' not in final['native']
        assert receipt.committed_version == 2
    assert lance.dataset(target).to_table()['value'].to_pylist() == [10, 11, 12, 13]


def test_fixed_merge_ambiguous_native_commit_never_reinvokes_writer(tmp_path, monkeypatch):
    from demiflow.execution.outputs.lance_sink import LanceSink
    patch, target = _inputs(tmp_path, 3)
    source = lambda: data.read_lance(patch, version=1, batch_size=3)
    directory = tmp_path/'run'
    def interrupt(*args):
        raise RuntimeError('native commit returned before receipt')
    with pytest.raises(RuntimeError, match='before receipt'), RunContext.start(directory) as run:
        ref = run.ref
        with monkeypatch.context() as mp:
            mp.setattr(LanceSink, '_receipt', interrupt)
            _merge(source(), target)
    assert lance.dataset(target).version == 2
    with pytest.raises(UnresolvedCommit), RunContext.resume(ref):
        _merge(source(), target)
    assert lance.dataset(target).version == 2
    assert lance.dataset(target).to_table()['value'].to_pylist() == [0, 1, 2]


def test_foreign_target_after_fixed_merge_prepare_fences_commit(tmp_path, monkeypatch):
    from demiflow.execution import commit as protocol
    patch, target = _inputs(tmp_path, 3)
    def compete(name, _):
        if name == 'after_prepared':
            lance.write_dataset(pa.table({'key':[9], 'value':[90], 'keep':[9]}), target, mode='append')
    with pytest.raises(UnresolvedCommit), RunContext.start(tmp_path/'run'):
        monkeypatch.setattr(protocol, 'fault_point', compete)
        _merge(data.read_lance(patch, version=1, batch_size=3), target)
    table = lance.dataset(target)
    assert table.version == 2
    assert table.to_table()['value'].to_pylist() == [-1, -1, -1, 90]
