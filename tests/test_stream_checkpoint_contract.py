"""Declared checkpoint ownership, fixed reads and retained partial commits."""
import json

import lance
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.execution.stream_checkpoint import StreamCheckpoint


def declared(tmp_path):
    schema = data.schema({'fields': [{'name': 'id', 'type': 'string', 'nullable': False}]})
    return schema, {'out': {'uri': str(tmp_path / 'out.lance'), 'schema': schema}}


def test_empty_checkpoint_read_and_fixed_completed_read(tmp_path):
    schema, outputs = declared(tmp_path)
    cp = data.open_checkpoint(str(tmp_path / 'checkpoint.json'), identity={'input': 1}, outputs=outputs)
    empty = data.read_checkpoint(cp, 'out')
    assert empty.take_all() == []
    stats = (data.from_items([{'id': 'a'}]).save_lance(outputs['out']['uri'], schema=schema,
             key='id', stage='out', mode='append', max_batch=1).run_stream(checkpoint=cp))
    fixed = data.read_checkpoint(cp, 'out')
    assert stats.outputs['out'] == cp.state['outputs']['out']
    assert fixed.take_all() == [{'id': 'a'}]
    (data.from_items([{'id': 'b'}]).save_lance(outputs['out']['uri'], schema=schema,
     key='id', stage='out', mode='append', max_batch=1).run_stream(checkpoint=cp))
    assert fixed.take_all() == [{'id': 'a'}]
    assert data.read_checkpoint(cp, 'out').count() == 2


def test_checkpoint_empty_stream_really_commits_schema(tmp_path):
    schema, outputs = declared(tmp_path)
    cp = data.open_checkpoint(str(tmp_path / 'checkpoint.json'), identity={}, outputs=outputs)
    data.from_arrow(pa.Table.from_pylist([], schema=schema)).save_lance(outputs['out']['uri'],
        schema=schema, key='id', stage='out', mode='append').run_stream(checkpoint=cp)
    ref = cp.state['outputs']['out']
    assert ref['version'] == 1 and lance.dataset(**ref).count_rows() == 0
    assert lance.dataset(**ref).schema.equals(schema, check_metadata=True)


def test_checkpoint_rejects_redirect_before_opening_table(tmp_path, monkeypatch):
    _, outputs = declared(tmp_path)
    path = tmp_path / 'checkpoint.json'
    data.open_checkpoint(str(path), identity={}, outputs=outputs)
    state = json.loads(path.read_text())
    state['outputs'] = {'out': {'uri': '/undeclared/table.lance', 'version': 1}}
    path.write_text(json.dumps(state))
    def forbidden(*args, **kwargs):
        pytest.fail('An unowned URI was opened before checkpoint validation')
    monkeypatch.setattr(lance, 'dataset', forbidden)
    with pytest.raises(ValueError, match='declared|ownership'):
        data.open_checkpoint(str(path), identity={}, outputs=outputs)


def test_checkpoint_schema_change_and_symlink_are_rejected(tmp_path):
    _, outputs = declared(tmp_path)
    path = tmp_path / 'checkpoint.json'
    data.open_checkpoint(str(path), identity={}, outputs=outputs)
    changed = {'out': {**outputs['out'], 'schema': pa.schema([('id', pa.int64())])}}
    with pytest.raises(ValueError, match='identity|schema|declaration'):
        data.open_checkpoint(str(path), identity={}, outputs=changed)
    alias = tmp_path / 'alias.json'
    alias.symlink_to(path)
    with pytest.raises(ValueError, match='symlink|symbolic'):
        data.open_checkpoint(str(alias), identity={}, outputs=outputs)


def test_declared_checkpoint_rejects_wrong_sink_before_write(tmp_path):
    schema, outputs = declared(tmp_path)
    cp = data.open_checkpoint(str(tmp_path / 'checkpoint.json'), identity={}, outputs=outputs)
    other = tmp_path / 'other.lance'
    with pytest.raises(ValueError, match='declared|ownership'):
        data.from_items([{'id': 'a'}]).save_lance(str(other), schema=schema,
            key='id', stage='out', mode='append').run_stream(checkpoint=cp)
    assert not other.exists() and not (tmp_path / 'out.lance').exists()


def test_legacy_checkpoint_cannot_supply_declared_resource_read(tmp_path):
    legacy = StreamCheckpoint(tmp_path / 'old.json', identity={})
    with pytest.raises(ValueError, match='declared'):
        data.read_checkpoint(legacy, 'out')


@pytest.mark.parametrize('commit_number', [1, 2])
def test_recovery_after_table_commit_before_checkpoint_publish(tmp_path, monkeypatch, commit_number):
    schema, outputs = declared(tmp_path)
    path = str(tmp_path / 'checkpoint.json')
    cp = data.open_checkpoint(path, identity={}, outputs=outputs)
    original = StreamCheckpoint.committed
    calls = []
    def fail_publish(self, *args):
        calls.append(True)
        if len(calls) == commit_number:
            raise OSError('injected checkpoint publication failure')
        return original(self, *args)
    monkeypatch.setattr(StreamCheckpoint, 'committed', fail_publish)
    with pytest.raises(Exception, match='injected|pipeline|Stream'):
        data.from_items([{'id': 'a'}]).save_lance(outputs['out']['uri'], schema=schema,
            key='id', stage='out', mode='append', max_batch=1).run_stream(checkpoint=cp)
    monkeypatch.setattr(StreamCheckpoint, 'committed', original)
    restored = data.open_checkpoint(path, identity={}, outputs=outputs)
    assert restored.state['outputs']['out']['version'] == commit_number
    assert data.read_checkpoint(restored, 'out').count() == commit_number - 1
    data.from_items([{'id': 'a'}]).save_lance(outputs['out']['uri'], schema=schema,
        key='id', stage='out', mode='append', max_batch=1).run_stream(checkpoint=restored)
    assert data.read_checkpoint(restored, 'out').take_all() == [{'id': 'a'}]


def test_disk_key_budget_rejects_before_batch_commit_and_releases_files(tmp_path):
    schema, outputs = declared(tmp_path)
    cp = data.open_checkpoint(str(tmp_path / 'checkpoint.json'), identity={}, outputs=outputs)
    with pytest.raises(Exception, match='index|pipeline|Stream'):
        data.from_items([{'id': str(i)} for i in range(1000)]).save_lance(outputs['out']['uri'],
            schema=schema, key='id', stage='out', mode='append', max_batch=1000,
            max_index_bytes=8192).run_stream(checkpoint=cp)
    assert lance.dataset(outputs['out']['uri']).count_rows() == 0
    assert not list((tmp_path / 'out.stream-writer').glob('keys-*'))
