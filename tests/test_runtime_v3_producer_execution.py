"""Actual worker graph recovery across queue-native / Dataset cut boundaries."""
import json

import pytest

from demiflow import data
from demiflow.execution.queue import ProducerSourceSpec, task_message
from demiflow.execution.pipeline.run_context import RunContext
from test_queue_v2_protocol import channel


def _expand(row):
    for index in range(row['n']):
        yield {'messages': [task_message(f"{row['id']}-{index}", {'i': index}, pool='p')]}


@pytest.mark.parametrize('seam', ['native_unit', 'dataset_cut'])
def test_same_run_producer_recovers_zero_and_many_outputs(channel, tmp_path, monkeypatch, seam):
    import demiflow.execution.commit as commits
    producer = channel.open('p', producer_id='recovery', source=ProducerSourceSpec(
        kind='records', source_id='fixed-records', uri='memory:records', version=1))
    directory = tmp_path / 'producer'
    rows = [{'id': 0, 'n': 0}, {'id': 1, 'n': 3}, {'id': 2, 'n': 2}]
    fired = []

    def fault(name, commit_id):
        if fired or name != 'after_checkpoint_committed':
            return
        with channel.database.connection() as db:
            native = db.execute('SELECT prepared FROM runtime_commits WHERE commit_id=?', (commit_id,)).fetchone()
        prepared = json.loads(native[0]) if native and native[0] else {}
        chunk = prepared.get('sinks', {}).get('publish', {})
        if ((seam == 'native_unit' and chunk.get('unit_end') and chunk['source_position'] == 1)
                or (seam == 'dataset_cut' and native is None and channel.state('p')['tasks'] == 3)):
            fired.append(commit_id)
            raise RuntimeError('producer-cut-interruption')

    def execute():
        return data.from_items(rows).flat_map(_expand).enqueue(channel.config, producer=producer).run_stream()

    monkeypatch.setattr(commits, 'fault_point', fault)
    with pytest.raises(RuntimeError, match='producer-cut-interruption'):
        with RunContext.start(directory) as run:
            reference = run.ref
            execute()
    assert fired and channel.state('p')['tasks'] == 3
    monkeypatch.setattr(commits, 'fault_point', lambda *_: None)
    with RunContext.resume(reference):
        result = execute()
    assert result.emitted == 5
    assert channel.seal(producer)['actual_tasks'] == 5
    with channel.database.connection() as db:
        assert db.execute('SELECT source_position,output_offset FROM queue_pools').fetchone() == (3, 0)
        assert db.execute("SELECT count(*) FROM runtime_commits WHERE status!='acked'").fetchone()[0] == 0
