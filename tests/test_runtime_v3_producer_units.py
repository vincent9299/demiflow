"""Native source units survive physical cuts; every effect uses the original journal."""
import json
import os
from pathlib import Path

import pytest
from demiflow.data.api import _use_executor
from demiflow.execution.action_driver.dataflow import DataflowExecution
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.execution.queue import QueueChannel, QueueConfig, ProducerSourceSpec
from demiflow.execution.run_context import RunContext
from runtime_v3_queue_business_scenarios import _producer, external_transport


@pytest.mark.parametrize('fault', ['none', 'after_source_cut', 'after_first_publication', 'after_unit_end'])
def test_native_producer_unit_park_and_resume(tmp_path, monkeypatch, fault):
    from runtime_v3_support import assert_tree_binding
    assert_tree_binding()
    assert os.environ['DEMIFLOW_RESOURCE_ROOT'].startswith('/tmp/demiflow-')
    monkeypatch.syspath_prepend(str(Path(__file__).parent/'native_search_fixtures'))
    channel = QueueChannel(QueueConfig(backend='sqlite', queue_id='parked-producer', path=str(tmp_path/'queue.sqlite')))
    producer = channel.open('p', producer_id='producer', source=ProducerSourceSpec(
        kind='keywords', source_id='keywords', uri='memory:oak-pine', version=1))
    executor = LocalDatasetExecutor(workers=4, resource_root=tmp_path)
    injected = []
    cuts = []
    import demiflow.execution.commit as commits
    advance = DataflowExecution._advance_cut
    def observe(execution):
        value = advance(execution)
        image = execution._restore_image
        if image and image.commit_id not in [cut['commit_id'] for cut in cuts]:
            fact = execution._action.context.journal.execution_commit(image.commit_id)
            assert fact['status'] == 'acked'
            with channel.database.connection() as db:
                native = db.execute('SELECT source_position,output_offset,tasks FROM queue_pools').fetchone()
            cuts.append({'commit_id': image.commit_id, 'native': native,
                         'unit': execution._acquisition._unit,
                         'parked': execution._durable_residency is not None})
            if fault == 'after_source_cut' and not injected and native[0] == 0 and native[2] == 0:
                assert execution._acquisition._unit is not None
                injected.append(image.commit_id)
                raise RuntimeError('owned-producer-interruption')
        return value
    def native_fault(name, commit_id):
        if injected: return
        if fault == 'after_first_publication' and name != 'after_sink_receipt': return
        if fault == 'after_unit_end' and name != 'after_acked': return
        if fault not in {'after_first_publication', 'after_unit_end'}: return
        with channel.database.connection() as db:
            row = db.execute('SELECT prepared FROM runtime_commits WHERE commit_id=?', (commit_id,)).fetchone()
        prepared = json.loads(row[0]).get('sinks', {}).get('publish', {}) if row and row[0] else {}
        match = bool(prepared.get('messages')) if fault == 'after_first_publication' else (
            prepared.get('unit_end') and not prepared.get('eos'))
        if match:
            injected.append(commit_id)
            raise RuntimeError('owned-producer-interruption')
    try:
        with _use_executor(executor), external_transport() as (base, calls, models):
            with monkeypatch.context() as patch:
                patch.setattr(DataflowExecution, '_advance_cut', observe)
                patch.setattr(commits, 'fault_point', native_fault)
                if fault == 'none':
                    with RunContext.start(tmp_path/'run', limits={'timeout_s':120}):
                        _producer(channel, producer, tmp_path, base)
                else:
                    with pytest.raises(Exception, match='owned-producer-interruption'):
                        with RunContext.start(tmp_path/'run', limits={'timeout_s':120}) as run:
                            reference = run.ref
                            _producer(channel, producer, tmp_path, base)
            if fault != 'none':
                assert injected
                # Cache deletion proves the original native search receipt owns reuse.
                cache = tmp_path/'search-cache.sqlite'
                if cache.exists(): cache.unlink()
                with RunContext.resume(reference):
                    _producer(channel, producer, tmp_path, base)
            assert any(cut['parked'] for cut in cuts)
            assert channel.seal(producer)['actual_tasks'] == 4
            assert sum(calls.values()) == 2 and not models
            with channel.database.connection() as db:
                assert db.execute('SELECT source_position,output_offset FROM queue_pools').fetchone() == (2, 0)
                tasks = db.execute('SELECT task_id FROM queue_tasks ORDER BY task_id').fetchall()
                assert tasks == [('oak-0',), ('oak-1',), ('pine-0',), ('pine-1',)]
                prepared = [json.loads(row[0]) for row in db.execute(
                    'SELECT prepared FROM runtime_commits WHERE prepared IS NOT NULL').fetchall()]
                publications = [value['sinks']['publish'] for value in prepared if 'publish' in value['sinks']]
                for position in (0, 1):
                    assert sorted(p['output_ordinal'] for p in publications if p['source_position'] == position) == [0, 1, 2]
    finally:
        executor.close()
