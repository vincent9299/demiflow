"""Managed native flows use the original service, journal and fixed results."""
import json
from pathlib import Path
import pytest
import time

from demiflow.workbench.projects import workspace as ws
from demiflow.workbench.runs import manager as runs
from demiflow.workbench import project_operations as project_ops
from test_forge_runs import base, root, execute
from test_forge_resume import stopped
from test_forge_runs import wait


def native_flow(root):
    (root / 'flows/native.py').write_text('''from demiflow import data
import pyarrow as pa
def run(*, target, stage):
    schema = pa.schema([('x', pa.int64())])
    return data.from_items([{'x': 1}, {'x': 2}]).save_lance(stage, schema=schema,
        key='x', stage='rows', mode='append', max_rows=2, max_index_bytes=8388608
    ).write_lance(target, schema=schema, mode='create', return_receipt=True)
''')
    arguments = dict(target=str(root / 'native-output.lance'), stage=str(root / 'native-stage.lance'))
    (root / 'flows/native.json').write_text(json.dumps(dict(format='demiflow_forge_flow_parameters_v1', arguments=arguments)))
    ws.register_flow(root, 'flows/native.py:run', params='flows/native.json')
    return runs.prepare(root, 'flows/native.py:run', params='flows/native.json')


def test_native_submit_seal_monitor_and_fixed_results(root):
    value = native_flow(root)
    assert value['status'] == 'prepared'
    assert value['report']['status'] == 'compiled'
    assert not (root / 'native-output.lance').exists()
    result = execute(root, value)
    assert result['status'] == 'succeeded', (result, runs.logs(root, value['run_id']))
    assert len(result['outputs']) == 2
    for output in result['outputs']:
        _, ref, uri = runs.result_table(root, result['run_id'], output['name'], attempt=1)
        assert ref.row_count == 2 and uri == Path(output['uri'])
    from demiflow.workbench.graph.model import get
    graph = get(root, basis_kind='run', run_id=result['run_id'], attempt=1)
    assert graph['logical']['actions'][0]['status'] == 'sealed'


def test_native_source_drift_rejected_before_worker(root):
    value = native_flow(root)
    with (root / 'flows/native.py').open('a') as stream:
        stream.write('\n# modified after preparation\n')
    with pytest.raises(Exception, match='changed'):
        runs.start(root, value['run_id'], expected=value['digest'])
    assert not (root / 'native-output.lance').exists()


def test_native_stop_resume_keeps_original_run_and_stage_receipts(root):
    (root / 'flows/native_stop.py').write_text('''from demiflow import data
import pyarrow as pa
import time
def slow(row):
    time.sleep(2)
    return row
def run(*, directory):
    schema = pa.schema([('x', pa.int64())])
    rows = data.from_items([{'x': i} for i in range(8)])
    rows = rows.save_lance(directory+'/stage.lance', schema=schema, key='x', stage='rows',
        mode='append', max_rows=8, max_batch=1, max_index_bytes=8388608)
    return rows.map(slow).write_lance(directory+'/final.lance', schema=schema, mode='create', return_receipt=True)
''')
    params = dict(format='demiflow_forge_flow_parameters_v1', arguments={'directory': str(root / 'stop-output')})
    (root / 'flows/native_stop.json').write_text(json.dumps(params))
    ws.register_flow(root, 'flows/native_stop.py:run', params='flows/native_stop.json')
    prepared = runs.prepare(root, 'flows/native_stop.py:run', params='flows/native_stop.json',
                            limits={'cancel_grace_s': 1, 'timeout_s': 180})
    identity = prepared['run_id']
    runs.start(root, identity, expected=prepared['digest'], expected_attempt=1)
    try:
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            current = runs.show(root, identity)
            if any(o['result'] for o in current['outputs']):
                break
            assert current['status'] not in runs.TERMINAL, (current, runs.logs(root, identity))
            time.sleep(.1)
        assert any(o['result'] for o in current['outputs'])
        runs.cancel(root, identity, expected_attempt=1)
        before = stopped(root, identity)
        assert before['status'] == 'cancelled', before
        committed = next(output for output in before['outputs'] if output['result'])
        old_selector = 'run:' + identity + ':' + committed['name'] + ':1'
        latest_selector = 'run:' + identity + ':' + committed['name']
        old_binding = project_ops.describe_table(root, latest_selector)['binding']['digest']
        from demiflow.workbench.runs.view import snapshot
        assert snapshot(root, identity)['availability'] == 'available'
        runs.resume(root, identity, expected=prepared['digest'], expected_attempt=1)
        after = wait(root, identity)
        assert after['status'] == 'succeeded', (after, runs.logs(root, identity))
        assert after['attempt'] == 2 and runs.list_runs(root)['total'] == 1
        assert runs.show(root, identity, attempt=1) == before
        assert all(o['result']['ref']['row_count'] == 8 for o in after['outputs'])
        assert project_ops.describe_table(root, old_selector)['reference']['ref'] == committed['result']['ref']
        latest = project_ops.describe_table(root, latest_selector)
        explicit = project_ops.describe_table(root, latest_selector + ':2')
        assert latest['reference']['ref'] == explicit['reference']['ref']
        if latest['reference']['ref'] != committed['result']['ref']:
            assert latest['binding']['digest'] != old_binding
            with pytest.raises(ValueError, match='binding'):
                project_ops.describe_table(root, latest_selector, expected_binding=old_binding)
        else:
            # Existing preview identity is the exact table, not an added
            # attempt generation. Reusing the same fixed ref stays admissible.
            assert project_ops.describe_table(root, latest_selector, expected_binding=old_binding)['reference']['ref'] == committed['result']['ref']
        import lance
        for output in after['outputs']:
            table = lance.dataset(output['uri'], version=output['result']['ref']['lance_version']).to_table()
            assert sorted(table.column('x').to_pylist()) == list(range(8))
    finally:
        state = runs.show(root, identity)
        if state['status'] in runs.ACTIVE:
            runs.cancel(root, identity, expected_attempt=state['attempt'])
            stopped(root, identity)
