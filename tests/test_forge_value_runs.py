"""Query/guard facts survive the sealed worker and durable run journal."""
import pytest
import json
import sqlite3
import time
from pathlib import Path
import lance

from demiflow.workbench.projects import workspace as ws
from demiflow.workbench.runs import manager as runs

from test_forge_runs import base, root, custom, execute


@pytest.mark.parametrize('minimum', [1, 4])
def test_sealed_query_guard_run(root, minimum):
    prepared = custom(root, '''
def run(*, source, target):
    rows = data.read_lance(source['uri'], version=source['version'])
    total = rows.count()
    data.require(total >= MINIMUM, 'incomplete input')
    return rows.write_lance(target['uri'], mode='create', return_receipt=True)
'''.replace('MINIMUM', str(minimum)))
    assert prepared['report']['format'] == 'demiflow_forge_flow_inspection_v3'
    result = execute(root, prepared)
    assert result['status'] == ('succeeded' if minimum == 1 else 'failed'), result
    query = next(n for n in prepared['report']['nodes'] if n['kind'] == 'count')
    assert result['actions'][query['id']]['value'] == {'value': 3}
    assert (result['outputs'][0]['receipt'] is not None) == (minimum == 1)


def test_stable_output_uses_explicit_permission_and_never_redirects(root):
    ws.add_location(root, 'stable', root / 'data', writable=True, write_modes=['overwrite'])
    (root / 'flows/update.py').write_text('from demiflow import data\n'
        'def run(*, source, target):\n'
        '    rows = data.read_lance(source["uri"], version=source["version"]).limit(1)\n'
        '    return rows.write_lance(target["uri"], mode=target["mode"], expected_version=target["expected_version"], return_receipt=True)\n')
    parameters = {'format': 'demiflow_forge_flow_parameters_v2', 'arguments': {
        'source': {'kind': 'table', 'alias': 'candidates'},
        'target': {'kind': 'output', 'location_id': 'stable', 'relative_uri': 'candidates.lance', 'mode': 'overwrite', 'expected_version': 3}}}
    (root / 'flows/update.json').write_text(json.dumps(parameters))
    ws.register_flow(root, 'flows/update.py:run', params='flows/update.json')
    prepared = runs.prepare(root, 'flows/update.py:run', params='flows/update.json')
    assert prepared['outputs'][0]['uri'] == str(root / 'data/candidates.lance')
    result = execute(root, prepared)
    assert result['status'] == 'succeeded', result
    assert result['outputs'][0]['receipt']['committed_version'] == 4
    assert lance.dataset(str(root / 'data/candidates.lance'), version=3).count_rows() == 3
    page = ws.sample_table(root, 'run:' + result['run_id'] + ':target')
    assert page['version'] == 4 and len(page['rows']) == 1
    assert ws.describe_table(root, 'candidates')['metadata']['version'] == 3


@pytest.mark.parametrize('minimum', [1, 4])
def test_sealed_checkpoint_partial_results_remain_readable(root, minimum):
    (root / 'data/stages').mkdir()
    ws.add_location(root, 'stages', root / 'data/stages', writable=True, write_modes=['checkpoint'])
    (root / 'flows/staged.py').write_text('''from demiflow import data
def run(*, source, stage, target, minimum):
    schema = data.schema({'fields': [{'name': 'case_id', 'type': 'string'}]})
    checkpoint = data.open_checkpoint('checkpoints/stages.json', identity={'input': source},
        outputs={'cases': {'uri': stage['uri'], 'schema': schema}})
    rows = data.read_lance(source['uri'], version=source['version'], columns=['case_id'])
    rows.save_lance(stage['uri'], schema=schema, key='case_id', stage='cases', mode='append',
        max_batch=1, queue_depth=1, max_row_bytes=4096, max_index_bytes=65536).run_stream(checkpoint=checkpoint)
    result = data.read_checkpoint(checkpoint, 'cases')
    data.require(result.count() >= minimum, 'not enough cases')
    return result.write_lance(target['uri'], mode='create', schema=schema, return_receipt=True)
''')
    parameters = {'format': 'demiflow_forge_flow_parameters_v2', 'arguments': {
        'source': {'kind': 'table', 'alias': 'candidates'}, 'minimum': minimum,
        'stage': {'kind': 'output', 'location_id': 'stages', 'relative_uri': 'cases.lance', 'mode': 'checkpoint', 'expected_version': None},
        'target': {'kind': 'output', 'location_id': 'local', 'relative_uri': 'complete.lance', 'mode': 'create', 'expected_version': None}}}
    (root / 'flows/staged.json').write_text(json.dumps(parameters))
    ws.register_flow(root, 'flows/staged.py:run', params='flows/staged.json')
    prepared = runs.prepare(root, 'flows/staged.py:run', params='flows/staged.json')
    result = execute(root, prepared)
    assert result['status'] == ('succeeded' if minimum == 1 else 'failed'), result
    stage, final = ({o['name']: o for o in result['outputs']}[name] for name in ('stage', 'target'))
    assert stage['receipt'] is None and stage['commit']['reference']['version'] == 4
    assert stage['result']['ref']['row_count'] == 3
    assert len(ws.sample_table(root, 'run:' + result['run_id'] + ':stage')['rows']) == 3
    assert (final['result'] is not None) == (minimum == 1)
    if minimum == 4:
        # Simulate lost control publication after the native checkpoint commits.
        # Only this disposable test run is edited; the original CP/Lance evidence
        # remains authoritative for explicit reconciliation.
        deadline = time.monotonic() + 5
        while any(runs.process_matches(result['state'].get(name)) for name in ('supervisor', 'worker')) and time.monotonic() < deadline:
            time.sleep(.05)
            result = runs.show(root, result['run_id'])
        with sqlite3.connect(runs.directory(root, result['run_id']) / 'run.sqlite') as db:
            db.execute('DELETE FROM action_events WHERE action_id=?', (stage['action_id'],))
        reconciled = runs.reconcile(root, result['run_id'])
        assert reconciled['reconciliation_errors'] == [], reconciled
        restored = next(o for o in reconciled['outputs'] if o['name'] == 'stage')
        assert restored['commit']['reference']['version'] == 4
        assert restored['result']['ref']['row_count'] == 3
        assert reconciled['status'] == 'failed'
