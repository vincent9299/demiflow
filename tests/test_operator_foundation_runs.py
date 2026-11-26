"""Managed native families commit real result tables using the same Flow path."""
import lance
import pytest

from test_forge_runs import base, root, custom, execute, rows


@pytest.mark.parametrize('family', ['async', 'flat_batch', 'flat_batch_bounded', 'actor', 'actor_instance', 'iterator'])
def test_managed_native_families(root, family):
    if family == 'flat_batch_bounded':
        from demiflow.workbench.projects import workspace as ws
        from demiflow.forge.platform import content_digest
        _, workspace, _ = ws.load_workspace(root)
        ws.set_budgets(root, {'max_row_bytes': 1024**2, 'max_batch_bytes': 2*1024**2},
                       expected=content_digest(workspace))
    declaration = '''from typing import TypedDict
class Input(TypedDict):
    case_id: str | None
class Output(TypedDict):
    case_id: str
'''
    if family == 'async':
        operator = declaration + '''async def transform(row: Input) -> Output | list[Output] | None:
    return [{'case_id': str(row['case_id']) + '-a'}, {'case_id': str(row['case_id']) + '-b'}]
'''
        wiring = '.map_async(transform, concurrency=1, queue_depth=1)'
    elif family in {'flat_batch', 'flat_batch_bounded'}:
        operator = declaration + '''def transform(row: Input) -> list[Output]:
    return [{'case_id': str(row['case_id']) + '-a'}, {'case_id': str(row['case_id']) + '-b'}]
async def batch(rows: list[Output]) -> list[Output]:
    return rows
'''
        wiring = '.flat_map(transform).batch_map(batch, max_batch=2, concurrency=1, queue_depth=1)'
    elif family == 'iterator':
        operator = declaration + '''from typing import Iterator
def transform(row: Input) -> Iterator[Output]:
    yield {'case_id': str(row['case_id']) + '-a'}
    yield {'case_id': str(row['case_id']) + '-b'}
'''
        wiring = '.flat_map(transform)'
    else:
        operator = declaration + '''class transform:
    def __init__(self):
        self.ready = False
    def snapshot_state(self) -> dict:
        return {}  # ready is a live resource flag, not semantic state.
    def restore_state(self, state: dict) -> None:
        if state != {}:
            raise ValueError('unexpected actor semantic state')
    async def astart(self) -> None:
        self.ready = True
    async def __call__(self, row: Input) -> list[Output]:
        if not self.ready:
            raise ValueError('actor did not start')
        return [{'case_id': str(row['case_id']) + '-a'}, {'case_id': str(row['case_id']) + '-b'}]
    async def aclose(self) -> None:
        self.ready = False
'''
        wiring = '.map_async(transform, concurrency=1, queue_depth=1)'
        if family == 'actor_instance':
            operator = operator.replace('def __init__(self):', 'def __init__(self, ready: bool):').replace('self.ready = False', 'self.ready = ready', 1)
            wiring = '.map_async(transform(False), concurrency=1, queue_depth=1)'
    names = 'transform, batch' if family in {'flat_batch', 'flat_batch_bounded'} else 'transform'
    flow = f'''from operators.custom import {names}
def run(*, source, target):
    result = data.read_lance(source['uri'], version=source['version'], columns=['case_id']){wiring}.materialize()
    return result.write_lance(target['uri'], mode='create', return_receipt=True)
'''
    prepared = custom(root, flow, operators=operator)
    if family in {'flat_batch', 'flat_batch_bounded'}:
        from demiflow.forge.runs import manager as runs

        from demiflow.forge.pipeline_policy import limits_with
        _, sealed = runs._load(root, prepared['run_id'])
        caps = sealed['manifest']['limits']
        if family == 'flat_batch':
            defaults = limits_with()
            assert caps['memory_bytes'] == defaults['memory_bytes']
            assert caps['max_row_bytes'] < defaults['max_row_bytes']
            assert caps['max_batch_bytes'] < defaults['max_batch_bytes']
        else:
            assert caps['max_row_bytes'] == 1024**2
            assert caps['max_batch_bytes'] == 2*1024**2
    result = execute(root, prepared)
    assert result['status'] == 'succeeded', result
    source = lance.dataset(str(root / 'data/candidates.lance'), version=3).to_table(columns=['case_id']).to_pylist()
    expected = sorted(str(row['case_id']) + suffix for row in source for suffix in ('-a', '-b'))
    assert sorted(row['case_id'] for row in rows(root, result)) == expected
    materializations = [facts for facts in result['actions'].values() if 'materialized' in facts]
    assert len(materializations) == 1
    assert materializations[0]['materialized']['rows'] == len(expected)
    if family in {'actor', 'actor_instance'}:
        observed = next(o for o in result['observations'] if o.get('lifecycle', {}).get('created'))
        assert observed['lifecycle']['started'] is True
        assert observed['lifecycle']['closed'] is True


def test_managed_fixed_table_lookup_and_observations(root):
    operator = '''from typing import TypedDict
class Input(TypedDict):
    case_id: str | None
class Output(TypedDict):
    case_id: str
def key(row: Input) -> Output:
    return {'case_id': str(row['case_id'])}
'''
    flow = '''from operators.custom import key
def run(*, source, target):
    result = data.read_lance(source['uri'], version=source['version'], columns=['case_id']).map(key)
    result = result.lookup_lance(source, on='case_id', columns=['case_id'], max_matches=1, max_bytes=4096).materialize()
    return result.write_lance(target['uri'], mode='create', return_receipt=True)
'''
    value = custom(root, flow, operators=operator)
    result = execute(root, value)
    assert result['status'] == 'succeeded', result
    assert all(r['matches'] == [{'case_id': r['case_id']}] for r in rows(root, result))
    lookup = next(n['id'] for n in value['report']['nodes'] if n['kind'] == 'lookup')
    observed, = [o for o in result['observations'] if o['node_id'] == lookup]
    assert observed['state'] == 'succeeded'
    assert observed['rows_input'] == observed['rows_output'] == len(rows(root, result))


from test_map_prompt_async import server
from test_operator_foundation_models import model_files


def test_managed_model_results_evidence_and_read_only_access(root, server):
    from demiflow.forge.runs import manager as runs

    from demiflow.workbench.projects import workspace as ws
    _, workspace, _ = ws.load_workspace(root)
    ws.bind_platform(root, expected=workspace['platform']['digest'], extras=['llm'])
    model_files(root)
    text = (root/'flows/model.py').read_text().replace("data.read_lance(source, version=1)",
        "data.read_lance(source['uri'], version=source['version'], columns=['case_id']).limit(2).rename_columns({'case_id': 'item'}).materialize()")
    text = text.replace("write_lance(target,", "write_lance(target['uri'],")
    prepared = custom(root, text.removeprefix('from demiflow import data\n'))
    result = execute(root, prepared)
    assert result['status'] == 'succeeded', result
    values = rows(root, result)
    assert len(values) == len(server['requests']) == 2
    assert all(row['answer'] == 'ok' for row in values)
    reference = values[0]['call']['request_ref']
    saved = runs.evidence(root, result['run_id'], reference)
    assert saved['document']['payload']['model'] == 'mock-model'
    assert len(server['requests']) == 2  # reads cannot replay requests
    with pytest.raises(ValueError, match='declared'):
        runs.evidence(root, result['run_id'], {**reference, 'journal_path': str(root/'other.sqlite')})
    assert not (runs.directory(root, result['run_id'])/'work/evidence').exists()


def test_managed_model_cancel_stops_dispatch_and_preserves_request(root, server, tmp_path):
    import json
    import sqlite3
    import threading
    import time
    from demiflow.forge.runs import manager as runs

    from demiflow.workbench.projects import workspace as ws
    _, workspace, _ = ws.load_workspace(root)
    ws.bind_platform(root, expected=workspace['platform']['digest'], extras=['llm'])
    from test_forge_runs import wait

    entered, release = threading.Event(), threading.Event()
    def respond(body, index):
        entered.set()
        release.wait(3)
        return {'result': 'ok'}
    server['respond'] = respond
    model_files(root)
    text = (root/'flows/model.py').read_text().removeprefix('from demiflow import data\n')
    text = text.replace('data.read_lance(source, version=1)',
        "data.read_lance(source['uri'], version=source['version'], columns=['case_id']).limit(2).rename_columns({'case_id': 'item'}).materialize()")
    text = text.replace('write_lance(target,', "write_lance(target['uri'],")
    prepared = custom(root, text)
    runs.start(root, prepared['run_id'], expected=prepared['digest'])
    try:
        assert entered.wait(30), runs.show(root, prepared['run_id'])
        started = time.monotonic()
        runs.cancel(root, prepared['run_id'])
        result = wait(root, prepared['run_id'])
        elapsed = time.monotonic() - started
    finally:
        release.set()
        if runs.show(root, prepared['run_id'])['status'] in runs.ACTIVE:
            runs.cancel(root, prepared['run_id'])
    assert result['status'] == 'cancelled', result
    assert elapsed < 12 and len(server['requests']) == 1
    assert all(output['result'] is None for output in result['outputs'])
    # Model calls are recorded in the run's own journal.
    ledger = runs.directory(root, prepared['run_id'])/'run.sqlite'
    with sqlite3.connect(ledger.resolve().as_uri() + '?mode=ro', uri=True) as db:
        calls = db.execute('SELECT request_json, response_json, error_json FROM calls').fetchall()
    assert len(calls) == 1 and calls[0][0] is not None and calls[0][1] is None
    (tmp_path/'cancellation.json').write_text(json.dumps({'seconds': elapsed, 'requests': 1,
        'status': result['status'], 'response_recorded': False, 'error_recorded': calls[0][2] is not None}))
