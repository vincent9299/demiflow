"""Original source -> native compiler, without starting the business action."""
import json
import os
import subprocess
import sys

import pytest

from test_forge_project_discovery import original, snapshot, cached_snapshot, business_bytes, register_fixture_flow
from demiflow.workbench.graph.preview import preview
from demiflow.workbench.projects import workspace as ws
from demiflow.forge.schema import ForgeError
from test_forge_public_api import service, request
from test_forge_workspace import demo, isolated


@pytest.mark.parametrize('import_write', [False, True])
def test_native_fixed_inputs_resolve_before_guard_but_project_import_stays_readonly(isolated, import_write):
    entry = isolated / 'flows/questions.py'
    if import_write:
        entry.write_text("from pathlib import Path\nPath('import-side-effect').write_text('bad')\n" + entry.read_text())
    before = business_bytes(isolated)
    if import_write:
        with pytest.raises(ForgeError, match='解析阶段'):
            preview(isolated, entry='flows/questions.py:select', params='flows/select_a.json')
    else:
        result = preview(isolated, entry='flows/questions.py:select', params='flows/select_a.json')
        assert result['kind'] == 'write_lance' and result['entry_completed'] is False
        assert len(result['dag']['nodes']) == 7
        assert sum(n['op_kind'] == 'lance' for n in result['dag']['nodes']) == 2
        assert not list(isolated.rglob('selected_a.lance'))
    assert business_bytes(isolated) == before


def test_original_helpers_and_parameter_branches_compile_without_execution(original):
    (original / 'operators/clean.py').write_text('def clean(row):\n    raise AssertionError("row executed")\n')
    (original / 'dispatch.py').write_text('''from demiflow import data
from operators.clean import clean
def config(): return {}
def source(): return data.from_items([{'id': 1}])
def run_pipeline(*, combine=False):
    ds = source().map(clean)
    if combine: ds = ds.union(source().filter(clean))
    return ds.take_all()
''')
    path = original / 'settings/compile.json'
    register_fixture_flow(original, 'settings/compile.json')
    results = []
    for combine in (False, True):
        path.write_text(json.dumps({'run_directory': '/unused', 'arguments': {'combine': combine}}))
        before = business_bytes(original)
        result = preview(original, entry='dispatch.py:run_pipeline', params='settings/compile.json')
        assert business_bytes(original) == before
        assert result['scope'] == 'first_action' and result['entry_completed'] is False
        assert result['kind'] == 'take_all' and 'physical' not in result and 'run_id' not in result
        assert 'operators/clean.py' in result['basis']['sources']
        assert result['basis']['params_sha256']
        results.append(result)
    assert len(results[1]['dag']['nodes']) > len(results[0]['dag']['nodes'])
    assert results[0]['basis']['digest'] != results[1]['basis']['digest']
    assert 'type' not in {n['op_kind'] for r in results for n in r['dag']['nodes']}


def test_construction_write_is_rejected_without_side_effect(original):
    (original / 'dispatch.py').write_text('''from pathlib import Path
from demiflow import data
def config(): return {}
def run_pipeline():
    Path('should-not-exist').write_text('bad')
    return data.from_items([1]).take_all()
''')
    register_fixture_flow(original)
    before = business_bytes(original)
    with pytest.raises(ForgeError, match='解析阶段'):
        preview(original, entry='dispatch.py:run_pipeline')
    assert business_bytes(original) == before


def test_http_node_details_keep_exact_compiled_source_and_require_matching_identity(original):
    operator = original / 'operators/clean.py'
    operator.write_text('def clean(row):\n    """Original frozen material."""\n    return row\n')
    (original / 'dispatch.py').write_text('from demiflow import data\n'
        'from operators.clean import clean\n'
        'def config(): return {}\n'
        'def run_pipeline(): return data.from_items([{\"id\":1}]).map(clean).take_all()\n')
    register_fixture_flow(original)
    with service(original) as app:
        code, result = request(app, '/api/plan-preview', dict(entry='dispatch.py:run_pipeline', params=None))
        assert code == 200, result
        assert '_node_details' not in result
        node = next(n for n in result['dag']['nodes'] if (n.get('program') or {}).get('qualname') == 'clean')
        assert node['source_views']['implementation']['available']
        basis = {k: result['basis'][k] for k in ('entry', 'params', 'digest', 'dag_id')}
        address = dict(basis=dict(basis, kind='preview'), logical_node_id=node['logical_node_id'],
                       occurrence_id=node['occurrence_id'], kind='implementation')
        code, detail = request(app, '/api/plan-node-detail', address)
        assert code == 200, detail
        assert detail['format'] == 'demiflow.plan_node_detail.v1'
        assert detail['basis'] == address['basis']
        assert detail['source_basis'] == 'compiled_snapshot'
        assert 'Original frozen material' in detail['text']
        operator.write_text('def clean(row):\n    return {\"changed\": True}\n')
        code, still_frozen = request(app, '/api/plan-node-detail', address)
        assert code == 200 and still_frozen == detail
        code, rejected = request(app, '/api/plan-node-detail', dict(address, logical_node_id='unrelated'))
        assert code == 409 and rejected['code'] == 'conflict'
        code, invalid = request(app, '/api/plan-node-detail', dict(address, kind={}))
        assert code == 400 and invalid['code'] == 'invalid_request'
        code, newer = request(app, '/api/plan-preview', dict(entry='dispatch.py:run_pipeline', params=None))
        assert code == 200 and newer['basis']['digest'] != result['basis']['digest']


@pytest.mark.parametrize('required', [False, True])
def test_http_accepts_null_params_and_explains_required_arguments(original, required):
    signature = '*, stage, target' if required else ''
    (original / 'dispatch.py').write_text('from demiflow import data\n'
        'def config(): return {}\n'
        f'def run_pipeline({signature}):\n    return data.from_items([1]).take_all()\n')
    register_fixture_flow(original)
    before = business_bytes(original)
    with service(original) as app:
        code, result = request(app, '/api/plan-preview', dict(entry='dispatch.py:run_pipeline', params=None))
        if required:
            assert code == 400 and result['code'] == 'invalid_request'
            assert '请选择配置' in result['error'] and 'stage' in result['error'] and 'target' in result['error']
        else:
            assert code == 200 and result['available'] and result['kind'] == 'take_all'
            assert result['basis']['params'] is None
        # Null belongs only to this endpoint; malformed path values remain invalid.
        code, result = request(app, '/api/plan-preview', dict(entry='dispatch.py:run_pipeline', params=True))
        assert code == 400 and result['code'] == 'invalid_request'
    assert business_bytes(original) == before


def test_guard_denies_mutation_network_and_processes_in_real_child(tmp_path):
    kept = tmp_path / 'keep'
    kept.write_text('unchanged')
    script = '''import os, pathlib, socket, subprocess, sys, threading
from demiflow.forge.readonly_guard import enter_readonly
p=pathlib.Path(sys.argv[1])
ready=threading.Event()
blocked=[]
def existing_thread():
 ready.wait()
 for attempt in (lambda:p.write_text('bad thread'),lambda:socket.socket()):
  try: attempt()
  except PermissionError: blocked.append(True)
  else: blocked.append(False)
thread=threading.Thread(target=existing_thread)
thread.start()
enter_readonly()
ready.set()
thread.join(timeout=5)
assert not thread.is_alive() and blocked==[True,True]
assert p.read_text()=='unchanged'
attempts=[lambda:p.write_text('bad'),lambda:p.unlink(),lambda:p.rename(p.with_name('moved')),
 lambda:p.chmod(0o777),lambda:p.with_name('new').mkdir(),lambda:socket.socket(),
 lambda:subprocess.run([sys.executable,'-c','print(1)'],check=True)]
for attempt in attempts:
 try: attempt()
 except OSError: pass
 else: raise AssertionError('mutation admitted')
assert p.read_text()=='unchanged'
print('guard verified')
'''
    result = subprocess.run([sys.executable, '-B', '-c', script, str(kept)],
        env={**os.environ, 'PYTHONDONTWRITEBYTECODE':'1'}, text=True, capture_output=True, timeout=20)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == 'guard verified'
    assert kept.read_text() == 'unchanged' and sorted(p.name for p in tmp_path.iterdir()) == ['keep']
