"""Original source -> native compiler, without starting the business action."""
import json
import os
import subprocess
import sys

import pytest

from test_forge_project_discovery import original, snapshot, cached_snapshot, business_bytes
from demiflow.workbench.graph.preview import preview
from demiflow.workbench.projects import workspace as ws
from demiflow.forge.schema import ForgeError
from test_forge_public_api import service, request


def test_original_helpers_and_parameter_branches_compile_without_execution(original):
    (original / 'operators/clean.py').write_text('def clean(row):\n    raise AssertionError("row executed")\n')
    (original / 'dispatch.py').write_text('''from demiflow import data
from shipping.operators.clean import clean
def config(): return {}
def source(): return data.from_items([{'id': 1}])
def run_pipeline(*, combine=False):
    ds = source().map(clean)
    if combine: ds = ds.union(source().filter(clean))
    return ds.take_all()
''')
    # The public project declaration fixes the repository import root. Its
    # package namespace is independent of the pipeline's registration name.
    path = original / 'settings/compile.json'
    ws.init_workspace(original, existing=True)
    results = []
    for combine in (False, True):
        path.write_text(json.dumps({'run_directory': '/unused', 'arguments': {'combine': combine}}))
        before = business_bytes(original)
        result = preview(original, entry='dispatch.py:run_pipeline', params='settings/compile.json')
        assert business_bytes(original) == before
        assert result['scope'] == 'first_action' and result['entry_completed'] is False
        assert result['kind'] == 'take_all' and 'physical' not in result and 'run_id' not in result
        assert 'shipping/operators/clean.py' in result['basis']['sources']
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
    ws.init_workspace(original, existing=True)
    before = business_bytes(original)
    with pytest.raises(ForgeError, match='解析阶段'):
        preview(original, entry='dispatch.py:run_pipeline')
    assert business_bytes(original) == before


@pytest.mark.parametrize('required', [False, True])
def test_http_accepts_null_params_and_explains_required_arguments(original, required):
    signature = '*, stage, target' if required else ''
    (original / 'dispatch.py').write_text('from demiflow import data\n'
        'def config(): return {}\n'
        f'def run_pipeline({signature}):\n    return data.from_items([1]).take_all()\n')
    ws.init_workspace(original, existing=True)
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
from demiflow.workbench.graph.preview_guard import enter_readonly
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
