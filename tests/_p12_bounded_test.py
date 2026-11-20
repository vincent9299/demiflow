"""Run worker-private failure probes under the real preview allocation guard."""
import json
import os
from pathlib import Path
import subprocess
import sys


def bounded_test(function, **arguments):
    identity = function.__module__ + ':' + function.__name__
    if os.environ.get('P12_BOUNDED_CASE') == identity:
        return False
    paths = [key for key, value in arguments.items() if isinstance(value, Path)]
    values = {key: str(value) if key in paths else value for key, value in arguments.items()}
    code = '''import importlib,json,os,resource,sys
from pathlib import Path
ceiling=4*1024**3
resource.setrlimit(resource.RLIMIT_AS,(ceiling,ceiling))
import demiflow,pytest
from demiflow.workbench.worker import prepare_native
prepare_native(analysis_read=True)
assert Path(demiflow.__file__).resolve().is_relative_to(Path.cwd())
module,name=os.environ['P12_BOUNDED_CASE'].split(':')
function=getattr(importlib.import_module(module),name)
values=json.loads(sys.argv[1])
for key in json.loads(sys.argv[2]): values[key]=Path(values[key])
with pytest.MonkeyPatch.context() as patch:
    if 'monkeypatch' in __import__('inspect').signature(function).parameters: values['monkeypatch']=patch
    function(**values)
'''
    env = {**os.environ, 'P12_BOUNDED_CASE': identity, 'PYTHONDONTWRITEBYTECODE': '1',
           **{name: '1' for name in ('TOKIO_WORKER_THREADS', 'RAYON_NUM_THREADS', 'OMP_NUM_THREADS',
               'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS', 'NUMEXPR_NUM_THREADS', 'LANCE_CPU_THREADS')}}
    result = subprocess.run([sys.executable, '-P', '-B', '-c', code, json.dumps(values), json.dumps(paths)],
                            cwd=Path(__file__).resolve().parents[1], env=env,
                            capture_output=True, text=True, timeout=180)
    assert result.returncode == 0, result.stdout + result.stderr
    return True
