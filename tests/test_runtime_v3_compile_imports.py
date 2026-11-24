"""A plain CPU plan must not initialize unrelated native services."""
import os
from pathlib import Path
import subprocess
import sys


def test_cpu_compilation_does_not_import_model_or_lance_services(tmp_path):
    root = Path(__file__).resolve().parents[1]
    script = r'''
import importlib.abc
import pathlib
import sys
import demiflow
assert pathlib.Path(demiflow.__file__).resolve().is_relative_to(pathlib.Path.cwd())
blocked = {'demiflow.execution.native_queue.dataflow_stream_lance', 'demiflow.execution.native_queue.stream_lance',
           'demiflow.operator_llm.runtime', 'demiflow.lance.search'}
assert not blocked.intersection(sys.modules)
class Guard(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname in blocked:
            raise AssertionError('plain CPU compilation initialized ' + fullname)
sys.meta_path.insert(0, Guard())
from test_runtime_v3_default_suffix_fusion import compiled
from demiflow import data
graph, stages, policy, bounds = compiled(dataset=data.range(20).map(
    lambda r: {'id': r['id'], 'n': r['id'] + 1}).filter(lambda r: r['n'] % 2 == 0))
assert len(graph.nodes) == 4
assert len(stages) == 4
assert all(n.kernel.effect == 'pure' for n in graph.nodes)
assert not blocked.intersection(sys.modules)
'''
    result = subprocess.run([sys.executable, '-P', '-B', '-c', script], cwd=root,
        env={**os.environ, 'PYTHONPATH': str(root) + os.pathsep + str(root/'tests'),
             'DEMIFLOW_RESOURCE_ROOT': str(tmp_path/'capacity')},
        capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
