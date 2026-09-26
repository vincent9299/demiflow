"""Reusable notebook execution and fingerprints stay independent of applications."""
import json
from pathlib import Path

import pytest

from demiflow.execution.artifacts import code_record, digest
from demiflow.execution.notebook import load_pipeline, pipeline_cells, graph_version
from demiflow.execution import processes


def test_only_explicit_graph_cells_execute(tmp_path):
    notebook = tmp_path / 'debug.ipynb'
    cells = [
        {'cell_type': 'code', 'metadata': {}, 'source': ['raise AssertionError("interactive cell executed")']},
        {'cell_type': 'code', 'metadata': {'tags': ['pipeline']},
         'source': ['def run_pipeline():\n', '    return __name__, 7\n']},
        {'cell_type': 'code', 'metadata': {'tags': ['visual-graph']}, 'source': ['visual = 9']},
    ]
    notebook.write_text(json.dumps({'cells': cells}))
    fn = load_pipeline(notebook, 'example.pipeline')
    assert fn() == ('example.pipeline', 7)
    assert pipeline_cells(notebook, tag='visual-graph') == ['visual = 9']
    version = graph_version(notebook)
    cells[0]['source'] = ['arbitrary_interactive_work()']
    cells[1]['outputs'] = [{'text': 'a saved output'}]
    notebook.write_text(json.dumps({'cells': cells}))
    assert graph_version(notebook) == version
    with pytest.raises(ValueError, match='pipeline-tagged'):
        load_pipeline(notebook, 'example.pipeline', name='missing')


def test_code_fingerprint_ignores_source_location_but_includes_nested_logic():
    left = compile('def f():\n    return lambda: 1\n', '/original.py', 'exec')
    moved = compile('def f():\n    return lambda: 1\n', '/moved.py', 'exec')
    changed = compile('def f():\n    return lambda: 2\n', '/moved.py', 'exec')
    assert digest(code_record(left)) == digest(code_record(moved))
    assert digest(code_record(left)) != digest(code_record(changed))


def test_spawn_uses_explicit_working_directory_without_changing_parent_env(tmp_path, monkeypatch):
    captured = {}
    monkeypatch.setenv('PATH', '/original/bin')
    def popen(command, **options):
        captured.update(command=command, **options)
        return 'owned-process'
    monkeypatch.setattr(processes.subprocess, 'Popen', popen)
    assert processes.spawn(['example'], tmp_path / 'service.log', cwd=tmp_path) == 'owned-process'
    assert captured['cwd'] == tmp_path and captured['start_new_session'] is True
    assert captured['env']['PATH'].endswith('/original/bin')
    assert processes.os.environ['PATH'] == '/original/bin'
