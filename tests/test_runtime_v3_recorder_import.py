"""Evidence capture must bind the first API import and release every alias."""
import os
import subprocess
import sys
from pathlib import Path


def test_first_import_and_second_recorder_observe_their_own_actions(tmp_path):
    script = tmp_path / 'first_import.py'
    script.write_text('''
from pathlib import Path
import sys
from runtime_v3_support import _ExecuteActionRecorder
from demiflow.execution.run_context import RunContext

if __name__ == '__main__':
    assert 'demiflow.execution.dataflow_api' not in sys.modules
    from demiflow import data
    seen = []
    for index in range(2):
        with _ExecuteActionRecorder() as recorder:
            with RunContext.start(Path(sys.argv[1]) / str(index)), data.local_execution():
                assert data.from_items([{'n': index}]).count() == 1
        import demiflow.execution.action_driver.dataflow as core
        import demiflow.execution.dataflow_api as api
        assert core.execute_action is api.execute_action
        seen.append(recorder)
    assert [len(item.calls) for item in seen] == [1, 1]
''')
    env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1]) + os.pathsep +
               str(Path(__file__).resolve().parent))
    result = subprocess.run([sys.executable, '-B', str(script), str(tmp_path / 'runs')],
                            capture_output=True, text=True, env=env, timeout=90)
    assert result.returncode == 0, result.stdout + result.stderr
