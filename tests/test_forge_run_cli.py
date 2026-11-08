"""CLI admission and read views share the workspace run protocol."""
import json
from click.testing import CliRunner
from demiflow.cli import main
from demiflow.workbench.runs import manager as runs
from test_forge_runs import base, root


def test_run_cli_prepares_without_execution_and_preserves_legacy(root):
    runner = CliRunner()
    result = runner.invoke(main, ['workbench', 'run', 'prepare', str(root),
        'flows/questions.py:select', '--params', 'flows/select_a.json', '--json'])
    assert result.exit_code == 0, result.output
    prepared = json.loads(result.output)
    assert prepared['status'] == 'prepared'
    assert runs.show(root, prepared['run_id'])['digest'] == prepared['digest']
    result = runner.invoke(main, ['workbench', 'run', 'show', str(root), prepared['run_id'], '--json'])
    assert json.loads(result.output)['status'] == 'prepared'
    assert result.exit_code == 0
    result = runner.invoke(main, ['workbench', 'run', 'start', str(root), prepared['run_id'], '--expected', 'wrong'])
    assert result.exit_code == 1 and json.loads(result.output)['status'] == 'rejected'
    result = runner.invoke(main, ['forge', 'run', '--help'])
    assert result.exit_code == 0 and '--config' in result.output
    assert 'prepare' not in result.output
