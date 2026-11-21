from click.testing import CliRunner
from demiflow.cli import main


def test_native_run_and_remaining_forge_entries_keep_separate_boundaries():
    runner = CliRunner()
    root = runner.invoke(main, ['run', '--help'])
    assert root.exit_code == 0 and '--root' in root.output and '--backend' in root.output
    active = runner.invoke(main, ['forge', '--help'])
    assert active.exit_code == 0 and 'platform' in active.output
    for command, option in [('context', '--api'), ('init', '--existing'), ('add', 'operator'), ('check', '--entry')]:
        help_result = runner.invoke(main, ['forge', command, '--help'])
        assert help_result.exit_code == 0 and option in help_result.output
    old_init = runner.invoke(main, ['workbench', 'init', '--help'])
    assert old_init.exit_code == 2 and 'No such command' in old_init.output
    for retired in ('dev', 'new', 'verify', 'freeze', 'run', 'facts', 'tools'):
        result = runner.invoke(main, ['forge', retired, '--help'])
        assert result.exit_code == 2 and 'No such command' in result.output
