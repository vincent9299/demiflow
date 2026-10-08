"""Lock observations never create runs, disturb writers or imply completion."""
import subprocess
import sys

from demiflow.execution.artifacts import run_is_active, run_lock


def test_activity_without_side_effects_and_same_process_holder(tmp_path):
    run = tmp_path / 'run'
    assert not run_is_active(run)
    assert not run.exists()
    with run_lock(run):
        assert run_is_active(run)
        assert run_is_active(run)
    assert not run_is_active(run)


def test_observe_external_writer_and_release(tmp_path):
    script = '''import sys
from demiflow.execution.artifacts import run_lock
with run_lock(sys.argv[1]):
    print('locked', flush=True)
    sys.stdin.readline()
'''
    run = tmp_path / 'run'
    child = subprocess.Popen([sys.executable, '-c', script, str(run)],
                             stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    try:
        assert child.stdout.readline().strip() == 'locked'
        assert run_is_active(run)
        child.communicate('\n', timeout=10)
        assert child.returncode == 0
        assert not run_is_active(run)
    finally:
        if child.poll() is None:
            child.kill()
            child.wait()
