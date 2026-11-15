from types import SimpleNamespace
import os
import pytest
from runtime_v3_recovery_scenarios import BOUNDARIES, commit_crash_matrix, driver_restart


@pytest.fixture
def recovery_fx(tmp_path):
    fx = SimpleNamespace(root=tmp_path, journal_dir=tmp_path/'journal', evidence_dir=tmp_path/'evidence',
                         provider=os.environ.get('DF_RECOVERY_TEST_PROVIDER', 'local'), bounds={'timeout_ms': 120000}, _children=[])
    fx.journal_dir.mkdir()
    fx.evidence_dir.mkdir()
    yield fx
    for child in fx._children:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=10)


@pytest.mark.parametrize('fault', list(BOUNDARIES))
def test_real_dataset_commit_six_kill_windows(recovery_fx, fault):
    result = commit_crash_matrix(recovery_fx, fault)
    assert result['crash']['returncode'] == -9
    assert len(result['recovered']['rows']) == 6


def test_real_driver_restart_rejects_actual_old_worker_event(recovery_fx):
    result = driver_restart(recovery_fx, None)
    assert result['recovered']['late_events'][0]['error_code'] == 'FENCED'


@pytest.mark.parametrize('window', ['before_send', 'after_send', 'after_confirm'])
def test_native_three_model_families_recover_without_duplicate_http(recovery_fx, window):
    from runtime_v3_recovery_scenarios import model_effect_recovery
    result = model_effect_recovery(recovery_fx, window)
    assert len(result['cases']) == 3
    assert all(c['resumed']['native']['requests_consumed'] == 1 for c in result['cases'])


def test_real_worker_remote_journal_guards_and_lost_reply_idempotency(recovery_fx):
    from runtime_v3_recovery_scenarios import remote_journal
    result = remote_journal(recovery_fx, None)
    assert result['worker_sqlite'] == 'forbidden'
