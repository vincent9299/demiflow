"""Native publication readers remain read-only and do not import their hosts."""
from pathlib import Path
from types import SimpleNamespace
import pytest
from demiflow.execution.journal import publication_facts as facts
from demiflow.execution.storage.object_resources import object_namespace
from demiflow.execution.contracts.recovery_contract import RecoveryContractError


def test_observation_context_is_the_shape_every_copy_built(tmp_path):
    binding = {'run_id': 'a' * 32, 'limits': {'batch_rows': 7}}
    journal = SimpleNamespace(directory=tmp_path, recovery_identity=lambda: binding,
                              execution_status=lambda: {'attempt': 3})
    context = facts.observation_context(journal)
    assert vars(context) == dict(directory=tmp_path, limits=binding['limits'], journal=journal,
                                 ref=context.ref, attempt=3, fence='observation-only')
    assert vars(context.ref) == {'run_id': binding['run_id']}
    other = tmp_path / 'other'
    assert facts.observation_context(journal, directory=other).directory == other
    journal.recovery_identity = lambda: None
    assert facts.observation_context(journal) is None
    assert facts.observation_action(journal, 'action-1', {'kind': 'write_table'}) is None


def test_observation_action_cannot_advance_a_real_journal(tmp_path):
    from demiflow.execution.pipeline.run_context import RunContext
    with RunContext.start(tmp_path / 'run') as run:
        journal, native_fence = run.journal, run.fence
    action = facts.observation_action(journal, 'action-1', {'kind': 'write_table'})
    assert action.context.fence == 'observation-only' != native_fence
    assert action.context.ref.run_id == journal.recovery_identity()['run_id']
    with pytest.raises(Exception):
        action.complete({'status': 'forged'})
    assert journal.execution_action('action-1') is None


def test_object_namespace_matches_the_host_fabricated_context(tmp_path):
    from demiflow.execution.storage.object_resources import namespace_record
    run_id = 'b' * 32
    for logical in (str(tmp_path / 'objects'), (tmp_path / 'objects').as_uri()):
        fabricated = SimpleNamespace(ref=SimpleNamespace(run_id=run_id))
        assert object_namespace(run_id, logical) == namespace_record(fabricated, logical)
    with pytest.raises(RecoveryContractError):
        object_namespace(run_id, 's3://bucket/objects')


def test_engine_module_does_not_load_hosts():
    import subprocess
    import sys
    root_dir = Path(__file__).resolve().parents[1]
    result = subprocess.run([sys.executable, '-I', '-B', '-c',
        'import sys;sys.path.insert(0,sys.argv[1]);import demiflow.execution.journal.publication_facts;'
        'assert not any(n.startswith(("demiflow.workbench","demiflow.forge")) for n in sys.modules)',
        str(root_dir)], capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stderr

