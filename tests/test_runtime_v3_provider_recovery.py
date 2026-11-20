"""Sealed recovery keeps its provider; confirmed results remain backend independent."""
from contextlib import contextmanager
import json

import pytest

from demiflow import data
from demiflow.data.api import _use_executor
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.action_driver.dataflow import DataflowExecution
from demiflow.execution.recovery_contract import RecoveryContractError
from demiflow.execution.run_context import RunContext
from demiflow.execution.state_artifacts import StateArtifacts


LIMITS = {'memory_bytes': 80 * 1024, 'max_batch_bytes': 16 * 1024,
          'max_row_bytes': 8 * 1024, 'max_state_bytes': 128 * 1024,
          'checkpoint_interval_ms': 60000, 'timeout_s': 30}
EXPECTED = [{'n': n} for n in range(1, 5)]


class Interrupted(Exception):
    pass


def dataset():
    return data.from_items([{'n': n} for n in (4, 1, 3, 2)]).sort('n').sort('n')


@contextmanager
def executor(provider):
    from demiflow.execution.executors.local import LocalDatasetExecutor
    from demiflow.execution.executors.ray import RayDatasetExecutor
    instance = LocalDatasetExecutor() if provider == 'local' else RayDatasetExecutor()
    try:
        with _use_executor(instance):
            yield
    finally:
        instance.close()


@contextmanager
def private_ray():
    import ray
    from runtime_v3_recovery_scenarios import _provider
    from runtime_v3_support import _RayDataBlocker
    assert not ray.is_initialized(), 'the test must own its private cluster'
    with _RayDataBlocker() as blocked:
        with _provider('ray_core'):
            yield
        assert not ray.is_initialized()
        assert not blocked.attempts


def facts(run):
    commits = run.journal.execution_commits('action-1')
    seal = run.journal.source_binding('action-1-dataflow-sealed')
    image = StateArtifacts(run).read(commits[-1]['prepared']['state']['dataflow-cut-v3'])
    return commits, seal, image


@pytest.mark.parametrize('origin', ['local', 'ray_core'])
def test_nonfinal_provider_change_rejected_but_original_provider_can_resume(
        tmp_path, monkeypatch, record_property, origin):
    alternate = 'ray_core' if origin == 'local' else 'local'
    cut_ids = []
    def stop_after_real_ack(execution, image):
        assert not image.cut.final
        assert execution._action.context.journal.execution_commit(image.commit_id)['status'] == 'acked'
        cut_ids.append(image.commit_id)
        raise Interrupted()
    with private_ray():
        with pytest.raises(Interrupted):
            with executor(origin), RunContext.start(tmp_path/'run', limits=LIMITS) as run:
                ref = run.ref
                with monkeypatch.context() as patch:
                    patch.setattr(DataflowExecution, '_retire_phase', stop_after_real_ack)
                    dataset().take_all()
        assert len(cut_ids) == 1
        launches = []
        def forbidden_launch(execution):
            launches.append(tuple(host.provider for host in execution.services.hosts))
            raise AssertionError('a changed provider reached worker launch for the sealed plan')
        with pytest.raises(Interrupted), executor(alternate), RunContext.resume(ref) as run:
            before, seal, image = facts(run)
            assert before[-1]['commit_id'] == cut_ids[0]
            assert not image['cut']['final']
            with monkeypatch.context() as patch:
                patch.setattr(DataflowExecution, '_launch_phase', forbidden_launch)
                with pytest.raises(DataflowError) as caught:
                    dataset().take_all()
            assert caught.value.code == 'INCOMPATIBLE_HISTORY'
            assert 'provider' in str(caught.value).lower()
            assert not launches
            assert run.journal.execution_commits('action-1') == before
            assert run.journal.source_binding('action-1-dataflow-sealed') == seal
            # The rejection is inspected inside this attempt, but the enclosing
            # run must remain interrupted rather than be marked successful.
            raise Interrupted()
        with executor(origin), RunContext.resume(ref) as resumed:
            assert dataset().take_all() == EXPECTED
            after, same_seal, final = facts(resumed)
            assert after[:len(before)] == before
            assert same_seal == seal
            assert final['cut']['final']
        record_property('provider_recovery', json.dumps({
            'original_provider': origin, 'rejected_provider': alternate,
            'original_ack': cut_ids[0], 'resumed_final_ack': after[-1]['commit_id'],
            'changed_provider_worker_launches': launches, 'original_seal_unchanged': same_seal == seal}))


@pytest.mark.parametrize('damage_final_proof', [False, True])
def test_ray_completed_result_reads_without_ray_but_requires_original_ack_proof(
        tmp_path, monkeypatch, record_property, damage_final_proof):
    import ray
    with private_ray(), pytest.raises(Interrupted):
        with RunContext.start(tmp_path/'run', limits=LIMITS) as run:
            ref = run.ref
            assert dataset().take_all() == EXPECTED
            before, seal, image = facts(run)
            assert image['cut']['final'] and before[-1]['status'] == 'acked'
            raise Interrupted()
    assert not ray.is_initialized()
    if damage_final_proof:
        reference = before[-1]['prepared']['state']['dataflow-cut-v3']
        path = tmp_path/'run'/reference['path']
        raw = path.read_bytes()
        # Keep the file length and valid JSON; its original journal digest must
        # still reject it, even though the forged document continues to say final.
        changed = raw.replace(b'demiflow.dataflow_cut.v3', b'demiflow.dataflow_cut.v4', 1)
        assert changed != raw and len(changed) == len(raw)
        assert json.loads(changed)['cut']['final']
        path.write_bytes(changed)
    launches = []
    def forbidden(*args, **kwargs):
        launches.append(True)
        raise AssertionError('history reading must not initialize Ray or launch a worker')
    monkeypatch.setattr(ray, 'init', forbidden)
    monkeypatch.setattr(DataflowExecution, '_launch_phase', forbidden)
    with pytest.raises(Interrupted), executor('local'), RunContext.resume(ref) as resumed:
        if damage_final_proof:
            with pytest.raises(RecoveryContractError, match='checksum'):
                dataset().take_all()
        else:
            assert dataset().take_all() == EXPECTED
        assert resumed.journal.execution_commits('action-1') == before
        assert resumed.journal.source_binding('action-1-dataflow-sealed') == seal
        raise Interrupted()
    assert not launches and not ray.is_initialized()
    record_property('provider_history', json.dumps({
        'original_provider': 'ray_core', 'read_provider': 'local',
        'final_ack': before[-1]['commit_id'], 'damaged_proof_rejected': damage_final_proof,
        'ray_initialized_after_read': ray.is_initialized(), 'worker_launches': launches}))
