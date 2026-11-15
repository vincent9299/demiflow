"""Independent finite producers share real admitted capacity, including resume."""
import pytest
from demiflow import data
from demiflow.execution.dataflow import DataflowExecution
from demiflow.execution.run_context import RunContext
from demiflow.execution.state_artifacts import StateArtifacts


def flow():
    return data.from_items([{'k': i, 'left': i} for i in range(8)]).join(
        data.from_items([{'k': i, 'right': i + 10} for i in range(8)]), on='k').map(lambda row: row)


@pytest.mark.parametrize('boundary', ['none', 'partial', 'complete'])
def test_independent_sources_pack_without_restarting_original_completed_work(tmp_path, monkeypatch, boundary):
    class Interrupted(Exception):
        pass
    launches = []
    launch, pump, retire = DataflowExecution._launch_phase, DataflowExecution.pump, DataflowExecution._retire_phase
    def observe(ex):
        phase = ex._phases[ex._phase_index]
        stages = [s for s in ex._physical.stages if s.stage_id in phase]
        assert sum(max(s.resident_bytes, s.profile.requests['memory_bytes']) * len(s.partitions)
                   for s in stages) + ex._physical.residency.control_reserved_bytes <= ex._bounds.max_resident_bytes
        launches.append(tuple(phase))
        return launch(ex)
    def interrupt_partial(ex, **kwargs):
        result = pump(ex, **kwargs)
        if ex._checkpoint.residency and not ex._checkpoint.residency['phase_complete']:
            raise Interrupted()
        return result
    def interrupt_complete(ex, image):
        raise Interrupted()
    limits = {'memory_bytes': 256 * 2**20, 'checkpoint_rows': 2,
              'checkpoint_interval_ms': 60000, 'timeout_s': 45}
    expected = [{'k': i, 'left': i, 'right': i + 10} for i in range(8)]
    with monkeypatch.context() as patch:
        patch.setattr(DataflowExecution, '_launch_phase', observe)
        if boundary != 'none':
            patch.setattr(DataflowExecution, 'pump' if boundary == 'partial' else '_retire_phase',
                          interrupt_partial if boundary == 'partial' else interrupt_complete)
        if boundary == 'none':
            with RunContext.start(tmp_path/'run', limits=limits):
                assert sorted(flow().take_all(), key=lambda r: r['k']) == expected
        else:
            with pytest.raises(Interrupted), RunContext.start(tmp_path/'run', limits=limits) as run:
                reference = run.ref
                flow().take_all()
    first = launches[0]
    assert len(first) == 2  # Original scheduler unnecessarily serialized these two sources.
    if boundary == 'none':
        return
    launches.clear()
    with RunContext.resume(reference) as run, monkeypatch.context() as patch:
        old = run.journal.execution_commits('action-1')
        saved = StateArtifacts(run).read(old[-1]['prepared']['state']['dataflow-cut-v3'])
        assert set(saved['residency']['active_stages']) == set(first)
        patch.setattr(DataflowExecution, '_launch_phase', observe)
        assert sorted(flow().take_all(), key=lambda r: r['k']) == expected
        assert run.journal.execution_commits('action-1')[:len(old)] == old
    if boundary == 'partial':
        assert set(launches[0]) == set(first)
    else:
        assert not set(first) & {stage for phase in launches for stage in phase}
