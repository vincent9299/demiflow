"""Optional terminal index space must fit beside the original live payload."""
import pytest

from demiflow import data
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.pipeline.run_context import RunContext
from runtime_v3_resource_scenarios import observe_execution
from test_runtime_v3_durable_residency import LIMITS


def test_terminal_metadata_uses_only_room_after_control_and_payload(tmp_path):
    class StopAfterResult(Exception):
        pass
    with observe_execution(workers=4, memory_bytes=LIMITS['memory_bytes']) as observed:
        with pytest.raises(StopAfterResult):
            with RunContext.start(tmp_path / 'run', limits=LIMITS) as run, data.local_execution(workers=4):
                ref = run.ref
                assert data.from_items([{'n': n} for n in range(8)]).take_all() == [
                    {'n': n} for n in range(8)]
                commits = run.journal.execution_commits('action-1')
                assert len(commits) > 2 and all(item['status'] == 'acked' for item in commits)
                # A run which fully succeeded cannot start another attempt.
                # Interrupt after this action's result while the run is open.
                raise StopAfterResult()
        sealed = observed['plans'][0]
        terminal = next(node for node in sealed['graph']['nodes'] if node['kind'] == 'terminal')
        window = terminal['kernel']['configuration']['terminal_memory']
        assert window == {'payload_bytes': 24 * 1024, 'metadata_bytes': 11 * 1024}
        assert sealed['physical']['residency']['control_reserved_bytes'] == 5 * 1024
        assert terminal['kernel']['profile']['requests']['memory_bytes'] == 35 * 1024
        assert observed['charged_peak']['memory_bytes'] == 35 * 1024
        launches = len(observed['launches'])
        with RunContext.resume(ref) as resumed, data.local_execution(workers=4):
            assert data.from_items([{'n': n} for n in range(8)]).take_all() == [
                {'n': n} for n in range(8)]
            assert resumed.journal.execution_commits('action-1') == commits
        assert len(observed['launches']) == launches
        assert observed['plans'][-1] == sealed


def test_terminal_accounts_live_index_and_payload_in_one_worker_grant(tmp_path, monkeypatch):
    from demiflow.execution.contracts.dataflow import TERMINAL_WORKSPACE
    from demiflow.execution.outputs.terminal_blocks import TerminalBlocks

    limits = {**LIMITS, 'memory_bytes': 30 * 1024}
    checked = []
    check = TerminalBlocks._check_workspace

    def observe(blocks, extra=0, *, metadata=None):
        result = check(blocks, extra, metadata=metadata)
        if blocks._workspace_limit is not None:
            index = blocks.retained_bytes if metadata is None else metadata
            checked.append((index, blocks._pending_resident, extra, blocks._workspace_limit))
        return result

    monkeypatch.setattr(TerminalBlocks, '_check_workspace', observe)
    with observe_execution(workers=4, memory_bytes=limits['memory_bytes']) as observed:
        with RunContext.start(tmp_path / 'run', limits=limits), data.local_execution(workers=4, worker_mode='thread'):
            assert data.from_items([{'n': 1}]).take_all() == [{'n': 1}]

    sealed = observed['plans'][0]
    terminal = next(node for node in sealed['graph']['nodes'] if node['kind'] == 'terminal')
    control = sealed['physical']['residency']['control_reserved_bytes']
    assert control == 3840
    grant = limits['memory_bytes'] - control
    assert terminal['kernel']['configuration']['terminal_memory'] == {'schema': TERMINAL_WORKSPACE, 'resident_bytes': grant}
    assert observed['charged_peak']['memory_bytes'] + control <= limits['memory_bytes']
    assert any(index and pending for index, pending, _, _ in checked)
    assert any(extra for _, _, extra, _ in checked)
    assert all(index + pending + extra <= limit == grant for index, pending, extra, limit in checked)


@pytest.mark.parametrize(('overrides', 'resource', 'required', 'available'), [
    ({'memory_bytes': 8192}, 'memory_bytes', 8192, 7168),
    ({'memory_bytes': 30 * 1024, 'max_state_bytes': 2304}, 'max_state_bytes', 3072, 2304),
])
def test_terminal_minimum_workspace_and_index_reject_before_launch(tmp_path, overrides, resource, required, available):
    limits = {**LIMITS, **overrides}
    with observe_execution(workers=4, memory_bytes=limits['memory_bytes']) as observed:
        with pytest.raises(DataflowError) as raised:
            with RunContext.start(tmp_path / 'run', limits=limits) as run, data.local_execution(workers=4):
                data.from_items([{'n': 1}]).take_all()
        assert raised.value.code == 'RESOURCE_INFEASIBLE'
        assert dict(raised.value.context) == {'resource': resource, 'required': required, 'available': available}
        assert run.journal.execution_commits('action-1') == []
        assert not observed['launches']


def test_terminal_small_grant_rejects_real_codec_peak(tmp_path):
    limits = {**LIMITS, 'memory_bytes': 30 * 1024}
    row = {'text': 'a' * 7000}
    with pytest.raises(DataflowError, match='workspace') as raised:
        with RunContext.start(tmp_path / 'run', limits=limits), data.local_execution(worker_mode='thread'):
            data.from_items([row]).take_all()
    assert raised.value.code == 'BUDGET_EXCEEDED'


def test_fixed_terminal_windows_cannot_borrow_from_each_other_or_control():
    from types import SimpleNamespace
    from demiflow.execution.outputs.terminal_blocks import TerminalBlocks

    available = 30 * 1024 - 3840
    bounds = SimpleNamespace(max_resident_bytes=available, max_batch_bytes=16 * 1024,
                             max_row_bytes=8 * 1024, max_state_bytes=128 * 1024)
    # Both variants of the historical sealed window must still fail: 2304
    # cannot fund the first block plus root, and 3072 exceeds the worker grant.
    for metadata in (2304, 3072):
        with pytest.raises(DataflowError, match='terminal payload and metadata exceed the original grant'):
            TerminalBlocks(SimpleNamespace(bounds=bounds), 0, budget={
                'payload_bytes': 24 * 1024, 'metadata_bytes': metadata})
