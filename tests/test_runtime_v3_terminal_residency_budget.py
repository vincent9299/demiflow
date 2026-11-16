"""Optional terminal index space must fit beside the original live payload."""
import pytest

from demiflow import data
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.run_context import RunContext
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


def test_terminal_minimum_index_cannot_borrow_payload_or_control(tmp_path):
    limits = {**LIMITS, 'memory_bytes': 30 * 1024}
    # After the unchanged 3,840-byte control and 24-KiB payload windows,
    # only 2,304 bytes remain: less than the real first block+root charge.
    with observe_execution(workers=4, memory_bytes=limits['memory_bytes']) as observed:
        with pytest.raises(DataflowError, match='terminal.*one block and root'):
            with RunContext.start(tmp_path / 'run', limits=limits) as run, data.local_execution(workers=4):
                data.from_items([{'n': 1}]).take_all()
        assert run.journal.execution_commits('action-1') == []
        assert not observed['launches']
