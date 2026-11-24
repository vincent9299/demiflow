"""A terminal reserves index metadata alongside the same payload grant."""
from types import SimpleNamespace
import pytest
from runtime_v3_resource_scenarios import arrow_worker_preallocation, observe_execution


def test_small_legal_arrow_batch_reaches_terminal_under_original_budget(tmp_path):
    with observe_execution(workers=4, memory_bytes=4 * 1024**2) as observed:
        arrow_worker_preallocation(tmp_path, rejected=False)
    terminal = next(n for n in observed['plans'][0]['graph']['nodes'] if n['kind'] == 'terminal')
    window = terminal['kernel']['configuration']['terminal_memory']
    # New plans include the measured codec workspace, while preserving the
    # original index capacity and unchanged 4-MiB total admission below.
    assert window == {'payload_bytes': 128 * 1024, 'metadata_bytes': 3072}
    assert terminal['kernel']['profile']['requests']['memory_bytes'] == 128 * 1024 + 3072
    assert observed['launches'] and observed['charged_peak']['memory_bytes'] <= 4 * 1024**2


def test_insufficient_terminal_state_fails_before_any_launch(tmp_path):
    from demiflow import data
    from demiflow.execution.pipeline.run_context import RunContext
    with observe_execution(workers=4, memory_bytes=4 * 1024**2) as observed:
        with pytest.raises(Exception, match='terminal state budget cannot hold one block and root'):
            with RunContext.start(tmp_path/'run', limits=dict(memory_bytes=4*1024**2,
                    max_row_bytes=1024, max_batch_bytes=4096, max_state_bytes=2048)), data.local_execution(workers=4):
                data.from_items([{'v': 1}]).count()
    assert not observed['launches']


def test_sealed_window_requires_original_grant_and_legacy_keeps_old_window():
    from demiflow.execution.outputs.terminal_blocks import TerminalBlocks
    bounds = SimpleNamespace(max_state_bytes=8192, max_resident_bytes=6144,
                             max_batch_bytes=4096, max_row_bytes=1024)
    services = SimpleNamespace(bounds=bounds, assignment=SimpleNamespace(stages=()))
    legacy = TerminalBlocks(services, 'old')
    assert legacy._metadata_limit == 3072
    with pytest.raises(Exception, match='terminal payload and metadata exceed the original grant'):
        TerminalBlocks(services, 'underfunded', budget={'payload_bytes':5120,'metadata_bytes':3072})
    bounds.max_resident_bytes = 8192
    current = TerminalBlocks(services, 'new', budget={'payload_bytes':5120,'metadata_bytes':3072})
    current._admit_metadata(blocks=1, roots=1)
    with pytest.raises(Exception, match='terminal block index exceeds'):
        current._admit_metadata(blocks=1, roots=2)
