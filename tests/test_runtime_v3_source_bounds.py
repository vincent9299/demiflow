"""Source read authority borrows finite worker bounds, never optional defaults."""
from types import SimpleNamespace

import pytest

from demiflow.execution.dataflow_source_context import source_run_view


@pytest.mark.parametrize('native,expected', [
    ({}, {'memory_bytes': 8192, 'max_row_bytes': 1024, 'max_batch_bytes': 4096}),
    ({'memory_bytes': 4096, 'max_row_bytes': 512, 'max_batch_bytes': 2048},
     {'memory_bytes': 4096, 'max_row_bytes': 512, 'max_batch_bytes': 2048}),
    ({'memory_bytes': 16384, 'max_row_bytes': 2048, 'max_batch_bytes': 8192},
     {'memory_bytes': 8192, 'max_row_bytes': 1024, 'max_batch_bytes': 4096}),
])
def test_source_limits_intersect_original_worker_grant(tmp_path, native, expected):
    services = SimpleNamespace(
        store=SimpleNamespace(root=tmp_path / '.dataflow-store'),
        assignment=SimpleNamespace(run_id='source-run', attempt=1, fence='source-fence'),
        bounds=SimpleNamespace(max_resident_bytes=8192, max_row_bytes=1024, max_batch_bytes=4096),
        constants={'native_limits': dict(native)}, check_stop=lambda: None,
    )
    view = source_run_view(services)
    assert dict(view.limits) == expected
    assert services.constants['native_limits'] == native
    with pytest.raises(TypeError):
        view.limits['memory_bytes'] = 999999
    assert not hasattr(view, 'journal') and not hasattr(view, 'reserve')


def test_callback_view_uses_actual_stage_grant_not_whole_run_pool(tmp_path):
    services = SimpleNamespace(
        store=SimpleNamespace(root=tmp_path / '.dataflow-store'),
        assignment=SimpleNamespace(run_id='run', attempt=1, fence='fence', stages=(
            SimpleNamespace(resident_bytes=2048, profile=SimpleNamespace(requests={'memory_bytes':4096})),
            SimpleNamespace(resident_bytes=8192, profile=SimpleNamespace(requests={'memory_bytes':4096})),
        )),
        bounds=SimpleNamespace(max_resident_bytes=65536, max_row_bytes=1024, max_batch_bytes=4096),
        constants={}, check_stop=lambda: None,
    )
    original = {'memory_bytes': 65536, 'max_row_bytes': 2048, 'max_batch_bytes': 8192, 'max_requests': 7}
    view = source_run_view(services, limits=original)
    assert dict(view.limits) == dict(memory_bytes=12288, max_row_bytes=1024, max_batch_bytes=4096, max_requests=7)
    assert original['memory_bytes'] == 65536
