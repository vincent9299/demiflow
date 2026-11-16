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
