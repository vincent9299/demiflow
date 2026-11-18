"""Explicit script policy admits a wide graph without bypassing its ceilings."""
import pytest

from demiflow import data
from demiflow.execution import RunContext


def test_default_and_explicit_worker_ceilings_still_reject(tmp_path):
    for planning in (None, {'limits': {'max_parallelism': 32}}):
        with pytest.raises(ValueError, match='max_parallelism'):
            with data.local_execution(workers=33, planning=planning):
                pytest.fail('invalid worker budget must reject before entering')
    with pytest.raises(ValueError, match='positive integer'):
        with data.local_execution(planning={'limits': {'max_parallelism': 0}}):
            pytest.fail('zero is not unlimited')


def test_explicit_policy_runs_more_than_32_memory_connected_stages(tmp_path):
    limits = {'memory_bytes': 8 * 1024**3, 'max_state_bytes': 16 * 1024**2}
    with RunContext.start(tmp_path / 'run', limits=limits), data.local_execution(
        workers=64, worker_mode='thread', partitions=1, memory_bytes=8 * 1024**3,
        planning={'limits': {'max_parallelism': 64}},
    ):
        rows = data.from_items([{'value': 1}])
        # Native filters are distinct kernels; unlike plain maps they cannot
        # collapse into a single Python program and conceal an unwired policy.
        for _ in range(34):
            rows = rows.filter(lambda row: row['value'] > 0)
        assert rows.take_all() == [{'value': 1}]
