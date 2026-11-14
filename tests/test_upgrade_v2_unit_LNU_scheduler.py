"""LNU01-04: TaskScheduler borrows the sealed stage budget, partitions it across
the simultaneously-alive task width (the single authority shared with run()),
realizes the sealed CPU concurrency in __enter__, and releases the borrowed
shares and pending tasks on exit/error.
"""
import pytest

from demiflow.execution.local_kernel import LocalKernelOptions
from demiflow.execution.local_tasks import Block, TaskOutput, TaskScheduler

MiB = 2**20


def options(workers=2):
    return LocalKernelOptions(workers=workers, worker_mode='thread', partitions=4, batch_rows=8)


def stats():
    return {'peak_pending_tasks': 0, 'workers_used': [], 'stages': []}


def stage():
    return dict(name='s', submitted=0, completed=0, worker_seconds=0.0, rows_input=0, rows_output=0)


def ok_task(value):
    return TaskOutput(Block('/tmp/demiflow-unit-block', 1, 1), 1)


def boom_task(value):
    if value == 3:
        raise ValueError('deliberate task failure')
    return ok_task(value)


def test_lnu01_budget_partitioned_across_width():
    scheduler = TaskScheduler(options(workers=2), stats(),
                              budget={'inflight_bytes': 8 * MiB, 'output_bytes': 16 * MiB})
    assert scheduler._width == 4
    with scheduler:
        # Whole-stage ceilings divided by the live width, not handed to each task.
        assert scheduler.task_inflight_bytes == 8 * MiB // 4
        assert scheduler.task_output_bytes == 16 * MiB // 4


def test_lnu01_unbound_scheduler_carries_no_shares():
    with TaskScheduler(options(), stats()) as scheduler:
        assert scheduler.task_inflight_bytes is None
        assert scheduler.task_output_bytes is None


def test_lnu02_enter_realizes_sealed_cpu_concurrency():
    # options.workers already borrows the sealed CPU budget; the pool is sized by it.
    with TaskScheduler(options(workers=1), stats(), budget={'inflight_bytes': 4 * MiB}) as scheduler:
        assert scheduler._width == 2
        assert scheduler.pool._max_workers == 1
        assert scheduler.task_inflight_bytes == 4 * MiB // 2


def test_lnu03_run_uses_single_width_authority():
    scheduler = TaskScheduler(options(workers=2), stats())
    st = stats()
    scheduler.stats = st
    with scheduler:
        results = list(scheduler.run(ok_task, ((i,) for i in range(12)), stage()))
        assert len(results) == 12
        assert st['peak_pending_tasks'] <= scheduler._width


def test_lnu04_shares_released_on_exit():
    scheduler = TaskScheduler(options(), stats(), budget={'inflight_bytes': 8 * MiB, 'output_bytes': 8 * MiB})
    with scheduler:
        assert scheduler.task_inflight_bytes is not None
    assert scheduler.task_inflight_bytes is None
    assert scheduler.task_output_bytes is None


def test_lnu04_error_releases_pending_and_shares():
    scheduler = TaskScheduler(options(workers=2), stats(),
                              budget={'inflight_bytes': 8 * MiB, 'output_bytes': 8 * MiB})
    with pytest.raises(ValueError, match='deliberate task failure'):
        with scheduler:
            list(scheduler.run(boom_task, ((i,) for i in range(12)), stage()))
    # After the failing action the pool is shut down and no budget is retained.
    assert scheduler.task_inflight_bytes is None
    assert scheduler.task_output_bytes is None
    assert scheduler.pool._shutdown
    assert not scheduler.pending