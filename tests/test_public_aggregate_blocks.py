"""Physical public aggregate calls, ordered fallback, and bounded failure."""
from collections import Counter
from contextlib import contextmanager
import sys
import threading

import pytest

from demiflow import data
from demiflow.data.aggregate import AggregateFnV2, Count, Sum
from demiflow.execution.pipeline.run_context import RunContext


@contextmanager
def observed_blocks():
    sizes = Counter()
    merges = Counter()
    def watch(frame, event, arg):
        if event == 'call' and frame.f_globals.get('__name__') == 'demiflow.data.aggregate':
            if frame.f_code.co_name == 'aggregate_partial':
                sizes[len(frame.f_locals['block'])] += 1
            elif frame.f_code.co_name == 'merge_states':
                merges[type(frame.f_locals['self']).__name__] += 1
    original, other = sys.getprofile(), threading.getprofile()
    sys.setprofile(watch)
    threading.setprofile(watch)
    try:
        yield sizes, merges
    finally:
        threading.setprofile(other)
        sys.setprofile(original)


def test_integer_aggregates_follow_real_row_and_byte_limits(tmp_path):
    limits = {'max_row_bytes': 4096, 'max_batch_bytes': 4096, 'batch_rows': 7,
              'memory_bytes': 32 * 2**20}
    with RunContext.start(tmp_path/'run', limits=limits), data.local_execution(
            workers=1, worker_mode='thread', temp_directory=str(tmp_path)):
        with observed_blocks() as (sizes, merges):
            result = data.from_items([{'n': i} for i in range(27)]).aggregate(Count(), Sum('n'))
        assert result == {'count()': 27, 'sum(n)': 351}
        assert sizes == {7: 6, 6: 2} and merges == {'Count': 4, 'Sum': 4}
        # Each row has > 1300 bytes of base64 on the wire. Two fit, three
        # exceed 4096 with the row/column tags. The seven-row limit still holds.
        with observed_blocks() as (sizes, merges):
            result = data.from_items([{'n': i, 'v': b'x'*1080} for i in range(10)]).aggregate(Count(), Sum('n'))
        assert result == {'count()': 10, 'sum(n)': 45}
        assert sizes == {2: 10} and merges == {'Count': 5, 'Sum': 5}


def test_public_512_integer_rows_share_native_partials(tmp_path):
    with RunContext.start(tmp_path/'run', limits={'batch_rows': 32}), data.local_execution(
            workers=1, worker_mode='thread', temp_directory=str(tmp_path)):
        with observed_blocks() as (sizes, merges):
            result = data.from_items([{'n': i} for i in range(512)]).aggregate(Count(), Sum('n'))
        assert result == {'count()': 512, 'sum(n)': 130816}
        assert sizes == {32: 32}
        assert merges == {'Count': 16, 'Sum': 16}


class Ordered(AggregateFnV2):
    def __init__(self):
        super().__init__('trace', tuple, on='n', ignore_nulls=False)

    def aggregate_block(self, block):
        assert type(block) is tuple and len(block) == 1 and type(block[0]) is dict
        return (block[0]['n'],)

    def combine(self, old, new):
        return old + new


def test_ordered_user_aggregate_mixed_floats_and_nulls(tmp_path):
    with RunContext.start(tmp_path/'run'), data.local_execution(
            workers=1, worker_mode='thread', temp_directory=str(tmp_path)):
        ns = [1.0e16, 1, 1, -1.0e16, None, 3]
        with observed_blocks() as (sizes, merges):
            value = data.from_items([{'n': n} for n in ns]).aggregate(Sum('n'))
        assert value == {'sum(n)': 3.0}
        assert sizes == {1: 6} and merges == {'Sum': 6}
        rows = [{'n': i} for i in (3, None, 7, 2)]
        with observed_blocks() as (sizes, merges):
            value = data.from_items(rows).aggregate(Ordered(), Count('n', ignore_nulls=True))
        assert value == {'trace': (3, None, 7, 2), 'count(n)': 3}
        assert sizes == {1: 8} and merges == {'Ordered': 4, 'Count': 4}
        assert data.from_items(rows).aggregate(Sum('n', ignore_nulls=False), Count()) == {
            'sum(n)': None, 'count()': 4}


def singleton_partial(block):
    assert type(block) is tuple and len(block) == 1
    return block[0]['n']


class SingletonSum(Sum):
    def aggregate_partial(self, block):
        return singleton_partial(block)


def test_modified_sum_and_subclass_keep_each_callback(tmp_path):
    patched = Sum('n')
    patched.aggregate_partial = singleton_partial
    with RunContext.start(tmp_path/'run'), data.local_execution(
            workers=1, worker_mode='thread', temp_directory=str(tmp_path)):
        dataset = data.from_items([{'n': i} for i in range(6)])
        for aggregate in (patched, SingletonSum('n')):
            assert dataset.aggregate(aggregate) == {'sum(n)': 15}


class TooLarge(Ordered):
    def aggregate_block(self, block):
        return b'x' * (256 * 1024)

    def combine(self, old, new):
        raise AssertionError('oversized partial reached user merge')


@pytest.mark.parametrize('fault', ['row', 'state', 'column'])
def test_failed_aggregate_does_not_publish(tmp_path, fault):
    limits = {'max_row_bytes': 512, 'max_batch_bytes': 4096, 'max_state_bytes': 128 * 1024}
    rows = [{'n': 1}, {'n': b'x'*1024}] if fault == 'row' else [{'n': 1}]
    agg = {'row': Count(), 'state': TooLarge(), 'column': Sum('missing')}[fault]
    with pytest.raises(Exception, match='missing' if fault == 'column' else 'BUDGET_EXCEEDED'):
        with RunContext.start(tmp_path/'run', limits=limits) as run, data.local_execution(
                workers=1, worker_mode='thread', temp_directory=str(tmp_path)):
            data.from_items(rows).aggregate(agg)
    action = run.journal.execution_action('action-1')
    assert action is None or action.get('result') is None
    assert not run.journal.execution_commits('action-1')
