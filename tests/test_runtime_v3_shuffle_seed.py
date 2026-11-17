"""Explicit shuffle seeds keep the same value order across native actions."""
from collections import Counter

import pytest

from demiflow import data
from demiflow.execution.run_context import RunContext


ROWS = [{'a': i % 3, 'b': i, 'v': float(i)} for i in range(30)]


@pytest.mark.parametrize('reuse_shuffle', [False, True])
def test_explicit_seed_repeats_the_original_thirty_row_order(tmp_path, reuse_shuffle):
    with RunContext.start(tmp_path / 'run'), data.local_execution():
        source = data.from_items(ROWS)
        shuffled = source.random_shuffle(seed=7)
        first = shuffled.take_all()
        second = (shuffled if reuse_shuffle else source.random_shuffle(seed=7)).take_all()
    assert Counter(row['b'] for row in first) == Counter(row['b'] for row in ROWS)
    # Preserve the original failing ordered assertion; multiplicity alone is insufficient.
    assert [row['b'] for row in first] == [row['b'] for row in second]


def test_seed_repeats_union_flatmap_multiplicity_despite_input_order(tmp_path):
    def expand(row):
        return [{'v': row['id'] % 5}, {'v': row['id'] % 5}]

    with RunContext.start(tmp_path / 'run'), data.local_execution(workers=2, partitions=3):
        source = data.range(30).flat_map(expand)
        first = source.union(source).random_shuffle(seed=7, num_blocks=2).take_all()
        alternate = data.from_items([{'v': i % 5} for i in reversed(range(30)) for _ in range(4)])
        second = alternate.random_shuffle(seed=7, num_blocks=4).take_all()
    assert Counter(row['v'] for row in first) == {i: 24 for i in range(5)}
    assert first == second
    assert any(first[i]['v'] != first[i + 1]['v'] for i in range(len(first) - 1))
