"""LNU05: rechunk is real input buffering. It preserves the declared callback row
boundary across Lance fragments and borrows the sealed inflight byte ceiling; a
declared group that cannot fit fails explicitly instead of being shrunk.
"""
import pyarrow as pa
import pytest

from demiflow.execution.arrow_batch_sink import rechunk
from demiflow.execution.recovery_contract import RecoveryContractError


def batches(sizes):
    return [pa.record_batch({'x': pa.array(list(range(n)), type=pa.int64())}) for n in sizes]


def row_bytes():
    return pa.record_batch({'x': pa.array([0], type=pa.int64())}).nbytes


def test_unbound_preserves_global_boundaries():
    groups = list(rechunk(batches([2, 2, 2]), 3))
    assert [g.num_rows for g in groups] == [3, 3]


def test_fitting_byte_budget_preserves_boundaries():
    groups = list(rechunk(batches([2, 2, 2]), 3, max_bytes=3 * row_bytes()))
    assert [g.num_rows for g in groups] == [3, 3]


def test_oversized_declared_group_fails_explicitly():
    # A 3-row group needs 3*row_bytes; a 2-row ceiling cannot admit it. The business
    # boundary is not silently reduced to 2 rows to make it fit.
    with pytest.raises(RecoveryContractError, match='inflight byte budget'):
        list(rechunk(batches([2, 2, 2]), 3, max_bytes=2 * row_bytes()))


def test_single_wide_row_fails():
    with pytest.raises(RecoveryContractError, match='inflight byte budget'):
        list(rechunk(batches([1]), 1, max_bytes=row_bytes() - 1))


def test_partial_final_group_under_budget():
    groups = list(rechunk(batches([2, 1]), 3, max_bytes=3 * row_bytes()))
    assert [g.num_rows for g in groups] == [3]


def test_byte_ceiling_resets_per_group():
    # Each group is independently bounded; many small groups all fit a per-group ceiling.
    groups = list(rechunk(batches([1, 1, 1, 1]), 1, max_bytes=row_bytes()))
    assert [g.num_rows for g in groups] == [1, 1, 1, 1]