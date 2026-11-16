"""A-U01: scalar/sequence sort normalization on the unified Local engine.

The v2 Ray-only rejection was intentionally retired by v3. Preserve argument
validation and laziness, and verify actual ordering plus bounded-row rejection.
"""
import pytest

from demiflow import data
from demiflow.data.dataset import Dataset
from demiflow.execution.run_context import RunContext
from demiflow.execution.contracts.dataflow_errors import DataflowError

def _ds():
    return data.from_items([{"x": 2, "y": 1}, {"x": 1, "y": 2}])


@pytest.mark.parametrize("descending", [False, True, (False,), (True,)])
def test_scalar_and_sequence_sort_execute_identically(descending):
    ds = _ds().sort("x", descending=descending)
    # sort stays lazy: it appends SortOp and returns a Dataset without reading.
    assert isinstance(ds, Dataset)
    reverse = descending if type(descending) is bool else descending[0]
    assert ds.take_all() == sorted([{"x": 2, "y": 1}, {"x": 1, "y": 2}], key=lambda row: row["x"], reverse=reverse)


def test_default_sort_executes_ascending():
    ds = _ds().sort("x")
    assert isinstance(ds, Dataset)
    assert [row["x"] for row in ds.take_all()] == [1, 2]


def test_scalar_default_matches_sequence_form():
    assert _ds().sort("x").take_all() == _ds().sort("x", descending=(False,)).take_all()


@pytest.mark.parametrize("descending", [False, True])
def test_multi_key_scalar_bool_executes(descending):
    # Scalar bool normalizes to one entry per key, not a len() crash.
    ds = _ds().sort(["x", "y"], descending=descending)
    assert isinstance(ds, Dataset)
    assert [row["x"] for row in ds.take_all()] == ([2, 1] if descending else [1, 2])


def test_multi_key_matching_sequence_executes():
    assert _ds().sort(["x", "y"], descending=(False, True)).take_all() == [
        {"x": 1, "y": 2}, {"x": 2, "y": 1}]



def test_scalar_bool_no_longer_raises_type_error():
    for descending in (False, True):
        assert [row["x"] for row in _ds().sort("x", descending=descending).take_all()] == (
            [2, 1] if descending else [1, 2])


@pytest.mark.parametrize("descending", [(True,), (), (False, True, False)])
def test_multi_key_length_mismatch_still_value_error(descending):
    with pytest.raises(ValueError, match="one bool per key"):
        _ds().sort(["x", "y"], descending=descending)


def test_single_key_length_mismatch_still_value_error():
    with pytest.raises(ValueError, match="one bool per key"):
        _ds().sort("x", descending=(False, True))


def test_non_bool_sequence_element_still_value_error():
    with pytest.raises(ValueError, match="one bool per key"):
        _ds().sort("x", descending=(1,))


def test_non_numeric_boundaries_still_type_error():
    with pytest.raises(TypeError, match="boundaries must be numeric"):
        _ds().sort("x", boundaries=[1, "2"])


def test_bool_boundaries_still_type_error():
    with pytest.raises(TypeError, match="boundaries must be numeric"):
        _ds().sort("x", boundaries=[True])


def test_sort_rejects_over_budget_row_without_successful_commit(tmp_path):
    with pytest.raises(DataflowError) as caught:
        with RunContext.start(tmp_path / 'run', limits={'max_row_bytes': 128,
                'max_batch_bytes': 2048}) as run:
            data.from_items([{'x': 1, 'payload': 'x' * 4096}]).sort('x').take_all()
    assert caught.value.code == 'BUDGET_EXCEEDED'
    assert not run.journal.execution_commits('action-1')
