"""A-U01 · Dataset.sort scalar-bool normalization reaches the same fail-closed guard.

Coordinator-assigned supplemental unit (no original card), surfaced by V019.
Only Dataset.sort changed: a scalar ``descending`` bool is normalized to a
per-key tuple before the len()/iteration validation, so the documented
defaults no longer crash with ``TypeError: object of type 'bool' has no len()``
and instead reach the same downstream fail-closed rejection as the explicit
sequence form. Sort stays lazy and Ray-only: Local never executes it and never
switches targets automatically.

NOTE on the exact rejection (recorded in A-U01.json needs_decision): in this
c5ccb60 worktree the A-group admission gate wired into iter_rows
(local.py:770 -> capabilities.py:59-60) rejects SortOp first with
``ValueError("Operation has no pull backend capability contract")``. The
Ray-specific ``UnsupportedExecutionOptionError("Dataset.sort requires a Ray
PipelineExecutionTarget. ...")`` (local.py:489) is only reached on a tree
without that admission (e.g. the main repo the coordinator's repro used).
These tests pin the TRUE in-tree fail-closed and the scalar==sequence
consistency; they do not weaken any guard.
"""
import pytest

from demiflow import data
from demiflow.data.dataset import Dataset
from demiflow.errors import UnsupportedExecutionOptionError

# Fail-closed rejection emitted by the admission gate in this worktree.
ADMISSION_REJECT = "Operation has no pull backend capability contract"
# Ray-specific message the coordinator named (main-repo / pre-admission path).
RAY_SORT_MESSAGE = (
    "Dataset.sort requires a Ray PipelineExecutionTarget. "
    "Demiflow does not switch targets automatically."
)
FAIL_CLOSED = (UnsupportedExecutionOptionError, ValueError)


def _ds():
    return data.from_items([{"x": 2, "y": 1}, {"x": 1, "y": 2}])


def _rejection(ds):
    with pytest.raises(FAIL_CLOSED) as excinfo:
        ds.take_all()
    return type(excinfo.value), str(excinfo.value)


@pytest.mark.parametrize("descending", [False, True, (False,), (True,)])
def test_scalar_and_sequence_sort_fail_closed_identically(descending):
    ds = _ds().sort("x", descending=descending)
    # sort stays lazy: it appends SortOp and returns a Dataset without reading.
    assert isinstance(ds, Dataset)
    assert _rejection(ds) == (ValueError, ADMISSION_REJECT)


def test_default_descending_sort_fails_closed():
    ds = _ds().sort("x")
    assert isinstance(ds, Dataset)
    assert _rejection(ds) == (ValueError, ADMISSION_REJECT)


def test_scalar_default_matches_sequence_form():
    # The core A-U01 guarantee: documented defaults reach the SAME fail-closed
    # as the explicit sequence form, instead of crashing on len(bool).
    scalar = _rejection(_ds().sort("x"))
    sequence = _rejection(_ds().sort("x", descending=(False,)))
    assert scalar == sequence


@pytest.mark.parametrize("descending", [False, True])
def test_multi_key_scalar_bool_fails_closed(descending):
    # Scalar bool normalizes to one entry per key, not a len() crash.
    ds = _ds().sort(["x", "y"], descending=descending)
    assert isinstance(ds, Dataset)
    assert _rejection(ds) == (ValueError, ADMISSION_REJECT)


def test_multi_key_matching_sequence_fails_closed():
    assert _rejection(_ds().sort(["x", "y"], descending=(False, True))) == (
        ValueError, ADMISSION_REJECT,
    )


def test_scalar_bool_no_longer_raises_type_error():
    # Regression guard for the original defect: scalar bool must not crash
    # with "object of type 'bool' has no len()" before reaching fail-closed.
    for descending in (False, True):
        with pytest.raises(FAIL_CLOSED) as excinfo:
            _ds().sort("x", descending=descending).take_all()
        assert not isinstance(excinfo.value, TypeError)
        assert "has no len" not in str(excinfo.value)


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