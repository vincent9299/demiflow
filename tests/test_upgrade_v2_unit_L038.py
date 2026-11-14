"""L038: Engine.batch_rows intersects the session row window, the sealed row byte
ceiling and the real payload width; an indivisible row over the hard ceiling is
rejected explicitly instead of being masked to one row by max(1, ...).
"""
from types import SimpleNamespace

import pytest

from demiflow.execution.dataset_native import Engine
from demiflow.execution.recovery_contract import RecoveryContractError

MiB = 2**20


def engine(*, batch_rows=8192, max_row_bytes=128 * MiB):
    instance = Engine.__new__(Engine)
    instance._session = SimpleNamespace(options=SimpleNamespace(
        batch_rows=batch_rows, max_row_bytes=max_row_bytes))
    instance.execution = None
    return instance


def rel(width):
    return SimpleNamespace(payload_row_bytes=width)


def test_byte_target_bounds_batch_count():
    # 8 MiB soft target / 1 MiB per row -> 8 rows, under the 8192 row window.
    assert engine().batch_rows(rel(1 * MiB)) == 8


def test_row_window_still_caps_small_rows():
    assert engine(batch_rows=64).batch_rows(rel(1)) == 64


def test_single_row_over_hard_ceiling_rejected():
    with pytest.raises(RecoveryContractError):
        engine(max_row_bytes=4 * MiB).batch_rows(rel(8 * MiB))


def test_sealed_row_ceiling_tightens_target():
    # Sealed max_row_bytes=2 MiB tightens the soft target below 8 MiB.
    assert engine(max_row_bytes=2 * MiB).batch_rows(rel(1 * MiB)) == 2


def test_unknown_width_is_not_rejected():
    assert engine().batch_rows(rel(0)) >= 1


def test_indivisible_row_under_hard_ceiling_emitted_alone():
    # A 10 MiB row exceeds the 8 MiB soft target but fits the 128 MiB hard ceiling;
    # it is emitted as a batch of one, not rejected and not silently truncated.
    assert engine(max_row_bytes=128 * MiB).batch_rows(rel(10 * MiB)) == 1


def test_row_exactly_at_ceiling_admitted():
    assert engine(max_row_bytes=8 * MiB).batch_rows(rel(8 * MiB)) == 1