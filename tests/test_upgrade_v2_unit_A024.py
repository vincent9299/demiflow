"""A024 · write_datasink rejects Ray options and admits streaming before the action gate.

Heavy Datasink commit execution is exercised by the WRITE/RECOVERY gate suites;
these unit checks pin the admission boundary added at the top of write_datasink.
"""
import pytest

from demiflow import data
from demiflow.errors import UnsupportedExecutionOptionError
from demiflow.execution.executors.local import LocalDatasetExecutor


def _stream():
    return data.from_items([{"id": i} for i in range(3)]).map(
        lambda r: r, execution="thread", concurrency=2)


def test_ray_native_options_rejected_before_admission():
    ex = LocalDatasetExecutor()
    ds = _stream()
    with pytest.raises(UnsupportedExecutionOptionError, match="Ray target"):
        ex.write_datasink(ds._source, ds._plan, object(), native_options=object())


def test_streaming_write_datasink_admits_then_requires_action_context():
    ex = LocalDatasetExecutor()
    ds = _stream()
    with pytest.raises(RuntimeError, match="execution action context"):
        ex.write_datasink(ds._source, ds._plan, object())