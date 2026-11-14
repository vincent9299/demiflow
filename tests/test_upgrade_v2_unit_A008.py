"""A008 · write_lance rejects Ray options and admits streaming before the action gate.

Heavy Lance commit/copy execution is exercised by the WRITE/RECOVERY gate suites;
these unit checks pin the admission boundary added at the top of write_lance.
"""
from types import SimpleNamespace

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
        ex.write_lance(ds._source, ds._plan, SimpleNamespace(schema=None),
                       native_options=object())


def test_streaming_write_lance_admits_then_requires_action_context():
    ex = LocalDatasetExecutor()
    ds = _stream()
    # Admission routes streaming write_lance to stream_materialize (allowed);
    # without an outer action the durable-write gate fires, proving no early reject.
    with pytest.raises(RuntimeError, match="execution action context"):
        ex.write_lance(ds._source, ds._plan, SimpleNamespace(schema=None))