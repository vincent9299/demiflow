"""Closed grant projection unit checks; real Ray coverage is in the adapter gate."""
import sys
import threading
from contextvars import copy_context
from collections import deque
from types import SimpleNamespace

import pytest

from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.hosts.dataflow_worker_host import HostedDataflowWorker
from demiflow.resources.capacity import inherited_envelope


def unit(monkeypatch):
    worker = HostedDataflowWorker.__new__(HostedDataflowWorker)
    worker._ray_host = True
    worker._closed = False
    worker._lock = threading.Lock()
    worker._spec_hash = 'a'*64
    worker._resource_directory = '/tmp/demiflow-grant-unit/cache/owned'
    worker._resource_envelope = None
    worker._runtime = SimpleNamespace(allocation_id='owned')
    worker._cancel_event = threading.Event()
    worker._mailbox = SimpleNamespace(close=lambda: None)
    worker._control_outbox = deque()
    called = []
    worker._handle = lambda message: called.append(inherited_envelope()) or {}
    monkeypatch.setitem(sys.modules, 'ray', SimpleNamespace(get_runtime_context=lambda:
        SimpleNamespace(get_assigned_resources=lambda: {'CPU': 2., 'memory': 1048576.})))
    envelope = {'allocation_id':'owned', 'spec_hash':'a'*64, 'cpu_millis':2000,
                'memory_bytes':1048576, 'resource_directory':worker._resource_directory}
    return worker, envelope, called


@pytest.mark.parametrize('field,value', [('allocation_id','other'), ('spec_hash','b'*64),
    ('cpu_millis',1000), ('memory_bytes',1048577), ('resource_directory','/tmp/demiflow-foreign'),
    ('cpu_millis',True), ('memory_bytes',True)])
def test_ray_grant_mismatch_fails_before_business(monkeypatch, field, value):
    worker, envelope, called = unit(monkeypatch)
    baseline = inherited_envelope()
    with pytest.raises(DataflowError, match='Ray envelope differs'):
        worker.handle({'kind':'Assign'}, resource_envelope={**envelope, field:value})
    assert not called and worker._resource_envelope is None
    assert inherited_envelope() == baseline


def test_ray_grant_missing_fails_before_business(monkeypatch):
    worker, _, called = unit(monkeypatch)
    with pytest.raises(DataflowError, match='original resource envelope'):
        worker.handle({'kind':'VerifyArtifactAndCapabilities'})
    assert not called


def test_ray_original_grant_reaches_copied_business_context_and_cleans_up(monkeypatch):
    worker, envelope, called = unit(monkeypatch)
    baseline = inherited_envelope()
    def business(message):
        context = copy_context()
        thread = threading.Thread(target=lambda: context.run(lambda: called.append(inherited_envelope())))
        thread.start(); thread.join(timeout=1)
        assert not thread.is_alive()
        return {}
    worker._handle = business
    worker.handle({'kind':'Assign'}, resource_envelope=envelope)
    assert called == [envelope]
    assert inherited_envelope() == baseline
    # External mutation cannot enlarge the already validated projection.
    envelope['memory_bytes'] += 1
    assert worker._resource_envelope['memory_bytes'] == 1048576
    with pytest.raises(DataflowError):
        worker.handle({'kind':'Assign'}, resource_envelope=envelope)
    closed = []
    worker._runtime.close = lambda: closed.append(inherited_envelope())
    worker.close()
    assert closed == called
    assert worker._resource_envelope is None and inherited_envelope() == baseline
    envelope['memory_bytes'] -= 1
    with pytest.raises(DataflowError, match='closed worker'):
        worker.handle({'kind':'Assign'}, resource_envelope=envelope)
