"""A buffered idle must not cancel business admitted after that notification."""
from types import SimpleNamespace as NS

import pytest

from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.action_driver.dataflow import DataflowExecution
from demiflow.execution.hosts.thread import ThreadHostEndpoint
from test_runtime_v3_control_batching import Capture, _endpoint, _frames
from test_runtime_v3_idle_notification import _finished_runner
from demiflow.execution.contracts.recovery_contract import RecoveryContractError


def driver(endpoint):
    ex = object.__new__(DataflowExecution)
    ex._endpoints = {'worker': endpoint}
    ex._scheduler = NS(leases={'worker': NS(boot_id='boot')}, scope_id='scope')
    ex._idle = set()
    return ex


def idle(ex, generation):
    payload = {'done': False}
    if generation is not None:
        payload['activity_generation'] = generation
    DataflowExecution._handle_event(ex, NS(kind='idle', allocation_id='worker', payload=payload))


def test_restored_idle_arriving_after_resume_cannot_park_the_new_source_window():
    capture = Capture(); endpoint = _endpoint(capture)
    ex = driver(endpoint)
    # This is the real endpoint framing path; the pre-resume idle remains in
    # flight while the authoritative resume is admitted. No worker is mocked
    # as having consumed or ACKed any business data.
    endpoint.send_business({'kind': 'resume'})
    assert _frames(capture.packets) == [{'type': 'dataflow_command',
        'command': {'kind': 'resume', 'activity_generation': 1}}]
    idle(ex, 0)
    assert not ex._idle
    idle(ex, None)  # A pre-watermark startup observation is likewise stale.
    assert not ex._idle
    idle(ex, 1)
    assert ex._idle == {'worker'}


def test_notification_keeps_its_original_generation_during_concurrent_resume():
    runner = _finished_runner()
    runner.receive({'kind': 'pause', 'activity_generation': 1})
    sent = []
    def send(kind, payload=None, ref=None):
        if kind == 'idle':
            if not sent:
                runner.receive({'kind': 'resume', 'activity_generation': 2})
            sent.append(payload)
    runner._send = send
    runner.step(max_records=1, deadline_ms=50)
    runner.step(max_records=1, deadline_ms=50)
    assert [x['activity_generation'] for x in sent] == [1, 2]
    assert all(x['done'] for x in sent)


@pytest.mark.parametrize('generation', [-1, True, '1', 2**63])
def test_invalid_or_oversized_worker_activity_is_rejected_before_resume(generation):
    runner = _finished_runner()
    with pytest.raises(DataflowError, match='activity generation'):
        runner.receive({'kind': 'resume', 'activity_generation': generation})
    assert runner._paused


def test_duplicate_activity_cannot_repeat_resume_or_reset_a_frontier():
    runner = _finished_runner()
    runner.receive({'kind': 'pause', 'activity_generation': 2})
    for generation in (1, 2):
        with pytest.raises(DataflowError, match='activity generation'):
            runner.receive({'kind': 'resume', 'activity_generation': generation})
    assert runner._paused


@pytest.mark.parametrize('generation', [-1, True, '1', 2, 2**63])
def test_driver_rejects_forged_future_or_invalid_idle(generation):
    ex = driver(NS(_activity_generation=1))
    with pytest.raises(DataflowError, match='idle activity generation'):
        idle(ex, generation)
    assert not ex._idle


def test_generation_does_not_replace_original_boot_fence():
    ex = driver(NS(_activity_generation=1))
    ex._attempt = lambda: 1; ex._fence = lambda: 'fence'; ex._run_id = lambda: 'run'
    ex._physical = NS(plan_revision=0)
    event = NS(attempt=1, fence='fence', run_id='run', scope_id='scope',
        controller_epoch=1, plan_revision=0, allocation_id='worker',
        boot_id='retired-boot', operation_id='unused')
    with pytest.raises(DataflowError, match='boot_id'):
        ex._validate_event(event)
    assert not ex._idle


def test_local_and_thread_admit_identical_activity_without_ack_or_lease_churn():
    capture = Capture(); local = _endpoint(capture)
    observed = []
    thread = object.__new__(ThreadHostEndpoint)
    thread._activated = True; thread._allocation_id = 'owned'
    thread._connector = NS(_host=lambda _: NS(assigned=lambda: True, receive=observed.append))
    commands = [{'kind': 'park_policy'}, {'kind': 'ack', 'delivery_id': 'old'},
        {'kind': 'renew_lease'}, {'kind': 'resume'},
        {'kind': 'data', 'delivery_id': 'new'}, {'kind': 'park'}]
    for command in commands:
        local.send_business(command); thread.send_business(command)
    local_commands = [f['command'] for f in _frames(capture.packets)]
    assert local_commands == observed
    assert [x.get('activity_generation') for x in observed] == [1, None, None, 2, 3, 4]
    assert all('activity_generation' not in x for x in commands)


def test_exhausted_activity_watermark_is_rejected_without_sending():
    capture = Capture(); endpoint = _endpoint(capture)
    endpoint._activity_generation = 2**63 - 1
    with pytest.raises(DataflowError, match='activity generation exhausted'):
        endpoint.send_business({'kind': 'resume'})
    assert not capture.packets


def test_invalid_frame_never_advances_an_unadmitted_activity():
    capture = Capture(); endpoint = _endpoint(capture)
    with pytest.raises(RecoveryContractError, match='strict JSON'):
        endpoint.send_business({'kind': 'data', 'invalid': object()})
    assert not capture.packets and getattr(endpoint, '_activity_generation', 0) == 0
