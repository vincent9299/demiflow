"""Invocation images contain bounded data, with exact output and slot frontiers."""
import threading

import pytest

from demiflow.execution.contracts.dataflow import Record
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.kernels.concurrent_calls import ConcurrentCalls, INVOCATION_SCOPE
from demiflow.execution.kernels import state_codec


def row(i):
    from demiflow.execution.recovery_contract import identity
    from demiflow.execution.exchange.dataflow_bridge import _encode_payload
    return Record(record_id=identity(['row', i]), source_position={'offset': i},
                  payload_ref=None, inline={'i': i}, encoded_bytes=len(_encode_payload({'i': i}, max_bytes=1024)), sequence=i)


def test_restored_invocation_keeps_scope_slot_and_emitted_frontier():
    release = threading.Event()
    first = ConcurrentCalls(2, check_stop=lambda: None)
    scope = 'a' * 64
    def delayed(slot):
        yield row(0)
        release.wait(5)
        yield row(1)
    try:
        delivered = list(first.submit(delayed, scope=lambda slot: scope, input=row(0)))
        while not delivered:
            delivered.extend(first.poll(wait_for=True))
        assert delivered == [row(0)]
        image = state_codec.loads(state_codec.dumps(first.snapshot(), max_bytes=8192), max_bytes=8192)
        slots = list(first.slots)
        assert image[0]['emitted'] == 1
    finally:
        release.set()
        first.close(timeout=5)
    replay = ConcurrentCalls(2, check_stop=lambda: None)
    seen = []
    def compute(slot, input):
        seen.append((slot, input, INVOCATION_SCOPE.get()))
        return iter((row(0), row(1)))
    try:
        replay.restore(image, slots=slots, compute=compute)
        assert list(replay.finish()) == [row(1)]
        assert seen == [(image[0]['slot'], row(0), scope)]
        assert sorted(replay.slots) == [0, 1]
    finally:
        replay.close(timeout=5)


@pytest.mark.parametrize('mutate', ['duplicate_slot', 'missing_slot', 'bad_input', 'bad_scope', 'negative_emitted'])
def test_corrupt_invocation_image_rejects_before_launch(mutate):
    image = [{'slot': 0, 'input': row(0), 'scope': 'b' * 64, 'emitted': 0}]
    slots = [1]
    if mutate == 'duplicate_slot': slots = [0]
    if mutate == 'missing_slot': slots = []
    if mutate == 'bad_input': image[0]['input'] = object()
    if mutate == 'bad_scope': image[0]['scope'] = 'unknown'
    if mutate == 'negative_emitted': image[0]['emitted'] = -1
    calls, launched = ConcurrentCalls(2, check_stop=lambda: None), []
    try:
        with pytest.raises(DataflowError):
            calls.restore(image, slots=slots, compute=lambda *args: launched.append(args))
        assert launched == [] and not calls.jobs
    finally:
        calls.close(timeout=5)
