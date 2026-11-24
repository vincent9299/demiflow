"""Invocation images contain bounded data, with exact output and slot frontiers."""
import threading

import pytest

from demiflow.execution.contracts.dataflow import Record
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.kernels.concurrent_calls import ConcurrentCalls, INVOCATION_SCOPE
from demiflow.execution.contracts import state_codec


def row(i):
    from demiflow.execution.contracts.recovery_contract import identity
    from demiflow.execution.contracts.record_codec import _encode_payload
    return Record(record_id=identity(['row', i]), source_position={'offset': i},
                  payload_ref=None, inline={'i': i}, encoded_bytes=len(_encode_payload({'i': i}, max_bytes=1024)), sequence=i)


@pytest.mark.parametrize('bounded', (False, True))
def test_restored_invocation_keeps_scope_slot_and_emitted_frontier(bounded):
    credit = row(0).encoded_bytes if bounded else None
    release = threading.Event()
    first = ConcurrentCalls(2, check_stop=lambda: None, input_limit=credit)
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
    replay = ConcurrentCalls(2, check_stop=lambda: None, input_limit=credit)
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


@pytest.mark.parametrize('mutate', ['duplicate_slot', 'missing_slot', 'bad_input', 'bad_scope', 'negative_emitted', 'over_credit'])
def test_corrupt_invocation_image_rejects_before_launch(mutate):
    image = [{'slot': 0, 'input': row(0), 'scope': 'b' * 64, 'emitted': 0}]
    slots = [1]
    if mutate == 'duplicate_slot': slots = [0]
    if mutate == 'missing_slot': slots = []
    if mutate == 'bad_input': image[0]['input'] = object()
    if mutate == 'bad_scope': image[0]['scope'] = 'unknown'
    if mutate == 'negative_emitted': image[0]['emitted'] = -1
    credit = row(0).encoded_bytes - 1 if mutate == 'over_credit' else None
    calls, launched = ConcurrentCalls(2, check_stop=lambda: None, input_limit=credit), []
    try:
        with pytest.raises(DataflowError):
            calls.restore(image, slots=slots, compute=lambda *args: launched.append(args))
        assert launched == [] and not calls.jobs
    finally:
        calls.close(timeout=5)

def test_byte_credit_waits_for_an_input_to_finish_and_delivers_its_output():
    first, second = row(0), row(1)
    calls = ConcurrentCalls(2, check_stop=lambda: None, input_limit=first.encoded_bytes)
    entered, release = threading.Event(), threading.Event()
    def delayed(slot):
        entered.set()
        release.wait(5)
        yield first
    def next_call(slot):
        assert calls.input_bytes <= calls.input_limit
        yield second
    try:
        assert list(calls.submit(delayed, scope=lambda slot: 'a' * 64, input=first)) == []
        assert entered.wait(2)
        release.set()
        emitted = list(calls.submit(next_call, scope=lambda slot: 'b' * 64, input=second))
        emitted.extend(calls.finish())
        assert emitted == [first, second] and calls.input_bytes == 0
    finally:
        release.set()
        calls.close(timeout=5)
