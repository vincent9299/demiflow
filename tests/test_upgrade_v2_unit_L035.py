"""L035 unit: GraphWorkerExecutor._request_contribution async bounded ACK (C08/C13).

The graph receipt is the C13 shape {status:'acked', checkpoint_id, sequence,
commit_id, channel_cuts} -- a different wire from the chain CommitReceipt -- and
deliver_receipt is keyed by operation_id. Proves the wait is awaitable (heartbeat
NOT frozen), the bound is min of the real deadlines, timeout/cancel keep the cut
unknown, a mismatched checkpoint_id is rejected, and a late ACK for a timed-out
contribution cannot disturb a subsequent pending one. Real validate_cut_contribution
and the real keyed deliver_receipt drive the handshake.
"""
import asyncio
import pathlib
import time

import pytest

import demiflow

assert pathlib.Path(demiflow.__file__).resolve().is_relative_to(
    pathlib.Path(__file__).resolve().parents[1]), demiflow.__file__

from demiflow.execution.graph_wire import CUT_CONTRIBUTION_SCHEMA
from demiflow.execution.recovery_contract import RecoveryContractError
from demiflow.execution.worker.graph_execution import GraphWorkerExecutor


class _Authority:
    def __init__(self, *, checkpoint_timeout_s=3600.0, grant_ttl_s=3600.0,
                 deadline_ttl_s=3600.0, timeout_s=3600.0):
        self.limits = {"checkpoint_timeout_s": checkpoint_timeout_s}
        self._grant = {"expires_at_ms": int((time.time() + grant_ttl_s) * 1000)}
        self.deadline = time.time() + deadline_ttl_s
        self._limits = {"timeout_s": timeout_s}
        self.started = time.monotonic()


class _GraphProgress:
    def __init__(self):
        self.sequence = 0
        self.acked_inputs = []

    def note_contribution_acked(self, inputs):
        self.acked_inputs.append(inputs)


class _Router:
    def __init__(self):
        self.gc_calls = []

    def advance_gc(self, cuts):
        self.gc_calls.append(cuts)


def _contribution(checkpoint_id):
    return {"schema_version": CUT_CONTRIBUTION_SCHEMA, "checkpoint_id": checkpoint_id,
            "allocation_id": "alloc-1", "channel_cuts": [], "operator_states": [],
            "inflight": [], "output_descriptor": None, "source_progress": [], "final": False}


def _receipt(checkpoint_id, *, status="acked", sequence=1, commit_id="commit-1",
             channel_cuts=None):
    return {"status": status, "checkpoint_id": checkpoint_id, "sequence": sequence,
            "commit_id": commit_id, "channel_cuts": channel_cuts or []}


def _executor(**authority_kw):
    sent = []
    launch_spec = {"graph": {
        "output_child_id": "out", "plan_ref": {"path": "p", "sha256": "x"},
        "source_id": "source-1", "occurrence_id": "occ-1",
    }}
    ex = GraphWorkerExecutor(launch_spec, send=sent.append)
    ex.authority = _Authority(**authority_kw)
    ex.progress = _GraphProgress()
    ex.output_router = _Router()
    return ex, sent


def _states(inputs):
    return {"stream-progress": {"inputs": inputs}}


def test_happy_path_advances_sequence_and_gc():
    async def scenario():
        ex, sent = _executor()
        task = asyncio.create_task(
            ex._request_contribution(_contribution("ck-1"), None, _states(5), False))
        await asyncio.sleep(0.02)
        assert not task.done()
        op = sent[0]["operation_id"]
        cuts = [{"channel_id": "chan-1", "sequence": 2}]
        ex.deliver_receipt(op, _receipt("ck-1", sequence=1, channel_cuts=cuts))
        receipt = await asyncio.wait_for(task, timeout=2.0)
        assert receipt["status"] == "acked"
        assert ex.progress.sequence == 1
        assert ex.progress.acked_inputs == [5]
        assert ex.output_router.gc_calls == [cuts]
        assert ex.pending_contributions() == set()
        assert len(sent) == 1
    asyncio.run(scenario())


def test_event_loop_heartbeat_not_frozen_while_awaiting_ack():
    async def scenario():
        ex, sent = _executor()
        ticks = []
        stop = asyncio.Event()

        async def heartbeat():
            while not stop.is_set():
                ticks.append(time.monotonic())
                await asyncio.sleep(0.005)

        hb = asyncio.create_task(heartbeat())
        task = asyncio.create_task(
            ex._request_contribution(_contribution("ck-1"), None, _states(5), False))
        await asyncio.sleep(0.05)
        assert not task.done()
        ticks_during_wait = len(ticks)
        ex.deliver_receipt(sent[0]["operation_id"], _receipt("ck-1"))
        await asyncio.wait_for(task, timeout=2.0)
        stop.set()
        await hb
        assert ticks_during_wait >= 3, ticks_during_wait
    asyncio.run(scenario())


def test_timeout_keeps_cut_unknown_without_resend():
    async def scenario():
        ex, sent = _executor(checkpoint_timeout_s=0.05)
        with pytest.raises(asyncio.TimeoutError):
            await ex._request_contribution(_contribution("ck-1"), None, _states(5), False)
        assert ex.progress.sequence == 0
        assert ex.progress.acked_inputs == []
        assert ex.output_router.gc_calls == []
        assert len(sent) == 1
        assert ex.pending_contributions() == set()
    asyncio.run(scenario())


def test_cancel_keeps_cut_unknown():
    async def scenario():
        ex, sent = _executor()
        task = asyncio.create_task(
            ex._request_contribution(_contribution("ck-1"), None, _states(5), False))
        await asyncio.sleep(0.02)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert ex.progress.sequence == 0
        assert ex.progress.acked_inputs == []
        assert len(sent) == 1
        assert ex.pending_contributions() == set()
    asyncio.run(scenario())


def test_mismatched_checkpoint_id_is_rejected():
    async def scenario():
        ex, sent = _executor()
        task = asyncio.create_task(
            ex._request_contribution(_contribution("ck-1"), None, _states(5), False))
        await asyncio.sleep(0.02)
        ex.deliver_receipt(sent[0]["operation_id"], _receipt("ck-WRONG"))
        with pytest.raises(RecoveryContractError):
            await task
        assert ex.progress.sequence == 0
        assert ex.progress.acked_inputs == []
    asyncio.run(scenario())


def test_non_acked_status_is_rejected():
    async def scenario():
        ex, sent = _executor()
        task = asyncio.create_task(
            ex._request_contribution(_contribution("ck-1"), None, _states(5), False))
        await asyncio.sleep(0.02)
        ex.deliver_receipt(sent[0]["operation_id"], _receipt("ck-1", status="data_committed"))
        with pytest.raises(RecoveryContractError):
            await task
        assert ex.progress.sequence == 0
    asyncio.run(scenario())


def test_late_ack_for_timed_out_cut_cannot_disturb_new_pending_cut():
    async def scenario():
        ex, sent = _executor(checkpoint_timeout_s=0.05)
        with pytest.raises(asyncio.TimeoutError):
            await ex._request_contribution(_contribution("ck-A"), None, _states(5), False)
        op_a = sent[0]["operation_id"]
        assert ex.pending_contributions() == set()
        # A new contribution B is outstanding under a fresh, generous bound.
        ex.authority = _Authority()
        task_b = asyncio.create_task(
            ex._request_contribution(_contribution("ck-B"), None, _states(6), False))
        await asyncio.sleep(0.02)
        assert not task_b.done()
        op_b = sent[1]["operation_id"]
        assert op_b != op_a
        # The keyed late ACK for A finds no pending entry: B is untouched.
        ex.deliver_receipt(op_a, _receipt("ck-A"))
        await asyncio.sleep(0.02)
        assert not task_b.done()
        assert ex.progress.sequence == 0
        # B still completes normally on its own ACK.
        ex.deliver_receipt(op_b, _receipt("ck-B"))
        await asyncio.wait_for(task_b, timeout=2.0)
        assert ex.progress.sequence == 1
        assert ex.progress.acked_inputs == [6]
    asyncio.run(scenario())


@pytest.mark.parametrize("small", [
    {"checkpoint_timeout_s": 0.05},
    {"grant_ttl_s": 0.05},
    {"deadline_ttl_s": 0.05},
    {"timeout_s": 0.05},
])
def test_bound_is_min_of_every_real_deadline(small):
    kw = dict(checkpoint_timeout_s=3600.0, grant_ttl_s=3600.0,
              deadline_ttl_s=3600.0, timeout_s=3600.0)
    kw.update(small)

    async def scenario():
        ex, sent = _executor(**kw)
        started = time.monotonic()
        with pytest.raises(asyncio.TimeoutError):
            await ex._request_contribution(_contribution("ck-1"), None, _states(5), False)
        assert time.monotonic() - started < 2.0
        assert len(sent) == 1
    asyncio.run(scenario())
