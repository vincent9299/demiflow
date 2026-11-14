"""L034 unit: WorkerChainExecutor._request_commit async bounded ACK wait (C08).

Proves the changed seam in isolation: the wait is awaitable on the event loop
(heartbeat NOT frozen), the bound is min(checkpoint_timeout, grant expiry, run
deadline, monotonic timeout) with no invented default, timeout/cancel keep the
commit unknown (no resend, no early ownership release), and a late ACK cannot
advance a subsequent cut. The commit authority/progress lifecycle is unchanged
baseline; faithful minimal stand-ins carry the real deadline fields so the bound
math is exercised genuinely. Real build_commit_cut/build_commit_receipt/
validate_commit_receipt and the real deliver_receipt drive the handshake.
"""
import asyncio
import pathlib
import threading
import time

import pytest

import demiflow

# The platform under test must come from this worktree, never the main repo.
assert pathlib.Path(demiflow.__file__).resolve().is_relative_to(
    pathlib.Path(__file__).resolve().parents[1]), demiflow.__file__

from demiflow.execution.chain import build_commit_receipt
from demiflow.execution.recovery_contract import RecoveryContractError
from demiflow.execution.worker.execution import WorkerChainExecutor


class _Authority:
    """Carries the exact deadline fields WorkerAuthority exposes to the bound."""

    def __init__(self, *, checkpoint_timeout_s=3600.0, grant_ttl_s=3600.0,
                 deadline_ttl_s=3600.0, timeout_s=3600.0):
        self.limits = {"checkpoint_timeout_s": checkpoint_timeout_s}
        self._grant = {"expires_at_ms": int((time.time() + grant_ttl_s) * 1000)}
        self.deadline = time.time() + deadline_ttl_s
        self._limits = {"timeout_s": timeout_s}
        self.started = time.monotonic()


class _Progress:
    def __init__(self):
        self.inputs = 0
        self.rows = 0
        self.sequence = 0


class _Sealed:
    def __init__(self, rows):
        self._rows = rows

    def descriptor(self):
        return {"blocks": [], "rows": self._rows}


def _executor(**authority_kw):
    sent = []
    launch_spec = {"execution": {
        "source_child_id": "src", "output_child_id": "out",
        "plan_ref": {"path": "p", "sha256": "x"},
        "source_reference": {"source_id": "source-1"},
        "channel_id": "chan-1",
    }}
    ex = WorkerChainExecutor(launch_spec, send=sent.append)
    ex.authority = _Authority(**authority_kw)
    ex.progress = _Progress()
    return ex, sent


def _ack(sent, index=0, commit_id="commit-1"):
    cut = sent[index]
    return build_commit_receipt(cut["operation_id"], commit_id, cut["sequence"],
                                "acked", {})


def test_happy_path_advances_confirmed_ledger():
    async def scenario():
        ex, sent = _executor()
        task = asyncio.create_task(ex._request_commit(3, _Sealed(7), {}, False))
        await asyncio.sleep(0.02)
        assert not task.done()
        ex.deliver_receipt(_ack(sent))
        receipt = await asyncio.wait_for(task, timeout=2.0)
        assert receipt["status"] == "acked"
        assert ex.progress.inputs == 3
        assert ex.progress.sequence == receipt["sequence"]
        assert ex.progress.rows == 7
        assert ex._pending is None
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
        task = asyncio.create_task(ex._request_commit(3, _Sealed(7), {}, False))
        await asyncio.sleep(0.05)
        # The request is genuinely pending and the loop kept ticking during it.
        assert not task.done()
        ticks_during_wait = len(ticks)
        ex.deliver_receipt(_ack(sent))
        await asyncio.wait_for(task, timeout=2.0)
        stop.set()
        await hb
        assert ticks_during_wait >= 3, ticks_during_wait
    asyncio.run(scenario())


def test_timeout_keeps_commit_unknown_without_resend():
    async def scenario():
        ex, sent = _executor(checkpoint_timeout_s=0.05)
        with pytest.raises(asyncio.TimeoutError):
            await ex._request_commit(3, _Sealed(7), {}, False)
        assert (ex.progress.inputs, ex.progress.sequence, ex.progress.rows) == (0, 0, 0)
        assert len(sent) == 1          # no auto-resend
        assert ex._pending is None     # invalidated; ownership not released early
    asyncio.run(scenario())


def test_cancel_keeps_commit_unknown():
    async def scenario():
        ex, sent = _executor()
        task = asyncio.create_task(ex._request_commit(3, _Sealed(7), {}, False))
        await asyncio.sleep(0.02)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert (ex.progress.inputs, ex.progress.sequence, ex.progress.rows) == (0, 0, 0)
        assert len(sent) == 1
        assert ex._pending is None
    asyncio.run(scenario())


def test_late_ack_cannot_push_a_subsequent_cut():
    async def scenario():
        ex, sent = _executor(checkpoint_timeout_s=0.05)
        with pytest.raises(asyncio.TimeoutError):
            await ex._request_commit(3, _Sealed(7), {}, False)
        late_ack = _ack(sent, 0, commit_id="commit-A")
        assert ex._pending is None
        # A new cut B is now outstanding under a fresh, generous bound.
        ex.authority = _Authority()
        task_b = asyncio.create_task(ex._request_commit(4, _Sealed(9), {}, False))
        await asyncio.sleep(0.02)
        assert not task_b.done()
        assert sent[1]["operation_id"] != sent[0]["operation_id"]
        # The late ACK for the timed-out cut A resolves B's future, but B's
        # operation-id guard rejects it: A can never advance B's ledger.
        ex.deliver_receipt(late_ack)
        with pytest.raises(RecoveryContractError):
            await task_b
        assert (ex.progress.inputs, ex.progress.sequence, ex.progress.rows) == (0, 0, 0)
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
            await ex._request_commit(3, _Sealed(7), {}, False)
        # The smallest deadline binds (~0.05s), not an invented 120s default.
        assert time.monotonic() - started < 2.0
        assert len(sent) == 1
    asyncio.run(scenario())


def test_receipt_operation_mismatch_is_rejected():
    async def scenario():
        ex, sent = _executor()
        task = asyncio.create_task(ex._request_commit(3, _Sealed(7), {}, False))
        await asyncio.sleep(0.02)
        wrong = build_commit_receipt("different-operation", "commit-1",
                                     sent[0]["sequence"], "acked", {})
        ex.deliver_receipt(wrong)
        with pytest.raises(RecoveryContractError):
            await task
        assert (ex.progress.inputs, ex.progress.sequence, ex.progress.rows) == (0, 0, 0)
    asyncio.run(scenario())


def test_non_acked_status_is_rejected():
    async def scenario():
        ex, sent = _executor()
        task = asyncio.create_task(ex._request_commit(3, _Sealed(7), {}, False))
        await asyncio.sleep(0.02)
        partial = build_commit_receipt(sent[0]["operation_id"], "commit-1",
                                       sent[0]["sequence"], "data_committed", {})
        ex.deliver_receipt(partial)
        with pytest.raises(RecoveryContractError):
            await task
        assert (ex.progress.inputs, ex.progress.sequence, ex.progress.rows) == (0, 0, 0)
    asyncio.run(scenario())
