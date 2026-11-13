"""Worker-side stream progress (U2/S3).

Reuses the driver's stage binding/alignment/snapshot verbatim via
StreamStageProgress. The commit terminal stays driver-only: seal returns a thunk
that delegates to an injected ``request_commit`` coroutine. Wiring that callback
to the CommitCut/CommitReceipt handshake over the exchange channel is S4; the
worker never constructs a CommitCoordinator or owns result-commit authority.
"""
import asyncio

from ..block_sink import BlockSink
from ..operator_state import OperatorStates
from ..recovery_contract import RecoveryContractError
from ..state_artifacts import StateArtifacts
from ..stream_materialize import StreamStageProgress


class WorkerStreamProgress(StreamStageProgress):
    def __init__(self, action, source, *, source_sink=None, object_specs=None,
                 restore=None, request_commit=None, on_checkpoint=None,
                 stop_requested=None, checkpoint_policy=None):
        self.action, self.run = action, action.context
        self.source = source
        self.source_sink = (source_sink if source_sink is not None
                            else BlockSink(action, 'source', staging_only=True))
        self.sink = BlockSink(action, staging_only=True)
        self.row_sink = self.sink
        self.has_objects = bool(object_specs)
        self.checkpoint_policy = checkpoint_policy
        self.batch_rows = (checkpoint_policy.checkpoint_rows if checkpoint_policy is not None
                           else self.run.limits["checkpoint_rows"])
        if object_specs:
            from ..object_sink import ObjectRowsSink
            self.sink = ObjectRowsSink(action, object_specs=object_specs)
            self.row_sink = self.sink.rows
            action.object_sinks = self.sink.object_sinks
        self.row_sink.staging_only = True
        self.on_checkpoint = on_checkpoint
        self.stop_requested = stop_requested or (lambda: False)
        self._request_commit = request_commit
        # Restore comes from driver-provided refs (Restore message), never from a
        # worker-side journal commit read; the driver remains the commit authority.
        self.inputs = self.rows = self.sequence = 0
        self.pending_cut = None
        restored = {}
        if restore is not None:
            position = restore["position"]
            if position["source_id"] != self.source["source_id"]:
                raise RecoveryContractError("Stream source progress differs")
            self.inputs = position["next_ordinal"]
            self.sequence = restore["sequence"]
            self.rows = restore["rows"]
            self.pending_cut = restore.get("pending_cut")
            restored = {key: StateArtifacts(self.run).read(ref)
                        for key, ref in restore["state"].items()}
        self.state = OperatorStates(self.run, restored)
        self.ordinal = 0
        self.complete = False
        self.append_lock = asyncio.Lock()
        self.append_slots = asyncio.Semaphore(64)
        self.pending_appends = {}
        self.direct_owners = []
        self.group_ends = []
        self.buffer_sinks = {}
        self.admitted_rows = self.rows

    def seal(self, barrier):
        from copy import copy
        if barrier.final and barrier.next_ordinal != self.source['data']['rows']:
            raise RecoveryContractError('Final barrier has an incomplete source')
        # Terminal workers have all arrived. Transfer buffers before any resumes;
        # the committing participant never sees subsequent append/reset calls.
        if self.has_objects:
            sealed = copy(self.sink)
            sealed.rows = self.row_sink.detach()
            sealed.object_sinks = barrier.objects
            sealed.objects = next(iter(sealed.object_sinks.values()))
        else:
            sealed = self.sink.detach()
        barrier.states['stream-progress'] = dict(rows=self.rows + sealed.rows.rows if self.has_objects else self.rows + sealed.rows,
                                                final=barrier.final, inputs=barrier.next_ordinal)
        if self._request_commit is None:
            raise RecoveryContractError('Worker commit request channel is not wired')
        return lambda: self._request_commit(barrier.next_ordinal, sealed, barrier.states, barrier.final)