"""Worker-side graph progress (U3/S3).

Reuses the driver's stage binding/alignment/snapshot verbatim via
StreamStageProgress (U2 WorkerStreamProgress stays untouched). Two roles share one
class:

* **ancestor** — reads the driver-captured fixed source, runs the shared stages
  once, and routes each output record to its downstream physical channels through
  an injected GraphOutputRouter (egress replication, not re-execution).
* **target** — reads its input physical channels through a GraphInputReader
  (timely dequeue), runs its stages, and stages terminal output under the granted
  fence. ``source`` is None so E1 never auto-finals from a fixed row count;
  finality arrives with input EOI / the driver's final cut.

The commit terminal stays driver-only: seal returns a thunk that delegates to an
injected ``request_contribution`` callback carrying a K6-shaped cut fragment. The
worker never constructs a CommitCoordinator or owns result-commit authority.
"""
import asyncio
import threading
import uuid

from ..block_sink import BlockSink
from ..effect_keys import collect_inflight
from ..exchange.carriage_local import inline_records, ref_records
from ..operator_state import OperatorStates
from ..recovery_contract import RecoveryContractError, identity
from ..state_artifacts import StateArtifacts
from ..stream_materialize import StreamStageProgress


class GraphWorkerProgress(StreamStageProgress):
    def __init__(self, action, *, allocation_id, occurrence_id, source_id,
                 source=None, source_sink=None, input_reader=None, output_router=None,
                 route_value=None, restore=None, request_contribution=None,
                 stop_requested=None, sink_id="output", exchange_mode="memory_incremental",
                 segment_store=None, cut_namespace=None, append_slots=64,
                 logical_node_id=None, checkpoint_policy=None):
        self.action, self.run = action, action.context
        self.allocation_id = allocation_id
        self.occurrence_id = occurrence_id
        self.logical_node_id = logical_node_id
        self.source_id = source_id
        self._cut_namespace = cut_namespace
        self.source = source
        self.input_reader = input_reader
        self.output_router = output_router
        self._exchange_mode = exchange_mode
        self._segment_store = segment_store
        self._route_value = route_value or (lambda row: None)
        self.source_sink = source_sink
        self.checkpoint_policy = checkpoint_policy
        self.sink = BlockSink(action, sink_id, staging_only=True)
        self.row_sink = self.sink
        self.has_objects = False
        # Graph cuts are driver/alignment-driven, never row-count auto-triggered.
        self.batch_rows = 2 ** 62
        self.stop_requested = stop_requested or (lambda: False)
        self._request_contribution = request_contribution
        # Restore comes only from driver-provided refs; the driver stays the
        # commit authority. A graph worker never reads a journal commit itself.
        self.inputs = self.rows = self.sequence = 0
        self.pending_cut = None
        self._cut_checkpoint_id = None
        self._cut_epoch = 0
        self._final_cut = False
        self._final_done = False
        self._cut_done = None
        self._control = None
        self._driver_finals = {}
        restored = {}
        if restore is not None:
            self.inputs = restore.get("inputs", 0)
            self.sequence = restore.get("sequence", 0)
            self.rows = restore.get("rows", 0)
            self.pending_cut = restore.get("pending_cut")
            # The driver hands back one combined per-participant state artifact
            # (state_schema_version 1); its value is the node-i keyed stage-state
            # mapping that OperatorStates/bind_stages restore by their local keys.
            for ref in restore.get("state", {}).values():
                value = StateArtifacts(self.run).read(ref)
                restored.update(value)
        self.state = OperatorStates(self.run, restored)
        self.ordinal = 0
        self.complete = False
        self.append_lock = asyncio.Lock()
        self.append_slots = asyncio.Semaphore(append_slots)
        self.pending_appends = {}
        self.direct_owners = []
        self.group_ends = []
        self.buffer_sinks = {}
        self.admitted_rows = self.rows
        # The egress routing ordinal resumes at the acked cut (driver hands it back
        # in the runtime restore dict, separate from stage-state) so rebalance
        # partition assignment continues instead of resetting to 0 on restore.
        self._egress_ordinal = restore.get("egress_ordinal", 0) if restore else 0
        # Target permanent seal is gated by actual input EOF (all channels drained to
        # EOI), never by the driver's provisional final hint: the ancestor sends EOI
        # only after the final cut is acked, so the hint may name a final-bound cut
        # that the budget forced early (ordinal < source_rows).
        self._input_exhausted = False

    def source_rows(self):
        if self.input_reader is not None:
            return self._target_rows()
        return self.source_sink.iter_rows(self.source["data"])

    def _target_rows(self):
        yield from self.input_reader.rows()
        # The reader returns only after every input channel reached EOI and drained
        # its tail; this is the target's true EOF, the sole authority for sealing it.
        self._input_exhausted = True

    def _bind_checkpoint_control(self, control):
        # Additive run_stream seam: graph cuts wake the E1 control loop precisely at
        # the pending bound instead of waiting on its interval timer. U2 progress
        # has no such hook, so its path is unchanged. Capture the loop here (this
        # runs in the run_stream event-loop thread) so a cut named from a foreign
        # thread (target reader generator / driver command) wakes it thread-safely.
        self._control = control
        self._control_loop = asyncio.get_running_loop()

    def _wake_control(self):
        if self._control is not None:
            # align_cut/request_cut run off the event loop (reader to_thread worker,
            # driver control thread). asyncio.Event.set() from a foreign thread does
            # not wake the selector, so the cut would stall until the interval timer;
            # call_soon_threadsafe both wakes the loop and is safe from any thread.
            self._control_loop.call_soon_threadsafe(self._control.changed.set)

    def select(self, row):
        key = super().select(row)
        if self.pending_cut is not None and self.ordinal >= self.pending_cut:
            self._wake_control()
        return key

    def request_cut(self, checkpoint_id, producer_sequence_bound, *, epoch=0, final=False):
        """Driver graph_checkpoint converges the same E1 pending_cut path (D3)."""
        self._cut_checkpoint_id = checkpoint_id
        self._cut_epoch = epoch
        self.pending_cut = producer_sequence_bound
        self._final_cut = final
        self._wake_control()

    def note_driver_cut(self, checkpoint_id, *, final=False):
        """Target: the driver names the cut and its finality; alignment drives the
        ordinal. Recording finality here avoids racing the ancestor's trailing EOI
        and avoids waiting on post-cut data (which the ancestor only sends after the
        commit ack, so waiting would deadlock the cut)."""
        self._driver_finals[checkpoint_id] = bool(final)

    def align_cut(self, checkpoint_id, consumed_rows, *, final=False):
        """Target reader: InputGate aligned on a barrier; cut at this input ordinal.

        Returns the event the reader waits on; the commit receipt sets it so the
        reader releases post-cut data only after the driver's single durable commit.
        """
        self._cut_checkpoint_id = checkpoint_id
        self._final_cut = self._driver_finals.get(checkpoint_id, final)
        self.pending_cut = consumed_rows
        self._cut_done = threading.Event()
        self._wake_control()
        return self._cut_done

    async def append(self, row):
        if self.stop_requested():
            raise RecoveryContractError("Graph worker stopped")
        if self.output_router is not None:
            if self._segment_store is not None:
                # Every mode rides pickle-4 segment files (only a K4 DataRef crosses
                # the socket, D3): this preserves business-row types (tuple/bytes) that
                # the strict-JSON inline path would silently coerce or reject.
                # memory_incremental uses ephemeral blocks per its declared policy;
                # durable_incremental durable ones. The consumer reads the segment (S2).
                durability = "durable" if self._exchange_mode == "durable_incremental" else "ephemeral"
                _, refs = self._segment_store.write([row], scope_ref=self.occurrence_id,
                                                    durability=durability)
                records = ref_records(refs, occurrence_id=self.occurrence_id)
            else:
                records = inline_records([row], source_id=self.source_id,
                                         occurrence_id=self.occurrence_id,
                                         start_ordinal=self._egress_ordinal)
            self._egress_ordinal += 1
            self.output_router.route(records, route_value=self._route_value(row))
            self.rows += 1
            return
        await super().append(row)

    def _channel_cuts(self):
        cuts = []
        if self.input_reader is not None:
            for channel_id, cursor in self.input_reader.cursors.items():
                bound = max(cursor + 1, 0)
                cuts.append({"channel_id": channel_id,
                             "producer_cut_sequence": bound,
                             "consumer_offsets": {self.input_reader.reader_id: bound}})
        if self.output_router is not None:
            for channel_id, channel in self.output_router.channels.items():
                cuts.append({"channel_id": channel_id,
                             "producer_cut_sequence": max(channel.producer.next_sequence - 1, 0),
                             "consumer_offsets": {}})
        return cuts

    def seal(self, barrier):
        if self._final_done or self._input_exhausted:
            return lambda: None
        real_cut = (self._cut_checkpoint_id is not None or barrier.final)
        if not real_cut:
            # Timer/budget-driven E1 trigger with no driver cut named: snapshot nothing,
            # publish nothing, keep staged rows. Only a driver-named cut (or true EOF,
            # barrier.final) publishes. The sticky final hint must NOT force a publish
            # here: a budget cut racing ahead of the driver's retry command would ride
            # the ordinal-derived fallback id and mismatch the bound-named cut group.
            return lambda: None
        if self.source is not None:
            # Ancestor reads the fixed source: actual EOF is the sole finality authority
            # (mirrors the chain U2 hard gate final <=> next_ordinal == source rows). The
            # driver's final hint never seals a budget-forced early cut prematurely.
            barrier.final = barrier.next_ordinal == self.source["data"]["rows"]
            contribution_final = barrier.final
        else:
            # Target: barrier.final drives the control-loop finish (stream_barrier) and
            # the permanent seal, so it must reflect TRUE input EOF (_input_exhausted:
            # every channel drained to EOI), never the driver's provisional hint. A
            # budget-forced early cut at a final-named bound still has rows to consume and
            # must not stop the target. The hint drives only the contribution's
            # descriptive final field (final_arrival), provisional per HC1.
            contribution_final = bool(barrier.final or self._final_cut)
            barrier.final = self._input_exhausted
        sealed = self.sink.detach()
        barrier.states["stream-progress"] = dict(rows=self.rows + sealed.rows,
                                                 final=barrier.final, inputs=barrier.next_ordinal)
        if self._request_contribution is None:
            raise RecoveryContractError("Worker contribution channel is not wired")
        if self._cut_checkpoint_id is not None:
            checkpoint_id = self._cut_checkpoint_id
        elif self._cut_namespace is not None:
            # E1 can auto-final at source EOF (control.finish) before the driver's
            # final cut command names the checkpoint — a race actor-method latency
            # exposes on Ray. Derive the SAME id the driver schedules for this bound
            # (identity([action, "graph-cut", ordinal])) so the contribution still
            # matches the driver's cut group instead of a random fallback id.
            checkpoint_id = identity([self._cut_namespace, "graph-cut", barrier.next_ordinal])
        else:
            checkpoint_id = identity(
                [self.allocation_id, "ck", barrier.next_ordinal, uuid.uuid4().hex])
        if self.output_router is not None:
            # Inject the K5 barrier after the pre-cut data already routed (E1 seal
            # runs once every pre-cut row traversed the pipeline). FIFO channel queues
            # keep the barrier ahead of any post-cut data routed later.
            self.output_router.barrier(checkpoint_id, epoch=self._cut_epoch, topology_revision=0)
        descriptor = sealed.descriptor() if self.output_router is None else None
        # Only the ancestor reads a fixed source; targets consume input channels
        # (covered by channel_cuts). The confirmed source ordinal rides K6-shaped
        # source_progress so the driver assembles full coverage and commits once.
        source_progress = ([{"source_id": self.source_id, "partition_id": "p0",
                             "position": {"next_ordinal": barrier.next_ordinal}}]
                           if self.source is not None else [])
        # In-flight external effects ride the existing K6-shaped inflight field:
        # the worker's native effect journal (LLM/embedding) is the one durable
        # authority; known_completed reuse via effect_ref, unknown never replay.
        # Pure-compute workers have no journal and contribute nothing (byte-neutral).
        contribution = {
            "schema_version": "demiflow.cut_contribution.v1",
            "checkpoint_id": checkpoint_id, "allocation_id": self.allocation_id,
            "channel_cuts": self._channel_cuts(), "operator_states": [],
            "inflight": collect_inflight(getattr(self.run, "_native_prompt_journal", None)),
            "output_descriptor": descriptor,
            "source_progress": source_progress, "final": contribution_final}
        states = dict(barrier.states)
        self._cut_checkpoint_id = None
        if barrier.final and self.source is not None:
            # Ancestor seals permanently at actual EOF; the target's permanent seal is
            # deferred to _input_exhausted (input EOI), never the provisional hint.
            self._final_done = True

        def publish():
            # Persist this worker's stage snapshots under its own occurrence key
            # (namespaced against sibling workers' identical node-i keys) and ride
            # the existing cut_contribution operator_states field as a K6 state_ref.
            worker_states = {k: v for k, v in states.items() if k != "stream-progress"}
            payload = dict(worker_states)
            if self.output_router is not None:
                # The ancestor's egress ordinal persists even when stage state is empty
                # (pure-compute), so rebalance routing resumes at the acked ordinal. The
                # reserved key rides the artifact VALUE; the driver extracts it into the
                # graph-progress summary and strips it from the committed stage-state so
                # OperatorStates/bind_stages local-key restore stays unpolluted.
                payload["__egress_ordinal__"] = self._egress_ordinal
            if payload and self.logical_node_id is not None:
                ref = StateArtifacts(self.run).put(self.occurrence_id, payload)
                contribution["operator_states"] = [{
                    "logical_node_id": self.logical_node_id,
                    "occurrence_id": self.occurrence_id,
                    "state_ref": ref, "state_schema_version": 1}]
            return self._request_contribution(contribution, sealed, states, barrier.final)

        return publish

    def note_contribution_acked(self, inputs):
        self.inputs = inputs
        if self._cut_done is not None:
            self._cut_done.set()