"""Worker-side graph chain execution (U3/S3, Local multi-worker host).

The ``executor`` seam injected into the shared WorkerRuntime for graph mode. On
Assign it drives run_stream in a dedicated thread under the granted fence: an
ancestor reads the driver-captured source blocks and routes egress into downstream
physical channels; a target reads its input channels (timely dequeue) and stages
terminal output. Each E1 seal requests a durable cut from the driver via a
``CutContribution`` frame and advances only on the receipt. The worker never builds
a commit authority; the driver assembles K6 and commits once (S4 wires full
coverage). U2 WorkerChainExecutor is untouched.
"""
import hashlib
import socket
import threading
import uuid
from concurrent.futures import Future

from ..block_sink import BlockSink
from ..exchange.carriage_local import LocalChannelLink, LocalSegmentStore
from ..exchange.graph_router import GraphInputReader, GraphOutputRouter
from ..exchange.segment_gc import SegmentIndex
from ..graph_wire import validate_cut_contribution
from ..recovery_contract import RecoveryContractError, document
from ..stream import run_stream
from .graph_progress import GraphWorkerProgress


class GraphWorkerExecutor:
    def __init__(self, launch_spec, *, send):
        graph = launch_spec["graph"]
        self._send = send
        self._graph = graph
        self.source_child_id = graph.get("source_child_id")
        self.output_child_id = graph["output_child_id"]
        self.plan_ref = graph["plan_ref"]
        self.sealed_operators = graph.get("sealed_operators")
        self.source_reference = graph.get("source_reference")
        self.source_id = graph["source_id"]
        self.occurrence_id = graph["occurrence_id"]
        self.host_id = graph.get("host_id", "local")
        self.restore = graph.get("restore")
        self._execution_plan = launch_spec.get("execution_plan")
        self.authority = None
        self.progress = None
        self.stats = None
        self.input_reader = None
        self.output_router = None
        self._thread = None
        self._lock = threading.Lock()
        self._pending = {}
        self._cut_commands = []
        self._error = None
        self._done = threading.Event()

    def _load_plan(self):
        import cloudpickle
        from ..chain import verify_loaded_plan
        path = self.authority.directory / self.plan_ref["path"]
        raw = path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != self.plan_ref["sha256"]:
            raise RecoveryContractError("Worker plan artifact digest differs")
        plan = cloudpickle.loads(raw)
        verify_loaded_plan(plan, self.plan_ref, self.sealed_operators)
        return plan

    def _links(self, specs, role):
        links = {}
        for spec in specs:
            if "ray_queue" in spec:
                # Ray carriage (S5): the channel is a named async queue actor; the
                # endpoint reconstructs its link by name. Imported lazily so the
                # Local process worker never loads Ray.
                import ray
                from ..exchange.carriage_ray import RayChannelLink
                links[spec["channel_id"]] = RayChannelLink(ray.get_actor(spec["ray_queue"]), role)
            else:
                sock = socket.socket(fileno=spec["fd"])
                links[spec["channel_id"]] = LocalChannelLink(sock)
        return links

    def _store(self, channel_specs):
        carriage = channel_specs[0].get("carriage", "local") if channel_specs else "local"
        if carriage == "ray":
            from ..exchange.carriage_ray import RayObjectStore
            return RayObjectStore(self.authority.directory, host_id=self.host_id)
        return LocalSegmentStore(self.authority.directory, host_id=self.host_id)

    def start(self, authority, assignment):
        if self._thread is not None:
            return
        self.authority = authority
        self._assignment = assignment
        self._thread = threading.Thread(target=self._run, name="demiflow-graph-chain", daemon=True)
        self._thread.start()

    def _run(self):
        from ..run_context import _CURRENT
        token = _CURRENT.set(self.authority)
        try:
            plan = self._load_plan()
            checkpoint_policy = None
            stage_limits = None
            if self._execution_plan is not None:
                from .layout import load_worker_layout
                _, checkpoint_policy, stage_limits = load_worker_layout(
                    {"execution_plan": self._execution_plan},
                    self.authority, self._assignment)
            graph = self._graph
            prefetch = graph.get("prefetch")
            channel_specs = graph.get("output_channels") or graph.get("input_channels") or []
            store = self._store(channel_specs)
            input_reader = None
            if graph.get("input_channels"):
                specs = graph["input_channels"]
                input_reader = GraphInputReader(
                    specs, self._links(specs, "consumer"), reader_id=specs[0]["reader_id"],
                    reader_domain=store.domain, store=store,
                    max_pending=prefetch["max_pending"] if prefetch else 1024)
                self.input_reader = input_reader
            output_router = None
            route_value = None
            segment_index = None
            exchange_mode = channel_specs[0].get("exchange_mode", "memory_incremental") if channel_specs else "memory_incremental"
            if graph.get("output_channels"):
                from ...planning.graph_channels import RouteTable
                specs = graph["output_channels"]
                table = RouteTable(graph["route_plan"],
                                   key_fields=graph.get("key_fields"),
                                   target_classes=graph.get("target_classes"))
                # The producer owns the live segment index for its egress channels;
                # the driver's cut ack advances the safe point and terminate frees all.
                segment_index = SegmentIndex(self.authority.directory)
                output_router = GraphOutputRouter(
                    specs, self._links(specs, "producer"),
                    producer={"logical_node_id": graph["logical_node_id"],
                              "occurrence_id": self.occurrence_id},
                    route_table=table, depth=graph.get("queue_depth", 256),
                    segment_index=segment_index,
                    initial_ordinal=(self.restore or {}).get("egress_ordinal", 0))
                self.output_router = output_router
                # The egress route_value extractor reads the declared routing keys
                # (RouteTable stores them as read-only instance attributes): hash
                # extracts the key tuple, target extracts the declared business-class
                # column. broadcast/rebalance/forward select by ordinal alone.
                route_table = output_router._route_table
                if route_table._key_fields is not None:
                    key_fields = route_table._key_fields
                    route_value = lambda row: tuple(row[k] for k in key_fields)
                elif route_table._target_classes is not None:
                    target_field = graph.get("target_field")
                    if target_field is None:
                        raise RecoveryContractError("target routing requires a declared target_field")
                    route_value = lambda row: row[target_field]
            source_sink = None
            if self.source_child_id is not None:
                with self.authority.action_scope(self.source_child_id) as source_action:
                    source_sink = BlockSink(source_action, "source", input_capture=True)
            with self.authority.action_scope(self.output_child_id) as output_action:
                self.progress = GraphWorkerProgress(
                    output_action, allocation_id=self._graph["allocation_id"],
                    occurrence_id=self.occurrence_id, source_id=self.source_id,
                    logical_node_id=graph.get("logical_node_id"),
                    source=self.source_reference, source_sink=source_sink,
                    input_reader=input_reader, output_router=output_router,
                    route_value=route_value,
                    restore=self.restore, request_contribution=self._request_contribution,
                    stop_requested=self._stop_requested, sink_id=self._graph.get("sink_id", "output"),
                    exchange_mode=exchange_mode, segment_store=store,
                    cut_namespace=self._graph.get("cut_namespace"),
                    append_slots=prefetch["append_slots"] if prefetch else 64,
                    checkpoint_policy=checkpoint_policy)
                if input_reader is not None:
                    input_reader.set_cut_hook(
                        lambda checkpoint, consumed, final: self.progress.align_cut(
                            checkpoint, consumed, final=final))
                # The driver may name the first cut before this thread built progress;
                # apply any buffered command now so no checkpoint is lost.
                self._apply_cut_commands()
                # Targets feed row-by-row so every pre-cut row reaches the pipeline
                # before the reader detects barrier alignment (the chunked feed drains
                # a whole block before feeding any row, which would deadlock a reader
                # that waits at alignment). The ancestor's fixed source never blocks.
                feed_batch = 1 if input_reader is not None else (
                    prefetch["source_batch_size"] if prefetch else 256)
                self.stats = run_stream(self.progress.source_rows(), plan,
                                        consume=self.progress.append, progress=self.progress,
                                        source_batch_size=feed_batch,
                                        stage_limits=stage_limits)
            if output_router is not None:
                output_router.end_of_input()
                if exchange_mode == "finite_sealed":
                    # finite_sealed seals the scope after EOI: the consumer releases
                    # reads only once this completion declaration (membership) arrives.
                    # The manifest commits to the sealed egress summary; the frozen
                    # validate_seal/seal_allowed run read-only inside ChannelProducer.
                    seal_scope = graph["route_plan"]["seal_scope"]
                    manifest = {"scope_id": seal_scope["scope_id"],
                                "generation": seal_scope["generation"],
                                "logical_node_id": graph["logical_node_id"],
                                "occurrence_id": self.occurrence_id,
                                "sent_records": output_router.sent_records()}
                    manifest_ref = {"sha256": hashlib.sha256(document(manifest)).hexdigest()}
                    output_router.seal(seal_scope=seal_scope, manifest_ref=manifest_ref,
                                       producer_statuses=["success"])
        except BaseException as exc:
            self._error = exc
            with self._lock:
                for future in self._pending.values():
                    if not future.done():
                        future.set_exception(exc)
        finally:
            _CURRENT.reset(token)
            self._done.set()

    def _stop_requested(self):
        return not self.authority.active or self.authority._local_cancel

    def _request_contribution(self, contribution, sealed, states, final):
        validate_cut_contribution(contribution)
        operation_id = uuid.uuid4().hex
        future = Future()
        with self._lock:
            self._pending[operation_id] = future
        self._send({"kind": "CutContribution", "operation_id": operation_id,
                    "contribution": contribution})
        receipt = future.result()
        if receipt.get("status") != "acked":
            raise RecoveryContractError("Worker cut was not durably acknowledged")
        self.progress.sequence += 1
        # inputs is the confirmed SOURCE ordinal (the feed skips source_ordinal <
        # inputs on recovery), never the input-channel cursor: the chunked feed
        # drains the reader ahead of the cut, so a cursor here would skip un-fed rows.
        # The ack also releases a target reader waiting at barrier alignment.
        self.progress.note_contribution_acked(states["stream-progress"]["inputs"])
        if self.output_router is not None:
            # The durable ack advances the egress GC safe point from the cut's
            # consumer offsets and frees segments every reader has consumed past.
            self.output_router.advance_gc(receipt.get("channel_cuts", []))
        with self._lock:
            self._pending.pop(operation_id, None)
        return receipt

    def deliver_receipt(self, operation_id, receipt):
        with self._lock:
            future = self._pending.get(operation_id)
        if future is not None and not future.done():
            future.set_result(receipt)

    def checkpoint(self, cut):
        # K3 CheckpointRequest converges to the same E1 pending_cut; the additive
        # graph_checkpoint wire (graph_host) carries the checkpoint_id/final flag.
        if self.progress is not None:
            self.progress.pending_cut = cut["producer_sequence_bound"]

    def pending_contributions(self):
        with self._lock:
            return set(self._pending)

    def request_cut(self, command):
        with self._lock:
            self._cut_commands.append(command)
        self._apply_cut_commands()

    def _apply_cut_commands(self):
        with self._lock:
            if self.progress is None:
                return
            commands = self._cut_commands
            self._cut_commands = []
            progress = self.progress
        for command in commands:
            if self.input_reader is not None:
                # Targets cut on barrier alignment, not a source ordinal; the command
                # only names the checkpoint and its finality.
                progress.note_driver_cut(command["checkpoint_id"], final=command["final"])
            else:
                progress.request_cut(command["checkpoint_id"],
                                     command["cut"]["producer_sequence_bound"],
                                     epoch=command["epoch"], final=command["final"])

    def drain(self, payload):
        deadline_s = max(0, payload.get("deadline_ms", 0)) / 1000.0
        self._join(deadline_s)
        if self._error is not None:
            raise self._error
        return {"release_hint": "DRAINED",
                "rows": self.progress.rows if self.progress else 0,
                "sequence": self.progress.sequence if self.progress else 0}

    def cancel(self, payload):
        if self.authority is not None:
            self.authority.cancel_local()
        grace_s = self.authority.limits["cancel_grace_s"] if self.authority is not None else 10
        self._join(grace_s)
        if self._thread is not None and self._thread.is_alive():
            if self.authority is not None:
                self.authority.quarantine_worker(self._thread)
            raise RecoveryContractError("Graph worker exceeded cancel grace; owned writer isolated")

    def _join(self, timeout_s):
        if self._thread is None:
            return
        self._thread.join(timeout=max(0.0, timeout_s))

    def join(self, timeout_s=None):
        self._join(timeout_s if timeout_s is not None else 30)
        if self.output_router is not None:
            if self.output_router.segment_index is not None:
                # Terminate (drain/cancel): the channel data plane is no longer
                # needed; committed output and the source capture live in BlockSink.
                self.output_router.segment_index.reclaim_all(reason="drained")
            self.output_router.close()
        if self.input_reader is not None:
            self.input_reader.close()

    @property
    def done(self):
        return self._done.is_set()

    @property
    def error(self):
        return self._error