"""Decoupled same-host graph data plane (U3/S3, exit gate 1).

One K5 protocol over 1:1 physical channels with **per-channel independence**: each
output channel owns a bounded queue, a sender thread and a credit-feedback reader,
so a slow reader exhausts only its own channel credit and never fences or stalls
sibling channels of the same producer. The shared ancestor computes each record
once; routing replicates it at egress (broadcast/fan_out) or selects one target
(forward/target/hash/rebalance). Records dequeue timely: incremental consumption
is decoupled from the global cut, the consumer replenishes credit as it reads.

Frames are the bootstrap length-prefixed strict-JSON format (carriage_local). The
producer writes data/barrier/eoi frames; the consumer writes credit frames back on
the same full-duplex socket. Large payloads ride DataRefs (S2), never the socket.
This is provider-independent protocol logic; Ray actor carriage is S5.
"""
import queue
import threading
import uuid

from ..contracts.errors import ProtocolError
from ..contracts.exchange import DATA_REF_SCHEMA
from ..recovery_contract import RecoveryContractError, identifier
from .carriage_local import expand_records
from .graph_channel import GraphChannelConsumer, GraphChannelProducer, StorageReservation

_CLOSE = object()


class _OutputChannel:
    """One producer endpoint: bounded queue + independent sender + credit reader."""

    def __init__(self, spec, link, *, producer, depth, segment_index=None):
        self.channel_id = identifier(spec["channel_id"])
        self._link = link
        self._producer = producer
        self._mode = spec["exchange_mode"]
        self._segment_index = segment_index
        storage = (StorageReservation(spec["storage_bytes"])
                   if self._mode == "finite_sealed" else None)
        self.producer = GraphChannelProducer(
            self.channel_id, reader_id=spec["reader_id"], exchange_mode=self._mode,
            route_epoch=spec.get("route_epoch", 0), storage=storage)
        self._queue = queue.Queue(maxsize=depth)
        self._credit = threading.Event()
        self._closed = threading.Event()
        self._error = None
        self.sent_records = 0
        self._sender = threading.Thread(target=self._send_loop, name="graph-send-" + self.channel_id[:8], daemon=True)
        self._reader = threading.Thread(target=self._credit_loop, name="graph-credit-" + self.channel_id[:8], daemon=True)
        self._sender.start()
        self._reader.start()

    def offer(self, item):
        if self._closed.is_set():
            raise ProtocolError("FENCED", "output channel is closed")
        self._queue.put(item)

    def _send_loop(self):
        try:
            while True:
                item = self._queue.get()
                if item is _CLOSE:
                    return
                kind = item[0]
                if kind == "data":
                    envelope = self._charge_data(item[1])
                    self._link.send({"kind": "data", "sequence": envelope["sequence"],
                                     "envelope": envelope})
                    self.sent_records += envelope["bounds"]["rows"]
                    if self._segment_index is not None:
                        # Track the live segment->(channel, sequence) reference at
                        # send time so GC frees a block only once every reader of
                        # every channel carrying it has consumed past this sequence.
                        for record in envelope["records"]:
                            payload = record.get("payload")
                            if type(payload) is dict and payload.get("schema_version") == DATA_REF_SCHEMA:
                                self._segment_index.register(
                                    payload["sha256"], [payload["location"]],
                                    self.channel_id, envelope["sequence"])
                elif kind == "barrier":
                    # Issue the barrier sequence here, in send order, so it always
                    # advances past every pre-cut data frame charged before it. The
                    # frozen ChannelCursor/InputGate reject out-of-order sequences;
                    # issuing at offer time (another thread) would race ahead of data
                    # still waiting for credit in this queue.
                    value = self.producer.barrier(
                        item[1]["checkpoint_id"], epoch=item[1]["epoch"],
                        topology_revision=item[1]["topology_revision"],
                        producer_sequence_bound=item[1]["producer_sequence_bound"])
                    self._link.send({"kind": "barrier", **value})
                elif kind == "eoi":
                    self._link.send({"kind": "eoi", **self.producer.end_of_input()})
                elif kind == "seal":
                    self._link.send({"kind": "seal", **self.producer.seal(**item[1])})
        except BaseException as exc:
            if not self._closed.is_set():
                self._error = exc
            self._closed.set()
            self._credit.set()

    def _charge_data(self, records):
        while True:
            if self._closed.is_set():
                raise ProtocolError("FENCED", "output channel closed while awaiting credit")
            try:
                return self.producer.data_envelope(records, producer=self._producer)
            except ProtocolError as exc:
                if exc.code != "CAPACITY_PENDING":
                    raise
                # A slow reader backpressures only this channel; siblings keep
                # flowing on their own credit. Wait for the consumer's replenish.
                self._credit.wait(timeout=0.05)
                self._credit.clear()

    def _credit_loop(self):
        try:
            while True:
                frame = self._link.recv()
                if frame is None:
                    self._closed.set()
                    self._credit.set()
                    return
                if frame.get("kind") == "credit":
                    self.producer.grant_credit(frame["credit"])
                    self._credit.set()
        except BaseException as exc:
            if not self._closed.is_set():
                self._error = exc
            self._closed.set()
            self._credit.set()

    def close(self, timeout=5.0):
        self._closed.set()
        self._credit.set()
        try:
            self._queue.put_nowait(_CLOSE)
        except queue.Full:
            pass
        # Drain the sender first (it exits on _CLOSE), then close the socket to
        # unblock the credit reader's recv; joining a recv-blocked thread before
        # closing would burn the whole timeout per channel.
        self._sender.join(timeout=timeout)
        self._link.shutdown()
        self._link.close()
        self._reader.join(timeout=timeout)
        if self._error is not None:
            raise self._error


class GraphOutputRouter:
    """Producer egress: route once, replicate/select into independent channels.

    ``route_table`` decides the physical channel ids a record enters; ``links``
    maps each physical channel id to its connected LocalChannelLink. The router
    never blocks a fast channel on a slow one's credit: each channel sender waits
    on its own credit event only.
    """

    def __init__(self, channel_specs, links, *, producer, route_table,
                 producer_partition="p0", depth=64, segment_index=None):
        self._route_table = route_table
        self._producer_partition = producer_partition
        self._segment_index = segment_index
        self._channels = {}
        for spec in channel_specs:
            link = links[spec["channel_id"]]
            self._channels[spec["channel_id"]] = _OutputChannel(
                spec, link, producer=producer, depth=depth, segment_index=segment_index)
        if set(self._channels) != set(route_table.channel_ids):
            raise RecoveryContractError("Router links do not cover the route table channels")
        self._ordinal = 0
        self._closed = False

    @property
    def channels(self):
        return dict(self._channels)

    @property
    def segment_index(self):
        return self._segment_index

    def advance_gc(self, channel_cuts):
        """The driver's cut ack advances each channel's safe point from the acked
        consumer offsets (== the reader cursor at alignment for 1:1 channels), then
        frees segments every reader has consumed past. Returns reclaimed ids."""
        if self._segment_index is None:
            return []
        for cut in channel_cuts:
            channel_id = cut["channel_id"]
            if channel_id not in self._channels:
                continue
            for offset in cut["consumer_offsets"].values():
                self._segment_index.note_cursor(channel_id, offset - 1)
                self._segment_index.note_acked(channel_id, offset)
        return self._segment_index.reclaim()

    def route(self, records, *, route_value=None):
        if self._closed:
            raise ProtocolError("FENCED", "router is closed")
        targets = self._route_table.targets(self._producer_partition, self._ordinal,
                                            route_value=route_value)
        self._ordinal += 1
        for channel_id in targets:
            self._channels[channel_id].offer(("data", records))
        return targets

    def barrier(self, checkpoint_id, *, epoch, topology_revision=0, producer_sequence_bound=None):
        spec = {"checkpoint_id": checkpoint_id, "epoch": epoch,
                "topology_revision": topology_revision,
                "producer_sequence_bound": producer_sequence_bound}
        for channel in self._channels.values():
            channel.offer(("barrier", spec))

    def end_of_input(self):
        for channel in self._channels.values():
            channel.offer(("eoi",))

    def seal(self, *, seal_scope, manifest_ref, producer_statuses):
        spec = {"seal_scope": seal_scope, "manifest_ref": manifest_ref,
                "producer_statuses": producer_statuses}
        for channel in self._channels.values():
            channel.offer(("seal", spec))

    def sent_records(self):
        return {channel_id: channel.sent_records for channel_id, channel in self._channels.items()}

    def close(self, timeout=5.0):
        if self._closed:
            return
        self._closed = True
        errors = []
        for channel in self._channels.values():
            try:
                channel.close(timeout=timeout)
            except BaseException as exc:
                errors.append(exc)
        if errors:
            raise errors[0]


class _InputChannel:
    """One consumer endpoint: reader thread feeds the InputGate; credit flows back."""

    def __init__(self, channel_id, link, consumer, *, budget_rows, budget_bytes):
        self.channel_id = channel_id
        self._link = link
        self._consumer = consumer
        self._budget_rows = budget_rows
        self._budget_bytes = budget_bytes
        self._changed = threading.Event()
        self._closed = threading.Event()
        self._error = None
        self.eoi = False
        self._reader = threading.Thread(target=self._recv_loop, name="graph-recv-" + channel_id[:8], daemon=True)
        self._reader.start()

    def grant_initial(self):
        credit = self._consumer.grant_credit(
            channel_id=self.channel_id, rows=self._budget_rows, size_bytes=self._budget_bytes,
            operation_id=uuid.uuid4().hex)
        self._link.send({"kind": "credit", "credit": credit})

    def _recv_loop(self):
        try:
            while True:
                frame = self._link.recv()
                if frame is None:
                    self._closed.set()
                    self._changed.set()
                    return
                kind = frame.get("kind")
                if kind == "eoi":
                    self.eoi = True
                self._consumer.push(self.channel_id, frame)
                self._changed.set()
        except BaseException as exc:
            if not self._closed.is_set():
                self._error = exc
            self._closed.set()
            self._changed.set()

    def replenish(self, bounds):
        credit = self._consumer.grant_credit(
            channel_id=self.channel_id, rows=bounds["rows"], size_bytes=bounds["bytes"],
            operation_id=uuid.uuid4().hex)
        self._link.send({"kind": "credit", "credit": credit})

    def close(self, timeout=5.0):
        self._closed.set()
        self._changed.set()
        self._link.shutdown()
        self._link.close()
        self._reader.join(timeout=timeout)
        if self._error is not None:
            raise self._error


class GraphInputReader:
    """Consumer ingress over its input physical channels (multi-input cut ready).

    ``rows`` yields business rows as they arrive (timely dequeue), replenishing
    producer credit per consumed envelope. Barriers stay in the frozen InputGate
    until aligned; ``aligned``/``release``/``cursors`` expose the cut surface S4
    wires into the global contribution. ``store`` expands DataRef payloads by
    direct same-host read (large payloads never transit the socket).
    """

    def __init__(self, channel_specs, links, *, reader_id, reader_domain, store,
                 route_epoch=0, max_pending=1024):
        self._store = store
        self._reader_id = identifier(reader_id)
        ids = [spec["channel_id"] for spec in channel_specs]
        self._consumer = GraphChannelConsumer(
            ids, reader_id=reader_id, reader_domain=reader_domain,
            route_epoch=route_epoch, max_pending=max_pending)
        self._channels = {}
        for spec in channel_specs:
            budget = spec["credit_budget"]
            self._channels[spec["channel_id"]] = _InputChannel(
                spec["channel_id"], links[spec["channel_id"]], self._consumer,
                budget_rows=budget["rows"], budget_bytes=budget["bytes"])
        self._ids = ids
        self._started = False
        self.consumed_rows = 0
        self._cut_hook = None

    def set_cut_hook(self, hook):
        """hook(checkpoint_id, consumed_rows, final) -> event waited before release."""
        self._cut_hook = hook

    def start(self):
        if self._started:
            return
        self._started = True
        for channel in self._channels.values():
            channel.grant_initial()

    @property
    def consumer(self):
        return self._consumer

    @property
    def reader_id(self):
        return self._reader_id

    @property
    def cursors(self):
        return self._consumer.cursors

    def aligned(self, checkpoint_id):
        return self._consumer.aligned(checkpoint_id)

    def release(self, checkpoint_id):
        return self._consumer.release(checkpoint_id)

    def barrier_effect(self, channel_id):
        return self._consumer.barrier_effect(channel_id)

    def _all_eoi(self):
        return all(channel.eoi or channel._closed.is_set() for channel in self._channels.values())

    def rows(self):
        self.start()
        import time

        def emit(channel_id, event):
            envelope = event["envelope"]
            rows = expand_records(envelope["records"], self._store)
            self._channels[channel_id].replenish(envelope["bounds"])
            self.consumed_rows += len(rows)
            return rows

        while True:
            progressed = False
            for channel_id in self._ids:
                event = self._consumer.consume(channel_id)
                if event is None:
                    continue
                progressed = True
                for row in emit(channel_id, event):
                    yield row
            if progressed:
                continue
            if self._cut_hook is not None:
                checkpoint = self._consumer.pending_checkpoint()
                if checkpoint is not None:
                    # Every input channel holds the same barrier and its pre-cut data
                    # is fully consumed (frozen InputGate alignment). Cut here, wait for
                    # the driver's single durable commit, then release post-cut data.
                    final = self._all_eoi()
                    done = self._cut_hook(checkpoint, self.consumed_rows, final)
                    done.wait()
                    self._consumer.release(checkpoint)
                    if final:
                        return
                    continue
            if self._all_eoi():
                # EOI is the last frame, but data may have been pushed between the
                # consume pass and this check; yield that tail rather than discard
                # it, and only finish when a full pass finds nothing left.
                tail = False
                for channel_id in self._ids:
                    event = self._consumer.consume(channel_id)
                    if event is None:
                        continue
                    tail = True
                    for row in emit(channel_id, event):
                        yield row
                if not tail:
                    return
                continue
            if all(channel._closed.is_set() for channel in self._channels.values()):
                return
            time.sleep(0.001)

    def close(self, timeout=5.0):
        self._consumer.unsubscribe()
        errors = []
        for channel in self._channels.values():
            try:
                channel.close(timeout=timeout)
            except BaseException as exc:
                errors.append(exc)
        if errors:
            raise errors[0]