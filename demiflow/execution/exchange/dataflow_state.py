"""Channel-set and routing state for the v3 exchange (module C, lane X).

ChannelSetState is the single worker-local ledger for every channel the worker
owns: per-connection credit, the frozen K4 sequence issuer, bounded buffers,
reader frontiers, EOI/seal/tombstone facts and the live segment index used by
GC. RoutingCounters holds the recoverable per-channel routing ordinal. Both
live in the worker's serial routing context; a lock keeps ordinal/sequence
issuance unique even if a caller overlaps sends. Mutable state is only touched
under that lock; snapshots are plain Json documents, never live objects.
"""
import threading
from collections import deque

from demiflow.execution.contracts.dataflow import Bounds, CreditLease, ExchangeSpec
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.contracts.exchange import ChannelSequence
from demiflow.execution.contracts.recovery_contract import RecoveryContractError, identifier

_MODES = frozenset({"memory_incremental", "durable_incremental", "finite_sealed"})
_ROUTINGS = frozenset({"forward", "hash", "rebalance", "broadcast", "target"})


def _ident(value, label):
    try:
        return identifier(value)
    except RecoveryContractError as exc:
        raise DataflowError("INVALID_CONTRACT", label + " is not a valid identifier") from exc


def _nonneg_int(value, label):
    if type(value) is not int or value < 0:
        raise DataflowError("INVALID_CONTRACT", label + " must be a nonnegative int")
    return value


class RoutingCounters:
    """Recoverable per-channel routing ordinals bound to one route epoch.

    The constructor copies the input mapping so a caller cannot keep mutating
    the ledger afterwards; next_ordinal hands out strictly increasing values
    and is included in every cut snapshot so a resumed run continues the same
    rebalance suffix instead of restarting from zero.
    """

    __slots__ = ("_values", "_route_epoch", "_lock")

    def __init__(self, *, values, route_epoch):
        _nonneg_int(route_epoch, "route_epoch")
        if not isinstance(values, dict) and not hasattr(values, "items"):
            raise DataflowError("INVALID_CONTRACT", "routing values must be a mapping")
        copied = {}
        for channel_id, ordinal in values.items():
            _ident(channel_id, "routing channel_id")
            copied[channel_id] = _nonneg_int(ordinal, "routing ordinal")
        self._values = copied
        self._route_epoch = route_epoch
        self._lock = threading.Lock()

    @property
    def route_epoch(self):
        return self._route_epoch

    def values(self):
        with self._lock:
            return dict(self._values)

    def next_ordinal(self, channel_id):
        _ident(channel_id, "routing channel_id")
        with self._lock:
            current = self._values.get(channel_id, 0)
            self._values[channel_id] = current + 1
            return current


class ChannelSetState:
    """One credit owner per (channel, reader) connection over bounded tables.

    Tables are sized against Bounds up front (channels <= max_edges, readers
    per channel <= max_partitions); every lookup is O(1) and buffers are
    bounded deques, so no operation scans full history. The routing ledger is
    bound once by open_channels/restore_channels (bind_routing); snapshot and
    restore move plain Json documents, never live carriers or locks.
    """

    __slots__ = (
        "specs", "bounds", "route_epoch", "routing",
        "_channels", "_readers", "_leases", "_lease_bytes",
        "_resident_bytes", "_carriers", "_closed", "_close_reason", "_lock",
    )

    def __init__(self, specs, *, bounds):
        if not isinstance(bounds, Bounds):
            raise DataflowError("INVALID_CONTRACT", "bounds must be a Bounds declaration")
        if not isinstance(specs, tuple):
            raise DataflowError("INVALID_CONTRACT", "specs must be a tuple of ExchangeSpec")
        if len(specs) > bounds.max_edges:
            raise DataflowError("BUDGET_EXCEEDED", "channel count exceeds max_edges")
        channels = {}
        readers = {}
        epochs = set()
        for spec in specs:
            if not isinstance(spec, ExchangeSpec):
                raise DataflowError("INVALID_CONTRACT", "spec must be an ExchangeSpec")
            _ident(spec.channel_id, "channel_id")
            if spec.channel_id in channels:
                raise DataflowError("INVALID_CONTRACT", "duplicate channel_id")
            if spec.mode not in _MODES:
                raise DataflowError("INVALID_CONTRACT", "unsupported exchange mode")
            if spec.routing not in _ROUTINGS:
                raise DataflowError("INVALID_CONTRACT", "unsupported routing kind")
            if not spec.producer_partitions:
                raise DataflowError("INVALID_CONTRACT", "channel has no producer partition")
            if not spec.consumer_partitions:
                raise DataflowError("INVALID_CONTRACT", "channel has no reader")
            if len(set(spec.consumer_partitions)) != len(spec.consumer_partitions):
                raise DataflowError("INVALID_CONTRACT", "duplicate reader partition")
            if len(spec.consumer_partitions) > bounds.max_partitions:
                raise DataflowError("BUDGET_EXCEEDED", "reader count exceeds max_partitions")
            if len(set(spec.producer_partitions)) != len(spec.producer_partitions):
                raise DataflowError("INVALID_CONTRACT", "duplicate producer partition")
            _nonneg_int(spec.route_epoch, "route_epoch")
            _nonneg_int(spec.credit_rows, "credit_rows")
            _nonneg_int(spec.credit_bytes, "credit_bytes")
            _nonneg_int(spec.storage_bytes, "storage_bytes")
            if spec.mode == "finite_sealed":
                if spec.storage_bytes <= 0 or not spec.scope_id:
                    raise DataflowError("INVALID_CONTRACT",
                                        "finite_sealed needs storage budget and scope_id")
            elif spec.credit_rows <= 0 or spec.credit_bytes <= 0:
                raise DataflowError("INVALID_CONTRACT",
                                    "incremental channels need positive credit windows")
            epochs.add(spec.route_epoch)
            buffer_cap = spec.storage_bytes if spec.mode == "finite_sealed" else spec.credit_bytes
            channels[spec.channel_id] = {
                "spec": spec,
                "sequence": ChannelSequence(),
                "buffer": deque(),
                "buffer_bytes": 0,
                "buffer_cap": buffer_cap,
                "eoi": {},
                "seal_ref": None,
                "tombstone": None,
                "published": {},
                "completions": {},
            }
            # One credit window per connection; the byte window is clamped to
            # the shared resident budget so a default row count can never
            # over-allocate the global window (shrink, not over-commit).
            reader_bytes = min(spec.credit_bytes, bounds.max_resident_bytes)
            for reader_id in spec.consumer_partitions:
                _ident(reader_id, "reader_id")
                readers[(spec.channel_id, reader_id)] = {
                    "rows_total": spec.credit_rows,
                    "bytes_total": reader_bytes,
                    "rows_left": spec.credit_rows,
                    "bytes_left": reader_bytes,
                    "cursor": -1,
                    "tombstone": None,
                }
        if len(epochs) > 1:
            raise DataflowError("INVALID_CONTRACT", "channels disagree on route_epoch")
        self.specs = specs
        self.bounds = bounds
        self.route_epoch = next(iter(epochs)) if epochs else 0
        self.routing = None
        self._channels = channels
        self._readers = readers
        self._leases = {}
        self._lease_bytes = 0
        self._resident_bytes = 0
        self._carriers = []
        self._closed = False
        self._close_reason = None
        self._lock = threading.RLock()

    # -- assembly surface -------------------------------------------------
    def bind_routing(self, counters):
        if not isinstance(counters, RoutingCounters):
            raise DataflowError("INVALID_CONTRACT", "routing must be a RoutingCounters ledger")
        if counters.route_epoch != self.route_epoch:
            raise DataflowError("FENCED", "routing ledger belongs to another route_epoch")
        with self._lock:
            if self.routing is not None:
                raise DataflowError("INVALID_CONTRACT", "routing ledger already bound")
            self.routing = counters

    def add_carrier(self, carrier):
        if not callable(getattr(carrier, "close", None)):
            raise DataflowError("INVALID_CONTRACT", "carrier must expose close()")
        with self._lock:
            self._carriers.append(carrier)

    @property
    def closed(self):
        return self._closed

    @property
    def resident_bytes(self):
        with self._lock:
            return self._resident_bytes

    def channel_ids(self):
        return tuple(self._channels)

    def spec(self, channel_id):
        with self._lock:
            channel = self._channels.get(channel_id)
        if channel is None:
            raise DataflowError("INVALID_CONTRACT", "unknown channel_id")
        return channel["spec"]

    def _channel(self, channel_id):
        channel = self._channels.get(channel_id)
        if channel is None:
            raise DataflowError("INVALID_CONTRACT", "unknown channel_id")
        return channel

    def _reader(self, channel_id, reader_id):
        reader = self._readers.get((channel_id, reader_id))
        if reader is None:
            raise DataflowError("INVALID_CONTRACT", "reader is not a connection of this channel")
        return reader

    def _open_channel(self, channel_id):
        if self._closed:
            raise DataflowError("FENCED", "channel set is closed: " + str(self._close_reason))
        channel = self._channel(channel_id)
        if channel["tombstone"] is not None:
            raise DataflowError("CANCELLED", "channel is cancelled: " + channel["tombstone"])
        return channel

    # -- sequence / buffer ------------------------------------------------
    def issue_sequence(self, channel_id):
        with self._lock:
            channel = self._open_channel(channel_id)
            return channel["sequence"].issue()

    def next_sequence(self, channel_id):
        with self._lock:
            return self._channel(channel_id)["sequence"]._next

    def enqueue(self, channel_id, sequence, encoded):
        with self._lock:
            channel = self._open_channel(channel_id)
            size = encoded.encoded_bytes
            if channel["buffer_bytes"] + size > channel["buffer_cap"]:
                raise DataflowError("CAPACITY_PENDING", "channel buffer is full", retryable=True)
            if self._resident_bytes + self._lease_bytes + size > self.bounds.max_resident_bytes:
                raise DataflowError("CAPACITY_PENDING", "resident window is full", retryable=True)
            channel["buffer"].append((sequence, encoded))
            channel["buffer_bytes"] += size
            self._resident_bytes += size

    def drain_buffer(self, channel_id):
        with self._lock:
            channel = self._channel(channel_id)
            items = tuple(channel["buffer"])
            channel["buffer"].clear()
            self._resident_bytes -= channel["buffer_bytes"]
            channel["buffer_bytes"] = 0
            return items

    def buffer_items(self, channel_id):
        with self._lock:
            return tuple(self._channel(channel_id)["buffer"])

    # -- credit -----------------------------------------------------------
    def reserve(self, channel_id, reader_id, *, rows, size_bytes, operation_id):
        _ident(operation_id, "operation_id")
        if type(rows) is not int or rows <= 0:
            raise DataflowError("INVALID_CONTRACT", "credit rows must be positive")
        _nonneg_int(size_bytes, "credit size_bytes")
        with self._lock:
            channel = self._open_channel(channel_id)
            reader = self._reader(channel_id, reader_id)
            if reader["tombstone"] is not None:
                raise DataflowError("CANCELLED", "reader is cancelled: " + reader["tombstone"])
            existing = self._leases.get(operation_id)
            if existing is not None:
                if (existing.channel_id != channel_id or existing.reader_id != reader_id
                        or existing.rows != rows or existing.size_bytes != size_bytes):
                    raise DataflowError("INVALID_CONTRACT",
                                        "operation_id already reserved different credit")
                return existing
            spec = channel["spec"]
            if size_bytes > reader["bytes_total"] or size_bytes > self.bounds.max_resident_bytes:
                raise DataflowError("BUDGET_EXCEEDED",
                                    "record exceeds the credit window and can never be admitted")
            if len(self._leases) >= self.bounds.max_pending_events:
                raise DataflowError("BUDGET_EXCEEDED", "outstanding credit ledger is full")
            if (reader["rows_left"] < rows or reader["bytes_left"] < size_bytes
                    or self._resident_bytes + self._lease_bytes + size_bytes
                    > self.bounds.max_resident_bytes):
                raise DataflowError("CAPACITY_PENDING",
                                    "credit window exhausted; wait within the deadline",
                                    retryable=True)
            reader["rows_left"] -= rows
            reader["bytes_left"] -= size_bytes
            self._lease_bytes += size_bytes
            lease = CreditLease(operation_id=operation_id, channel_id=channel_id,
                                reader_id=reader_id, rows=rows, size_bytes=size_bytes)
            self._leases[operation_id] = lease
            return lease

    def release(self, lease):
        """Return credit only after the downstream confirmation (never before)."""
        if not isinstance(lease, CreditLease):
            raise DataflowError("INVALID_CONTRACT", "release needs a CreditLease")
        with self._lock:
            existing = self._leases.get(lease.operation_id)
            if existing is None:
                return
            if existing != lease:
                raise DataflowError("INVALID_CONTRACT", "lease does not match the reservation")
            reader = self._reader(lease.channel_id, lease.reader_id)
            reader["rows_left"] = min(reader["rows_total"], reader["rows_left"] + lease.rows)
            reader["bytes_left"] = min(reader["bytes_total"], reader["bytes_left"] + lease.size_bytes)
            self._lease_bytes -= lease.size_bytes
            del self._leases[lease.operation_id]

    def outstanding_leases(self):
        with self._lock:
            return tuple(self._leases.values())

    # -- reader frontier / lifecycle facts --------------------------------
    def note_cursor(self, channel_id, reader_id, sequence):
        _nonneg_int(sequence, "reader cursor")
        with self._lock:
            reader = self._reader(channel_id, reader_id)
            if sequence < reader["cursor"]:
                raise DataflowError("INVALID_CONTRACT", "reader cursor cannot regress")
            reader["cursor"] = sequence

    def reader_cursor(self, channel_id, reader_id):
        with self._lock:
            return self._reader(channel_id, reader_id)["cursor"]

    def reader_credit(self, channel_id, reader_id):
        with self._lock:
            reader = self._reader(channel_id, reader_id)
            return {"rows_left": reader["rows_left"], "bytes_left": reader["bytes_left"],
                    "rows_total": reader["rows_total"], "bytes_total": reader["bytes_total"]}

    def cancel_reader(self, channel_id, reader_id, reason):
        if not isinstance(reason, str):
            raise DataflowError("INVALID_CONTRACT", "cancel reason must be a string")
        with self._lock:
            reader = self._reader(channel_id, reader_id)
            reader["tombstone"] = reason

    def cancel_channel(self, channel_id, reason):
        if not isinstance(reason, str):
            raise DataflowError("INVALID_CONTRACT", "cancel reason must be a string")
        with self._lock:
            channel = self._channel(channel_id)
            channel["tombstone"] = reason
            for reader_id in channel["spec"].consumer_partitions:
                self._readers[(channel_id, reader_id)]["tombstone"] = reason

    def reader_tombstone(self, channel_id, reader_id):
        with self._lock:
            return self._reader(channel_id, reader_id)["tombstone"]

    def mark_eoi(self, channel_id, partition_id):
        with self._lock:
            channel = self._channel(channel_id)
            if partition_id not in channel["spec"].producer_partitions:
                raise DataflowError("INVALID_CONTRACT", "EOI from a non-producer partition")
            channel["eoi"][partition_id] = True

    def eoi_partitions(self, channel_id):
        with self._lock:
            return tuple(sorted(self._channel(channel_id)["eoi"]))

    def register_completion(self, channel_id, completion):
        with self._lock:
            channel = self._channel(channel_id)
            if completion.partition_id not in channel["spec"].producer_partitions:
                raise DataflowError("INVALID_CONTRACT", "completion from a non-producer partition")
            channel["completions"][completion.partition_id] = completion

    def completions(self, channel_id):
        with self._lock:
            return dict(self._channel(channel_id)["completions"])

    def set_seal(self, channel_id, ref):
        with self._lock:
            self._channel(channel_id)["seal_ref"] = ref

    def seal_ref(self, channel_id):
        with self._lock:
            return self._channel(channel_id)["seal_ref"]

    # -- live segment index (GC owns removal, pins retain) -----------------
    def register_segment(self, channel_id, ref, *, seq_end, pinned=False):
        _nonneg_int(seq_end, "segment seq_end")
        with self._lock:
            channel = self._channel(channel_id)
            channel["published"][ref.digest] = {"ref": ref, "seq_end": seq_end, "pinned": pinned}

    def pin_segment(self, channel_id, digest):
        with self._lock:
            entry = self._channel(channel_id)["published"].get(digest)
            if entry is not None:
                entry["pinned"] = True

    def published_segments(self, channel_id):
        with self._lock:
            return dict(self._channel(channel_id)["published"])

    def drop_segment(self, channel_id, digest):
        with self._lock:
            self._channel(channel_id)["published"].pop(digest, None)

    # -- cut snapshot document (plain Json, no live objects) ---------------
    def restore_channel(self, channel_id, doc):
        """Apply one saved channel document: sequence frontier, reader windows,
        lifecycle facts and outstanding leases. Credit is restored as recorded
        (never re-granted); buffered data is re-enqueued separately by X009."""
        with self._lock:
            channel = self._channel(channel_id)
            frontier = doc["sequence_next"]
            _nonneg_int(frontier, "saved sequence frontier")
            channel["sequence"]._next = frontier
            spec = channel["spec"]
            for reader_id in spec.consumer_partitions:
                saved = doc["readers"][reader_id]
                reader = self._readers[(channel_id, reader_id)]
                reader["cursor"] = saved["cursor"]
                reader["rows_left"] = saved["rows_left"]
                reader["bytes_left"] = saved["bytes_left"]
                reader["tombstone"] = saved["tombstone"]
            channel["eoi"] = {partition: True for partition in doc["eoi"]}
            channel["tombstone"] = doc["tombstone"]
            for lease_doc in doc["leases"]:
                lease = CreditLease(operation_id=lease_doc["operation_id"],
                                    channel_id=channel_id, reader_id=lease_doc["reader_id"],
                                    rows=lease_doc["rows"], size_bytes=lease_doc["size_bytes"])
                self._leases[lease.operation_id] = lease
                self._lease_bytes += lease.size_bytes

    def snapshot_document(self, channel_id):
        with self._lock:
            channel = self._channel(channel_id)
            spec = channel["spec"]
            readers = {}
            for reader_id in spec.consumer_partitions:
                reader = self._readers[(channel_id, reader_id)]
                readers[reader_id] = {
                    "cursor": reader["cursor"],
                    "rows_left": reader["rows_left"],
                    "bytes_left": reader["bytes_left"],
                    "tombstone": reader["tombstone"],
                }
            ordinal = self.routing.values().get(channel_id, 0) if self.routing is not None else 0
            return {
                "schema": "demiflow.channel_state.v3",
                "channel_id": channel_id,
                "edge_id": spec.edge_id,
                "mode": spec.mode,
                "routing": spec.routing,
                "route_epoch": spec.route_epoch,
                "sequence_next": channel["sequence"]._next,
                "routing_ordinal": ordinal,
                "readers": readers,
                "eoi": sorted(channel["eoi"]),
                "tombstone": channel["tombstone"],
                "buffer_bytes": channel["buffer_bytes"],
                "resident_bytes": self._resident_bytes,
                "lease_bytes": self._lease_bytes,
                "leases": [
                    {"operation_id": lease.operation_id, "reader_id": lease.reader_id,
                     "rows": lease.rows, "size_bytes": lease.size_bytes}
                    for lease in sorted(self._leases.values(), key=lambda x: x.operation_id)
                    if lease.channel_id == channel_id
                ],
            }

    def close(self):
        """Idempotent carrier shutdown; one failure never skips the others.

        Confirmed transient leases (buffers fully drained) are returned; when
        data remains buffered the outstanding ownership stays recorded as
        unknown instead of being fake-released. Recovery pins on published
        segments are never touched here - reclaim_segments owns that decision.
        """
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._close_reason = self._close_reason or "closed"
            carriers = tuple(self._carriers)
        errors = []
        for carrier in carriers:
            try:
                carrier.close()
            except BaseException as exc:
                errors.append(exc)
        with self._lock:
            drained = all(not channel["buffer"] for channel in self._channels.values())
            if drained:
                for lease in tuple(self._leases.values()):
                    reader = self._readers[(lease.channel_id, lease.reader_id)]
                    reader["rows_left"] = min(reader["rows_total"], reader["rows_left"] + lease.rows)
                    reader["bytes_left"] = min(reader["bytes_total"],
                                              reader["bytes_left"] + lease.size_bytes)
                self._leases.clear()
                self._resident_bytes = 0
                self._lease_bytes = 0
        if errors:
            raise errors[0]
