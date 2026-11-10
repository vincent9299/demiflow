"""Per-reader credit and channel-isolated endpoints (U3/S1, K5 over 1:1 channels).

Each physical channel has exactly one reader (D2), so credit is tracked per
physical channel: a slow reader exhausts only its own channel and never blocks
sibling channels of the same producer (exit gate 1). Incremental modes charge
consumer credit (retriable CAPACITY_PENDING); finite_sealed bypasses consumer
credit and charges a bounded storage reservation whose exhaustion fails early
(non-retriable QUOTA_EXCEEDED) instead of deadlocking. The consumer side
reports per-channel consumed cursors for the segment-GC safe point and releases
its pin explicitly on unsubscribe. Pure protocol models; carriage is S2/S5.
"""
from ..contracts.errors import ProtocolError
from ..contracts.exchange import check_route_epoch, validate_credit
from ..recovery_contract import identifier
from .channel import ChannelConsumer, ChannelProducer

_MODES = frozenset({"memory_incremental", "durable_incremental", "finite_sealed"})


class StorageReservation:
    """Bounded sealed-mode storage budget; exhaustion is an early, final failure."""

    def __init__(self, budget_bytes):
        if type(budget_bytes) is not int or budget_bytes < 0:
            raise ValueError("storage budget must be a nonnegative int")
        self._budget = budget_bytes
        self._used = 0

    @property
    def remaining(self):
        return self._budget - self._used

    @property
    def used(self):
        return self._used

    def feasible(self, size_bytes):
        if type(size_bytes) is not int or size_bytes < 0:
            raise ValueError("size must be a nonnegative int")
        return size_bytes <= self.remaining

    def charge(self, size_bytes):
        if not self.feasible(size_bytes):
            # The frozen error vocabulary has no CAPACITY_EXHAUSTED; QUOTA_EXCEEDED
            # is the inherited resource code for a final (non-retriable) budget fail.
            raise ProtocolError("QUOTA_EXCEEDED",
                                "sealed storage reservation exhausted", retriable=False)
        self._used += size_bytes

    def release(self, size_bytes):
        if type(size_bytes) is not int or size_bytes < 0 or size_bytes > self._used:
            raise ValueError("release must be within the charged amount")
        self._used -= size_bytes


class GraphChannelProducer(ChannelProducer):
    """One physical channel endpoint: single declared reader, mode-aware charge."""

    def __init__(self, channel_id, *, reader_id, exchange_mode, route_epoch=0, storage=None):
        super().__init__(channel_id, route_epoch=route_epoch)
        if exchange_mode not in _MODES:
            raise ValueError("unsupported exchange mode")
        if (exchange_mode == "finite_sealed") != (storage is not None):
            raise ValueError("storage reservation belongs to finite_sealed only")
        self._reader_id = identifier(reader_id)
        self._exchange_mode = exchange_mode
        self._storage = storage

    def grant_credit(self, credit):
        validate_credit(credit)
        if credit["reader_id"] != self._reader_id:
            raise ProtocolError("INVALID_SPEC", "credit comes from a non-reader of this channel")
        check_route_epoch(self.route_epoch, credit["route_epoch"])
        return super().grant_credit(credit)

    def _charge(self, rows, size_bytes):
        if self._exchange_mode != "finite_sealed":
            super()._charge(rows, size_bytes)
            return
        if self._cancelled is not None:
            raise ProtocolError("FENCED", "channel is cancelled: " + self._cancelled)
        self._storage.charge(size_bytes)


class GraphChannelConsumer:
    """One reader over its input physical channels: gate, cursors, credit, fence."""

    def __init__(self, channels, *, reader_id, reader_domain, route_epoch=0, max_pending=1024):
        self._inner = ChannelConsumer(channels, reader_domain=reader_domain, max_pending=max_pending)
        self._reader_id = identifier(reader_id)
        self._route_epoch = route_epoch
        self._channel_ids = list(channels)
        self._cursors = {channel: -1 for channel in channels}
        self._unsubscribed = False

    @property
    def cursors(self):
        """Last consumed data sequence per channel; feeds the GC safe point."""
        return dict(self._cursors)

    def push(self, channel_id, event):
        if self._unsubscribed:
            raise ProtocolError("FENCED", "reader has unsubscribed")
        if event.get("kind") == "data":
            check_route_epoch(self._route_epoch, event["envelope"]["route_epoch"])
        return self._inner.push(channel_id, event)

    def consume(self, channel_id):
        if self._unsubscribed:
            raise ProtocolError("FENCED", "reader has unsubscribed")
        event = self._inner.consume(channel_id)
        if event is not None:
            self._cursors[channel_id] = event["sequence"]
        return event

    def barrier_effect(self, channel_id):
        return self._inner.barrier_effect(channel_id)

    def pending_checkpoint(self):
        """The common barrier checkpoint once every input channel is aligned.

        Uses the frozen gate's public barrier_effect (pre-cut data fully consumed,
        barrier arrived); returns None until all channels hold the same checkpoint.
        """
        checkpoints = set()
        for channel_id in self._channel_ids:
            try:
                barrier = self._inner.barrier_effect(channel_id)
            except ProtocolError:
                return None
            checkpoints.add(barrier["checkpoint_id"])
        if len(checkpoints) == 1:
            return next(iter(checkpoints))
        return None

    def aligned(self, checkpoint_id):
        return self._inner.aligned(checkpoint_id)

    def release(self, checkpoint_id):
        return self._inner.release(checkpoint_id)

    def grant_credit(self, *, channel_id, rows, size_bytes, operation_id):
        return self._inner.grant_credit(channel_id=channel_id, reader_id=self._reader_id,
                                        rows=rows, size_bytes=size_bytes,
                                        route_epoch=self._route_epoch, operation_id=operation_id)

    def cancel(self, value):
        return self._inner.cancel(value)

    def unsubscribe(self):
        """Explicit pin release: late data is fenced, cursors freeze for GC."""
        self._unsubscribed = True