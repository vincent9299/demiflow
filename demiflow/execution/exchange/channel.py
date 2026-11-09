"""First-batch ordered channel endpoints (U2/S4, K4/K5 over D3 carriers).

Data and control draw from one sequence space (frozen ChannelSequence/Cursor).
The consumer orders consumption with the frozen InputGate pure model: a barrier
never overtakes pre-cut data, post-barrier data is held until the cut is aligned
and released. Credit is bounded by rows/bytes (full => retriable backpressure);
a cancel tombstone suppresses late data; only successful producers may seal.
The physical carrier is the staging block DataRef (segments.py), not a new
transport. These endpoints hold bounded buffers only.
"""
from ..contracts.errors import ProtocolError
from ..contracts.exchange import (
    ChannelSequence, InputGate, seal_allowed, validate_barrier, validate_credit,
    validate_data_envelope, validate_data_ref, validate_end_of_input,
    validate_seal, validate_channel_cancel, check_read_domain,
    DATA_ENVELOPE_SCHEMA,
)
from ..recovery_contract import identifier


class ChannelProducer:
    """Worker-side result-out endpoint: issues sequences, charges bounded credit."""

    def __init__(self, channel_id, *, route_epoch=0, credit_rows=0, credit_bytes=0):
        identifier(channel_id)
        self.channel_id = channel_id
        self.route_epoch = route_epoch
        self._sequence = ChannelSequence()
        self._credit_rows = credit_rows
        self._credit_bytes = credit_bytes
        self._cancelled = None

    @property
    def next_sequence(self):
        return self._sequence._next

    def grant_credit(self, credit):
        validate_credit(credit)
        if credit["channel_id"] != self.channel_id:
            raise ProtocolError("INVALID_SPEC", "credit targets another channel")
        self._credit_rows += credit["credits"]["rows"]
        self._credit_bytes += credit["credits"]["bytes"]
        return credit

    def _charge(self, rows, size_bytes):
        if self._cancelled is not None:
            raise ProtocolError("FENCED", "channel is cancelled: " + self._cancelled)
        if rows > self._credit_rows or size_bytes > self._credit_bytes:
            raise ProtocolError("CAPACITY_PENDING", "channel credit exhausted", retriable=True)
        self._credit_rows -= rows
        self._credit_bytes -= size_bytes

    def cancel(self, reason):
        self._cancelled = reason

    def data_envelope(self, records, *, producer, partition_id="p0"):
        if type(records) is not list:
            raise ProtocolError("INVALID_SPEC", "records must be a list")
        size_bytes = 0
        for record in records:
            size_bytes += record["size_bytes"]
            payload = record.get("payload")
            if type(payload) is dict and payload.get("schema_version") == "demiflow.data_ref.v1":
                validate_data_ref(payload)
        self._charge(len(records), size_bytes)
        envelope = {
            "schema_version": DATA_ENVELOPE_SCHEMA, "channel_id": self.channel_id,
            "sequence": self._sequence.issue(), "partition_id": partition_id,
            "route_epoch": self.route_epoch, "producer": producer,
            "records": records, "bounds": {"rows": len(records), "bytes": size_bytes},
        }
        return validate_data_envelope(envelope)

    def barrier(self, checkpoint_id, *, epoch, topology_revision,
                producer_sequence_bound=None, source_progress_ref=None):
        if self._cancelled is not None:
            raise ProtocolError("FENCED", "channel is cancelled: " + self._cancelled)
        sequence = self._sequence.issue()
        # Data issued before this barrier is pre-cut; the bound defaults to the
        # barrier's own sequence (frozen InputGate convention).
        bound = sequence if producer_sequence_bound is None else producer_sequence_bound
        cut = {"producer_sequence_bound": bound}
        if source_progress_ref is not None:
            cut["source_progress_ref"] = source_progress_ref
        value = {
            "channel_id": self.channel_id, "sequence": sequence,
            "checkpoint_id": checkpoint_id, "epoch": epoch, "cut": cut,
            "topology_revision": topology_revision,
        }
        return validate_barrier(value)

    def end_of_input(self, *, partition_id="p0"):
        value = {
            "channel_id": self.channel_id, "sequence": self._sequence.issue(),
            "producer_partition_id": partition_id, "final": True,
        }
        return validate_end_of_input(value)

    def seal(self, *, seal_scope, manifest_ref, producer_statuses):
        seal_allowed(producer_statuses)
        value = {
            "channel_id": self.channel_id, "sequence": self._sequence.issue(),
            "seal_scope": seal_scope, "manifest_ref": manifest_ref, "status": "success",
        }
        return validate_seal(value)


class ChannelConsumer:
    """Driver-side endpoint: InputGate ordering, read_domain, credit, tombstone."""

    def __init__(self, channels, *, reader_domain, max_pending=1024):
        self._gate = InputGate(channels, max_pending=max_pending)
        self.reader_domain = reader_domain
        self._tombstone = {}

    def _cancelled(self, channel_id):
        return channel_id in self._tombstone or "*" in self._tombstone

    def push(self, channel_id, event):
        if self._cancelled(channel_id):
            raise ProtocolError("FENCED", "late data on a cancelled channel/scope")
        if event.get("kind") == "data":
            envelope = event["envelope"]
            validate_data_envelope(envelope)
            if envelope["channel_id"] != channel_id:
                raise ProtocolError("INVALID_SPEC", "envelope targets another channel")
            for record in envelope["records"]:
                payload = record.get("payload")
                if type(payload) is dict and payload.get("schema_version") == "demiflow.data_ref.v1":
                    check_read_domain(payload, self.reader_domain)
            return self._gate.push(channel_id, {"kind": "data", "sequence": event["sequence"],
                                                "envelope": envelope})
        return self._gate.push(channel_id, event)

    def consume(self, channel_id):
        return self._gate.consume(channel_id)

    def barrier_effect(self, channel_id):
        return self._gate.barrier_effect(channel_id)

    def aligned(self, checkpoint_id):
        return self._gate.aligned(checkpoint_id)

    def release(self, checkpoint_id):
        return self._gate.release(checkpoint_id)

    def grant_credit(self, *, channel_id, reader_id, rows, size_bytes,
                     route_epoch, operation_id):
        credit = {"channel_id": channel_id, "reader_id": reader_id,
                  "credits": {"rows": rows, "bytes": size_bytes},
                  "route_epoch": route_epoch, "operation_id": operation_id}
        return validate_credit(credit)

    def cancel(self, value):
        validate_channel_cancel(value)
        if "channel_id" in value:
            self._tombstone[value["channel_id"]] = value["reason"]
        else:
            self._tombstone["*"] = value["reason"]
        return value