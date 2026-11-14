"""Native stage buffer snapshots on the single run commit protocol (C05).

This module closes the previously UNFROZEN ``StageBufferSnapshot`` payload for the
local stream buffer kinds. It is a durable *input remainder* carrier only: it never
owns commit authority. The driver's ``CommitCoordinator`` / graph contribution stays
the single publication path; a snapshot file existing is not a confirmed checkpoint.

Protocol ``demiflow.stage_buffer.v1``
-------------------------------------
One snapshot describes one stage's pending (unconsumed) input at a single cut. The
cut stops cross-cut input first, then freezes the rows each active worker still
holds, together with that worker's batch lineage. Participants are the real active
worker slots ``0..concurrency-1``; at every local cut all of them are active because
EOF sentinels are injected only after the final cut publishes (stream._arun).

Shape::

    {
      "schema": "demiflow.stage_buffer.v1",
      "kind": "fixed_batch"|"concurrent_batch"|"timed_batch"|"grouped"|"prepend",
      "stage_key": "node-<index>",
      "participants": <int >= 1>,
      "slots": {
        "<slot>": {
          "batch_ordinal": <int >= 0>,        # next flush ordinal for this slot
          "data": <BlockDescriptor>,           # pending rows (batch/grouped kinds)
          "groups": [                          # grouped kind only
            {"key": <JSON value>, "data": <BlockDescriptor>,
             "deadline_ms": <int >= 0>}
          ]
        }
      },
      "prefix": {"position": <int >= 0>, "exhausted": <bool>}   # prepend kind only
    }

Rules frozen here:

* ``fixed_batch`` keeps reading the legacy ``{batch_ordinal, data}`` shape verbatim;
  it is interpreted as slot 0 of a one-participant buffer and is never rewritten.
* ``native_row_local`` is NOT a buffer kind: in-flight model effects stay in the
  existing ``native_pending`` registry / ``node-<i>-inflight`` artifact and are not
  copied into this snapshot.
* Timed/grouped flush windows restart on restore (``deadline_ms`` is advisory); the
  preserved rows, group keys and batch ordinals are the durable contract, not the
  wall-clock timer.
* Every byte is accounted against the shared ``barrier.snapshot_bytes`` budget
  (``run.limits['max_state_bytes']``); a buffer never gets a private quota.
* Unknown shapes, mismatched identity, oversized payloads and duplicate/foreign
  slots fail closed with ``RecoveryContractError``; unknown is never turned into
  success.
"""
from .recovery_contract import RecoveryContractError

SCHEMA = "demiflow.stage_buffer.v1"
BUFFER_KINDS = ("fixed_batch", "concurrent_batch", "timed_batch", "grouped", "prepend")

# Kinds opened for partial commits only after a runnable crash-recovery counterexample
# passes on this branch (C05 "open per kind with real evidence"). A kind absent here
# stays fail-closed in bind_stages/_arun rather than faking recovery.
PROVEN_KINDS = {"fixed_batch", "concurrent_batch", "timed_batch", "grouped", "prepend"}

# Group keys are tuple((type(v), v)) pairs (stream_grouping.group_key). StateArtifacts
# persists JSON, so the type objects are encoded by name and rebuilt on restore; the
# explicit name keeps bool/int/float and signed zero distinct (no Python == folding).
_KEY_TYPES = {"str": str, "int": int, "float": float, "bool": bool, "NoneType": type(None)}


def _encode_group_key(key):
    return [[t.__name__, v] for t, v in key]


def _decode_group_key(encoded):
    try:
        return tuple((_KEY_TYPES[name], value) for name, value in encoded)
    except (KeyError, TypeError, ValueError):
        raise RecoveryContractError("Grouped snapshot carries an unsupported key encoding") from None


def classify_stage_buffer(stage):
    """Return the native buffer kind for a lowered stage, or None for row-local.

    Mirrors the C05 capability split: prepend/grouped are their own kinds; a batch
    stage is timed when it has a flush window, concurrent when it has more than one
    worker, otherwise a fixed batch. Row-wise stages have no platform buffer.
    """
    if getattr(stage, "prefix", None) is not None:
        return "prepend"
    if getattr(stage, "grouping", None) is not None:
        return "grouped"
    if getattr(stage, "max_batch", None) is not None:
        if getattr(stage, "flush_interval", None) is not None:
            return "timed_batch"
        if stage.concurrency > 1:
            return "concurrent_batch"
        return "fixed_batch"
    return None


class StageBufferAdapter:
    """Stateful per-stage buffer carried by ``_Stage.buffer_adapter``.

    The adapter borrows the run's existing BlockSink/StateArtifacts authorities; it
    does not create a journal, a second ledger, or a commit path. Workers register
    their slot ordinal as they flush and hand their pending rows to the barrier at a
    cut; ``align`` freezes those rows into the shared snapshot, ``restore`` validates
    and rehydrates them, and each worker takes back exactly its own slot.
    """

    def __init__(self, stage, *, action, stage_key):
        kind = classify_stage_buffer(stage)
        if kind is None:
            raise RecoveryContractError("Stage has no native buffer to adapt")
        if type(stage_key) is not str or not stage_key:
            raise RecoveryContractError("Buffer stage_key must be a stable nonempty string")
        self.stage = stage
        self.action = action
        self.run = action.context
        self.stage_key = stage_key
        self.kind = kind
        self.participants = stage.concurrency if kind != "prepend" else 1
        if self.participants < 1:
            raise RecoveryContractError("Buffer participants must be positive")
        self.closed = False
        # Next flush ordinal per active worker slot. Slot 0 mirrors the legacy
        # stage.batch_ordinal so fixed-batch lineage stays byte-identical.
        self.slot_ordinals = {slot: 0 for slot in range(self.participants)}
        self.slot_ordinals[0] = getattr(stage, "batch_ordinal", 0) or 0
        self._restored = {}
        self._restored_ordinals = {}
        self.prefix_position = 0
        self.prefix_exhausted = False
        self._sinks = {}
        import asyncio
        self.next_slot = 0
        self._input_turn = asyncio.Condition()

    async def read(self, queue, slot):
        """Deterministic row-to-slot routing, including cut and EOF tokens.

        A shared queue's scheduler race must not decide paid request membership.
        The cursor is cut state; each worker retains its own partial batch.
        """
        async with self._input_turn:
            await self._input_turn.wait_for(lambda: self.next_slot == slot)
            row = await queue.get()
            self.next_slot = (slot + 1) % self.participants
            self._input_turn.notify_all()
            return row

    # -- worker-side lineage -------------------------------------------------
    def next_ordinal(self, slot):
        """Reserve the next flush ordinal for a slot (called by ``_flush``)."""
        self._check_slot(slot)
        ordinal = self.slot_ordinals[slot]
        self.slot_ordinals[slot] = ordinal + 1
        if slot == 0:
            self.stage.batch_ordinal = self.slot_ordinals[0]
        return ordinal

    def replay_batch_size(self, slot):
        """Recover an already-dispatched native batch's original time boundary."""
        from .recovery_contract import identity
        scope = self.scope_identity(slot, self.slot_ordinals[slot])
        key = identity(['native-batch-size', self.action.action_id, scope])
        fact = self.run.journal.source_binding(key)
        return None if fact is None else fact['rows']

    def seal_batch_size(self, slot, ordinal, rows):
        from .recovery_contract import identity
        key = identity(['native-batch-size', self.action.action_id, self.scope_identity(slot, ordinal)])
        self.run.journal.bind_source(self.run.fence, key, {'rows': rows})

    def scope_identity(self, slot, ordinal):
        """Row-scope identity for a flush.

        A one-participant fixed batch keeps the historical ``['fixed-batch', key,
        ordinal]`` shape so existing effect keys/snapshots stay compatible; concurrent
        and timed buffers add the slot so two workers never collide on an ordinal.
        """
        from .recovery_contract import identity
        if self.participants == 1 and self.kind in ("fixed_batch", "grouped"):
            return identity(["fixed-batch", self.stage_key, ordinal])
        return identity(["fixed-batch", self.stage_key, slot, ordinal])

    def take_restored(self, slot):
        """Return ``(payload, ordinal)`` restored for a slot, once, at worker start."""
        self._check_slot(slot)
        payload = self._restored.pop(slot, None)
        ordinal = self._restored_ordinals.pop(slot, None)
        if ordinal is not None:
            self.slot_ordinals[slot] = ordinal
            if slot == 0:
                self.stage.batch_ordinal = ordinal
        return payload, ordinal

    def take_prefix(self):
        return self.prefix_position, self.prefix_exhausted

    # -- cut snapshot --------------------------------------------------------
    async def align(self, barrier, pending):
        """Freeze every active slot's pending input into ``barrier.states``.

        ``pending`` maps slot -> payload produced by the worker at the cut: a row
        list for batch kinds, a list of ``(key, rows, deadline_ms)`` for grouped, or
        a ``{"position", "exhausted"}`` mapping for prepend. All bytes are charged to
        the one shared barrier budget.
        """
        if self.closed:
            raise RecoveryContractError("Cannot align a closed buffer adapter")
        if not isinstance(pending, dict):
            raise RecoveryContractError("Buffer pending must be a per-slot mapping")
        expected = set(range(self.participants))
        if set(pending) != expected:
            raise RecoveryContractError("Buffer pending slots do not match active participants")
        from ..data.runtime_contract import check_size
        from .block_sink import BlockSink
        limit = self.run.limits["max_state_bytes"]
        slots = {}
        for slot in sorted(expected):
            payload = pending[slot]
            entry = {"batch_ordinal": self.slot_ordinals[slot]}
            if self.kind == "prepend":
                if not isinstance(payload, dict):
                    raise RecoveryContractError("Prepend pending must carry a position")
                position = payload.get("position", 0)
                if type(position) is not int or position < 0:
                    raise RecoveryContractError("Prepend position must be a nonnegative integer")
                entry["position"] = position
                entry["exhausted"] = bool(payload.get("exhausted", False))
            elif self.kind == "grouped":
                entry["groups"] = self._freeze_groups(barrier, slot, payload, check_size, limit)
            else:
                entry["data"] = self._freeze_rows(barrier, slot, payload, check_size, limit)
            slots[str(slot)] = entry
        snapshot = {"schema": SCHEMA, "kind": self.kind, "stage_key": self.stage_key,
                    "participants": self.participants, "slots": slots}
        snapshot['next_slot'] = self.next_slot
        if self.kind == "prepend":
            snapshot["prefix"] = {"position": self.prefix_position,
                                  "exhausted": self.prefix_exhausted}
        barrier.snapshot_bytes += 256
        if barrier.snapshot_bytes > limit:
            raise RecoveryContractError("Aggregate buffer snapshot exceeds state budget")
        barrier.states[self.stage_key + "-buffer"] = snapshot

    def _sink(self, slot):
        from .block_sink import BlockSink
        sink = self._sinks.get(slot)
        if sink is None:
            # Slot 0 keeps the historical ``node-<i>-buffer`` sink id so legacy
            # fixed-batch descriptors stay readable verbatim; extra worker slots
            # get their own root so concurrent remainders never collide.
            sink_id = self.stage_key + "-buffer" if slot == 0 else self.stage_key + "-buffer-" + str(slot)
            sink = BlockSink(self.action, sink_id, staging_only=True)
            self._sinks[slot] = sink
        return sink

    def _freeze_rows(self, barrier, slot, rows, check_size, limit):
        if rows is None:
            rows = []
        if not isinstance(rows, (list, tuple)):
            raise RecoveryContractError("Buffer pending rows must be a bounded sequence")
        sink = self._sink(slot)
        sink.reset()
        for row in rows:
            amount = check_size(row, self.run.limits["max_row_bytes"]) + 16
            barrier.snapshot_bytes += amount
            if barrier.snapshot_bytes > limit:
                raise RecoveryContractError("Aggregate buffer snapshot exceeds state budget")
            sink.append(row)
        sink._flush()
        descriptor = sink.descriptor()
        sink.reset()
        return descriptor

    def _freeze_groups(self, barrier, slot, groups, check_size, limit):
        if groups is None:
            groups = []
        if not isinstance(groups, (list, tuple)):
            raise RecoveryContractError("Grouped pending must be a bounded sequence")
        frozen = []
        for item in groups:
            key, rows, deadline_ms = item
            descriptor = self._freeze_rows(barrier, slot, rows, check_size, limit)
            frozen.append({"key": _encode_group_key(key), "data": descriptor,
                           "deadline_ms": max(0, int(deadline_ms))})
        return frozen

    # -- recovery ------------------------------------------------------------
    def restore(self, snapshot):
        """Validate identity/shape/size, then rehydrate per-slot pending input."""
        if self.closed:
            raise RecoveryContractError("Cannot restore a closed buffer adapter")
        if not isinstance(snapshot, dict):
            raise RecoveryContractError("Buffer snapshot must be a mapping")
        # Legacy fixed-batch shape stays readable; it is slot 0 of a single worker.
        if "schema" not in snapshot and "batch_ordinal" in snapshot and "data" in snapshot:
            if self.kind != "fixed_batch":
                raise RecoveryContractError("Legacy fixed-batch snapshot cannot restore a " + self.kind + " buffer")
            snapshot = {"schema": SCHEMA, "kind": "fixed_batch", "stage_key": self.stage_key,
                        "participants": 1,
                        "slots": {"0": {"batch_ordinal": snapshot["batch_ordinal"],
                                        "data": snapshot["data"]}}}
        if snapshot.get("schema") != SCHEMA:
            raise RecoveryContractError("Unsupported buffer snapshot schema")
        if snapshot.get("kind") != self.kind:
            raise RecoveryContractError("Buffer snapshot kind differs from the sealed stage")
        if snapshot.get("stage_key") != self.stage_key:
            raise RecoveryContractError("Buffer snapshot identity differs from the sealed stage")
        participants = snapshot.get("participants")
        if type(participants) is not int or participants != self.participants:
            raise RecoveryContractError("Buffer snapshot participants differ from the sealed stage")
        slots = snapshot.get("slots")
        next_slot = snapshot.get('next_slot', 0)
        if type(next_slot) is not int or not 0 <= next_slot < self.participants:
            raise RecoveryContractError('Buffer routing cursor is outside its worker slots')
        self.next_slot = next_slot
        if not isinstance(slots, dict) or set(slots) != {str(s) for s in range(self.participants)}:
            raise RecoveryContractError("Buffer snapshot slots do not match active participants")
        from ..data.runtime_contract import check_size
        from .block_sink import BlockSink
        limit = self.run.limits["max_state_bytes"]
        used = 256
        restored = {}
        ordinals = {}
        for slot in range(self.participants):
            entry = slots[str(slot)]
            ordinal = entry.get("batch_ordinal")
            if type(ordinal) is not int or ordinal < 0:
                raise RecoveryContractError("Buffer batch_ordinal must be a nonnegative integer")
            ordinals[slot] = ordinal
            if self.kind == "prepend":
                position = entry.get("position", 0)
                if type(position) is not int or position < 0:
                    raise RecoveryContractError("Prepend position must be a nonnegative integer")
                restored[slot] = {"position": position, "exhausted": bool(entry.get("exhausted", False))}
                self.prefix_position = position
                self.prefix_exhausted = bool(entry.get("exhausted", False))
            elif self.kind == "grouped":
                groups = entry.get("groups", [])
                if not isinstance(groups, list):
                    raise RecoveryContractError("Grouped snapshot groups must be a list")
                payload = []
                for group in groups:
                    rows, used = self._read_rows(group.get("data"), slot, check_size, limit, used)
                    payload.append((_decode_group_key(group.get("key")), rows, group.get("deadline_ms", 0)))
                restored[slot] = payload
            else:
                rows, used = self._read_rows(entry.get("data"), slot, check_size, limit, used)
                if self.kind == "fixed_batch" and len(rows) >= self.stage.max_batch:
                    raise RecoveryContractError("Fixed batch state exceeds its original group")
                restored[slot] = rows
        self._restored = restored
        self._restored_ordinals = ordinals
        self.slot_ordinals = dict(ordinals)
        self.stage.batch_ordinal = ordinals[0]

    def _read_rows(self, descriptor, slot, check_size, limit, used):
        if descriptor is None:
            return [], used
        reader = self._sink(slot)
        rows = []
        for row in reader.iter_rows(descriptor):
            used += check_size(row, self.run.limits["max_row_bytes"]) + 16
            if used > limit:
                raise RecoveryContractError("Buffer recovery exceeds byte budget")
            rows.append(row)
        return rows, used

    def close(self):
        """Idempotently release staging buffers; committed references are kept."""
        if self.closed:
            return
        self.closed = True
        for sink in self._sinks.values():
            sink.reset()
        self._sinks = {}
        self._restored = {}
        self._restored_ordinals = {}

    def _check_slot(self, slot):
        if type(slot) is not int or slot < 0 or slot >= self.participants:
            raise RecoveryContractError("Invalid buffer worker slot")


def make_stage_buffer_adapter(stage, *, action, stage_key, restored=None):
    """Build the unique adapter for a known native buffer kind, or None.

    Row-local stages return None. An unknown buffered shape is not bypassed: the
    caller's kind guard rejects it before this factory is reached. When a verified
    ``restored`` snapshot is supplied it is rehydrated through the same validation.
    """
    kind = classify_stage_buffer(stage)
    if kind is None:
        return None
    adapter = StageBufferAdapter(stage, action=action, stage_key=stage_key)
    if restored is not None:
        adapter.restore(restored)
    return adapter
