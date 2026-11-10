"""Same-host Ray Core data-plane carriage: actor-method frames + ray_object (U3/S5).

The provider-independent K5 protocol (data/barrier/eoi/credit frames, InputGate,
credit, seal) is unchanged from carriage_local; only "how a frame is delivered"
differs. A physical channel is one async Ray actor holding two asyncio queues
(producer->consumer data, consumer->producer credit), so a full-duplex Local
socketpair maps to actor-method push/pop. Small records ride the frame inline
(actor method 直送 envelope); large records ``ray.put`` to a K4 DataRef
(kind=ray_object, ephemeral) and the consumer ``ray.get`` reads them directly from
the same-host plasma store — the business payload never transits the driver (D3).

The record helpers (inline_records/ref_records/expand_records) are re-exported from
carriage_local: record identity and ref expansion are provider-neutral. Cross-host
read domains stay rejected (U6); this is single-host semantic-adaptation evidence.
"""
import asyncio
import hashlib
import pickle
from pathlib import Path

import ray

from ..contracts.errors import ProtocolError
from ..contracts.exchange import (
    build_data_ref, check_read_domain, validate_data_ref, DATA_REF_SCHEMA,
)
from ..recovery_contract import RecoveryContractError, identifier
from .carriage_local import expand_records, inline_records, ref_records

__all__ = ["RayChannelLink", "ray_channel_pair", "RayObjectStore", "ray_read_domain",
           "inline_records", "ref_records", "expand_records"]

_CLOSE_FRAME = {"kind": "__ray_channel_close__"}


@ray.remote(max_restarts=0, max_task_retries=0)
class _RayChannelQueue:
    """One physical channel: async data + credit queues over a single actor.

    Async methods let a blocked ``pop`` suspend while the peer ``push`` runs on the
    same actor event loop, reproducing the full-duplex socket without a thread per
    direction. ``shutdown`` releases both blocked pops with a close sentinel.
    """

    def __init__(self):
        self._data = asyncio.Queue()
        self._credit = asyncio.Queue()
        self._closed = False

    async def push_data(self, event):
        if self._closed:
            return False
        await self._data.put(event)
        return True

    async def pop_data(self):
        return await self._data.get()

    async def push_credit(self, event):
        if self._closed:
            return False
        await self._credit.put(event)
        return True

    async def pop_credit(self):
        return await self._credit.get()

    async def shutdown(self):
        if self._closed:
            return
        self._closed = True
        await self._data.put(_CLOSE_FRAME)
        await self._credit.put(_CLOSE_FRAME)


class RayChannelLink:
    """One endpoint of a Ray physical channel; same surface as LocalChannelLink.

    ``role`` selects the direction: the producer sends data and receives credit;
    the consumer sends credit and receives data. ``recv`` blocks the calling thread
    (a router sender/reader thread) on the async actor pop until a frame or the
    close sentinel arrives, then returns None on close like a socket EOF.
    """

    def __init__(self, actor, role):
        if role not in ("producer", "consumer"):
            raise ValueError("role must be producer or consumer")
        self._actor = actor
        self._role = role
        self._closed = False

    def send(self, event):
        method = (self._actor.push_data if self._role == "producer"
                  else self._actor.push_credit)
        if not ray.get(method.remote(event)):
            raise RecoveryContractError("Ray channel is closed")

    def recv(self):
        method = (self._actor.pop_credit if self._role == "producer"
                  else self._actor.pop_data)
        frame = ray.get(method.remote())
        if frame is None or frame.get("kind") == _CLOSE_FRAME["kind"]:
            return None
        return frame

    def shutdown(self):
        try:
            ray.get(self._actor.shutdown.remote(), timeout=5)
        except Exception:
            pass

    def close(self):
        self._closed = True


def ray_channel_pair(channel_id):
    """Create the named channel actor; return (producer_link, consumer_link, name)."""
    identifier(channel_id)
    name = "demiflow-chan-" + channel_id
    actor = _RayChannelQueue.options(name=name, get_if_exists=True, num_cpus=0).remote()
    return RayChannelLink(actor, "producer"), RayChannelLink(actor, "consumer"), name


def ray_read_domain(directory, host_id):
    return "ray://" + host_id + ":" + str(Path(directory).resolve())


class RayObjectStore:
    """Large-payload carriage via Ray plasma: ray.put refs, consumer ray.get direct.

    Mirrors LocalSegmentStore.write/read so the provider-neutral router/reader and
    ref_records/expand_records work unchanged. The producer pins each ObjectRef so
    the object stays alive until the consumer reads it; the consumer reconstructs
    the ObjectRef from its hex location and reads the same-host plasma directly
    (never through the driver). Bytes + sha256 are verified before unpickling.
    """

    def __init__(self, directory, *, host_id):
        self._directory = Path(directory).resolve()
        self._domain = ray_read_domain(self._directory, host_id)
        self._objects = {}

    @property
    def domain(self):
        return self._domain

    def write(self, rows, *, scope_ref, durability="ephemeral"):
        identifier(scope_ref)
        if durability != "ephemeral":
            raise ValueError("ray_object carriage is provider-transient (ephemeral)")
        raw = pickle.dumps(list(rows), protocol=4)
        obj = ray.put(raw)
        location = obj.hex()
        self._objects[location] = obj
        digest = hashlib.sha256(raw).hexdigest()
        ref = build_data_ref(kind="ray_object", read_domain=self._domain, location=location,
                             payload_bytes=len(raw), sha256=digest, durability="ephemeral",
                             scope_ref=scope_ref)
        descriptor = {"blocks": [{"path": location, "bytes": len(raw), "sha256": digest}],
                      "rows": len(rows)}
        return descriptor, [ref]

    def _resolve(self, ref):
        validate_data_ref(ref)
        check_read_domain(ref, self._domain)
        if ref["kind"] != "ray_object":
            raise RecoveryContractError("Ray object carriage reads ray_object refs only")
        return self._objects.get(ref["location"]) or ray.ObjectRef(bytes.fromhex(ref["location"]))

    def read(self, refs):
        rows = []
        for ref in refs:
            raw = ray.get(self._resolve(ref))
            if len(raw) != ref["bytes"] or hashlib.sha256(raw).hexdigest() != ref["sha256"]:
                raise ProtocolError("ARTIFACT_MISMATCH", "ray_object digest differs")
            rows.extend(pickle.loads(raw))
        return rows

    def release(self, refs):
        """Free pinned plasma objects once every reader has consumed past them."""
        freed = []
        for ref in refs:
            obj = self._objects.pop(ref["location"], None)
            if obj is not None:
                freed.append(obj)
        if freed:
            ray.internal.free(freed)
        return [ref["location"] for ref in refs]