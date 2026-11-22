"""Bounded execution metadata shared by native executors and their adapters.

These are storage/effect contracts, not another plan or scheduling API. Payloads
belong to sinks/state artifacts; the run journal only stores bounded references.
"""
from dataclasses import dataclass
import hashlib
import json
import math
import re

from ..data.runtime_contract import RuntimeContractError

PROTOCOL_VERSION = 1
MAX_DOCUMENT_BYTES = 1024 * 1024
MAX_RECORDS = 16384
MAX_SINKS = 64
MAX_STATES = 256
_IDENTIFIER = re.compile(r"[A-Za-z0-9_-]{1,128}\Z")
COMMIT_STATES = ("pending", "prepared", "data_committed", "checkpoint_committed", "acked")


class RecoveryContractError(RuntimeContractError):
    """A replay cannot safely satisfy the recorded execution contract."""


class UnresolvedCommit(RecoveryContractError):
    """An external publication has no conclusive durable outcome."""


def identifier(value):
    if type(value) is not str or not _IDENTIFIER.fullmatch(value):
        raise RecoveryContractError("Invalid execution identity")
    return value


def document(value, maximum=MAX_DOCUMENT_BYTES):
    """Validate before encoding, including depth, node count and finite floats."""
    pending = [(value, 0)]
    nodes = 0
    encoded_upper = 0
    while pending:
        item, depth = pending.pop()
        kind = type(item)
        nodes += 1
        if nodes > 65536 or depth > 32:
            raise RecoveryContractError("Execution metadata exceeds structural budget")
        if kind is dict:
            if len(item) > 16384:
                raise RecoveryContractError("Invalid execution metadata keys")
            encoded_upper += 2 + 4 * len(item)
            for key in item:
                if type(key) is not str:
                    raise RecoveryContractError("Invalid execution metadata keys")
                encoded_upper += 6 * len(key)
            pending.extend((v, depth + 1) for v in item.values())
        elif kind in (list, tuple):
            if len(item) > 16384:
                raise RecoveryContractError("Execution metadata collection exceeds budget")
            encoded_upper += 2 + len(item)
            pending.extend((v, depth + 1) for v in item)
        elif kind is float:
            if not math.isfinite(item):
                raise RecoveryContractError("Nonfinite execution metadata")
            encoded_upper += 32
        elif kind not in (str, int, bool, type(None)):
            raise RecoveryContractError("Execution metadata must be strict JSON")
        elif kind is str:
            if len(item) > maximum:
                raise RecoveryContractError("Execution metadata exceeds byte budget")
            encoded_upper += 2 + 6 * len(item)
        elif kind is int:
            encoded_upper += item.bit_length() // 3 + 3
        else:
            encoded_upper += 5
    if encoded_upper <= maximum:
        # The same structural validation above has proved that the complete
        # encoded object fits before allowing the C encoder to allocate it.
        # Six bytes/codepoint bounds UTF-8 and every JSON escape; integer width
        # uses log10(2) < 1/3. Ambiguous near-budget cases keep the old incremental
        # path below, so this conservative estimate never rejects a valid input.
        return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False,
                          separators=(",", ":")).encode("utf-8")
    chunks, size = [], 0
    for part in json.JSONEncoder(sort_keys=True, ensure_ascii=False, allow_nan=False,
                                 separators=(",", ":")).iterencode(value):
        raw = part.encode("utf-8")
        size += len(raw)
        if size > maximum:
            raise RecoveryContractError(f"Execution metadata exceeds byte budget (encoded_prefix_bytes={size}, limit_bytes={maximum})")
        chunks.append(raw)
    return b"".join(chunks)


def identity(value):
    return hashlib.sha256(document(value)).hexdigest()


@dataclass(frozen=True)
class RunRef:
    directory: str
    run_id: str


@dataclass(frozen=True)
class CommitContext:
    """Passed by the execution owner, never selected by a business row."""
    run_id: str
    action_id: str
    commit_id: str
    attempt: int
    fence: str

    def __post_init__(self):
        for value in (self.run_id, self.action_id, self.commit_id, self.fence):
            identifier(value)
        if type(self.attempt) is not int or self.attempt < 1:
            raise RecoveryContractError("Invalid execution attempt")


@dataclass(frozen=True)
class SourcePosition:
    """Occurrence position in a fixed source, independent of row values/keys."""
    source_id: str
    next_ordinal: int

    def __post_init__(self):
        identifier(self.source_id)
        if type(self.next_ordinal) is not int or not 0 <= self.next_ordinal < 2**63:
            raise RecoveryContractError("Invalid source position")

    def to_dict(self):
        return dict(source_id=self.source_id, next_ordinal=self.next_ordinal)


def validate_receipt(receipt, context, sink_id):
    if type(receipt) is not dict:
        raise RecoveryContractError("A durable sink must return a receipt")
    required = {"commit_id", "sink_id", "status", "data"}
    if set(receipt) != required or receipt["commit_id"] != context.commit_id:
        raise RecoveryContractError("Sink receipt belongs to another commit")
    if receipt["sink_id"] != sink_id or receipt["status"] != "committed":
        raise UnresolvedCommit("Sink has no matching durable commit evidence")
    document(receipt)
    return receipt
