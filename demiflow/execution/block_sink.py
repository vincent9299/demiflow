"""Native bounded materialization sink, shared by source capture and results."""
from pathlib import Path
import hashlib
import json
import pickle
import stat
import struct
import uuid

from ..data.datasink import Datasink
from ..data.runtime_contract import check_size
from .recovery_contract import document, RecoveryContractError, identifier, identity
from .state_artifacts import atomic_bytes

BLOCK_BYTES = 4 * 1024**2
_CANON_MAX_DEPTH = 64


def storage_usage(context):
    """Indexed reserved/settled bytes; no history or directory scan on writes."""
    return context.journal.storage_usage()


def _canonical_row(value):
    """Rebuild a row as a value-canonical graph so re-prepare reserves identical bytes."""
    leaves, containers, stack, fallback = {}, {}, set(), [False]

    def walk(node, depth):
        if fallback[0]:
            return node, None
        # Fallback is value-determined: only depth overflow or a true cycle, never a caught defect.
        if depth > _CANON_MAX_DEPTH:
            fallback[0] = True
            return node, None
        if isinstance(node, str):
            return leaves.setdefault(('s', node), node), ('s', node)
        if isinstance(node, bytes):
            return leaves.setdefault(('b', node), node), ('b', node)
        if isinstance(node, (dict, list, tuple)):
            nid = id(node)
            if nid in stack:
                fallback[0] = True
                return node, None
            stack.add(nid)
            if isinstance(node, dict):
                keys, vals = [], []
                for k, v in node.items():
                    ck, sk = walk(k, depth + 1)
                    cv, sv = walk(v, depth + 1)
                    if fallback[0]:
                        stack.discard(nid)
                        return node, None
                    keys.append((ck, sk))
                    vals.append((cv, sv))
                stack.discard(nid)
                sig = ('d', tuple((sk, sv) for (_, sk), (_, sv) in zip(keys, vals)))
                if sig in containers:
                    return containers[sig], sig
                out = {ck: cv for (ck, _), (cv, _) in zip(keys, vals)}
                containers[sig] = out
                return out, sig
            kids = []
            for v in node:
                cv, sv = walk(v, depth + 1)
                if fallback[0]:
                    stack.discard(nid)
                    return node, None
                kids.append((cv, sv))
            stack.discard(nid)
            if isinstance(node, list):
                sig = ('l', tuple(sv for _, sv in kids))
                if sig in containers:
                    return containers[sig], sig
                out = [cv for cv, _ in kids]
                containers[sig] = out
                return out, sig
            sig = ('t', tuple(sv for _, sv in kids))
            if sig in containers:
                return containers[sig], sig
            out = tuple(cv for cv, _ in kids)
            containers[sig] = out
            return out, sig
        if node is None or isinstance(node, (int, bool)):
            return node, ('v', type(node).__name__, node)
        # IEEE-754 bytes, not Python ==: -0.0/0.0 and distinct NaN payloads must
        # never collapse a container signature (C06 value fidelity).
        if isinstance(node, float):
            return node, ('f', struct.pack('>d', node))
        if isinstance(node, complex):
            return node, ('c', struct.pack('>dd', node.real, node.imag))
        # Opaque objects pass through; an identity sig keeps distinct objects distinct (out of value-canonical scope).
        return node, ('o', id(node))

    result, _ = walk(value, 0)
    return value if fallback[0] else result


class BlockSink(Datasink):
    """Data receipts live with data; execution progress lives only in RunJournal."""
    def __init__(self, action, sink_id="output", *, input_capture=False, staging_only=False, output_buffer_bytes=None):
        self.action, self.run, self.sink_id = action, action.context, identifier(sink_id)
        self.root = self.run.directory / "materializations" / action.action_id / sink_id
        self.input_capture = input_capture
        self.block_bytes = min(BLOCK_BYTES, self.run.limits["max_batch_bytes"])
        if output_buffer_bytes is not None:
            self.block_bytes = min(self.block_bytes, output_buffer_bytes)
        self.staging_only=staging_only
        self.reset()

    def reset(self):
        self.generation = uuid.uuid4().hex
        self.buffer, self.blocks = [], []
        self.buffer_bytes = self.rows = 0
        self.manifest = None
        self.block_count = 0

    def detach(self):
        """Transfer this epoch's buffers to a private publication participant."""
        from copy import copy
        sealed = copy(self)
        self.reset()
        return sealed

    def write(self, blocks, context):
        raise RecoveryContractError("BlockSink must use the native commit protocol")

    def append(self, row):
        self.run.check_stop()
        check_size(row, self.run.limits["max_row_bytes"])
        raw = pickle.dumps(_canonical_row(row), protocol=4)
        if len(raw) > self.run.limits["max_row_bytes"]:
            raise RecoveryContractError("Encoded row exceeds byte budget")
        if self.buffer and self.buffer_bytes + len(raw) > self.block_bytes:
            self._flush()
        self.buffer.append(raw)
        self.buffer_bytes += len(raw)
        self.rows += 1
        maximum = self.run.limits["max_input_rows" if self.input_capture else "max_output_rows"]
        if self.rows > maximum:
            raise RecoveryContractError("Materialization row budget exceeded")
        if self.buffer_bytes >= self.block_bytes:
            self._flush()

    def _flush(self):
        if not self.buffer:
            return
        payload = b"".join(self.buffer)
        path = self.root / self.generation / (str(self.block_count) + ".pkl")
        self.run.allocate_artifact(path, size=len(payload))
        with self.run.journal.publication_guard(self.run.fence):
            # Object staging and row staging use this same fence/transaction.
            # Admission cannot race with another native writer's allocation.
            atomic_bytes(path, payload)
        self.run.release_temporaries([path.with_name(path.name + ".staging")])
        self.blocks.append(dict(path=str(path.relative_to(self.run.directory)),
            bytes=len(payload), sha256=hashlib.sha256(payload).hexdigest()))
        self.block_count += 1
        if len(self.blocks) >= 256:
            from .artifact_manifest import ArtifactManifest
            if self.manifest is None:
                self.manifest = ArtifactManifest(self.run, self.root)
            self.manifest.append(self.blocks)
            self.blocks = []
        self.buffer, self.buffer_bytes = [], 0

    def prepare(self, blocks, context):
        if blocks is not None:
            for row in blocks:
                self.append(row)
        self._flush()
        descriptor = self.descriptor()
        if not self.input_capture and not self.staging_only:
            # Commit-keyed + ordinal: a window-3 re-prepare reuses the journal's idempotent charge.
            for ordinal, block in enumerate(self.block_entries(descriptor)):
                self.run.reserve_output('row_bytes', block['bytes'],
                    reservation_id=identity({'commit': context.commit_id, 'sink': self.sink_id,
                                             'component': 'row_bytes', 'block': ordinal}))
            self.run.reserve('output_rows', self.rows, ceiling=self.run.limits['max_output_rows'],
                reservation_id=identity({'commit': context.commit_id, 'sink': self.sink_id,
                                         'component': 'output_rows'}))
        self.run.allocate_artifact(self._marker(context), commit_id=context.commit_id)
        return descriptor

    def descriptor(self):
        self._flush()
        if self.manifest is not None:
            if self.blocks:
                self.manifest.append(self.blocks)
                self.blocks = []
            blocks = self.manifest.seal()
        else:
            blocks = list(self.blocks)
        return dict(blocks=blocks, rows=self.rows)

    def block_entries(self, prepared):
        from .artifact_manifest import entries
        return entries(self.run, prepared['blocks'])

    def _marker(self, context):
        return self.root / (identifier(context.commit_id) + ".json")

    def _validate(self, prepared):
        if type(prepared) is not dict or set(prepared) != {"blocks", "rows"}:
            raise RecoveryContractError("Invalid block preparation")
        if type(prepared["rows"]) is not int or prepared["rows"] < 0:
            raise RecoveryContractError("Invalid materialization row count")
        for block in self.block_entries(prepared):
            relative = Path(block["path"])
            path = self.run.directory / relative
            if relative.is_absolute() or ".." in relative.parts or not path.is_relative_to(self.root):
                raise RecoveryContractError("Block reference escapes its sink")
            if any(p.is_symlink() for p in (path, *path.parents)):
                raise RecoveryContractError("Block reference contains a symlink")
            size = block["bytes"]
            if type(size) is not int or not 0 <= size <= max(self.block_bytes, self.run.limits["max_row_bytes"]):
                raise RecoveryContractError("Block reference exceeds budget")
            if not path.is_file() or path.stat().st_size != size:
                raise RecoveryContractError("Committed materialization block size differs")
            from .artifact_proof import verify_artifact
            verify_artifact(self.run, path, size, block['sha256'])

    def inspect(self, prepared, context):
        self._validate(prepared)
        marker = self._marker(context)
        if not marker.exists():
            return None
        if marker.is_symlink() or marker.stat().st_size > 1024**2:
            raise RecoveryContractError("Invalid materialization receipt")
        value = json.loads(marker.read_bytes())
        expected = dict(commit_id=context.commit_id, sink_id=self.sink_id,
                        status="committed", data=prepared)
        if value != expected:
            raise RecoveryContractError("Materialization receipt differs")
        return value

    def commit(self, prepared, context):
        self._validate(prepared)
        receipt = dict(commit_id=context.commit_id, sink_id=self.sink_id,
                       status="committed", data=prepared)
        existing = self.inspect(prepared, context)
        if existing is not None:
            return existing
        raw = document(receipt)
        self.run.allocate_artifact(self._marker(context), commit_id=context.commit_id, size=len(raw))
        atomic_bytes(self._marker(context), raw)
        return receipt

    def abort(self, prepared, context):
        if self._marker(context).exists():
            return
        self._validate(prepared)
        for block in self.block_entries(prepared):
            (self.run.directory / block["path"]).unlink(missing_ok=True)

    def iter_rows(self, prepared):
        self._validate(prepared)
        count = 0
        for block in self.block_entries(prepared):
            with (self.run.directory / block["path"]).open("rb") as stream:
                while stream.tell() < block["bytes"]:
                    row = pickle.load(stream)
                    check_size(row, self.run.limits["max_row_bytes"])
                    count += 1
                    if count > prepared["rows"]:
                        raise RecoveryContractError("Materialized row count differs")
                    yield row
        if count != prepared["rows"]:
            raise RecoveryContractError("Materialized row count differs")
