"""Native bounded materialization sink, shared by source capture and results."""
from pathlib import Path
import hashlib
import json
import pickle
import stat
import uuid

from ..data.datasink import Datasink
from ..data.runtime_contract import check_size
from .recovery_contract import document, RecoveryContractError, identifier
from .state_artifacts import atomic_bytes

BLOCK_BYTES = 4 * 1024**2


def storage_usage(context):
    """One aggregate scratch/state/materialization budget, with bounded scans."""
    size = count = 0
    roots=[context.directory/name for name in ('state','materializations','scratch')]
    object_roots=[]
    for record in context.journal.object_roots():
        physical=Path(record['physical_root'])
        if not any(physical.is_relative_to(root) for root in roots):
            roots.append(physical)
            object_roots.append(physical)
    non_objects=0
    for root in roots:
        if not root.exists():
            continue
        pending = [root]
        while pending:
            path = pending.pop()
            try:
                info = path.lstat()
            except FileNotFoundError:
                # Decoder workers can remove completed private scratch while
                # another writer admits storage. Deletion cannot add usage;
                # durable objects/blocks are still verified by their receipts.
                continue
            if stat.S_ISLNK(info.st_mode):
                raise RecoveryContractError("Owned runtime storage contains a symlink")
            if stat.S_ISDIR(info.st_mode):
                try:
                    for child in path.iterdir():
                        count += 1
                        if count > context.limits["max_artifacts"]:
                            raise RecoveryContractError("Runtime artifact count exceeds budget")
                        pending.append(child)
                except FileNotFoundError:
                    continue  # The same completed scratch-directory cleanup.
            elif stat.S_ISREG(info.st_mode):
                size += info.st_size
                if not any(path.is_relative_to(r) for r in object_roots):
                    non_objects+=info.st_size
                if size > context.limits["max_scratch_bytes"]:
                    raise RecoveryContractError("Runtime storage exceeds scratch budget")
            else:
                raise RecoveryContractError("Unknown runtime storage entry")
    size=max(size,non_objects+context.journal.budget_consumed('image_scratch_admission_bytes'))
    if size>context.limits['max_scratch_bytes']:
        raise RecoveryContractError('Runtime storage exceeds scratch admission budget')
    return size


class BlockSink(Datasink):
    """Data receipts live with data; execution progress lives only in RunJournal."""
    def __init__(self, action, sink_id="output", *, input_capture=False, staging_only=False):
        self.action, self.run, self.sink_id = action, action.context, identifier(sink_id)
        self.root = self.run.directory / "materializations" / action.action_id / sink_id
        self.input_capture = input_capture
        self.block_bytes = min(BLOCK_BYTES, self.run.limits["max_batch_bytes"])
        self.staging_only=staging_only
        self.reset()

    def reset(self):
        self.generation = uuid.uuid4().hex
        self.buffer, self.blocks = [], []
        self.buffer_bytes = self.rows = 0

    def write(self, blocks, context):
        raise RecoveryContractError("BlockSink must use the native commit protocol")

    def append(self, row):
        self.run.check_stop()
        check_size(row, self.run.limits["max_row_bytes"])
        raw = pickle.dumps(row, protocol=4)
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
        if len(self.blocks) >= 1024:
            raise RecoveryContractError("Materialization block count exceeds budget")
        payload = b"".join(self.buffer)
        path = self.root / self.generation / (str(len(self.blocks)) + ".pkl")
        if not self.input_capture and not self.staging_only:
            self.run.reserve_output('row_bytes', len(payload),
                reservation_id='rows-' + hashlib.sha256(str(path.relative_to(self.run.directory)).encode()).hexdigest())
        self.run.allocate_artifact(path)
        with self.run.journal.execution_guard(self.run.fence):
            # Object staging and row staging use this same fence/transaction.
            # Admission cannot race with another native writer's allocation.
            if storage_usage(self.run) + len(payload) > self.run.limits["max_scratch_bytes"]:
                raise RecoveryContractError("Materialization exceeds scratch budget")
            atomic_bytes(path, payload)
        self.blocks.append(dict(path=str(path.relative_to(self.run.directory)),
            bytes=len(payload), sha256=hashlib.sha256(payload).hexdigest()))
        self.buffer, self.buffer_bytes = [], 0

    def prepare(self, blocks, context):
        if blocks is not None:
            for row in blocks:
                self.append(row)
        self._flush()
        if not self.input_capture and not self.staging_only:
            self.run.reserve('output_rows', self.rows, ceiling=self.run.limits['max_output_rows'])
        self.run.allocate_artifact(self._marker(context), commit_id=context.commit_id)
        return dict(blocks=list(self.blocks), rows=self.rows)

    def _marker(self, context):
        return self.root / (identifier(context.commit_id) + ".json")

    def _validate(self, prepared):
        if type(prepared) is not dict or set(prepared) != {"blocks", "rows"}:
            raise RecoveryContractError("Invalid block preparation")
        if type(prepared["blocks"]) is not list or len(prepared["blocks"]) > 1024:
            raise RecoveryContractError("Block reference count exceeds budget")
        if type(prepared["rows"]) is not int or prepared["rows"] < 0:
            raise RecoveryContractError("Invalid materialization row count")
        for block in prepared["blocks"]:
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
            digest = hashlib.sha256()
            with path.open("rb") as stream:
                for raw in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(raw)
            if digest.hexdigest() != block["sha256"]:
                raise RecoveryContractError("Committed materialization block content differs")

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
        if storage_usage(self.run) + len(raw) > self.run.limits["max_scratch_bytes"]:
            raise RecoveryContractError("Materialization receipt exceeds scratch budget")
        atomic_bytes(self._marker(context), raw)
        return receipt

    def abort(self, prepared, context):
        if self._marker(context).exists():
            return
        self._validate(prepared)
        for block in prepared["blocks"]:
            (self.run.directory / block["path"]).unlink(missing_ok=True)

    def iter_rows(self, prepared):
        self._validate(prepared)
        count = 0
        for block in prepared["blocks"]:
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
