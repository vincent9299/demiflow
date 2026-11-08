"""Native-produced fixed Case row-ID index, verified without controller Lance.

At most100000 uint64 keys:800 KiB plus a56-byte header. Native workers scan
actual Lance row IDs before publication, sort at most100000 integers (bounded
temporary Python expansion, below8 MiB), reject duplicates and bind the schema.
The immutable object manifest authenticates the index file; row count alone is
never interpreted as the set of valid Lance row IDs.
"""
from __future__ import annotations

from array import array
import hashlib
import os
from pathlib import Path
import stat
import struct

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.arrow_bridge import schema_digest

NAME = "demiflow_case_rowids.bin"
MAGIC = b"DFCASE-ROWIDS-1".ljust(16, b"\0")
HEADER = struct.Struct("!16sQ32s")
ROW = struct.Struct("!Q")
MAX_ROWS = 100000


def build_index(snapshot, directory):
    """Trusted native worker only, after require_job and output row admission."""
    import pyarrow as pa
    rows = snapshot.count_rows()
    c.integer(rows, "Case index rows", MAX_ROWS)
    schema = schema_digest(snapshot.schema)
    keys = array("Q")
    batches = snapshot.scanner(columns=[], with_row_id=True, batch_size=1024, batch_size_bytes=1024*1024,
        batch_readahead=1, fragment_readahead=1, io_buffer_size=1024*1024, scan_in_order=True).to_batches()
    try:
        for batch in batches:
            if batch.num_rows > 1024 or len(keys)+batch.num_rows > rows:
                c.reject("Native Case index scan exceeds its admitted rows", code="resource_limit")
            index = batch.schema.get_field_index("_rowid")
            if index < 0:
                c.reject("Native Case scan omitted its actual row ID", code="operation_failed")
            values = batch.column(index)
            if values.type != pa.uint64() or values.null_count:
                c.reject("Native Case row IDs have an unexpected type/null", code="operation_failed")
            keys.extend(values.to_pylist())  # at most1024 primitive values here
    finally:
        batches.close()
    if len(keys) != rows:
        c.reject("Native Case index scan differs from its row receipt", code="conflict")
    keys = array("Q", sorted(keys))
    if any(keys[index] == keys[index-1] for index in range(1, len(keys))):
        c.reject("Native Case IDs are not unique", code="conflict")
    path = Path(directory)/NAME
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as output:
        output.write(HEADER.pack(MAGIC, rows, bytes.fromhex(schema)))
        # Small writes buffered to8 KiB; no800 KiB duplicate serialization.
        for value in keys:
            output.write(ROW.pack(value))
        output.flush(); os.fsync(output.fileno())
    return dict(rows=rows, schema_sha256=schema, file=NAME)


def contains(path, *, entry, rows, schema_sha256, row_id):
    c.fields(entry, {"bytes", "sha256"}, label="immutable Case index file")
    c.integer(rows, "Case rows", MAX_ROWS); c.sha(schema_sha256); c.sha(entry["sha256"])
    if type(row_id) is not str or len(row_id) > 20 or not row_id.isascii() or not row_id.isdecimal() or str(int(row_id)) != row_id or not 0 <= int(row_id) < 2**64:
        c.reject("Case row ID must be a canonical uint64 string")
    if entry["bytes"] != HEADER.size + rows*ROW.size:
        c.reject("Immutable Case index size differs", code="conflict")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as source:
        before = os.fstat(source.fileno())
        if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_uid != os.geteuid()
                or stat.S_IMODE(before.st_mode) != 0o400 or before.st_size != entry["bytes"]):
            c.reject("Immutable Case index file identity differs", code="conflict")
        digest = hashlib.sha256()
        for chunk in iter(lambda: source.read(65536), b""):
            digest.update(chunk)
        if digest.hexdigest() != entry["sha256"]:
            c.reject("Immutable Case index content differs", code="conflict")
        source.seek(0)
        if source.read(HEADER.size) != HEADER.pack(MAGIC, rows, bytes.fromhex(schema_sha256)):
            c.reject("Immutable Case index source/schema header differs", code="conflict")
        wanted = int(row_id)
        low, high = 0, rows
        while low < high:
            middle = (low+high)//2
            source.seek(HEADER.size + middle*ROW.size)
            raw = source.read(ROW.size)
            if len(raw) != ROW.size:
                c.reject("Immutable Case index read is incomplete", code="conflict")
            current = ROW.unpack(raw)[0]
            if current < wanted:
                low = middle+1
            else:
                high = middle
        found = False
        if low < rows:
            source.seek(HEADER.size + low*ROW.size)
            found = source.read(ROW.size) == ROW.pack(wanted)
        after = os.fstat(source.fileno())
        if (before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
            c.reject("Immutable Case index changed while verifying", code="conflict")
        return found
