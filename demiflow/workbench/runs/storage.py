"""Account native owned artifacts together with the Forge execution unit.

Logical object locations come from the checked resource closure. Physical
namespaces and orphan decisions remain owned by the one native RunJournal.
"""

from contextlib import closing
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import time
import uuid

from demiflow.forge.io import no_symlinks
from demiflow.forge.schema import ForgeError
from demiflow.execution.run_journal import RunJournal


def _storage_json(raw, *, characters=None):
    """Bound decoded metadata before allocating JSON container objects."""
    if raw is None:
        return None
    from demiflow.execution.recovery_contract import RecoveryContractError
    depth = tokens = 0
    quoted = escaped = False
    for byte in raw:
        if quoted:
            if escaped:
                escaped = False
            elif byte == 92:
                escaped = True
            elif byte == 34:
                quoted = False
        elif byte == 34:
            quoted = True
            tokens += 1
        elif byte in (91, 123):
            depth += 1
            tokens += 1
        elif byte in (93, 125):
            depth -= 1
        elif byte in (44, 58):
            tokens += 1
        if depth > 64 or tokens > 524288:
            raise RecoveryContractError('Historical JSON structural budget exceeded')
    text = raw.decode('utf-8')
    if characters is not None and len(text) > characters:
        raise RecoveryContractError('Historical state exceeds budget')
    def invalid_constant(value):
        raise RecoveryContractError('Invalid historical JSON constant: ' + value)
    value = json.loads(text, parse_constant=invalid_constant)
    if type(value) is not dict:
        raise RecoveryContractError('Invalid historical metadata document')
    return value


def storage_snapshot(directory):
    """Bounded native ownership/control facts in a short read transaction.

    No schema initialization or writer connection. Close the read transaction
    before decoding or traversing storage. Optional runtime tables are absent
    in old journals.
    This is not a filesystem snapshot or a retained usage summary.
    """
    from demiflow.execution.recovery_contract import MAX_DOCUMENT_BYTES, RecoveryContractError

    path = Path(directory) / "run.sqlite"

    def file_identity():
        info = path.stat(follow_symlinks=False)
        if not stat.S_ISREG(info.st_mode) or info.st_size > 64 * 1024**2:
            raise RecoveryContractError('Historical journal type/size exceeds budget')
        return info.st_dev, info.st_ino

    identity = file_identity()
    with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro",
                                 uri=True, timeout=0.1, cached_statements=8)) as db:
        db.execute("PRAGMA cache_size=-2048")
        try:
            db.execute('BEGIN')
            tables = {r[0] for r in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name IN "
                "('manifest','execution_state','runtime_identity','runtime_object_roots') LIMIT 5")}
            if 'execution_state' not in tables:
                raise RecoveryContractError('Historical journal has no execution state table')

            def document(table, maximum):
                if table not in tables:
                    return None
                # Literal table names above; bound each BLOB before it is
                # returned to Python, including corrupt oversized records.
                rows = db.execute(
                    f"SELECT CASE WHEN typeof(document)='text' THEN "
                    f'substr(CAST(document AS BLOB),1,?) END FROM {table} LIMIT 2',
                    (maximum + 1,)).fetchall()
                if len(rows) > 1:
                    raise RecoveryContractError('Ambiguous historical ' + table)
                if not rows:
                    return None
                raw = rows[0][0]
                if not isinstance(raw, bytes) or len(raw) > maximum:
                    raise RecoveryContractError('Historical ' + table + ' exceeds byte budget')
                return raw

            state = document('execution_state', 4 * 65536)
            records = []
            if 'runtime_object_roots' in tables:
                fields = ('root_id', 'logical_root', 'physical_root', 'run_id')
                bounds = (64, 4096, 4096, 32)
                projection = ','.join(
                    f"CASE WHEN typeof({field})='text' THEN "
                    f'substr(CAST({field} AS BLOB),1,{maximum + 1}) END'
                    for field, maximum in zip(fields, bounds))
                rows = db.execute('SELECT ' + projection + ' FROM runtime_object_roots LIMIT 65').fetchall()
                if len(rows) > 64:
                    raise RecoveryContractError('Object resource count exceeds ceiling')
                for row in rows:
                    if any(not isinstance(v, bytes) or len(v) > maximum
                           for v, maximum in zip(row, bounds)):
                        raise RecoveryContractError('Historical object root field exceeds byte budget')
                    record = dict(zip(fields, (v.decode('utf-8') for v in row)))
                    # Same aggregate record limit used by register_object_root.
                    from demiflow.execution.recovery_contract import document as checked_document
                    checked_document(record, 8192)
                    records.append(record)
                # Sort only the bounded result, never an untrusted entire
                # table (legacy/corrupt schemas may lack the native index).
                records.sort(key=lambda record: record['root_id'])
            # Validate/hash complete documents, then retain only storage
            # fields. No large source inventories live across traversal.
            manifest = document('manifest', 8 * 1024**2)
            binding = document('runtime_identity', MAX_DOCUMENT_BYTES)
        finally:
            if db.in_transaction:
                db.rollback()
    if file_identity() != identity:
        raise RecoveryContractError('Historical journal identity changed')
    # Decode after ending the short SQL read transaction. Even a bounded
    # large document must not retain a reader lock during Python parsing.
    def projected(raw, keys, *, characters=None):
        value = _storage_json(raw, characters=characters)
        if value is None:
            return None, None
        result = {key: value[key] for key in keys if key in value}
        return result, hashlib.sha256(raw).hexdigest()

    state, _ = projected(state, ('status', 'attempt'), characters=65536)
    manifest, manifest_digest = projected(manifest, ('run_id', 'object_locations', 'report_digest'))
    binding, identity_digest = projected(binding, ('run_id',))
    return dict(state=state, manifest=manifest, identity=binding, object_roots=records,
                manifest_digest=manifest_digest, identity_digest=identity_digest)


def object_locations(*, report=None, manifest=None, target=None):
    if manifest is not None and "object_locations" in manifest:
        roots = manifest["object_locations"]
    else:
        if (
            report is None
            and target is not None
            and (target / "inspection.json").exists()
        ):
            from demiflow.workbench.runs.manager import _report

            report = _report(target, manifest)
        # The launcher still verifies the required checked seal before execute.
        roots = sorted(
            {
                r["root"]
                for r in (report or {}).get("binding", {}).get("resources", {}).values()
                if r.get("kind") == "objects" and r.get("access") == "write"
            }
        )
    if (
        type(roots) is not list
        or len(roots) > 32
        or any(
            type(p) is not str
            or len(p.encode()) > 4096
            or not Path(p).is_absolute()
            or ".." in Path(p).parts
            for p in roots
        )
    ):
        raise ForgeError("Managed object location budget or identity differs")
    if len(set(roots)) != len(roots):
        raise ForgeError("Duplicate managed object location")
    return [str(no_symlinks(Path(p))) for p in roots]


def owned_objects(target, allowed=None):
    journal = RunJournal(target)
    records = journal.object_roots()
    if not records:
        return []
    if allowed is None:
        with closing(
            sqlite3.connect(
                journal.path.resolve().as_uri() + "?mode=ro", uri=True, timeout=1
            )
        ) as db:
            row = db.execute(
                "SELECT substr(document,1,8388609) FROM manifest"
            ).fetchone()
        if not row or len(row[0].encode()) > 8 * 1024**2:
            raise ForgeError("Historical run manifest exceeds budget")
        allowed = object_locations(manifest=json.loads(row[0]), target=target)
    binding = journal.recovery_identity()
    return _checked_owned_objects(records, allowed, binding)


def _checked_owned_objects(records, allowed, binding):
    """One namespace validator for ordinary native polls and history admission."""
    if not records:
        return []
    native_id = _native_run_id(binding)
    from demiflow.execution.object_resources import namespace_record
    from types import SimpleNamespace

    context = SimpleNamespace(ref=SimpleNamespace(run_id=native_id))
    paths = []
    for record in records:
        logical = record["logical_root"]
        if logical not in allowed or record != namespace_record(context, logical):
            raise ForgeError("Owned object namespace differs from checked locations")
        path = no_symlinks(Path(record["physical_root"]))
        if path not in paths:
            paths.append(path)
    return paths


def _native_run_id(binding):
    if not binding:
        raise ForgeError("Owned objects have no native run identity")
    native_id = binding.get("run_id")
    if (
        type(native_id) is not str
        or len(native_id) != 32
        or any(c not in "0123456789abcdef" for c in native_id)
    ):
        raise ForgeError("Owned objects have invalid native run identity")
    return native_id


def history_objects(snapshot, target):
    """Resolve only the namespaces from a bounded native storage snapshot."""
    if snapshot['identity'] is not None:
        _native_run_id(snapshot['identity'])
    if not snapshot['object_roots']:
        return []
    manifest = snapshot['manifest']
    if manifest is None:
        raise ForgeError('Owned objects have no historical manifest')
    allowed = object_locations(manifest=manifest, target=target)
    return _checked_owned_objects(snapshot['object_roots'], allowed, snapshot['identity'])


def scratch_bytes(target, work, allowed):
    roots = [
        work,
        *(target / name for name in ("state", "materializations", "scratch")),
        *owned_objects(target, allowed),
    ]
    pending = [(no_symlinks(Path(p)), 0) for p in roots]
    total = 0
    seen = set()
    deadline = time.monotonic() + 2
    while pending:
        path, depth = pending.pop()
        if depth > 24 or time.monotonic() > deadline:
            raise ForgeError("Aggregate scratch traversal exceeds time/depth budget")
        try:
            info = path.lstat()
        except FileNotFoundError:
            continue
        key = (info.st_dev, info.st_ino)
        if key in seen:
            continue
        seen.add(key)
        if len(seen) > 65536 or stat.S_ISLNK(info.st_mode):
            raise ForgeError(
                "Aggregate scratch traversal exceeds entry budget or contains symlink"
            )
        if stat.S_ISDIR(info.st_mode):
            try:
                with os.scandir(path) as entries:
                    for entry in entries:
                        if len(pending) + len(seen) >= 65536:
                            raise ForgeError(
                                "Aggregate scratch traversal queue exceeds budget"
                            )
                        pending.append((Path(entry.path), depth + 1))
            except FileNotFoundError:
                continue
        elif stat.S_ISREG(info.st_mode):
            total += info.st_size
        else:
            raise ForgeError("Unexpected native scratch entry")
    return total


def clean_stopped(run):
    """Caller holds the Forge lease and has proven execution children stopped.

    Take the native execution lock before transferring its dead owner solely
    for the existing fenced orphan cleanup. Never execute an action, advance
    progress, start another attempt, or remove a prepared/committed artifact.
    """
    fd = os.open(
        run.directory / ".runtime.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600
    )
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        for _ in range(4):
            state = run.execution_status()
            if state.get("execution_stopped") is True:
                break
            if run.transition(
                {state["status"]}, state["status"], execution_stopped=True
            ):
                break
        else:
            raise ForgeError("Stopped execution state kept changing")
        binding = run.recovery_identity()
        if binding is None:
            return
        fence = uuid.uuid4().hex
        run.claim_execution(binding, fence)
        try:
            run.clean_orphans(fence)
        finally:
            run.release_execution(fence)
    finally:
        os.close(fd)
