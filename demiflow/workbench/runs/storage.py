"""Account native owned artifacts together with the Forge execution unit.

Logical object locations come from the checked resource closure. Physical
namespaces and orphan decisions remain owned by the one native RunJournal.
"""

import fcntl
import hashlib
import json
import os
from pathlib import Path
import stat
import time
import uuid

from demiflow.forge.io import no_symlinks
from demiflow.forge.schema import ForgeError
from demiflow.execution.run_journal import RunJournal





from demiflow.execution.run_reader import read_run, storage_snapshot, RunReadError


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
        try:
            with read_run(target) as reader:
                manifest = reader.document('manifest', 8 * 1024**2)
        except RunReadError as exc:
            raise ForgeError(str(exc), code=exc.code or 'operation_rejected') from exc
        if manifest is None:
            raise ForgeError("Historical run manifest exceeds budget")
        allowed = object_locations(manifest=manifest, target=target)
    binding = journal.recovery_identity()
    return _checked_owned_objects(records, allowed, binding)


def _checked_owned_objects(records, allowed, binding):
    """One namespace validator for ordinary native polls and history admission."""
    if not records:
        return []
    native_id = _native_run_id(binding)
    from demiflow.execution.publication_facts import object_namespace
    paths = []
    for record in records:
        logical = record["logical_root"]
        if logical not in allowed or record != object_namespace(native_id, logical):
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
