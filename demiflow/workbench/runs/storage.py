"""Account native owned artifacts together with the Forge execution unit.

Logical object locations come from the checked resource closure. Physical
namespaces and orphan decisions remain owned by the one native RunJournal.
"""

import os
from pathlib import Path
import stat
import time

from demiflow.forge.io import no_symlinks
from demiflow.forge.schema import ForgeError
from demiflow.execution.host.inspection import observe_storage, RunReadError


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
    try:
        with observe_storage(target) as observation:
            footprint = observation.native_footprint()
            if not footprint['object_roots']:
                return []
            if allowed is None:
                manifest = observation.snapshot()['manifest']
                if manifest is None:
                    raise ForgeError("Owned objects have no historical manifest")
                allowed = object_locations(manifest=manifest, target=target)
            return _checked_owned_objects(footprint['object_roots'], allowed)
    except RunReadError as exc:
        raise ForgeError(str(exc), code=exc.code or 'operation_rejected') from exc


def _checked_owned_objects(records, allowed):
    """Authorize native-validated ranges against this product's locations."""
    paths = []
    for record in records:
        if record['logical_root'] not in allowed:
            raise ForgeError("Owned object namespace differs from checked locations")
        path = no_symlinks(Path(record['physical_root']))
        if path not in paths:
            paths.append(path)
    return paths


def history_objects(snapshot, target):
    """Resolve only the namespaces from a bounded native storage snapshot."""
    if not snapshot['object_roots']:
        return []
    manifest = snapshot['manifest']
    if manifest is None:
        raise ForgeError('Owned objects have no historical manifest')
    allowed = object_locations(manifest=manifest, target=target)
    return _checked_owned_objects(snapshot['object_roots'], allowed)


def scratch_bytes(target, work, allowed):
    try:
        with observe_storage(target) as observation:
            footprint = observation.native_footprint()
            roots = [work, *(target / name for name in footprint['relative_roots']),
                     *_checked_owned_objects(footprint['object_roots'], allowed)]
    except RunReadError as exc:
        raise ForgeError(str(exc), code=exc.code or 'operation_rejected') from exc
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


def cleanup_deadline():
    """One existing control-operation budget, starting after process draining."""
    from demiflow.forge.operation_budget import child_deadline, OPERATION_SECONDS
    return child_deadline(OPERATION_SECONDS)


def remove_attempt_work(work, *, deadline):
    """Remove only a caller-proven stopped attempt's private directory."""
    from demiflow.forge.operation_budget import check
    work = no_symlinks(Path(work).absolute())
    check(deadline)
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    parent = os.open('/', flags)
    try:
        # Every component is resolved from its already-held parent. A replaced
        # ancestor cannot redirect this cleanup into a foreign work directory.
        for part in work.parent.parts[1:]:
            check(deadline)
            previous = parent
            parent = os.open(part, flags, dir_fd=previous)
            os.close(previous)
    except FileNotFoundError:
        os.close(parent)
        return
    except BaseException:
        os.close(parent)
        raise
    count = 0

    def remove(parent_fd, name, depth):
        nonlocal count
        check(deadline)
        count += 1
        if depth > 24 or count > 65536:
            raise ForgeError('Attempt work cleanup exceeds entry/depth budget', code='resource_limit')
        try:
            visible = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            return
        if not (stat.S_ISDIR(visible.st_mode) or stat.S_ISREG(visible.st_mode)):
            raise ForgeError('Attempt work contains a symlink or special entry')
        flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK
        if stat.S_ISDIR(visible.st_mode):
            flags |= os.O_DIRECTORY
        descriptor = os.open(name, flags, dir_fd=parent_fd)
        try:
            held = os.fstat(descriptor)
            identity = (held.st_dev, held.st_ino)
            if identity != (visible.st_dev, visible.st_ino):
                raise ForgeError('Attempt work identity changed')
            if stat.S_ISDIR(held.st_mode):
                with os.scandir(descriptor) as entries:
                    for entry in entries:
                        check(deadline)
                        remove(descriptor, entry.name, depth + 1)
            check(deadline)
            visible = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            if identity != (visible.st_dev, visible.st_ino):
                raise ForgeError('Attempt work identity changed during cleanup')
            if stat.S_ISDIR(held.st_mode):
                os.rmdir(name, dir_fd=parent_fd)
            else:
                os.unlink(name, dir_fd=parent_fd)
        finally:
            os.close(descriptor)

    try:
        remove(parent, work.name, 0)
    finally:
        os.close(parent)


def clean_stopped(run, *, expected_revision=None, confirm_stopped, deadline, preserve_jobs=False):
    """Delegate native ownership and GC exclusively to their original owner."""
    from demiflow.execution.host.control import clean_stopped_execution
    from demiflow.execution.host.control import local_capacity_domain
    from demiflow.execution.host.inspection import read_run
    from contextlib import nullcontext
    with read_run(run.directory, deadline=deadline) as reader:
        manifest = reader.execution_view(detail='identity')['manifest']
    domain = manifest.get('capacity_domain')
    with local_capacity_domain(domain) if domain is not None else nullcontext():
        return clean_stopped_execution(run, expected_attempt=run.attempt,
            expected_revision=expected_revision, confirm_stopped=confirm_stopped, deadline=deadline,
            preserve_jobs=preserve_jobs)
