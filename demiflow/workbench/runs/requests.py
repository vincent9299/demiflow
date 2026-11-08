"""Bounded durable prepare receipts. A request never receives a second run ID."""

from contextlib import contextmanager
from contextlib import closing
import fcntl
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import time
import uuid

from demiflow.forge.io import no_symlinks
from demiflow.forge.io import read_json
from demiflow.forge.io import atomic_write_json
from demiflow.forge.io import fsync_directory
from demiflow.forge.platform import content_digest
from demiflow.forge.schema import ForgeError
from demiflow.forge import operation_budget

FORMAT = "demiflow_forge_prepare_request_v1"
DAY = 86400


def request_id():
    return f"p{int(time.time()*1000):013d}-{uuid.uuid4().hex}"


def validate(identity):
    if type(identity) is not str or not re.fullmatch(
        r"p[0-9]{13}-[0-9a-f]{32}", identity
    ):
        raise ForgeError("Invalid preparation request identity", code="invalid_request")
    return int(identity[1:14]) / 1000


@contextmanager
def locked(root):
    parent = Path(root) / ".forge/requests"
    no_symlinks(parent)
    parent.mkdir(mode=0o700, exist_ok=True)
    fsync_directory(parent.parent)
    fd = os.open(
        parent / "requests.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600
    )
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ForgeError(
                "Preparation receipt is busy; retry the same request ID", code="busy"
            ) from None
        yield parent
    finally:
        os.close(fd)


def read(root, identity, *, allow_expired=False):
    created = validate(identity)
    if not allow_expired and time.time() - created > 7 * DAY:
        raise ForgeError(
            "Preparation request expired; it cannot create a new run", code="conflict"
        )
    value = read_json(
        Path(root) / ".forge/requests" / (identity + ".json"), maximum=4096
    )
    if value.get("format") != FORMAT or value.get("request_id") != identity:
        raise ForgeError("Invalid preparation receipt")
    return value


def _capacity(parent):
    count = pending = total = 0
    now = time.time()
    with os.scandir(parent) as entries:
        for entry in entries:
            operation_budget.check()
            count += 1
            if (
                count > 4097
                or entry.is_symlink()
                or not entry.is_file(follow_symlinks=False)
            ):
                raise ForgeError(
                    "Preparation receipt capacity exceeded", code="resource_limit"
                )
            if entry.name == "requests.lock":
                continue
            # Crash leftovers are small but counted and never silently promoted.
            if entry.stat().st_size > 4096:
                raise ForgeError("Preparation receipt exceeds byte budget")
            total += entry.stat().st_size
            if entry.name.endswith(".json"):
                identity = entry.name[:-5]
                created = validate(identity)
                value = read_json(Path(entry.path), maximum=4096)
                if now - created > 7 * DAY and value.get("status") in {
                    "ready",
                    "failed",
                }:
                    Path(entry.path).unlink()
                    count -= 1
                elif value.get("status") == "pending":
                    pending += 1
    fsync_directory(parent)
    if count >= 4097 or pending >= 16 or total + 4096 > 16 * 1024**2:
        raise ForgeError("Preparation receipt capacity exceeded", code="resource_limit")


def prepare(root, entry, arguments, operation, identity):
    operation_budget.check()
    created = validate(identity)
    fingerprint = content_digest(dict(entry=entry, **arguments))
    with locked(root) as parent:
        path = parent / (identity + ".json")
        if path.exists():
            value = read(root, identity)
            if value["fingerprint"] != fingerprint:
                raise ForgeError(
                    "Request ID was used with different preparation inputs",
                    code="conflict",
                )
            if value["status"] != "ready":
                raise ForgeError(
                    "Preparation is "
                    + value["status"]
                    + "; inspect/reconcile the same request ID",
                    code="conflict",
                )
            from demiflow.workbench.runs.manager import show

            return show(root, value["run_id"], include_report=True)
        if not -300 <= time.time() - created <= DAY:
            raise ForgeError(
                "Unknown preparation request is outside its creation window",
                code="conflict",
            )
        _capacity(parent)
        operation_budget.check()
        value = dict(
            format=FORMAT,
            request_id=identity,
            fingerprint=fingerprint,
            run_id="r" + uuid.uuid4().hex[:24],
            status="pending",
            created_at=time.time(),
        )
        atomic_write_json(path, value)  # fsync file + directory BEFORE any run exists.
        try:
            operation_budget.check()
            result = operation(
                "run_prepare",
                root,
                entry=entry,
                **arguments,
                reserved_run_id=value["run_id"],
                prepare_request_id=identity,
            )
        except Exception as exc:
            # A timeout/crashed inspector may have produced a sealed directory.
            # Retain pending for explicit reconciliation; never destroy execution.
            from demiflow.workbench.runs.manager import directory

            value.update(
                status=(
                    "pending" if directory(root, value["run_id"]).exists() else "failed"
                ),
                error=str(exc)[:512],
            )
            atomic_write_json(path, value)
            raise
        value.update(status="ready", digest=result["digest"])
        atomic_write_json(path, value)
        _ready(root, value)
        return result


def _ready(root, value):
    from demiflow.workbench.runs.manager import directory
    from demiflow.workbench.runs.manager import RunJournal

    journal = RunJournal(directory(root, value["run_id"]), attempt=1)
    journal.transition({"prepared"}, "prepared", preparation_ready=True)


def reconcile(root, identity):
    from demiflow.workbench.worker import operation
    from demiflow.workbench.runs.domain import preparation_lock

    with preparation_lock() as lease:
        return operation(
            "run_prepare_reconcile", root, identity=identity, _pass_fds=(lease,)
        )


def _incomplete_seal(target, identity, run_id, root):
    """Prove preparation never reached executable state; unknown stays pending."""
    if not target.exists():
        return True
    with os.scandir(target) as entries:
        for count, entry in enumerate(entries):
            if count >= 128 or entry.is_symlink() or entry.name.startswith("unit-"):
                raise ForgeError(
                    "Preparation contains unknown execution/ownership evidence"
                )
    path = target / "run.sqlite"
    if not path.exists():
        return True
    no_symlinks(path)
    if not path.is_file() or path.stat().st_size > 64 * 1024**2:
        raise ForgeError("Preparation journal exceeds budget")
    with closing(
        sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=1)
    ) as db:
        db.execute("PRAGMA cache_size=-2048")
        db.execute("BEGIN")
        tables = {
            row[0]
            for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' LIMIT 33"
            )
        }
        if len(tables) > 32:
            raise ForgeError("Preparation journal shape exceeds budget")

        def document(table, maximum):
            if table not in tables:
                return None
            row = db.execute(
                f"SELECT substr(document,1,{maximum+1}) FROM {table} LIMIT 1"
            ).fetchone()
            if not row:
                return None
            if type(row[0]) is not str or len(row[0].encode()) > maximum:
                raise ForgeError("Preparation control exceeds budget")
            value = json.loads(row[0])
            if type(value) is not dict:
                raise ForgeError("Invalid preparation control")
            return value

        state = document("execution_state", 65536)
        if state is not None:
            if state.get("status") is not None:
                return False
            if state != {"status": None, "revision": 0, "attempt": 1}:
                raise ForgeError("Unknown preparation state")
        manifest = document("manifest", 8 * 1024**2)
        if manifest is not None and (
            manifest.get("prepare_request_id") != identity
            or manifest.get("run_id") != run_id
            or manifest.get("root") != str(Path(root).absolute())
        ):
            raise ForgeError("Preparation ownership differs")
        for table in ("action_events", "completion", "stages", "activities"):
            if (
                table in tables
                and db.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone()
            ):
                raise ForgeError("Preparation contains execution facts")
    return True


def _reconcile(root, identity):
    from demiflow.workbench.runs import manager as runs

    # Public reconcile holds the inherited deployment preparation lease. The
    # receipt and workspace execution locks also exclude prepare/start races.
    with locked(root) as parent, runs.execution_lock(root):
        value = read(root, identity, allow_expired=True)
        if value["status"] != "pending":
            return value
        target = runs.directory(root, value["run_id"])
        if _incomplete_seal(target, identity, value["run_id"], root):
            if target.exists():
                runs._tree_bytes(target)  # Bounded shape check before private cleanup.
                shutil.rmtree(target)
                fsync_directory(target.parent)
            value.update(
                status="failed",
                reason="preparation_incomplete",
                reconciled_at=time.time(),
            )
            atomic_write_json(parent / (identity + ".json"), value)
            return value
        target, run = runs._load(root, value["run_id"])
        expired = time.time() - validate(identity) > 7 * DAY
        expiry_closed = (
            expired
            and run["state"]["status"] == "failed"
            and run["state"].get("reason") == "preparation_expired"
            and run["state"].get("execution_stopped") is True
            and run["state"].get("cleanup") == "complete"
        )
        if run["manifest"].get("prepare_request_id") != identity or (
            run["state"]["status"] != "prepared" and not expiry_closed
        ):
            raise ForgeError(
                "Preparation cannot be reconciled without owned prepared evidence"
            )
        if expired:
            # Expiry never grants execution or a fresh identity. Close the
            # unstarted prepared record, preserving its seal as history.
            if not expiry_closed:
                runs.RunJournal(target, attempt=1).transition(
                    {"prepared"},
                    "failed",
                    reason="preparation_expired",
                    execution_stopped=True,
                    cleanup="complete",
                )
            value.update(
                status="failed", reason="preparation_expired", reconciled_at=time.time()
            )
            atomic_write_json(parent / (identity + ".json"), value)
            return value
        runs._check_basis(root, target, run["manifest"])
        runs.durable_tree(target)
        value.update(status="ready", digest=run["manifest"]["digest"])
        atomic_write_json(parent / (identity + ".json"), value)
        _ready(root, value)
        return value
