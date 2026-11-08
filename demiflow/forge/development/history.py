"""Bounded, read-only views of saved facts, independent of Project validation.

No locks, mkdir, receipt writes, refresh, source snapshots or command execution.
Budgets cover enumeration, JSON complexity, input bytes, elapsed time and output.
"""
import hashlib
import json
import os
from pathlib import Path
import stat
import time

from .project import ForgeError, PAGE_SIZE, binding, digest, file_identity, no_symlinks, safe_id
from .resources import configured_tool_root

MAX_FILE_BYTES = 8 * 1024 * 1024
MAX_TOTAL_BYTES = 32 * 1024 * 1024
MAX_ENTRIES = 10000
MAX_JSON_TOKENS = 200000
MAX_DEPTH = 64
MAX_OUTPUT_BYTES = 1024 * 1024
MAX_SECONDS = 10
ARTIFACTS = {"task": "task.json", "handoff": "handoff.json", "handoff-md": "handoff.md"}


class Budget:
    def __init__(self):
        self.deadline = time.monotonic() + MAX_SECONDS
        self.bytes = 0

    def check(self):
        if time.monotonic() > self.deadline:
            raise ForgeError("History time budget exceeded")

    def read(self, path):
        self.check()
        no_symlinks(path)
        before = path.stat()
        if not stat.S_ISREG(before.st_mode):
            raise ForgeError(f"Expected regular history file: {path}")
        if before.st_size > MAX_FILE_BYTES or self.bytes + before.st_size > MAX_TOTAL_BYTES:
            raise ForgeError("History byte budget exceeded; request fewer tasks")
        self.bytes += before.st_size
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, "rb") as stream:
            if file_identity(os.fstat(stream.fileno())) != file_identity(before):
                raise ForgeError("History file changed before reading")
            data = stream.read(before.st_size + 1)
            if len(data) != before.st_size or file_identity(os.fstat(stream.fileno())) != file_identity(before):
                raise ForgeError("History file changed during reading")
        no_symlinks(path)
        if file_identity(path.stat()) != file_identity(before):
            raise ForgeError("History file changed during reading")
        self.check()
        return data.decode("utf-8")

    def json(self, path):
        text = self.read(path)
        # Check depth/token growth before json.loads allocates the object graph.
        depth = tokens = 0
        quoted = escaped = False
        for index, char in enumerate(text):
            if index % 65536 == 0:
                self.check()
            if quoted:
                if escaped:
                    escaped = False
                elif char == "\\":
                    escaped = True
                elif char == '"':
                    quoted = False
            elif char == '"':
                quoted = True
                tokens += 1
            elif char in "[{":
                depth += 1
                tokens += 1
            elif char in "]}":
                depth -= 1
            elif char in ",:":
                tokens += 1
            if depth > MAX_DEPTH or tokens > MAX_JSON_TOKENS:
                raise ForgeError("History JSON complexity budget exceeded")
        try:
            value = json.loads(text)
        except (ValueError, RecursionError) as error:
            raise ForgeError(f"Invalid saved history JSON: {path}") from error
        if not isinstance(value, dict):
            raise ForgeError(f"Expected a saved history object: {path}")
        self.check()
        return value

    def hash(self, path):
        return hashlib.sha256(self.read(path).encode("utf-8")).hexdigest()


def _record(directory, budget):
    state = budget.json(directory / "task.json")
    if state.get("schema") != "demiforge-task-v1" or state.get("id") != directory.name:
        raise ForgeError(f"Invalid saved task identity: {directory.name}")
    return state


def _current_binding(root, budget):
    try:
        config = budget.json(root / ".demiforge/project.json")
        configured_tool_root(root, config.get("tool_root"))
        value = binding(root / ".demiforge/project.json", config, hash_file=budget.hash)
        budget.check()
        return value, None
    except (ForgeError, OSError, ValueError, KeyError, TypeError, AttributeError) as error:
        return None, str(error)


def _summary(directory, state, current, unavailable, budget):
    handoff_path = no_symlinks(directory / "handoff.json")
    handoff = budget.json(handoff_path) if handoff_path.exists() else None
    record = {key: state.get(key) for key in ("id", "pipeline", "goal", "created")}
    record.update(
        saved_binding=state.get("binding"),
        binding_status=("unavailable" if current is None else
                        "matches_tool_config_only" if current == state.get("binding") else "stale"),
        binding_error=unavailable,
        as_of_result=handoff.get("status", "unknown") if handoff else "no_saved_handoff",
        current_evidence="unverified",
    )
    return record


def view(root, task_id=None, *, artifact=None, page=1, offset=0, limit=20):
    if not 1 <= limit <= 50 or not 0 <= offset <= MAX_ENTRIES or page < 1:
        raise ForgeError("History requires limit 1..50, bounded offset and positive page")
    if artifact not in {None, *ARTIFACTS} or artifact and not task_id:
        raise ForgeError("History artifact requires --id and task|handoff|handoff-md")
    if page != 1 and artifact is None:
        raise ForgeError("History --page requires --artifact")
    budget = Budget()
    root = no_symlinks(Path(root).absolute())
    tasks = no_symlinks(root / ".demiforge/tasks")
    current, unavailable = _current_binding(root, budget)
    result = {"schema": "demiflow-development-history-v1", "read_only": True,
              "notice": "Saved results describe their original snapshot; current sources, logs and evidence are unverified. No continuation or acceptance is performed."}
    if task_id is not None:
        safe_id(task_id)
        directory = no_symlinks(tasks / task_id)
        state = _record(directory, budget)
        result["task"] = _summary(directory, state, current, unavailable, budget)
        if artifact:
            text_path = no_symlinks(directory / ARTIFACTS[artifact])
            if not text_path.exists():
                result.update(artifact=artifact, missing=True)
            else:
                text = budget.read(text_path)
                pages = max(1, (len(text) + PAGE_SIZE - 1) // PAGE_SIZE)
                if page > pages:
                    raise ForgeError(f"History page must be 1..{pages}")
                result.update(artifact=artifact, page=page, pages=pages,
                              sha256=hashlib.sha256(text.encode()).hexdigest(),
                              text=text[(page - 1) * PAGE_SIZE:page * PAGE_SIZE])
        else:
            result["saved_scope"] = {key: state.get(key) for key in ("writes", "non_goals", "notes")}
            result["baseline"] = {key: {"present": key in state,
                                           "sha256": digest(state[key]) if key in state else None}
                                  for key in ("baseline", "readonly_baseline")}
            result["context"] = {"status": "original_text_not_saved; missing_from_record",
                                 "saved_sources": state.get("sources", {}),
                                 "notice": "Original paths and hashes are saved facts; current files are not substituted and no read receipts are written."}
    else:
        names = []
        if tasks.exists():
            with os.scandir(tasks) as entries:
                for index, entry in enumerate(entries, 1):
                    budget.check()
                    if index > MAX_ENTRIES:
                        raise ForgeError("History directory entry budget exceeded")
                    if entry.is_symlink():
                        raise ForgeError(f"Symlink in saved tasks: {entry.name}")
                    if entry.is_dir(follow_symlinks=False):
                        safe_id(entry.name)
                        names.append(entry.name)
        names.sort()
        result.update(offset=offset, limit=limit, total=len(names),
                      next_offset=offset + limit if offset + limit < len(names) else None,
                      tasks=[])
        for name in names[offset:offset + limit]:
            directory = no_symlinks(tasks / name)
            path = no_symlinks(directory / "task.json")
            if not path.exists():
                result["tasks"].append({"id": name, "missing": "task.json", "current_evidence": "unverified"})
            else:
                state = _record(directory, budget)
                result["tasks"].append(_summary(directory, state, current, unavailable, budget))
    budget.check()
    encoded = json.dumps(result, ensure_ascii=False, indent=2)
    if len(encoded.encode()) > MAX_OUTPUT_BYTES:
        raise ForgeError("History output budget exceeded; select an artifact page or fewer tasks")
    return result
