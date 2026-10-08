"""Durable, business-neutral work checkpoint for streaming operators.

This module stores execution state only. It does not know row schemas, storage
formats, or acceptance rules. Operators provide an opaque task key/payload and
an opaque result; the runtime owns claiming, leasing, completion and retry.
"""
from __future__ import annotations

import json
import asyncio
import inspect
import hashlib
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any


class OperatorCheckpoint:
    """SQLite-backed task ledger shared by operator workers and restarts."""

    def __init__(self, path: str | Path, *, operator: str, lease_s: float = 300.0,
                 payload_policy: str = 'immutable'):
        if not operator or not isinstance(operator, str):
            raise ValueError("operator must be a non-empty string")
        if lease_s <= 0:
            raise ValueError("lease_s must be positive")
        if payload_policy not in {'immutable', 'key_authoritative'}:
            raise ValueError("payload_policy must be immutable or key_authoritative")
        self.path = Path(path)
        self.operator = operator
        self.lease_s = float(lease_s)
        self.payload_policy = payload_policy
        self._lock = threading.RLock()
        self._init()

    def _connect(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        db = sqlite3.connect(self.path, timeout=30, check_same_thread=False)
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=FULL")
        db.execute("PRAGMA busy_timeout=30000")
        return db

    def _init(self):
        with self._lock, self._connect() as db:
            db.executescript("""
              CREATE TABLE IF NOT EXISTS operator_tasks (
                operator TEXT NOT NULL,
                task_key TEXT NOT NULL,
                payload TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('pending','running','completed','retryable')),
                result TEXT,
                error TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                lease_until REAL,
                updated_at REAL NOT NULL,
                PRIMARY KEY(operator, task_key)
              );
              CREATE INDEX IF NOT EXISTS operator_tasks_recovery
                ON operator_tasks(operator, state, lease_until);
            """)
            # Opening a checkpoint creates a new executor owner.  Tasks left
            # running by a previous process must be immediately replayable;
            # waiting for the old lease would turn a restart into a silent
            # multi-minute hole.  A pipeline run owns its checkpoint lock, so
            # this does not race a live sibling executor.
            db.execute(
                "UPDATE operator_tasks SET state='pending', lease_until=NULL "
                "WHERE operator=? AND state='running'", (self.operator,)
            )

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    def register(self, task_key: str, payload: Any) -> dict[str, Any]:
        """Insert an unseen task; return current state and stored result."""
        if not task_key:
            raise ValueError("task_key must be non-empty")
        now = time.time()
        encoded = self._json(payload)
        with self._lock, self._connect() as db:
            db.execute(
                "INSERT OR IGNORE INTO operator_tasks "
                "(operator,task_key,payload,state,updated_at) VALUES (?,?,?,?,?)",
                (self.operator, task_key, encoded, "pending", now),
            )
            row = db.execute(
                "SELECT state,result,error,attempts,lease_until,payload FROM operator_tasks "
                "WHERE operator=? AND task_key=?", (self.operator, task_key)
            ).fetchone()
            if row is not None and row[5] != encoded:
                if self.payload_policy != 'key_authoritative':
                    raise ValueError(f"checkpoint payload changed for task {task_key}")
                # The caller has declared task_key to be the stable identity.
                # A resumed row may carry new transient fields; keep the
                # completed result and bind future recovery to the newest
                # opaque payload.
                db.execute("UPDATE operator_tasks SET payload=?,updated_at=? "
                           "WHERE operator=? AND task_key=?",
                           (encoded, now, self.operator, task_key))
                row = (row[0], row[1], row[2], row[3], row[4], encoded)
        return self._row(row)

    def claim(self, task_key: str) -> dict[str, Any] | None:
        """Atomically claim a task; stale leases become runnable again."""
        now = time.time()
        with self._lock, self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT state,result,error,attempts,lease_until,payload FROM operator_tasks "
                "WHERE operator=? AND task_key=?", (self.operator, task_key)
            ).fetchone()
            if row is None:
                db.rollback()
                return None
            state, result, error, attempts, lease_until, payload = row
            if state == "completed":
                db.commit()
                return self._row(row)
            if state == "running" and lease_until is not None and lease_until > now:
                db.commit()
                return None
            db.execute(
                "UPDATE operator_tasks SET state='running', attempts=?, lease_until=?, "
                "updated_at=?, error=NULL WHERE operator=? AND task_key=?",
                (int(attempts) + 1, now + self.lease_s, now, self.operator, task_key),
            )
            db.commit()
            return {"state": "running", "result": json.loads(result) if result else None,
                    "error": error, "attempts": int(attempts) + 1,
                    "lease_until": now + self.lease_s,
                    "payload": json.loads(payload)}

    def complete(self, task_key: str, result: Any):
        now = time.time()
        with self._lock, self._connect() as db:
            db.execute(
                "UPDATE operator_tasks SET state='completed',result=?,error=NULL,lease_until=NULL,updated_at=? "
                "WHERE operator=? AND task_key=?",
                (self._json(result), now, self.operator, task_key),
            )
            if db.total_changes != 1:
                raise KeyError(task_key)

    def fail(self, task_key: str, error: Any, *, retryable: bool = True):
        now = time.time()
        state = "retryable" if retryable else "pending"
        with self._lock, self._connect() as db:
            db.execute(
                "UPDATE operator_tasks SET state=?,error=?,lease_until=NULL,updated_at=? "
                "WHERE operator=? AND task_key=?",
                (state, self._json(error), now, self.operator, task_key),
            )
            if db.total_changes != 1:
                raise KeyError(task_key)

    def pending(self) -> list[dict[str, Any]]:
        """Return tasks that need replay, including expired running leases."""
        now = time.time()
        with self._lock, self._connect() as db:
            rows = db.execute(
                "SELECT state,result,error,attempts,lease_until,payload,task_key FROM operator_tasks "
                "WHERE operator=? AND (state IN ('pending','retryable') OR "
                "(state='running' AND lease_until<=?)) ORDER BY updated_at,task_key",
                (self.operator, now),
            ).fetchall()
        return [dict(self._row(row[:-1]), task_key=row[-1]) for row in rows]

    def stats(self) -> dict[str, int]:
        with self._lock, self._connect() as db:
            rows = db.execute(
                "SELECT state,COUNT(*) FROM operator_tasks WHERE operator=? GROUP BY state",
                (self.operator,),
            ).fetchall()
        return {state: int(count) for state, count in rows}

    def _row(self, row):
        state, result, error, attempts, lease_until, payload = row
        return {"state": state, "result": json.loads(result) if result else None,
                "error": json.loads(error) if error else None, "attempts": int(attempts),
                "lease_until": lease_until, "payload": json.loads(payload)}

class CheckpointedCallable:
    """Generic row operator wrapper; payload/result remain opaque to the platform."""
    def __init__(self, fn, checkpoint, operator):
        self.fn = fn
        self._checkpoint = checkpoint
        self.operator = operator
        for name in ('label', 'concurrency', 'queue_depth', 'catch', 'hard_timeout', 'resources'):
            if hasattr(fn, name):
                setattr(self, name, getattr(fn, name))

    async def __call__(self, row):
        key = row.get('record_id') if isinstance(row, dict) else None
        if not key:
            key = self._checkpoint._json(row)
        key = hashlib.sha256(str(key).encode('utf-8')).hexdigest()
        await asyncio.to_thread(self._checkpoint.register, key, row)
        while True:
            claimed = await asyncio.to_thread(self._checkpoint.claim, key)
            if claimed is None:
                await asyncio.sleep(.05)
                continue
            if claimed['state'] == 'completed':
                return claimed['result']
            try:
                result = self.fn(row)
                if inspect.isawaitable(result):
                    result = await result
            except BaseException as exc:
                await asyncio.to_thread(self._checkpoint.fail, key,
                    {'type': type(exc).__name__, 'message': str(exc)}, retryable=True)
                raise
            await asyncio.to_thread(self._checkpoint.complete, key, result)
            return result

    def recovery_rows(self):
        return [item.get('payload') for item in self._checkpoint.pending()
                if isinstance(item.get('payload'), (dict, list))]

    async def astart(self):
        start = getattr(self.fn, 'astart', None)
        if start is not None:
            result = start()
            if inspect.isawaitable(result):
                await result

    async def astop(self):
        stop = getattr(self.fn, 'astop', None)
        if stop is not None:
            result = stop()
            if inspect.isawaitable(result):
                await result

    async def aclose(self):
        close = getattr(self.fn, 'aclose', None)
        if close is not None:
            result = close()
            if inspect.isawaitable(result):
                await result
