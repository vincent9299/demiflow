"""Structured, low-noise observability for Dataset execution."""
from __future__ import annotations

import logging
import os
import time
import threading
import contextvars
from contextlib import contextmanager
from typing import Any, Iterable, Iterator

from demiflow._compat.observability import log_event

logger = logging.getLogger("demiflow.data.dataset")
_CURRENT_ACTION: contextvars.ContextVar["ActionObserver | None"] = (
    contextvars.ContextVar("demiflow_current_action", default=None)
)


def source_summary(source: Any) -> dict[str, Any]:
    value: dict[str, Any] = {"source_type": type(source).__name__}
    datasource = getattr(source, "datasource", None)
    if datasource is not None:
        value["datasource_type"] = type(datasource).__name__
        table = getattr(datasource, "table", "")
        if table:
            value["source_target"] = str(table)
    paths = getattr(source, "paths", None)
    if paths:
        value["source_files"] = len(paths)
    return value


def plan_summary(plan: Any) -> list[dict[str, Any]]:
    stages = []
    for index, operation in enumerate(getattr(plan, "operations", ())):
        item: dict[str, Any] = {
            "index": index,
            "operation": type(operation).__name__,
        }
        concurrency = getattr(operation, "concurrency", None)
        if concurrency is not None:
            item["concurrency"] = concurrency
        inputs = getattr(operation, "inputs", None)
        if inputs:
            item["inputs"] = dict(inputs)
        output = getattr(operation, "output", None)
        if output:
            item["output"] = output
        outputs = getattr(operation, "outputs", None)
        if outputs:
            item["outputs"] = dict(outputs)
        limit = getattr(operation, "limit", None)
        if limit is not None:
            item["limit"] = limit
        columns = getattr(operation, "columns", None)
        if columns:
            item["columns"] = list(columns)
        names = getattr(operation, "names", None)
        if names:
            item["names"] = dict(names) if isinstance(names, dict) else list(names)
        batch_size = getattr(operation, "batch_size", None)
        if batch_size is not None:
            item["batch_size"] = batch_size
        batch_format = getattr(operation, "batch_format", None)
        if batch_format is not None:
            item["batch_format"] = batch_format
        fraction = getattr(operation, "fraction", None)
        if fraction is not None:
            item["fraction"] = fraction
        keys = getattr(operation, "keys", None)
        if keys:
            item["keys"] = list(keys)
        descending = getattr(operation, "descending", None)
        if descending not in (None, False):
            item["descending"] = (
                list(descending) if isinstance(descending, tuple)
                else descending
            )
        shuffle = getattr(operation, "shuffle", None)
        if shuffle is not None:
            item["shuffle"] = bool(shuffle)
        num_blocks = getattr(operation, "num_blocks", None)
        if num_blocks is not None:
            item["num_blocks"] = num_blocks
        callable_spec = getattr(operation, "callable", None)
        target = getattr(callable_spec, "target", None)
        if target is not None:
            item["callable"] = getattr(target, "__name__", type(target).__name__)
        stages.append(item)
    return stages


def backend_name(executor: Any) -> str:
    return str(getattr(executor, "NAME", type(executor).__name__))


class ActionObserver:
    def __init__(self, action: str, source: Any, plan: Any, executor: Any, **fields: Any) -> None:
        self.action = action
        self.fields = {
            "action": action,
            "backend": backend_name(executor),
            "stage_count": len(getattr(plan, "operations", ())),
            "stages": [
                type(operation).__name__
                for operation in getattr(plan, "operations", ())
            ],
            **source_summary(source),
            **fields,
        }
        self.stages = plan_summary(plan)
        self.started = time.monotonic()
        self.last_progress = self.started
        self.rows = 0
        self.batches = 0
        self.phase = "starting"
        self.phase_started = self.started
        self.progress_fields: dict[str, Any] = {}
        self._stop = threading.Event()
        self._heartbeat: threading.Thread | None = None
        self._finished = False
        self._lock = threading.Lock()

    def start(self) -> None:
        log_event(logger, "demiflow.dataset.action_started", **self.fields)
        log_event(
            logger,
            "demiflow.dataset.plan",
            level=logging.DEBUG,
            **self.fields,
            plan_details=self.stages,
        )
        if logger.isEnabledFor(logging.INFO):
            self._heartbeat = threading.Thread(
                target=self._heartbeat_loop,
                name=f"demiflow-{self.action}-heartbeat",
                daemon=True,
            )
            self._heartbeat.start()

    def progress(self, *, rows: int = 0, batches: int = 0) -> None:
        with self._lock:
            self.rows += max(0, int(rows))
            self.batches += max(0, int(batches))

    def set_phase(self, phase: str, **fields: Any) -> None:
        with self._lock:
            normalized = str(phase or "").strip()
            if normalized and normalized != self.phase:
                self.phase = normalized
                self.phase_started = time.monotonic()
            self.progress_fields.update(fields)

    def complete(self, **fields: Any) -> None:
        if not self._finish_once():
            return
        log_event(
            logger,
            "demiflow.dataset.action_completed",
            **self.fields,
            rows=self.rows,
            batches=self.batches,
            phase=self.phase,
            duration_ms=round((time.monotonic() - self.started) * 1000, 2),
            **fields,
        )

    def fail(self, exc: BaseException) -> None:
        if not self._finish_once():
            return
        log_event(
            logger,
            "demiflow.dataset.action_failed",
            level=logging.ERROR,
            **self.fields,
            rows=self.rows,
            batches=self.batches,
            phase=self.phase,
            duration_ms=round((time.monotonic() - self.started) * 1000, 2),
            error_type=type(exc).__name__,
            error=str(exc),
        )


    def _heartbeat_loop(self) -> None:
        interval = max(
            0.1, float(os.getenv("DEMIFLOW_LOG_HEARTBEAT_SECONDS", "30"))
        )
        while not self._stop.wait(interval):
            with self._lock:
                rows = self.rows
                batches = self.batches
                phase = self.phase
                phase_started = self.phase_started
                progress_fields = dict(self.progress_fields)
            log_event(
                logger,
                "demiflow.dataset.action_progress",
                **self.fields,
                rows=rows,
                batches=batches,
                phase=phase,
                phase_duration_ms=round((time.monotonic() - phase_started) * 1000, 2),
                duration_ms=round((time.monotonic() - self.started) * 1000, 2),
                **progress_fields,
            )

    def _finish_once(self) -> bool:
        with self._lock:
            if self._finished:
                return False
            self._finished = True
            self._stop.set()
            return True


def observe_rows(action: str, rows: Iterable[Any], source: Any, plan: Any, executor: Any) -> Iterator[Any]:
    observer = ActionObserver(action, source, plan, executor)
    observer.start()
    token = _CURRENT_ACTION.set(observer)
    try:
        for row in rows:
            observer.progress(rows=1)
            yield row
    except GeneratorExit:
        observer.complete(early_stopped=True)
        raise
    except Exception as exc:
        observer.fail(exc)
        raise
    else:
        observer.complete()
    finally:
        _CURRENT_ACTION.reset(token)


def observe_batches(action: str, batches: Iterable[Any], source: Any, plan: Any, executor: Any) -> Iterator[Any]:
    observer = ActionObserver(action, source, plan, executor)
    observer.start()
    token = _CURRENT_ACTION.set(observer)
    try:
        for batch in batches:
            size = _batch_size(batch)
            observer.progress(rows=size, batches=1)
            yield batch
    except GeneratorExit:
        observer.complete(early_stopped=True)
        raise
    except Exception as exc:
        observer.fail(exc)
        raise
    else:
        observer.complete()
    finally:
        _CURRENT_ACTION.reset(token)


@contextmanager
def observe_action(action: str, source: Any, plan: Any, executor: Any, **fields: Any):
    observer = ActionObserver(action, source, plan, executor, **fields)
    observer.start()
    token = _CURRENT_ACTION.set(observer)
    try:
        yield observer
    except Exception as exc:
        observer.fail(exc)
        raise
    finally:
        _CURRENT_ACTION.reset(token)


def current_action_observer() -> ActionObserver | None:
    return _CURRENT_ACTION.get()


def _batch_size(batch: Any) -> int:
    value = getattr(batch, "num_rows", None)
    if value is not None:
        return int(value)
    try:
        return len(batch)
    except (TypeError, AttributeError):
        return 0
