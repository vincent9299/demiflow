"""Structured lifecycle observation for backend-neutral Datasource reads."""
from __future__ import annotations

import logging
import time
from typing import Any

from demiflow._compat.observability import log_event

from ..observability import ActionObserver
from .constraints import SourceReadConstraints

logger = logging.getLogger("demiurge.demiflow.data.datasource")


class DatasourceReadObserver:
    def __init__(
        self, datasource: Any, constraints: SourceReadConstraints,
        action: ActionObserver | None,
    ) -> None:
        self.datasource = datasource
        self.constraints = constraints
        self.action = action
        self.started = time.monotonic()
        self.task_started_at = self.started
        self.first_block_at: float | None = None
        self.blocks = 0
        self.physical_rows = 0
        self.yielded_rows = 0
        self._task_blocks = 0
        self._task_physical_rows = 0
        self._task_yielded_rows = 0
        self.fields = {
            "datasource_type": type(datasource).__name__,
            "source_target": str(getattr(datasource, "table", "") or ""),
            "source_row_limit": constraints.row_limit,
        }

    def planning_started(self, *, parallelism: int) -> None:
        if self.action is not None:
            self.action.set_phase(
                "planning_source_tasks", source_row_limit=self.constraints.row_limit,
            )
        log_event(
            logger, "demiflow.datasource.tasks_planning_started",
            **self.fields, parallelism=parallelism,
        )

    def planning_completed(self, *, task_count: int, parallelism: int) -> None:
        if self.action is not None:
            self.action.set_phase(
                "waiting_first_source_block", source_row_limit=self.constraints.row_limit,
                source_task_count=task_count,
            )
        log_event(
            logger, "demiflow.datasource.tasks_planning_completed",
            **self.fields, parallelism=parallelism, task_count=task_count,
            duration_ms=_elapsed(self.started),
        )

    def task_started(self, task_index: int) -> None:
        self.task_started_at = time.monotonic()
        self._task_blocks = 0
        self._task_physical_rows = 0
        self._task_yielded_rows = 0
        log_event(
            logger, "demiflow.datasource.task_started",
            **self.fields, task_index=task_index,
        )

    def block_received(self, task_index: int, row_count: int) -> None:
        now = time.monotonic()
        self.blocks += 1
        self.physical_rows += max(0, int(row_count))
        self._task_blocks += 1
        self._task_physical_rows += max(0, int(row_count))
        if self.first_block_at is None:
            self.first_block_at = now
            if self.action is not None:
                self.action.set_phase(
                    "processing", source_row_limit=self.constraints.row_limit,
                    source_task_count=task_index + 1,
                    time_to_first_source_block_ms=round(
                        (now - self.started) * 1000, 2,
                    ),
                )
            log_event(
                logger, "demiflow.datasource.first_block",
                **self.fields, task_index=task_index, rows=row_count,
                time_to_first_block_ms=round((now - self.started) * 1000, 2),
            )
        else:
            log_event(
                logger, "demiflow.datasource.block_received",
                level=logging.DEBUG, **self.fields,
                task_index=task_index, rows=row_count, blocks=self.blocks,
            )

    def row_yielded(self) -> None:
        self.yielded_rows += 1
        self._task_yielded_rows += 1
        if self.action is not None:
            self.action.set_phase(
                "processing", source_yielded_rows=self.yielded_rows,
                source_blocks=self.blocks,
            )

    def task_completed(self, task_index: int, *, early_stopped: bool) -> None:
        log_event(
            logger, "demiflow.datasource.task_completed",
            **self.fields, task_index=task_index, blocks=self._task_blocks,
            physical_rows=self._task_physical_rows,
            yielded_rows=self._task_yielded_rows, early_stopped=early_stopped,
            duration_ms=_elapsed(self.task_started_at),
        )

    def task_failed(self, task_index: int, exc: BaseException) -> None:
        log_event(
            logger, "demiflow.datasource.task_failed", level=logging.ERROR,
            **self.fields, task_index=task_index, blocks=self._task_blocks,
            physical_rows=self._task_physical_rows,
            yielded_rows=self._task_yielded_rows,
            duration_ms=_elapsed(self.task_started_at),
            error_type=type(exc).__name__, error=str(exc),
        )

    def failed(self, exc: BaseException) -> None:
        log_event(
            logger, "demiflow.datasource.read_failed", level=logging.ERROR,
            **self.fields, blocks=self.blocks, physical_rows=self.physical_rows,
            yielded_rows=self.yielded_rows,
            duration_ms=_elapsed(self.started), error_type=type(exc).__name__,
            error=str(exc),
        )


def block_row_count(block: Any) -> int:
    value = getattr(block, "num_rows", None)
    if value is not None:
        return int(value)
    try:
        return len(block)
    except (TypeError, AttributeError):
        return 0


def _elapsed(started: float) -> float:
    return round((time.monotonic() - started) * 1000, 2)


__all__ = ["DatasourceReadObserver", "block_row_count"]
