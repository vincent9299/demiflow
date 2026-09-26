"""Driver context handed to the single PipelineProgram invocation."""
from __future__ import annotations

from typing import TYPE_CHECKING

from ..data.api import DataAPI
from .resources import ResourceAPI

if TYPE_CHECKING:
    from ..execution.executors.base import DatasetExecutor


class ProgramContext:
    """Data capability available to a Pipeline Driver program."""

    def __init__(self, *, dataset_executor: "DatasetExecutor", resource_root) -> None:
        dataset_executor.resource_root = resource_root
        self.data = DataAPI(dataset_executor)
        self.resources = ResourceAPI(resource_root)
