"""Single-program Pipeline loading and execution."""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Mapping
from urllib.parse import urlsplit

from demiflow._compat.observability import log_event

if TYPE_CHECKING:
    from ..execution.executors.base import DatasetExecutor
    from .context import ProgramContext
    from .operator import PipelineProgram

logger = logging.getLogger(__name__)
PIPELINE_PACKAGE_PATH = "pipeline"
PIPELINE_BACKENDS = ("local", "ray")
PIPELINE_DEFAULT_TIMEOUT_SECONDS = 7200
PIPELINE_DEFAULT_DATASET_WORKERS = 4
PIPELINE_EXECUTION_FORMAT = "demiflow_execution_v3"
_PIPELINE_EXECUTION_FIELDS = frozenset({"mode", "backend"})


@dataclass(frozen=True)
class PipelineExecution:
    mode: str
    backend_affinity: str = ""

    def to_dict(self) -> dict[str, Any]:
        value: dict[str, Any] = {"mode": self.mode}
        if self.backend_affinity:
            value["backend"] = self.backend_affinity
        return value


@dataclass(frozen=True)
class PipelineContractDiagnostic:
    code: str
    message: str
    path: str = ""
    line: int = 0
    column: int = 0
    field_path: str = ""


class PipelineContractError(ValueError):
    def __init__(self, diagnostics: tuple[PipelineContractDiagnostic, ...]):
        if not diagnostics:
            raise ValueError("PipelineContractError requires diagnostics")
        self.diagnostics = diagnostics
        super().__init__("; ".join(item.message for item in diagnostics))


class Pipeline:
    def __init__(
        self, name: str, entrypoint: str, program: PipelineProgram,
        execution: PipelineExecution, resource_root: str | Path | None = None,
    ) -> None:
        self.name = name
        self.entrypoint = entrypoint
        self.program = program
        self.execution = execution
        self.resource_root = Path(resource_root or ".").resolve()

    @classmethod
    def load(cls, bundle_root: str | Path, programs_package: str | Any) -> "Pipeline":
        from .discovery import discover_pipeline_definition, load_program
        root = Path(bundle_root).resolve()
        definition = discover_pipeline_definition(root)
        program = load_program(programs_package, definition.entrypoint)
        declared = getattr(program, "execution", None)
        if definition.execution_resource:
            if declared != Path(definition.execution_resource).name:
                raise ValueError("Imported PipelineProgram execution resource differs from static definition")
        else:
            runtime_execution = parse_pipeline_execution(
                declared, label=f"{definition.entrypoint}.execution",
            )
            if runtime_execution != definition.execution:
                raise ValueError("Imported PipelineProgram execution differs from static definition")
        return cls(root.name, definition.entrypoint, program, definition.execution, root / "pipeline")

    def run(
        self, *, dataset_executor: "DatasetExecutor",
    ) -> None:
        log_event(
            logger, "demiflow.pipeline.started", pipeline=self.name,
            executor=getattr(dataset_executor, "NAME", type(dataset_executor).__name__),
            entrypoint=self.entrypoint,
        )
        started = time.monotonic()
        try:
            value = self.program.run(ProgramContext(
                dataset_executor=dataset_executor, resource_root=self.resource_root,
            ))
            if value is not None:
                raise TypeError("PipelineProgram.run must return None")
        except Exception as exc:
            log_event(
                logger, "demiflow.pipeline.failed", level=logging.ERROR,
                pipeline=self.name, entrypoint=self.entrypoint,
                duration_ms=round((time.monotonic() - started) * 1000, 2),
                error_type=type(exc).__name__, error=str(exc),
            )
            raise
        log_event(
            logger, "demiflow.pipeline.completed", pipeline=self.name,
            entrypoint=self.entrypoint,
            duration_ms=round((time.monotonic() - started) * 1000, 2),
        )
        return None


def parse_pipeline_execution(value: object, *, label: str = "execution") -> PipelineExecution:
    diagnostics = validate_pipeline_execution(value, label=label)
    if diagnostics:
        raise PipelineContractError(diagnostics)
    assert isinstance(value, Mapping)
    return PipelineExecution(
        mode=str(value["mode"]),
        backend_affinity=str(value.get("backend") or ""),
    )


def validate_pipeline_execution(
    value: object, *, label: str = "execution",
) -> tuple[PipelineContractDiagnostic, ...]:
    issues: list[PipelineContractDiagnostic] = []
    if not isinstance(value, Mapping):
        return (PipelineContractDiagnostic(
            "execution_not_mapping", f"{label} must be a mapping", field_path="$",
        ),)
    raw = dict(value)
    _unsupported(raw, _PIPELINE_EXECUTION_FIELDS, label, "$", issues)
    mode = raw.get("mode")
    if mode not in {"portable", "native"}:
        issues.append(PipelineContractDiagnostic(
            "execution_mode_invalid",
            f"{label}.mode must be portable or native",
            field_path="$.mode",
        ))
    backend = raw.get("backend")
    if mode == "portable" and backend is not None:
        issues.append(PipelineContractDiagnostic(
            "portable_backend_forbidden", f"{label}.backend is forbidden in portable mode",
            field_path="$.backend",
        ))
    if mode == "native" and backend not in PIPELINE_BACKENDS:
        issues.append(PipelineContractDiagnostic(
            "native_backend_required", f"{label}.backend must be one of {list(PIPELINE_BACKENDS)} in native mode",
            field_path="$.backend",
        ))
    return tuple(issues)


def _unsupported(raw, allowed, label, prefix, issues):
    for name in sorted(set(raw) - set(allowed)):
        issues.append(PipelineContractDiagnostic(
            "execution_field_unsupported", f"{label} contains unsupported field: {name}",
            field_path=f"{prefix}.{name}",
        ))


__all__ = [
    "PIPELINE_BACKENDS", "PIPELINE_DEFAULT_DATASET_WORKERS",
    "PIPELINE_DEFAULT_TIMEOUT_SECONDS",
    "PIPELINE_EXECUTION_FORMAT", "PIPELINE_PACKAGE_PATH", "Pipeline",
    "PipelineContractDiagnostic", "PipelineContractError", "PipelineExecution",
    "parse_pipeline_execution", "validate_pipeline_execution",
]
