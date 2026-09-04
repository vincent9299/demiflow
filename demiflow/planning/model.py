"""Backend-neutral immutable physical planning values."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Mapping

@dataclass(frozen=True)
class ResourceBundle:
    cpu: float = 0.0
    gpu: float = 0.0
    memory_bytes: int = 0
    custom_resources: Mapping[str, float] = field(default_factory=dict)

    def scale(self, count: int) -> "ResourceBundle":
        return ResourceBundle(
            self.cpu * count, self.gpu * count, self.memory_bytes * count,
            {key: value * count for key, value in self.custom_resources.items()},
        )

    def plus(self, other: "ResourceBundle") -> "ResourceBundle":
        keys = set(self.custom_resources) | set(other.custom_resources)
        return ResourceBundle(
            self.cpu + other.cpu, self.gpu + other.gpu,
            self.memory_bytes + other.memory_bytes,
            {key: self.custom_resources.get(key, 0.0) + other.custom_resources.get(key, 0.0) for key in sorted(keys)},
        )

    def fits(self, capacity: "ResourceBundle") -> bool:
        return (
            self.cpu <= capacity.cpu and self.gpu <= capacity.gpu
            and self.memory_bytes <= capacity.memory_bytes
            and all(value <= capacity.custom_resources.get(key, 0.0) for key, value in self.custom_resources.items())
        )

    def to_dict(self) -> dict[str, Any]:
        return {"cpu": self.cpu, "gpu": self.gpu, "memory_bytes": self.memory_bytes, "custom_resources": dict(self.custom_resources)}

@dataclass(frozen=True)
class BackendResourceSnapshot:
    backend: str
    nodes: tuple[ResourceBundle, ...]
    snapshot_id: str
    analysis_complete: bool = True

    @property
    def aggregate(self) -> ResourceBundle:
        value = ResourceBundle()
        for node in self.nodes: value = value.plus(node)
        return value

@dataclass(frozen=True)
class OperatorTraits:
    category: str
    worker_model: str
    lifecycle: str
    policy_class: str
    elastic: bool = True

@dataclass(frozen=True)
class PhysicalStage:
    ordinal: int
    kind: str
    logical_node: Any
    traits: OperatorTraits
    min_workers: int
    initial_workers: int
    max_workers: int
    worker_resources: ResourceBundle
    work_units_upper_bound: int | None = None
    native_exact_fields: frozenset[str] = frozenset()

@dataclass(frozen=True)
class PhysicalPlan:
    backend: str
    action_kind: str
    source: PhysicalStage
    transforms: tuple[PhysicalStage, ...]
    terminal: PhysicalStage
    auxiliaries: tuple[PhysicalStage, ...]
    concurrency_groups: tuple[tuple[int, ...], ...]
    resource_snapshot_id: str
    built_in_rule_version: str
    platform_policy_digest: str
    plan_id: str

    @property
    def stages(self) -> tuple[PhysicalStage, ...]:
        return (self.source, *self.transforms, self.terminal, *self.auxiliaries)
