"""Closed platform planning policy and built-in rule identity."""
from __future__ import annotations
import hashlib, json, math, re
from dataclasses import dataclass
from typing import Any, Mapping
from .model import ResourceBundle

BUILT_IN_RULE_VERSION = "demiflow-planning-rules-v3"
_SAFE_RESOURCE = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{0,63}$")
_DEFAULTS = {
    "resource_reserve": {"cpu": 0.0, "gpu": 0.0, "memory_bytes": 0, "custom_resources": {}},
    "defaults": {
        "ephemeral_worker": {"cpu": 1.0, "gpu": 0.0, "memory_bytes": 0, "custom_resources": {}},
        "reusable_worker": {"cpu": 1.0, "gpu": 0.0, "memory_bytes": 0, "custom_resources": {}},
        "network_io_worker": {"cpu": 0.1, "gpu": 0.0, "memory_bytes": 0, "custom_resources": {}},
    },
    "limits": {"max_parallelism": 32, "max_source_tasks": 16, "max_sink_writers": 8, "max_network_io_workers": 8},
}

@dataclass(frozen=True)
class PlatformPlanningPolicy:
    resource_reserve: ResourceBundle
    ephemeral_worker: ResourceBundle
    reusable_worker: ResourceBundle
    network_io_worker: ResourceBundle
    max_parallelism: int
    max_source_tasks: int
    max_sink_writers: int
    max_network_io_workers: int

    def worker(self, name: str) -> ResourceBundle:
        return getattr(self, name)

    def limit_for(self, category: str) -> int:
        values = [self.max_parallelism]
        if category == "source": values.append(self.max_source_tasks)
        if category == "sink": values.append(self.max_sink_writers)
        if category == "network_io": values.append(self.max_network_io_workers)
        return min(values)

    def to_dict(self) -> dict[str, Any]:
        return {"resource_reserve": self.resource_reserve.to_dict(), "defaults": {
            "ephemeral_worker": self.ephemeral_worker.to_dict(), "reusable_worker": self.reusable_worker.to_dict(), "network_io_worker": self.network_io_worker.to_dict()},
            "limits": {"max_parallelism": self.max_parallelism, "max_source_tasks": self.max_source_tasks, "max_sink_writers": self.max_sink_writers, "max_network_io_workers": self.max_network_io_workers}}

    @property
    def digest(self) -> str:
        raw=json.dumps(self.to_dict(),sort_keys=True,separators=(",",":"),allow_nan=False).encode()
        return hashlib.sha256(raw).hexdigest()


def parse_platform_planning_policy(value: Mapping[str, Any] | None) -> PlatformPlanningPolicy:
    raw = {} if value is None else value
    if not isinstance(raw, Mapping) or set(raw) - {"resource_reserve","defaults","limits"}: raise ValueError("planning fields are unsupported")
    defaults = raw.get("defaults") or {}
    limits = raw.get("limits") or {}
    if not isinstance(defaults, Mapping) or set(defaults)-set(_DEFAULTS["defaults"]): raise ValueError("planning.defaults fields are unsupported")
    if not isinstance(limits, Mapping) or set(limits)-set(_DEFAULTS["limits"]): raise ValueError("planning.limits fields are unsupported")
    bundles={name:_bundle(defaults.get(name),_DEFAULTS["defaults"][name],f"planning.defaults.{name}") for name in _DEFAULTS["defaults"]}
    reserve=_bundle(raw.get("resource_reserve"),_DEFAULTS["resource_reserve"],"planning.resource_reserve")
    numbers={}
    for name, fallback in _DEFAULTS["limits"].items():
        item=limits.get(name,fallback)
        if isinstance(item,bool) or not isinstance(item,int) or item<=0: raise ValueError(f"planning.limits.{name} must be a positive integer")
        numbers[name]=item
    return PlatformPlanningPolicy(reserve,bundles["ephemeral_worker"],bundles["reusable_worker"],bundles["network_io_worker"],**numbers)


def _bundle(value, fallback, label):
    raw=dict(fallback) if value is None else value
    fields={"cpu","gpu","memory_bytes","custom_resources"}
    if not isinstance(raw,Mapping) or set(raw)-fields: raise ValueError(f"{label} fields are unsupported")
    def number(name):
        item=raw.get(name,fallback[name])
        if isinstance(item,bool) or not isinstance(item,(int,float)) or not math.isfinite(float(item)) or float(item)<0: raise ValueError(f"{label}.{name} must be finite and non-negative")
        return float(item)
    memory=raw.get("memory_bytes",fallback["memory_bytes"])
    if isinstance(memory,bool) or not isinstance(memory,int) or memory<0: raise ValueError(f"{label}.memory_bytes must be a non-negative integer")
    custom=raw.get("custom_resources",fallback["custom_resources"])
    if not isinstance(custom,Mapping): raise ValueError(f"{label}.custom_resources must be a mapping")
    normalized={}
    for key,item in custom.items():
        if not isinstance(key,str) or not _SAFE_RESOURCE.fullmatch(key) or isinstance(item,bool) or not isinstance(item,(int,float)) or not math.isfinite(float(item)) or float(item)<=0: raise ValueError(f"{label}.custom_resources is invalid")
        normalized[key]=float(item)
    return ResourceBundle(number("cpu"),number("gpu"),memory,normalized)
