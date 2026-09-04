"""Closed native execution options owned exclusively by Demiflow."""
from __future__ import annotations
import json,math,re
from dataclasses import dataclass
from typing import Any,Mapping
_SAFE=re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{0,63}$")
@dataclass(frozen=True)
class NativeOptions:
    backend: str
    compute_kind: str = ""
    min_workers: int|None = None
    initial_workers: int|None = None
    max_workers: int|None = None
    worker_resources: Mapping[str,Any] = None

def parse_native_options(value:Mapping[str,Any]|None,*,family:str)->NativeOptions|None:
    if value is None:return None
    if not isinstance(value,Mapping) or set(value)!={"ray"} or not isinstance(value["ray"],Mapping): raise ValueError("backend_options requires only a ray mapping")
    raw=dict(value["ray"]); allowed={"compute","worker_resources","tasks","writers"}
    if set(raw)-allowed: raise ValueError("ray backend_options fields are unsupported")
    if family not in {"source","row_transform","batch_transform","global_transform","sink"}: raise ValueError("native option family is invalid")
    range_value=raw.get("tasks") if family=="source" else raw.get("writers") if family=="sink" else None
    allowed_range={"tasks"} if family=="source" else {"writers"} if family=="sink" else set()
    forbidden={"tasks","writers"}-allowed_range
    if set(raw)&forbidden: raise ValueError(f"{family} backend_options contains an unsupported worker range")
    kind=""; crange=None
    if "compute" in raw:
        if family not in {"row_transform","batch_transform"}: raise ValueError("compute is unsupported for this API family")
        compute=raw["compute"]
        if not isinstance(compute,Mapping): raise ValueError("compute must be a mapping")
        kind=compute.get("kind")
        if kind not in {"task_pool","actor_pool"}: raise ValueError("compute.kind is invalid")
        fields={"kind","size"} if kind=="task_pool" else {"kind","min_size","initial_size","max_size","max_tasks_in_flight_per_actor"}
        if set(compute)-fields: raise ValueError("compute fields are unsupported")
        if kind=="task_pool":
            size=_positive(compute.get("size",1),"compute.size");crange=(size,size,size)
        else: crange=_range(compute,"size")
    if range_value is not None:
        if not isinstance(range_value,Mapping): raise ValueError("worker range must be a mapping")
        r=_range(range_value,"workers")
        if crange is not None and r!=crange: raise ValueError("compute and worker ranges conflict")
        crange=r
    resources=_resources(raw.get("worker_resources"))
    # Force strict JSON and detach caller-owned mappings.
    json.loads(json.dumps(value,allow_nan=False))
    return NativeOptions("ray",kind,*(crange or (None,None,None)),resources)

def _positive(value,label):
    if isinstance(value,bool) or not isinstance(value,int) or value<=0: raise ValueError(f"{label} must be positive")
    return value

def _range(value,label):
    minimum=_positive(value.get("min",value.get("min_size",1)),label+".min")
    maximum=_positive(value.get("max",value.get("max_size",minimum)),label+".max")
    initial=_positive(value.get("initial",value.get("initial_size",minimum)),label+".initial")
    if not minimum<=initial<=maximum: raise ValueError(f"{label} range is invalid")
    return minimum,initial,maximum

def _resources(value):
    if value is None:return {}
    if not isinstance(value,Mapping) or set(value)-{"cpu","gpu","memory_bytes","custom_resources"}: raise ValueError("worker_resources fields are unsupported")
    out={}
    for key in ("cpu","gpu"):
        if key in value:
            item=value[key]
            if isinstance(item,bool) or not isinstance(item,(int,float)) or not math.isfinite(float(item)) or item<0: raise ValueError(f"worker_resources.{key} is invalid")
            out[key]=float(item)
    if "memory_bytes" in value:
        item=value["memory_bytes"]
        if isinstance(item,bool) or not isinstance(item,int) or item<=0: raise ValueError("worker_resources.memory_bytes is invalid")
        out["memory_bytes"]=item
    custom=value.get("custom_resources",{})
    if not isinstance(custom,Mapping): raise ValueError("custom_resources must be a mapping")
    normalized={}
    for key,item in custom.items():
        if not isinstance(key,str) or not _SAFE.fullmatch(key) or isinstance(item,bool) or not isinstance(item,(int,float)) or not math.isfinite(float(item)) or item<=0: raise ValueError("custom_resources is invalid")
        normalized[key]=float(item)
    if normalized:out["custom_resources"]=normalized
    return out
