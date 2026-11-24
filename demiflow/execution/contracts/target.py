"""Per-run execution target supplied by Smoke or Production Run callers."""
from __future__ import annotations
import re
from dataclasses import dataclass
from typing import Any, Mapping
from urllib.parse import urlsplit

_NAMESPACE=re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")

@dataclass(frozen=True)
class PipelineExecutionTarget:
    backend: str
    timeout_seconds: int
    job_api_address: str = ""
    namespace: str = ""

    def to_dict(self)->dict[str,Any]:
        value={"backend":self.backend,"timeout_seconds":self.timeout_seconds}
        if self.backend=="ray": value["ray"]={"job_api_address":self.job_api_address,"namespace":self.namespace}
        return value

    @classmethod
    def from_mapping(cls,value:Mapping[str,Any])->"PipelineExecutionTarget":
        if not isinstance(value,Mapping): raise TypeError("PipelineExecutionTarget must be a mapping")
        allowed={"backend","timeout_seconds","ray"}
        if set(value)-allowed: raise ValueError("PipelineExecutionTarget fields are unsupported")
        backend=value.get("backend")
        timeout=value.get("timeout_seconds",7200)
        if backend not in {"local","ray"}: raise ValueError("PipelineExecutionTarget backend must be local or ray")
        if isinstance(timeout,bool) or not isinstance(timeout,int) or timeout<=0: raise ValueError("PipelineExecutionTarget timeout_seconds must be positive")
        ray=value.get("ray")
        if backend=="local":
            if ray is not None: raise ValueError("Local PipelineExecutionTarget cannot contain ray")
            return cls("local",timeout)
        if not isinstance(ray,Mapping) or set(ray)!={"job_api_address","namespace"}: raise ValueError("Ray PipelineExecutionTarget requires job_api_address and namespace")
        address=ray.get("job_api_address"); namespace=ray.get("namespace")
        if not isinstance(address,str) or address!=address.strip(): raise ValueError("Ray job_api_address is invalid")
        parsed=urlsplit(address)
        if parsed.scheme not in {"http","https"} or not parsed.netloc or parsed.username is not None or parsed.password is not None or parsed.fragment: raise ValueError("Ray job_api_address must be a credential-free HTTP(S) URL")
        if not isinstance(namespace,str) or not _NAMESPACE.fullmatch(namespace): raise ValueError("Ray namespace is invalid")
        return cls("ray",timeout,address,namespace)
