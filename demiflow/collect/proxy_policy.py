"""Generic proxy declarations compiled only for retained search resources."""
from __future__ import annotations

import copy
import math
from typing import Any


_COMMON_SESSION_KEYS = {
    "factory", "factory_options", "size", "ttl_s", "interval_s",
    "creation_window_s", "max_creations_per_window", "acquisition_timeout_s",
}
_SEARCH_SESSION_KEYS = _COMMON_SESSION_KEYS | {
    "min_healthy", "shortfall_s", "max_size", "capacity_reserve_ratio",
    "background_maintenance", "worker_reserve", "worker_prepare_concurrency",
    "worker_language", "prefer_unused_fallback",
}
_STATIC_POOL_KEYS = {'pool','routes'}


def _finite_number(value: Any, name: str) -> None:
    if type(value) not in (int, float) or not math.isfinite(value):
        raise ValueError(f"{name} must be a finite number")


def _routes(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, (list, tuple)) or not 1 <= len(value) <= 128:
        raise ValueError("proxy policy routes must contain 1 to 128 entries")
    result = []
    names = set()
    for item in value:
        if not isinstance(item, dict):
            raise ValueError("proxy policy route must be a mapping")
        allowed = {"name", "proxy", "interval_s", "reuse_connections"}
        if set(item) - allowed or "proxy" not in item:
            raise ValueError("invalid proxy policy route fields")
        name = item.get("name")
        if not isinstance(name, str) or not name or name in names:
            raise ValueError("proxy policy route names must be unique")
        interval = item.get("interval_s", 1.0)
        _finite_number(interval, "route interval")
        if interval < 0:
            raise ValueError("route interval must be nonnegative")
        reuse = item.get("reuse_connections", True)
        if type(reuse) is not bool:
            raise ValueError("reuse_connections must be boolean")
        names.add(name)
        result.append({"name": name, "proxy": item["proxy"], "interval_s": interval,
                       "reuse_connections": reuse})
    return result


def _session_parts(policy: dict[str, Any]) -> dict[str, Any]:
    if set(policy)-({'kind','search'}|_COMMON_SESSION_KEYS):
        raise ValueError('Only search session proxy fields are supported')
    common={k:copy.deepcopy(policy[k]) for k in _COMMON_SESSION_KEYS if k in policy}
    override=policy.get('search',{})
    if not isinstance(override,dict):raise ValueError('session search override must be a mapping')
    search={**common,**copy.deepcopy(override)}
    if set(search)-_SEARCH_SESSION_KEYS or 'factory' not in search:
        raise ValueError('Invalid search session policy')
    return search


def _static_parts(policy: dict[str, Any]) -> list[dict[str, Any]]:
    if set(policy) - ({"kind", "routes"} | _STATIC_POOL_KEYS):
        raise ValueError("invalid static proxy policy fields")
    routes = _routes(policy.get("routes", policy.get("pool")))
    search = [{k: route[k] for k in ("name", "proxy", "interval_s", "reuse_connections")}
              for route in routes]
    return search


def compile_proxy_policies(value: Any) -> dict[str, Any] | None:
    """Compile only search declarations; raw fetch owns its explicit policy."""
    if value is None:
        return None
    if not isinstance(value, dict) or set(value) != {"default_policy", "policies"}:
        raise ValueError("proxy requires default_policy and policies")
    default = value["default_policy"]
    policies = value["policies"]
    if not isinstance(default, str) or not default:
        raise ValueError("proxy default_policy must be a nonempty name")
    if not isinstance(policies, dict) or not 1 <= len(policies) <= 32 or default not in policies:
        raise ValueError("proxy policies must contain the default policy")
    compiled = {}
    for name, raw in policies.items():
        if not isinstance(name, str) or not name or not isinstance(raw, dict):
            raise ValueError("proxy policy names and values must be mappings")
        kind = raw.get("kind")
        if kind == "static_ip_pool":
            compiled[name] = {"kind": kind, "search_routes": _static_parts(raw)}
        elif kind == "session_pool":
            compiled[name] = {"kind": kind, "search_session_pool": _session_parts(raw)}
        else:
            raise ValueError("proxy policy kind must be static_ip_pool or session_pool")
    return {"default_policy": default, "policies": compiled,
            "selected": copy.deepcopy(compiled[default])}
