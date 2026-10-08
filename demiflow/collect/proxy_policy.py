"""Generic proxy policy declarations for search and fetch operators.

The declaration is deliberately transport-only. It contains no source names,
provider rules, or business decisions. WebSession compiles one policy into its
existing search and fetch implementations while preserving their durable
health and retry state.
"""
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
_FETCH_SESSION_KEYS = _COMMON_SESSION_KEYS | {"identity_envs"}
_STATIC_POOL_KEYS = {
    "pool", "routes", "failure_limit", "cooldown_s", "transient_cooldown_s",
    "cooldown_wait_s", "health_scope", "max_host_health_entries",
    "rotate_on_rate_limit", "warm_start",
}


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
        allowed = {"name", "proxy", "interval_s", "concurrency", "reuse_connections"}
        if set(item) - allowed or "proxy" not in item:
            raise ValueError("invalid proxy policy route fields")
        name = item.get("name")
        if not isinstance(name, str) or not name or name in names:
            raise ValueError("proxy policy route names must be unique")
        interval = item.get("interval_s", 1.0)
        _finite_number(interval, "route interval")
        if interval < 0:
            raise ValueError("route interval must be nonnegative")
        concurrency = item.get("concurrency", 1)
        if type(concurrency) is not int or not 1 <= concurrency <= 64:
            raise ValueError("route concurrency must be 1..64")
        reuse = item.get("reuse_connections", True)
        if type(reuse) is not bool:
            raise ValueError("reuse_connections must be boolean")
        names.add(name)
        result.append({"name": name, "proxy": item["proxy"], "interval_s": interval,
                       "concurrency": concurrency, "reuse_connections": reuse})
    return result


def _session_parts(policy: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return independent declarations for the search and fetch pools."""
    common = {k: copy.deepcopy(policy[k]) for k in _COMMON_SESSION_KEYS if k in policy}
    search_override = policy.get("search", {})
    fetch_override = policy.get("fetch", {})
    if not isinstance(search_override, dict) or not isinstance(fetch_override, dict):
        raise ValueError("session policy search/fetch overrides must be mappings")
    search = {**common, **copy.deepcopy(search_override)}
    fetch = {**common, **copy.deepcopy(fetch_override)}
    if set(search) - _SEARCH_SESSION_KEYS:
        raise ValueError("invalid session search policy fields")
    if set(fetch) - _FETCH_SESSION_KEYS:
        raise ValueError("invalid session fetch policy fields")
    if "factory" not in search or "factory" not in fetch:
        raise ValueError("session policy requires a factory for search and fetch")
    return search, fetch


def _static_parts(policy: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if set(policy) - ({"kind", "routes"} | _STATIC_POOL_KEYS):
        raise ValueError("invalid static proxy policy fields")
    routes = _routes(policy.get("routes", policy.get("pool")))
    fetch = {k: copy.deepcopy(policy[k]) for k in _STATIC_POOL_KEYS
             if k in policy and k != "routes"}
    fetch["pool"] = routes
    search = [{k: route[k] for k in ("name", "proxy", "interval_s", "reuse_connections")}
              for route in routes]
    return search, fetch


def compile_proxy_policies(value: Any) -> dict[str, Any] | None:
    """Validate and compile the public ``proxy`` declaration.

    The returned object is an internal adapter result. It is safe for the
    platform to split this into search and fetch declarations; callers must
    not put source-specific routing decisions in this module.
    """
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
            search, fetch = _static_parts(raw)
            compiled[name] = {"kind": kind, "search_routes": search,
                              "fetch_proxy_routes": {"*": fetch}}
        elif kind == "session_pool":
            search, fetch = _session_parts(raw)
            compiled[name] = {"kind": kind, "search_session_pool": search,
                              "fetch_session_pool": fetch}
        else:
            raise ValueError("proxy policy kind must be static_ip_pool or session_pool")
    return {"default_policy": default, "policies": compiled,
            "selected": copy.deepcopy(compiled[default])}
