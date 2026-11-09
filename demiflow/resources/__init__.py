"""Neutral resource contracts and shared management.

Importing this package loads only standard-library-backed value types; it never
loads Ray, Forge, RunContext or business operators. Providers and the journal
state adapter are imported explicitly by their composition points.
"""
from .contracts import (
    RESOURCE_PROTOCOL_VERSION, ERROR_CODES, HOST_PHASES, TERMINAL_HOST_PHASES,
    FRESHNESS, CLEANUP_STATES, RELEASE_MODES, SCOPE_STATES, ResourceError,
    ResourceProfile, WorkerLaunchSpec, WorkerRequest, ReleaseSpec, MutationContext,
    ResourceScope, ResourcePlan, ResourceProvider, ResourceState,
    identifier, document, spec_hash, negotiate_version, validate_budget,
    parse_plan, validate_observation,
)

__all__ = [
    "RESOURCE_PROTOCOL_VERSION", "ERROR_CODES", "HOST_PHASES", "TERMINAL_HOST_PHASES",
    "FRESHNESS", "CLEANUP_STATES", "RELEASE_MODES", "SCOPE_STATES", "ResourceError",
    "ResourceProfile", "WorkerLaunchSpec", "WorkerRequest", "ReleaseSpec", "MutationContext",
    "ResourceScope", "ResourcePlan", "ResourceProvider", "ResourceState",
    "identifier", "document", "spec_hash", "negotiate_version", "validate_budget",
    "parse_plan", "validate_observation",
]