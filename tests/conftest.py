"""Shared pytest configuration.

Lane V owns only the ``runtime_v3_fixture`` addition (V036). No existing
fixture/hook/collection logic is modified here, so the 577 historical test
files keep collecting unchanged.
"""
from __future__ import annotations

import os
import shutil
import tempfile
from typing import Iterator

import pytest

from runtime_v3_support import (
    DEFAULT_BOUNDS,
    RuntimeV3Fixture,
    assert_tree_binding,
    make_scope,
)


def pytest_configure(config):
    """An ordinary session admits against its own capacity domain, never the host's ledger.

    An explicit DEMIFLOW_RESOURCE_ROOT is respected together with its device inventory.
    Without one the session, its xdist workers and their child processes share one private
    root. A deployment device inventory is dropped with it: a private device ledger must
    not advertise the host's real devices a second time. Device tests bring a test inventory.
    """
    if 'DEMIFLOW_RESOURCE_ROOT' in os.environ:
        return
    config._demiflow_private_domain = tempfile.mkdtemp(prefix='demiflow-test-domain-')
    os.environ['DEMIFLOW_RESOURCE_ROOT'] = config._demiflow_private_domain
    os.environ.pop('DEMIFLOW_DEVICE_INVENTORY', None)


def pytest_unconfigure(config):
    directory = getattr(config, '_demiflow_private_domain', None)
    if directory is not None:
        shutil.rmtree(directory, ignore_errors=True)


@pytest.fixture
def runtime_v3_fixture(tmp_path, request) -> Iterator[RuntimeV3Fixture]:
    """Tree-bound acceptance fixture.

    Reads an explicit ``runtime_v3(provider=..., environment=...)`` marker (or
    the DEMIFLOW_V3_PROVIDER / DEMIFLOW_V3_ENV overrides), creates a scoped
    RuntimeV3Fixture under /tmp/demiflow-*, asserts the in-process worktree
    binding, yields, and always closes in ``finally``. A missing environment is
    recorded not_run (never a skip counted as pass); a same-host two_hosts claim
    or a missing worktree binding fails.
    """
    marker = request.node.get_closest_marker("runtime_v3")
    provider = os.environ.get("DEMIFLOW_V3_PROVIDER")
    environment = os.environ.get("DEMIFLOW_V3_ENV")
    if marker is not None:
        provider = marker.kwargs.get("provider", provider)
        environment = marker.kwargs.get("environment", environment)
    provider = provider or "local"
    declared_envs = tuple(e.strip() for e in environment.split(",")) if environment else ("local",)

    # Every fixture invocation re-asserts the worktree binding in-process.
    assert_tree_binding()

    scope = make_scope()
    fixture = RuntimeV3Fixture(scope, provider=provider, bounds=DEFAULT_BOUNDS)
    # A declared environment that this machine cannot satisfy is not_run, and a
    # ray_core provider without ray fails rather than silently falling back.
    if provider == "ray_core":
        fixture.require_environment("ray")
    for env in declared_envs:
        if env != "local":
            fixture.require_environment(env)
    try:
        yield fixture
    except BaseException as exc:
        fixture.record_original_failure(exc)
        raise
    finally:
        assert_tree_binding()
        fixture.close()

@pytest.fixture(scope='session')
def legacy_seal_writer(tmp_path_factory):
    """Last released SDK containing the v1/v2 writers, in its own import root."""
    from runtime_v3_history_scenarios import _extract_original
    checkout, _ = _extract_original(tmp_path_factory.mktemp('legacy-seal-writer'),
                                    '3d7b4e8c6a844cfbeeb072958bf5b013d648fca0')
    return checkout
