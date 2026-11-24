"""The cleanup coordinates a resource scope records name a local pool only when there is one.

A sealed local plan can be continued under another provider; that continuation is refused a few
lines later with INCOMPATIBLE_HISTORY. Building the scope binding must not fail first.
"""
from types import SimpleNamespace

from demiflow.execution.hosts.configuration import resource_scope_binding


def resources(**provider):
    return SimpleNamespace(provider=SimpleNamespace(**provider))


def test_a_local_run_records_its_worker_mode_and_capacity_root():
    pool = SimpleNamespace(root="/tmp/demiflow-capacity-x/capacity")
    for mode, recorded in (("thread", "thread"), ("process", "process"), ("subprocess", "process")):
        policy = SimpleNamespace(provider="local", worker_mode=mode)
        assert resource_scope_binding(resources(pool=pool), policy) == {
            "provider": "local", "worker_mode": recorded, "capacity_root": "/tmp/demiflow-capacity-x/capacity"}


def test_a_local_plan_under_a_provider_without_a_pool_records_the_provider_alone():
    policy = SimpleNamespace(provider="local", worker_mode="process")
    assert resource_scope_binding(resources(), policy) == {"provider": "local"}


def test_another_provider_records_the_provider_alone():
    policy = SimpleNamespace(provider="ray_core", worker_mode="process")
    pool = SimpleNamespace(root="/tmp/demiflow-capacity-x/capacity")
    assert resource_scope_binding(resources(pool=pool), policy) == {"provider": "ray_core"}
