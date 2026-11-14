"""L039: dataset_session intersects native/run/envelope limits with the sealed
ExecutionPlacement profile and stage_limits, tightening only, and the real
DataFusionOptions plus slot admission consume the result (no second allocator).
"""
from types import SimpleNamespace

import pytest

from demiflow.execution import native_resources as resources
from demiflow.execution.local_kernel import LocalKernelOptions
from demiflow.execution.recovery_contract import RecoveryContractError

GiB = 2**30
MiB = 2**20


def placement(*, memory_bytes=2 * GiB, scratch_bytes=None, cpu_millis=None,
              stage_limits=None, stage_ids=(), limits=False):
    budget = {'memory_bytes': memory_bytes}
    if scratch_bytes is not None:
        budget['scratch_bytes'] = scratch_bytes
    if cpu_millis is not None:
        budget['cpu_millis'] = cpu_millis
    profile = {'limits' if limits else 'requests': budget}
    return SimpleNamespace(execution_id='e1', placement='driver', group_id=None,
                           backend='datafusion', stage_ids=tuple(stage_ids), profile=profile,
                           programs=(), stage_limits=stage_limits or {}, recovery='final_commit')


@pytest.fixture
def host(monkeypatch, tmp_path):
    monkeypatch.setattr(resources, '_cpu_capacity', lambda: 16)
    monkeypatch.setattr(resources, '_cgroup_memory', lambda: (0, 256 * GiB))
    return SimpleNamespace(resource_root=tmp_path, _local_kernel=None)


def test_unbound_session_keeps_existing_authority(host, tmp_path):
    with resources.dataset_session(host, tmp_path) as session:
        options = session.options
        assert (options.memory_bytes, options.max_rss_bytes, options.threads,
                options.max_scratch_bytes) == (8 * GiB, 24 * GiB, 8, 128 * GiB)


def test_bound_profile_and_stage_tighten_every_dimension(host, tmp_path):
    execution = placement(memory_bytes=2 * GiB, scratch_bytes=4 * GiB, cpu_millis=2000,
                          stage_limits={'s1': {'inflight_rows': 100, 'inflight_bytes': 8 * MiB,
                                               'output_bytes': 16 * MiB}}, stage_ids=('s1',))
    with resources.dataset_session(host, tmp_path) as unbound:
        base = unbound.options
    with resources.dataset_session(host, tmp_path, execution=execution) as session:
        options = session.options
        assert options.max_rss_bytes <= 2 * GiB
        assert options.memory_bytes <= 2 * GiB
        assert options.memory_bytes < options.max_rss_bytes
        assert options.max_scratch_bytes <= 4 * GiB
        assert options.threads <= 2
        assert options.partitions <= options.threads
        assert options.batch_rows <= 100
        assert options.max_row_bytes <= 8 * MiB
        # Only tighten: every sealed dimension is <= the unbound authority.
        for name in ('memory_bytes', 'max_rss_bytes', 'max_scratch_bytes', 'threads',
                     'partitions', 'batch_rows', 'max_row_bytes'):
            assert getattr(options, name) <= getattr(base, name)
        # The slot ledger still covers the tightened options it admits against.
        assert session._slot_capacity['max_rss_bytes_per_slot'] >= options.max_rss_bytes
        assert session._slot_capacity['threads_per_slot'] >= options.threads


def test_sealed_ceiling_beats_looser_native_configuration(host, tmp_path):
    host._local_kernel = LocalKernelOptions(native_resources={
        'memory_bytes': 8 * GiB, 'max_rss_bytes': 16 * GiB, 'threads': 8})
    execution = placement(memory_bytes=1 * GiB)
    with resources.dataset_session(host, tmp_path, execution=execution) as session:
        assert session.options.max_rss_bytes <= 1 * GiB
        assert session.options.memory_bytes <= 1 * GiB


def test_limits_profile_preferred_over_requests(host, tmp_path):
    execution = placement(memory_bytes=2 * GiB, limits=True)
    with resources.dataset_session(host, tmp_path, execution=execution) as session:
        assert session.options.max_rss_bytes <= 2 * GiB


def test_stage_selection_uses_only_bound_stages_and_takes_min(host, tmp_path):
    limits = {'s1': {'inflight_rows': 5000, 'inflight_bytes': 64 * MiB},
              's2': {'inflight_rows': 50, 'inflight_bytes': 2 * MiB}}
    execution = placement(memory_bytes=8 * GiB, stage_limits=limits, stage_ids=('s2',))
    with resources.dataset_session(host, tmp_path, execution=execution) as session:
        assert session.options.batch_rows <= 50
        assert session.options.max_row_bytes <= 2 * MiB
    merged = placement(memory_bytes=8 * GiB, stage_limits=limits, stage_ids=('s1', 's2'))
    with resources.dataset_session(host, tmp_path, execution=merged) as session:
        # Simultaneously-alive merged stages take the tightest, not the sum.
        assert session.options.batch_rows <= 50
        assert session.options.max_row_bytes <= 2 * MiB


@pytest.mark.parametrize('profile', [
    None, {}, {'requests': {}}, {'requests': {'memory_bytes': 0}},
    {'requests': {'memory_bytes': -1}}, {'requests': {'memory_bytes': 'x'}},
    {'requests': {'memory_bytes': GiB, 'scratch_bytes': 0}},
    {'requests': {'memory_bytes': GiB, 'cpu_millis': True}},
])
def test_invalid_sealed_profile_fails_closed(host, tmp_path, profile):
    execution = SimpleNamespace(profile=profile, stage_limits={}, stage_ids=())
    with pytest.raises(RecoveryContractError):
        resources.dataset_session(host, tmp_path, execution=execution)


def test_invalid_stage_budget_fails_closed(host, tmp_path):
    execution = placement(memory_bytes=GiB, stage_limits={'s1': {'inflight_rows': 0}}, stage_ids=('s1',))
    with pytest.raises(RecoveryContractError):
        resources.dataset_session(host, tmp_path, execution=execution)


def test_admission_ceiling_is_the_tightened_options(host, tmp_path):
    # Admission charges options.max_rss_bytes (proven by test_native_resources.py);
    # L039's job is that the sealed ceiling is what tightens that options value and
    # the slot ledger still covers it, so no second host total is introduced.
    execution = placement(memory_bytes=2 * GiB, cpu_millis=1000)
    with resources.dataset_session(host, tmp_path, execution=execution) as session:
        assert session.options.max_rss_bytes <= 2 * GiB
        assert session.options.threads <= 1
        assert session._slot_capacity['max_rss_bytes_per_slot'] >= session.options.max_rss_bytes
        assert session._slot_capacity['threads_per_slot'] >= session.options.threads