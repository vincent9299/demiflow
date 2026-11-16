"""V026 reuses the original physical-host business cases on an owned cluster."""
from contextlib import contextmanager
from dataclasses import asdict
from itertools import islice
from pathlib import Path
from types import SimpleNamespace
import json
import os


def _remote_wrong_copy(path, *, expected_bytes=None, remove=False):
    """A bounded wrong-byte fixture in this test's unshared /tmp namespace."""
    import ray
    path = Path(path)
    assert str(path).startswith('/tmp/demiflow-')
    if remove:
        path.unlink()
    else:
        assert type(expected_bytes) is int and 0 < expected_bytes <= 65536
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('xb') as stream:
            # Match the declared length, so the real reader reaches the hash
            # comparison rather than refusing an unrelated truncated object.
            stream.write(b'!' * expected_bytes)
    return {'node_id': ray.get_runtime_context().get_node_id(), 'path': str(path)}


def _different_bytes(fx, nodes):
    import ray
    import pytest
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
    from demiflow import data
    from demiflow.execution import dataflow_services
    from demiflow.execution.run_context import RunContext
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    local = ray.get_runtime_context().get_node_id()
    remote, = set(nodes) - {local}
    original = dataflow_services.verify_host_domain
    calls = []

    def different(host, *, store, challenge, bounds):
        if host.host_id != 'ray-' + remote:
            return original(host, store=store, challenge=challenge, bounds=bounds)
        path = store.root / store.store_id / 'objects' / challenge.key
        assert path.is_file() and str(path).startswith(str(fx.root))
        task = ray.remote(num_cpus=0, max_retries=0)(_remote_wrong_copy).options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(remote, soft=False))
        proof = ray.get(task.remote(str(path), expected_bytes=challenge.size_bytes), timeout=20)
        try:
            assert proof['node_id'] == remote and proof['path'] == str(path)
            original(host, store=store, challenge=challenge, bounds=bounds)
        except DataflowError as error:
            calls.append({'code': error.code, 'message': str(error), **proof})
            raise
        finally:
            ray.get(task.remote(str(path), remove=True), timeout=20)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(dataflow_services, 'verify_host_domain', different)
        with pytest.raises(DataflowError, match='Remote read-domain challenge differs'):
            with RunContext.start(fx.root / 'different-domain-bytes', limits={'timeout_s':30}) as run:
                data.range(1).take_all()
        assert not run.journal.execution_commits('action-1')
    assert len(calls) == 1 and calls[0]['code'] == 'CORRUPT_ARTIFACT'
    return calls[0]


def validate_evidence(value):
    assert len(set(value['hosts'])) == len(set(value['nodes'])) == 2
    cases = value['cases']
    assert set(cases) == {'test_unshared_tmp_domain_is_rejected_before_execution',
        'test_dataset_cross_host_source_transform_sink_ack',
        'test_owned_remote_actor_loss_resumes_original_cut',
        'test_shared_source_union_routes_preserve_multiplicity'}
    negative = cases['test_unshared_tmp_domain_is_rejected_before_execution']
    assert not negative['phases'] and not negative['cuts']
    for name, case in cases.items():
        if name == 'test_unshared_tmp_domain_is_rejected_before_execution':
            continue
        placements = [p for phase in case['phases'] for p in phase['placements']]
        assert {p['node_id'] for p in placements} == set(value['nodes'])
        assert {p['hostname'] for p in placements} == set(value['hosts'])
        assert case['cuts'] and case['cuts'][-1]['final']
        assert all(c['status'] == 'acked' for c in case['cuts'])
    recovery = cases['test_owned_remote_actor_loss_resumes_original_cut']
    assert len(recovery['killed']) == 1
    assert len({p['attempt'] for p in recovery['phases']}) == 2
    assert len({p['plan_id'] for p in recovery['phases']}) == 1
    assert sorted(row['id'] for row in recovery['rows']) == list(range(12))
    proofs = value['domain_proofs']
    assert {p['host_id'] for p in proofs} == {'ray-' + n for n in value['nodes']}
    assert all(p['durable'] and p['challenge_digest'] and p['namespace_digest'] for p in proofs)
    caches = value['worker_code_caches']
    assert {p['node_id'] for p in caches} == set(value['nodes'])
    assert len({p['path'] for p in caches}) >= 2
    assert all(p['exists'] and 0 < p['files'] <= 256 for p in caches)
    assert value['different_bytes']['code'] == 'CORRUPT_ARTIFACT'
    assert value['fault_scope'] == 'owned remote actor loss; no physical host shutdown'


def two_hosts(fx, fault=None):
    import pytest
    from runtime_v3_support import NotRunError
    assert fault is None
    if not os.environ.get('DEMIFLOW_PHYSICAL_HOSTS_ROOT'):
        detail = {'missing_semantic_harness': 'explicit owned two-physical-host Ray cluster and shared run root'}
        fx._record_not_run('two_hosts', detail)
        raise NotRunError('two_hosts', detail)
    import test_runtime_v3_physical_hosts as original_cases
    from demiflow.execution.dataflow import DataflowExecution
    from demiflow.execution import dataflow_services
    original_domain = dataflow_services.verify_host_domain
    domain_proofs, caches, cases = [], [], {}

    def domain(host, **kwargs):
        result = original_domain(host, **kwargs)
        domain_proofs.append(asdict(result))
        return result

    # Use the original fixture and business assertions directly. This shares
    # their real allocator/executor and does not launch a nested pytest runner.
    with contextmanager(original_cases.physical_hosts.__wrapped__)() as physical:
        root, nodes = physical
        with pytest.MonkeyPatch.context() as all_patches:
            all_patches.setattr(dataflow_services, 'verify_host_domain', domain)
            for function in (
                    original_cases.test_unshared_tmp_domain_is_rejected_before_execution,
                    original_cases.test_dataset_cross_host_source_transform_sink_ack,
                    original_cases.test_owned_remote_actor_loss_resumes_original_cut,
                    original_cases.test_shared_source_union_routes_preserve_multiplicity):
                name = function.__name__
                with pytest.MonkeyPatch.context() as patch:
                    request = SimpleNamespace(node=SimpleNamespace(name=name))
                    with contextmanager(original_cases.observed.__wrapped__)(patch, physical, request) as observed:
                        launch = DataflowExecution._launch_phase

                        def launched(execution):
                            result = launch(execution)
                            for allocation, endpoint in execution._endpoints.items():
                                cache = execution._action.context.directory / 'worker-cache' / allocation
                                files = list(islice((p for p in cache.rglob('*') if p.is_file()), 257))
                                assert len(files) <= 256
                                if files:
                                    caches.append({'allocation_id': allocation, 'path': str(cache),
                                        'node_id': endpoint._assignment.lease.host_id.removeprefix('ray-'),
                                        'exists': cache.is_dir(), 'files': len(files)})
                            return result

                        patch.setattr(DataflowExecution, '_launch_phase', launched)
                        if name == 'test_unshared_tmp_domain_is_rejected_before_execution':
                            function(fx.root / 'unshared-domain', physical, observed)
                        elif name == 'test_owned_remote_actor_loss_resumes_original_cut':
                            function(physical, observed, patch)
                        else:
                            function(physical, observed)
                        cases[name] = observed
        fx._save_evidence('two_hosts-components', {'cases': cases,
            'domain_proofs': domain_proofs, 'worker_code_caches': caches})
        different = _different_bytes(fx, nodes)
        value = {'hosts': sorted(n['NodeManagerHostname'] for n in nodes.values()),
            'nodes': sorted(nodes), 'cases': cases, 'domain_proofs': domain_proofs,
            'worker_code_caches': caches, 'different_bytes': different,
            'fault_scope': 'owned remote actor loss; no physical host shutdown'}
        validate_evidence(value)
        return value
