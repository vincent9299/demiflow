"""Counterexamples for the remote measurement guard, not synthetic engine passes."""
from copy import deepcopy
import os
from pathlib import Path
import subprocess
import sys

import pytest

from runtime_v3_performance_ray import _birth, _metrics, _process_tree, ray_receipt_errors
from runtime_v3_performance_scenarios import _receipt_errors, snapshot, OWN_TREE, expected_baseline, evaluate_report


def _receipt():
    nodes = [{'hostname': host, 'errors': [], 'samples': 4,
        'allocations': [{'allocation_id': host, 'sample_count': 4, 'rss_peak_bytes': 100,
            'fd_peak': 10, 'remaining': [], 'birth': '123', 'process_births': [[1234, '123']]}]}
        for host in ('a', 'b')]
    return {'errors': [], 'nodes': nodes,
        'original_cleanup': [{'allocation_id': host, 'cleanup': 'CONFIRMED', 'actor_state': 'DEAD'}
            for host in ('a', 'b')],
        'monitor_exit': [{'same_process_alive': False}, {'same_process_alive': False}]}


def test_complete_two_node_measurement_receipt():
    assert ray_receipt_errors(_receipt(), require_two_hosts=True) == []


@pytest.mark.parametrize('damage', ['missing', 'duplicate', 'unsampled', 'alive', 'no_birth',
    'no_rss', 'no_fd', 'unconfirmed', 'no_cleanup', 'actor_alive', 'one_host', 'monitor_alive', 'monitor_absent'])
def test_incomplete_remote_evidence_cannot_be_green(damage):
    receipt = deepcopy(_receipt())
    entry = receipt['nodes'][0]['allocations'][0]
    if damage == 'missing': receipt = None
    elif damage == 'duplicate': receipt['nodes'][1]['allocations'][0]['allocation_id'] = 'a'
    elif damage == 'unsampled': entry['sample_count'] = 0
    elif damage == 'alive': entry['remaining'] = [1234]
    elif damage == 'no_birth': entry['birth'] = None
    elif damage == 'no_rss': entry['rss_peak_bytes'] = 0
    elif damage == 'no_fd': entry['fd_peak'] = 0
    elif damage == 'unconfirmed': receipt['original_cleanup'][0]['cleanup'] = 'UNKNOWN'
    elif damage == 'no_cleanup': receipt['original_cleanup'].pop()
    elif damage == 'actor_alive': receipt['original_cleanup'][0]['actor_state'] = 'ALIVE'
    elif damage == 'one_host': receipt['nodes'][1]['hostname'] = 'a'
    elif damage == 'monitor_alive': receipt['monitor_exit'][0]['same_process_alive'] = True
    elif damage == 'monitor_absent': receipt['monitor_exit'].pop()
    assert ray_receipt_errors(receipt, require_two_hosts=True)


def test_driver_only_receipt_cannot_prove_ray_workers():
    from test_runtime_v3_performance_harness import receipt_report
    row = receipt_report()['trials'][0]
    row['provider'] = 'ray'
    assert 'Ray allocation samples absent' in _receipt_errors(row)


def test_born_process_metrics_and_physical_exit():
    child = subprocess.Popen([sys.executable, '-c', 'import sys; sys.stdin.read()'], stdin=subprocess.PIPE)
    try:
        birth = _birth(child.pid)
        assert birth
        assert _process_tree(child.pid, 'wrong-birth') == {}
        assert _metrics(child.pid, 'wrong-birth') == (0, 0)
        assert _process_tree(child.pid, birth) == {child.pid: birth}
        rss, fds = _metrics(child.pid, birth)
        assert rss > 0 and fds > 0
    finally:
        child.communicate(timeout=5)
    assert _birth(child.pid) is None
    assert _metrics(child.pid, birth) == (0, 0)


def test_remote_sampler_bytes_are_bound_into_harness_evidence():
    evidence = snapshot(OWN_TREE)
    assert set(evidence['harness_inputs']) == {
        'runtime_v3_performance_ray.py', 'runtime_v3_performance_scenarios.py'}


def test_reference_roles_keep_historical_and_capability_claims_separate():
    assert expected_baseline('fast_cpu:local')['commit'].startswith('267b914')
    assert expected_baseline('slow_api:local')['commit'].startswith('5ff5da9')
    for cell in ('fast_cpu:ray', 'slow_api:two_hosts', 'finite_sealed:ray:p2'):
        role = expected_baseline(cell)
        assert role['commit'].startswith('2fbe839') and role['kind'] == 'reference'


@pytest.mark.parametrize('damage', ['reference_as_historical', 'wrong_commit', 'dirty_production', 'missing_policy'])
def test_swapped_or_modified_reference_cannot_pass(damage):
    from test_runtime_v3_performance_harness import receipt_report
    report = receipt_report()
    row = report['trials'][0]
    if damage == 'reference_as_historical': row['baseline_role']['label'] = 'upgraded before'
    elif damage == 'wrong_commit':
        row['source_before']['head'] = row['source_after']['head'] = 'a' * 40
    elif damage == 'dirty_production':
        row['source_before']['production_clean'] = row['source_after']['production_clean'] = False
    elif damage == 'missing_policy': report.pop('baseline_policy')
    assert evaluate_report(report)['status'] == 'failed'
