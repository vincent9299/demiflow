"""Performance receipt checks; synthetic reports test the checker, never the engine."""
from copy import deepcopy
import json
from pathlib import Path

import pytest

from runtime_v3_performance_scenarios import (
    PLAN_PATH, OWN_TREE, _digest, expected_rows, input_digest,
    evaluate_report, measure_trial, required_cells, expected_baseline, TIMING_BOUNDARY,
    filesystem_identity, _storage_locations, _comparison_environment_errors, run_comparison,
)


def storage_fixture(row):
    """Synthetic checker input only, never evidence of an engine measurement."""
    locations = {}
    for role, path in _storage_locations(row['variant']['root'], row['output_root']).items():
        locations[role] = dict(status='observed', resolved_path=path, path=path,
            observed_existing_path=path, path_exists=True, device='8:1', fsid=123,
            fs_type='xfs', mount_source='/dev/checker-fixture', mount_root='/', mount_point='/',
            mount_id='1', mount_record_sha256='a'*64, mount_options=['rw'], super_options=['rw'])
    node = dict(status='observed', hostname='checker-fixture-host',
                boot_id='aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', mount_namespace='mnt:[123]', locations=locations)
    captured = dict(driver=node, nodes=[], errors=[])
    return dict(schema='demiflow.performance.storage.v1', timing=deepcopy(TIMING_BOUNDARY),
                before=deepcopy(captured), after=deepcopy(captured))


def timed_fixture(row, seconds):
    row['elapsed_s'] = seconds
    row['measurement_started_ns'] = 1000000000
    row['measurement_closed_ns'] = 1000000000 + round(seconds * 1e9)


def receipt_report():
    spec = next(item for item in json.loads(PLAN_PATH.read_text())['cases'] if item['id'] == 'small_data')
    evidence = {'head': expected_baseline('small_data:local')['commit'], 'production_clean': True,
                'production_sha256': {'demiflow/x.py': 'b' * 64},
                'harness_sha256': 'c' * 64, 'plan_sha256': 'd' * 64}
    base = {'spec': spec, 'input': input_digest('small_data'),
            'output': _digest(expected_rows('small_data'), ordered=True), 'status': 'passed',
            'elapsed_s': 2., 'action_elapsed_s': 1.9, 'first_result_s': 1.8,
            'throughput_rows_s': 1.5, 'public_rows_observed': 3,
            'source_before': evidence, 'source_after': evidence,
            'source_unchanged': True, 'returncode': 0, 'failures': [],
            'resources': {'samples': 20, 'errors': [],
                'sampled_peaks': {'rss_bytes': 1, 'fd_count': 1, 'disk_bytes': 1},
                'last_original_pool': {'active': [], 'charged': {}}},
            'remaining_observed_descendants_after_driver_exit': [],
            'carrier': 'unified_dataflow_v3', 'ray_data_import_attempts': [],
            'child_execute_action_calls': [{'kind': 'take_all', 'provider': 'local'}],
            'cuts': [{'commit_id': 'e' * 64, 'final': True, 'status': 'acked'}],
            'provider': 'local', 'cell': 'small_data:local'}
    report = {'required_cells': ['small_data:local'], 'trials': [],
              'baseline_policy': json.loads(PLAN_PATH.read_text())['baseline_policy']}
    for arm in ('baseline', 'candidate'):
        for phase, repetition in [('warmup', -1)] + [('measure', n) for n in range(5)]:
            row = deepcopy(base); row.update(arm=arm, phase=phase, repetition=repetition)
            row.update(variant={'root': '/checker-fixture/' + arm, 'kind': 'candidate'},
                       output_root='/tmp/demiflow-checker/' + arm + str(repetition))
            if arm == 'baseline':
                row.update(baseline_role=expected_baseline('small_data:local'))
                row['variant']['kind'] = 'historical'
            timed_fixture(row, row['elapsed_s'])
            row['comparison_environment'] = storage_fixture(row)
            report['trials'].append(row)
    row = deepcopy(report['trials'][-1]); row.update(arm='candidate', phase='cancel', repetition=0,
        output=None, first_result_s=None, throughput_rows_s=None, cancel_latency_s=.2,
        error={'type': 'DataflowError', 'message': 'CANCELLED'}, cuts=[])
    report['trials'].append(row)
    return report


def test_valid_same_budget_comparison_and_nearest_rank_percentiles():
    report = receipt_report()
    rows = [r for r in report['trials'] if r['arm'] == 'candidate' and r['phase'] == 'measure']
    for row, seconds in zip(rows, [1.5, 2., 1.8, 2.1, 2.3]):
        row.update(elapsed_s=seconds, first_result_s=1., action_elapsed_s=seconds-.1,
                   throughput_rows_s=3/seconds)
        timed_fixture(row, seconds)
    result = evaluate_report(report)
    assert result['status'] == 'passed'
    assert result['comparisons'][0]['candidate']['p50_s'] == 2.
    assert result['comparisons'][0]['candidate']['p95_s'] == 2.3


@pytest.mark.parametrize('damage', ['missing', 'duplicate', 'warmup', 'cancel', 'unsupported'])
def test_incomplete_or_duplicated_trials_cannot_be_green(damage):
    report = receipt_report()
    if damage == 'missing': report['trials'].pop(2)
    elif damage == 'duplicate': report['trials'][2]['repetition'] = 0
    elif damage == 'warmup': report['trials'][0]['phase'] = 'measure'
    elif damage == 'cancel': report['trials'].pop()
    else: report['trials'][2]['status'] = 'unsupported'
    assert evaluate_report(report)['status'] != 'passed'


@pytest.mark.parametrize('damage', ['source', 'head', 'budget', 'output', 'harness', 'ack',
    'route', 'ray_data', 'rss', 'nan', 'negative', 'throughput', 'cleanup', 'pool', 'timing'])
def test_invalid_receipt_fails_closed(damage):
    report = receipt_report(); row = report['trials'][8]
    if damage == 'source': row['source_unchanged'] = False
    elif damage == 'head': row['source_before']['head'] = 'f' * 40
    elif damage == 'budget': row['spec']['limits']['memory_bytes'] *= 2
    elif damage == 'output': row['output']['rows'] += 1
    elif damage == 'harness': row['source_before']['harness_sha256'] = 'f' * 64
    elif damage == 'ack': row['cuts'] = []
    elif damage == 'route': row['child_execute_action_calls'] = []
    elif damage == 'ray_data': row['ray_data_import_attempts'] = ['ray.data']
    elif damage == 'rss': row['resources']['errors'] = ['sampling budget exceeded']
    elif damage == 'nan': row['elapsed_s'] = float('nan')
    elif damage == 'negative': row['elapsed_s'] = -1.
    elif damage == 'throughput': row['throughput_rows_s'] = 1000.
    elif damage == 'cleanup': row['remaining_observed_descendants_after_driver_exit'] = [123]
    elif damage == 'pool': row['resources']['last_original_pool']['active'] = [{'allocation_id': 'x'}]
    elif damage == 'timing': row['first_result_s'] = row['elapsed_s'] + 1
    assert evaluate_report(report)['status'] == 'failed'


def test_twenty_percent_regression_is_not_waived_by_correct_output():
    report = receipt_report()
    for row in report['trials']:
        if row['arm'] == 'candidate' and row['phase'] == 'measure':
            row.update(elapsed_s=2.5, throughput_rows_s=1.2)
            timed_fixture(row, 2.5)
    verdict = evaluate_report(report)
    assert verdict['status'] == 'failed'
    assert any('regression' in item['reason'] for item in verdict['failures'])


@pytest.mark.parametrize('damage', ['same_smaller_budget', 'same_wrong_output', 'same_wrong_input'])
def test_both_arms_agreeing_on_a_changed_workload_is_not_success(damage):
    report = receipt_report()
    for row in report['trials']:
        if row['phase'] == 'cancel': continue
        if damage == 'same_smaller_budget': row['spec']['limits']['memory_bytes'] //= 2
        elif damage == 'same_wrong_output': row['output']['sum256'] = '0' * 64
        else: row['input']['sha256'] = '0' * 64
    assert evaluate_report(report)['status'] == 'failed'


def test_empty_or_selected_subset_does_not_certify_whole_matrix():
    assert evaluate_report({'required_cells': [], 'trials': []})['status'] != 'passed'
    report = receipt_report()
    assert evaluate_report(report, required=required_cells())['status'] == 'not_run'
    cells = required_cells()
    assert 'finite_sealed:local:p1' in cells and 'finite_sealed:local:p2' in cells
    assert len([cell for cell in cells if ':local' in cell]) == 10


@pytest.mark.slow
def test_actual_small_dataset_uses_original_route_ack_and_process_cleanup(tmp_path, monkeypatch):
    # tmp_path is controlled by --basetemp=/tmp/demiflow-* in the exact command.
    spec = next(item for item in json.loads(PLAN_PATH.read_text())['cases'] if item['id'] == 'small_data')
    inherited = tmp_path / 'must-not-use-parent-domain'
    for name in ('DEMIFLOW_RESOURCE_ROOT', 'DEMIFLOW_RESOURCE_HOME', 'DEMIFLOW_FORGE_DOMAIN', 'DEMIFLOW_LOCATION_CACHE'):
        monkeypatch.setenv(name, str(inherited / name))
    trial = measure_trial(spec, {'root': str(OWN_TREE), 'kind': 'candidate'},
                          'warmup', -1, 'local', tmp_path / 'actual')
    assert trial['status'] == 'passed', trial.get('error') or trial.get('failures')
    assert trial['output'] == _digest(expected_rows('small_data'), ordered=True)
    assert trial['child_execute_action_calls'] and trial['cuts'][-1]['final']
    assert trial['cuts'][-1]['status'] == 'acked'
    assert trial['source_unchanged'] and trial['resources']['samples'] > 0
    assert trial['remaining_observed_descendants_after_driver_exit'] == []
    assert not inherited.exists()
    assert trial['private_environment'] == {
        'DEMIFLOW_RESOURCE_ROOT': str(tmp_path / 'actual' / 'capacity'),
        'DEMIFLOW_RESOURCE_HOME': str(tmp_path / 'actual' / 'resource-home'),
        'DEMIFLOW_FORGE_DOMAIN': str(tmp_path / 'actual' / 'forge-domain'),
        'DEMIFLOW_LOCATION_CACHE': str(tmp_path / 'actual' / 'locations'),
    }
    # This is one real functional trial, not a synthetic five-round result.
    assert _comparison_environment_errors([trial]) == []
    locations = trial['comparison_environment']['before']['driver']['locations']
    assert locations['source_package']['resolved_path'] == str(OWN_TREE / 'demiflow/__init__.py')
    assert locations['trial_root']['resolved_path'] == str(tmp_path / 'actual')


def test_ordered_small_pipeline_cannot_pass_after_result_permutation():
    rows = list(expected_rows('small_data'))
    assert _digest(rows) == _digest(reversed(rows))
    assert _digest(rows, ordered=True) != _digest(reversed(rows), ordered=True)


@pytest.mark.parametrize('damage', ['missing', 'source_fs', 'source_device', 'data_fs',
    'tmp_mount', 'location_cache_mount', 'location_cache_missing', 'unknown', 'after_missing', 'during_change', 'repetition_change',
    'different_host', 'wrong_path', 'timing_boundary', 'timing_interval', 'profile'])
def test_unmatched_or_unobserved_storage_never_produces_net_code_gain(damage):
    report = receipt_report()
    for row in report['trials']:
        if row['arm'] != 'candidate': continue
        if damage == 'repetition_change' and row['repetition'] != 2: continue
        env = row['comparison_environment']
        if damage == 'missing': del row['comparison_environment']; continue
        if damage == 'after_missing': del env['after']; continue
        if damage == 'timing_boundary': env['timing']['start'] = 'after worker startup'; continue
        if damage == 'timing_interval': row['measurement_closed_ns'] += 1000000; continue
        if damage == 'profile': row['profile'] = True; continue
        for phase in ('before', 'after'):
            if damage == 'during_change' and phase != 'after': continue
            host = env[phase]['driver']; locations = host['locations']
            if damage == 'different_host': host['boot_id'] = 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb'
            elif damage == 'source_fs': locations['source_checkout']['fs_type'] = 'dpc'
            elif damage == 'source_device': locations['source_package']['device'] = '8:2'
            elif damage == 'data_fs': locations['run_directory']['fs_type'] = 'dpc'
            elif damage == 'tmp_mount': locations['temporary']['mount_options'] = ['rw', 'sync']
            elif damage == 'location_cache_mount': locations['location_cache']['fs_type'] = 'nfs'
            elif damage == 'location_cache_missing': del locations['location_cache']
            elif damage == 'unknown': locations['harness']['status'] = 'unknown'
            elif damage == 'wrong_path': locations['capacity']['resolved_path'] = '/unobserved-other-root'
            else: locations['source_checkout']['device'] = '8:2'
    verdict = evaluate_report(report)
    assert verdict['status'] == 'not_comparable'
    assert verdict['comparisons'] == []
    assert verdict['failures'] == []  # Missing comparability is not a measured regression.
    assert verdict['not_comparable']


def test_real_filesystem_probe_records_device_and_nearest_existing_parent(tmp_path):
    import os
    from runtime_v3_performance_scenarios import _MOUNT_IDENTITY
    direct = filesystem_identity(tmp_path)
    child = filesystem_identity(tmp_path / 'future-run' / 'scratch')
    assert direct['status'] == child['status'] == 'observed'
    device = tmp_path.stat().st_dev
    assert direct['device'] == f'{os.major(device)}:{os.minor(device)}'
    assert direct['path_exists'] and not child['path_exists']
    assert child['observed_existing_path'] == str(tmp_path)
    assert {k: direct[k] for k in _MOUNT_IDENTITY} == {k: child[k] for k in _MOUNT_IDENTITY}
    alias = tmp_path / 'source-alias'; alias.symlink_to(OWN_TREE, target_is_directory=True)
    assert filesystem_identity(alias)['resolved_path'] == str(OWN_TREE)


def test_unreadable_mount_evidence_is_unknown_not_assumed_local(tmp_path, monkeypatch):
    import builtins
    original = builtins.open
    def read(path, *args, **kwargs):
        if str(path) == '/proc/self/mountinfo': raise PermissionError('unavailable probe')
        return original(path, *args, **kwargs)
    monkeypatch.setattr(builtins, 'open', read)
    assert filesystem_identity(tmp_path)['status'] == 'unknown'


def test_missing_source_deployment_stops_formal_trials_before_launch(tmp_path, monkeypatch):
    import runtime_v3_performance_scenarios as harness
    def forbidden(*args, **kwargs):
        raise AssertionError('incomparable deployment must not start business measurements')
    monkeypatch.setattr(harness, 'measure_trial', forbidden)
    report = run_comparison(json.loads(PLAN_PATH.read_text()),
        {'small_data:local': {'root': str(tmp_path / 'missing-reference'), 'kind': 'historical'}},
        OWN_TREE, tmp_path / 'not-comparable', cells=['small_data:local'], frozen_candidate=True)
    assert report['trials'] == []
    assert report['evaluation']['status'] == 'not_comparable'
    assert not report['evaluation']['comparisons']


def test_driver_storage_never_claims_ray_node_coverage():
    row = receipt_report()['trials'][0]
    row.update(provider='ray', ray_nodes=[{'node_id': 'actual-ray-node'}])
    assert any('actual Ray node' in item for item in _comparison_environment_errors([row]))


def test_remote_storage_changes_are_not_hidden_by_matching_driver_mounts():
    rows = receipt_report()['trials'][:2]
    for row in rows:
        row.update(provider='two_hosts', ray_nodes=[{'node_id': 'node-a'}, {'node_id': 'node-b'}])
        for phase in ('before', 'after'):
            value = row['comparison_environment'][phase]
            for index, node_id in enumerate(('node-a', 'node-b')):
                node = deepcopy(value['driver']); node.update(node_id=node_id, hostname='host-'+node_id,
                    boot_id=('a' if index == 0 else 'b') * 8 + '-aaaa-aaaa-aaaa-aaaaaaaaaaaa')
                value['nodes'].append(node)
    assert _comparison_environment_errors(rows) == []
    rows[-1]['comparison_environment']['after']['nodes'][1]['locations']['temporary']['fs_type'] = 'dpc'
    assert any('changed during' in item for item in _comparison_environment_errors(rows))


@pytest.mark.parametrize('unavailable', [False, True])
def test_ray_probe_is_bounded_node_affine_read_only_and_failure_is_explicit(monkeypatch, tmp_path, unavailable):
    """Adapter unit coverage, not a live remote-filesystem acceptance claim."""
    import sys
    from types import SimpleNamespace
    import runtime_v3_performance_scenarios as harness
    submitted, cancelled, options = [], [], []
    class Probe:
        def options(self, *, scheduling_strategy):
            assert scheduling_strategy.soft is False
            node = scheduling_strategy.node_id
            def remote(source, output):
                assert source == str(OWN_TREE) and output == str(tmp_path)
                submitted.append(node)
                return node
            return SimpleNamespace(remote=remote)
    def remote(**configuration):
        options.append(configuration)
        def wrap(function):
            assert function is harness._ray_storage_probe
            return Probe()
        return wrap
    def get(refs, *, timeout):
        assert timeout == 30 and refs == ['node-a', 'node-b']
        if unavailable: raise TimeoutError('owned node probe unavailable')
        return [dict(node_id=node, status='observed') for node in refs]
    monkeypatch.setitem(sys.modules, 'ray', SimpleNamespace(remote=remote, get=get,
        nodes=lambda: [{'NodeID': node, 'Alive': True} for node in ('node-a', 'node-b')],
        cancel=lambda ref, force: cancelled.append((ref, force))))
    monkeypatch.setitem(sys.modules, 'ray.util.scheduling_strategies', SimpleNamespace(
        NodeAffinitySchedulingStrategy=lambda node, soft: SimpleNamespace(node_id=node, soft=soft)))
    observed = harness._capture_storage_environment(OWN_TREE, tmp_path, 'two_hosts')
    assert options == [dict(num_cpus=0, num_gpus=0, max_retries=0, max_calls=1)]
    assert submitted == ['node-a', 'node-b']
    assert bool(observed['errors']) is unavailable
    assert cancelled == ([('node-a', True), ('node-b', True)] if unavailable else [])
