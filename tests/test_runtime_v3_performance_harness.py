"""Performance receipt checks; synthetic reports test the checker, never the engine."""
from copy import deepcopy
import json
from pathlib import Path

import pytest

from runtime_v3_performance_scenarios import (
    PLAN_PATH, OWN_TREE, _digest, expected_rows, input_digest,
    evaluate_report, measure_trial, required_cells, expected_baseline,
)


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
            if arm == 'baseline':
                row.update(baseline_role=expected_baseline('small_data:local'), variant={'kind': 'historical'})
            report['trials'].append(row)
    row = deepcopy(base); row.update(arm='candidate', phase='cancel', repetition=0,
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
def test_actual_small_dataset_uses_original_route_ack_and_process_cleanup(tmp_path):
    # tmp_path is controlled by --basetemp=/tmp/demiflow-* in the exact command.
    spec = next(item for item in json.loads(PLAN_PATH.read_text())['cases'] if item['id'] == 'small_data')
    trial = measure_trial(spec, {'root': str(OWN_TREE), 'kind': 'candidate'},
                          'warmup', -1, 'local', tmp_path / 'actual')
    assert trial['status'] == 'passed', trial.get('error') or trial.get('failures')
    assert trial['output'] == _digest(expected_rows('small_data'), ordered=True)
    assert trial['child_execute_action_calls'] and trial['cuts'][-1]['final']
    assert trial['cuts'][-1]['status'] == 'acked'
    assert trial['source_unchanged'] and trial['resources']['samples'] > 0
    assert trial['remaining_observed_descendants_after_driver_exit'] == []


def test_ordered_small_pipeline_cannot_pass_after_result_permutation():
    rows = list(expected_rows('small_data'))
    assert _digest(rows) == _digest(reversed(rows))
    assert _digest(rows, ordered=True) != _digest(reversed(rows), ordered=True)
