"""Embedding telemetry is real actor work, never reconstructed billing."""
from copy import deepcopy
from types import SimpleNamespace

import pytest
from demiflow import data
from demiflow.embeddings.runtime import EmbeddingMetrics
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.recovery_contract import RecoveryContractError
from demiflow.execution.dataflow_model_observations import validate_summaries, observe_models
from test_embeddings import server


def summary():
    value = EmbeddingMetrics()
    value.observe({'usage': {'prompt_tokens': 2}, 'elapsed_s': .1})
    return {**value.summary(), 'shared_admission': {'requests': 1, 'peak': 1, 'wait_s': 0.}}


@pytest.mark.parametrize('bad', ['bool', 'negative', 'nan', 'unknown', 'histogram', 'phase', 'overflow', 'byte_budget'])
def test_model_observation_closed_shape_and_bounds(bad):
    value, limit = summary(), 16384
    if bad == 'bool': value['call_records'] = True
    if bad == 'negative': value['input_tokens'] = -1
    if bad == 'nan': value['latency']['total_s'] = float('nan')
    if bad == 'unknown': value['invented_bill'] = 1
    if bad == 'histogram': value['latency']['p95_upper_s'] = 'fast'
    if bad == 'phase': value['phases'] = {'decode': {'count': 1, 'total': 0., 'max': 3.}}
    if bad == 'overflow': value['latency']['overflow_count'] = 2
    if bad == 'byte_budget': limit = 16
    with pytest.raises((DataflowError, RecoveryContractError)):
        validate_summaries([value], maximum=1, max_bytes=limit)


def test_worker_boot_and_instance_counters_never_fabricate_a_merged_histogram():
    node = SimpleNamespace(occurrence_id='o', kernel=SimpleNamespace(kind='embedding', configuration={'concurrency': 2}))
    execution = SimpleNamespace(_graph=SimpleNamespace(nodes=[node]), _bounds=SimpleNamespace(max_control_bytes=16384))
    payload = {'occurrence_id': 'o', 'partition_id': 'p', 'model_observations': [summary(), summary()]}
    observe_models(execution, SimpleNamespace(boot_id='old'), payload)
    assert len(execution._model_observations[('o', 'p')][1]) == 2
    smaller = deepcopy(payload); smaller['model_observations'][0]['input_tokens'] = 0
    with pytest.raises(DataflowError, match='backwards'):
        observe_models(execution, SimpleNamespace(boot_id='old'), smaller)
    observe_models(execution, SimpleNamespace(boot_id='new'), smaller)
    assert execution._model_observations[('o', 'p')][1][0]['input_tokens'] == 0
    with pytest.raises(DataflowError, match='count'):
        validate_summaries([summary(), summary()], maximum=1, max_bytes=16384)


def test_real_final_metrics_and_original_history_audit_reach_on_drain(tmp_path, server):
    observed = []
    path = tmp_path/'calls.sqlite'
    for reused in (0, 4):
        with data.local_execution(workers=4):
            stats = data.from_items([{'text': str(i)} for i in range(4)]).map_embeddings(
                model=server['model'], inputs={'text': 'text'}, batch_size=1, concurrency=2,
                options={'sqlite_journal': {'path': str(path)}, 'profile_path': str(tmp_path/'profile.json')}).run_stream(
                    on_drain=lambda value: observed.append(deepcopy(value.metrics)))
        values = list(stats.metrics['models'].values())
        assert sum(v['call_records'] for v in values) == 4
        assert sum(v['reused'] for v in values) == reused
        assert all(v['profiled_batches'] > 0 and v['phases'] for v in values)
        assert stats.metrics['native_journal_totals'][str(path)]['responses'] == 4
        assert observed[-1] == stats.metrics
    assert len(server['bodies']) == 4
