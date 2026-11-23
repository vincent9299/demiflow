"""Embedding telemetry is real actor work, never reconstructed billing."""
from copy import deepcopy
from types import SimpleNamespace

import pytest
from demiflow import data
from demiflow.embeddings.runtime import EmbeddingMetrics
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.recovery_contract import RecoveryContractError
from demiflow.execution.journal.dataflow_model_observations import validate_summaries, observe_models
from test_embeddings import server
from test_map_prompt_async import server as prompt_server, prompt


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


def test_prompt_summary_has_no_invented_embedding_fields():
    from demiflow.execution.journal.run_telemetry import CallMetrics
    value = CallMetrics().summary()
    assert validate_summaries([value], maximum=1, max_bytes=16384, kind='prompt') == [value]
    assert 'phases' not in value and 'profiled_batches' not in value and 'shared_admission' not in value
    with pytest.raises(DataflowError, match='invalid'):
        validate_summaries([{**value, 'phases': {}, 'profiled_batches': 0}],
                           maximum=1, max_bytes=16384, kind='prompt')


def test_real_prompt_and_embedding_keep_both_observed_models(server, prompt_server):
    with data.local_execution(workers=4):
        flow = prompt(data.from_items([{'item': i, 'text': str(i)} for i in range(2)]), concurrency=2)
        stats = flow.map_embeddings(model=server['model'], inputs={'text': 'text'},
                                    batch_size=1, concurrency=2).run_stream()
    models = list(stats.metrics['models'].values())
    assert len(models) == 2 and all(value['call_records'] == 2 for value in models)
    assert sum('phases' in value for value in models) == 1
    assert len(server['bodies']) == len(prompt_server['requests']) == 2
    assert stats.emitted == 2


def test_sealed_prompt_route_audit_skip_and_normalized_path_dedup(tmp_path, monkeypatch):
    from demiflow.execution.journal.dataflow_model_observations import finish_journal_audit
    from demiflow.operator_llm import call_ref
    first, second = tmp_path/'calls.sqlite', tmp_path/'skipped.sqlite'
    calls = []
    def audit(path):
        calls.append(path)
        return {'responses': 4}
    monkeypatch.setattr(call_ref, 'journal_totals', audit)
    declarations = {('route-node', 'a'): {'private_journal': {'path': str(first)}},
        ('route-node', 'b'): {'private_journal': {'path': str(tmp_path/'child'/'..'/'calls.sqlite')}},
        ('route-node', 'c'): {'private_journal': {'path': str(second)}, 'collect_journal_totals': False}}
    execution = SimpleNamespace(_services=SimpleNamespace(journal_service=SimpleNamespace(
        _private_prompt_nodes=declarations)), _observation_labels={'route-node': 'route'})
    stats = SimpleNamespace(metrics={'native_journal_totals': {}, 'native_journal_totals_skipped': []})
    finish_journal_audit(execution, stats)
    assert calls == [str(first)]
    assert stats.metrics['native_journal_totals'] == {str(first): {'responses': 4}}
    assert stats.metrics['native_journal_totals_skipped'] == [{
        'stage': 'route', 'route': 'c', 'path': str(second), 'reason': 'collect_journal_totals=False'}]


def test_routed_observation_requires_each_sealed_child_once():
    from demiflow.execution.journal.run_telemetry import CallMetrics
    node = SimpleNamespace(occurrence_id='o', kernel=SimpleNamespace(kind='prompt',
        configuration={'concurrency': 1, 'routes': {'a': {}, 'b': {}}}))
    execution = SimpleNamespace(_graph=SimpleNamespace(nodes=[node]), _bounds=SimpleNamespace(max_control_bytes=16384))
    entries = [{'instance': 0, 'route': name, 'summary': CallMetrics().summary()} for name in ('a', 'b')]
    def observe(value):
        observe_models(execution, SimpleNamespace(boot_id='boot'), {
            'occurrence_id': 'o', 'partition_id': 'p', 'model_observations': value})
    observe(entries)
    for invalid in (entries[:1], [entries[0], entries[0]],
                    [entries[0], {**entries[1], 'route': 'undeclared'}],
                    [entries[0], {**entries[1], 'instance': True}]):
        with pytest.raises(DataflowError):
            observe(invalid)


def test_real_routed_prompts_keep_named_child_call_metrics(prompt_server):
    from demiflow.operator_llm.parser import parse_prompt_pack
    from test_map_prompt_async import PACK
    config = parse_prompt_pack(PACK)
    routes = {name: {'config': parse_prompt_pack(PACK.replace('mock-model', name)), 'concurrency': 1}
              for name in ('a', 'b')}
    routes['a']['request_policy'] = {'initial_concurrency': 1, 'max_concurrency': 1}
    with data.local_execution(workers=4):
        stats = data.from_items([{'item': i, 'route': name} for i, name in enumerate(('a', 'a', 'b'))]).map_prompt_async(
            'enrich', config=config, inputs={'payload': 'item'}, output='answer',
            routes=routes, route='route', concurrency=2).run_stream()
    values = {value['route']: value for value in stats.metrics['models'].values()}
    assert set(values) == {'a', 'b'}
    assert values['a']['call_records'] == values['a']['shared_admission']['requests'] == 2
    assert values['b']['call_records'] == 1 and 'shared_admission' not in values['b']
    assert values['a']['shared_admission']['peak'] <= 1
    assert values['a']['shared_admission']['adaptive']['fresh_responses'] == 2
    assert all('phases' not in value for value in values.values())
    assert stats.emitted == len(prompt_server['requests']) == 3
