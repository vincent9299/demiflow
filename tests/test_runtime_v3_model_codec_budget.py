"""Native model payload copies use the original finite stage and run grants."""
from dataclasses import replace

import lance
import pytest

from demiflow import data
from demiflow.execution import RunContext
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.planning.dataflow_budget import _model_profile
from demiflow.execution.run_journal import RunJournal
from demiflow.operator_llm.parser import parse_prompt_pack
from demiflow.planning.dataflow_compile import lower_action
from demiflow.planning.dataflow_policy import resolve_runtime_policy
from test_map_prompt_async import PACK, server
from test_execution_payload_limits import wide_graph
from test_runtime_v3_codec_profile_groups import physical, request


def test_real_wide_prompt_keeps_payload_and_paid_effect_on_final_replay(tmp_path, server):
    # The original failing payload, default limits and real native materialize
    # followed by Lance publication. No worker/source grant is borrowed.
    with pytest.raises(RuntimeError, match='controlled interruption after action ACK'):
        with RunContext.start(tmp_path / 'run') as run:
            reference, limits = run.ref, dict(run.limits)
            receipt = wide_graph(tmp_path)
            raise RuntimeError('controlled interruption after action ACK')
    assert len(server['requests']) == 1
    assert receipt.committed_version == 1
    expected = [{'id': 1, 'payload': b'wide\x00original' * 750000, 'answer': 'ok'}]
    assert lance.dataset(str(tmp_path / 'wide.lance')).to_table().to_pylist() == expected
    journal = RunJournal(reference.directory)
    before = {a: journal.execution_commits(a) for a in ('action-1', 'action-2')}
    assert all(any(c['status'] == 'acked' for c in cuts) for cuts in before.values())
    with RunContext.resume(reference) as resumed:
        assert dict(resumed.limits) == limits
        assert wide_graph(tmp_path).committed_version == 1
    assert len(server['requests']) == 1
    assert lance.dataset(str(tmp_path / 'wide.lance')).to_table().to_pylist() == expected
    assert {a: journal.execution_commits(a) for a in before} == before


def test_native_codec_targets_fit_one_original_incremental_component():
    dataset = data.from_items([{'id': 1}]).map_prompt_async('enrich',
        config=parse_prompt_pack(PACK), inputs={'payload': 'id'}, output='answer',
        concurrency=1, queue_depth=1, max_requests=1)
    policy, bounds = resolve_runtime_policy({}, {}, provider='local')
    graph = lower_action(request(dataset), bounds=bounds).graph
    model = next(n.kernel for n in graph.nodes if n.kernel.kind == 'prompt')
    assert model.profile.requests['memory_bytes'] > 65_005_338
    assert model.profile.requests['cpu_millis'] == 100
    control = min(bounds.max_control_bytes, bounds.max_resident_bytes // 8)
    selected = physical(graph, policy, bounds)
    assert len(selected.residency.simultaneous_groups) == 1
    assert sum(s.profile.requests['memory_bytes'] for s in selected.stages) <= bounds.max_resident_bytes - control
    assert all(n.kernel.profile.requests['memory_bytes'] >= bounds.max_batch_bytes + bounds.max_row_bytes
               for n in graph.nodes)


def test_native_publication_is_separate_from_workers_and_must_be_sealed():
    from types import SimpleNamespace
    from demiflow.execution.scheduler.dataflow_publication_budget import graph_workspace_bytes, workspace_bytes
    from demiflow.execution.dataflow_sinks import make_native_terminal_sink
    policy, bounds = resolve_runtime_policy({}, {}, provider='local')
    action = replace(request(data.from_items([{'id': 1}])), kind='write_lance')
    graph = lower_action(action, bounds=bounds).graph
    publication = graph_workspace_bytes(graph)
    control = min(bounds.max_control_bytes, bounds.max_resident_bytes // 8)
    assert publication > 65_005_338
    selected = physical(graph, policy, bounds)
    reserved = selected.residency.control_reserved_bytes
    assert control < reserved <= control + publication
    assert sum(s.profile.requests['memory_bytes'] for s in selected.stages) + reserved <= bounds.max_resident_bytes
    assert workspace_bytes({}) == 0  # Historical absence is not a new default grant.
    terminal = next(n.kernel for n in graph.nodes if n.kind == 'terminal')
    with pytest.raises(DataflowError, match='lacks sealed residency reservation'):
        make_native_terminal_sink(terminal, request=action, services=SimpleNamespace(bounds=bounds),
                                  publication_reserved_bytes=control)


def test_explicit_native_model_grant_is_not_enlarged_and_still_rejects_real_peak(tmp_path, server):
    dataset = data.from_items([{'id': 1, 'payload': b'wide\x00original' * 750000}]).map_prompt(
        'enrich', config=parse_prompt_pack(PACK), inputs={'payload': 'id'}, output='answer', max_requests=1,
        backend_options={'ray': {'worker_resources': {'memory_bytes': 32 * 1024**2, 'cpu': 0.25}}})
    _, bounds = resolve_runtime_policy({}, {}, provider='local')
    model = next(n.kernel for n in lower_action(request(dataset), bounds=bounds).graph.nodes
                 if n.kernel.kind == 'prompt')
    assert model.profile.requests == {'cpu_millis': 250, 'memory_bytes': 32 * 1024**2}
    with pytest.raises(DataflowError, match='record frame codec workspace exceeds original resident budget') as error:
        with RunContext.start(tmp_path / 'run'):
            dataset.materialize()
    assert error.value.code == 'BUDGET_EXCEEDED'
    assert server['requests'] == []


def test_native_concurrency_state_and_accelerator_minima_stay_additive():
    _, bounds = resolve_runtime_policy({}, {}, provider='local')
    bounds = replace(bounds, max_row_bytes=4096, max_batch_bytes=8192, max_state_bytes=16384)
    fixed = _model_profile('gpu', bounds, 3, 'nonrecoverable', codec_workspace=False)
    desired = _model_profile('gpu', bounds, 3, 'nonrecoverable')
    assert fixed.requests['memory_bytes'] == 3 * (4096 + 8192) + 16384
    assert desired.requests['memory_bytes'] >= fixed.requests['memory_bytes']
    assert desired.requests['accelerators'] == [{'type': 'gpu', 'count': 1}]


def test_real_wide_materialized_model_reuses_its_original_ack(tmp_path, server):
    payload = b'wide\x00original' * 750000
    def execute():
        return data.from_items([{'id': 1, 'payload': payload}]).map_prompt_async(
            'enrich', config=parse_prompt_pack(PACK), inputs={'payload': 'id'}, output='answer',
            concurrency=1, queue_depth=1, max_requests=1).materialize().take_all()
    expected = [{'id': 1, 'payload': payload, 'answer': 'ok'}]
    with pytest.raises(RuntimeError, match='controlled interruption after action ACK'):
        with RunContext.start(tmp_path / 'run') as run:
            reference, limits = run.ref, dict(run.limits)
            assert execute() == expected
            raise RuntimeError('controlled interruption after action ACK')
    assert len(server['requests']) == 1
    journal = RunJournal(reference.directory)
    before = journal.execution_commits('action-1')
    assert any(c['status'] == 'acked' for c in before)
    with RunContext.resume(reference) as resumed:
        assert dict(resumed.limits) == limits
        assert execute() == expected
    assert journal.execution_commits('action-1') == before
    assert len(server['requests']) == 1
