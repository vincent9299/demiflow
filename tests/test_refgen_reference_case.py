"""Full reference Dataset graph; only external model transport is synthetic."""
from collections import Counter
import importlib.util
import json
from pathlib import Path
import sys

import lance
import pytest

from demiflow import data
from demiflow.execution.plan_capture import capture_pipeline, build_logical_plan_view
from demiflow.execution.run_context import RunContext
from demiflow.objects import ObjectRef


EXAMPLE = Path(__file__).resolve().parents[1] / 'examples' / 'refgen_reference_case'
LIMITS = {'memory_bytes': 384 * 1024**2, 'max_row_bytes': 256 * 1024,
          'max_batch_bytes': 1024 * 1024, 'max_output_bytes': 32 * 1024**2,
          'max_scratch_bytes': 64 * 1024**2, 'checkpoint_rows': 64,
          'checkpoint_unconfirmed_bytes': 32 * 1024**2}


@pytest.fixture
def case_modules(monkeypatch):
    monkeypatch.syspath_prepend(str(EXAMPLE))
    result = []
    for name, path in [('refgen_case_flow', EXAMPLE / 'flow.py'), ('refgen_case_fixture', EXAMPLE / 'fixture.py')]:
        spec = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(spec)
        monkeypatch.setitem(sys.modules, name, module); spec.loader.exec_module(module)
        result.append(module)
    return tuple(result)


def _image_inputs(request):
    return [part for message in request['body'].get('messages', [])
            for part in (message.get('content') if isinstance(message.get('content'), list) else [])
            if part.get('type') == 'image_url']


def _records(root):
    return lance.dataset(str(root / 'results.lance')).to_table().to_pylist()


def test_full_case_capture_is_one_real_action_with_shared_model_prefixes(case_modules, tmp_path, monkeypatch):
    flow, fixture = case_modules
    # Inputs need not exist for the preview, and no HTTP service is started.
    root = tmp_path / 'not-created'
    config = {'sources': {name: {'uri': str(root / (name + '.lance')), 'version': 1}
                          for name in ('concepts', 'images', 'relations')},
              'output_root': str(root), 'models': fixture.profiles('http://127.0.0.1:1/v1'),
              'execution_kind': 'fixture', 'max_concepts': 1}
    from demiflow.execution import dataflow
    def forbidden(*args, **kwargs): raise AssertionError('Preview attempted execution')
    monkeypatch.setattr(dataflow.DataflowExecution, 'start', forbidden)
    monkeypatch.setattr(dataflow, 'snapshot_source', forbidden)
    captured = capture_pipeline(flow.run, config)
    graph = captured.compiled.graph
    view = build_logical_plan_view(graph, declarations=captured.compiled.declarations,
                                  observation_labels=captured.compiled.observation_labels)
    labels = [node['label'] for node in view['nodes']]
    for label in ('已有图质量筛选', '概念参考价值筛选', '联合出题 v4.1', '合成参考图',
                  '合成图看图定稿 v4.1', 'Boogu 有参无参作答', '概念呈现评分 v22', '文本遵循评分 v1'):
        assert labels.count(label) == 1, labels
    kinds = Counter(node.kernel.kind for node in graph.nodes)
    print('Compiled reference graph:', dict(kinds), 'nonrecoverable:',
          [node.kernel.kind for node in graph.nodes if node.kernel.state == 'nonrecoverable'])
    assert captured.kind == 'write_lance' and len(graph.terminals) == 1
    assert kinds['SaveLanceOp'] == 1 and kinds['join'] == 3 and kinds['union'] >= 3
    assert kinds['reduce'] == 2 and kinds['FilterOp'] == 5
    assert view['dag_id'] == graph.dag_id and not root.exists()
    assert len(view['nodes']) == len(graph.nodes) and len(view['edges']) == len(graph.edges)


def test_full_case_real_http_objects_fixed_delivery_and_history_replay(case_modules, tmp_path):
    flow, fixture = case_modules
    root = tmp_path / 'case'
    class InterruptedAfterFinalAck(RuntimeError): pass
    with fixture.model_transport() as server:
        config = fixture.create_inputs(root, base=server['base'])
        # Moving results must not move the identity of existing input images.
        object_root = root
        root = object_root / 'experiment'
        root.mkdir()
        config.update(object_root=str(object_root), output_root=str(root))
        # A completed RunContext is read-only and cannot create another attempt.
        # Exercise the actual recovery case: action ACK exists, outer run failed
        # before it could finalize. Resume must return that authenticated result.
        with pytest.raises(InterruptedAfterFinalAck):
            with RunContext.start(root / 'run', limits=LIMITS) as run:
                with data.local_execution(workers=4, partitions=1, worker_mode='thread', memory_bytes=384 * 1024**2):
                    receipt = flow.run(**config)
                raise InterruptedAfterFinalAck('controlled interruption after the final action ACK')
        assert receipt.status == 'committed'
        assert not server['errors'], server['errors']
        assert server['counts'] == Counter(quality=2, focus=1, author=1, synthesis=1,
                                          finalize=1, boogu=12, concept=12, alignment=12)
        pairs = lance.dataset(str(root / 'questions.lance')).to_table().to_pylist()
        assert len(pairs) == 2 and {p['status'] for p in pairs} == {'reused', 'finalized'}
        assert all(p['ready'] and p['execution_kind'] == 'fixture' for p in pairs)
        for pair in pairs:
            assert ObjectRef(**pair['reference_image']).read(max_bytes=64 * 1024)
            provenance = json.loads(pair['provenance_json'])
            assert ('finalize' in provenance) == (pair['reference_route'] == 'synthesize')
        rows = _records(root)
        scores = [r for r in rows if r['record_kind'] == 'score']
        summaries = [r for r in rows if r['record_kind'] == 'summary']
        assert len(scores) == 24 and len(summaries) == 12
        assert all(r['status'] == 'scored' and r['execution_kind'] == 'fixture' for r in scores)
        assert all(json.loads(r['summary_json']) == {'expected': 2, 'observed': 2, 'scored': 2,
            'failed': 0, 'uncertain': 0, 'mean_of_scored': 8.0 if r['rubric'] == 'concept' else 10.0} for r in summaries)
        for row in scores:
            assert ObjectRef(**row['generated_image']).read(max_bytes=64 * 1024)
            delivery = json.loads(row['delivery_json'])
            assert delivery['uri'] == str(root / 'questions.lance') and delivery['version'] >= 1
            assert json.loads(row['details_json'])['generation_profile']['parameters'] == config['models']['boogu']['parameters']
            assert (row['reference_image'] is None) == (row['conditioning_mode'] == 'detail_only')
        for pair in pairs:
            for tier in ('short', 'medium', 'long'):
                selected = [r for r in scores if r['pair_id'] == pair['pair_id'] and r['tier'] == tier]
                assert len(selected) == 4 and len({r['instruction'] for r in selected}) == 1
                for mode in ('detail_only', 'detail_reference'):
                    arm = [r for r in selected if r['conditioning_mode'] == mode]
                    assert len({json.dumps(r['generated_image'], sort_keys=True) for r in arm}) == 1
        boogu = [request for request in server['requests'] if request['stage'] == 'boogu']
        assert Counter(len(_image_inputs(request)) for request in boogu) == {0: 6, 1: 6}
        assert all(all(request['body'][key] == value for key, value in config['models']['boogu']['parameters'].items()) for request in boogu)
        assert all(len(_image_inputs(request)) == 1 for request in server['requests'] if request['stage'] in ('concept', 'alignment', 'finalize'))
        before_counts = dict(server['counts'])
        before_versions = {name: lance.dataset(str(root / name)).version for name in ('questions.lance', 'results.lance')}
        with RunContext.resume(run.ref):
            with data.local_execution(workers=4, partitions=1, worker_mode='thread', memory_bytes=384 * 1024**2):
                replay = flow.run(**config)
        assert replay.status == 'committed'
        assert server['counts'] == before_counts
        assert {name: lance.dataset(str(root / name)).version for name in before_versions} == before_versions
        assert _records(root) == rows


def test_failed_generation_keeps_both_score_positions_and_fixed_denominators(case_modules, tmp_path):
    flow, fixture = case_modules
    root = tmp_path / 'failed-case'
    with fixture.model_transport(fail_boogu_at=1) as server:
        config = fixture.create_inputs(root, base=server['base'])
        with RunContext.start(root / 'run', limits=LIMITS):
            with data.local_execution(workers=4, partitions=1, worker_mode='thread', memory_bytes=384 * 1024**2):
                flow.run(**config)
        assert not server['errors'], server['errors']
        assert server['counts'] == Counter(quality=2, focus=1, author=1, synthesis=1,
                                          finalize=1, boogu=12, concept=11, alignment=11)
        rows = _records(root)
        scores = [r for r in rows if r['record_kind'] == 'score']
        summaries = [json.loads(r['summary_json']) for r in rows if r['record_kind'] == 'summary']
        failed = [r for r in scores if r['status'] == 'failed']
        assert len(scores) == 24 and len(failed) == 2 and len(summaries) == 12
        assert {r['rubric'] for r in failed} == {'concept', 'alignment'}
        assert len({r['sample_id'] for r in failed}) == 1 and all(r['score'] is None for r in failed)
        assert all(r['expected'] == r['observed'] == 2 for r in summaries)
        assert sum(r['failed'] for r in summaries) == 2 and sum(r['scored'] for r in summaries) == 22


def test_quality_transport_failure_is_not_an_empty_qualified_pool(case_modules):
    flow, _ = case_modules
    op = flow.op
    row = {'concept_id': 'c', 'image_id': 'i', 'image': {'uri': 'file:///not-read.png', 'sha256': '0' * 64},
           'pixels': b'fixture', 'quality_error': 'controlled transport unavailable'}
    reviewed = op.quality_result(row)
    assert not reviewed['quality_passed'] and op.keep_quality_evidence(reviewed)
    group = op.collect_candidates(None, reviewed)
    selected = op.focus_result(op.focus_input({**group, 'concept': 'fixture', 'description': 'fixture'}))
    author = op.author_input(selected, execution_kind='fixture')
    assert not op.has_images(selected) and not op.author_ready(author)
    failed = list(op.authored_pairs(author))
    assert len(failed) == 2 and {row['slot'] for row in failed} == {'01', '02'}
    assert all(not row['ready'] and row['status'] == 'author_failed'
               and 'controlled transport unavailable' in row['error'] for row in failed)


def test_unreadable_inputs_preserve_failed_scores_without_model_calls(case_modules, tmp_path):
    flow, fixture = case_modules
    root = tmp_path / 'unreadable-case'
    with fixture.model_transport() as server:
        config = fixture.create_inputs(root, base=server['base'])
        for image in (root / 'input-images').glob('*.png'):
            image.unlink()
        with RunContext.start(root / 'run', limits=LIMITS):
            with data.local_execution(workers=4, partitions=1, worker_mode='thread', memory_bytes=384 * 1024**2):
                receipt = flow.run(**config)
        assert receipt.status == 'committed'
        assert not server['requests'] and not server['errors']
        rows = _records(root)
        scores = [row for row in rows if row['record_kind'] == 'score']
        summaries = [json.loads(row['summary_json']) for row in rows if row['record_kind'] == 'summary']
        assert len(scores) == 24 and len(summaries) == 12
        assert all(row['status'] == 'failed' and row['score'] is None and row['generated_image'] is None for row in scores)
        assert all(row['expected'] == row['observed'] == row['failed'] == 2 and row['scored'] == 0 for row in summaries)
