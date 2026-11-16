"""The V035 evidence gate refuses false green; one real child proves execution."""
from copy import deepcopy
import json
from pathlib import Path
import sys

import pytest
import runtime_v3_regression_scenarios as gate
from runtime_v3_regression_inventory import snapshot


@pytest.fixture
def fixture(tmp_path, monkeypatch):
    """Synthetic report documents only; these are not business-run evidence."""
    real_root = gate.ROOT
    for name in ('historical-dispositions.json', 'regression-files.json'):
        relative = Path('docs/unified-runtime-v3-20261114/evidence') / name
        (tmp_path / relative).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / relative).write_bytes((real_root / relative).read_bytes())
    source = gate.read_json(real_root / 'docs/unified-runtime-v3-20261114/evidence/historical-dispositions.json')
    changes = {
        'test_every_explicit_option_rejects_lazy_actions_before_reads': 'test_every_explicit_option_supports_lazy_actions_without_eager_reads',
        'test_checkpoint_bridges_map_only_and_replays_without_calls': 'test_retired_checkpoint_is_rejected_and_native_run_replays_without_calls',
        'test_stream_sink_checkpoint_accepts_thread_map_policy_change': 'test_stream_native_recovery_preserves_sealed_policy_and_new_run_can_append',
        'test_nested_relations_and_lazy_workers_reject_before_source_access': 'test_nested_relations_support_stream_options_without_eager_source_access',
        'test_lance_checkpoint_writer_failure_joins_stream_and_closes_workers': 'test_native_lance_writer_failure_joins_stream_and_closes_workers',
    }
    nodes = []
    for case in source['cases']:
        node = case['nodeid']
        for old, new in changes.items():
            node = node.replace('::' + old, '::' + new)
        nodes.append(node)
    nodes += [gate.META_NODE, 'tests/test_leaf.py::test_leaf']
    # Small synthetic baseline for validator-only tests; the real harness uses
    # the repository's source-bound 577-file historical inventory.
    files = sorted({n.split('::')[0] for n in nodes})
    (tmp_path / 'docs/unified-runtime-v3-20261114/evidence/regression-files.json').write_text(
        json.dumps({'count': len(files), 'files': [{'file': f} for f in files]}))
    inputs = {'demiflow/guard-fixture.py': '0' * 64}
    bound = {'head': 'guard-fixture-only', 'inputs': inputs}
    collection = {'kind': 'collection_only', 'args': ['--collect-only', 'tests/'],
        'source_before': bound, 'source_after': bound, 'source_unchanged': True,
        'returncode': 0, 'deselected': [], 'collection_problems': [],
        'items': [{'nodeid': n, 'file': n.split('::')[0]} for n in nodes]}
    monkeypatch.setattr(gate, 'ROOT', tmp_path)
    monkeypatch.setattr(gate, 'snapshot', lambda: bound)
    reviewed = {n.split('::')[0]: {'environment': 'local', 'reason': 'synthetic validator-only fixture'} for n in nodes}
    plan = gate.make_plan([collection], reviewed_files=reviewed)
    run = {**deepcopy(collection), 'kind': 'execution', 'args': nodes,
           'items': [i for i in collection['items'] if i['nodeid'] != gate.META_NODE]}
    run['reports'] = [{'nodeid': i['nodeid'], 'when': phase, 'outcome': 'passed',
                      'wasxfail': None, 'rerun': 0} for i in run['items']
                     for phase in ('setup', 'call', 'teardown')]
    return plan, collection, run, inputs


def check(fixture):
    plan, collection, run, inputs = fixture
    return gate.evaluate(plan, [collection], [run], inputs=inputs)


def test_every_leaf_needs_three_real_phases_and_historical_replacement(fixture):
    result = check(fixture)
    assert result['complete'] and result['historical_replacements_passed']
    assert all(case['passed'] for case in result['dispositions'])
    assert len(result['executed']) + 1 == result['collection']['nodeid_count']


def test_collection_without_execution_is_never_pass(fixture):
    plan, collection, _, inputs = fixture
    result = gate.evaluate(plan, [collection], [], inputs=inputs)
    assert not result['complete'] and result['not_run'] and not result['executed']


@pytest.mark.parametrize('defect', ['setup', 'call', 'teardown', 'skip', 'xfail', 'empty_xfail', 'rerun', 'missing', 'duplicate', 'fail_then_pass'])
def test_phase_counterexamples_never_count_as_pass(fixture, defect):
    plan, collection, run, inputs = fixture
    if defect in ('setup', 'call', 'teardown'):
        next(p for p in run['reports'] if p['when'] == defect)['outcome'] = 'failed'
    elif defect == 'skip': run['reports'][0]['outcome'] = 'skipped'
    elif defect == 'xfail': run['reports'][1]['wasxfail'] = 'expected failure'
    elif defect == 'empty_xfail': run['reports'][1]['wasxfail'] = ''
    elif defect == 'rerun': run['reports'][1]['rerun'] = 1
    elif defect == 'missing': run['reports'].pop(1)
    elif defect == 'duplicate': run['reports'].append(deepcopy(run['reports'][1]))
    else:
        phase = deepcopy(run['reports'][1]); phase['outcome'] = 'failed'
        run['reports'].insert(1, phase)
    result = check(fixture)
    assert not result['complete'] and result['failed']


@pytest.mark.parametrize('defect', ['stale_before', 'stale_after', 'deselected', 'collection_skip', 'duplicate_node', 'unreviewed', 'missing_disposition', 'wrong_mapping', 'partial_inventory', 'recursive'])
def test_invalid_candidate_or_scope_is_rejected(fixture, defect):
    plan, collection, run, inputs = fixture
    if defect == 'stale_before': run['source_before'] = {'inputs': {}}
    elif defect == 'stale_after': run['source_after'] = {'inputs': {}}
    elif defect == 'deselected': run['deselected'] = ['tests/test_other.py::test_x']
    elif defect == 'collection_skip': run['collection_problems'] = [{'outcome': 'skipped'}]
    elif defect == 'duplicate_node': run['items'].append(run['items'][0])
    elif defect == 'unreviewed': plan['entries'][0]['state'] = 'unreviewed'
    elif defect == 'missing_disposition': plan['entries'].pop()
    elif defect == 'wrong_mapping': plan['dispositions'][0]['replacement_nodeids'] = ['fake::test']
    elif defect == 'partial_inventory': collection['args'] = ['--collect-only', 'tests/test_leaf.py']
    elif defect == 'recursive': run['items'].append({'nodeid': gate.META_NODE, 'file': gate.META_NODE.split('::')[0]})
    with pytest.raises(ValueError): check(fixture)


def test_repeated_receipt_cannot_overwrite_first_execution(fixture):
    plan, collection, run, inputs = fixture
    with pytest.raises(ValueError, match='repeated'):
        gate.evaluate(plan, [collection], [run, deepcopy(run)], inputs=inputs)


def test_repository_file_omitted_by_ignore_is_not_complete(fixture, tmp_path):
    (tmp_path / 'tests').mkdir()
    (tmp_path / 'tests/test_ignored.py').write_text('def test_ignored(): assert False\n')
    result = check(fixture)
    assert not result['complete']
    assert result['collection_problems'][0]['nodeid'] == 'tests/test_ignored.py'


def test_only_collected_other_environment_closes_import_skip(fixture):
    plan, collection, run, inputs = fixture
    collection['collection_problems'] = [{'nodeid': 'tests/test_external_ray.py', 'outcome': 'skipped'}]
    assert not check(fixture)['complete']
    # A skip in a module that another environment actually collected is closed.
    collection['collection_problems'] = [{'nodeid': run['items'][0]['file'], 'outcome': 'skipped'}]
    assert check(fixture)['complete']


@pytest.mark.parametrize('defect', ['unreviewed', 'unknown', 'duplicate', 'recursive', 'wrong_environment'])
def test_unreviewed_batches_never_spawn(fixture, tmp_path, monkeypatch, defect):
    plan, _, run, _ = fixture
    entries = {e['nodeid']: e for e in plan['entries']}
    node = run['items'][0]['nodeid']
    batch = {'nodeids': [node], 'environment': 'local'}
    if defect == 'unreviewed': entries[node]['state'] = 'unreviewed'
    elif defect == 'unknown': batch['nodeids'] = ['tests/not_a_test.py::test_missing']
    elif defect == 'duplicate': batch['nodeids'] *= 2
    elif defect == 'recursive': batch['nodeids'] = [gate.META_NODE]
    elif defect == 'wrong_environment': batch['environment'] = 'gpu'
    monkeypatch.setattr(gate.subprocess, 'Popen', lambda *a, **k: pytest.fail('unapproved subprocess started'))
    with pytest.raises(ValueError):
        gate.execute_batch(batch, entries=entries, destination=Path('/tmp/demiflow-unspawned-regression'))


def test_optional_hardware_and_postgres_require_explicit_scope(fixture, monkeypatch):
    plan, _, run, _ = fixture
    node = run['items'][0]['nodeid']
    entries = {e['nodeid']: e for e in plan['entries']}
    entries[node]['environment'] = 'gpu'
    monkeypatch.delenv('DEMIFLOW_REGRESSION_ENVIRONMENTS', raising=False)
    result = gate.execute_batch({'nodeids': [node], 'environment': 'gpu'}, entries=entries,
                                destination=Path('/tmp/demiflow-unspawned-gpu'))
    assert result['not_run']
    entries[node]['environment'] = 'postgres'
    monkeypatch.setenv('DEMIFLOW_REGRESSION_ENVIRONMENTS', 'postgres')
    monkeypatch.delenv('DEMIFLOW_TEST_POSTGRES_DSN', raising=False)
    result = gate.execute_batch({'nodeids': [node], 'environment': 'postgres'}, entries=entries,
                                destination=Path('/tmp/demiflow-unspawned-pg'))
    assert 'DSN' in result['not_run']


def test_real_reviewed_subprocess_runs_actual_unified_aggregate(tmp_path):
    node = 'tests/test_upgrade_v2_unit_A020.py::test_streaming_aggregate_preserves_real_sum'
    entries = {node: {'state': 'reviewed', 'environment': 'local'}}
    result = gate.execute_batch({'nodeids': [node], 'environment': 'local', 'python': sys.executable},
        entries=entries, destination=tmp_path / 'real-child')
    receipt = gate.read_json(result['receipt'])
    assert result['sha256'] == gate.sha256(result['receipt'])
    assert receipt['returncode'] == 0 and receipt['source_unchanged']
    assert receipt['source_before']['inputs'] == snapshot()['inputs']
    assert [r['when'] for r in receipt['reports']] == ['setup', 'call', 'teardown']
    assert all(r['outcome'] == 'passed' for r in receipt['reports'])
    assert [i['nodeid'] for i in receipt['items']] == [node]
    assert not receipt['collection_problems'] and not receipt['deselected']
    for key in ('DEMIFLOW_RESOURCE_ROOT', 'DEMIFLOW_RESOURCE_HOME', 'DEMIFLOW_FORGE_DOMAIN', 'TMPDIR'):
        assert Path(receipt['environment'][key]).is_relative_to(tmp_path / 'real-child')
    assert Path(receipt['environment']['TMPDIR']).is_dir()


def test_missing_historical_file_is_not_silently_retired(fixture, tmp_path):
    path = tmp_path / 'docs/unified-runtime-v3-20261114/evidence/regression-files.json'
    baseline = json.loads(path.read_text())
    baseline['files'].append({'file': 'tests/test_historical_deleted.py'})
    path.write_text(json.dumps(baseline))
    result = check(fixture)
    assert not result['complete']
    assert result['collection_problems'][0]['nodeid'] == 'tests/test_historical_deleted.py'


def test_duplicate_planned_batches_are_refused_before_launch(fixture):
    plan, _, run, _ = fixture
    batch = {'environment': 'local', 'nodeids': [run['items'][0]['nodeid']]}
    plan['batches'] = [batch, deepcopy(batch)]
    with pytest.raises(ValueError, match='duplicated'):
        check(fixture)


@pytest.mark.parametrize('changed', [
    'demiflow/workbench/web/workbench.js',
    'demiflow/workbench/web/workbench.css',
    'demiflow/workbench/web/index.html',
    'tests/fixtures/example.json',
    'examples/forge_workspace/make_demo.py',
    'examples/forge_text_map/flows/config.json',
    'docs/unified-runtime-v3-20261114/plan.json',
])
def test_js_html_css_and_fixture_contract_changes_invalidate_old_receipt(tmp_path, monkeypatch, changed):
    import subprocess
    import runtime_v3_regression_inventory as inventory
    repository = tmp_path / 'owned-repository'
    repository.mkdir()
    subprocess.run(['git', 'init', '-q', str(repository)], check=True)
    source = repository / 'demiflow/__init__.py'
    source.parent.mkdir()
    source.write_text('# tracked source\n')
    subprocess.run(['git', 'add', '.'], cwd=repository, check=True)
    subprocess.run(['git', '-c', 'user.name=Regression fixture', '-c', 'user.email=fixture@invalid',
                    '-c', 'core.hooksPath=/dev/null', 'commit', '-qm', 'owned fixture'],
                   cwd=repository, check=True)
    asset = repository / changed
    asset.parent.mkdir(parents=True, exist_ok=True)
    asset.write_text('original bytes')  # Deliberately untracked, still an input.
    unrelated = repository / 'production-data/not-a-source.json'
    unrelated.parent.mkdir()
    unrelated.write_text('not scanned')
    generated = repository / 'docs/unified-runtime-v3-20261114/evidence/new-verification.json'
    generated.parent.mkdir(parents=True, exist_ok=True)
    generated.write_text('generated evidence is not a recursive input')
    monkeypatch.setattr(inventory, 'ROOT', repository)
    before = inventory.snapshot()
    assert changed in before['inputs']
    assert 'production-data/not-a-source.json' not in before['inputs']
    assert str(generated.relative_to(repository)) not in before['inputs']
    receipt = {'source_before': before, 'source_after': before, 'source_unchanged': True}
    assert gate._bound(receipt, before['inputs'])
    asset.write_text('modified bytes')
    after = inventory.snapshot()
    assert before['head'] == after['head']  # HEAD alone is not a content proof.
    assert not gate._bound(receipt, after['inputs'])
