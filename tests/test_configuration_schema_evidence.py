"""Compilation-only evidence: real stages, child scope and incomplete receipts."""
from dataclasses import replace
import json
import logging
import os
from pathlib import Path
import subprocess
import sys

import pytest

import compile_contract_corpus as corpus
from configuration_schema_evidence import ProcessEvidence, source_binding, summarize, MAX_CASES
from configuration_schema_gate import reports_on_purpose
from demiflow.planning import dataflow_configuration as schema

TREE = Path(__file__).resolve().parents[1]


def _compile(context):
    return corpus.lower_case('map', context)[0]


@pytest.fixture(scope='module')
def context(tmp_path_factory):
    return corpus.Context(tmp_path_factory.mktemp('stage-corpus'))


def test_real_stages_complete_before_callback_and_do_not_copy_configuration(context, monkeypatch):
    from demiflow.execution.contracts.dataflow import ActionRequest
    from demiflow.planning.dataflow_compile import lower_action
    events = []
    original = schema.report_configuration_stage
    def observe(nodes, connections, **kwargs):
        before = tuple(id(n.kernel.configuration) for n in nodes)
        result = original(nodes, connections, **kwargs)
        assert result is None
        assert before == tuple(id(n.kernel.configuration) for n in nodes)
        events.append(kwargs['stage'])
    monkeypatch.setattr(schema, 'report_configuration_stage', observe)
    dataset = context.rows().map(corpus.identity)
    action = type('Action', (), {'action_id': 'private-action-not-logged'})()
    request = ActionRequest(kind='take', source=dataset._source, plan=dataset._plan,
                            arguments={'limit': 2}, sink=None, programs=(), action=action, provider='local')
    source = source_binding(TREE)
    observer = ProcessEvidence(source, {'map': 'compiled'})
    with observer, observer.case('map') as case:
        lower_action(request, bounds=corpus.bounds(), on_compiled=lambda _: events.append('callback'))
        case['outcome'] = 'compiled'
    assert events == [*schema.STAGES, 'callback']
    receipt = observer.receipt()
    assert receipt['ok'] and receipt['completed']
    assert receipt['cases']['map']['stages'] == dict.fromkeys(schema.STAGES, 1)


class RecoveryActor:
    def __call__(self, row):
        return row

    def recovery_rows(self):
        return []


def test_shared_binding_is_observed_in_final_with_all_optional_keys(context, monkeypatch):
    from demiflow.execution.contracts.dataflow import ActionRequest
    from demiflow.planning.dataflow_compile import lower_action
    actor = RecoveryActor()
    dataset = context.rows().map(actor).map(actor)
    seen = {}
    original = schema.report_configuration_stage
    def observe(nodes, connections, **kwargs):
        seen[kwargs['stage']] = {key for node in nodes for key in node.kernel.configuration}
        return original(nodes, connections, **kwargs)
    monkeypatch.setattr(schema, 'report_configuration_stage', observe)
    observer = ProcessEvidence(source_binding(TREE), {'shared_recovery': 'compiled'})
    with observer, observer.case('shared_recovery') as case:
        compiled = lower_action(ActionRequest(kind='take', source=dataset._source, plan=dataset._plan,
            arguments={'limit': 2}, sink=None, programs=(),
            action=type('Action', (), {'action_id': 'shared-recovery'})(), provider='local'),
            bounds=corpus.bounds())
        case['outcome'] = 'compiled'
    assert schema.SHARED_KEYS <= seen['final']
    assert not schema.SHARED_KEYS & seen['budget']
    assert observer.receipt()['ok']
    assert not schema.configuration_findings(compiled.graph)


@reports_on_purpose
def test_bad_stage_warns_but_compilation_remains_enabled(context, monkeypatch):
    from demiflow.planning.dataflow_compile import _Capture
    original = _Capture.capture_terminal
    def defect(capture, producer):
        result = original(capture, producer)
        for i, node in enumerate(capture.nodes):
            if node.kernel.kind == 'MapOp':
                capture.nodes[i] = replace(node, kernel=replace(node.kernel, configuration={
                    **node.kernel.configuration, 'task_batch_rows': {}, 'unregistered-private-key': 'secret-value'}))
        return result
    monkeypatch.setattr(_Capture, 'capture_terminal', defect)
    observer = ProcessEvidence(source_binding(TREE), {'map': 'compiled'})
    with observer, observer.case('map') as case:
        assert _compile(context).graph.nodes
        case['outcome'] = 'compiled'
    receipt = observer.receipt()
    assert not receipt['ok']
    assert receipt['cases']['map']['codes'] == {'compiler_key_shape': 4, 'unknown_key': 4}
    assert 'secret-value' not in json.dumps(receipt)


def test_missing_stage_zero_cases_and_unscoped_compiles_cannot_pass(context, monkeypatch):
    with ProcessEvidence(source_binding(TREE), {}) as empty:
        pass
    assert empty.receipt()['problems'] == ['zero_cases']
    with ProcessEvidence(source_binding(TREE), {'map': 'compiled'}) as missing:
        pass
    assert 'unobserved_case' in missing.receipt()['problems']
    with ProcessEvidence(source_binding(TREE), {'map': 'compiled'}) as unscoped:
        _compile(context)
    assert 'unscoped_compilation' in unscoped.receipt()['problems']
    original = schema.report_configuration_stage
    def skip(nodes, connections, **kwargs):
        if kwargs['stage'] != 'graph':
            original(nodes, connections, **kwargs)
    monkeypatch.setattr(schema, 'report_configuration_stage', skip)
    with ProcessEvidence(source_binding(TREE), {'map': 'compiled'}) as observer:
        with observer.case('map') as case:
            _compile(context)
            case['outcome'] = 'compiled'
    assert {'missing_stage', 'stage_order'} <= set(observer.receipt()['problems'])


def test_expected_refusal_and_completed_noncompiler_are_explicit(context):
    baseline = json.loads((TREE / 'tests/fixtures/compile-contract-structure.json').read_text())
    name = next(name for name, result in baseline.items() if not result['ok'])
    expected = baseline[name]['error']
    with ProcessEvidence(source_binding(TREE), {name: expected}) as observer:
        with observer.case(name) as case:
            result = corpus.compile_case(name, context)
            assert not result['ok']
            case['outcome'] = result['error'].split(': ', 1)[0]
    assert observer.receipt()['ok']
    assert observer.receipt()['cases'][name]['outcome'] == expected
    with ProcessEvidence(source_binding(TREE), {}, role='not_a_compiler',
                         role_basis='test-only process: no compile calls') as noncompiler:
        pass
    assert noncompiler.receipt()['ok'] and noncompiler.receipt()['role'] == 'not_a_compiler'
    with pytest.raises(ValueError, match='basis'):
        ProcessEvidence(source_binding(TREE), {}, role='not_a_compiler')


def test_receipt_limits_fail_explicitly_without_silent_truncation(tmp_path):
    with pytest.raises(ValueError, match='budget'):
        ProcessEvidence({}, {str(i): 'compiled' for i in range(MAX_CASES + 1)})
    expected = {'fixture-' + str(i): 'DataflowError:UNSUPPORTED_CAPABILITY' for i in range(20)}
    with ProcessEvidence(source_binding(TREE), expected) as observer:
        for name, outcome in expected.items():
            with observer.case(name) as case:
                case['outcome'] = outcome
    receipt = observer.write(tmp_path / 'limited.json', max_bytes=1024)
    assert not receipt['ok'] and 'evidence_budget_exceeded' in receipt['problems']
    assert (tmp_path / 'limited.json').stat().st_size <= 1024
    assert not summarize({}, source={}, expected_processes={})['ok']
    assert not summarize({}, source={}, expected_processes={str(i): {} for i in range(65)})['ok']


def test_logging_is_explicit_restored_and_receipt_needs_completion():
    logger = logging.getLogger('demiflow.planning.configuration')
    level, handlers = logger.level, list(logger.handlers)
    observer = ProcessEvidence(source_binding(TREE), {'map': 'compiled'})
    assert logger.handlers == handlers
    assert 'missing_completion' in observer.receipt()['problems']
    with observer:
        assert logger.level == logging.DEBUG
    assert logger.level == level and logger.handlers == handlers


@pytest.mark.parametrize('mode', ['clean', 'finding', 'checker_error', 'missing_completion'])
def test_child_process_requires_its_own_installed_probe(tmp_path, mode):
    source = source_binding(TREE)
    output = tmp_path / 'child.json'
    result = subprocess.run([sys.executable, '-B', __file__, mode, str(output), str(tmp_path / 'fixtures')],
                            cwd=TREE, capture_output=True, text=True, timeout=60)
    pid = int(result.stdout.strip())
    assert result.returncode == (17 if mode == 'missing_completion' else 0), result.stderr
    summary = summarize({'child': output}, source=source,
                        expected_processes={'child': {'pid': pid, 'role': 'compiler'}})
    assert summary['ok'] is (mode == 'clean')
    if output.exists():
        receipt = json.loads(output.read_text())
        assert receipt['pid'] != os.getpid()
        assert receipt['cases']['map']['findings'] == (0 if mode == 'clean' else 4)
        if mode == 'checker_error':
            assert receipt['cases']['map']['check_errors'] == 4
            assert 'private-exception' not in output.read_text()
    # An unobserved driver or extra child cannot be inferred from a good child.
    assert not summarize({'child': output, 'driver': None}, source=source, expected_processes={
        'child': {'pid': pid, 'role': 'compiler'}, 'driver': {'pid': os.getpid(), 'role': 'compiler'}})['ok']


def _child(mode, path, root):
    print(os.getpid(), flush=True)
    context = corpus.Context(root)
    if mode == 'missing_completion':
        os._exit(17)
    original = schema._node_findings
    def changed(node, by_occurrence, *, stage):
        yield from original(node, by_occurrence, stage=stage)
        if node.kernel.kind == 'MapOp':
            if mode == 'checker_error':
                raise RuntimeError('private-exception')
            if mode == 'finding':
                yield schema._finding(node, 'occurrence_binding', 'occurrence_id does not name this node',
                                      stage=stage, key='occurrence_id')
    schema._node_findings = changed
    with ProcessEvidence(source_binding(TREE), {'map': 'compiled'}) as observer:
        with observer.case('map') as case:
            _compile(context)
            case['outcome'] = 'compiled'
    observer.write(path)


if __name__ == '__main__':
    _child(*sys.argv[1:])
