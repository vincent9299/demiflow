"""Actual native product integration, plus proof-substitution counterexamples."""
import copy
import os
from pathlib import Path

import pytest

from test_forge_runs import base, root, custom, execute
from runtime_v3_product_scenarios import read_native_execution, validate_native_execution_evidence


@pytest.mark.parametrize('materialized', [False, True])
def test_original_native_proof_rejects_substitution(root, materialized):
    value = custom(root, '''
def run(*, source, target):
    rows = data.read_lance(source['uri'], version=source['version'], columns=['case_id'])
    return rows.write_lance(target['uri'], mode='create', return_receipt=True)
'''.replace('return rows.write_lance', 'return rows.materialize().write_lance' if materialized else 'return rows.write_lance'))
    result = execute(root, value)
    assert result['status'] == 'succeeded', result
    owner = next(p for p in root.parents if p.parent == Path('/tmp') and p.name.startswith('demiflow-'))
    proof = read_native_execution(root, result['run_id'], attempt=result['attempt'], owned_root=owner)
    evidence = dict(original_native_execution_evidence=[proof])
    assert validate_native_execution_evidence(evidence, owned_root=owner)
    with pytest.raises(AssertionError):
        validate_native_execution_evidence(dict(original_native_execution_evidence=[]), owned_root=owner)
    alterations = [
        lambda p: p.update(run_id='r'+'0'*24),
        lambda p: p.update(attempt=2),
        lambda p: p.update(report_digest='0'*64),
        lambda p: p['plans'][0].update(plan_id='0'*64),
        lambda p: p['outputs'][0]['receipt'].update(committed_version=99),
        lambda p: p['candidate_sources'].update({'execution/dataflow.py':'0'*64}),
    ]
    for alter in alterations:
        changed = copy.deepcopy(proof); alter(changed)
        with pytest.raises((AssertionError, ValueError, FileNotFoundError)):
            validate_native_execution_evidence(dict(original_native_execution_evidence=[changed]), owned_root=owner)
    # A valid looking proof cannot mask modified sealed code on disk either.
    sealed = root/'.forge/runs'/result['run_id']/'runtime/demiflow/execution/dataflow.py'
    original = sealed.read_bytes(); mode = sealed.stat().st_mode
    try:
        sealed.chmod(0o600); sealed.write_bytes(original+b'\n# owned negative fixture\n')
        with pytest.raises((AssertionError, ValueError)):
            validate_native_execution_evidence(evidence, owned_root=owner)
    finally:
        sealed.write_bytes(original); sealed.chmod(mode)


def test_managed_union_and_nested_materialize_bind_every_occurrence(root, record_property):
    value = custom(root, '''
def run(*, source, target):
    left = data.read_lance(source['uri'], version=source['version'], columns=['case_id'])
    right = data.read_lance(source['uri'], version=source['version'], columns=['case_id'])
    return left.union(right.materialize()).write_lance(target['uri'], mode='create', return_receipt=True)
''')
    result = execute(root, value)
    assert result['status'] == 'succeeded', result
    owner = Path(os.environ.get('DEMIFLOW_PRODUCT_OWNER', str(next(
        p for p in root.parents if p.parent == Path('/tmp') and p.name.startswith('demiflow-')))))
    proof = read_native_execution(root, result['run_id'], attempt=result['attempt'], owned_root=owner)
    assert len(proof['plans']) == 2, 'nested materialize and outer union each need their own original plan'
    assert sorted(row['case_id'] for row in proof['outputs'][0]['rows']) == ['c01','c01','c02','c02','c03','c03']
    assert len({p['plan_id'] for p in proof['plans']}) == 2
    record_property('original_native_execution', proof)


def test_private_jupyter_sql_uses_original_compiler_broker_native_query_and_ingress(tmp_path, monkeypatch, record_property):
    from runtime_v3_sql_scenario import jupyter_sql
    evidence = jupyter_sql(tmp_path, monkeypatch)
    assert evidence['rows'] == [{'bucket':'a','total':5}, {'bucket':'b','total':7}]
    assert evidence['kernel_status'] == 'succeeded'
    record_property('private_jupyter_sql', evidence)
