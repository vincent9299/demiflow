"""Actual native product integration, plus proof-substitution counterexamples."""
import copy
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from test_forge_runs import base, root, custom, execute
from test_forge_workspace import demo, isolated
from test_forge_markdown_reading import browser
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
    from test_runtime_v3_acceptance import _run_unified
    def gate(document):
        result = dict(document, ray_data_import_attempts=[], module_paths={}, execute_action_calls=[])
        return _run_unified(SimpleNamespace(root=owner, run=lambda *a,**k:result), 'workbench_real_plan')
    assert gate(evidence)['execute_action_calls'] == []  # Original facts are not fabricated calls.
    with pytest.raises(AssertionError):
        gate(dict(original_native_execution_evidence=[]))
    alterations = [
        lambda p: p.update(run_id='r'+'0'*24),
        lambda p: p.update(attempt=2),
        lambda p: p.update(report_digest='0'*64),
        lambda p: p.update(candidate_project_digest='0'*64),
        lambda p: p['plans'][0].update(plan_id='0'*64),
        lambda p: p['outputs'][0]['receipt'].update(committed_version=99),
        lambda p: p['candidate_sources'].update({'execution/action_driver/dataflow.py':'0'*64}),
    ]
    for alter in alterations:
        changed = copy.deepcopy(proof); alter(changed)
        with pytest.raises((AssertionError, ValueError, FileNotFoundError)):
            gate(dict(original_native_execution_evidence=[changed]))
    # A valid looking proof cannot mask modified sealed code on disk either.
    sealed = root/'.forge/runs'/result['run_id']/'runtime/demiflow/execution/action_driver/dataflow.py'
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


def test_real_sealed_physical_facts_are_visible_in_browser(isolated, browser, monkeypatch, tmp_path, record_property):
    from playwright.sync_api import expect
    from test_forge_public_api import service
    from test_forge_workbench_browser import page_for
    from test_workbench_execution_binding_browser import select_registered_flow
    from demiflow.workbench.runs import manager as runs
    from demiflow.workbench.graph import model
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN', str(tmp_path/'domain'))
    with service(isolated,enable_runs=True) as app, page_for(browser,app) as (page,errors):
        select_registered_flow(page)
        page.get_by_role('button',name='运行',exact=True).click()
        expect(page.locator('.wb-run-bar > .wb-tag')).to_have_text('成功')
        run_id = page.evaluate('ForgeView.decodeView(location.hash)')['run_id']
        result = runs.show(isolated, run_id)
        graph = model.get(isolated,basis_kind='run',run_id=run_id,attempt=result['attempt'])
        entry = next(e for e in graph['logical']['actions'] if e.get('binding_level')=='sealed')
        physical = entry['physical']
        page.get_by_role('button',name='运行封存计划（E2）',exact=True).click()
        expect(page.locator('.wb-logical-bar')).to_contain_text('恢复级别 '+physical['recovery'])
        expect(page.locator('.wb-logical-bar')).to_contain_text('交换通道 '+str(len(physical['exchanges'])))
        expect(page.locator('.wb-logical-bar')).to_contain_text('恢复限制 '+str(len(physical['limitations']))+' 项')
        for exchange in physical['exchanges']:
            expect(page.locator('.wb-logical-bar')).to_contain_text(exchange['mode'])
        page.locator('.wb-graph .wb-node').filter(has_text='normalize_question').click()
        inspector = page.locator('.wb-inspector')
        expect(inspector).to_contain_text('资源画像')
        # Compare visible values to the original projected stage; never supply a
        # fixture physical plan to the renderer or substitute computed labels.
        bridge = entry['identity_bridge']['bridge']['entries']
        report_id = next(n['id'] for n in graph['nodes'] if (n.get('operator') or {}).get('name')=='normalize_question')
        occurrence = next(e['occurrence_id'] for e in bridge if e['report_node_id']==report_id)
        stage = physical['stages'][occurrence]
        for key,value in stage['profile']['requests'].items():
            expect(inspector).to_contain_text(str(key)+'='+str(value))
        assert not errors, errors
        record_property('actual_sealed_physical_display',dict(run_id=run_id,attempt=result['attempt'],
            plan_id=entry['plan_id'],dag_id=entry['dag']['dag_id'],stage=stage,
            exchanges=physical['exchanges'],recovery=physical['recovery'],limitations=physical['limitations']))
