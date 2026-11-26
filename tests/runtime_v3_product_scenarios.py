"""Real historical/product gates, with cross-process proofs kept distinct.

Managed workbench workers execute sealed code under Python -I. Their evidence
is the original journal/ACK/plan/receipt, not invented execute_action calls.
Analysis authority and model transports used by selected browser fixtures are
explicit test adapters; actual Chromium, SQLite, Lance, Jupyter and SQL execute.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys

WORKTREE = Path(__file__).resolve().parents[1]
PROOF_SCHEMA = "demiflow.test.original_native_execution.v1"

WORKBENCH_CASES = (
    "tests/test_runtime_v3_product_scenarios.py::test_managed_union_and_nested_materialize_bind_every_occurrence",
    "tests/test_workbench_execution_binding_browser.py::test_real_run_sealed_graph_f10_and_honest_bridge",
    "tests/test_workbench_execution_binding_browser.py::test_real_run_data_graph_source_and_params_still_load",
    "tests/test_forge_workbench_runtime_browser.py::test_native_multiple_runs_select_graph_logs_outputs_and_discard_late_delivery",
    "tests/test_forge_workbench_runtime_browser.py::test_native_stop_survives_current_code_removal_and_keeps_attempt_identity",
    "tests/test_runtime_v3_product_scenarios.py::test_real_sealed_physical_facts_are_visible_in_browser",
    "tests/test_workbench_native_gpu.py::test_real_managed_cuda_plan_bridge_and_browser",
)
PRODUCT_CASES = (
    "tests/test_workbench_native_notebook.py::test_native_edit_execute_save_reopen_and_dictionary",
    "tests/test_runtime_v3_product_scenarios.py::test_managed_union_and_nested_materialize_bind_every_occurrence",
    "tests/test_runtime_v3_product_scenarios.py::test_private_jupyter_sql_uses_original_compiler_broker_native_query_and_ingress",
    "tests/test_forge_outputs_jupyter.py::test_real_private_jupyter_mime_output_projects_to_same_typed_renderer_contract",
    "tests/test_forge_analysis_records.py::test_notebook_response_loss_cas_and_history_survive_controller_crash",
    "tests/test_forge_development_chat_browser.py::test_browser_conversation_navigation_refresh_and_mobile",
    "tests/test_forge_development_chat_browser.py::test_acp_permission_diff_markdown_and_dialog_lifecycle",
    "tests/test_workbench_execution_binding_browser.py::test_real_run_data_graph_source_and_params_still_load",
)


def _sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _owned(path, owner):
    path, owner = Path(path), Path(owner).resolve()
    assert owner.is_relative_to('/tmp') and owner.parts[2].startswith('demiflow-')
    assert path.resolve().is_relative_to(owner), ('evidence outside owned root', path, owner)
    assert not any(p.is_symlink() for p in (path, *path.parents)), 'symlink evidence'
    return path.resolve()


def read_native_execution(workspace, run_id, *, attempt, owned_root):
    """Re-read the actual sealed candidate, ACKs and fixed output; no execution.

    A proof is only emitted for a completed native write. Cancelled runs and
    partial outputs are tested separately and cannot masquerade as this proof.
    """
    import lance
    from demiflow.execution.journal.run_journal import RunJournal
    from demiflow.execution.seal.sealed_plans import load_sealed_plan
    from demiflow.forge.runs import manager as runs

    from demiflow.workbench.runs import facts
    from demiflow.execution.journal.publication_facts import inspect_write_receipt
    from demiflow.workbench.graph import model

    workspace = _owned(workspace, owned_root)
    target = _owned(runs.directory(workspace, run_id), owned_root)
    journal = RunJournal(target, attempt=attempt)
    view = journal.execution_view()
    manifest, state = view['manifest'], view['state']
    assert state['attempt'] == attempt and state['status'] == 'succeeded'
    assert manifest['run_id'] == run_id and Path(manifest['root']).resolve() == workspace
    report = runs.verify_seal(target, manifest, environment=False)
    platform = manifest['platform']
    assert Path(platform['origin']).resolve() == WORKTREE / 'demiflow'
    project_digest = _sha(WORKTREE/'pyproject.toml')
    assert platform['declaration']['mode']=='source'
    assert platform['declaration']['project_sha256']==project_digest
    source_digests = {}
    for item in platform['files']:
        path = Path(item['path'])
        assert not path.is_absolute() and '..' not in path.parts
        actual = _sha(WORKTREE / 'demiflow' / path)
        assert actual == item['sha256'], ('sealed platform is not this candidate', path)
        source_digests[str(path)] = actual
    assert {'execution/action_driver/dataflow.py', 'execution/checkpoint/coordinator.py',
            'workbench/runs/worker.py'} <= set(source_digests)
    result = runs.show(workspace, run_id, attempt=attempt)
    graph = model.get(workspace, basis_kind='run', run_id=run_id, attempt=attempt)
    assert graph['binding']['run_id'] == run_id and graph['binding']['attempt'] == attempt
    report_ids = {node['id'] for node in report['nodes']}
    assert report_ids == {node['id'] for node in graph['nodes']}, 'fixture graph must not be clipped'
    plans = []
    for entry in graph['logical']['actions']:
        if entry.get('binding_level') != 'sealed':
            continue
        action_id = entry['action_id']
        assert entry['version'] == 'v3'
        bundle = load_sealed_plan(target, action_id)
        sealed = facts.read_sealed_plan(target, action_id)
        assert sealed['available'] and sealed['version'] == 'v3'
        assert entry['dag']['dag_id'] == bundle['graph'].dag_id == sealed['dag_id']
        assert entry['plan_id'] == bundle['physical'].plan_id == sealed['plan_id']
        bridge = facts.read_identity_bridge(target, action_id,
            dag_id=bundle['graph'].dag_id, report_digest=report['digest'])
        assert bridge['available'], bridge
        bridge = bridge['bridge']
        assert bridge['entries'] and all(e['report_node_id'] in report_ids for e in bridge['entries'])
        assert bridge['action_id'] == action_id and bridge['report_digest'] == manifest['report_digest']
        action = journal.execution_action(action_id)
        commits = journal.execution_commits(action_id)
        assert action and commits and all(c['status'] == 'acked' for c in commits)
        marker = journal.source_binding(action_id + '-dataflow-sealed')
        assert marker is not None
        plans.append(dict(action_id=action_id, dag_id=sealed['dag_id'], plan_id=sealed['plan_id'],
            marker=marker, action=action, commits=commits, bridge=bridge,
            physical=entry['physical']))
    assert plans, 'no original sealed v3 action'
    outputs = []
    for output in result['outputs']:
        # SaveLance's child scopes have their own stage-commit contract. This
        # proof deliberately requires a parent native write_lance result.
        if output.get('publication') == 'native_stage_v3':
            continue
        action_id = output['action_id']
        assert any(p['action_id'] == action_id for p in plans)
        receipt = inspect_write_receipt(journal, action_id)
        assert receipt is not None and output['result'] is not None
        reference = output['result']['ref']
        _, resolved, uri = runs.result_table(workspace, run_id, output['name'])
        assert str(uri) == output['uri'] == receipt['uri']
        assert resolved.to_dict() == reference
        assert reference['lance_version'] == receipt['committed_version']
        assert reference['row_count'] == receipt['written_rows']
        snapshot = lance.dataset(str(uri), version=reference['lance_version'])
        assert snapshot.count_rows() == reference['row_count']
        outputs.append(dict(action_id=action_id, result=reference, receipt=receipt,
            rows=snapshot.to_table().to_pylist()))
    assert outputs, 'no original ACKed native output'
    return dict(schema=PROOF_SCHEMA, workspace=str(workspace), owned_root=str(Path(owned_root).resolve()),
        run_id=run_id, attempt=attempt, run_digest=manifest['digest'], report_digest=report['digest'],
        candidate_sources=source_digests, candidate_project_digest=project_digest, plans=plans, outputs=outputs,
        import_probe_scope='parent harness only; sealed managed worker has no import probe')


def validate_native_execution_evidence(evidence, *, owned_root):
    """Fail closed on empty/stale/cross-run proofs by re-reading their authority."""
    proofs = evidence.get('original_native_execution_evidence')
    assert type(proofs) is list and proofs, 'missing original native execution evidence'
    for proof in proofs:
        assert type(proof) is dict and proof.get('schema') == PROOF_SCHEMA
        assert Path(proof['owned_root']).resolve() == Path(owned_root).resolve()
        actual = read_native_execution(proof['workspace'], proof['run_id'],
            attempt=proof['attempt'], owned_root=owned_root)
        assert actual == proof, 'original execution evidence differs from journal/plan/receipt/candidate'
    return proofs


def history_identity(fx, fault=None):
    from runtime_v3_history_scenarios import prompt_identities, original_native_history
    identity = prompt_identities()
    native = original_native_history(fx.root / 'original-history')
    assert native['cases']['interrupted']['original_environment_resumed']['written_rows'] == 2
    return dict(enum_identities=identity['identities'], identity_golden=identity, native_history=native,
        support_domain='C12: original native v1 carrier/aligned-barrier-v2 reader, original environment resume; current runtime rejects unsupported continuation',
        unmeasured_carriers=native['unsupported'])


def _browser_gate(fx, scenario, nodeids):
    from runtime_v3_support import NotRunError
    root = fx.root / scenario
    root.mkdir()
    output = root / 'result.json'
    env = dict(os.environ, PYTHONPATH=str(WORKTREE)+os.pathsep+str(WORKTREE/'tests'),
        PYTHONDONTWRITEBYTECODE='1', DEMIFLOW_RESOURCE_ROOT=str(root/'capacity'),
        FORGE_WORKBENCH_EVIDENCE=str(root/'browser'), FORGE_OUTPUT_EVIDENCE=str(root/'kernel'),
        FORGE_ACCEPTANCE_DIR=str(root/'development'), DEMIFLOW_PRODUCT_OWNER=str(fx.root))
    command = [sys.executable, '-B', str(Path(__file__)), 'pytest', str(output), *nodeids]
    with (root/'pytest.log').open('wb') as log:
        process = subprocess.run(command, cwd=WORKTREE, env=env, stdout=log,
            stderr=subprocess.STDOUT, timeout=900)
    assert output.is_file(), (process.returncode, root/'pytest.log')
    result = json.loads(output.read_text())
    # Preserve failed as well as passing children before RuntimeV3Fixture.close
    # removes its owned workspaces. Never reduce a failed report to a node count.
    result['log'] = (root/'pytest.log').read_text()[-256*1024:]
    fx._save_evidence(scenario+'-child', result)
    assert process.returncode == result['exit_code']
    assert not result['collection_errors'] and set(result['collected']) == set(nodeids)
    skipped = [r for r in result['reports'] if r['outcome'] == 'skipped']
    if skipped:
        detail = {'skipped_required_product_cases': skipped, 'log': str(root/'pytest.log')}
        fx._save_evidence(scenario+'-partial', result)
        fx._record_not_run(scenario, detail)
        raise NotRunError(scenario, detail)
    assert process.returncode == 0, (scenario, root/'pytest.log', result['reports'])
    assert len(result['reports']) == len(nodeids)*3
    assert all(r['outcome']=='passed' and not r['wasxfail'] and not r['rerun'] for r in result['reports'])
    proofs = result['original_native_execution_evidence']
    # Independent parent read after all child fixtures/services have shut down.
    validate_native_execution_evidence(result, owned_root=fx.root)
    return dict(required_nodeids=list(nodeids), subprocess_command=command, child_report=result,
        original_native_execution_evidence=proofs,
        child_execute_action_calls=result['execute_action_calls'],
        child_ray_data_import_attempts=result['ray_data_import_attempts'],
        import_probe_scope='test process only; managed sealed subprocesses proven by original native facts',
        explicit_fixture_boundaries={
            'workbench':'real service/manager/checked code/native workers/ACKs/plan bridge; no execution substitute',
            'case':'real authenticated HTTP/SQLite/Lance/Chromium; explicit analysis-helper authority transport adapter',
            'sql':'real Jupyter/compiler/BrokerClient/native query/DataFusion/Lance/Ingress; fixed test broker authority and private path map, not B1/OCI',
            'development':'real UI/service/store/owned tool process; external model transport is deterministic protocol fixture'})


def workbench_real_plan(fx, fault=None):
    # Physical GPU is a required original-profile display witness. The child
    # explicitly skips without authorised hardware; _browser_gate converts
    # any required skip to NotRun, never a CPU-only successful V030 gate.
    return _browser_gate(fx, 'workbench_real_plan', WORKBENCH_CASES)


def product_regression(fx, fault=None):
    return _browser_gate(fx, 'product_regression', PRODUCT_CASES)


def _main():
    import pytest
    from runtime_v3_support import ReportPlugin, _RayDataBlocker, _ExecuteActionRecorder, assert_tree_binding
    assert sys.argv[1] == 'pytest'
    output = Path(sys.argv[2])
    owner = Path(os.environ['DEMIFLOW_PRODUCT_OWNER'])
    _owned(output, owner)
    proofs, properties = [], []

    class Witnesses:
        @pytest.hookimpl(hookwrapper=True)
        def pytest_runtest_call(self, item):
            outcome = yield
            if outcome.excinfo is not None:
                return
            # Explicit fixture roots only; never search the host/main repository.
            roots = {Path(item.funcargs[key]) for key in ('root','isolated','unregistered')
                     if key in item.funcargs and isinstance(item.funcargs[key], Path)}
            from demiflow.forge.runs import manager as runs

            for workspace in roots:
                _owned(workspace, owner)
                parent = workspace/'.forge/runs'
                if not parent.is_dir():
                    continue
                for path in sorted(parent.iterdir()):
                    if re.fullmatch(r'r[0-9a-f]{24}', path.name) is None:
                        continue  # the original driver-resources directories are not runs
                    state = runs.show(workspace, path.name)
                    if state['status'] == 'succeeded':
                        proofs.append(read_native_execution(workspace, path.name,
                            attempt=state['attempt'], owned_root=owner))

        def pytest_runtest_logreport(self, report):
            if report.when == 'call':
                properties.append(dict(nodeid=report.nodeid, properties=report.user_properties,
                    failure=str(report.longrepr) if report.failed else None))

    report = ReportPlugin()
    assert_tree_binding()
    with _RayDataBlocker() as blocker, _ExecuteActionRecorder() as recorder:
        code = pytest.main(['-q','-p','no:cacheprovider','--basetemp='+str(output.parent/'pytest'),
                            *sys.argv[3:]], plugins=[report, Witnesses()])
    output.write_text(json.dumps(dict(exit_code=int(code), collected=report.collected,
        collection_errors=report.collection_errors, reports=report.results, properties=properties,
        module_paths=assert_tree_binding(), original_native_execution_evidence=proofs,
        execute_action_calls=recorder.calls, ray_data_import_attempts=blocker.attempts), indent=2, default=str)+'\n')
    raise SystemExit(code)


if __name__ == '__main__':
    _main()
