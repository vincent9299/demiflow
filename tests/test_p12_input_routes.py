"""Real fixed run products traverse inspection, graph, analysis and worker routes."""
import json
from pathlib import Path

import pytest

from demiflow.forge.platform import content_digest
from demiflow.forge.schema import ForgeError
from demiflow.workbench import project_operations as project_ops
from demiflow.workbench.projects import workspace as ws
from demiflow.workbench.runs import manager as runs, admission
from demiflow.workbench.graph import model, parameters, preview
from demiflow.workbench.analysis import web_sources
from demiflow.workbench.worker import operation
from test_forge_runs import base, root, prepared, execute
from test_workbench_native_submission import native_flow


def fixed_reads(root, result, output, column):
    name = output['name']
    for alias in ['run:' + result['run_id'] + ':' + name, 'run:' + result['run_id'] + ':' + name + ':1']:
        described = project_ops.describe_table(root, alias)
        assert described['reference']['ref'] == output['result']['ref']
        assert described['metadata']['row_count'] == output['result']['ref']['row_count']
        locator = dict(kind='workspace_table', alias=alias,
            binding=described['binding']['digest'], columns=[column])
        source = web_sources.resolve(root, locator)['source']
        value = operation('analysis_source_read', root, source_locator=locator,
            source_snapshot=source['digest'], query=dict(kind='sample', columns=[column], limit=2))
        assert value['source_snapshot'] == source['digest'] and value['rows']
        profile = project_ops.profile_table(root, alias, expected_binding=described['binding']['digest'], columns=[column])
        assert profile['coverage']['complete']
        assert project_ops.table_versions(root, alias)['reason'] == 'run_output_sealed_version'
        with pytest.raises(ForgeError):
            project_ops.table_versions(root, alias, version=1)
        with pytest.raises(ForgeError, match='declaration'):
            operation('analysis_media_sources', root, source_locator=locator,
                source_snapshot=source['digest'], case_key=value['rows'][0]['_rowid'], fields=[column])
    return described


def test_fixed_run_input_reaches_every_inspection_and_parameter_owner(root):
    result = execute(root, prepared(root))
    assert result['status'] == 'succeeded'
    output = result['outputs'][0]
    fixed_reads(root, result, output, 'case_id')
    explicit = 'run:' + result['run_id'] + ':target:1'
    latest = 'run:' + result['run_id'] + ':target'
    entry, path = 'flows/consumer.py:run', 'flows/consumer.json'
    (root / 'flows/consumer.py').write_text('''from demiflow import data
def run(*, source, target):
    return data.read_lance(source['uri'], version=source['version']).limit(1).write_lance(target['uri'], mode='create', return_receipt=True)
''')
    (root / path).write_text(json.dumps(dict(format='demiflow_forge_flow_parameters_v1', arguments={
        'source': dict(kind='table', alias=explicit),
        'target': dict(kind='output', location_id='local', relative_uri='consumer.lance')})))
    ws.register_flow(root, entry, params=path)
    report = project_ops.inspect_flow(root, entry, params=path)
    assert report['status'] == 'checked'
    assert report['binding']['tables'][explicit]['reference']['ref'] == output['result']['ref']
    assert not any('run:' in key for key in report['binding']['controls'])
    assert project_ops.report_status(root, report['digest'])['status'] == 'checked'
    checked = project_ops.check_project(root)
    assert next(item for item in checked['entries'] if item['entry'] == entry)['status'] == 'checked'
    assert project_ops.project_check_status(root)['static_status'] == checked['static_status']
    project_ops.bind_audit_report(root, report['digest'], expected=content_digest(ws.load_workspace(root)[1]))
    observation = project_ops.audit_check(root, kind='report', report_digest=report['digest'],
        expected_policy=ws.audit_status(root)['policy_digest'])
    assert observation['result']['status'] == 'checked'
    assert admission.describe(root, entry, params=path)['status'] == 'requires_prepare'
    native_target = root / 'native-preview-only.lance'
    native = operation('native_submission_inspect', root, entry=entry, limits=runs.limits_with(),
        parameters=dict(format='demiflow_forge_flow_parameters_v1', arguments={
            'source': dict(kind='table', alias=explicit), 'target': {'uri': str(native_target)}}))
    assert native['status'] == 'compiled'
    assert native['binding']['tables'][explicit]['reference']['ref'] == output['result']['ref']
    assert not native_target.exists()
    graph = model.get(root, basis_kind='source', entry=entry, params=path)
    request = dict(path=path, expected_sha256=graph['parameters']['sha256'], graph_binding=graph['binding'],
        replace=[dict(op='replace', path='/arguments/source/alias', value=latest)])
    proposed = parameters.preview(root, **request)
    applied = parameters.update(root, authorize=lambda: None, proposed_sha256=proposed['proposed_sha256'], **request)
    assert applied['applied']
    plan = preview.preview(root, entry=entry, params=path)
    assert plan['available'] and plan['entry_completed'] is False
    assert not (root / 'data/consumer.lance').exists()
    # Original omitted-attempt and effective explicit-attempt parameters both
    # resolve through the real prepare worker before the sealed start recheck.
    continuation = runs.prepare(root, entry, params=path, inputs={'source': explicit})
    assert continuation['report']['binding']['tables'][explicit]['reference']['ref'] == output['result']['ref']
    assert execute(root, continuation)['status'] == 'succeeded'


def test_native_fixed_stage_uses_sealed_root_without_granting_media(root):
    result = execute(root, native_flow(root))
    assert result['status'] == 'succeeded'
    for output in result['outputs']:
        described = fixed_reads(root, result, output, 'x')
        # Native output locations are sealed by the native submission; they are
        # not required to become new writable project catalog locations.
        assert Path(output['uri']).is_file() or Path(output['uri']).is_dir()
        assert described['reference']['ref'] == output['result']['ref']
