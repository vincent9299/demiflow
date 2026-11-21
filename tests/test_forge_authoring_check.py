"""Forge checks inspect selected source, configuration and SDK bytes without a running product."""
from hashlib import sha256
import json

import pytest

from demiflow.forge.api_reference import build_reference
from demiflow.forge.pipeline_check import check_project, verify_check_inputs
from demiflow.forge.project import initialize_project
from demiflow.forge.schema import ForgeError


@pytest.fixture(scope='module')
def reference():
    return build_reference()


@pytest.fixture
def project(tmp_path):
    initialize_project(tmp_path)
    for name in ('flows', 'operators', 'prompts', 'parameters'):
        (tmp_path / name).mkdir()
    return tmp_path


def put(root, path, value):
    (root / path).write_text(value if isinstance(value, str) else json.dumps(value))


def set_entries(root, entries):
    manifest = json.loads((root / 'workspace.json').read_bytes())
    manifest['entries'] = entries
    put(root, 'workspace.json', manifest)


def test_empty_project_and_targeted_variants(project, reference):
    assert check_project(project, reference=reference)['status'] == 'not_applicable'
    put(project, 'flows/a.py', 'from demiflow.data import range\ndef run(n=4):\n    return range(n).count()\n')
    for i in range(2):
        put(project, f'parameters/{i}.json', {'format': 'demiflow_forge_flow_parameters_v2', 'arguments': {'n': i+1}})
    entry = 'flows/a.py:run'
    default = check_project(project, reference=reference, entry=entry)
    assert default['status'] == 'passed', default
    set_entries(project, {entry: ['parameters/0.json', 'parameters/1.json']})
    both = check_project(project, reference=reference, entry=entry)
    assert both['status'] == 'passed', both
    assert [row['params'] for row in both['entries']] == ['parameters/0.json', 'parameters/1.json']
    one = check_project(project, reference=reference, entry=entry, params='parameters/1.json')
    assert len(one['entries']) == 1 and set(one['basis']['parameter_digests']) == {'parameters/1.json'}
    assert one['basis']['sources'] == {'flows/a.py': sha256((project/'flows/a.py').read_bytes()).hexdigest()}
    assert not list((project / '.forge').glob('inspections/*'))
    (project / 'parameters/1.json').write_text((project / 'parameters/1.json').read_text()+' ')
    with pytest.raises(ForgeError, match='changed'):
        verify_check_inputs(project, one['basis'])


def test_shared_operator_missing_deleted_and_restricted(project, reference):
    put(project, 'operators/id.py', 'from typing import TypedDict\nclass Row(TypedDict):\n    id: int\ndef positive(row: Row) -> bool:\n    return row["id"] > 0\n')
    put(project, 'flows/left.py', 'from demiflow.data import range\nfrom operators.id import positive\ndef run():\n    return range(4).filter(positive).count()\n')
    put(project, 'flows/right.py', (project/'flows/left.py').read_text())
    put(project, 'parameters/shared.json', {'format': 'demiflow_forge_flow_parameters_v2', 'arguments': {}})
    entries = {f'flows/{n}.py:run': ['parameters/shared.json'] for n in ('left','right')}
    set_entries(project, entries)
    both = check_project(project, paths=['operators/id.py'], reference=reference)
    assert both['status'] == 'passed', both
    assert set(entries) == {row['entry'] for row in both['entries']}
    assert 'operators/id.py' in both['basis']['sources']
    restricted = check_project(project, entry=next(iter(entries)), reference=reference, exclude=['operators'])
    assert restricted['status'] == 'incomplete', restricted
    assert 'operators/id.py' not in restricted['basis']['sources']
    (project/'operators/id.py').unlink()
    deleted = check_project(project, paths=['operators/id.py'], reference=reference)
    assert deleted['status'] in {'incomplete','failed'} and deleted['coverage']['unverified']


def test_detached_definitions_and_nonflow_paths(project, reference):
    put(project, 'operators/normalize.py', 'def normalize(key: str) -> str:\n    return key.lower()\n')
    report = check_project(project, paths=['operators/normalize.py'], reference=reference)
    assert report['status'] == 'passed', report
    assert report['native_flow']['status'] == 'not_applicable'
    assert report['definitions'] == [dict(path='operators/normalize.py', symbols=['normalize'], scope='definition')]
    put(project, 'prompts/bad.yaml', 'schema_version: invalid\n')
    bad = check_project(project, paths=['prompts/bad.yaml'], reference=reference)
    assert bad['status'] == 'failed' and bad['diagnostics'][0]['path'] == 'prompts/bad.yaml'
    put(project, 'README.md', '# Local notes\n')
    assert check_project(project, paths=['README.md'], reference=reference)['status'] == 'not_applicable'


def test_budget_and_missing_required_parameter(project, reference):
    put(project, 'flows/a.py', 'from demiflow.data import range\ndef run(count):\n    return range(count).count()\n')
    entry = 'flows/a.py:run'
    missing = check_project(project, entry=entry, reference=reference)
    assert missing['status'] in {'failed','incomplete'} and any('count' in d['detail'] for d in missing['diagnostics']), missing
    for i in range(2):
        put(project, f'parameters/{i}.json', {'format': 'demiflow_forge_flow_parameters_v2', 'arguments': {'count': i+2}})
    set_entries(project, {entry: ['parameters/0.json','parameters/1.json']})
    partial = check_project(project, reference=reference, max_variants=1)
    assert partial['status'] == 'incomplete' and len(partial['entries']) == 1
    assert partial['coverage']['requested_variants'] == 2 and partial['coverage']['unverified']


@pytest.mark.parametrize('path', ['flows/visible.py', 'operators/shared.py'])
def test_changed_path_reports_inaccessible_consumers_without_disclosing_them(project, reference, path):
    put(project, 'operators/shared.py', 'def normalize(key: str) -> str:\n    return key.lower()\n')
    put(project, 'flows/visible.py', 'from demiflow.data import from_items\nfrom operators.shared import normalize\n'
        'def run():\n    return from_items([{\'key\':\'A\'}]).map(normalize, inputs={\'key\':\'key\'}, output=\'result\').count()\n')
    public, private = 'parameters/visible.json', 'parameters/private-name.json'
    for binding in (public, private):
        put(project, binding, {'format': 'demiflow_forge_flow_parameters_v2', 'arguments': {}})
    set_entries(project, {'flows/visible.py:run': [public,private]})
    report = check_project(project, paths=[path], exclude=[private], reference=reference)
    assert report['status'] == report['native_flow']['status'] == 'incomplete', report
    assert report['coverage']['unverified'] and any(row['params'] == public for row in report['entries'])
    assert private not in json.dumps(report)


def test_unrelated_registered_layout_does_not_fail_changed_operator(project, reference):
    put(project, 'misplaced.py', 'from demiflow.data import range\ndef run():\n    return range(3).count()\n')
    put(project, 'operators/normal.py', 'def normal(name: str) -> str:\n    return name.lower()\n')
    put(project, 'parameters/registered.json', {'format':'demiflow_forge_flow_parameters_v2','arguments':{}})
    set_entries(project, {'misplaced.py:run':['parameters/registered.json']})
    full = check_project(project, reference=reference)
    assert full['status'] == 'failed' and full['project_layout']['status'] == 'failed'
    local = check_project(project, paths=['operators/normal.py'], reference=reference)
    assert local['status'] == 'passed' and not local['entries'] and not local['diagnostics'], local
    assert local['native_flow']['status'] == 'not_applicable' and local['definitions'][0]['symbols'] == ['normal']
    put(project, 'misplaced.py', 'from external import get_dataset\ndef run():\n    return get_dataset().count()\n')
    unknown = check_project(project, paths=['operators/normal.py'], reference=reference)
    assert unknown['status'] == 'incomplete' and unknown['coverage']['unverified'], unknown
