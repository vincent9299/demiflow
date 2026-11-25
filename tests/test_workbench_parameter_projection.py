"""Editor availability must not be confused with parameter file identity."""
from demiflow.forge.project import initialize_project
from _forge_project_fixtures import declare_existing_flow
import hashlib
import json

import pytest
from playwright.sync_api import expect

from demiflow.forge.schema import ForgeError
from demiflow.workbench.graph import model, parameters
from test_forge_markdown_reading import browser
from test_forge_public_api import service
from test_forge_web_redesign import opened
from test_forge_workspace import demo, isolated


ENTRY = 'flows/questions.py:select'
PARAMS = 'flows/select_a.json'


@pytest.mark.parametrize('document', [
    {'arguments': {'minimum': .5}},
    {'arguments': {'minimum': .5}, 'run_directory': '/tmp/unused-preview'},
    {'format': 'unsupported', 'arguments': {}},
])
def test_uneditable_parameters_keep_file_digest_without_granting_native_editing(isolated, document):
    path = isolated / PARAMS
    raw = json.dumps(document).encode()
    path.write_bytes(raw)
    graph = model.get(isolated, basis_kind='source', entry=ENTRY, params=PARAMS)
    projection = graph['parameters']
    assert projection['sha256'] == hashlib.sha256(raw).hexdigest()
    assert not projection['available'] and projection['readonly']
    assert projection['fields'] == []
    with pytest.raises(ForgeError, match='Unsupported parameter document'):
        parameters.read_file(isolated, PARAMS)
    assert path.read_bytes() == raw


@pytest.mark.parametrize('native', [False, True])
def test_real_change_between_source_and_parameter_read_still_rejects(isolated, monkeypatch, native):
    path = isolated / PARAMS
    if not native:
        path.write_text(json.dumps({'arguments': {'minimum': .5}}))
    original = parameters.public_model

    def changed(*args, **kwargs):
        document = json.loads(path.read_text())
        document['arguments']['minimum'] = .75
        path.write_text(json.dumps(document))
        return original(*args, **kwargs)

    monkeypatch.setattr(parameters, 'public_model', changed)
    with pytest.raises(ForgeError, match='Parameters changed while projecting') as caught:
        model.get(isolated, basis_kind='source', entry=ENTRY, params=PARAMS)
    assert caught.value.code == 'conflict'


def test_registered_plain_configuration_cold_open_switch_and_reload(browser, tmp_path):
    root = tmp_path / 'plain-project'
    initialize_project(root, name='Plain preview')
    (root / 'flow.py').write_text(
        'from demiflow import data\n'
        'def transform(row): raise AssertionError("must not execute rows")\n'
        'def keep(row): return row["id"] > 0\n'
        'def run(*, extra):\n'
        '    rows = data.from_items([{"id":1}]).map(transform)\n'
        '    if extra: rows = rows.filter(keep)\n'
        '    return rows.take_all()\n')
    entry = 'flow.py:run'
    parameter_file = root / 'base.json'
    parameter_file.write_text(json.dumps({'arguments': {'extra': False}}))
    declare_existing_flow(root, entry, params=parameter_file.name)
    with service(root) as app, opened(browser, app) as (page, _):
        flows = page.get_by_role('navigation', name='项目功能').get_by_role('button', name='管线', exact=True)
        flows.click()
        expect(page.locator('.wb-node')).to_have_count(3, timeout=65000)
        for extra, expected in [(True, 4), (False, 3)]:
            # The project changes its saved business values at the registered path.
            raw = json.dumps({'arguments': {'extra': extra}}).encode()
            parameter_file.write_bytes(raw)
            with page.expect_response(lambda r: r.url.endswith('/plan-preview'), timeout=65000) as compiled:
                page.get_by_role('button', name='解析执行计划', exact=True).click()
            response = compiled.value
            assert response.ok, response.text()
            assert response.json()['basis']['params_sha256'] == hashlib.sha256(raw).hexdigest()
            expect(page.locator('.wb-node')).to_have_count(expected)
        page.reload()
        flows.click()
        expect(page.locator('.wb-node')).to_have_count(3, timeout=65000)
        expect(page.locator('.wb-notice')).to_be_hidden()
        expect(page.locator('.wb-actions').get_by_role('button', name='运行', exact=True)).to_be_disabled()

    # Existing declarations with more than one binding still compile precisely
    # the supplied file, independently of which business values are editable.
    from demiflow.workbench.graph.preview import preview
    (root / 'extra.json').write_text(json.dumps({'arguments': {'extra': True}}))
    declare_existing_flow(root, entry, params='extra.json')
    for name, size in [('base.json', 3), ('extra.json', 4)]:
        plan = preview(root, entry=entry, params=name)
        assert len(plan['dag']['nodes']) == size
        assert plan['basis']['params_sha256'] == hashlib.sha256((root / name).read_bytes()).hexdigest()
