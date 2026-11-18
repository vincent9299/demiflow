"""Editor availability must not be confused with parameter file identity."""
import hashlib
import json

import pytest
from playwright.sync_api import expect

from demiflow.forge.schema import ForgeError
from demiflow.workbench.graph import model, parameters
from demiflow.workbench.projects import workspace as ws
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
    ws.init_workspace(root, name='Plain preview')
    (root / 'flow.py').write_text(
        'from demiflow import data\n'
        'def transform(row): raise AssertionError("must not execute rows")\n'
        'def keep(row): return row["id"] > 0\n'
        'def run(*, extra):\n'
        '    rows = data.from_items([{"id":1}]).map(transform)\n'
        '    if extra: rows = rows.filter(keep)\n'
        '    return rows.take_all()\n')
    for name, extra in [('base.json', False), ('extra.json', True)]:
        (root / name).write_text(json.dumps({'arguments': {'extra': extra}}))
        ws.register_flow(root, 'flow.py:run', params=name)
    with service(root) as app, opened(browser, app) as (page, requests):
        graph_reply = lambda response: '/graph?' in response.url
        # Wait for the initial graph response *before* clicking compile. A
        # compile-only test missed this registered-source initialization error.
        with page.expect_response(graph_reply) as first:
            page.locator('.function-nav').get_by_role('button', name='管道', exact=True).click()
        assert first.value.status == 200, first.value.text()
        expect(page.locator('.wb-graph')).not_to_have_attribute('aria-busy', 'true')
        expect(page.locator('.wb-notice')).to_be_hidden()
        expect(page.locator('.wb-node')).to_have_count(0)
        for name, expected in [('base.json', 3), ('extra.json', 4), ('base.json', 3)]:
            selector = page.get_by_role('combobox', name='配置', exact=True)
            if selector.input_value() != name:
                with page.expect_response(graph_reply) as switched:
                    selector.select_option(name)
                assert switched.value.status == 200, switched.value.text()
                expect(page.locator('.wb-node')).to_have_count(0)
            with page.expect_response(lambda response: response.url.endswith('/plan-preview')) as compiled:
                page.get_by_role('button', name='解析执行计划', exact=True).click()
            assert compiled.value.status == 200, compiled.value.text()
            expect(page.locator('.wb-node')).to_have_count(expected)
            expect(page.locator('.wb-notice')).to_be_hidden()
        with page.expect_response(graph_reply) as reloaded:
            page.reload()
        assert reloaded.value.status == 200, reloaded.value.text()
        expect(page.locator('.wb-node')).to_have_count(3)
        expect(page.locator('.wb-notice')).to_be_hidden()
        expect(page.get_by_role('button', name='运行', exact=True)).to_be_disabled()
