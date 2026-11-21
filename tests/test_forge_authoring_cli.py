"""A terminal-only author can deliver the same project that the Workbench opens."""
import json
import subprocess
import sys
from textwrap import dedent

from playwright.sync_api import expect

from test_forge_markdown_reading import browser
from test_forge_public_api import service
from test_forge_web_redesign import opened


def test_author_without_workbench_then_open_compile_and_run(tmp_path, browser):
    root = tmp_path / 'odd-and-even'
    # Use a separate author process. Any import of the product is a contract violation.
    program = dedent("""
        import json
        from pathlib import Path
        import sys
        from click.testing import CliRunner
        class ProductImport:
            def find_spec(self, fullname, *args):
                if fullname == 'demiflow.workbench' or fullname.startswith('demiflow.workbench.'):
                    raise AssertionError(fullname)
        sys.meta_path.insert(0, ProductImport())
        from demiflow.cli import main
        root = Path(sys.argv[1])
        def forge(*args):
            result = CliRunner().invoke(main, ['forge', *args, '--root', str(root), '--json'])
            assert result.exit_code == 0, (result.output, result.exception)
            return json.loads(result.output)
        assert forge('context')['status'] == 'ready'
        assert forge('init')['status'] == 'created'
        assert forge('add', 'operator', 'is_even')['status'] == 'created'
        assert forge('add', 'flow', 'counts')['registered'] is False
        (root/'operators/is_even.py').write_text(
            "from typing import TypedDict\\nclass Row(TypedDict):\\n    id: int\\ndef is_even(row: Row) -> bool:\\n    return row['id'] % 2 == 0\\n")
        (root/'flows/counts.py').write_text(
            'from demiflow import data\\nfrom operators.is_even import is_even\\ndef run():\\n    return data.range(10).filter(is_even).count()\\n')
        entry = 'flows/counts.py:run'
        assert forge('check', '--entry', entry)['status'] == 'passed'
        binding = forge('add', 'flow', '--register', '--entry', entry)
        assert binding['status'] == 'registered' and binding['params'] == 'parameters/counts.json'
        assert forge('check')['status'] == 'passed'
        # The native managed run writes a declared output. Provision this private test location.
        from demiflow.forge.platform import content_digest
        from demiflow.forge.project_io import read_control, write_control
        (root/'data').mkdir()
        manifest = root/'workspace.json'
        control = read_control(manifest)
        previous = content_digest(control)
        control['locations']['local'] = {'root': 'data', 'writable': True}
        write_control(manifest, control, expected=previous)
        forge('add', 'flow', 'evens')
        (root/'flows/evens.py').write_text(
            "from demiflow import data\\nfrom operators.is_even import is_even\\ndef run(*, target):\\n    return data.range(10).filter(is_even).write_lance(target['uri'], mode='create', return_receipt=True)\\n")
        (root/'parameters/evens.json').write_text(json.dumps({
            'format': 'demiflow_forge_flow_parameters_v2', 'arguments': {
            'target': {'kind': 'output', 'location_id': 'local', 'relative_uri': 'evens.lance', 'mode': 'create', 'expected_version': None}}}))
        assert forge('add', 'flow', '--register', '--entry', 'flows/evens.py:run',
                     '--params', 'parameters/evens.json')['status'] == 'registered'
        assert not (root/'.forge/index.sqlite').exists()
    """)
    subprocess.run([sys.executable, '-B', '-c', program, str(root)], check=True)
    from demiflow.workbench.projects import registry
    from demiflow.workbench.runs import manager
    from test_forge_runs import execute
    console = tmp_path / 'projects.json'
    registry.add(console, root)
    with service(None, registry=console) as app, opened(browser, app) as (page, _):
        page.locator('.function-nav').get_by_role('button', name='管道', exact=True).click()
        button = page.get_by_role('button', name='解析执行计划', exact=True)
        expect(button).to_be_enabled()
        with page.expect_response(lambda res: res.url.endswith('/plan-preview')) as pending:
            button.click()
        response = pending.value
        assert response.ok, response.text()
        assert response.json()['basis']['entry'] == 'flows/counts.py:run'
        expect(page.locator('.wb-node').first).to_be_visible()
    run = manager.prepare(root, 'flows/evens.py:run', params='parameters/evens.json')
    result = execute(root, run)
    assert result['status'] == 'succeeded', result
    import lance
    assert lance.dataset(result['outputs'][0]['uri']).to_table()['id'].to_pylist() == [0, 2, 4, 6, 8]
