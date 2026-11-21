"""A terminal-only author can deliver the same project that the Workbench opens."""
import os
from pathlib import Path
import subprocess
import sys
from textwrap import dedent

from playwright.sync_api import expect

from test_forge_markdown_reading import browser
from test_forge_public_api import service
from test_forge_web_redesign import opened


def test_author_without_workbench_then_open_compile_and_run(tmp_path, browser, monkeypatch):
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN', str(tmp_path/'runtime-domain'))
    root = tmp_path / 'odd-and-even'
    # Use a separate author process. Any import of the product is a contract violation.
    author = dedent("""
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
    """)
    first = author + dedent("""
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
        sys.path.insert(0, str(root))
        from flows.counts import run
        assert run() == 5
    """)
    second = author + dedent("""
        forge('add', 'flow', 'evens')
        (root/'flows/evens.py').write_text(
            "from demiflow import data\\nfrom operators.is_even import is_even\\ndef run(*, source, target):\\n    return data.read_lance(source['uri'], version=source['version']).filter(is_even).write_lance(target['uri'], mode='create', return_receipt=True)\\n")
        (root/'parameters/evens.json').write_text(json.dumps({
            'format': 'demiflow_forge_flow_parameters_v2', 'arguments': {
            'source': {'kind': 'table', 'alias': 'numbers'}, 'target': {'kind': 'output', 'location_id': 'local', 'relative_uri': 'evens.lance', 'mode': 'create', 'expected_version': None}}}))
        assert forge('add', 'flow', '--register', '--entry', 'flows/evens.py:run',
                     '--params', 'parameters/evens.json')['status'] == 'registered'
        assert not (root/'.forge/index.sqlite').exists()
    """)
    author_env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1]))
    subprocess.run([sys.executable, '-B', '-c', first, str(root)], env=author_env, check=True)
    import lance
    import pyarrow as pa
    from demiflow.workbench.projects import workspace
    from demiflow.workbench import project_operations
    # Provision the test's fixed input and output location with the real data service.
    (root/'data').mkdir()
    (root/'tables').mkdir()
    workspace.add_location(root, 'local', root/'data', writable=True)
    lance.write_dataset(pa.table({'id': list(range(10))}, schema=pa.schema([pa.field('id', pa.int64(), nullable=False)])), str(root/'data/numbers.lance'))
    project_operations.attach_table(root, 'numbers', relative_uri='numbers.lance', version=1,
        dataset_id='numbers', schema_name='numbers', schema_version='1')
    subprocess.run([sys.executable, '-B', '-c', second, str(root)], env=author_env, check=True)
    from demiflow.workbench.projects import registry
    from demiflow.workbench.runs import manager
    from test_forge_runs import execute
    console = tmp_path / 'projects.json'
    registry.add(console, root)
    with service(None, registry=console) as app, opened(browser, app) as (page, _):
        page.locator('.function-nav').get_by_role('button', name='管道', exact=True).click()
        with page.expect_response(lambda res: res.url.endswith('/plan-preview') and
                                  res.request.post_data_json.get('entry') == 'flows/evens.py:run',
                                  timeout=65000) as pending:
            page.locator('.wb-flow-record[data-entry="flows/evens.py:run"]').click()
        response = pending.value
        assert response.ok, response.text()
        assert response.json()['basis']['entry'] == 'flows/evens.py:run'
        expect(page.locator('.wb-node').first).to_be_visible()
    run = manager.prepare(root, 'flows/evens.py:run', params='parameters/evens.json')
    result = execute(root, run)
    assert result['status'] == 'succeeded', result
    assert lance.dataset(result['outputs'][0]['uri']).to_table()['id'].to_pylist() == [0, 2, 4, 6, 8]
