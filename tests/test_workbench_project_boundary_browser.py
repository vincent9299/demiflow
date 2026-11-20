"""P12 real browser recovery uses explicit directory maintenance and fixed input."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
from urllib.parse import urlsplit

from playwright.sync_api import expect

import demiflow
from demiflow.workbench.projects import workspace as ws, index
from demiflow.workbench.runs import manager as runs, domain
from demiflow.workbench import project_operations
from test_forge_runs import base, root, prepared
from test_forge_markdown_reading import browser
from test_forge_public_api import service
from test_forge_panel_browser import open_panel
from test_forge_workbench_browser import page_for

TREE = Path(__file__).resolve().parents[1]


def test_pending_start_explicit_cli_sync_retry_and_current_dictionary(root, browser, tmp_path):
    assert Path(demiflow.__file__).resolve().is_relative_to(TREE)
    value = prepared(root); identity = value['run_id']; target = runs.directory(root, identity)
    before = runs.journal(root, identity).execution_status()
    sentinel = root / 'notebooks/history.ipynb'; sentinel.parent.mkdir(exist_ok=True)
    sentinel.write_text(json.dumps(dict(nbformat=4, nbformat_minor=5, metadata={}, cells=[dict(
        cell_type='code', metadata={}, execution_count=1, source=['print(42)'], outputs=[dict(output_type='stream', name='stdout', text=['42\\n'])])])))
    original_notebook = sentinel.read_bytes()
    with ws.control_lock(root):
        marker = index.begin(root, 'run', identity)
    pending = root / '.forge/index-pending' / ('run-' + identity + '.json')
    pending_bytes = pending.read_bytes()
    out = Path(os.environ.get('FORGE_ACCEPTANCE_DIR', str(tmp_path / 'evidence'))); out.mkdir(parents=True, exist_ok=True)
    network = []; receipt = dict(source_sha=subprocess.check_output(['git','rev-parse','HEAD'],cwd=TREE,text=True).strip(),
        imported_from=demiflow.__file__, run_id=identity, attempt=1)
    with service(root, enable_runs=True) as app, page_for(browser, app) as (page, errors):
        receipt['origin'] = app.origin
        assert app.server_port not in (55971,55972,59865,59866)
        page.on('response', lambda response: network.append(dict(path=urlsplit(response.url).path,status=response.status))
                if '/api/' in response.url and len(network)<512 else None)
        open_panel(app, page, dict(v=1, workspace=str(root), kind='run', run_id=identity))
        start = page.get_by_role('button', name='开始这次运行', exact=True)
        expect(start).to_be_enabled(timeout=65000)
        with page.expect_response('**/run-start') as rejected:
            start.click()
        assert rejected.value.status >= 400
        refusal = rejected.value.json()
        assert 'synchronize' in json.dumps(refusal)
        expect(page.locator('.error-box')).to_contain_text('synchronize')
        assert runs.journal(root, identity).execution_status() == before
        assert pending.read_bytes() == pending_bytes
        assert not (target / 'unit-1.lock').exists() and not domain.reserved(root, identity, 1)
        page.screenshot(path=str(out / 'p12-pending-start-refused.png'),full_page=True)
        cli = subprocess.run([sys.executable,'-P','-B','-m','demiflow.cli','workbench','index',str(root),'--synchronize','--json'],
                             cwd=TREE,capture_output=True,text=True,timeout=90)
        assert cli.returncode == 0, cli.stdout + cli.stderr
        synced = json.loads(cli.stdout); assert synced['pending'] == 0 and synced['errors'] == []
        with page.expect_response('**/run-start') as started:
            start.click()
        assert started.value.status == 200, started.value.text()
        expect(page.locator('.wb-run-bar > .wb-tag')).to_have_text('成功',timeout=240000)
        shown = runs.show(root, identity)
        assert shown['status'] == 'succeeded' and shown['attempt'] == 1
        page.get_by_role('button',name='输出结果',exact=True).click()
        page.get_by_label('结果别名 target').fill('p12_fixed')
        with page.expect_response('**/run-attach') as attached:
            page.get_by_role('button',name='保留为输入表',exact=True).click()
        assert attached.value.status == 200, attached.value.text()
        result = attached.value.json()
        assert result['table']['ref'] == shown['outputs'][0]['result']['ref']
        binding = project_operations.describe_table(root,'p12_fixed')['binding']['digest']
        open_panel(app,page,dict(v=2,workspace=ws.workspace_id(root,ws.load_workspace(root)[1]),kind='table_request',alias='p12_fixed'))
        page.wait_for_function('digest=>ForgeView.decodeView(location.hash)?.binding_digest===digest',arg=binding,timeout=65000)
        expect(page.locator('.dictionary-table[open] .dictionary-fields')).to_contain_text('case_id')
        page.reload()
        expect(page.locator('.dictionary-table[open] .dictionary-fields')).to_contain_text('case_id')
        assert page.evaluate('ForgeView.decodeView(location.hash).binding_digest') == binding
        assert sentinel.read_bytes() == original_notebook
        page.screenshot(path=str(out / 'p12-fixed-result-dictionary.png'),full_page=True)
        assert not errors, errors
        receipt.update(refusal=refusal,synchronization=synced,result_ref=result['table']['ref'],binding=binding,
            source_preserved=True,notebook_sha256=hashlib.sha256(original_notebook).hexdigest(),
            pageerrors=errors,network=network,view=page.evaluate('ForgeView.decodeView(location.hash)'))
    receipt['service_closed'] = app.fileno() == -1
    receipt['owned_run_processes_stopped'] = not any(runs.process_matches(shown['state'].get(name)) for name in ('supervisor','worker'))
    (out / 'p12-browser-receipt.json').write_text(json.dumps(receipt,ensure_ascii=False,indent=2)+'\n')
