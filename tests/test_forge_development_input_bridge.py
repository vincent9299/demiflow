"""Actual confined CLI -> trusted input bridge -> external read-only data."""
import hashlib
import json
from pathlib import Path
import shutil
import sys

import pyarrow as pa
import pyarrow.parquet as pq

from demiflow.workbench.api import WorkspaceServer
from test_forge_development_chat import wait_turn
from test_forge_development_runtime import combined_configuration


def test_restricted_cli_reads_external_input_without_user_path_or_registration(tmp_path, monkeypatch):
    root = tmp_path / "orders"
    root.mkdir()
    # Enable scoped reads on this project so native CLI cannot read host paths outside root
    # while the trusted read_input bridge still grants scoped read-only access to external inputs.
    (root / 'pyproject.toml').write_text('[tool.demiflow.packages]\n', encoding='utf-8')
    external = root.with_name(root.name + '-inputs')
    external.mkdir()
    try:
        (external / 'input.txt').write_text('external-input-fixture\n')
        pq.write_table(pa.table({'case_id': ['one', 'two'], 'score': [0.3, 0.9]}), external / 'input.parquet')
        token = external / 'session-key.opaque'
        token.write_text('offline-fixture-' + 'x' * 43); token.chmod(0o600)
        (external / 'ready.json').write_text(json.dumps(dict(token=token.read_text())))
        before = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in external.iterdir()}
        host = tmp_path / 'host'; host.mkdir()
        path, registry, workspace, domain, _ = combined_configuration(host, root, monkeypatch)
        config = json.loads(path.read_text())
        config['projects'][workspace]['kind'] = 'pipeline'
        domain.chmod(0o700)
        executable = host / 'input-provider'
        fixture = Path(__file__).with_name('devchat_fake_input_provider.py')
        executable.write_text('#!' + sys.executable + '\nimport runpy\nrunpy.run_path(' + repr(str(fixture)) + ',run_name="__main__")\n')
        executable.chmod(0o700)
        config['mode'] = 'restricted'; config['limits']['turn_seconds'] = 45
        config['providers']['codex']['executable'] = str(executable)
        # The fake harness never makes a model request; retain the real broker
        # handshake using a deliberately unreachable local-only endpoint.
        monkeypatch.setenv('FIXTURE_INPUT_KEY', 'offline-fixture-only')
        config['providers']['codex'].update(base_url='http://127.0.0.1:1/v1',
            credential_env='FIXTURE_INPUT_KEY')
        path.write_text(json.dumps(config))
        server = WorkspaceServer(registry=registry, development_config=path, token_file=token)
        service = server.development
        try:
            sid = service.create_project(workspace, root, 'create', 'codex')['id']
            started = service.start_project(workspace, root, sid, 'read', 0, '请先看看有什么可用的输入数据，说明发现即可。')
            saved = wait_turn(service, started['id'])
            assert saved['state'] == 'completed', saved['result']
            assert 'pipeline_checks' not in json.loads(saved['result'])
            assert json.loads(saved['context'])['input_access'] == 'conversation_readonly'
            events = service.store.events(workspace, sid)['events']
            assert any('已读取项目外输入' in e['data'].get('text', '') for e in events)
            assert {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in external.iterdir()} == before
            assert not service.store.pending(started['id'])
        finally:
            server.server_close()
    finally:
        shutil.rmtree(external)


def test_notebook_turn_generates_a_cell_with_read_only_project_access(tmp_path,monkeypatch):
    root=tmp_path/'orders';root.mkdir()
    host=tmp_path/'host';host.mkdir()
    path,registry,workspace,domain,_=combined_configuration(host,root,monkeypatch)
    config=json.loads(path.read_text())
    config['mode']='restricted';domain.chmod(0o700)
    monkeypatch.setenv('NOTEBOOK_FIXTURE_KEY','offline-fixture-only')
    config['providers']['codex'].update(base_url='http://127.0.0.1:1/v1',credential_env='NOTEBOOK_FIXTURE_KEY')
    path.write_text(json.dumps(config))
    server=WorkspaceServer(registry=registry,development_config=path)
    try:
        service=server.development
        assert service.capabilities(workspace,root)['notebook_cells']
        sid=service.create_project(workspace,root,'create','codex',new_conversation=True)['id']
        args=(workspace,root,sid,'cell-1',0,f'generate notebook widget fixture {root / "notes.py"}')
        started=service.start_project(*args,operation='notebook_cell')
        assert service.start_project(*args,operation='notebook_cell')['id']==started['id']
        saved=wait_turn(service,started['id'])
        assert saved['state']=='completed',saved['result']
        assert json.loads(saved['context'])['operation']=='notebook_cell'
        assert json.loads(saved['result'])['execution_assurance']=='enforced'
        events=service.store.events(workspace,sid)['events']
        assert any(e['type']=='message.completed' and 'IntSlider()' in e['data']['text'] for e in events)
        assert (root/'notes.py').read_text()=='# fixture note\n'
    finally:server.server_close()
