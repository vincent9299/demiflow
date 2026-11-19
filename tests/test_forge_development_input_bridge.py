"""Actual confined CLI -> trusted input bridge -> external read-only data."""
import hashlib
import json
from pathlib import Path
import shutil
import sys

import pyarrow as pa
import pyarrow.parquet as pq

from demiflow.workbench.api import WorkspaceServer
from test_forge_development_chat_managed import managed_project, gate_done
from test_forge_development_runtime import combined_configuration


def test_restricted_cli_reads_external_input_without_user_path_or_registration(managed_project, tmp_path, monkeypatch):
    root, path, config = managed_project
    external = root.with_name(root.name + '-inputs')
    external.mkdir()
    try:
        (external / 'input.txt').write_text('external-input-fixture\n')
        pq.write_table(pa.table({'case_id': ['one', 'two'], 'score': [0.3, 0.9]}), external / 'input.parquet')
        token = external / 'session-key.opaque'
        token.write_text('offline-fixture-' + 'x' * 43); token.chmod(0o600)
        (external / 'ready.json').write_text(json.dumps(dict(token=token.read_text())))
        before = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in external.iterdir()}
        declaration = (root / '.demiforge/project.json').read_bytes()
        executable = path.parent / 'input-provider'
        fixture = Path(__file__).with_name('devchat_fake_input_provider.py')
        executable.write_text('#!' + sys.executable + '\nimport runpy\nrunpy.run_path(' + repr(str(fixture)) + ',run_name="__main__")\n')
        executable.chmod(0o700)
        host = tmp_path / 'host'; host.mkdir()
        host_config, registry, workspace, domain, _ = combined_configuration(host, root, monkeypatch)
        partition = json.loads(host_config.read_text()); domain.chmod(0o700)
        config.update({key: partition[key] for key in ('state_root', 'runtime_domain', 'host_budget')})
        config['projects'][workspace] = config['projects'].pop('wfixture')
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
            saved, result = gate_done(service, started['id'], timeout=45)
            assert saved['state'] == 'completed', saved['result']
            task = json.loads((service.root / 'forge-development' / workspace / sid / 'task.json').read_text())
            assert result['state'] == 'not_applicable', json.dumps(dict(result=result, check=task['check']), ensure_ascii=False)
            assert task['verification'] is None and task['review'] is None
            assert task['managed']['acceptance'] is None
            assert json.loads(saved['context'])['input_access'] == 'conversation_readonly'
            events = service.store.events(workspace, sid)['events']
            assert any('已读取项目外输入' in e['data'].get('text', '') for e in events)
            assert (root / '.demiforge/project.json').read_bytes() == declaration
            assert {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in external.iterdir()} == before
            assert not service.store.pending(started['id'])
        finally:
            server.server_close()
    finally:
        shutil.rmtree(external)
