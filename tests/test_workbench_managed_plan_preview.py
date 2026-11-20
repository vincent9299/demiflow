"""Registered managed preview uses the real external readonly boundary."""
import json
from pathlib import Path
import socket
import subprocess
import sys

import pytest

from demiflow.forge.schema import ForgeError
from demiflow.workbench.conversations import admission
from demiflow.workbench.conversations.service import Service
from demiflow.workbench.graph.preview import preview
from test_forge_development_chat_managed import managed_project
from test_forge_development_runtime import combined_configuration
from test_forge_project_management import snapshot, cached_snapshot


SOURCE = '''from demiflow import data
def config(): return {}
def run_pipeline():
    return data.from_items([1, 2]).map(lambda row: row+1).take_all()
'''


@pytest.fixture
def registered_preview(managed_project, tmp_path, monkeypatch):
    root, _, original = managed_project
    (root/'orders/orders_pipeline.py').write_text(SOURCE)
    configuration = tmp_path/'combined'; configuration.mkdir()
    path, _, workspace, domain, _ = combined_configuration(configuration, root, monkeypatch)
    value = json.loads(path.read_text())
    value['mode'] = 'restricted'
    value['projects'][workspace] = original['projects']['wfixture']
    value['projects'][workspace]['run_policy'] = 'observe'
    path.write_text(json.dumps(value)); domain.chmod(0o700)
    service = Service(path)
    try:
        from demiflow.workbench.projects.workspace import init_workspace
        init_workspace(root/'orders', existing=True)
        assert admission.lookup(root/'orders')[1]['root'] == str(root)
        yield root, service
    finally: service.close()


def test_managed_readonly_preview_still_constructs_first_action(registered_preview):
    root, _ = registered_preview
    result = preview(root/'orders', entry='orders_pipeline.py:run_pipeline')
    assert result['available'] is True and result['scope'] == 'first_action'
    assert result['entry_completed'] is False and result['kind'] == 'take_all'
    assert result['dag']['nodes']


def test_managed_native_lance_bindings_compile_without_reading_rows_or_writing_output(registered_preview):
    import lance
    import pyarrow as pa
    from demiflow.workbench.projects import workspace as ws
    from demiflow.workbench import project_operations as project_ops
    root, _ = registered_preview
    workspace = root/'orders'
    data = workspace/'data'; data.mkdir()
    (workspace/'tables').mkdir()
    lance.write_dataset(pa.table({'id': [1, 2, 3], 'name': ['One', 'Two', 'Three']}), str(data/'input.lance'))
    ws.add_location(workspace, 'local', data, writable=True)
    project_ops.attach_table(workspace, 'input', relative_uri='input.lance', version=1,
        dataset_id='input', schema_name='input', schema_version='1')
    (workspace/'orders_pipeline.py').write_text('''import resource
from demiflow import data
def config(): return {}
def untouched(row):
    raise AssertionError('Plan preview executed a data row')
def run_pipeline(*, source, target):
    soft, hard = resource.getrlimit(resource.RLIMIT_AS)
    assert 0 < soft <= 4 * 1024**3 and soft == hard
    try:
        resource.setrlimit(resource.RLIMIT_AS, (hard + 4096, hard + 4096))
    except (ValueError, PermissionError):
        pass
    else:
        raise AssertionError('Candidate raised its inherited allocation ceiling')
    return data.read_lance(source['uri'], version=source['version']).map(untouched).write_lance(
        target['uri'], mode='create', return_receipt=True)
''')
    (workspace/'parameters.json').write_text(json.dumps(dict(format='demiflow_forge_flow_parameters_v1', arguments=dict(
        source=dict(kind='table', alias='input'), target=dict(kind='output', location_id='local', relative_uri='preview-only.lance')))))
    result = preview(workspace, entry='orders_pipeline.py:run_pipeline', params='parameters.json')
    assert result['available'] and result['kind'] == 'write_lance' and not result['entry_completed']
    assert result['scope'] == 'first_action' and any(node['op_kind'] == 'lance' for node in result['dag']['nodes'])
    assert result['basis']['params_sha256'] and len(result['dag']['nodes']) >= 3
    assert not (data/'preview-only.lance').exists()
    assert lance.dataset(str(data/'input.lance')).count_rows() == 3


def test_managed_preview_preserves_lower_inherited_allocation_limits():
    code = '''import resource
from demiflow.workbench.conversations.inspection import limit_native_allocations
resource.setrlimit(resource.RLIMIT_AS, (2 * 1024**3, 3 * 1024**3))
limit_native_allocations()
assert resource.getrlimit(resource.RLIMIT_AS) == (2 * 1024**3, 2 * 1024**3)
'''
    result = subprocess.run([sys.executable, '-B', '-c', code], capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize('attack', ['host_read', 'proc_read', 'rule_write', 'network'])
def test_managed_candidate_import_cannot_reach_host_or_modify_authority(registered_preview, tmp_path, attack):
    root, _ = registered_preview
    secret = tmp_path/'host-private'; secret.write_text('FIXTURE_SECRET_MUST_NOT_ESCAPE')
    authority = root/'AGENTS.md'; before = authority.read_bytes()
    with socket.socket() as sentinel:
        sentinel.bind(('127.0.0.1', 0)); sentinel.listen(1); sentinel.settimeout(.1)
        attempts = dict(host_read=f'from pathlib import Path\nraise RuntimeError(Path({str(secret)!r}).read_text())\n',
            proc_read='from pathlib import Path\nPath("/proc/self/environ").read_bytes()\n',
            rule_write=f'from pathlib import Path\nPath({str(authority)!r}).write_text("tampered")\n',
            network=f'import socket\nsocket.create_connection(("127.0.0.1",{sentinel.getsockname()[1]}),timeout=1)\n')
        (root/'orders/orders_pipeline.py').write_text(attempts[attack]+SOURCE)
        with pytest.raises((ForgeError, PermissionError)) as error:
            preview(root/'orders', entry='orders_pipeline.py:run_pipeline')
        assert 'FIXTURE_SECRET_MUST_NOT_ESCAPE' not in str(error.value)
        assert authority.read_bytes() == before
        with pytest.raises(socket.timeout): sentinel.accept()


def test_managed_preview_cannot_inherit_preparation_descriptors(registered_preview, tmp_path):
    from demiflow.workbench.worker import operation
    root, _ = registered_preview
    with (tmp_path/'control-capability').open('w') as capability:
        with pytest.raises(ForgeError, match='cannot inherit preparation authority'):
            operation('plan_preview', root/'orders', entry='orders_pipeline.py:run_pipeline', _pass_fds=(capability.fileno(),))
