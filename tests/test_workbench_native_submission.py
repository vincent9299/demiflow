"""Managed native flows use the original service, journal and fixed results."""
from _forge_project_fixtures import declare_existing_flow
import json
from pathlib import Path
import pytest
import time

from demiflow.workbench.projects import workspace as ws
from demiflow.workbench.runs import manager as runs
from demiflow.workbench import project_operations as project_ops
from test_forge_runs import base, root, execute
from test_forge_resume import stopped
from test_forge_runs import wait


def native_flow(root, imports='', value='1', transform='', *, limits=None, python=None):
    (root / 'flows/native.py').write_text('''from demiflow import data
import pyarrow as pa
''' + imports + f'''
def run(*, target, stage):
    schema = pa.schema([('x', pa.int64())])
    return data.from_items([{{'x': {value}}}, {{'x': 2}}]){transform}.save_lance(stage, schema=schema,
        key='x', stage='rows', mode='append', max_rows=2, max_index_bytes=8388608
    ).write_lance(target, schema=schema, mode='create', return_receipt=True)
''')
    arguments = dict(target=str(root / 'native-output.lance'), stage=str(root / 'native-stage.lance'))
    (root / 'flows/native.json').write_text(json.dumps(dict(format='demiflow_forge_flow_parameters_v1', arguments=arguments)))
    declare_existing_flow(root, 'flows/native.py:run', params='flows/native.json')
    return runs.prepare(root, 'flows/native.py:run', params='flows/native.json', limits=limits, python=python)


def test_native_submit_seal_monitor_and_fixed_results(root):
    value = native_flow(root)
    assert value['status'] == 'prepared'
    assert value['report']['status'] == 'compiled'
    assert not (root / 'native-output.lance').exists()
    result = execute(root, value)
    assert result['status'] == 'succeeded', (result, runs.logs(root, value['run_id']))
    assert len(result['outputs']) == 2
    for output in result['outputs']:
        _, ref, uri = runs.result_table(root, result['run_id'], output['name'], attempt=1)
        assert ref.row_count == 2 and uri == Path(output['uri'])
    from demiflow.workbench.graph.model import get
    graph = get(root, basis_kind='run', run_id=result['run_id'], attempt=1)
    assert graph['logical']['actions'][0]['status'] == 'sealed'


@pytest.mark.parametrize('project,overrides', [
    ({}, {}),
    ({'max_row_bytes': runs.DEFAULT_LIMITS['max_row_bytes']}, {'batch_rows': 128}),
])
def test_managed_budget_declarations_reach_the_native_run(root, project, overrides):
    from demiflow.forge.platform import content_digest
    ws.set_budgets(root, project, expected=content_digest(ws.read_control(root / 'workspace.json')))
    prepared = native_flow(root, limits=overrides)
    expected = sorted(project.keys() | overrides.keys())
    _, record = runs._load(root, prepared['run_id'])
    assert record['manifest']['limits_explicit'] == expected
    result = execute(root, prepared)
    assert result['status'] == 'succeeded', (result, runs.logs(root, result['run_id']))
    identity = runs.journal(root, result['run_id']).recovery_identity()
    assert identity['limits_explicit'] == expected
    assert all(identity['limits'][name] == value for name, value in record['manifest']['limits'].items())


def test_nested_package_preview_and_managed_run_use_same_sources(root):
    package = root.parent.name + '.' + root.name
    (root.parent / '__init__.py').write_text('raise AssertionError("must not import the parent")\n')
    (root / 'operators/local_input.py').write_text('value = 41\n' + '#' + 'x' * (4 * 1024**2) + '\n')
    prepared = native_flow(root, f'from {package}.operators.local_input import value\n', 'value')
    run_id = prepared['run_id']
    from demiflow.workbench.graph.preview import preview
    result = preview(root, entry='flows/native.py:run', params='flows/native.json')
    assert 'operators/local_input.py' in result['basis']['sources']
    target, record = runs._load(root, run_id)
    report = runs.verify_seal(target, record['manifest'])
    assert report['binding']['source_package'] == package
    assert report['binding']['sources']['operators/local_input.py'] == result['basis']['sources']['operators/local_input.py']
    assert (target / 'source').joinpath(*package.split('.'), 'operators/local_input.py').is_file()
    result = execute(root, prepared)
    assert result['status'] == 'succeeded', (result, runs.logs(root, run_id))
    import lance
    for output in result['outputs']:
        assert sorted(lance.dataset(output['uri']).to_table()['x'].to_pylist()) == [2, 41]


def test_shared_source_preview_and_run_pin_explicit_modules(root, monkeypatch):
    from demiflow.workbench.graph.preview import preview
    from demiflow.forge.runtime import ENVIRONMENT, FORMAT

    namespace = root.parent.name
    project_package = namespace + '.' + root.name
    external = root.parent / namespace
    external.mkdir()
    (root.parent / '__init__.py').write_text('raise AssertionError("undeclared initializer")\n')
    (external / '__init__.py').write_text('label = "approved"\n')
    (root / 'operators/local_input.py').write_text('value = 1\n')
    (external / 'step.py').write_text('def adjust(row):\n'
        f'    from {namespace}.metrics.settings import increment\n'
        '    return {"x": row["x"] + increment}\n')
    setting = external / 'settings.py'
    setting.write_text('increment = 20\n' + '#' + 'x' * (8 * 1024**2) + '\n')
    policy = root.parent / 'python-sources.json'
    policy.write_text(json.dumps(dict(format=FORMAT, workspaces={str(root): {
        namespace: str(external / '__init__.py'),
        namespace + '.metrics.step': str(external / 'step.py'),
        namespace + '.metrics.settings': str(setting),
    }})))
    monkeypatch.setenv(ENVIRONMENT, str(policy))
    prepared = native_flow(root, f'from {namespace}.metrics.step import adjust\n'
        f'from {project_package}.operators.local_input import value\n', 'value', transform='.map(adjust)')
    run_id = prepared['run_id']
    plan = preview(root, entry='flows/native.py:run', params='flows/native.json')
    binding = prepared['report']['binding']['shared_sources']
    assert plan['basis']['shared_sources'] == {name: row['sha256'] for name, row in binding.items()}
    assert any(row['implementation']['path'] == f'source/{namespace}/metrics/step.py'
               for row in plan['_node_details'].values())
    snapshot = runs.directory(root, run_id) / 'source' / namespace
    assert (snapshot / '__init__.py').read_bytes() == (external / '__init__.py').read_bytes()
    assert sorted(path.name for path in (snapshot / 'metrics').iterdir()) == ['settings.py', 'step.py']
    assert (snapshot / root.name / 'operators/local_input.py').is_file()
    setting.write_text('increment = 40\n')
    with pytest.raises(Exception, match='prepared shared source changed: ' + namespace + r'\.metrics\.settings'):
        runs.start(root, run_id, expected=prepared['digest'])
    setting.write_text('increment = 20\n' + '#' + 'x' * (8 * 1024**2) + '\n')
    monkeypatch.delenv(ENVIRONMENT)
    result = execute(root, prepared)
    assert result['status'] == 'succeeded', (result, runs.logs(root, run_id))
    import lance
    for output in result['outputs']:
        assert sorted(lance.dataset(output['uri']).to_table()['x'].to_pylist()) == [21, 22]


def test_shared_source_missing_grant_does_not_search_other_files(root, monkeypatch):
    from demiflow.forge.schema import ForgeError
    from demiflow.workbench.graph.preview import preview
    from demiflow.forge.runtime import ENVIRONMENT, FORMAT

    external = root.parent / 'shared'
    external.mkdir()
    (external / 'allowed.py').write_text('from shared.private import value\n')
    (external / 'private.py').write_text('value = 20\n')
    policy = root.parent / 'python-sources.json'
    policy.write_text(json.dumps(dict(format=FORMAT, workspaces={str(root): {
        'shared.allowed': str(external / 'allowed.py'),
    }})))
    monkeypatch.setenv(ENVIRONMENT, str(policy))
    with pytest.raises(ForgeError, match=r'管道依赖无法导入：shared\.private'):
        native_flow(root, 'from shared.allowed import value\n')
    with pytest.raises(ForgeError, match=r'管道依赖无法导入：shared\.private'):
        preview(root, entry='flows/native.py:run', params='flows/native.json')


def test_native_source_drift_rejected_before_worker(root):
    value = native_flow(root)
    with (root / 'flows/native.py').open('a') as stream:
        stream.write('\n# modified after preparation\n')
    with pytest.raises(Exception, match='changed'):
        runs.start(root, value['run_id'], expected=value['digest'])
    assert not (root / 'native-output.lance').exists()


@pytest.mark.parametrize('vendored', [False, True], ids=['legacy', 'vendored'])
def test_native_stop_resume_keeps_original_run_and_stage_receipts(root, tmp_path, vendored):
    extra = ''
    prepare_python = None
    if vendored:
        import sys
        from demiflow.forge.dependencies import vendor

        seed = tmp_path / 'vendor-seed'
        (seed / 'nested').mkdir(parents=True)
        (seed / '__init__.py').write_text('')
        (seed / 'nested/__init__.py').write_text('')
        (seed / 'nested/settings.py').write_text('STEP = 0\n')
        (root / 'pyproject.toml').write_text('[tool.demiflow.packages]\n'
            'native_dep_recovery = {source = "vendored", path = "packages/native_dep_recovery"}\n')
        vendor(root, 'native_dep_recovery', seed)
        prepare_python = sys.executable
        extra = '    from native_dep_recovery.nested.settings import STEP\n    return {"x": row["x"] + STEP}\n'
    (root / 'flows/native_stop.py').write_text('''from demiflow import data
import pyarrow as pa
import time
def slow(row):
    time.sleep(2)
''' + (extra or '    return row\n') + '''def run(*, directory):
    schema = pa.schema([('x', pa.int64())])
    rows = data.from_items([{'x': i} for i in range(8)])
    rows = rows.save_lance(directory+'/stage.lance', schema=schema, key='x', stage='rows',
        mode='append', max_rows=8, max_batch=1, max_index_bytes=8388608)
    return rows.map(slow).write_lance(directory+'/final.lance', schema=schema, mode='create', return_receipt=True)
''')
    params = dict(format='demiflow_forge_flow_parameters_v1', arguments={'directory': str(root / 'stop-output')})
    (root / 'flows/native_stop.json').write_text(json.dumps(params))
    declare_existing_flow(root, 'flows/native_stop.py:run', params='flows/native_stop.json')
    prepared = runs.prepare(root, 'flows/native_stop.py:run', params='flows/native_stop.json',
                            limits={'cancel_grace_s': 1, 'timeout_s': 180}, python=prepare_python)
    identity = prepared['run_id']
    runs.start(root, identity, expected=prepared['digest'], expected_attempt=1)
    try:
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            current = runs.show(root, identity)
            if any(o['result'] for o in current['outputs']):
                break
            assert current['status'] not in runs.TERMINAL, (current, runs.logs(root, identity))
            time.sleep(.1)
        assert any(o['result'] for o in current['outputs'])
        runs.cancel(root, identity, expected_attempt=1)
        before = stopped(root, identity)
        assert before['status'] == 'cancelled', before
        committed = next(output for output in before['outputs'] if output['result'])
        old_selector = 'run:' + identity + ':' + committed['name'] + ':1'
        latest_selector = 'run:' + identity + ':' + committed['name']
        old_binding = project_ops.describe_table(root, latest_selector)['binding']['digest']
        from demiflow.workbench.runs.view import snapshot
        assert snapshot(root, identity)['availability'] == 'available'
        if vendored:
            path = root / 'packages/native_dep_recovery/nested/settings.py'
            path.write_text('STEP = 100\n')
        runs.resume(root, identity, expected=prepared['digest'], expected_attempt=1)
        after = wait(root, identity)
        assert after['status'] == 'succeeded', (after, runs.logs(root, identity))
        assert after['attempt'] == 2 and runs.list_runs(root)['total'] == 1
        assert runs.show(root, identity, attempt=1) == before
        assert all(o['result']['ref']['row_count'] == 8 for o in after['outputs'])
        assert project_ops.describe_table(root, old_selector)['reference']['ref'] == committed['result']['ref']
        latest = project_ops.describe_table(root, latest_selector)
        explicit = project_ops.describe_table(root, latest_selector + ':2')
        assert latest['reference']['ref'] == explicit['reference']['ref']
        if latest['reference']['ref'] != committed['result']['ref']:
            assert latest['binding']['digest'] != old_binding
            with pytest.raises(ValueError, match='binding'):
                project_ops.describe_table(root, latest_selector, expected_binding=old_binding)
        else:
            # Existing preview identity is the exact table, not an added
            # attempt generation. Reusing the same fixed ref stays admissible.
            assert project_ops.describe_table(root, latest_selector, expected_binding=old_binding)['reference']['ref'] == committed['result']['ref']
        import lance
        for output in after['outputs']:
            table = lance.dataset(output['uri'], version=output['result']['ref']['lance_version']).to_table()
            assert sorted(table.column('x').to_pylist()) == list(range(8))
    finally:
        state = runs.show(root, identity)
        if state['status'] in runs.ACTIVE:
            runs.cancel(root, identity, expected_attempt=state['attempt'])
            stopped(root, identity)


def test_project_dependency_preview_and_native_run_use_the_locked_package(root, tmp_path):
    import site
    import sys
    import venv
    from demiflow.forge.dependencies import binding
    from demiflow.workbench.graph.preview import preview

    venv_path = tmp_path / 'project-python'
    venv.EnvBuilder(with_pip=False, symlinks=True, system_site_packages=True).create(venv_path)
    python = venv_path / 'bin/python'
    installed = venv_path / 'lib' / f'python{sys.version_info.major}.{sys.version_info.minor}' / 'site-packages'
    (installed / 'platform-test.pth').write_text('\n'.join(site.getsitepackages()) + '\n')
    (installed / 'project_dependency_fixture.py').write_text('from pathlib import Path\nimport sys\n'
        f'assert Path(sys.prefix) == Path({str(venv_path)!r})\nVALUE = 1\n')
    metadata = installed / 'project_dependency_fixture-1.0.dist-info'
    metadata.mkdir()
    (metadata / 'METADATA').write_text('Metadata-Version: 2.1\nName: project-dependency-fixture\nVersion: 1.0\n')

    package = tmp_path / 'approved'
    package.mkdir()
    (package / '__init__.py').write_text('VALUE = 3\n')
    (package / 'step.py').write_text('def adjust(row):\n'
        '    from native_dep_fixture.late import OFFSET\n'
        '    return {"x": row["x"] + OFFSET}\n')
    (package / 'late.py').write_text('OFFSET = 20\n')
    (root / 'pyproject.toml').write_text('[project]\ndependencies = ["project-dependency-fixture==1.0"]\n'
        '[tool.demiflow.packages]\n'
        f'native_dep_fixture = {{source = "local_ref", path = "{package}"}}\n')
    bound = binding(root, python=python, require_python=True, require_environment=True)
    prepared = native_flow(root, 'from native_dep_fixture.step import adjust\n'
        'from project_dependency_fixture import VALUE\n', 'VALUE', transform='.map(adjust)', python=python)
    result = preview(root, entry='flows/native.py:run', params='flows/native.json', python=python)
    assert result['basis']['project_dependencies'] == bound['digest']
    target, value = runs._load(root, prepared['run_id'])
    assert value['manifest']['project_dependencies']['digest'] == bound['digest']
    assert (target / 'source/native_dep_fixture/late.py').read_text() == 'OFFSET = 20\n'
    late = package / 'late.py'
    late.write_text('OFFSET = 40\n')
    with pytest.raises(Exception, match='changed|differs'):
        runs.start(root, prepared['run_id'], expected=prepared['digest'], python=python)
    late.write_text('OFFSET = 20\n')
    outcome = execute(root, prepared)
    assert outcome['status'] == 'succeeded', (outcome, runs.logs(root, outcome['run_id']))
    import lance
    assert all(sorted(lance.dataset(row['uri']).to_table()['x'].to_pylist()) == [21, 22]
               for row in outcome['outputs'])

    outcome = stopped(root, outcome['run_id'])
    assert outcome['state']['cleanup'] == 'complete'
    upstream = outcome['outputs'][0]
    selector = f"run:{outcome['run_id']}:{upstream['name']}:1"
    (root / 'flows/continued.py').write_text('''from demiflow import data
import pyarrow as pa

def run(*, source, target):
    schema = pa.schema([('x', pa.int64())])
    return data.read_lance(source['uri'], version=source['version']).write_lance(
        target, schema=schema, mode='create', return_receipt=True)
''')
    params = 'flows/continued.json'
    (root / params).write_text(json.dumps(dict(format='demiflow_forge_flow_parameters_v1', arguments={
        'source': {'kind': 'table', 'alias': selector}, 'target': str(root / 'continued.lance'),
    })))
    entry = 'flows/continued.py:run'
    declare_existing_flow(root, entry, params=params)
    assert preview(root, entry=entry, params=params, python=python)['basis']['project_dependencies'] == bound['digest']
    continued = runs.prepare(root, entry, params=params, python=python)
    assert continued['report']['binding']['tables'][selector]['version'] == upstream['result']['ref']['lance_version']
    followed = execute(root, continued)
    assert followed['status'] == 'succeeded', (followed, runs.logs(root, followed['run_id']))
    assert sorted(lance.dataset(followed['outputs'][0]['uri']).to_table()['x'].to_pylist()) == [21, 22]


def test_project_dependency_bootstrap_reaches_real_local_stage_process(tmp_path, monkeypatch):
    import hashlib
    import os
    import sys
    from demiflow import data
    from demiflow.forge.runtime import import_bootstrap, install_bound_imports

    platform_root = Path(__file__).resolve().parents[1]
    package = tmp_path / 'run/source/stage_dependency_fixture'
    package.mkdir(parents=True)
    modules = {}
    for name, filename, raw in [
        ('stage_dependency_fixture', '__init__.py', b''),
        ('stage_dependency_fixture.callback', 'callback.py',
         b'def enrich(row):\n    import os\n    from stage_dependency_fixture.later import OFFSET\n'
         b'    return {"x": row["x"] + OFFSET, "pid": os.getpid()}\n'),
        ('stage_dependency_fixture.later', 'later.py', b'OFFSET = 7\n'),
    ]:
        path = package / filename
        path.write_bytes(raw)
        modules[name] = dict(path=str(path), sha256=hashlib.sha256(raw).hexdigest(), bytes=len(raw))
    monkeypatch.setattr(sys, 'meta_path', list(sys.meta_path))
    install_bound_imports(modules)
    bootstrap = import_bootstrap(tmp_path / 'run/work/imports', modules, platform_root=platform_root)
    monkeypatch.setenv('PYTHONPATH', os.pathsep.join((str(bootstrap), str(platform_root))))
    try:
        from stage_dependency_fixture.callback import enrich
        assert 'stage_dependency_fixture.later' not in sys.modules
        with data.local_execution(worker_mode='process', workers=1, partitions=1):
            rows = data.from_items([{'x': 3}]).map(enrich).take_all()
        assert rows[0]['x'] == 10 and rows[0]['pid'] != os.getpid()
    finally:
        for name in modules:
            sys.modules.pop(name, None)
