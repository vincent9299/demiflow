"""UI-D1.9/2 independent integration acceptance fixtures (V10 owned_fixture, V11 OwnedFixture).

Private, tree-bound test domain only. Every service binds port=0 on 127.0.0.1,
every byte lives under a fresh /tmp root, and the fixture tears down only what it
started. No main-repo service, production data, paid model or shared run service
is touched.

Two real input classes are provided for the C8 gates (V03/V04/V08/V09):
  * legacy  -- a real prepare/start run sealed by the candidate tree's current
               executor, read back through the genuine graph/facts chain;
  * v3      -- the same real run, accepted only when the candidate actually seals
               a ``demiflow.dataflow_graph.v3`` action. When the unified Runtime /
               platform W is not integrated the v3 parameter is reported not_run
               (pytest.skip), never satisfied with a consumer mock.

Explicit doubles, declared so they can never be counted as runtime admission:
  * the Case/metadata world (``case_world=True``) reuses the existing private
    Records/native-media driver from legacy_case_fixture.
    Its ceiling is ``analysis_execute=False`` and ``execution_profile() is None``;
    it proves source/Case/media binding, not kernel execution.
  * ``need_runtime=True`` instead admits the real analysis helper named by
    WORKBENCH_D19_ANALYSIS_CONFIG. Without that approved private candidate config
    the gate is not_run; a metadata double never substitutes for it (V05).
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import threading
import time

import pytest

TREE = Path(__file__).resolve().parents[1]
WEB = TREE / 'demiflow/workbench/web'
V3_SCHEMA = 'demiflow.dataflow_graph.v3'
LEGACY_SCHEMA = 'demiflow.logical_dag.v1'


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def assert_tree_imports() -> dict:
    """Fail loudly if any loaded implementation byte comes from another tree."""
    import demiflow
    import demiflow.workbench.api as api
    import demiflow.workbench.graph.model as graph_model
    import demiflow.workbench.runs.facts as facts
    roots = {}
    for module in (demiflow, api, graph_model, facts):
        origin = Path(module.__file__).resolve()
        assert origin.is_relative_to(TREE), f'{module.__name__} imported outside candidate tree: {origin}'
        roots[module.__name__] = str(origin)
    return roots


def source_head() -> str:
    return subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=TREE, capture_output=True,
                          text=True, check=True).stdout.strip()


def source_digests() -> dict:
    """Bind the exact implementation bytes under acceptance (design targets included)."""
    names = [
        'demiflow/workbench/api.py',
        'demiflow/workbench/graph/model.py',
        'demiflow/workbench/runs/facts.py',
        'demiflow/workbench/worker.py',
        'demiflow/workbench/projects/workspace.py',
        'demiflow/workbench/analysis/web_sources.py',
        'demiflow/workbench/analysis/source_export.py',
        'demiflow/workbench/analysis/media_source_export.py',
        'demiflow/workbench/analysis/catalog_versions.py',
        'demiflow/workbench/web/index.html',
        'demiflow/workbench/web/notebooks.js',
        'demiflow/workbench/web/app.js',
        'demiflow/workbench/web/view.js',
        'demiflow/workbench/web/workbench.js',
        'demiflow/workbench/web/workbench-state.js',
        'demiflow/workbench/web/workbench-graph.js',
        'demiflow/workbench/web/workbench-presentation.js',
    ]
    digests = {}
    for name in names:
        path = TREE / name
        digests[name] = _sha256(path) if path.is_file() else None
    return digests


@dataclass
class OwnedFixture:
    """V11 test-domain object. Only V10 owns its lifecycle; never used by product code."""
    root: Path
    project_id: str
    app: object
    evidence_dir: Path
    source_head: str
    source_digests: dict
    analysis_profile: object
    # Plumbing attached by owned_fixture after construction (not part of the
    # frozen seven-field identity); helpers below read these.
    context: dict = field(default_factory=dict, repr=False)

    # -- convenience accessors -------------------------------------------------
    @property
    def tmp_path(self) -> Path:
        return self.context['tmp_path']

    @property
    def monkeypatch(self):
        return self.context['monkeypatch']

    @property
    def registry(self) -> Path:
        return self.context['registry']

    @property
    def execution_format(self) -> str:
        return self.context['execution_format']

    @property
    def version_facts(self) -> dict:
        return self.context['version_facts']

    @property
    def run_facts(self) -> dict:
        return self.context['run_facts']

    @property
    def sealed_actions(self) -> list:
        return self.context['sealed_actions']

    @property
    def v3_available(self) -> bool:
        return self.context['v3_available']

    @property
    def case(self):
        return self.context.get('case')

    @property
    def import_roots(self) -> dict:
        return self.context['import_roots']

    # -- helpers ---------------------------------------------------------------
    def shot(self, page, name: str) -> Path:
        path = self.evidence_dir / 'screenshots' / (name + '.png')
        path.parent.mkdir(parents=True, exist_ok=True)
        page.screenshot(path=str(path), full_page=True)
        return path

    def record(self, name: str, value) -> Path:
        path = self.evidence_dir / (name + '.json')
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str) + '\n')
        return path

    def api(self, endpoint: str, body=None):
        from test_forge_public_api import request
        assert endpoint.startswith('/api/'), endpoint
        path = '/api/workspaces/' + self.project_id + endpoint[len('/api'):]
        return request(self.app, path, body)

    def grant(self, **changes):
        from demiflow.workbench.projects import registry as console
        from demiflow.forge.platform import content_digest
        value = console.load_registry(self.registry)
        return console.set_capabilities(
            self.registry, self.project_id, expected=content_digest(value),
            capabilities={**console.capabilities(value['projects'][self.project_id]), **changes},
            analysis_service=self.app.analysis)

    @contextmanager
    def open_page(self, browser, *, viewport=(1440, 1050), hash_token=True):
        """Real browser page bound to this private service, with API fact capture."""
        from playwright.sync_api import expect
        context = browser.new_context(viewport={'width': viewport[0], 'height': viewport[1]})
        page = context.new_page()
        page.set_default_timeout(65000)
        expect.set_options(timeout=65000)
        errors = []
        page.on('pageerror', lambda error: errors.append(str(error)))
        requests = []
        responses = []

        def on_request(request):
            if '/api/' in request.url:
                requests.append(dict(method=request.method, url=request.url.split('#')[0],
                                     body=request.post_data))

        def on_response(response):
            if '/api/' in response.url and len(responses) < 2048:
                responses.append(dict(url=response.url.split('#')[0], status=response.status))

        page.on('request', on_request)
        page.on('response', on_response)
        log = dict(requests=requests, responses=responses, errors=errors)
        try:
            suffix = ('#token=' + self.app.token) if hash_token else ''
            page.goto(self.app.origin + '/' + suffix)
            page.locator('.function-nav').get_by_role('button', name='管道', exact=True).wait_for()
            yield page, log
            assert not errors, errors
        except BaseException:
            self.shot(page, 'failure-' + str(int(time.time())))
            self.record('failure-api-log', log)
            raise
        finally:
            context.close()


def _make_versioned_table(root: Path) -> dict:
    """Two registered Lance versions with genuinely different schema/rows/image.

    v1 and v2 share only the identity/image columns so one display config is valid
    for both; every other column, the row set and the image bytes differ, so a
    pass-through bug that always reads the current version cannot fake green.
    """
    import lance
    import pyarrow as pa
    from PIL import Image, ImageDraw
    from demiflow.workbench.projects import workspace as ws
    from demiflow.workbench.worker import operation
    from demiflow.lance.refs import DatasetRef
    from demiflow.lance.registry import Catalog

    data = root / 'data'
    alias = 'versioned'
    uri = str(data / (alias + '.lance'))

    def image(color: str, label: str) -> bytes:
        picture = Image.new('RGB', (320, 240), '#f4f1ea')
        draw = ImageDraw.Draw(picture)
        draw.rounded_rectangle((40, 40, 280, 200), radius=24, fill=color)
        draw.text((60, 110), label, fill='#ffffff')
        import io
        buffer = io.BytesIO()
        picture.save(buffer, format='PNG')
        return buffer.getvalue()

    image_v1 = image('#2f6f4f', 'version-one')
    image_v2 = image('#7a3b2e', 'version-two')
    (data / 'versioned_v1.png').write_bytes(image_v1)
    (data / 'versioned_v2.png').write_bytes(image_v2)
    sha1 = hashlib.sha256(image_v1).hexdigest()
    sha2 = hashlib.sha256(image_v2).hexdigest()

    schema_v1 = pa.schema([
        pa.field('case_id', pa.string(), nullable=False),
        pa.field('question', pa.string()),
        pa.field('image_uri', pa.string()),
        pa.field('image_sha', pa.string()),
        pa.field('score', pa.float64()),
    ])
    rows_v1 = [
        dict(case_id='v1-c01', question='第一版问题一', image_uri='versioned_v1.png', image_sha=sha1, score=0.5),
        dict(case_id='v1-c02', question='第一版问题二', image_uri='versioned_v1.png', image_sha=sha1, score=0.75),
    ]
    schema_v2 = pa.schema([
        pa.field('case_id', pa.string(), nullable=False),
        pa.field('question', pa.string()),
        pa.field('image_uri', pa.string()),
        pa.field('image_sha', pa.string()),
        pa.field('rating', pa.int64()),
        pa.field('extra', pa.string()),
    ])
    rows_v2 = [
        dict(case_id='v2-c01', question='第二版问题一', image_uri='versioned_v2.png', image_sha=sha2, rating=3, extra='甲'),
        dict(case_id='v2-c02', question='第二版问题二', image_uri='versioned_v2.png', image_sha=sha2, rating=4, extra='乙'),
        dict(case_id='v2-c03', question='第二版问题三', image_uri='versioned_v2.png', image_sha=sha2, rating=5, extra='丙'),
    ]
    pa.set_cpu_count(1)
    pa.set_io_thread_count(1)
    lance.write_dataset(pa.Table.from_pylist(rows_v1, schema=schema_v1), uri)
    lance.write_dataset(pa.Table.from_pylist(rows_v2, schema=schema_v2), uri, mode='overwrite')

    catalog = Catalog(data)
    metas = {}
    for version in (1, 2):
        meta = operation('describe', uri, version=version)
        metas[version] = meta
        catalog.register(DatasetRef(alias, alias + '.lance', version, alias, str(version),
                                    meta['schema_hash'], meta['row_count'], store_id='local'))
    attached = ws.attach_table(root, alias, relative_uri=alias + '.lance', version=2,
                               dataset_id=alias, schema_name=alias, schema_version='2')
    display = dict(fields=[
        dict(path=['image_uri'], kind='image', location_id='local', sha256_path=['image_sha'],
             max_bytes=1024 * 1024, mime='image/png'),
        dict(path=['case_id'], kind='text'),
        dict(path=['question'], kind='text'),
    ])
    ws.set_table_view(root, alias, expected=attached['digest'], display=display, provenance={})

    from demiflow.workbench.projects.workspace import preview_binding, table_binding
    control, ref, table_uri = table_binding(root, ws.load_workspace(root)[1], alias)

    def version_binding(version: int) -> dict:
        # Mirror catalog_versions.resolve_version: the virtual control replaces the
        # whole ref with the registered version DatasetRef (its real schema_hash /
        # schema_version / row_count), not merely a lance_version swap on the
        # current ref. preview_binding then digests that full version identity.
        registered_ref = catalog.get(alias, relative_uri=alias + '.lance', lance_version=version)
        assert registered_ref is not None, version
        virtual = {**control, 'ref': registered_ref.to_dict()}
        return preview_binding(root, virtual, table_uri)

    binding_v1 = version_binding(1)
    binding_v2 = version_binding(2)
    alias_binding = preview_binding(root, control, table_uri)
    listed = ws.table_versions(root, alias, expected_binding=alias_binding['digest'])
    assert listed['total_registered'] == 2, listed
    assert [item['lance_version'] for item in listed['versions']] == [1, 2]
    assert metas[1]['schema_hash'] != metas[2]['schema_hash']
    assert metas[1]['row_count'] != metas[2]['row_count']
    assert image_v1 != image_v2
    control_path = root / 'tables' / (alias + '.json')
    return dict(
        alias=alias, uri=uri, control_path=str(control_path),
        control_sha256=_sha256(control_path),
        alias_binding=alias_binding['digest'],
        versions={
            1: dict(binding=binding_v1['digest'], schema_hash=metas[1]['schema_hash'],
                    row_count=metas[1]['row_count'], rows=rows_v1,
                    columns=list(schema_v1.names), image='versioned_v1.png',
                    image_sha256=sha1, image_bytes=image_v1),
            2: dict(binding=binding_v2['digest'], schema_hash=metas[2]['schema_hash'],
                    row_count=metas[2]['row_count'], rows=rows_v2,
                    columns=list(schema_v2.names), image='versioned_v2.png',
                    image_sha256=sha2, image_bytes=image_v2),
        },
    )


def _create_real_run(root: Path, producer_tree: Path | None = None) -> dict:
    """A genuine Local prepare/start run through the candidate's own manager."""
    if producer_tree is not None:
        script = """
import json, pathlib, sys, time, subprocess
import demiflow
from demiflow.workbench.runs import manager as runs
from demiflow.workbench.projects import workspace as ws
tree, root = map(pathlib.Path, sys.argv[1:])
assert pathlib.Path(demiflow.__file__).resolve().is_relative_to(tree)
# The historical producer must inspect against its own platform snapshot.
# This changes only the private workspace's current binding, never old reports.
ws.bind_platform(root, expected=ws.load_workspace(root)[1]['platform']['digest'])
prepared = runs.prepare(root, 'flows/questions.py:select', params='flows/select_a.json')
runs.start(root, prepared['run_id'], expected=prepared['digest'])
deadline = time.monotonic() + 120
while True:
    run = runs.show(root, prepared['run_id'])
    if run['status'] in runs.TERMINAL: break
    assert time.monotonic() < deadline, run
    time.sleep(.2)
assert run['status'] == 'succeeded', run
print(json.dumps(dict(run_id=prepared['run_id'], attempt=run['attempt'], status=run['status'],
    digest=prepared['digest'], outputs=run.get('outputs', []), producer_tree=str(tree),
    producer_import=str(pathlib.Path(demiflow.__file__).resolve()),
    producer_head=subprocess.check_output(['git', '-C', str(tree), 'rev-parse', 'HEAD'], text=True).strip())))
"""
        result = subprocess.run([sys.executable, '-B', '-c', script, str(producer_tree), str(root)],
            cwd=root, env={**os.environ, 'PYTHONPATH': str(producer_tree), 'PYTHONDONTWRITEBYTECODE': '1'},
            capture_output=True, text=True, timeout=150)
        assert result.returncode == 0, result.stdout + result.stderr
        facts = json.loads(result.stdout.splitlines()[-1])
        from demiflow.workbench.projects import workspace as ws
        # Explicitly upgrade the same workspace after producing the old run.
        # Its sealed artifacts remain untouched; all subsequent runs use this tree.
        ws.bind_platform(root, expected=ws.load_workspace(root)[1]['platform']['digest'])
        for variant in ('a', 'b'):
            report = ws.inspect_flow(root, 'flows/questions.py:select',
                                     params=f'flows/select_{variant}.json')
            assert report['status'] == 'checked', report
        return facts
    from demiflow.workbench.runs import manager as runs
    prepared = runs.prepare(root, 'flows/questions.py:select', params='flows/select_a.json')
    runs.start(root, prepared['run_id'], expected=prepared['digest'])
    deadline = time.monotonic() + 120
    run = None
    while time.monotonic() < deadline:
        run = runs.show(root, prepared['run_id'])
        if run['status'] in runs.TERMINAL:
            break
        time.sleep(0.2)
    assert run is not None and run['status'] == 'succeeded', run
    return dict(run_id=prepared['run_id'], attempt=run['attempt'], status=run['status'],
                digest=prepared['digest'], outputs=run.get('outputs', []))


def _sealed_actions(root: Path, run_facts: dict) -> list:
    from demiflow.workbench.graph import model
    graph = model.get(root, basis_kind='run', run_id=run_facts['run_id'], attempt=run_facts['attempt'])
    logical = graph.get('logical') or {}
    return [action for action in logical.get('actions', []) if action.get('status') == 'sealed']


def _is_v3(action: dict) -> bool:
    dag = action.get('dag') or {}
    return action.get('version') == 'v3' or dag.get('schema_version') == V3_SCHEMA


@pytest.fixture
def owned_fixture(tmp_path, monkeypatch, request):
    """V10: tree-bound private domain, port=0 service, real pipeline + version fixtures.

    Indirect parameters (dict):
      execution_format: 'legacy' (default) | 'v3'  -- C8 real input class
      case_world:       bool -- real Records/native-media Case double (no runtime)
      need_runtime:     bool -- admit the real WORKBENCH_D19_ANALYSIS_CONFIG helper
    """
    params = dict(getattr(request, 'param', None) or {})
    execution_format = params.get('execution_format', 'auto')
    case_world = bool(params.get('case_world', False))
    need_runtime = bool(params.get('need_runtime', False))
    assert execution_format in ('auto', 'legacy', 'v3'), execution_format

    import_roots = assert_tree_imports()
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN', str(tmp_path / 'runtime-domain'))
    monkeypatch.setenv('PYTHONDONTWRITEBYTECODE', '1')
    evidence = Path(os.environ.get('FORGE_WORKBENCH_EVIDENCE', str(tmp_path / 'evidence')))
    evidence.mkdir(parents=True, exist_ok=True)

    # Real synthetic pipeline workspace (7-node flow, Lance tables, registered flows).
    spec = importlib.util.spec_from_file_location('d19_demo_fixture', TREE / 'examples/forge_workspace/make_demo.py')
    demo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(demo)
    root = tmp_path / 'workspace'
    demo.create(root)

    version_facts = _make_versioned_table(root)

    from demiflow.workbench.projects import registry as console
    from demiflow.workbench.projects import workspace as ws
    from demiflow.forge.platform import content_digest
    registry = tmp_path / 'registry.json'
    project_id = console.add(registry, root, allow_runs=True)['workspace_id']
    console.migrate(registry, expected=content_digest(console.load_registry(registry)))
    assert project_id == ws.workspace_id(root, ws.load_workspace(root)[1])

    # Real Local run sealed by the candidate tree; the v3 class is accepted only
    # when this genuine run actually produces a v3 action (no consumer mock).
    legacy_path = os.environ.get('WORKBENCH_D19_LEGACY_TREE') if execution_format == 'legacy' else None
    producer_tree = Path(legacy_path).resolve() if legacy_path else None
    if producer_tree:
        assert producer_tree != TREE and (producer_tree / 'demiflow').is_dir()
    # Notebook execution is an independent application; its fixed input tables
    # do not need a Pipeline run. Keep that unrelated engine out of V05 admission.
    run_facts = {} if need_runtime else _create_real_run(root, producer_tree)
    sealed_actions = [] if need_runtime else _sealed_actions(root, run_facts)
    v3_available = any(_is_v3(action) for action in sealed_actions)
    if execution_format == 'v3' and not v3_available:
        pytest.skip('not_run: candidate tree seals no demiflow.dataflow_graph.v3 action; '
                    'unified Runtime / platform W not integrated (U1-U3 required_not_verified)')
    if execution_format == 'legacy' and not any(not _is_v3(action) for action in sealed_actions):
        pytest.skip('not_run: legacy gate needs WORKBENCH_D19_LEGACY_TREE with a real legacy producer; '
                    'the current producer seals v3')
    if execution_format == 'auto':
        execution_format = 'analysis' if need_runtime else ('v3' if v3_available else 'legacy')

    analysis_config = None
    analysis_profile = None
    case = None
    driver = None
    lock = threading.Lock()
    helper_calls = []
    helper_responses = []

    if need_runtime:
        config_path = os.environ.get('WORKBENCH_D19_ANALYSIS_CONFIG')
        if not config_path and os.environ.get('WORKBENCH_D19_PRIVATE_RUNTIME') == '1':
            from d19_runtime import private_runtime
            runtime = private_runtime(TREE, root, evidence)
            config_path = runtime.__enter__()
            request.addfinalizer(lambda: runtime.__exit__(None, None, None))
        if not config_path or not Path(config_path).is_file():
            pytest.skip('not_run: WORKBENCH_D19_ANALYSIS_CONFIG must name an approved private '
                        'candidate analysis config; no real kernel admitted (V05)')
        analysis_config = config_path
    elif case_world:
        case, driver, analysis_config = _build_case_world(
            root, tmp_path, project_id, monkeypatch, lock, helper_calls, helper_responses)

    from test_forge_public_api import service
    with service(None, registry=registry, enable_runs=True,
                 enable_config_write=True, analysis_config=analysis_config) as app:
        from demiflow.workbench.projects import registry as console2
        value = console2.load_registry(registry)
        caps = {**console2.capabilities(value['projects'][project_id]), 'config_write': True}
        if app.analysis is not None:
            caps.update(analysis_write=True, case_annotate=True)
        if need_runtime:
            caps['analysis_execute'] = True
        console2.set_capabilities(registry, project_id, expected=content_digest(value),
                                  capabilities=caps, analysis_service=app.analysis)
        if need_runtime:
            profile_reader = getattr(app.analysis, 'execution_profile', None)
            analysis_profile = profile_reader() if callable(profile_reader) else None
            assert analysis_profile, 'admitted runtime must expose a real execution profile'
            assert app.analysis.ceiling().get('analysis_execute') is True, \
                'WORKBENCH_D19_ANALYSIS_CONFIG did not admit real execution'

        fixture = OwnedFixture(
            root=root, project_id=project_id, app=app, evidence_dir=evidence,
            source_head=source_head(), source_digests=source_digests(),
            analysis_profile=analysis_profile)
        fixture.context.update(
            tmp_path=tmp_path, monkeypatch=monkeypatch, registry=registry,
            execution_format=execution_format, version_facts=version_facts,
            run_facts=run_facts, sealed_actions=sealed_actions, v3_available=v3_available,
            case=case, import_roots=import_roots,
            helper_calls=helper_calls, helper_responses=helper_responses)
        fixture.record('source-binding', dict(
            source_tree=str(TREE), source_head=fixture.source_head,
            import_roots=import_roots, source_digests=fixture.source_digests,
            execution_format=execution_format, v3_available=v3_available,
            run=run_facts, analysis_profile=analysis_profile,
            doubles=('case metadata driver: analysis_execute=False, execution_profile=None'
                     if case_world else ('real admitted analysis helper' if need_runtime else 'none'))))
        try:
            yield fixture
        finally:
            if need_runtime:
                # Complete the real cleanup protocol while this Web authority
                # is alive, including assertion failures inside the browser.
                documents = app.analysis.call(project_id, 'documents', dict(kind='notebook', limit=32))
                for document in documents['items']:
                    sessions = app.analysis.call(project_id, 'notebook-sessions', dict(notebook_id=document['id']))
                    for session in sessions['items']:
                        if session['state'] == 'stopped':
                            continue
                        app.analysis.stop_generation(project_id, request_id='d19-cleanup-' + session['id'],
                                                     session_id=session['id'], generation=session['generation'])
                        deadline = time.monotonic() + 30
                        while True:
                            status = app.analysis.call(project_id, 'session', dict(session_id=session['id']))
                            if status['state'] == 'stopped':
                                assert status.get('cleanup_complete') is True, status
                                break
                            assert time.monotonic() < deadline, status
                            time.sleep(.1)
            if driver is not None:
                driver.__exit__(None, None, None)




def _build_case_world(root, tmp_path, project_id, monkeypatch, lock, calls, responses):
    """Real Records/native-media Case v2 world on the shared workspace (explicit double).

    Mirrors legacy_case_fixture.scene: real Lance rows, real
    PNG media, real SQLite Records via the private driver; runtime/watchdog/Task
    scheduling remain explicit doubles and analysis_execute stays False.
    """
    import lance
    import pyarrow as pa
    from demiflow.workbench.projects import workspace as ws
    from demiflow.workbench.analysis import web_sources, web_service
    from demiflow.workbench.analysis.output_transport import OutputRange
    from demiflow.forge.schema import ForgeError
    from demiflow.forge.platform import content_digest
    from demiflow.workbench.projects import registry as console
    from legacy_case_fixture import rpc, cup

    rows = []
    for i in range(3):
        title = ['红色陶瓷杯', '蓝色波点杯', '青绿色杯'][i % 3]
        row = dict(case_title=title, category='杯具', prompt='检查' + title + '的颜色、把手与材质。',
                   reference='### 共同参考\n\n目标与参考图保持一致。', spare_answer='补充复核材料 ' + str(i))
        for name, color in [('reference', '#ba3b3b'), ('base', '#3c6fa4'), ('next', '#b9413d')]:
            data = cup(color, i % 2)
            filename = name + str(i) + '.png'
            (root / 'data' / filename).write_bytes(data)
            row[name + '_image'] = filename
            row[name + '_sha'] = hashlib.sha256(data).hexdigest()
        row.update(base_answer='**Base 回答**\n\n颜色偏蓝，杯形完整。', base_score=62. + i,
                   next_answer=None if i == 2 else '**Checkpoint 回答**\n\n颜色接近目标，把手较完整。',
                   next_score=91. + i)
        rows.append(row)
    pa.set_cpu_count(1)
    pa.set_io_thread_count(1)
    for alias, data in [('cases', rows), ('other', rows[:1])]:
        lance.write_dataset(pa.Table.from_pylist(data), str(root / 'data' / (alias + '.lance')))
        attached = ws.attach_table(root, alias, relative_uri=alias + '.lance', version=1,
                                   dataset_id=alias, schema_name='group-demo', schema_version='1')
        ws.set_table_view(root, alias, expected=attached['digest'], display=dict(fields=[
            dict(path=[name + '_image'], kind='image', location_id='local', sha256_path=[name + '_sha'],
                 max_bytes=1024 * 1024, mime='image/png') for name in ['reference', 'base', 'next']] + [
            dict(path=[name], kind='json' if name.endswith('_score') else 'text')
            for name in ['case_title', 'category', 'prompt', 'reference', 'base_answer', 'base_score',
                         'next_answer', 'next_score', 'spare_answer']]), provenance={})

    fields = [dict(id=name, field=name, label=label, role=role, format=fmt) for name, label, role, fmt in [
        ('case_title', 'Case 名称', 'context', 'text'), ('category', '类别', 'context', 'text'),
        ('prompt', '任务说明', 'input', 'text'), ('reference', '参考说明', 'reference', 'md'),
        ('reference_image', '参考图片', 'reference', 'image'),
        ('base_image', 'Base 图片', 'output', 'image'), ('base_answer', 'Base 回答', 'output', 'md'),
        ('base_score', 'Base 模型分', 'evaluation', 'json'),
        ('next_image', 'Checkpoint 图片', 'output', 'image'), ('next_answer', 'Checkpoint 回答', 'output', 'md'),
        ('next_score', 'Checkpoint 模型分', 'evaluation', 'json'), ('spare_answer', '补充回答', 'output', 'text')]]
    info = ws.describe_table(root, 'cases')
    locator = dict(kind='workspace_table', alias='cases', binding=info['binding']['digest'], columns=list(rows[0]))
    source = web_sources.resolve(root, locator)['source']
    outbox = tmp_path / 'outbox'
    outbox.mkdir()
    outbox.chmod(0o2770)
    config = tmp_path / 'case-fixture.json'
    config.write_text(json.dumps(dict(workspace=str(root), outbox=str(outbox), state=str(tmp_path / 'case-state'),
                                      project=project_id, locator=locator, source=source)))
    questions = [
        dict(id='tags', title='标签', type='multi', required=False,
             options=[dict(id='favorite', label='收藏'), dict(id='review', label='待复核')]),
        dict(id='verdict', title='结论', type='choice', required=False,
             options=[dict(id='yes', label='通过'), dict(id='review', label='待复核')]),
        dict(id='score', title='人工评分', type='rating', required=False, minimum=1, maximum=5),
        dict(id='note', title='备注', type='text', required=False)]
    descriptor = dict(version=2, source_snapshot=source['digest'], case_key=source['case_key'], fields=fields,
                      output_groups=[dict(id='base', label='Base', fields=['base_image', 'base_answer', 'base_score']),
                                     dict(id='next', label='Checkpoint', fields=['next_image', 'next_answer', 'next_score'])],
                      grouping=['category'], filters=None, sort=[], layout='comparison', questions=questions)
    draft = dict(id='v' + '9' * 32, view_id=None, revision=0, base=None, view_touched=False, current=None,
                 offset=0, answers=[], review_group='base', descriptor=descriptor)

    driver = rpc(tmp_path, config, lock=lock)
    invoke = driver.__enter__()

    class Helper(web_service.WebService):
        principal = 'd19-fixture-reader'
        features = dict(case_media=True, annotation_media_basis=True, case_output_groups=True, case_review_filter=True)

        def __init__(self, server, config):
            self.server = server

        def ceiling(self):
            return dict(analysis_write=True, case_annotate=True, analysis_execute=False)

        def execution_profile(self):
            return None  # Metadata-only double: no admitted runtime (never counts for V05).

        def assert_ready(self):
            return True

        def close(self):
            pass

        def cancel_output(self, key):
            pass

        def call(self, key, name, payload):
            actual, _ = console.resolve_workspace(self.server.inventory(), key)
            assert actual == root
            permissions = {'read': True, **self.server.effective_permissions(self.server.inventory()['projects'][key])}
            with lock:
                calls.append((name, payload))
                value = invoke(dict(name=name, payload=payload, permissions=permissions))
                responses.append((name, value))
            if 'error' in value:
                raise ForgeError(value['error'], code=value['code'])
            return value['result']

        def read_output(self, key, name, payload, **kwargs):
            value = self.call(key, name, payload)
            if value.get('format') == 'fixture-verified-range':
                return OutputRange(value['artifact'], value['metadata'], bytes.fromhex(value['data']))
            return value

    monkeypatch.setattr(web_service, 'WebService', Helper)
    case = dict(root=root, locator=locator, source=source, descriptor=descriptor, draft=draft,
                questions=questions, calls=calls, responses=responses,
                config=str(config), driver_config=config)
    return case, driver, 'D19 private real Records/native media; Runtime doubles, analysis_execute=False'
