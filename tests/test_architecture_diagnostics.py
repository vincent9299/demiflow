"""Source-object/report contracts; private Git fixtures, no analyzed module imports."""
from dataclasses import replace
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('task_a_architecture_diagnostics', ROOT/'tools/architecture_diagnostics.py')
diag = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = diag
SPEC.loader.exec_module(diag)


class Repository:
    def __init__(self, path):
        self.path = path
        path.mkdir()
        self.git('init', '-q', '-b', 'fixture')

    def git(self, *args):
        return subprocess.check_output(['git', '-c', 'core.hooksPath=/dev/null',
            '-c', 'user.name=Architecture fixture', '-c', 'user.email=fixture@example.invalid',
            '-C', str(self.path), *args], stderr=subprocess.PIPE, text=True).strip()

    def commit(self, files, *, deleted=()):
        for name in deleted:
            (self.path/name).unlink()
        for name, source in files.items():
            path = self.path/name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(source)
        self.git('add', '--all')
        self.git('commit', '-q', '-m', 'fixed fixture')
        return self.git('rev-parse', 'HEAD')


@pytest.fixture
def repo(tmp_path):
    return Repository(tmp_path/'repository')


def generate(repo, base, candidate, output, **kwargs):
    receipt = diag.generate(repo.path, base, candidate, output, **kwargs)
    assert receipt['status'] == 'complete', receipt
    return receipt, tuple(json.loads((output/name).read_text()) for name in diag.REPORTS)


def members(rows):
    return {r['module'] for r in rows}


def test_scc_tracks_merge_split_new_existing_exit_and_deletion(repo, tmp_path):
    base = repo.commit({'demiflow/__init__.py': '',
        'demiflow/a.py': 'import demiflow.b\n', 'demiflow/b.py': 'import demiflow.a\n',
        'demiflow/c.py': 'import demiflow.d\n', 'demiflow/d.py': 'import demiflow.c\n',
        'demiflow/e.py': '', 'demiflow/f.py': 'import demiflow.gone\n',
        'demiflow/gone.py': 'import demiflow.f\n'})
    merged = repo.commit({'demiflow/b.py': 'import demiflow.a, demiflow.c\n',
        'demiflow/c.py': 'import demiflow.d, demiflow.a\n',
        'demiflow/e.py': 'import demiflow.new\n', 'demiflow/new.py': 'import demiflow.e\n',
        'demiflow/f.py': ''}, deleted=['demiflow/gone.py'])
    _, (scc, _) = generate(repo, base, merged, tmp_path/'merge-report')
    delta = scc['delta']
    assert members(delta['cyclic_entered_new']) == {'demiflow.new'}
    assert members(delta['cyclic_entered_existing']) == {'demiflow.e'}
    assert members(delta['cyclic_exited_existing']) == {'demiflow.f'}
    assert members(delta['cyclic_deleted_modules']) == {'demiflow.gone'}
    assert sum(r['merge'] for r in delta['overlap_links']) == 2
    assert any(members(c['members']) == {'demiflow.a', 'demiflow.b', 'demiflow.c', 'demiflow.d'}
               for c in scc['snapshots']['candidate']['components'])
    assert all(m['path'].startswith('demiflow/') for c in delta['before_changed_components'] for m in c['members'])
    split = repo.commit({'demiflow/b.py': 'import demiflow.a\n', 'demiflow/c.py': 'import demiflow.d\n'})
    _, (scc, _) = generate(repo, merged, split, tmp_path/'split-report')
    assert sum(r['split'] for r in scc['delta']['overlap_links']) == 2


def test_deterministic_fixed_objects_ignore_working_tree_and_only_relocate_lines(repo, tmp_path, monkeypatch):
    base = repo.commit({'demiflow/__init__.py': '', 'demiflow/a.py': 'from demiflow.b import X\n',
                        'demiflow/b.py': 'import demiflow.a\n'})
    candidate = repo.commit({'demiflow/a.py': '\n\nfrom demiflow.b import X\n'})
    (repo.path/'demiflow/a.py').write_text('uncommitted invalid Python ?!\n')
    monkeypatch.setenv('GIT_WORK_TREE', str(tmp_path/'wrong-tree'))
    outputs = (tmp_path/'first', tmp_path/'second')
    for output in outputs:
        _, (scc, _) = generate(repo, base, candidate, output)
        changes = scc['delta']['dependencies']
        assert changes['added'] == changes['removed'] == []
        assert len(changes['relocated']) == 1
        assert changes['relocated'][0]['before_locations'][0]['line'] == 1
        assert changes['relocated'][0]['locations'][0]['line'] == 3
        assert not scc['delta']['before_changed_components']
    for name in diag.REPORTS:
        assert (outputs[0]/name).read_bytes() == (outputs[1]/name).read_bytes()
        assert str(tmp_path).encode() not in (outputs[0]/name).read_bytes()


def test_static_relative_kinds_and_dynamic_observations_stay_separate(repo, tmp_path):
    source = '''import demiflow.peer
from .peer import One, Two
from . import other
if TYPE_CHECKING:
    import demiflow.typed
if typing.TYPE_CHECKING:
    import demiflow.typedtwo
try:
    import demiflow.guarded
except ImportError:
    pass
def deferred():
    from .peer import Three
    importlib.import_module(module_name)
importlib.import_module('.peer', 'demiflow')
symbol = 'demiflow.peer:One'
raise AssertionError('source must never execute')
'''
    commit = repo.commit({'demiflow/__init__.py': '', 'demiflow/a.py': source,
        **{'demiflow/'+name+'.py': '' for name in ('peer', 'other', 'typed', 'typedtwo', 'guarded')}})
    budget = diag.Budget()
    identity = diag.fixed_inputs(repo.path, commit, commit, budget)['base']
    snapshot = diag.analyze_snapshot(repo.path, identity, budget)
    rows = snapshot['rows']
    named = {e['names'][0]: e for e in rows['static'] if e['names']}
    assert named['One']['dst'] == named['Two']['dst'] == 'demiflow.peer'
    assert named['Three']['kind'] == 'lazy'
    assert next(e for e in rows['static'] if e['dst'] == 'demiflow.other')['names'] == []
    assert {e['kind'] for e in rows['static'] if e['dst'].startswith('demiflow.typed')} == {'typing'}
    assert next(e for e in rows['static'] if e['dst'] == 'demiflow.guarded')['kind'] == 'guarded'
    assert rows['literal_dynamic'][0]['dst'] == 'demiflow.peer'
    assert rows['unresolved_dynamic'][0]['kind'] == 'lazy'
    assert any(r['raw'] == 'demiflow.peer:One' for r in rows['symbols'])
    assert not any(e['dst'].startswith('demiflow.typed') for e in snapshot['run'])
    assert snapshot['cycles'] == []


def test_declaration_closure_candidates_and_added_missing_seeds(repo, tmp_path):
    base = repo.commit({'demiflow/__init__.py': '',
        'demiflow/data/plan.py': '''from demiflow.foreign import Contract
if TYPE_CHECKING:
    from demiflow.helpers import T
def f():
    from demiflow.execution.dataflow import Driver
    from demiflow.helpers import useful
''', 'demiflow/foreign.py': 'from demiflow.data.plan import P\n', 'demiflow/helpers.py': '',
        'demiflow/execution/dataflow.py': 'import demiflow.execution.impl\n',
        'demiflow/execution/impl.py': 'import demiflow.execution.dataflow\n'})
    candidate = repo.commit({'demiflow/data/sources.py': 'from demiflow.resources.contracts import R\n',
                             'demiflow/resources/contracts.py': ''})
    _, (_, report) = generate(repo, base, candidate, tmp_path/'declaration')
    view = report['snapshots']['base']
    assert members(view['closure']) == {'demiflow.data.plan', 'demiflow.foreign'}
    assert view['counts']['seed_top_candidates']['statements'] == 1
    assert view['counts']['lazy_into_base_scc']['statements'] == 1
    assert view['counts']['lazy_outside_base_scc']['statements'] == 1
    assert 'demiflow.data.sources' in view['missing_fixed_seeds']
    assert members(report['delta']['seeds_added']) == {'demiflow.data.sources'}
    assert len(view['typing_from_closure']) == 1


def test_missing_acyclic_and_dissolved_anchor_are_explicit(repo, tmp_path):
    base = repo.commit({'demiflow/__init__.py': '', 'demiflow/execution/dataflow.py': 'import demiflow.a\n',
                        'demiflow/a.py': 'import demiflow.execution.dataflow\n'})
    acyclic = repo.commit({'demiflow/a.py': ''})
    _, (report, _) = generate(repo, base, acyclic, tmp_path/'acyclic')
    assert report['snapshots']['candidate']['anchors']['base']['status'] == 'acyclic'
    assert report['snapshots']['candidate']['components'] == []
    missing = repo.commit({}, deleted=['demiflow/execution/dataflow.py'])
    _, (report, _) = generate(repo, base, missing, tmp_path/'missing')
    assert report['snapshots']['candidate']['anchors']['base']['status'] == 'missing'
    assert members(report['delta']['cyclic_deleted_modules']) == {'demiflow.execution.dataflow'}


def test_actual_uncommitted_merge_tree_has_null_commit_and_matches_later_commit(repo, tmp_path):
    base = repo.commit({'demiflow/__init__.py': '', 'demiflow/a.py': ''})
    repo.git('checkout', '-q', '-b', 'side')
    repo.commit({'demiflow/b.py': 'import demiflow.a\n'})
    repo.git('checkout', '-q', 'fixture')
    repo.commit({'demiflow/c.py': ''})
    repo.git('merge', '--no-commit', '--no-ff', 'side')
    staged = repo.git('write-tree')
    _, (scc, _) = generate(repo, base, staged, tmp_path/'merge-tree')
    identity = scc['inputs']['candidate']
    assert identity == {'input_kind': 'tree', 'requested_sha': staged, 'tree_sha': staged, 'commit_sha': None}
    repo.git('commit', '-q', '-m', 'real merge fixture')
    merged = repo.git('rev-parse', 'HEAD')
    _, (other, _) = generate(repo, base, merged, tmp_path/'merge-commit')
    assert other['inputs']['candidate']['tree_sha'] == staged
    assert scc['snapshots'] == other['snapshots']


@pytest.mark.parametrize('case', ['symbolic', 'short', 'missing', 'base_tree', 'candidate_blob', 'syntax', 'symlink', 'missing_tree'])
def test_invalid_inputs_never_publish_empty_success(repo, tmp_path, case):
    base = repo.commit({'demiflow/__init__.py': '', 'demiflow/a.py': ''})
    candidate = base
    if case == 'symbolic': candidate = 'HEAD'
    if case == 'short': candidate = base[:12]
    if case == 'missing': candidate = '0'*40
    if case == 'base_tree': base = repo.git('rev-parse', base+'^{tree}')
    if case == 'candidate_blob': candidate = repo.git('rev-parse', base+':demiflow/a.py')
    if case == 'syntax': candidate = repo.commit({'demiflow/a.py': 'def ???\n'})
    if case == 'symlink':
        (repo.path/'demiflow/link.py').symlink_to('../../outside.py')
        repo.git('add', '--all'); repo.git('commit', '-q', '-m', 'symlink fixture')
        candidate = repo.git('rev-parse', 'HEAD')
    if case == 'missing_tree':
        candidate = repo.commit({'README': 'no platform source\n'}, deleted=['demiflow/__init__.py', 'demiflow/a.py'])
    output = tmp_path/'failed'
    receipt = diag.generate(repo.path, base, candidate, output)
    assert receipt['status'] == 'failed' and receipt['files'] == []
    assert not any((output/name).exists() for name in diag.REPORTS)
    assert json.loads((output/'receipt.json').read_text())['status'] == 'failed'
    assert receipt['requested_inputs'] == {'base': base, 'candidate': candidate}
    if case == 'base_tree': assert receipt['inputs']['base']['input_kind'] == 'tree'
    if case == 'candidate_blob': assert receipt['inputs']['candidate']['input_kind'] == 'blob'


@pytest.mark.parametrize('field', ['modules', 'blob_bytes', 'source_bytes', 'path_bytes', 'metadata_entries', 'metadata_bytes', 'rows', 'report_bytes'])
def test_small_real_input_budgets_reject_incomplete_reports(repo, tmp_path, field):
    commit = repo.commit({'demiflow/__init__.py': '', 'demiflow/a.py': 'import demiflow.b\n',
                          'demiflow/b.py': 'import demiflow.a\n'})
    output = tmp_path/'small'
    receipt = diag.generate(repo.path, commit, commit, output, limits=replace(diag.Limits(), **{field: 1}))
    assert receipt['status'] == 'failed'
    assert receipt['files'] == [] and receipt['error']['code'] in ('object_size', 'budget_'+field)
    assert set(p.name for p in output.iterdir()) == {'receipt.json'}


def test_declared_object_size_is_rejected_before_body_admission():
    reader = diag.GitReader.__new__(diag.GitReader)
    reader.budget = diag.Budget()
    reader.child = SimpleNamespace(stdin=io.BytesIO())
    oid = 'a'*40
    reader.line = lambda: oid+' blob 999999999'
    def forbidden_read(size):
        raise AssertionError('body read before declared size admission')
    reader.exact = forbidden_read
    with pytest.raises(diag.DiagnosticError, match='body not admitted'):
        reader.object(oid, 4)


def test_deadline_and_long_scc_walk_are_bounded_without_recursion(repo):
    commit = repo.commit({'demiflow/__init__.py': ''})
    values = iter([0, 61])
    budget = diag.Budget(clock=lambda: next(values))
    with pytest.raises(diag.DiagnosticError, match='deadline'):
        diag.fixed_inputs(repo.path, commit, commit, budget)
    nodes = {str(i) for i in range(1200)}
    edges = [{'src': str(i), 'dst': str((i+1) % 1200)} for i in range(1200)]
    assert diag.components(nodes, edges, diag.Budget()) == [sorted(nodes)]


def test_exclusive_external_output_and_partial_publication_cleanup(repo, tmp_path, monkeypatch):
    commit = repo.commit({'demiflow/__init__.py': ''})
    existing = tmp_path/'existing'; existing.mkdir(); (existing/'sentinel').write_bytes(b'keep')
    assert diag.generate(repo.path, commit, commit, existing)['status'] == 'failed'
    assert (existing/'sentinel').read_bytes() == b'keep'
    inside = repo.path/'evidence'
    assert diag.generate(repo.path, commit, commit, inside)['error']['code'] == 'output_inside_repository'
    assert not inside.exists()
    original = diag.os.link
    def fail_second(source, target):
        if Path(target).name == diag.REPORTS[1]:
            raise OSError('controlled publication failure')
        return original(source, target)
    monkeypatch.setattr(diag.os, 'link', fail_second)
    output = tmp_path/'write-failure'
    assert diag.generate(repo.path, commit, commit, output)['status'] == 'failed'
    assert {p.name for p in output.iterdir()} == {'receipt.json'}


def test_cli_exit_status_and_os_backup_receipt(repo, tmp_path):
    commit = repo.commit({'demiflow/__init__.py': ''})
    command = [sys.executable, '-I', '-B', str(ROOT/'tools/architecture_diagnostics.py'),
               '--repository', str(repo.path), '--base', commit, '--candidate', commit, '--output-dir']
    good = subprocess.run([*command, str(tmp_path/'cli-good')], capture_output=True, text=True, timeout=20)
    assert good.returncode == 0, good.stdout+good.stderr
    assert json.loads(good.stdout)['backups']['resource_limits'] == 'applied'
    bad = subprocess.run([*command, str(tmp_path/'cli-good')], capture_output=True, text=True, timeout=20)
    assert bad.returncode != 0 and json.loads(bad.stdout)['status'] == 'failed'


@pytest.mark.parametrize('oversized', ['sha', 'repository', 'output'])
def test_cli_rejects_oversized_metadata_without_expanding_error_receipts(repo, tmp_path, oversized):
    commit = repo.commit({'demiflow/__init__.py': ''})
    repository, base, candidate, output = str(repo.path), commit, commit, str(tmp_path/'bounded-error')
    if oversized == 'sha': base = candidate = 'x'*70000
    if oversized == 'repository': repository = 'x'*5000
    if oversized == 'output': output = 'x'*5000
    child = subprocess.run([sys.executable, '-I', '-B', str(ROOT/'tools/architecture_diagnostics.py'),
        '--repository', repository, '--base', base, '--candidate', candidate, '--output-dir', output],
        capture_output=True, text=True, timeout=20)
    assert child.returncode != 0 and len(child.stdout.encode()) < 65536 and len(child.stderr.encode()) < 65536
    receipt = json.loads(child.stdout)
    assert receipt['status'] == 'failed' and receipt['files'] == []
    if oversized == 'sha':
        assert receipt['requested_inputs']['base'] == {'prefix': 'x'*64, 'characters': 70000}
    else:
        assert receipt['error']['code'] == 'invalid_path'
