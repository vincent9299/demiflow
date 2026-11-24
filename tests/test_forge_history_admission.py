"""Retained history accounting, ownership races, and bounded read-only scans."""
from contextlib import closing
import copy
import json
import os
from pathlib import Path
import socket
import shutil
import sqlite3
import threading
import time
from types import SimpleNamespace

import pytest

from demiflow.execution.pipeline.run_context import RunContext
from demiflow.execution.journal.run_journal import RunJournal
from demiflow.execution.journal.run_reader import storage_snapshot as native_storage_snapshot
from demiflow.workbench.runs import storage as run_storage
from demiflow.workbench.runs import domain as run_domain
from demiflow.forge.schema import ForgeError


@pytest.fixture(autouse=True)
def private_domain(tmp_path, monkeypatch):
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN', str(tmp_path / 'domain'))


def run_fixture(root, digit='1', *, status='prepared', objects=(), attempts=True):
    run_id = 'r' + digit * 24
    target = Path(root) / '.forge/runs' / run_id
    journal = RunJournal(target)
    journal.initialize(dict(run_id=run_id, object_locations=list(objects)), attempts=attempts)
    journal.transition({None}, status)
    return target, RunJournal(target, attempt=1 if attempts else None)


def database(target, sql, parameters=()):
    with closing(sqlite3.connect(target / 'run.sqlite')) as db:
        db.execute(sql, parameters)
        db.commit()


def placeholder_root(target):
    # For byte/schema rejection tests only; never accepted as an owned root.
    database(target, 'INSERT INTO runtime_object_roots VALUES (?,?,?,?)',
             ('a' * 64, '/logical', '/physical', 'a' * 32))


def fd_count():
    return len(list(Path('/proc/self/fd').iterdir()))


def test_actual_paths_capacity_and_exclusions(tmp_path):
    target, _ = run_fixture(tmp_path)
    for name, size in [('source', 17), ('state', 23), ('materializations', 31),
                       ('scratch', 37), ('work', 100), ('evidence', 200), ('checkpoints', 300)]:
        (target / name).mkdir()
        (target / name / 'payload').write_bytes(b'x' * size)
    # Logical bytes per path, including hardlinks; no inode dedup or blocks.
    os.link(target / 'source/payload', target / 'source/alias')
    expected = (target / 'run.sqlite').stat().st_size + 17 * 2 + 23 + 31 + 37
    assert run_domain.history_admission(tmp_path, reserve=0) == expected
    assert run_domain.history_admission(tmp_path, reserve=1, ceiling=expected + 1) == expected
    with pytest.raises(ForgeError, match='history capacity'):
        run_domain.history_admission(tmp_path, reserve=1, ceiling=expected)


@pytest.mark.parametrize('shape', ['workspace', 'forge', 'runs', 'empty_run', 'old_run'])
def test_missing_and_old_history_compatibility(tmp_path, shape):
    if shape != 'workspace':
        (tmp_path / '.forge').mkdir()
    if shape in {'runs', 'empty_run', 'old_run'}:
        (tmp_path / '.forge/runs').mkdir()
    expected = 0
    if shape == 'empty_run':
        (tmp_path / '.forge/runs/new-empty').mkdir()
    if shape == 'old_run':
        target, _ = run_fixture(tmp_path, attempts=False)
        # Truly old schema lacks the later optional native runtime tables.
        for table in ('runtime_object_roots', 'runtime_identity'):
            database(target, 'DROP TABLE ' + table)
        expected = (target / 'run.sqlite').stat().st_size
    assert run_domain.history_admission(tmp_path, reserve=0) == expected


@pytest.mark.parametrize('status', ['prepared', 'starting', 'running', 'stopping', 'succeeded', 'failed', 'cancelled', 'interrupted'])
def test_active_and_admitting_reserve(tmp_path, status):
    target, _ = run_fixture(tmp_path, status=status)
    actual = (target / 'run.sqlite').stat().st_size
    expected = run_domain.MANAGEMENT_RESERVE if status in {'starting', 'running', 'stopping'} else actual
    assert run_domain.history_admission(tmp_path, reserve=0) == expected
    assert run_domain.history_admission(tmp_path, reserve=0, admitting=target.name) == run_domain.MANAGEMENT_RESERVE


def test_owned_namespace_including_run_reserved_names_is_all_charged(tmp_path):
    location = tmp_path / 'objects'
    project = tmp_path / 'project'
    target, journal = run_fixture(project, objects=[str(location)])
    with RunContext.attach(journal) as context:
        namespace = context.object_namespace(location)
    extra = 0
    for name, size in [('work', 11), ('evidence', 13), ('checkpoints', 17), ('payload', 19)]:
        (namespace / name).mkdir()
        (namespace / name / 'item').write_bytes(b'x' * size)
        extra += size
    expected = (target / 'run.sqlite').stat().st_size + (namespace / '.owner').stat().st_size + extra
    assert run_domain.history_admission(project, reserve=0) == expected


def test_snapshot_is_readonly_bounded_and_ends_each_transaction(tmp_path):
    target, journal = run_fixture(tmp_path)
    before = (target / 'run.sqlite').read_bytes()
    first = native_storage_snapshot(target)
    assert first['state']['status'] == 'prepared'
    assert (target / 'run.sqlite').read_bytes() == before
    journal.transition({'prepared'}, 'starting')
    second = native_storage_snapshot(target)
    assert second['state']['status'] == 'starting'
    # The only write was the explicit native transition, never a read migration.
    assert first['manifest'] == second['manifest']
    journal.transition({'starting'}, 'prepared')
    assert (target / 'run.sqlite').stat().st_size == len(before)


@pytest.mark.parametrize('table,value', [
    ('execution_state', '{"status":"mystery"}'),
    ('execution_state', '{}'),
    ('execution_state', '[]'),
    ('execution_state', '{'),
    ('execution_state', '{"status":[]}'),
    ('execution_state', '{"status":"prepared","value":NaN}'),
    ('manifest', 'null'),
    ('runtime_identity', '[]'),
])
def test_unknown_or_invalid_metadata_refuses(tmp_path, table, value):
    target, _ = run_fixture(tmp_path)
    if table != 'execution_state':
        placeholder_root(target)
    database(target, 'DELETE FROM ' + table)
    database(target, 'INSERT INTO ' + table + '(singleton,document) VALUES (1,?)', (value,))
    with pytest.raises(ForgeError):
        run_domain.history_admission(tmp_path, reserve=0)


@pytest.mark.parametrize('table,maximum', [('execution_state', 65536), ('manifest', 8 * 1024**2), ('runtime_identity', 1024**2)])
def test_document_length_is_bounded_before_decode(tmp_path, table, maximum):
    target, _ = run_fixture(tmp_path)
    if table != 'execution_state':
        placeholder_root(target)
    text = json.dumps({'status': 'prepared', 'x': 'x' * maximum})
    database(target, 'DELETE FROM ' + table)
    database(target, 'INSERT INTO ' + table + '(singleton,document) VALUES (1,?)', (text,))
    with pytest.raises(ForgeError, match='budget'):
        run_domain.history_admission(tmp_path, reserve=0)


@pytest.mark.parametrize('field,size', [('root_id', 65), ('logical_root', 4097), ('physical_root', 4097), ('run_id', 33)])
def test_owned_root_fields_have_byte_bounds(tmp_path, field, size):
    target, _ = run_fixture(tmp_path)
    record = dict(root_id='a' * 64, logical_root='/logical', physical_root='/physical', run_id='a' * 32)
    record[field] = 'x' * size
    database(target, 'INSERT INTO runtime_object_roots VALUES (?,?,?,?)', tuple(record.values()))
    with pytest.raises(ForgeError, match='field exceeds byte budget'):
        run_domain.history_admission(tmp_path, reserve=0)


@pytest.mark.parametrize('corruption', ['namespace', 'native_id', 'locations', 'missing_identity'])
def test_owned_namespace_authority_cannot_be_bypassed(tmp_path, corruption):
    location = tmp_path / 'objects'
    project = tmp_path / 'project'
    target, journal = run_fixture(project, objects=[str(location)])
    with RunContext.attach(journal) as context:
        context.object_namespace(location)
    if corruption == 'namespace':
        database(target, "UPDATE runtime_object_roots SET physical_root='/foreign'")
    elif corruption == 'native_id':
        binding = journal.recovery_identity()
        binding['run_id'] = '../escape'
        database(target, 'UPDATE runtime_identity SET document=?', (json.dumps(binding),))
    elif corruption == 'missing_identity':
        database(target, 'DELETE FROM runtime_identity')
    else:
        database(target, 'UPDATE manifest SET document=?', (json.dumps({'run_id': target.name, 'object_locations': []}),))
    with pytest.raises(ForgeError):
        run_domain.history_admission(project, reserve=0)


@pytest.mark.parametrize('kind', ['symlink', 'skipped_symlink', 'fifo', 'socket', 'root_symlink', 'journal_symlink'])
def test_unknown_files_and_symlinks_refuse_without_fd_leak(tmp_path, monkeypatch, kind):
    target, _ = run_fixture(tmp_path)
    project = tmp_path
    held = None
    if kind == 'symlink':
        (target / 'link').symlink_to('/etc')
    elif kind == 'skipped_symlink':
        (target / 'work').symlink_to('/tmp')
    elif kind == 'fifo':
        os.mkfifo(target / 'fifo')
    elif kind == 'socket':
        held = socket.socket(socket.AF_UNIX)
        monkeypatch.chdir(target)
        held.bind('native.sock')
    elif kind == 'root_symlink':
        project = tmp_path / 'alias'
        project.symlink_to(tmp_path, target_is_directory=True)
    else:
        (target / 'run.sqlite').rename(target / 'saved.sqlite')
        (target / 'run.sqlite').symlink_to(target / 'saved.sqlite')
    try:
        before = fd_count()
        with pytest.raises(ForgeError):
            run_domain.history_admission(project, reserve=0)
        assert fd_count() == before
    finally:
        if held is not None:
            held.close()


@pytest.mark.parametrize('kind', ['journal_limit', 'broken_sqlite', 'missing_state', 'run_identity'])
def test_journal_type_size_schema_and_run_identity(tmp_path, kind):
    target, _ = run_fixture(tmp_path)
    if kind == 'journal_limit':
        # Sparse only to trigger the logical journal limit, never a benchmark.
        with (target / 'run.sqlite').open('ab') as stream:
            stream.truncate(64 * 1024**2 + 1)
    elif kind == 'broken_sqlite':
        (target / 'run.sqlite').write_bytes(b'broken SQLite')
    elif kind == 'missing_state':
        database(target, 'DROP TABLE execution_state')
    else:
        placeholder_root(target)
        database(target, 'UPDATE manifest SET document=?', (json.dumps({'run_id': 'another'}),))
    before = fd_count()
    with pytest.raises(ForgeError):
        run_domain.history_admission(tmp_path, reserve=0)
    assert fd_count() == before


@pytest.mark.parametrize('change', ['state_to_terminal', 'state_to_active', 'roots', 'identity', 'manifest', 'attempt'])
def test_before_after_progress_keeps_reserve_and_identity_changes_refuse(tmp_path, monkeypatch, change):
    run_fixture(tmp_path, status='running' if change == 'state_to_terminal' else 'prepared')
    original = run_domain._HistoryScan.snapshot
    calls = 0

    def snapshot(self, journal, run_id):
        nonlocal calls
        value = copy.deepcopy(original(self, journal, run_id))
        calls += 1
        if calls == 2:
            if change.startswith('state_to_'):
                value['state']['status'] = 'succeeded' if change == 'state_to_terminal' else 'running'
            elif change == 'roots':
                value['object_roots'].append({'new': 'unknown'})
            elif change == 'identity':
                value['identity'] = {'run_id': 'changed'}
            elif change == 'manifest':
                value['manifest'] = {'object_locations': ['/unknown']}
            else:
                value['state']['attempt'] += 1
        return value

    monkeypatch.setattr(run_domain._HistoryScan, 'snapshot', snapshot)
    if change.startswith('state_to_'):
        assert run_domain.history_admission(tmp_path, reserve=0) == run_domain.MANAGEMENT_RESERVE
    else:
        with pytest.raises(ForgeError) as caught:
            run_domain.history_admission(tmp_path, reserve=0)
        assert caught.value.code == 'busy'


@pytest.mark.parametrize('change', ['run_replaced', 'ancestor_replaced', 'directory_added'])
def test_directory_changes_are_unknown_and_release_descriptors(tmp_path, monkeypatch, change):
    project = tmp_path / 'project'
    target, _ = run_fixture(project)
    original = run_domain._HistoryScan.bytes
    changed = False

    def walk(self, fd, **kwargs):
        nonlocal changed
        if changed:
            return original(self, fd, **kwargs)
        changed = True
        if change == 'run_replaced':
            target.rename(target.with_name('moved'))
            target.mkdir()
        elif change == 'ancestor_replaced':
            project.rename(tmp_path / 'moved-project')
            project.mkdir()
        else:
            # Mutation after the initial directory token, during iteration.
            real_scandir = os.scandir
            class ChangingEntries:
                def __init__(self, path):
                    self.inner = real_scandir(path)
                def __enter__(self):
                    return self
                def __exit__(self, *args):
                    self.inner.close()
                def __iter__(self):
                    return self
                def __next__(self):
                    entry = next(self.inner)
                    if not (target / 'new-directory').exists():
                        (target / 'new-directory').mkdir()
                    return entry
            monkeypatch.setattr(os, 'scandir', ChangingEntries)
        return original(self, fd, **kwargs)

    monkeypatch.setattr(run_domain._HistoryScan, 'bytes', walk)
    before = fd_count()
    with pytest.raises(ForgeError):
        run_domain.history_admission(project, reserve=0)
    assert fd_count() == before


def test_limits_and_deadline_are_enforced(tmp_path, monkeypatch):
    assert (run_domain._HISTORY_RUNS, run_domain._HISTORY_ENTRIES,
            run_domain._HISTORY_DEPTH, run_domain._HISTORY_SECONDS,
            run_domain._HISTORY_CPU_SECONDS) == (4096, 65536, 24, 30, 5)
    target, _ = run_fixture(tmp_path)
    with monkeypatch.context() as patch:
        patch.setattr(run_domain, '_HISTORY_ENTRIES', 0)
        with pytest.raises(ForgeError, match='entry budget'):
            run_domain.history_admission(tmp_path, reserve=0)
    with monkeypatch.context() as patch:
        patch.setattr(run_domain, '_HISTORY_RUNS', 0)
        with pytest.raises(ForgeError, match='run budget'):
            run_domain.history_admission(tmp_path, reserve=0)
    (target / 'nested').mkdir()
    with monkeypatch.context() as patch:
        patch.setattr(run_domain, '_HISTORY_DEPTH', 0)
        with pytest.raises(ForgeError, match='depth budget'):
            run_domain.history_admission(tmp_path, reserve=0)
    with monkeypatch.context() as patch:
        patch.setattr(run_domain, '_HISTORY_SECONDS', -1)
        with pytest.raises(ForgeError, match='wait budget'):
            run_domain.history_admission(tmp_path, reserve=0)


def test_deadline_is_checked_inside_a_single_flat_directory(tmp_path, monkeypatch):
    target = tmp_path / '.forge/runs/flat'
    target.mkdir(parents=True)
    for i in range(100):
        (target / str(i)).write_bytes(b'x')
    calls = 0
    def monotonic():
        nonlocal calls
        calls += 1
        return 0 if calls < 80 else 31
    monkeypatch.setattr(run_domain.time, 'monotonic', monotonic)
    before = fd_count()
    with pytest.raises(ForgeError, match='wait budget'):
        run_domain.history_admission(tmp_path, reserve=0)
    assert calls < 120  # Did not finish 100 files before checking the deadline.
    assert fd_count() == before


def test_exact_entry_run_and_real_depth_boundaries(tmp_path, monkeypatch):
    parent = tmp_path / '.forge/runs'
    target = parent / 'first'
    target.mkdir(parents=True)
    (target / 'a').write_bytes(b'a')
    (target / 'b').write_bytes(b'bb')
    with monkeypatch.context() as patch:
        patch.setattr(run_domain, '_HISTORY_ENTRIES', 2)
        assert run_domain.history_admission(tmp_path, reserve=0) == 3
        (target / 'c').write_bytes(b'c')
        with pytest.raises(ForgeError, match='entry budget'):
            run_domain.history_admission(tmp_path, reserve=0)
    (parent / 'second').mkdir()
    with monkeypatch.context() as patch:
        patch.setattr(run_domain, '_HISTORY_RUNS', 2)
        assert run_domain.history_admission(tmp_path, reserve=0) == 4
        (parent / 'third').mkdir()
        with pytest.raises(ForgeError, match='run budget'):
            run_domain.history_admission(tmp_path, reserve=0)
    leaf = target
    for _ in range(24):
        leaf = leaf / 'd'
        leaf.mkdir()
    assert run_domain.history_admission(tmp_path, reserve=0) == 4
    (leaf / 'too-deep').mkdir()
    with pytest.raises(ForgeError, match='depth budget'):
        run_domain.history_admission(tmp_path, reserve=0)


def test_root_row_count_limit_and_unicode_byte_limit(tmp_path):
    target, journal = run_fixture(tmp_path)
    for i in range(64):
        database(target, 'INSERT INTO runtime_object_roots VALUES (?,?,?,?)',
                 (f'{i:064x}', f'/logical-{i}', f'/physical-{i}', 'a' * 32))
    assert len(native_storage_snapshot(journal.directory)['object_roots']) == 64
    database(target, 'INSERT INTO runtime_object_roots VALUES (?,?,?,?)',
             (f'{64:064x}', '/extra', '/physical-extra', 'a' * 32))
    with pytest.raises(ForgeError, match='count exceeds ceiling'):
        run_domain.history_admission(tmp_path, reserve=0)
    database(target, 'DELETE FROM runtime_object_roots')
    database(target, 'INSERT INTO runtime_object_roots VALUES (?,?,?,?)',
             ('a' * 64, '/' + '界' * 1400, '/physical', 'a' * 32))
    with pytest.raises(ForgeError, match='field exceeds byte budget'):
        run_domain.history_admission(tmp_path, reserve=0)


def test_legacy_unicode_state_character_budget_is_preserved(tmp_path):
    target, _ = run_fixture(tmp_path)
    text = json.dumps({'status': 'prepared', 'detail': '界' * 60000}, ensure_ascii=False)
    assert len(text) < 65536 < len(text.encode())
    database(target, 'UPDATE execution_state SET document=?', (text,))
    assert run_domain.history_admission(tmp_path, reserve=0) == (target / 'run.sqlite').stat().st_size


def test_namespace_created_during_traversal_refuses_unknown_capacity(tmp_path, monkeypatch):
    project = tmp_path / 'project'
    location = tmp_path / 'objects'
    _, journal = run_fixture(project, objects=[str(location)])
    original = run_domain._HistoryScan.bytes
    changed = False
    def walk(self, fd, **kwargs):
        nonlocal changed
        if not changed:
            changed = True
            with RunContext.attach(journal) as context:
                context.object_namespace(location)
        return original(self, fd, **kwargs)
    monkeypatch.setattr(run_domain._HistoryScan, 'bytes', walk)
    with pytest.raises(ForgeError, match='ownership changed'):
        run_domain.history_admission(project, reserve=0)


def test_journal_replaced_before_sql_open_cannot_be_admitted(tmp_path, monkeypatch):
    target, _ = run_fixture(tmp_path)
    original = sqlite3.connect
    changed = False
    def connect(*args, **kwargs):
        nonlocal changed
        if not changed:
            changed = True
            (target / 'run.sqlite').rename(target / 'saved.sqlite')
            shutil.copyfile(target / 'saved.sqlite', target / 'run.sqlite')
        return original(*args, **kwargs)
    monkeypatch.setattr(sqlite3, 'connect', connect)
    before = fd_count()
    with pytest.raises(ForgeError, match='identity changed'):
        run_domain.history_admission(tmp_path, reserve=0)
    assert fd_count() == before


def test_oversized_journal_is_rejected_before_any_sql_read(tmp_path, monkeypatch):
    target, _ = run_fixture(tmp_path)
    with (target / 'run.sqlite').open('ab') as stream:
        stream.truncate(64 * 1024**2 + 1)
    def reject_connect(*args, **kwargs):
        pytest.fail('SQLite was reached before journal size admission')
    monkeypatch.setattr(sqlite3, 'connect', reject_connect)
    with pytest.raises(ForgeError, match='type/size exceeds budget'):
        run_domain.history_admission(tmp_path, reserve=0)


@pytest.mark.parametrize('document', ['{"x":' + '[' * 65 + '0' + ']' * 65 + '}',
                                     '{"x":[' + ','.join('{}' for _ in range(270000)) + ']}'])
def test_container_budget_precedes_json_allocation(tmp_path, monkeypatch, document):
    target, _ = run_fixture(tmp_path)
    placeholder_root(target)
    database(target, 'UPDATE manifest SET document=?', (document,))
    original = json.loads
    def decode(raw, *args, **kwargs):
        assert raw != document, 'Unbounded container document reached JSON allocation'
        return original(raw, *args, **kwargs)
    monkeypatch.setattr(json, 'loads', decode)
    with pytest.raises(ForgeError, match='structural budget'):
        run_domain.history_admission(tmp_path, reserve=0)


def test_directory_descriptors_grow_with_depth_and_close(tmp_path, monkeypatch):
    leaf = tmp_path / '.forge/runs/deep'
    leaf.mkdir(parents=True)
    for _ in range(24):
        leaf = leaf / 'd'
        leaf.mkdir()
    original = os.open
    baseline = fd_count()
    peak = baseline
    def opened(*args, **kwargs):
        nonlocal peak
        fd = original(*args, **kwargs)
        peak = max(peak, fd_count())
        return fd
    monkeypatch.setattr(os, 'open', opened)
    assert run_domain.history_admission(tmp_path, reserve=0) == 0
    assert peak <= baseline + 56
    assert fd_count() == baseline


def test_decode_does_not_hold_a_sql_read_transaction(tmp_path, monkeypatch):
    target, _ = run_fixture(tmp_path)
    from demiflow.execution.journal import run_reader
    original = run_reader._bounded_object
    def decode(*args, **kwargs):
        # A real exclusive writer can enter while Python decodes the snapshot.
        with closing(sqlite3.connect(target / 'run.sqlite', timeout=0)) as db:
            db.execute('BEGIN EXCLUSIVE')
            db.rollback()
        return original(*args, **kwargs)
    monkeypatch.setattr(run_reader, '_bounded_object', decode)
    assert native_storage_snapshot(target)['state']['status'] == 'prepared'


def test_disappearing_file_is_not_silently_charged_zero(tmp_path, monkeypatch):
    target = tmp_path / '.forge/runs/owned'
    target.mkdir(parents=True)
    victim = target / 'gone.bin'
    victim.write_bytes(b'x')
    original = os.scandir
    class Entry:
        def __init__(self, entry):
            self.entry = entry
        def __getattr__(self, name):
            return getattr(self.entry, name)
        def stat(self, **kwargs):
            victim.unlink()
            return self.entry.stat(**kwargs)
    class Entries:
        def __init__(self, path):
            self.inner = original(path)
        def __enter__(self):
            return self
        def __exit__(self, *args):
            self.inner.close()
        def __iter__(self):
            return self
        def __next__(self):
            entry = next(self.inner)
            return Entry(entry) if entry.name == victim.name else entry
    monkeypatch.setattr(os, 'scandir', Entries)
    before = fd_count()
    with pytest.raises(ForgeError, match='coverage is unavailable'):
        run_domain.history_admission(tmp_path, reserve=0)
    assert fd_count() == before


def test_native_identity_change_without_objects_still_refuses(tmp_path, monkeypatch):
    target, journal = run_fixture(tmp_path)
    with RunContext.attach(journal):
        pass
    original = run_domain._HistoryScan.bytes
    changed = False
    def walk(self, fd, **kwargs):
        nonlocal changed
        if not changed:
            changed = True
            binding = journal.recovery_identity()
            binding['run_id'] = 'b' * 32
            database(target, 'UPDATE runtime_identity SET document=?', (json.dumps(binding),))
        return original(self, fd, **kwargs)
    monkeypatch.setattr(run_domain._HistoryScan, 'bytes', walk)
    with pytest.raises(ForgeError, match='ownership changed'):
        run_domain.history_admission(tmp_path, reserve=0)


def test_run_set_mutation_refuses_even_when_new_entry_is_empty(tmp_path, monkeypatch):
    target = tmp_path / '.forge/runs/owned'
    target.mkdir(parents=True)
    (target / 'payload').write_bytes(b'x')
    original = run_domain._HistoryScan.bytes
    changed = False
    def walk(self, fd, **kwargs):
        nonlocal changed
        if not changed:
            changed = True
            (target.parent / 'new-empty').mkdir()
        return original(self, fd, **kwargs)
    monkeypatch.setattr(run_domain._HistoryScan, 'bytes', walk)
    with pytest.raises(ForgeError, match='run set changed'):
        run_domain.history_admission(tmp_path, reserve=0)


def test_run_set_check_does_not_depend_on_timestamp_resolution(tmp_path, monkeypatch):
    target = tmp_path / '.forge/runs/owned'
    target.mkdir(parents=True)
    (target / 'payload').write_bytes(b'x')
    original = run_domain._HistoryScan.bytes
    changed = False
    def walk(self, fd, **kwargs):
        nonlocal changed
        result = original(self, fd, **kwargs)
        if not changed:
            changed = True
            (target.parent / 'new-empty').mkdir()
        return result
    monkeypatch.setattr(run_domain._HistoryScan, 'bytes', walk)
    monkeypatch.setattr(run_domain._HistoryScan, 'changed', staticmethod(lambda before, after: False))
    with pytest.raises(ForgeError, match='run set changed'):
        run_domain.history_admission(tmp_path, reserve=0)


def test_run_set_identity_buffer_is_bounded_and_released(tmp_path, monkeypatch):
    for index in range(100):
        (tmp_path / str(index)).mkdir()
    monkeypatch.setattr(run_domain, '_HISTORY_BUFFER_BYTES', 4096)
    scan = run_domain._HistoryScan()
    fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with pytest.raises(ForgeError, match='buffer exceeds management budget'):
            with scan.run_set(fd):
                pytest.fail('over-budget run set was admitted')
        assert scan.buffer_bytes == 0
    finally:
        os.close(fd)


def test_four_workers_have_one_cpu_bounded_jobs_and_restore_caller(tmp_path, monkeypatch):
    for number in range(4):
        target = tmp_path / '.forge/runs' / str(number)
        target.mkdir(parents=True)
        (target / 'payload').write_bytes(b'x')
    barrier = threading.Barrier(4, timeout=3)
    lock = threading.Lock()
    original = run_domain._HistoryScan.run
    masks, threads = set(), set()
    active = peak = 0
    submitted = []
    peak_inflight = 0
    executor = run_domain.ThreadPoolExecutor
    class TrackingExecutor(executor):
        def submit(self, *args, **kwargs):
            nonlocal peak_inflight
            job = super().submit(*args, **kwargs)
            submitted.append(job)
            peak_inflight = max(peak_inflight, sum(not f.done() for f in submitted))
            return job
    caller = os.sched_getaffinity(0)
    def work(self, *args):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
            masks.add(tuple(sorted(os.sched_getaffinity(0))))
            threads.add(threading.get_native_id())
        try:
            barrier.wait()
            return original(self, *args)
        finally:
            with lock:
                active -= 1
    monkeypatch.setattr(run_domain._HistoryScan, 'run', work)
    monkeypatch.setattr(run_domain, 'ThreadPoolExecutor', TrackingExecutor)
    assert run_domain.history_admission(tmp_path, reserve=0) == 4
    assert peak == len(threads) == run_domain._HISTORY_WORKERS == 4
    assert masks == {(min(caller),)}
    assert peak_inflight <= 4
    assert len(threads | {threading.get_native_id()}) == 5
    assert os.sched_getaffinity(0) == caller
    assert not [t for t in threading.enumerate() if t.name.startswith('forge-history')]


def test_global_entry_budget_is_shared_between_concurrent_jobs(tmp_path, monkeypatch):
    for number in range(2):
        target = tmp_path / '.forge/runs' / str(number)
        target.mkdir(parents=True)
        (target / 'a').write_bytes(b'a')
        (target / 'b').write_bytes(b'bb')
    monkeypatch.setattr(run_domain, '_HISTORY_ENTRIES', 4)
    assert run_domain.history_admission(tmp_path, reserve=0) == 6
    monkeypatch.setattr(run_domain, '_HISTORY_ENTRIES', 3)
    with pytest.raises(ForgeError, match='entry budget'):
        run_domain.history_admission(tmp_path, reserve=0)
    assert not [t for t in threading.enumerate() if t.name.startswith('forge-history')]


@pytest.mark.parametrize('failure', ['exception', 'timeout', 'identity', 'memory'])
def test_failure_drains_other_worker_before_return_or_next_admission(tmp_path, monkeypatch, failure):
    from demiflow.workbench.projects.workspace import control_lock
    for name in ('one', 'two'):
        target = tmp_path / '.forge/runs' / name
        target.mkdir(parents=True)
        (target / 'payload').write_bytes(b'x')
    barrier = threading.Barrier(2, timeout=3)
    original = run_domain._HistoryScan.bytes
    drained = threading.Event()
    def walk(self, fd, **kwargs):
        name = Path('/proc/self/fd', str(fd)).resolve().name
        barrier.wait()
        if failure == 'timeout' and name == 'one':
            self.deadline = time.monotonic() + .03
        if name == 'two':
            time.sleep(.12)  # Simulate an in-flight syscall that cannot cancel.
            drained.set()
        elif failure == 'exception':
            raise ForgeError('injected history failure', code='resource_limit')
        elif failure == 'identity':
            target = tmp_path / '.forge/runs/one'
            target.rename(target.with_name('moved'))
            target.mkdir()
        elif failure == 'memory':
            raise MemoryError('injected metadata allocation failure')
        else:
            time.sleep(.1)
        return original(self, fd, **kwargs)
    before = fd_count()
    with monkeypatch.context() as patch:
        patch.setattr(run_domain._HistoryScan, 'bytes', walk)
        with control_lock(tmp_path):
            with pytest.raises(ForgeError):
                run_domain.history_admission(tmp_path, reserve=0)
            assert drained.is_set()
            assert not [t for t in threading.enumerate() if t.name.startswith('forge-history')]
            assert fd_count() == before + 1  # caller's still-held control FD
    assert fd_count() == before
    # Next admission starts after all prior jobs have actually exited.
    assert run_domain.history_admission(tmp_path, reserve=0) == 2


def test_failed_cpu_reservation_refuses_without_background_workers(tmp_path, monkeypatch):
    target = tmp_path / '.forge/runs/one'
    target.mkdir(parents=True)
    def unavailable(*args):
        raise PermissionError('CPU affinity is unavailable')
    monkeypatch.setattr(os, 'sched_setaffinity', unavailable)
    with pytest.raises(ForgeError, match='CPU affinity is unavailable'):
        run_domain.history_admission(tmp_path, reserve=0)
    assert not [t for t in threading.enumerate() if t.name.startswith('forge-history')]


def test_buffer_quota_precedes_retention_and_is_released_on_failure(tmp_path, monkeypatch):
    target = tmp_path / '.forge/runs/flat'
    target.mkdir(parents=True)
    for i in range(100):
        (target / str(i)).write_bytes(b'x')
    monkeypatch.setattr(run_domain, '_HISTORY_BUFFER_BYTES', 4096)
    scan = run_domain._HistoryScan()
    fd = os.open(target, os.O_RDONLY | os.O_DIRECTORY)
    before = fd_count()
    try:
        with pytest.raises(ForgeError, match='buffer exceeds management budget'):
            scan.bytes(fd)
        assert scan.buffer_bytes == 0
        assert scan.remaining > run_domain._HISTORY_ENTRIES-5
        assert fd_count() == before
    finally:
        os.close(fd)


def test_buffer_quota_is_shared_and_counts_held_list_capacity(tmp_path, monkeypatch):
    target = tmp_path / 'entries'
    target.mkdir()
    (target / 'one').write_bytes(b'x')
    monkeypatch.setattr(run_domain, '_HISTORY_BUFFER_BYTES', 6500)
    scan = run_domain._HistoryScan()
    fd = os.open(target, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with scan.entries(fd,0) as first:
            with scan.entries(fd,0) as second:
                assert scan.buffer_bytes > 4000
                with pytest.raises(ForgeError, match='buffer exceeds management budget'):
                    with scan.entries(fd,0):
                        pass
            assert scan.buffer_bytes > 0
        assert scan.buffer_bytes == 0
    finally:
        os.close(fd)


@pytest.mark.parametrize('table', ['manifest', 'runtime_identity'])
def test_projection_drops_large_values_but_full_document_changes_are_detected(tmp_path, table):
    target, journal = run_fixture(tmp_path)
    value = dict(run_id=target.name if table=='manifest' else 'a'*32, ignored='x'*10000)
    database(target, 'DELETE FROM '+table)
    database(target, 'INSERT INTO '+table+'(singleton,document) VALUES (1,?)', (json.dumps(value),))
    before = native_storage_snapshot(journal.directory)
    value['ignored'] = 'y'*10000
    database(target, 'UPDATE '+table+' SET document=?', (json.dumps(value),))
    after = native_storage_snapshot(journal.directory)
    key = 'manifest' if table=='manifest' else 'identity'
    assert before[key] == after[key]
    assert 'ignored' not in after[key]
    assert before[key+'_digest'] != after[key+'_digest']


def test_digest_distinguishes_missing_manifest_from_empty_object(tmp_path):
    target, journal = run_fixture(tmp_path)
    database(target,'DELETE FROM manifest')
    absent=native_storage_snapshot(journal.directory)
    database(target,'INSERT INTO manifest VALUES (1,?)',('{}',))
    empty=native_storage_snapshot(journal.directory)
    assert absent['manifest'] is None and absent['manifest_digest'] is None
    assert empty['manifest']=={} and empty['manifest_digest'] is not None
