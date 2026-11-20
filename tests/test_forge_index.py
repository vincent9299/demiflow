"""Derived pages survive failed publications without becoming source authority."""
import json
import os
from pathlib import Path
import sqlite3

import pytest

from demiflow.workbench.projects import index
from demiflow.workbench import directory_operations
from demiflow.workbench.projects import workspace as ws
from demiflow.forge.platform import content_digest


@pytest.fixture
def root(tmp_path):
    (tmp_path / '.forge/inspections').mkdir(parents=True)
    index.initialize_empty(tmp_path)
    return tmp_path


def report(root, number, *, publish=True):
    value = dict(format='demiflow_forge_flow_inspection_v3', entry='flows/test.py:run', status='invalid',
                 binding={'parameter_path': None}, fixture=number)
    value['digest'] = content_digest(value)
    with ws.control_lock(root):
        marker = index.begin(root, 'report', value['digest'])
        path = root / '.forge/inspections' / (value['digest'] + '.json')
        ws.write_control(path, value, create=True)
        os.utime(path, (1000 + number, 1000 + number))
        if publish:
            assert ws.publish_report_index(root, marker) is None
    return value, marker


def test_more_than_128_reports_keyset_paging_and_concurrent_new_entry(root, monkeypatch):
    saved = [report(root, i)[0] for i in range(150)]
    assert index.status(root)['reports'] == 150
    page = index.page(root, 'report', limit=17)
    seen = [e['identity'] for e in page['entries']]
    added, _ = report(root, 200)
    while page['next_cursor']:
        page = index.page(root, 'report', cursor=page['next_cursor'], limit=17)
        seen += [e['identity'] for e in page['entries']]
    assert len(seen) == len(set(seen)) == 150
    assert seen == [v['digest'] for v in reversed(saved)]
    assert added['digest'] not in seen
    calls = []
    original = ws.load_report
    def counted(*args):
        calls.append(args[1])
        return original(*args)
    monkeypatch.setattr(ws, 'load_report', counted)
    page = ws.list_reports(root, limit=5)
    assert len(page['reports']) == len(calls) == 5
    assert page['total'] == 151 and page['index']['complete']
    # Change observation must not reintroduce the old 128-report ceiling.
    assert ws.author_roots(root, {'locations': {}, 'entries': {'recipe.py:run': ['params.json']}}) == ['params.json', 'recipe.py']


def test_source_commit_survives_index_failure_and_pending_sync(root, monkeypatch):
    value, marker = report(root, 1, publish=False)
    path = root / '.forge/inspections' / (value['digest'] + '.json')
    original = path.read_bytes()
    def failed(*args):
        raise sqlite3.OperationalError('injected index failure')
    insert = index._insert
    monkeypatch.setattr(index, '_insert', failed)
    with ws.control_lock(root):
        assert 'injected' in ws.publish_report_index(root, marker)
    assert path.read_bytes() == original
    assert index.status(root)['complete'] is False
    assert index.page(root, 'report')['total'] == 0
    monkeypatch.setattr(index, '_insert', insert)
    result = directory_operations.synchronize(root)
    assert result['complete'] and result['pending'] == 0 and result['reports'] == 1
    assert path.read_bytes() == original


def test_publication_recovery_is_idempotent_after_commit_before_marker_clear(root, monkeypatch):
    value, marker = report(root, 1, publish=False)
    unlink = Path.unlink
    def fail_marker(path, *args, **kwargs):
        if path.parent.name == 'index-pending':
            raise OSError('injected marker removal failure')
        return unlink(path, *args, **kwargs)
    monkeypatch.setattr(Path, 'unlink', fail_marker)
    with ws.control_lock(root):
        assert 'injected' in ws.publish_report_index(root, marker)
    assert index.status(root)['reports'] == 1
    monkeypatch.setattr(Path, 'unlink', unlink)
    assert directory_operations.synchronize(root)['reports'] == 1
    assert index.status(root)['complete']


def test_corrupt_index_explicit_rebuild_preserves_sources_and_invalidates_cursor(root):
    for i in range(3):
        report(root, i)
    cursor = index.page(root, 'report', limit=1)['next_cursor']
    originals = {p.name: p.read_bytes() for p in (root / '.forge/inspections').iterdir()}
    (root / '.forge/index.sqlite').write_bytes(b'corrupt')
    assert index.status(root)['reason'] == 'index_unavailable'
    with pytest.raises(ValueError, match='unavailable'):
        index.page(root, 'report')
    assert directory_operations.rebuild(root)['reports'] == 3
    with pytest.raises(ValueError, match='cursor'):
        index.page(root, 'report', cursor=cursor)
    assert {p.name: p.read_bytes() for p in (root / '.forge/inspections').iterdir()} == originals


def test_rebuild_limits_and_legacy_reads_do_not_silently_change_sources(root):
    for i in range(3):
        report(root, i)
    before = (root / '.forge/index.sqlite').read_bytes()
    with pytest.raises(ValueError, match='budget'):
        directory_operations.rebuild(root, max_entries=2)
    assert (root / '.forge/index.sqlite').read_bytes() == before
    (root / '.forge/index.sqlite').unlink()
    page = ws.list_reports(root)
    assert page['total'] == 3 and not page['index']['complete']
    assert not (root / '.forge/index.sqlite').exists()
    assert directory_operations.rebuild(root)['complete']


def test_pending_capacity_and_source_less_intent_recovery(root, monkeypatch):
    monkeypatch.setattr(index, 'MAX_PENDING', 1)
    with ws.control_lock(root):
        index.begin(root, 'report', '0' * 64)
        with pytest.raises(ValueError, match='pending budget'):
            index.begin(root, 'report', '1' * 64)
    result = directory_operations.synchronize(root)
    assert result['complete'] and result['reports'] == 0
