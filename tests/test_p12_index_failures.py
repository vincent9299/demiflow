"""P12 deadline and streaming replacement failure windows."""
from pathlib import Path
import time

import pytest

from demiflow.forge import operation_budget
from demiflow.workbench import directory_operations
from demiflow.workbench.projects import index, workspace as ws
from demiflow.workbench.runs import manager as runs
from test_forge_index import root, report


def test_expired_request_prevents_refresh_marker_before_optional_work(root, monkeypatch):
    monkeypatch.setattr(operation_budget, 'current_deadline', lambda: time.monotonic() - 1)
    monkeypatch.setattr(index, 'begin', lambda *a: pytest.fail('expired request created a marker'))
    assert 'deferred' in runs.refresh_index(root, 'r' + '1' * 24, deadline=time.monotonic() + 60)
    assert index.status(root)['pending'] == 0


def test_request_expiry_after_refresh_begin_keeps_pending_without_summary(root, monkeypatch):
    original = index.begin
    def expire(*a, **k):
        marker = original(*a, **k)
        monkeypatch.setattr(operation_budget, 'current_deadline', lambda: time.monotonic() - 1)
        return marker
    monkeypatch.setattr(index, 'begin', expire)
    monkeypatch.setattr(runs, 'run_index_entry', lambda *a, **k: pytest.fail('expired optional summary'))
    assert runs.refresh_index(root, 'r' + '1' * 24)
    assert index.status(root)['pending'] == 1


@pytest.mark.parametrize('fault', ['source', 'invalid_name', 'symlink', 'replace', 'deadline_after_last'])
def test_rebuild_before_replace_preserves_old_database_markers_and_closes_sources(root, monkeypatch, fault):
    report(root, 1)
    _, marker = report(root, 2, publish=False)
    before = (root / '.forge/index.sqlite').read_bytes()
    pending = root / '.forge/index-pending' / ('report-' + marker['identity'] + '.json')
    marker_bytes = pending.read_bytes()
    original = ws.report_index_entries; closed = []
    def entries(*a, **k):
        try:
            for entry in original(*a, **k):
                if fault == 'source': raise PermissionError('source unreadable')
                yield entry
            if fault == 'deadline_after_last':
                monkeypatch.setattr(index.time, 'monotonic', lambda: k['deadline'] + 1)
        finally: closed.append(True)
    monkeypatch.setattr(ws, 'report_index_entries', entries)
    if fault == 'invalid_name': (root / '.forge/inspections/not-a-report').write_text('invalid')
    if fault == 'symlink': (root / '.forge/inspections/link.json').symlink_to(pending)
    if fault == 'replace':
        monkeypatch.setattr(index.os, 'replace', lambda *a: (_ for _ in ()).throw(OSError('replace refused')))
    with pytest.raises((OSError, ValueError)):
        directory_operations.rebuild(root)
    assert (root / '.forge/index.sqlite').read_bytes() == before
    assert pending.read_bytes() == marker_bytes
    assert closed == [True]
    assert not list((root / '.forge').glob('.index-*'))
