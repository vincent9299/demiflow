"""Read-only full-suite inventory and explicit batch receipts; collection is never pass.

Use only from a pinned isolated checkout with three private resource domains.
The resulting nodeids are exact pytest selectors, not AST-guessed test names.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def snapshot():
    files = sorted((ROOT / 'demiflow').rglob('*.py')) + sorted((ROOT / 'tests').rglob('*.py'))
    assert len(files) <= 10000
    return {'head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
            'inputs': {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in files}}


def assert_private():
    import demiflow
    assert Path(demiflow.__file__).resolve().is_relative_to(ROOT)
    for name in ('DEMIFLOW_RESOURCE_ROOT', 'DEMIFLOW_RESOURCE_HOME', 'DEMIFLOW_FORGE_DOMAIN'):
        path = Path(os.environ[name]).resolve()
        assert path.is_relative_to('/tmp') and path.parts[2].startswith('demiflow-'), (name, str(path))


class Inventory:
    def __init__(self):
        self.items = []
        self.collection = []
        self.deselected = []
        self.reports = []

    def pytest_collection_finish(self, session):
        self.items = [{'nodeid': item.nodeid, 'file': str(item.path.relative_to(ROOT)),
                       'line': item.location[1] + 1,
                       'fixtures': list(item.fixturenames),
                       'markers': [{'name': m.name, 'args': repr(m.args), 'kwargs': repr(m.kwargs)}
                                   for m in item.iter_markers()]}
                      for item in session.items]
        assert len(self.items) <= 100000

    def pytest_collectreport(self, report):
        if report.failed or report.skipped:
            self.collection.append({'nodeid': report.nodeid, 'outcome': report.outcome,
                                    'detail': str(report.longrepr)[:32000]})

    def pytest_deselected(self, items):
        self.deselected.extend(item.nodeid for item in items)

    def pytest_runtest_logreport(self, report):
        self.reports.append({'nodeid': report.nodeid, 'when': report.when, 'outcome': report.outcome,
                             'wasxfail': getattr(report, 'wasxfail', None),
                             'seconds': report.duration, 'detail': str(report.longrepr)[:32000] if report.failed else None})


def run(destination, args):
    import pytest
    assert_private()
    dest = Path(destination).resolve()
    assert dest.is_relative_to('/tmp') and dest.parts[2].startswith('demiflow-')
    dest.mkdir(parents=True, exist_ok=True)
    before = snapshot()
    plugin = Inventory()
    code = pytest.main(args, plugins=[plugin])
    after = snapshot()
    result = {'kind': 'collection_only' if '--collect-only' in args else 'execution',
              'python': sys.executable, 'args': args, 'returncode': int(code),
              'environment': {k: os.environ.get(k) for k in ('DEMIFLOW_RESOURCE_ROOT', 'DEMIFLOW_RESOURCE_HOME',
                               'DEMIFLOW_FORGE_DOMAIN', 'PYTHONPATH')},
              'source_before': before, 'source_after': after, 'source_unchanged': before == after,
              'items': plugin.items, 'collection_problems': plugin.collection,
              'deselected': plugin.deselected, 'reports': plugin.reports}
    (dest / 'receipt.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    assert before == after, 'source changed while collecting or executing'
    assert not plugin.deselected, 'implicit deselection is not accepted'
    return int(code)
