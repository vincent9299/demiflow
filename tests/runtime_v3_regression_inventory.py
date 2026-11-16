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
    """Hash actual source/fixtures, including UI assets and consumed contracts.

    Git's tracked + non-ignored untracked inventory limits the walk to declared
    source roots. Generated review evidence and unrelated repository data never
    become input. File hashing uses bounded chunks; an oversized source fails
    explicitly instead of silently disappearing from the provenance boundary.
    """
    package = 'docs/unified-runtime-v3-20261114/'
    roots = ['demiflow', 'tests', 'examples', 'tools/forge_analysis', 'pyproject.toml', 'pytest.ini', 'setup.cfg', 'tox.ini',
             package.rstrip('/')]
    listed = subprocess.check_output(['git', 'ls-files', '-z', '--cached', '--others',
                                      '--exclude-standard', '--', *roots], cwd=ROOT)
    if len(listed) > 4 * 2**20:
        raise ValueError('source file inventory exceeds its bound')
    evidence_inputs = {'historical-dispositions.json', 'regression-files.json',
                       'frozen-files.json', 'source-inventory.json', 'intent-sources.json'}
    names = []
    for raw in set(listed.split(b'\0')) - {b''}:
        name = os.fsdecode(raw)
        if name.startswith(package):
            tail = name[len(package):]
            if tail.startswith('review-'):
                continue
            if tail.startswith('evidence/') and tail.split('/', 1)[1] not in evidence_inputs:
                continue
        names.append(name)
    if len(names) > 10000:
        raise ValueError('source file count exceeds its bound')
    digests, total = {}, 0
    for name in sorted(names):
        path = ROOT / name
        if not path.is_file() or not path.resolve().is_relative_to(ROOT.resolve()):
            raise ValueError('source inventory contains a missing or external file: ' + name)
        size = path.stat().st_size
        total += size
        if size > 64 * 2**20 or total > 512 * 2**20:
            raise ValueError('source bytes exceed the explicit snapshot bound: ' + name)
        digest, seen = hashlib.sha256(), 0
        with path.open('rb') as stream:
            while chunk := stream.read(64 * 1024):
                seen += len(chunk)
                if seen > size:
                    raise ValueError('source grew while hashing: ' + name)
                digest.update(chunk)
        if seen != size:
            raise ValueError('source changed size while hashing: ' + name)
        digests[name] = digest.hexdigest()
    return {'head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
            'inputs': digests}


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
                       # Literal parametrize payloads can be many MiB. Exact
                       # nodeids and source hashes identify them without copying
                       # their bytes into every item in the evidence inventory.
                       'markers': [{'name': m.name, 'argument_count': len(m.args),
                                    'keyword_names': sorted(m.kwargs)}
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
                             'rerun': getattr(report, 'rerun', 0),
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
                               'DEMIFLOW_FORGE_DOMAIN', 'PYTHONPATH', 'TMPDIR')},
              'source_before': before, 'source_after': after, 'source_unchanged': before == after,
              'items': plugin.items, 'collection_problems': plugin.collection,
              'deselected': plugin.deselected, 'reports': plugin.reports}
    (dest / 'receipt.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    assert before == after, 'source changed while collecting or executing'
    assert not plugin.deselected, 'implicit deselection is not accepted'
    return int(code)


def main():
    """Child entry: an owned JSON selector request, never a shell command."""
    request = json.loads(Path(sys.argv[1]).read_text())
    destination = Path(request['destination']).resolve()
    selectors = request['selectors']
    assert type(selectors) is list and 0 < len(selectors) <= 100000
    assert all(type(n) is str and n.startswith('tests/') and '..' not in n.split('::', 1)[0].split('/')
               and (('::' in n) or (request.get('collect_only') and (n == 'tests/' or n.endswith('.py'))))
               for n in selectors)
    args = ['-q', '--tb=short', '-p', 'no:cacheprovider',
            '--basetemp=' + str(destination / 'tmp'),
            '--junitxml=' + str(destination / 'result.xml')]
    if request.get('collect_only'):
        args.append('--collect-only')
    return run(destination, args + selectors)


if __name__ == '__main__':
    raise SystemExit(main())
