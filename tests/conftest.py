"""Env-guarded evidence capture for the lane H gate G-H (inert unless activated).

Set DEMIFLOW_H_EVIDENCE=/tmp/demiflow-h-evidence.json to record a strict
evidence_guard report: per-nodeid setup/call/teardown outcomes, in-process real
module paths bound to this worktree, and source digests before/after the run.
Synthetic test-domain doubles (no __file__) are excluded from module_paths and
listed separately under test_doubles so real modules and fakes stay distinct.
"""
import hashlib
import os
import sys
from pathlib import Path

_ACTIVE = os.environ.get('DEMIFLOW_H_EVIDENCE')
_TREE = Path(__file__).resolve().parents[1]
_OWNED = [
    'demiflow/execution/dataflow_authority.py',
    'demiflow/execution/dataflow_hosts.py',
    'demiflow/execution/dataflow_artifacts.py',
    'demiflow/execution/dataflow_services.py',
    'demiflow/execution/dataflow_store.py',
    'demiflow/execution/dataflow_control.py',
    'demiflow/execution/worker/dataflow_host.py',
    'demiflow/execution/run_journal.py',
    'tests/test_runtime_v3_h.py',
    'tests/_runtime_v3_h_doubles.py',
    'tests/conftest.py',
]
_state = {'collected': [], 'results': [], 'collection_errors': [], 'deselected': [], 'before': {}}


def _digests():
    out = {}
    for rel in _OWNED:
        path = _TREE / rel
        out[rel] = hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else None
    return out


def _module_paths():
    paths = {}
    for name, module in list(sys.modules.items()):
        if not name.startswith('demiflow'):
            continue
        file = getattr(module, '__file__', None)
        if not file:
            continue
        resolved = Path(file).resolve()
        if resolved.is_relative_to(_TREE):
            paths[name] = str(resolved)
    return paths


def _test_doubles():
    return [name for name, module in list(sys.modules.items())
            if name.startswith('demiflow.execution.contracts.dataflow')
            and getattr(module, '__file__', None) is None]


if _ACTIVE:
    def pytest_configure(config):
        _state['before'] = _digests()

    def pytest_collection_modifyitems(session, config, items):
        _state['collected'] = [item.nodeid for item in items]

    def pytest_collectreport(report):
        if report.failed:
            _state['collection_errors'].append(str(report.longrepr))

    def pytest_runtest_logreport(report):
        _state['results'].append({
            'nodeid': report.nodeid,
            'when': report.when,
            'outcome': report.outcome,
            'wasxfail': bool(getattr(report, 'wasxfail', False)),
            'rerun': 0,
        })

    def pytest_sessionfinish(session, exitstatus):
        report = {
            'exit_code': int(exitstatus),
            'collection_errors': _state['collection_errors'],
            'deselected': _state['deselected'],
            'collected': _state['collected'],
            'source_before': _state['before'],
            'source_after': _digests(),
            'module_paths': _module_paths(),
            'test_doubles': _test_doubles(),
            'results': _state['results'],
        }
        Path(_ACTIVE).write_text(__import__('json').dumps(report, indent=2, sort_keys=True))