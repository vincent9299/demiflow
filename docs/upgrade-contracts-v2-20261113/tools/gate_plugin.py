"""Pytest evidence producer. Activated only by run_gate.py in an isolated tree."""
import json
import os
from pathlib import Path

DATA = {'collected': [], 'tests': {}, 'collection_errors': [], 'deselected': []}


def pytest_sessionstart(session):
    import demiflow
    expected = Path(os.environ['DEMIFLOW_GATE_ROOT']).resolve()
    if not Path(demiflow.__file__).resolve().is_relative_to(expected):
        raise RuntimeError('Wrong demiflow import; refusing acceptance')


def pytest_collection_finish(session):
    DATA['collected'] = [item.nodeid for item in session.items]


def pytest_deselected(items):
    DATA['deselected'].extend(item.nodeid for item in items)


def pytest_collectreport(report):
    if report.failed or report.skipped:
        DATA['collection_errors'].append({'nodeid': report.nodeid, 'outcome': report.outcome})


def pytest_runtest_logreport(report):
    DATA['tests'].setdefault(report.nodeid, []).append({
        'phase': report.when, 'outcome': report.outcome,
        'wasxfail': hasattr(report, 'wasxfail'), 'duration_s': report.duration,
    })


def pytest_sessionfinish(session, exitstatus):
    DATA['pytest_exit_code'] = int(exitstatus)
    Path(os.environ['DEMIFLOW_GATE_REPORT']).write_text(json.dumps(DATA, indent=2)+'\n')
