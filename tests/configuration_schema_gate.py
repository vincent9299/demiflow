"""Opt-in pytest plugin: a test fails when a plan it compiled does not fit the configuration schema.

    python -m pytest -p configuration_schema_gate tests/...

The schema check in the engine is report-only and silent by default. This
plugin turns its reports into test failures, for the driver process of each
test. It rejects nothing in the engine and changes no test; it is how the
schema can be enforced where a mistake is cheap to find, before it is enforced
anywhere a user's run could be refused.

A test that produces reports on purpose opts out by decorating itself with
``reports_on_purpose`` from this module (no marker to register).
"""
import logging
import os

import pytest

_LOGGER = 'demiflow.planning.configuration'


class _Reports(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.DEBUG)
        self.findings = []
        self.graphs = 0
        self.pid = os.getpid()
        self.stages = {}

    def emit(self, record):
        if os.getpid() != self.pid:
            return                  # inherited handlers do not observe a child
        summary = getattr(record, 'configuration_schema', None)
        if summary is not None:
            stage = summary['stage']
            self.stages[stage] = self.stages.get(stage, 0) + 1
            self.graphs += stage == 'final'
            if summary['findings'] or not summary['check_completed']:
                if len(self.findings) < 20:
                    self.findings.append('stage %s: %d findings, completed=%s' % (
                        stage, summary['findings'], summary['check_completed']))
        elif record.levelno >= logging.WARNING and len(self.findings) < 20:
            self.findings.append(record.getMessage())


def reports_on_purpose(function):
    """Mark a test that makes the schema report something deliberately."""
    function.reports_schema_findings_on_purpose = True
    return function


@pytest.fixture(autouse=True)
def _configuration_schema_gate(request):
    if getattr(getattr(request, 'function', None), 'reports_schema_findings_on_purpose', False):
        yield
        return
    logger = logging.getLogger(_LOGGER)
    handler, level, propagate = _Reports(), logger.level, logger.propagate
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False        # do not leak DEBUG records into a test's own log capture
    try:
        yield
    finally:
        logger.removeHandler(handler)
        logger.setLevel(level)
        logger.propagate = propagate
    if handler.findings:
        pytest.fail('a compiled plan does not fit the configuration schema:\n  ' + '\n  '.join(handler.findings[:20]),
                    pytrace=False)
