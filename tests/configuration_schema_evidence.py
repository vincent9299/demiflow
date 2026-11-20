"""Explicit, process-local compiler evidence; importing this module installs nothing.

Each declared case has an outcome and ordered stage counts. A refused case uses
its fixed baseline error code and may have only a prefix of stages. Success
requires at least one complete capture/graph/budget/final cycle. Driver logs say
nothing about a child: each declared PID must return its own completion receipt.
This is a test harness, not a production recorder or an enforcement mode.
"""
from contextlib import contextmanager
import hashlib
import json
import logging
import os
from pathlib import Path
import subprocess

from demiflow.planning.dataflow_configuration import STAGES

LOGGER = 'demiflow.planning.configuration'
MAX_BYTES = 8 * 1024 * 1024
MAX_PROCESSES = 64
MAX_CASES = 4096


def source_binding(tree):
    """Bind the observer to the actual package and a clean, committed tree."""
    import demiflow
    tree = Path(tree).resolve()
    imported = Path(demiflow.__file__).resolve()
    if not imported.is_relative_to(tree / 'demiflow'):
        raise ValueError('observer imported a different tree')
    def git(*args):
        return subprocess.check_output(['git', *args], cwd=tree, text=True, timeout=10).strip()
    if git('status', '--porcelain'):
        raise ValueError('evidence requires a clean committed source')
    return {'sha': git('rev-parse', 'HEAD'), 'tree': str(tree), 'import_path': str(imported)}


class ProcessEvidence(logging.Handler):
    """Install explicitly with ``with``; only ``write`` records completion.

    expected maps fixture names to 'compiled' or a fixed refusal code, e.g.
    'DataflowError:UNSUPPORTED_CAPABILITY'. Refusal equality is checked by the
    caller against its original baseline, never learned from a failing run.
    """
    def __init__(self, source, expected, *, role='compiler', role_basis=None):
        super().__init__(logging.DEBUG)
        if len(expected) > MAX_CASES or any(type(k) is not str or len(k) > 256 for k in expected):
            raise ValueError('case manifest exceeds evidence budget')
        if role not in ('compiler', 'not_a_compiler'):
            raise ValueError('unknown process role')
        if role == 'not_a_compiler' and (expected or not role_basis or len(role_basis) > 256):
            raise ValueError('non-compiler role needs a bounded source-bound basis and no compiler cases')
        self.source, self.expected = dict(source), dict(expected)
        self.role, self.role_basis = role, role_basis
        self.pid = os.getpid()
        self.cases = {}
        self.active = None
        self.problems = set()
        self.closed = False
        self.installed = False
        self._logger = logging.getLogger(LOGGER)

    def __enter__(self):
        if self.installed or self.closed:
            raise ValueError('observer is single use')
        self.level_before = self._logger.level
        self._logger.addHandler(self)
        self._logger.setLevel(logging.DEBUG)
        self.installed = True
        return self

    def __exit__(self, exc_type, exc, traceback):
        self._logger.removeHandler(self)
        self._logger.setLevel(self.level_before)
        self.installed = False
        self.closed = True
        if exc_type is not None:
            self.problems.add('process_scope_failed')

    @contextmanager
    def case(self, name):
        if not self.installed or self.active is not None or name not in self.expected or name in self.cases:
            self.problems.add('invalid_case_scope')
            raise ValueError('case must be declared once in an installed observer')
        record = {'outcome': None, 'stages': {s: 0 for s in STAGES},
                  'findings': 0, 'check_errors': 0, 'codes': {}, 'categories': {}}
        self.cases[name] = record
        self.active = record
        try:
            yield record
        finally:
            self.active = None
            if record['outcome'] != self.expected[name]:
                self.problems.add('missing_or_changed_outcome')
            counts = list(record['stages'].values())
            if record['outcome'] == 'compiled' and (not counts[0] or len(set(counts)) != 1):
                self.problems.add('missing_stage')
            if record['findings'] or record['check_errors']:
                self.problems.add('schema_findings')

    def emit(self, record):
        # A fork inherits Python handlers; that is not an installed child probe.
        if os.getpid() != self.pid:
            return
        summary = getattr(record, 'configuration_schema', None)
        if summary is None:
            return
        current = self.active
        if current is None:
            self.problems.add('unscoped_compilation')
            return
        try:
            stage = summary['stage']
            if (summary['pid'] != self.pid or stage not in STAGES or summary['version'] != 1
                    or type(summary['nodes']) is not int or summary['nodes'] < 1
                    or type(summary['edges']) is not int or summary['edges'] < 0):
                raise ValueError()
            counts = current['stages']
            # One active compile at a time. A new capture can follow a rejected
            # prefix (budget fitting retries); final must follow all its stages.
            if stage == 'capture':
                current['_next'] = 1
            elif current.get('_next') != STAGES.index(stage):
                self.problems.add('stage_order')
            else:
                current['_next'] += 1
            counts[stage] += 1
            current['findings'] += summary['findings']
            current['check_errors'] += not summary['check_completed']
            for field in ('codes', 'categories'):
                for code, count in summary[field].items():
                    # Schema-owned codes, finite counts; no message/value copies.
                    if type(code) is not str or len(code) > 64 or type(count) is not int or count < 0:
                        raise ValueError()
                    if code not in current[field] and len(current[field]) >= 64:
                        raise ValueError()
                    current[field][code] = current[field].get(code, 0) + count
        except (KeyError, TypeError, ValueError):
            self.problems.add('invalid_summary')

    def receipt(self):
        if os.getpid() != self.pid:
            raise ValueError('inherited observer cannot complete a child receipt')
        problems = set(self.problems)
        if not self.closed or self.active is not None:
            problems.add('missing_completion')
        if set(self.expected) != set(self.cases):
            problems.add('unobserved_case')
        if self.role == 'compiler' and not self.expected:
            problems.add('zero_cases')
        cases = {name: {k: v for k, v in item.items() if not k.startswith('_')}
                 for name, item in self.cases.items()}
        return {'schema': 'demiflow.configuration_evidence.v1', 'pid': self.pid,
                'role': self.role, 'role_basis': self.role_basis, 'source': self.source,
                'completed': self.closed, 'ok': not problems, 'problems': sorted(problems),
                'expected': self.expected, 'cases': cases}

    def write(self, path, *, max_bytes=MAX_BYTES):
        if not 1024 <= max_bytes <= MAX_BYTES:
            raise ValueError('invalid receipt budget')
        result = self.receipt()
        encoded = json.dumps(result, sort_keys=True, indent=2).encode() + b'\n'
        if len(encoded) > max_bytes:
            result = {k: v for k, v in result.items() if k not in ('cases', 'expected')}
            result.update(ok=False, problems=sorted(set(result['problems']) | {'evidence_budget_exceeded'}))
            encoded = json.dumps(result, sort_keys=True).encode() + b'\n'
        if len(encoded) > max_bytes:
            raise ValueError('metadata exceeds receipt budget')
        with Path(path).open('xb') as output:
            output.write(encoded)
        return result


def summarize(paths, *, source, expected_processes):
    """Only explicitly declared PIDs/roles count. Missing/crashed children fail.

    paths maps the same role slots to receipt paths, with None for a process that
    did not complete. No glob/discovery of prior runs or other processes.
    """
    if not expected_processes or len(expected_processes) > MAX_PROCESSES:
        return {'ok': False, 'problems': ['invalid_process_manifest'], 'processes': {}}
    problems, processes = [], {}
    if set(paths) != set(expected_processes):
        problems.append('process_manifest_mismatch')
    seen = set()
    for slot, expected in expected_processes.items():
        try:
            path = Path(paths[slot])
            with path.open('rb') as stream:
                raw = stream.read(MAX_BYTES + 1)
            if len(raw) > MAX_BYTES:
                raise ValueError()
            receipt = json.loads(raw)
            pid = receipt['pid']
            if (pid in seen or pid != expected['pid'] or receipt['role'] != expected['role']
                    or receipt['source'] != source
                    or receipt['schema'] != 'demiflow.configuration_evidence.v1'
                    or receipt['completed'] is not True or receipt['ok'] is not True):
                raise ValueError()
            seen.add(pid)
            processes[slot] = {'pid': pid, 'role': receipt['role'], 'cases': len(receipt.get('cases', {})),
                               'sha256': hashlib.sha256(raw).hexdigest()}
        except (OSError, TypeError, ValueError, KeyError):
            problems.append('incomplete_process:' + slot)
    return {'ok': not problems, 'problems': problems, 'processes': processes}


def observe_corpus(root, *, baseline, source, raw=None):
    """Use the unchanged corpus entrypoints and a fixed, pre-existing baseline.

    Returns the ordinary dump plus an observer whose receipt can be written.
    The caller must also run corpus.compare: stage evidence alone is not byte,
    declaration, report binding, label or refusal-message equivalence evidence.
    """
    import compile_contract_corpus as corpus
    import demiflow
    families = {'cases': (corpus.CASES, corpus.compile_case),
                'captured': (corpus.CAPTURED, corpus.capture_case)}
    if any(set(baseline[family]) != set(entries) for family, (entries, _) in families.items()):
        raise ValueError('corpus manifest differs from the fixed baseline')
    expected = {family + '/' + name: 'compiled' if item['ok'] else item['error'].split(': ', 1)[0]
                for family in families for name, item in baseline[family].items()}
    context = corpus.Context(root)
    if raw is not None:
        Path(raw).mkdir(parents=True, exist_ok=False)
    result = {'engine': str(Path(demiflow.__file__).resolve().parents[1]), 'root': str(context.root)}
    with ProcessEvidence(source, expected) as observer:
        for family, (entries, compile_case) in families.items():
            result[family] = {}
            for name in sorted(entries):
                target = None if raw is None else Path(raw) / (family + '-' + name.replace('/', '-') + '.wire')
                with observer.case(family + '/' + name) as case:
                    item = compile_case(name, context, raw=target)
                    result[family][name] = item
                    case['outcome'] = 'compiled' if item['ok'] else item['error'].split(': ', 1)[0]
    return result, observer
