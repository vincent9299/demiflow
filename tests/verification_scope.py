"""What a change has to run before main moves: docs/repository-rules.md section 5.1, as a tool.

    build   <out.json> <head> <lane>=<run root> ...
            From one full run, write the test-file -> module map: for every executed test
            file of every lane, the demiflow modules it loaded at run time (recorded by
            tests/import_recorder) together with the ones the file imports itself.
    select  <map.json> <changed path> ...     (or: select <map.json> --git <base> <head>)
            Print the scope of the change and, for an engine change, the standard-lane
            test files that load a changed module and the lanes to add.

Source-only apart from reading receipts and logs: nothing under test is imported.
"""
from __future__ import annotations

import ast
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCHEMA = 'demiflow_test_modules_v1'
LANES = ('standard', 'historical', 'ray', 'historical_ray')

# Section 5.1. A path belongs to the first row it matches; a change takes the union of its rows.
WORKBENCH = ('demiflow/workbench/', 'demiflow/forge/', 'tools/forge_analysis/', 'integrations/coding-agent/')
# Dependencies, packaging and run configuration; entry assembly; the test framework and the
# verification tools themselves. Helper modules under tests/ are shared by many test files.
CROSS = ('pyproject.toml', 'MANIFEST.in', 'pytest.ini', 'setup.cfg', 'tox.ini', 'demiflow/cli.py')
# Persistent formats, sealing, the run journal and recovery: old data must still read and resume.
PERSISTENCE = ('demiflow/execution/seal/', 'demiflow/execution/journal/', 'demiflow/execution/checkpoint/',
               'demiflow/execution/storage/', 'demiflow/execution/contracts/', 'demiflow/execution/pipeline/',
               'demiflow/execution/hosts/execution_recovery.py', 'demiflow/execution/hosts/run_state.py')
# The Ray backend, scheduling and resource management.
RAY = ('demiflow/resources/', 'demiflow/execution/hosts/', 'demiflow/execution/scheduler/',
       'demiflow/execution/worker/')


def is_workbench_test(name):
    """The current selector of section 5.1: by file name."""
    return any(word in name for word in ('workbench', 'forge', 'browser'))


def module_name(path):
    """demiflow/a/b.py -> demiflow.a.b ; demiflow/a/__init__.py -> demiflow.a ; anything else -> None."""
    if not (path.startswith('demiflow/') and path.endswith('.py')):
        return None
    parts = path[:-3].split('/')
    if parts[-1] == '__init__':
        parts.pop()
    return '.'.join(parts)


def static_imports(source):
    """demiflow modules a test file names in its own import statements (function-local ones too)."""
    found = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            found.add(node.module)
            found.update(node.module + '.' + alias.name for alias in node.names)
    return {name for name in found if name == 'demiflow' or name.startswith('demiflow.')}


def build(head, lanes, tree=ROOT):
    """lanes: {lane: run root}. A run root holds f/<index>/meta.json and f/<index>/imports.log."""
    known = {module_name(path.relative_to(tree).as_posix()) for path in (tree / 'demiflow').rglob('*.py')}
    known.discard(None)
    tests = {}
    for lane, root in lanes.items():
        if lane not in LANES:
            raise ValueError('unknown lane: ' + lane)
        files = {}
        for meta in sorted(Path(root).glob('f/*/meta.json')):
            entry = json.loads(meta.read_text())
            name = entry.get('file')
            if not name or 'returncode' not in entry:
                continue
            loaded = static_imports((tree / 'tests' / name).read_text())
            log = meta.with_name('imports.log')
            if log.is_file():
                loaded.update(log.read_text().split())
            # Names that are not modules of this tree (attributes named in `from x import y`,
            # modules of another checkout a historical test loads) say nothing about it.
            files[name] = sorted(loaded & known)
        tests[lane] = files
    modules = sorted(set().union(*(set(found) for files in tests.values() for found in files.values())) or ())
    index = {name: number for number, name in enumerate(modules)}
    return {'schema': SCHEMA, 'candidate': head, 'modules': modules,
            'tests': {lane: {name: [index[m] for m in found] for name, found in files.items()}
                      for lane, files in tests.items()}}


def scope_of(paths):
    """The rows of section 5.1 a set of changed paths falls into."""
    rows = set()
    for path in paths:
        name = path.rsplit('/', 1)[-1]
        if path in CROSS or (path.startswith('tests/') and not name.startswith('test_')):
            rows.add('cross')
        elif path.startswith(WORKBENCH) or (path.startswith('tests/') and is_workbench_test(name)):
            rows.add('workbench')
        elif path.startswith('demiflow/') or path.startswith('tests/'):
            rows.add('engine')
        elif path.startswith('docs/') or path.endswith('.md'):
            rows.add('docs')
        else:
            rows.add('cross')          # unclassified: the wider scope
    if 'cross' in rows or {'workbench', 'engine'} <= rows:
        return 'cross'
    return 'engine' if 'engine' in rows else 'workbench' if 'workbench' in rows else 'docs'


def select(document, paths, tree=ROOT):
    """The verification a change needs. `tests` lists standard-lane files; None means all of them."""
    if document.get('schema') != SCHEMA:
        raise ValueError('not a test-module map')
    scope = scope_of(paths)
    changed_tests = sorted(path[6:] for path in paths
                           if path.startswith('tests/test_') and path.endswith('.py') and (tree / path).is_file())
    result = {'scope': scope, 'map_candidate': document['candidate'], 'changed_tests': changed_tests,
              'lanes': [], 'tests': [], 'reasons': []}
    standard = document['tests'].get('standard', {})
    if scope == 'docs':
        return result
    if scope == 'cross':
        result.update(lanes=list(LANES), tests=None)
        result['reasons'].append('cross-cutting change: the full four lanes')
        return result
    if scope == 'workbench':
        result['lanes'] = ['standard']
        result['tests'] = sorted({name for name in standard if is_workbench_test(name)} | set(changed_tests))
        return result
    modules = document['modules']
    loaded_by = {}
    for name, found in standard.items():
        if not is_workbench_test(name):
            for number in found:
                loaded_by.setdefault(modules[number], set()).add(name)
    engine = {name for name in standard if not is_workbench_test(name)}
    chosen, everything = set(changed_tests), False
    for path in paths:
        module = module_name(path)
        if module is None:
            continue
        if module in loaded_by:
            chosen |= loaded_by[module]
            continue
        # No test of the last full run recorded this module. It is new, or only loaded where
        # the recorder cannot see. A new module is covered by the tests of its package.
        package = module.rsplit('.', 1)[0]
        siblings = set().union(*(tests for name, tests in loaded_by.items()
                                 if name == package or name.startswith(package + '.')), set())
        if (tree / path).is_file() and siblings and not any(m == module for m in modules):
            chosen |= siblings
            result['reasons'].append(module + ': not in the map, covered by the tests of ' + package)
        else:
            everything = True
            result['reasons'].append(module + ': no test recorded loading it, all engine tests')
    # A test file the map has never seen is new or renamed: it always runs.
    present = {path.name for path in (tree / 'tests').glob('test_*.py') if not is_workbench_test(path.name)}
    recorded = {name for lane in document['tests'].values() for name in lane}
    chosen |= present - recorded
    result['lanes'] = ['standard']
    result['tests'] = sorted(engine | chosen) if everything else sorted(chosen)
    persistence = any(path.startswith(PERSISTENCE) for path in paths)
    ray = any(path.startswith(RAY) for path in paths)
    if persistence:
        result['lanes'].append('historical')
    if ray:
        result['lanes'].append('ray')
    if persistence and ray:
        result['lanes'].append('historical_ray')
    return result


def main(argv):
    if len(argv) >= 4 and argv[0] == 'build':
        lanes = dict(item.split('=', 1) for item in argv[3:])
        Path(argv[1]).write_text(json.dumps(build(argv[2], lanes), separators=(',', ':')) + '\n')
        return 0
    if len(argv) >= 3 and argv[0] == 'select':
        document = json.loads(Path(argv[1]).read_text())
        if argv[2] == '--git':
            paths = subprocess.check_output(['git', 'diff', '--name-only', argv[3], argv[4]], cwd=ROOT,
                                            text=True).split()
        else:
            paths = argv[2:]
        print(json.dumps(select(document, paths), indent=1, ensure_ascii=False))
        return 0
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1:]))
