"""Static ratchet for the workbench -> engine boundary.

The workbench may use the engine directly (AR v1.3: no engine service, no
second state store, no pass-through adapter layer). What it may not do is keep
reaching further into engine internals. This test parses source only; nothing
is imported or executed.

``PUBLIC`` is the declared engine surface for the workbench.
``PENDING_*`` are today's known violations, each tagged with the step of
``demiflow-workbench-engine-boundary-design-20261118.md`` that removes it.

Rules:
  1. every workbench import of ``demiflow.execution|planning|data|pipeline``
     is in ``PUBLIC`` or ``PENDING_IMPORTS``;
  2. underscore-prefixed names are never public;
  3. ``._connection`` / ``._context`` / ``._executor`` and direct
     ``sqlite3.connect`` under ``workbench/runs`` occur only as pending;
  4. pending entries only shrink: a fixed violation must be deleted here, and a
     new one needs an edit to this file, where a reviewer will see it.
"""
import ast
import re
from collections import Counter
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
WORKBENCH = REPO / 'demiflow' / 'workbench'
ENGINE = re.compile(r'demiflow\.(execution|planning|data|pipeline)(\.|$)')
PRIVATE_ATTRIBUTES = ('_connection', '_context', '_executor')

PUBLIC = {
    # Public SDK types; preview records their real calls without executing rows.
    'demiflow.data.api': {'DataAPI'},
    'demiflow.data.dataset': {'Dataset', 'MaterializedDataset'},
    # Native metadata interpreters shared by drivers and host admission/readers.
    'demiflow.execution.dataflow_schema': {'terminal_schema'},
    'demiflow.execution.lance_sink': {'spec_from_identity'},
    'demiflow.execution': {'run_telemetry'},
    'demiflow.execution.artifacts': {'resolve_local_artifact'},
    'demiflow.execution.capabilities': {'VERSION', 'stream_residency'},
    # Managed host contract: docs/p4-workbench-boundaries-20261118.md.
    'demiflow.execution.checked': {'CheckedExecution'},
    'demiflow.execution.local_kernel': {'local_execution'},
    'demiflow.execution.dataflow_stage_facts': {'native_stage_commit', 'iter_native_stage_commits'},
    'demiflow.execution.datafusion': {'DataFusionOptions', 'DataFusionSession'},
    'demiflow.execution.output_watch': {'OutputWatch'},
    'demiflow.execution.payload_limits': {'resolve_payload_bounds', 'stream_limits'},
    'demiflow.execution.plan_capture': {'build_logical_plan_view', 'capture_pipeline'},
    'demiflow.execution.plan_facts': {'execution_instances'},
    'demiflow.execution.publication_facts': {
        'PreparedPublicationUnresolved', 'PublicationMismatch', 'admit_prepared_stage',
        'commit_write_context', 'inspect_write_receipt', 'object_namespace',
        'observation_action', 'observation_context'},
    'demiflow.execution.recovery_contract': {
        'MAX_DOCUMENT_BYTES', 'RecoveryContractError', 'document', 'identity'},
    'demiflow.execution.recovery_identity': {'platform_identity'},
    'demiflow.execution.run_context': {'create_target_absent_binding', 'RunContext'},
    'demiflow.execution.run_journal': {'OBSERVATION_COUNT', 'RunJournal'},
    'demiflow.execution.run_reader': {
        'JOURNAL_BYTES', 'RunReadError', 'RunReader', 'open_journal', 'read_run', 'storage_snapshot'},
    'demiflow.execution.run_telemetry': {'latest', 'observe'},
    'demiflow.execution.runtime_limits': {'DEFAULT_LIMITS'},
    'demiflow.execution.sealed_plan_facts': {
        'PlanContractError', 'identity_bridge', 'recovery_restriction', 'sealed_plan_binding',
        'sealed_plan_view'},
    'demiflow.planning.dag': {
        'compile_declared_dag', 'validate_logical_dag', 'validate_physical_execution_plan'},
}

STEP2 = 'step 2: publication inspection moves to execution.publication_facts'
STEP3 = 'step 3: bounded read-only journal reader'
STEP4 = 'step 4: sealed plan view and identity bridge reader'
STEP5 = 'step 5: single engine launch entry'
HEAD = 'unreviewed (added after b4396f8): node description should come from the engine'

# (workbench file, engine module, imported name) -> why it is still allowed
# Added on the newer integration base, audited separately: callsite capture
# observes compiler frame locals. This presentation seam remains pending; it
# must not be described as a stable public compiler API or a step 0-4 fix.
PENDING_IMPORTS = {
    ('graph/plan_callsite.py', 'demiflow.planning.dataflow_compile', 'capture_graph'): HEAD,
    ('graph/plan_details.py', 'demiflow.data.plan', 'CallableSpec'): 'unreviewed (added after b4396f8): node description should come from the engine',
    ('graph/plan_details.py', 'demiflow.data.plan', 'OperatorLLMMapOp'): 'unreviewed (added after b4396f8): node description should come from the engine',
    ('graph/plan_details.py', 'demiflow.execution.dataflow_model_factory', 'CacheFactory'): 'unreviewed (added after b4396f8): node description should come from the engine',
    ('graph/plan_details.py', 'demiflow.execution.dataflow_model_factory', 'PromptFactory'): 'unreviewed (added after b4396f8): node description should come from the engine',
    ('graph/plan_details.py', 'demiflow.execution.dataflow_model_factory', 'RoutedPromptFactory'): 'unreviewed (added after b4396f8): node description should come from the engine',
}

# (workbench file, private attribute) -> exact number of uses still allowed
PENDING_PRIVATE_ATTRIBUTES = {}


# workbench/runs file -> exact number of direct sqlite3.connect calls (step 3)
PENDING_DIRECT_SQLITE = {}


def _scan():
    imports, attributes, sqlite = set(), Counter(), Counter()
    for path in sorted(WORKBENCH.rglob('*.py')):
        relative = path.relative_to(WORKBENCH).as_posix()
        tree = ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                if node.level == 0 and node.module and ENGINE.match(node.module):
                    imports.update((relative, node.module, alias.name) for alias in node.names)
            elif isinstance(node, ast.Import):
                imports.update((relative, alias.name, '<module>') for alias in node.names
                               if ENGINE.match(alias.name))
            elif isinstance(node, ast.Attribute):
                if node.attr in PRIVATE_ATTRIBUTES:
                    attributes[(relative, node.attr)] += 1
                elif (node.attr == 'connect' and isinstance(node.value, ast.Name)
                      and node.value.id == 'sqlite3' and relative.startswith('runs/')):
                    sqlite[relative] += 1
    return imports, dict(attributes), dict(sqlite)


IMPORTS, ATTRIBUTES, SQLITE = _scan()


def _lines(items):
    return '\n  ' + '\n  '.join(sorted(map(repr, items)))


def test_scan_saw_the_workbench():
    assert len(IMPORTS) >= 20 and any(item[0].startswith('runs/') for item in IMPORTS)


def test_every_engine_import_is_declared():
    undeclared = {item for item in IMPORTS
                  if item[2] not in PUBLIC.get(item[1], ()) and item not in PENDING_IMPORTS}
    assert not undeclared, (
        'Workbench imports engine names outside the declared surface. Use a PUBLIC name, or '
        'move the logic into the engine; do not extend PENDING_IMPORTS for new code:' + _lines(undeclared))


def test_private_names_are_never_public():
    private = {(module, name) for module, names in PUBLIC.items() for name in names
               if name.startswith('_')}
    assert not private, 'PUBLIC must not declare private names:' + _lines(private)
    undeclared = {item for item in IMPORTS if item[2].startswith('_') and item not in PENDING_IMPORTS}
    assert not undeclared, 'Workbench imports private engine names:' + _lines(undeclared)


def test_pending_imports_only_shrink():
    fixed = set(PENDING_IMPORTS) - IMPORTS
    assert not fixed, 'These violations are gone; delete them from PENDING_IMPORTS:' + _lines(fixed)
    redundant = {item for item in PENDING_IMPORTS if item[2] in PUBLIC.get(item[1], ())}
    assert not redundant, 'Listed as both PUBLIC and pending:' + _lines(redundant)


def test_private_engine_members_only_shrink():
    assert ATTRIBUTES == PENDING_PRIVATE_ATTRIBUTES, (
        'Private engine member use changed. New uses are not allowed; removed uses must be '
        'deleted from PENDING_PRIVATE_ATTRIBUTES. Found:' + _lines(ATTRIBUTES.items()))


def test_direct_journal_connections_only_shrink():
    assert SQLITE == PENDING_DIRECT_SQLITE, (
        'workbench/runs opens SQLite directly. New opens are not allowed; removed opens must be '
        'deleted from PENDING_DIRECT_SQLITE. Found:' + _lines(SQLITE.items()))


def _defined_names(module):
    path = REPO.joinpath(*module.split('.'))
    source = path / '__init__.py' if path.is_dir() else path.with_suffix('.py')
    names = set()
    for node in ast.parse(source.read_text(encoding='utf-8')).body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(node.name)
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                names.update(n.id for n in ast.walk(target) if isinstance(n, ast.Name))
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            names.update((alias.asname or alias.name).split('.')[0] for alias in node.names)
    if path.is_dir():
        names.update(p.stem for p in path.glob('*.py'))
        names.update(p.name for p in path.iterdir() if p.is_dir())
    return names


def test_public_surface_exists_in_the_engine():
    missing = {(module, name) for module, names in PUBLIC.items()
               for name in names - _defined_names(module)}
    assert not missing, 'PUBLIC names the engine does not define:' + _lines(missing)


def test_engine_never_imports_its_hosts():
    hosts = re.compile(r'demiflow\.(workbench|forge)(\.|$)')
    offenders = set()
    for package in ('execution', 'planning', 'data', 'pipeline', 'resources'):
        for path in sorted((REPO / 'demiflow' / package).rglob('*.py')):
            for node in ast.walk(ast.parse(path.read_text(encoding='utf-8'), filename=str(path))):
                if isinstance(node, ast.ImportFrom) and node.level == 0 and node.module and hosts.match(node.module):
                    offenders.add((path.relative_to(REPO).as_posix(), node.module))
                elif isinstance(node, ast.Import):
                    offenders.update((path.relative_to(REPO).as_posix(), alias.name)
                                     for alias in node.names if hosts.match(alias.name))
    assert not offenders, 'Engine packages import a host:' + _lines(offenders)
