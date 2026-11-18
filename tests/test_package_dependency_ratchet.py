"""Repository-wide ratchet for package dependencies.

Source-only (nothing is imported or executed). It pins the dependency facts
that hold today so that structural work can only make them better:

  1. the computing base never imports the workbench or Forge, Forge never
     imports the workbench, and only ``demiflow/cli.py`` assembles both;
  2. ``resources`` and ``_compat`` import no other demiflow package;
  3. imports of another package's underscore-private names may not grow, per
     (importer package, imported package);
  4. the pre-v3 execution modules may not gain importers.

Budgets are upper bounds. When a number drops, lower the budget in the same
change; raising one needs an edit here, where a reviewer will see it.
"""
import ast
from collections import Counter, defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
PACKAGE = REPO / 'demiflow'
HOSTS = {'workbench', 'forge'}
ASSEMBLY = {'demiflow.cli'}          # the only module allowed to register both hosts
LEAVES = {'resources', '_compat'}    # import no other demiflow package

# (importer package, imported package) -> most private-name imports allowed.
# 'root' is the top-level modules of ``demiflow/`` (errors, objects, cli, ...).
PRIVATE_IMPORT_BUDGET = {
    ('execution', 'data'): 10, ('execution', 'lance'): 10, ('workbench', 'resources'): 8,
    ('execution', 'planning'): 7, ('execution', 'root'): 4, ('root', 'operator_llm'): 3,
    ('data', 'root'): 3, ('execution', 'operator_llm'): 3, ('execution', 'resources'): 3,
    ('workbench', 'execution'): 3, ('data', 'execution'): 2, ('execution', 'services'): 2,
    ('workbench', 'root'): 2, ('planning', 'execution'): 2, ('operator_llm', 'root'): 1,
    ('execution', 'collect'): 1, ('embeddings', 'operator_llm'): 1, ('pipeline', 'data'): 1,
    ('lance', 'execution'): 1,
}

# Pre-v3 execution modules. runtime-v3 has one orchestration entry
# (``execution.dataflow.execute_action``); these modules predate it and are still
# imported, partly to read original history and partly as reused components.
# Listing a module here does not say it can be deleted. It says: do not build new
# code on it without deciding that on purpose. Value: today's importers.
PRE_V3_MODULES = {
    'demiflow.execution.chain': {
        'demiflow.execution.dataflow_code', 'demiflow.execution.dataflow_seal',
        'demiflow.execution.graph', 'demiflow.execution.kernels.task_scope',
        'demiflow.execution.native_plan', 'demiflow.execution.queue_chain',
        'demiflow.execution.worker.bootstrap', 'demiflow.execution.worker.execution',
        'demiflow.execution.worker.graph_execution', 'demiflow.execution.worker.queue_transform'},
    'demiflow.execution.stream': {
        'demiflow.data.dataset', 'demiflow.execution.dataflow_stream',
        'demiflow.execution.request_limits', 'demiflow.execution.worker.execution',
        'demiflow.execution.worker.graph_execution', 'demiflow.execution.worker.queue_transform'},
    'demiflow.execution.plan_interpreter': {
        'demiflow.execution.chain', 'demiflow.execution.dataflow_history',
        'demiflow.execution.graph', 'demiflow.execution.native_plan',
        'demiflow.execution.sealed_plan_facts', 'demiflow.execution.sealed_plans',
        'demiflow.execution.worker.layout', 'demiflow.planning.dataset_dag',
        'demiflow.workbench.runs.facts'},
    'demiflow.execution.local_tasks': {
        'demiflow.execution.arrow_batch_sink', 'demiflow.execution.dataset_native'},
    'demiflow.execution.local': {'demiflow.execution'},
    'demiflow.execution.ray': {'demiflow.execution'},
}

# Execution modules the plan compiler imports today. Plan compilation and run
# orchestration are one module by intent, so this dependency is allowed; what
# must not happen is the compiler learning about yet another runtime module.
PLANNING_MAY_IMPORT = {
    'demiflow.execution.contracts.dataflow',
    'demiflow.execution.contracts.dataflow_codec',
    'demiflow.execution.contracts.dataflow_errors',
    'demiflow.execution.contracts.errors',
    'demiflow.execution.dataflow_compile_inputs',
    'demiflow.execution.dataflow_managed_devices',
    'demiflow.execution.dataflow_model_factory',
    'demiflow.execution.dataflow_models',
    'demiflow.execution.dataflow_native',
    'demiflow.execution.dataflow_native_limits',
    'demiflow.execution.dataflow_prompt_service',
    'demiflow.execution.dataflow_publication_budget',
    'demiflow.execution.dataflow_routed_prompt',
    'demiflow.execution.dataflow_schema',
    'demiflow.execution.dataflow_shared',
    'demiflow.execution.kernels.reducer_memory',
    'demiflow.execution.kernels.shuffle_order',
    'demiflow.execution.kernels.task_scope',
    'demiflow.execution.local_kernel',
    'demiflow.execution.operator_policy',
    'demiflow.execution.payload_limits',
    'demiflow.execution.plan_facts',
    'demiflow.execution.plan_interpreter',
    'demiflow.execution.recovery_contract',
    'demiflow.execution.recovery_identity',
    'demiflow.execution.reservation',
    'demiflow.execution.runtime_limits',
    'demiflow.execution.sealed_plans',
}


def _modules():
    found = {}
    for path in sorted(PACKAGE.rglob('*.py')):
        parts = path.relative_to(REPO).with_suffix('').parts
        if '_vendor' in parts:
            continue
        name = '.'.join(parts[:-1] if parts[-1] == '__init__' else parts)
        found[name] = path
    return found


MODULES = _modules()


def _package(module):
    parts = module.split('.')
    if len(parts) == 1 or not (PACKAGE / parts[1]).is_dir():
        return 'root'
    return parts[1]


def _imports():
    """Yield (importer module, imported module, imported name or '<module>')."""
    for module, path in MODULES.items():
        tree = ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
        is_package = path.name == '__init__.py'
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                if node.level:
                    base = module.split('.') if is_package else module.split('.')[:-1]
                    base = base[:len(base) - (node.level - 1)]
                    target = '.'.join(base + ([node.module] if node.module else []))
                else:
                    target = node.module or ''
                if target.split('.')[0] != 'demiflow':
                    continue
                for alias in node.names:
                    submodule = target + '.' + alias.name
                    if submodule in MODULES:
                        yield module, submodule, '<module>'
                    else:
                        yield module, target, alias.name
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.split('.')[0] == 'demiflow':
                        yield module, alias.name, '<module>'


IMPORTS = sorted(set(_imports()))


def _lines(items):
    return '\n  ' + '\n  '.join(sorted(map(repr, items)))


def test_scan_covers_the_repository():
    assert len(MODULES) > 400 and len(IMPORTS) > 2000
    assert {'execution', 'data', 'planning', 'workbench', 'forge', 'resources'} <= {_package(m) for m in MODULES}


def test_base_never_imports_hosts_and_forge_never_imports_workbench():
    offenders = set()
    for importer, imported, _ in IMPORTS:
        source, target = _package(importer), _package(imported)
        if target not in HOSTS or source == target:
            continue
        if source == 'workbench' and target == 'forge':
            continue  # the workbench may reuse Forge
        if importer in ASSEMBLY:
            continue
        offenders.add((importer, imported))
    assert not offenders, 'Dependency direction violated:' + _lines(offenders)


def test_leaf_packages_import_no_other_package():
    offenders = {(importer, imported) for importer, imported, _ in IMPORTS
                 if _package(importer) in LEAVES and _package(imported) != _package(importer)}
    assert not offenders, 'Leaf package imports another demiflow package:' + _lines(offenders)


def test_cross_package_private_imports_do_not_grow():
    found = defaultdict(set)
    for importer, imported, name in IMPORTS:
        source, target = _package(importer), _package(imported)
        if source != target and name.startswith('_') and not name.startswith('__'):
            found[(source, target)].add((importer, imported, name))
    over = {pair: items for pair, items in found.items()
            if len(items) > PRIVATE_IMPORT_BUDGET.get(pair, 0)}
    assert not over, 'Private names of another package imported beyond budget:' + ''.join(
        f'\n {pair}: {len(items)} > {PRIVATE_IMPORT_BUDGET.get(pair, 0)}' + _lines(items)
        for pair, items in sorted(over.items()))
    assert sum(len(items) for items in found.values()) <= sum(PRIVATE_IMPORT_BUDGET.values())


def test_pre_v3_modules_gain_no_importers():
    found = defaultdict(set)
    for importer, imported, _ in IMPORTS:
        if imported in PRE_V3_MODULES and importer != imported:
            found[imported].add(importer)
    new = {(module, importer) for module, importers in found.items()
           for importer in importers - PRE_V3_MODULES[module]}
    assert not new, ('New code imports a pre-v3 execution module. Use the runtime-v3 entry, or '
                     'add the importer to PRE_V3_MODULES on purpose:' + _lines(new))
    missing = {module for module in PRE_V3_MODULES if module not in MODULES}
    assert not missing, 'Listed pre-v3 modules no longer exist; delete them here:' + _lines(missing)


def test_plan_compiler_learns_no_new_runtime_modules():
    used = {imported for importer, imported, _ in IMPORTS
            if _package(importer) == 'planning' and _package(imported) == 'execution'}
    new = used - PLANNING_MAY_IMPORT
    assert not new, ('planning imports an execution module it did not import before. Register the '
                     'kind in planning/dataflow_registry.py instead of importing its implementation:' + _lines(new))
    stale = PLANNING_MAY_IMPORT - used
    assert not stale, ('planning no longer imports these execution modules. Remove them from '
                       'PLANNING_MAY_IMPORT so that they cannot come back unnoticed:' + _lines(stale))


def test_budgets_are_not_stale_by_more_than_they_help():
    """Report-only guard: a budget far above reality stops protecting anything."""
    found = Counter()
    for importer, imported, name in IMPORTS:
        source, target = _package(importer), _package(imported)
        if source != target and name.startswith('_') and not name.startswith('__'):
            found[(source, target)] += 1
    slack = {pair: (budget, found[pair]) for pair, budget in PRIVATE_IMPORT_BUDGET.items()
             if budget - found[pair] > 3}
    assert not slack, 'Lower these private-import budgets to the current count:' + _lines(slack.items())
