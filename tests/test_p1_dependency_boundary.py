"""P1's completed boundaries and explicit retained native/legacy dependencies.

The inventory is a per-symbol, per-function upper bound. It is deliberately not
an exemption for entire modules and does not claim retained actors are pure.
"""
import ast
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INVENTORY = json.loads((ROOT / 'tests/fixtures/p1-sdk-retained-imports.json').read_text())


def imports(path):
    found = set()
    def walk(node, scopes=()):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            scopes = (*scopes, node.name)
        if isinstance(node, ast.ImportFrom):
            if node.level == 2:
                module = 'demiflow.' + (node.module or '')
            elif node.level == 1:
                module = 'demiflow.' + path.parts[-2] + '.' + (node.module or '')
            else:
                module = node.module or ''
            for alias in node.names:
                found.add(('.'.join(scopes), module, alias.name))
        elif isinstance(node, ast.Import):
            for alias in node.names:
                found.add(('.'.join(scopes), alias.name, '<module>'))
        for child in ast.iter_child_nodes(node):
            walk(child, scopes)
    walk(ast.parse(path.read_text()))
    return found


def test_sdk_retained_runtime_dependencies_do_not_expand():
    for filename, allowed in INVENTORY.items():
        actual = {entry for entry in imports(ROOT / filename)
                  if entry[1] == 'demiflow.execution' or entry[1].startswith('demiflow.execution.')}
        assert actual <= {tuple(entry) for entry in allowed}, actual - {tuple(entry) for entry in allowed}
    assert not (ROOT / 'demiflow/execution/native_authoring.py').exists()


def test_execution_rebinding_and_checkpoint_core_do_not_construct_sdk_objects():
    for filename in ['demiflow/execution/pipeline/checked.py', 'demiflow/execution/executors/stream_checkpoint.py',
                     'demiflow/execution/sources/checkpoint_read.py',
                     'demiflow/data/relational_declaration.py']:
        found = imports(ROOT / filename)
        core = {entry for entry in found if not (filename.endswith('/executors/stream_checkpoint.py') and entry[0] == 'read_checkpoint')}
        assert not {entry for entry in core if entry[1] in {'demiflow.data.api', 'demiflow.data.dataset'}}, core
    # The canonical public checkpoint function is the sole assembly exception.
    assembly = {entry for entry in imports(ROOT / 'demiflow/execution/executors/stream_checkpoint.py')
                if entry[1] in {'demiflow.data.api', 'demiflow.data.dataset'}}
    assert assembly <= {
        ('read_checkpoint', 'demiflow.data.api', '_select_executor'),
        ('read_checkpoint', 'demiflow.data.dataset', 'Dataset'),
        ('read_checkpoint', 'demiflow.data.dataset', 'MaterializedDataset'),
    }, assembly


def test_single_executor_context_and_no_reverse_assembly_import_for_context():
    definitions = []
    for path in (ROOT / 'demiflow').rglob('*.py'):
        if '_vendor' in path.parts:
            continue
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if (isinstance(node, ast.Call) and node.args
                    and isinstance(node.args[0], ast.Constant)
                    and node.args[0].value == 'demiflow_dataset_executor'):
                definitions.append(str(path.relative_to(ROOT)))
        if path.is_relative_to(ROOT / 'demiflow/execution') or path.is_relative_to(ROOT / 'demiflow/pipeline'):
            assert not {entry for entry in imports(path)
                        if entry[1] == 'demiflow.data.api' and entry[2] in {'_current_executor', '_use_executor'}}
    assert definitions == ['demiflow/data/execution_contract.py']


def test_retired_actions_have_no_executor_definitions_and_graph_remains():
    retired = {'count', 'take', 'take_batch', 'aggregate', 'iter_rows', 'iter_batches',
               'materialize', 'materialize_stream', 'size_bytes', 'write_datasink',
               'write_lance', 'write_file', 'from_items', 'from_arrow', 'from_numpy', 'from_pandas'}
    for name in ['base', 'local', 'ray']:
        tree = ast.parse((ROOT / f'demiflow/execution/executors/{name}.py').read_text())
        cls = next(node for node in tree.body if isinstance(node, ast.ClassDef)
                   and node.name.endswith('DatasetExecutor'))
        methods = {node.name for node in cls.body if isinstance(node, ast.FunctionDef)}
        assert not methods & retired
        if name != 'base':
            assert 'materialize_graph' in methods
