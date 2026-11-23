"""The kind matrix of a loaded engine: every decision that depends on a kind name.

For every kind name (every registered operator, source and terminal, every
kernel kind, and a few names that are none of these) this evaluates each
decision the compiler and the runtime make by kind, using the engine's own
objects and functions. Run it once under the baseline tree and once under a
changed tree and compare the two files: they must be equal cell for cell.

    python tests/compile_contract_matrix.py out.json
    python tests/compile_contract_matrix.py compare base.json changed.json

Unlike ``compile_contract_inventory`` this imports the engine, so it shows what
is loaded, not what the source says.
"""
import json
import sys
from pathlib import Path
from types import SimpleNamespace

UNKNOWN = ('', 'not_a_kind', 'mapop', 'MAPOP', 'Map', 'take ', 'source', 'transform')


def _layout_preserving(kernels):
    try:
        from demiflow.execution.contracts.dataflow_kinds import kinds_with
    except ImportError:
        # Before the kind table: the expression written in worker/terminal_blocks.py.
        return kernels._UNARY_KINDS | kernels._BATCH_KINDS | {'RepartitionOp', 'RandomShuffleOp', 'RandomizeBlockOrderOp'}
    return kinds_with('keeps_layout')


def matrix():
    import demiflow
    from demiflow.execution.kernels import batch_membership as dataflow_batch_membership
    from demiflow.execution.seal import plan as dataflow_seal
    from demiflow.execution.kernels import dataflow as kernels, operator, task_scope
    from demiflow.planning import dataflow_compile as compiler, dataflow_registry as registry
    operators, sources, terminals = (registry.operator_registry(), registry.source_registry(),
                                     registry.terminal_registry())
    preserving = _layout_preserving(kernels)
    names = sorted(set(operators) | set(sources) | set(terminals) | set(kernels.KERNEL_KINDS)
                   | set(operator._INSTANCE_KINDS) | set(preserving) | set(compiler._TRAITS)
                   | set(compiler._NATIVE_TRAITS) | set(UNKNOWN))

    def port(name, kind):
        value = compiler._input_port(name, kind)
        return [value.name, value.requirement, value.ordering]

    def sealable(node_kind, kind):
        return dataflow_seal._builtin_node(SimpleNamespace(kind=node_kind, kernel=SimpleNamespace(kind=kind, configuration={})))

    def definition(table, kind):
        value = table.get(kind)
        if value is None:
            return None
        return {name: (list(item) if isinstance(item, tuple) else item) for name, item in vars(value).items()}

    cells = {}
    for kind in names:
        cells[kind] = {
            'operator': definition(operators, kind), 'source': definition(sources, kind),
            'terminal': definition(terminals, kind),
            'typed_traits': list(compiler._TRAITS[kind]) if kind in compiler._TRAITS else None,
            'native_traits': list(compiler._NATIVE_TRAITS[kind]) if kind in compiler._NATIVE_TRAITS else None,
            'port_main': port('main', kind), 'port_prefix': port('prefix', kind), 'port_left': port('left', kind),
            'admitted': kind in kernels.KERNEL_KINDS,
            'unary': kind in kernels._UNARY_KINDS, 'batch': kind in kernels._BATCH_KINDS,
            'model': kind in kernels._MODEL_KINDS, 'relational': kind in kernels._RELATIONAL_KINDS,
            'needs_instance': kind in operator._INSTANCE_KINDS,
            'row_call': kind in kernels._UNARY_KINDS or (
                kind in kernels._MODEL_KINDS and kind not in operator._BATCH_INSTANCE_KINDS),
            'batch_call': kind in operator._BATCH_INSTANCE_KINDS,
            'stream_group': kind in kernels._STREAM_GROUP_KINDS,
            'ordering_finish': kind in kernels._ORDERING_KINDS,
            'aggregate_finish': kind in kernels._AGGREGATE_KINDS,
            'native_batch_membership': kind in dataflow_batch_membership.KINDS,
            'layout_preserving': kind in preserving,
            'task_scoped': kind in task_scope.TASK_KINDS,
            'sealable_as': {node_kind: sealable(node_kind, kind)
                            for node_kind in ('source', 'transform', 'relational', 'terminal')},
        }
    return {
        'engine': str(Path(demiflow.__file__).resolve().parents[1]),
        'order': {'operators': list(operators), 'sources': list(sources), 'terminals': list(terminals),
                  'typed_traits': list(compiler._TRAITS), 'native_traits': list(compiler._NATIVE_TRAITS)},
        'types': {name: type(value).__name__ for name, value in {
            'KERNEL_KINDS': kernels.KERNEL_KINDS, '_UNARY_KINDS': kernels._UNARY_KINDS,
            '_INSTANCE_KINDS': operator._INSTANCE_KINDS, '_BATCH_INSTANCE_KINDS': operator._BATCH_INSTANCE_KINDS,
            'TASK_KINDS': task_scope.TASK_KINDS, 'KINDS': dataflow_batch_membership.KINDS,
            '_TRAITS': compiler._TRAITS, '_NATIVE_TRAITS': compiler._NATIVE_TRAITS,
            '_FINITE_COMPLETE_KINDS': compiler._FINITE_COMPLETE_KINDS, '_ORDERING_KINDS': compiler._ORDERING_KINDS,
            'operator_registry': operators}.items()},
        'cells': cells,
    }


def compare(left, right):
    found = []
    for section in ('order', 'types'):
        if left[section] != right[section]:
            found.append((section, None, None))
    for kind in sorted(set(left['cells']) | set(right['cells'])):
        a, b = left['cells'].get(kind), right['cells'].get(kind)
        if a is None or b is None:
            found.append(('cells', kind, 'present on one side only'))
            continue
        for name in sorted(set(a) | set(b)):
            if a.get(name) != b.get(name):
                found.append(('cells', kind, name))
    return found


if __name__ == '__main__':
    if sys.argv[1] == 'compare':
        left, right = (json.loads(Path(path).read_text(encoding='utf-8')) for path in sys.argv[2:4])
        found = compare(left, right)
        cells = sum(len(row) for row in left['cells'].values())
        print('left ', left['engine'])
        print('right', right['engine'])
        print(len(left['cells']), 'kind names,', cells, 'cells,', len(found), 'differ')
        for item in found[:60]:
            print('  ', *item)
        sys.exit(1 if found else 0)
    Path(sys.argv[1]).write_text(json.dumps(matrix(), indent=1, sort_keys=True) + '\n', encoding='utf-8')
