"""Program identity implementation."""
from __future__ import annotations

from demiflow.data.plan import actor_owner
from demiflow.execution.contracts.dataflow import shared_binding
from demiflow.execution.contracts.dataflow_errors import DataflowError

from pathlib import Path
from demiflow.execution.contracts.artifact import digest


def file_record(path):
    path = Path(path).resolve()
    return {"path": str(path), "sha256": digest(path.read_bytes())}


def validate_shared_actor_binding(request, graph, *, bounds):
    """Compare current recipe aliases only for plans declaring this contract.

    This traverses declarations without compiling schemas, instantiating actors or opening sources.
    It never updates historical graph/program bytes or their identity algorithms.
    Use the original physical bounds, before starting any replay worker.
    """
    marked = [n for n in graph.nodes if 'actor_binding_version' in n.kernel.configuration]
    if not marked:
        return  # Old plans retain their original interpretation.
    def relation(nodes):
        result = {}
        for node in nodes:
            config = node.kernel.configuration
            if 'actor_binding_version' not in config:
                continue
            if type(config['actor_binding_version']) is not int or config['actor_binding_version'] != 1:
                raise DataflowError('INCOMPATIBLE_HISTORY', 'Unsupported actor ownership binding version')
            binding = shared_binding(config, node.occurrence_id)
            result[node.occurrence_id] = frozenset(binding['members']) if binding else frozenset((node.occurrence_id,))
        return result
    original = relation(graph.nodes)
    if _recipe_actor_relation(request, bounds=bounds) != original:
        raise DataflowError('INCOMPATIBLE_HISTORY', 'Actor instance sharing differs from the original sealed plan')

def _recipe_actor_relation(request, *, bounds):
    """Replay only the compiler's occurrence preorder, never its source facts.

    Leaf sources contribute one position without touching rows, schema hints,
    SQL connections, files, reader factories or serialized programs. Relational
    and extra inputs reuse the original public structural expansion functions.
    """
    from dataclasses import replace
    from demiflow.data.plan import CallableSpec
    from demiflow.data.sources import KeyedSource, UnionSource
    from demiflow.execution.contracts.dataflow import NativeOperation
    from demiflow.planning.dataflow_compile import expand_relational_inputs, expand_operation_inputs, _hash
    from demiflow.execution.dataflow_model_factory import native_model_declaration
    if (request.kind == 'take' and request.arguments.get('limit') == 0
            or request.kind == 'take_batch' and request.arguments.get('batch_size') == 0):
        return {}
    sources, operations, visiting, owners, retained = {}, {}, set(), {}, []
    next_index = 0
    def add(spec=None):
        nonlocal next_index
        if next_index >= bounds.max_nodes:
            raise DataflowError('BUDGET_EXCEEDED', 'Actor relation exceeds original node budget')
        occurrence = _hash(['dataflow-occurrence-v3', request.action.action_id, next_index])
        next_index += 1
        owner = actor_owner(spec.target) if isinstance(spec, CallableSpec) else None
        if owner is not None:
            # Keep declaration owners alive while using object identity as the
            # alias key; structural adapters may create temporary wrappers.
            retained.append(owner)
            owners.setdefault(id(owner), []).append(occurrence)
        return occurrence
    def source_node(source, path):
        key = id(source)
        if key in sources:
            return sources[key]
        if key in visiting:
            raise DataflowError('INVALID_CONTRACT', 'Cyclic source declaration during actor admission')
        visiting.add(key)
        reducer = None
        if isinstance(source, (KeyedSource, UnionSource)):
            for item in expand_relational_inputs(source, path=path, bounds=bounds):
                selected = item.source
                if isinstance(source, KeyedSource) and source.kind == 'exclude_keys' and item.port == 'left':
                    from demiflow.execution.lance_key_source import LanceKeySource
                    selected = LanceKeySource(replace(item.source.query, columns=tuple(source.on)))
                    retained.append(selected)
                chain(selected, item.plan, item.path)
            if isinstance(source, KeyedSource) and source.kind == 'reduce':
                reducer = CallableSpec.create(source.reducer)
        occurrence = add(reducer)
        visiting.remove(key)
        sources[key] = occurrence
        return occurrence
    def chain(source, plan, path):
        if len(path) > bounds.max_depth:
            raise DataflowError('BUDGET_EXCEEDED', 'Actor relation exceeds original graph depth')
        occurrence = source_node(source, path)
        for operation in plan.operations:
            key = (id(source), id(operation))
            if key in operations:
                occurrence = operations[key]
                continue
            spec = getattr(operation, 'callable', None)
            native = native_model_declaration(operation)
            if native is not None:
                spec = native
            if isinstance(operation, NativeOperation):
                spec = operation.factory
            occurrence = add(spec)
            for item in expand_operation_inputs(operation, path=path, bounds=bounds):
                chain(item.source, item.plan, item.path)
            operations[key] = occurrence
        return occurrence
    chain(request.source, request.plan, ())
    return {occurrence: frozenset(members) for members in owners.values() for occurrence in members}
