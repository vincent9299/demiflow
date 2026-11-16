"""Worker-local shared actor ownership authenticated by the original graph."""
import functools
import inspect
from collections.abc import Mapping
from .contracts.dataflow_errors import DataflowError


def actor_owner(target):
    # Passive exact wrappers only. A factory or function is a declaration, not
    # an already constructed actor, even when reused at several occurrences.
    seen = set()
    while id(target) not in seen:
        seen.add(id(target))
        if type(target) is functools.partial:
            target = target.func
        elif inspect.ismethodwrapper(target) and target.__name__ == '__call__' and type(target.__self__) is functools.partial:
            target = target.__self__
        elif inspect.ismethod(target):
            target = target.__self__
        else:
            return None if inspect.isroutine(target) or inspect.isclass(target) else target
    raise DataflowError('INVALID_CONTRACT', 'Cyclic shared actor wrapper')


def shared_binding(configuration, occurrence):
    value = configuration.get('shared_actor')
    if value is None:
        return None
    if (not isinstance(value, Mapping) or set(value) != {'owner', 'members', 'state_owner'}
            or not isinstance(value['members'], (tuple, list)) or len(value['members']) < 2
            or any(type(o) is not str for o in value['members'])
            or len(set(value['members'])) != len(value['members'])
            or occurrence not in value['members'] or value['owner'] != value['members'][0]
            or value['state_owner'] != value['members'][-1]):
        raise DataflowError('INVALID_CONTRACT', 'Invalid shared actor occurrence binding')
    return {'owner': value['owner'], 'members': list(value['members']), 'state_owner': value['state_owner']}


class SharedActor:
    """One real lifecycle/event loop, distinct per-occurrence call adapters."""
    def __init__(self, binding, owner):
        self.binding, self.owner = binding, owner
        self.base = None
        self.active = set()
        self.restored = set()

    def acquire(self, occurrence, target, services, *, timeout, await_calls, lifecycle):
        from .kernels.async_instance import AsyncInstance
        if occurrence in self.active:
            raise DataflowError('INVALID_CONTRACT', 'Shared actor occurrence acquired twice')
        if self.base is None:
            self.base = AsyncInstance(self.owner, services, owner=self.owner, lifecycle=lifecycle)
        self.active.add(occurrence)
        view = AsyncInstance(target, services, owner=self.owner, timeout=timeout, await_calls=await_calls)
        view._shared = self
        view._shared_occurrence = occurrence
        view._stopping = self.base._stopping
        return view

    def release(self, occurrence):
        self.active.discard(occurrence)
        if not self.active and self.base is not None:
            self.base.close()

    def restoring(self, occurrence, state):
        if occurrence in self.restored or not isinstance(state, Mapping):
            raise DataflowError('INCOMPATIBLE_HISTORY', 'Shared actor restore membership is missing or duplicated')
        if occurrence == self.binding['state_owner']:
            if state.get('mode') == 'shared_owner':
                raise DataflowError('INCOMPATIBLE_HISTORY', 'Shared actor authority image is absent')
        elif state != {'mode': 'shared_owner', 'owner': self.binding['state_owner']}:
            raise DataflowError('INCOMPATIBLE_HISTORY', 'Shared actor alias differs from its sealed authority')
        self.restored.add(occurrence)
        return occurrence == self.binding['state_owner']


def shared_actors(specs, programs, assignment):
    groups = {}
    for occ, spec in specs.items():
        binding = shared_binding(spec.configuration, occ)
        if binding is None:
            continue
        if not set(binding['members']) <= set(specs) or len(assignment.lease.partition_ids) != 1:
            raise DataflowError('INVALID_CONTRACT', 'Shared actor group is split between worker owners')
        owner = actor_owner(programs[occ].value.target)
        group = groups.setdefault(binding['owner'], SharedActor(binding, owner))
        if group.binding != binding or owner is None or group.owner is not owner:
            raise DataflowError('CORRUPT_ARTIFACT', 'Shared program lost its actual actor identity')
    return {occ: group for group in groups.values() for occ in group.binding['members']}


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
    from ..data.plan import CallableSpec
    from ..execution.local_kernel import KeyedSource, UnionSource
    from .contracts.dataflow import NativeOperation
    from ..planning.dataflow_compile import expand_relational_inputs, expand_operation_inputs, _hash
    from .dataflow_model_factory import native_model_declaration
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
                    from .lance_key_source import LanceKeySource
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
