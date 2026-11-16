"""Internal graph_spec adapter: real v3 occurrences, partitions and observations.

No scheduling, journal, source reader or commit implementation lives here. The
compiler hooks specialize a normal captured UnionSource before its only seal.
"""
from dataclasses import dataclass, replace
import inspect
import re
import time

from ..data.plan import CallableSpec, LogicalPlan, MapOp, MapStreamOptions
from ..execution.local_kernel import PlanInput, UnionSource
from ..execution.contracts.dataflow_errors import DataflowError


@dataclass(frozen=True)
class RoutedGraphSource(UnionSource):
    declaration: dict
    shared_source: object
    ancestor: LogicalPlan
    router: MapOp
    target_ends: tuple


def _identity(row):
    return row


def _unwrap(row):
    return row['graph_payload']


def _tag(row, *, reader):
    return {'reader_id': reader, 'row': row}


class _ClassTarget:
    """Declared class aliases use the original kernel snapshot authority."""
    def __init__(self, field, classes):
        self._field, self._classes, self._ordinal = field, classes, 0

    def __call__(self, row):
        value = row[self._field]
        if type(value) is not str or value not in self._classes:
            raise DataflowError('INVALID_CONTRACT', 'undeclared graph target class')
        readers = self._classes[value]
        selected = readers[self._ordinal % len(readers)]
        self._ordinal += 1
        return {'graph_reader': selected, 'graph_payload': row}

    def snapshot_state(self):
        return {'ordinal': self._ordinal}

    def restore_state(self, state):
        if type(state) is not dict or set(state) != {'ordinal'} or type(state['ordinal']) is not int or state['ordinal'] < 0:
            raise DataflowError('INCOMPATIBLE_HISTORY', 'invalid graph target ordinal')
        self._ordinal = state['ordinal']


def _map(target, **options):
    return MapOp(CallableSpec.create(target, **options),
                 stream_options=MapStreamOptions(concurrency=1))


def _synchronous_target(operation):
    """No user construction/call: admit only a plain synchronous map function."""
    if type(operation) is not MapOp:
        return False
    spec = operation.callable
    fn = spec.target
    if (not inspect.isfunction(fn) or inspect.iscoroutinefunction(fn)
            or inspect.isasyncgenfunction(fn) or inspect.isgeneratorfunction(fn)
            or vars(fn) or spec.constructor_args or spec.constructor_kwargs):
        return False
    options = operation.stream_options
    return options is None or (options.concurrency == 1 and options.execution == 'inline'
                               and options.callable_scope == 'stage')


def translate_graph(graph_spec):
    """Build ordinary real branches without invoking any user declaration."""
    if type(graph_spec) is not dict:
        raise DataflowError('INVALID_CONTRACT', 'graph_spec must be an object')
    source, ancestor, targets = graph_spec['source'], graph_spec['ancestor']['plan'], graph_spec['targets']
    if type(ancestor) is not LogicalPlan or type(targets) not in (list, tuple) or not 1 <= len(targets) <= 4096:
        raise DataflowError('INVALID_CONTRACT', 'graph needs a bounded target list and an ancestor plan')
    readers = tuple(target['reader_id'] for target in targets)
    if (any(type(reader) is not str or re.fullmatch(r'[A-Za-z0-9_.-]{1,128}', reader) is None for reader in readers)
            or len(set(readers)) != len(readers)):
        raise DataflowError('INVALID_CONTRACT', 'graph reader IDs must be unique native identifiers')
    route = {'fan_out': 'rebalance', 'fan_in': 'forward'}.get(graph_spec.get('routing'), graph_spec.get('routing', 'broadcast'))
    if route not in {'broadcast', 'rebalance', 'hash', 'target', 'forward'}:
        raise DataflowError('INVALID_CONTRACT', 'unknown graph routing')
    if route == 'forward' and len(readers) != 1:
        raise DataflowError('INVALID_CONTRACT', 'forward graph routing requires one target')
    mode = graph_spec.get('exchange_mode', 'memory_incremental')
    if mode not in {'memory_incremental', 'durable_incremental', 'finite_sealed'}:
        raise DataflowError('INVALID_CONTRACT', 'unknown graph exchange mode')
    keys = graph_spec.get('key_fields') or ()
    if route == 'hash' and (type(keys) not in (list, tuple) or not keys or any(type(key) is not str or not key for key in keys)):
        raise DataflowError('INVALID_CONTRACT', 'hash graph requires declared key fields')
    declaration = {key: graph_spec[key] for key in ('source_id', 'credit_budget', 'prefetch_slots', 'cut_rows', 'queue_depth') if key in graph_spec}
    declaration.update(routing=route, exchange_mode=mode, reader_ids=list(readers), key_fields=list(keys))
    prefix = tuple(ancestor.operations)
    if route == 'target':
        classes, field = graph_spec.get('target_classes'), graph_spec.get('target_field')
        if type(field) is not str or not field or type(classes) is not dict or not classes:
            raise DataflowError('INVALID_CONTRACT', 'target graph requires a field and declared classes')
        covered = []
        for name, members in classes.items():
            if type(name) is not str or not name or type(members) not in (list, tuple) or not members:
                raise DataflowError('INVALID_CONTRACT', 'invalid graph target class')
            covered.extend(members)
        if len(covered) != len(readers) or set(covered) != set(readers):
            raise DataflowError('INVALID_CONTRACT', 'each graph target must belong to one class')
        classes = {name: list(members) for name, members in classes.items()}
        prefix += (_map(_ClassTarget, fn_constructor_args=(field, classes)),)
        declaration.update(target_field=field, target_classes=classes)
    router = _map(_unwrap if route == 'target' else _identity)
    prefix += (router,)
    branches, ends = [], []
    for reader, target in zip(readers, targets):
        plan = target['plan']
        if type(plan) is not LogicalPlan:
            raise DataflowError('INVALID_CONTRACT', 'graph target must be a LogicalPlan')
        # One reused plan object still declares independent target actors. Only
        # the ancestor and routing operations intentionally share construction.
        operations = tuple(replace(operation) for operation in plan.operations)
        # The platform tag is one synchronous dictionary operation; it does not
        # need a streaming actor, pending-call queue or background lifecycle.
        tag = MapOp(CallableSpec.create(_tag, fn_kwargs={'reader': reader}))
        branches.append(PlanInput(source, LogicalPlan(prefix + operations + (tag,))))
        ends.append((reader, operations[0] if operations else tag, operations[-1] if operations else router))
    return RoutedGraphSource(tuple(branches), declaration, source, ancestor, router, tuple(ends))


def decorate_graph_capture(capture, request, *, terminal):
    """Bind routing and branch identity into the single graph before dag_id."""
    source = request.source
    if type(source) is not RoutedGraphSource:
        return terminal
    from ..execution.recovery_contract import identity
    from .dataflow_compile import _plain
    def node_index(op):
        return capture.alias_op[(id(source.shared_source), id(op))]
    router = node_index(source.router)
    ancestor = (node_index(source.ancestor.operations[-1]) if source.ancestor.operations
                else capture.alias_source[id(source.shared_source)])
    targets = {}
    def mark(index, marker):
        node = capture.nodes[index]
        configuration = {**_plain(node.kernel.configuration), **marker}
        capture.nodes[index] = replace(node,
            semantic_id=identity(['graph-adapter-v3', node.semantic_id, marker]),
            kernel=replace(node.kernel, configuration=configuration, mergeable=False))
    mark(ancestor, {'graph_observe': True})
    for (reader, first, last), branch in zip(source.target_ends, source.inputs):
        first_index, end_index = node_index(first), node_index(last)
        mark(first_index, {'graph_reader': reader})
        mark(end_index, {'graph_observe': True})
        targets[reader] = {'occurrence_id': capture.nodes[end_index].occurrence_id,
                           'partition_id': reader if last is source.router else None}
        # This tag was created above by this adapter, not inferred from an
        # arbitrary user map's field names. Retain its original occurrence and
        # program; only the newly compiled physical owner may be co-located.
        tag = branch.plan.operations[-1]
        if type(tag) is not MapOp or tag.callable.target is not _tag:
            raise DataflowError('INVALID_CONTRACT', 'graph branch lacks its platform tag')
        mark(node_index(tag), {'graph_private_tag': {
            'reader': reader, 'producer': capture.nodes[end_index].occurrence_id,
            'synchronous_target': last is not source.router and _synchronous_target(last)}})
    mark(router, {'graph_route': source.declaration})
    terminal = next(i for i, node in enumerate(capture.nodes) if node.kind == 'terminal')
    mark(terminal, {'graph_result': {'ancestor': capture.nodes[ancestor].occurrence_id,
                                   'targets': targets, 'router': capture.nodes[router].occurrence_id}})
    # The internal UnionSource only captures branch structure. Materialize can
    # consume its existing named input ports directly; no extra union process,
    # intermediate publication or second terminal is needed.
    merge = capture.alias_source[id(source)]
    if terminal != len(capture.nodes) - 1 or merge != terminal - 1:
        raise DataflowError('INVALID_CONTRACT', 'graph terminal must directly consume the captured union')
    union, output = capture.nodes[merge], capture.nodes[terminal]
    config = _plain(output.kernel.configuration)
    config['occurrence_id'] = union.occurrence_id
    capture.nodes[merge] = replace(output, occurrence_id=union.occurrence_id,
        shared_token=union.shared_token,
        kernel=replace(output.kernel, inputs=union.kernel.inputs, configuration=config))
    capture.nodes.pop()
    capture.edges = [edge for edge in capture.edges if edge[2] != terminal]
    return merge


def specialize_graph_stages(graph, stages, *, policy, bounds):
    routers = {node.occurrence_id: node.kernel.configuration['graph_route']
               for node in graph.nodes if 'graph_route' in node.kernel.configuration}
    if not routers:
        return stages
    result = []
    for stage in stages:
        names = [occ for occ in stage.occurrences if occ in routers]
        if not names:
            result.append(stage)
            continue
        if len(stage.occurrences) != 1:
            raise DataflowError('INVALID_CONTRACT', 'graph router may not be fused')
        readers = tuple(routers[names[0]]['reader_ids'])
        if len(readers) > min(bounds.max_partitions, policy.max_replicas):
            raise DataflowError('RESOURCE_INFEASIBLE', 'graph readers exceed admitted replica ceiling')
        result.append(replace(stage, partitions=readers, replica_min=1,
                              replica_target=len(readers), replica_max=len(readers)))
    return _colocate_private_tags(graph, tuple(result), policy=policy, bounds=bounds)


def _colocate_private_tags(graph, stages, *, policy, bounds):
    """Run each private tag after its own plain target in the same worker.

    This does not merge reader branches, router partitions, or explicit graph
    exchanges. Both kernels and their full live memory/output/state windows
    remain in the sealed stage. One CPU executes the two synchronous functions
    sequentially; actor/async/concurrent or resource-constrained targets retain
    their original independent owner.
    """
    if not policy.fusion:
        return stages
    nodes = {node.occurrence_id: node for node in graph.nodes}
    owners = {occ: stage for stage in stages for occ in stage.occurrences}
    incoming, outgoing = {}, {}
    for edge in graph.edges:
        incoming.setdefault(edge.consumer, []).append(edge)
        outgoing.setdefault(edge.producer, []).append(edge)
    merged, removed = {}, set()
    for node in graph.nodes:
        marker = node.kernel.configuration.get('graph_private_tag')
        if marker is None or not marker['synchronous_target']:
            continue
        if node.kernel.configuration.get('streaming_map'):
            continue
        producer = marker['producer']
        head, tail = owners[producer], owners[node.occurrence_id]
        links = incoming.get(node.occurrence_id, ())
        if (head.stage_id == tail.stage_id or head.stage_id in merged
                or len(links) != 1 or links[0].producer != producer
                or len(outgoing.get(producer, ())) != 1
                or head.occurrences != (producer,) or tail.occurrences != (node.occurrence_id,)
                or len(head.partitions) != 1 or len(tail.partitions) != 1):
            continue
        kernels = (nodes[producer].kernel, node.kernel)
        if any(k.kind != 'MapOp' or k.state != 'none' or k.effect != 'pure'
               or len(k.inputs) != 1 or k.inputs[0].requirement != 'incremental'
               or k.configuration.get('concurrency', 1) != 1
               or k.configuration.get('execution', 'inline') != 'inline'
               or 'graph_route' in k.configuration for k in kernels):
            continue
        if any(s.profile.limits or s.profile.requirements
               or set(s.profile.requests) != {'cpu_millis', 'memory_bytes'} for s in (head, tail)):
            continue
        resident = sum(max(s.resident_bytes, s.profile.requests['memory_bytes']) for s in (head, tail))
        control = max(1, min(bounds.max_control_bytes, bounds.max_resident_bytes // 8))
        if resident + control > bounds.max_resident_bytes:
            continue
        from ..resources.contracts import ResourceProfile
        profile = ResourceProfile(requests={
            'cpu_millis': max(s.profile.requests['cpu_millis'] for s in (head, tail)),
            'memory_bytes': resident})
        merged[head.stage_id] = replace(head,
            occurrences=head.occurrences + tail.occurrences, profile=profile,
            resident_bytes=resident, output_bytes=head.output_bytes + tail.output_bytes,
            state_bytes=head.state_bytes + tail.state_bytes,
            kernel_refs=tuple(dict.fromkeys(head.kernel_refs + tail.kernel_refs)))
        removed.add(tail.stage_id)
    return tuple(merged.get(stage.stage_id, stage) for stage in stages if stage.stage_id not in removed)


def graph_credit(declaration, *, bounds):
    from ..execution.reservation import resolve_prefetch_credit
    credit = dict(declaration.get('credit_budget', {'rows': 64, 'bytes': 8 * 1024**2}))
    slots = declaration.get('prefetch_slots')
    solved = resolve_prefetch_credit(slots,
        inflight_bytes=max(bounds.max_row_bytes + 256, bounds.max_batch_bytes),
        credit_budget=credit, credit_explicit='credit_budget' in declaration)
    credit = solved['credit_budget']
    row_limit = bounds.max_pending_events
    byte_limit = min(bounds.max_resident_bytes, max(bounds.max_row_bytes + 256, bounds.max_batch_bytes * 4))
    if 'credit_budget' in declaration and (credit['rows'] > row_limit or credit['bytes'] > byte_limit):
        raise DataflowError('BUDGET_EXCEEDED', 'explicit graph credit exceeds the admitted channel grant')
    return {'rows': min(credit['rows'], row_limit), 'bytes': min(credit['bytes'], byte_limit)}


def specialize_graph_exchanges(graph, stages, exchanges, *, bounds):
    nodes = {node.occurrence_id: node for node in graph.nodes}
    owners = {occ: stage for stage in stages for occ in stage.occurrences}
    edges = {edge.edge_id: edge for edge in graph.edges}
    result = []
    for exchange in exchanges:
        edge = edges[exchange.edge_id]
        incoming = nodes[edge.consumer].kernel.configuration.get('graph_route')
        outgoing = nodes[edge.producer].kernel.configuration.get('graph_route')
        if incoming is None and outgoing is None:
            result.append(exchange)
            continue
        declaration = incoming or outgoing
        credit = graph_credit(declaration, bounds=bounds)
        mode = declaration['exchange_mode']
        updates = {'mode': mode, 'credit_rows': credit['rows'], 'credit_bytes': credit['bytes'],
                   'storage_bytes': 0 if mode == 'memory_incremental' else bounds.max_spill_bytes,
                   'scope_id': 'scope-' + edge.edge_id if mode == 'finite_sealed' else None,
                   'reason': 'explicit graph adapter: original v3 routing and ACK'}
        if incoming is not None:
            route = incoming['routing']
            updates.update(consumer_partitions=owners[edge.consumer].partitions, routing=route,
                           keys=('graph_reader',) if route == 'target' else tuple(incoming['key_fields']) if route == 'hash' else ())
        else:
            reader = nodes[edge.consumer].kernel.configuration.get('graph_reader')
            if reader not in outgoing['reader_ids']:
                raise DataflowError('INVALID_CONTRACT', 'router edge has no declared branch')
            updates.update(producer_partitions=(reader,), consumer_partitions=owners[edge.consumer].partitions,
                           routing='forward', keys=())
        result.append(replace(exchange, **updates))
    return tuple(result)


def observe_graph_progress(execution, event, payload):
    """Read-only first completion evidence after original worker authentication."""
    if payload.get('node_finished') is not True:
        return
    occurrence, partition = payload['occurrence_id'], payload['partition_id']
    node = next(node for node in execution._graph.nodes if node.occurrence_id == occurrence)
    if not node.kernel.configuration.get('graph_observe'):
        return
    observations = getattr(execution, '_graph_finished', None)
    if observations is None:
        observations = execution._graph_finished = {}
    # Missing historical or budget-truncated times remain unavailable.
    if len(observations) < execution._bounds.max_control_bytes // 512:
        observations.setdefault((occurrence, partition), time.monotonic())


def graph_result(execution):
    """Project native counters, first EOI observations and original commit facts."""
    configuration = next(node.kernel.configuration['graph_result'] for node in execution._graph.nodes
                         if 'graph_result' in node.kernel.configuration)
    committed = committed_graph_metrics(execution)
    def rows(occurrence, partition=None):
        values = [value['rows_output'] for (occ, part), value in committed.items()
                  if occ == occurrence and (partition is None or part == partition)]
        return sum(values) if values else None
    def finished(occurrence, partition=None):
        owners = [stage for stage in execution._physical.stages if occurrence in stage.occurrences]
        members = (partition,) if partition is not None else owners[0].partitions
        observations = getattr(execution, '_graph_finished', {})
        values = [observations.get((occurrence, member)) for member in members]
        return max(values) if values and all(value is not None for value in values) else None
    targets = configuration['targets']
    journal = execution.request.action.context.journal
    facts = journal.execution_commits(execution.request.action.action_id)
    return {'workers': execution._observed_worker_count,
            'egress_rows': rows(configuration['ancestor']),
            'target_rows': {reader: rows(binding['occurrence_id'], binding['partition_id']) for reader, binding in targets.items()},
            'final_arrival': {'ancestor': finished(configuration['ancestor']),
                **{reader: finished(binding['occurrence_id'], binding['partition_id']) for reader, binding in targets.items()}},
            'checkpoints': {fact['commit_id']: {'commit': fact} for fact in facts if fact['status'] == 'acked'},
            'result_ref': execution._terminal_result.result_ref,
            'target_bindings': {reader: dict(binding) for reader, binding in targets.items()},
            'dag_id': execution._graph.dag_id, 'plan_id': execution._physical.plan_id}


def validate_graph_replay(request, *, bounds):
    """Validate the original graph declaration and credit without rewriting it."""
    action = request.action
    if action.context.journal.source_binding(action.action_id + '-dataflow-sealed') is None:
        return
    from ..execution.sealed_plans import load_sealed_plan
    from .dataflow_compile import _plain
    bundle = load_sealed_plan(action.context.directory, action.action_id, version='v3')
    graph, physical = bundle['graph'], bundle['physical']
    declaration = request.source.declaration
    originals = [node.kernel.configuration['arguments'].get('graph') for node in graph.nodes if node.kind == 'terminal']
    if [_plain(value) for value in originals] != [declaration]:
        raise DataflowError('INCOMPATIBLE_HISTORY', 'graph declaration differs from the sealed routing')
    credit = graph_credit(declaration, bounds=bounds)
    router = next(node.occurrence_id for node in graph.nodes if 'graph_route' in node.kernel.configuration)
    selected = {edge.edge_id for edge in graph.edges if edge.producer == router or edge.consumer == router}
    for exchange in physical.exchanges:
        if exchange.edge_id in selected and (exchange.credit_rows > credit['rows'] or exchange.credit_bytes > credit['bytes']):
            raise DataflowError('BUDGET_EXCEEDED', 'sealed graph credit exceeds the current admitted grant')


def committed_graph_metrics(execution):
    """Read counters from authenticated final contributions, including resume."""
    import json
    from ..execution.recovery_contract import identity
    from ..execution.contracts.dataflow_codec import compute_participant_id
    image = execution._restore_image
    if image is None or not image.cut.final:
        raise DataflowError('INVALID_CONTRACT', 'graph result has no original final cut')
    owners = {compute_participant_id(stage.stage_id, partition): (stage, partition)
              for stage in execution._physical.stages for partition in stage.partitions}
    wanted = {node.occurrence_id for node in execution._graph.nodes if node.kernel.configuration.get('graph_observe')}
    result = {}
    for contribution in image.contributions:
        stage, partition = owners[contribution.participant_id]
        if not wanted.intersection(stage.occurrences):
            continue
        ref = contribution.operator_states.get(identity(['worker-frontier', partition]))
        if ref is None:
            raise DataflowError('CORRUPT_ARTIFACT', 'graph contribution lacks its worker frontier')
        frontier = json.loads(b''.join(execution._store.read(ref, max_bytes=execution._bounds.max_state_bytes)))
        if frontier.get('schema') != 'demiflow.worker-frontier.v3':
            raise DataflowError('CORRUPT_ARTIFACT', 'graph contribution has an unknown frontier')
        for occurrence in wanted.intersection(stage.occurrences):
            values = frontier.get('metrics', {}).get(occurrence)
            if (type(values) is not dict or set(values) != {'rows_input', 'rows_output'}
                    or any(type(value) is not int or value < 0 for value in values.values())
                    or occurrence not in frontier['nodes']):
                raise DataflowError('CORRUPT_ARTIFACT', 'graph node has no committed completion counters')
            result[(occurrence, partition)] = values
    return result


def tighten_graph_credits(graph, exchanges, *, bounds):
    """Parking may change transport mode, never enlarge a graph input window."""
    declarations = [node.kernel.configuration['graph_route'] for node in graph.nodes
                    if 'graph_route' in node.kernel.configuration]
    if not declarations:
        return exchanges
    credit = graph_credit(declarations[0], bounds=bounds)
    routers = {node.occurrence_id for node in graph.nodes if 'graph_route' in node.kernel.configuration}
    routed = {edge.edge_id for edge in graph.edges if edge.producer in routers or edge.consumer in routers}
    explicit = declarations[0].get('prefetch_slots') is not None
    # The ordinary planner budgets durable reference windows independently of
    # a memory kernel's one-row input queue. Preserve that grant on unrelated
    # edges; an explicit graph prefetch cap applies to every branch edge.
    return tuple(replace(exchange,
        credit_rows=min(exchange.credit_rows, credit['rows']),
        credit_bytes=min(exchange.credit_bytes, credit['bytes']))
        if explicit or exchange.edge_id in routed else exchange for exchange in exchanges)


def pack_graph_residency(graph, stages, exchanges, residency, *, requested, hosts, bounds, policy):
    """Keep affordable original memory edges under the sole native solver.

    The fallback first proves a legal durable plan. Deterministic, bounded
    proposals can only restore automatic upgrades of original memory edges;
    an infeasible proposal leaves that prior legal plan intact. No history is
    replanned here: this hook runs only while compiling a newly sealed plan.
    """
    if not any('graph_route' in node.kernel.configuration for node in graph.nodes):
        return exchanges, residency
    from .dataflow_place import solve_residency
    from ..execution.dataflow_native_limits import sealed_execution_ceilings
    originals = {edge.edge_id: edge for edge in requested}
    selected = exchanges
    attempts = 0
    for index, edge in enumerate(exchanges):
        original = originals[edge.edge_id]
        if (original.mode != 'memory_incremental' or edge.mode != 'durable_incremental'
                or 'requires_checkpointed_parking' not in edge.reason):
            continue
        if attempts >= 64:
            break
        attempts += 1
        proposed = tuple(original if i == index else value for i, value in enumerate(selected))
        try:
            candidate = solve_residency(stages, proposed, hosts=hosts, bounds=bounds,
                max_active_workers=policy.max_active_workers,
                max_cpu_millis=sealed_execution_ceilings(graph).get('cpu_millis'))
        except DataflowError as error:
            if error.code != 'RESOURCE_INFEASIBLE':
                raise
        else:
            selected, residency = proposed, candidate
    return selected, residency
