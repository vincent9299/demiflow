"""Persistent shape and resident limits of a sealed relational region and its reducers."""
from dataclasses import dataclass

from .dataflow_errors import DataflowError
from ..recovery_contract import identity

PREFIX = 'relational-v1-'
SQL_PREFIX = PREFIX + 'sql1-'
# Separate from the v3 Python row/codec window. The child imports Arrow and
# DataFusion, processes only bounded keys/offsets, and never owns business rows.
SQL_CHILD_RSS = 256 << 20
SQL_PARENT_WINDOW = 64 << 20
MAX_JOIN_DEPTH = 2
REDUCER_MEMORY_FIELD = 'reducer_memory_v1'


def reducer_contract(spec):
    value = spec.configuration.get(REDUCER_MEMORY_FIELD)
    if value is None:
        return None
    expected = {'version', 'initial_bytes', 'initial_copy_bytes', 'copy_workspace_bytes',
                'state_resident_bytes', 'inspection_bytes', 'inspection_nodes'}
    if (set(value) != expected or value['version'] != 1
            or any(type(v) is not int or v < 0 for v in value.values())
            or min(value['state_resident_bytes'],value['inspection_bytes'],value['inspection_nodes']) < 1):
        raise DataflowError('INCOMPATIBLE_HISTORY', 'Invalid sealed reducer resident contract')
    return value


def reducer_resident_bytes(value, bounds, *, join_depth=None):
    """Maximum of disjoint build/hydrate/fold/copy/output phases, not their sum."""
    row, batch = bounds.max_row_bytes, bounds.max_batch_bytes
    initial, state = value['initial_bytes'], value['state_resident_bytes']
    hydrate = batch + ((join_depth + 1) if join_depth is not None else 2) * row
    build = 8 * row + hydrate
    fold = max(hydrate + row + state,            # next row + saved group key/state
               3 * row + 2 * state,             # row/two group keys + old/new state
               3 * row + state + max(state, bounds.max_state_bytes),
               3 * row + value['initial_copy_bytes'] + value['copy_workspace_bytes'],
               batch + 4 * row + state)         # next group row + final output/codec
    return initial + value['inspection_bytes'] + max(build, 7 * row + fold)


@dataclass(frozen=True)
class RelationalRegionInput:
    edge_id: str
    consumer: str
    port: str
    fields: tuple[str, ...]


@dataclass(frozen=True)
class RelationalRegionPlan:
    region_id: str
    occurrences: tuple[str, ...]
    exit_occurrence: str
    inputs: tuple[RelationalRegionInput, ...]
    internal_edges: tuple[str, ...]
    join_depth: int
    ordered_groups: bool
    reducer_memory: tuple[tuple[str,int], ...] = ()
    group_batch_rows: int = 0
    sql_offsets: bool = False

    def group_batch_bytes(self, bounds):
        # Like an output Record, the whole batch is subject to one row ceiling.
        # Allocate up to eight row windows for the decoded Python containers.
        return min(self.group_batch_rows, 8) * bounds.max_row_bytes


def region_key_fields(node, port):
    config = node.kernel.configuration
    return tuple(config.get('right_on') or config['on']) if port == 'right' else tuple(config['on'])


def describe_region(graph, members, *, sql_offsets=False):
    nodes = {n.occurrence_id: n for n in graph.nodes}
    selected = set(members)
    incoming = {occ: [] for occ in members}
    outgoing = {occ: [] for occ in members}
    for edge in graph.edges:
        if edge.consumer in selected:
            incoming[edge.consumer].append(edge)
        if edge.producer in selected:
            outgoing[edge.producer].append(edge)
    exits = [occ for occ in members if not any(e.consumer in selected for e in outgoing[occ])]
    if len(exits) != 1 or len(members) < 2:
        return None
    terminal = exits[0]
    if any(nodes[o].kernel.kind not in ('join', 'reduce', 'groups') for o in members):
        return None
    if any(nodes[o].kernel.kind in ('reduce', 'groups') and o != terminal for o in members):
        return None
    depth, external, inner = {}, [], []
    for occ in members:
        node = nodes[occ]
        if (node.kernel.effect != 'pure' or node.kernel.resource_role != 'cpu'
                or not node.kernel.inputs or any(p.requirement != 'finite_complete' for p in node.kernel.inputs)
                or node.kernel.profile.limits or node.kernel.profile.requirements
                or set(node.kernel.profile.requests) - {'cpu_millis', 'memory_bytes'}):
            return None
        if occ != terminal and (len(outgoing[occ]) != 1 or outgoing[occ][0].consumer not in selected):
            return None
        parents = []
        for edge in incoming[occ]:
            fields = region_key_fields(node, edge.input_port)
            if edge.producer in selected:
                producer = nodes[edge.producer]
                if producer.kernel.kind != 'join' or tuple(producer.kernel.configuration['on']) != fields:
                    return None
                if edge.producer not in depth:
                    return None
                parents.append(depth[edge.producer])
                inner.append(edge.edge_id)
            else:
                external.append(RelationalRegionInput(edge.edge_id, occ, edge.input_port, fields))
        depth[occ] = max(parents, default=0) + (node.kernel.kind == 'join')
    maximum = max(depth.values())
    if not 1 <= maximum <= MAX_JOIN_DEPTH:
        return None
    if sql_offsets:
        if (len(members) != 2 or maximum != 2 or len(inner) != 1
                or not any(e.consumer == terminal and e.input_port == 'left' and e.edge_id in inner
                           for e in incoming[terminal])
                or any(nodes[o].kernel.kind != 'join' or
                       nodes[o].kernel.configuration.get('how', 'inner') not in ('left', 'inner') for o in members)):
            return None
    payload = ['relational-region-v1', members, terminal,
               [(x.edge_id, x.consumer, x.port, x.fields) for x in external], inner, maximum]
    memory = reducer_contract(nodes[terminal].kernel) if nodes[terminal].kernel.kind == 'reduce' else None
    if nodes[terminal].kernel.kind == 'reduce' and memory is None:
        return None
    group_rows = 0
    if nodes[terminal].kernel.kind == 'groups':
        group_rows = nodes[terminal].kernel.configuration.get('max_rows', 32)
        if type(group_rows) is not int or group_rows < 1:
            return None
        payload.append(['group-batches-v1', group_rows])
    if memory is not None:
        payload.append(['reducer-memory-v1',dict(memory)])
    if sql_offsets:
        payload.append(['offset-sql-v1', SQL_CHILD_RSS, SQL_PARENT_WINDOW])
    return RelationalRegionPlan((SQL_PREFIX if sql_offsets else PREFIX) + identity(payload), tuple(members), terminal,
                                tuple(external), tuple(inner), maximum,
                                nodes[terminal].kernel.kind in ('reduce', 'groups'),
                                tuple(sorted(memory.items())) if memory is not None else (), group_rows, sql_offsets)


def interpret_region(graph, stage):
    if not stage.stage_id.startswith(PREFIX):
        return None
    if any(n.occurrence_id in stage.occurrences and n.kernel.kind == 'reduce'
           and reducer_contract(n.kernel) is None for n in graph.nodes):
        raise DataflowError('INCOMPATIBLE_HISTORY',
            'sealed relational region predates its reducer resident declaration; cannot infer a new grant')
    plan = describe_region(graph, stage.occurrences, sql_offsets=stage.stage_id.startswith(SQL_PREFIX))
    if plan is None or plan.region_id != stage.stage_id or len(stage.partitions) != 1:
        raise DataflowError('INCOMPATIBLE_HISTORY', 'sealed relational region differs from original graph')
    return plan


def region_resident_bytes(plan, bounds):
    # Only references (not hydrated parent rows) are retained between joins.
    # Hydration has at most depth+1 input/result rows, two codec/copy rows and
    # one output handoff. Input preparation reuses that window sequentially.
    # One bounded index and traversal metadata use the remaining fixed window.
    metadata = min(4 * 1024**2, max(4096,
        min(bounds.max_control_bytes, bounds.max_resident_bytes) // 16))
    if plan.group_batch_rows:
        # Group output must itself fit one Record. The bounded key/offset
        # sort and rehydration coexist with the retained group and codec.
        row = bounds.max_row_bytes
        hydrate = bounds.max_batch_bytes + (plan.join_depth + 1) * row
        retained_batch = plan.group_batch_bytes(bounds)
        return metadata + max(8 * row + hydrate,  # build bounded key/offset sort
            7 * row + retained_batch + max(hydrate, bounds.max_batch_bytes + 3 * row))
    if plan.ordered_groups:
        return reducer_resident_bytes(dict(plan.reducer_memory),bounds,join_depth=plan.join_depth)
    rows = bounds.max_batch_bytes + (plan.join_depth + 4) * bounds.max_row_bytes + metadata
    return rows + (SQL_CHILD_RSS + SQL_PARENT_WINDOW if plan.sql_offsets else 0)
