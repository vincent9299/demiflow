"""Dataflow schema implementation."""
from __future__ import annotations

from collections import deque
from collections.abc import Mapping
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.contracts.record_schema import KEY, encode, decode, encode_schema
from demiflow.lance.flow_contract import stream_lance_output_schema


def node_schemas(graph, physical=None, *, bounds):
    """Interpret all nodes once; None means no physical source snapshots."""
    if len(graph.nodes) > bounds.max_nodes or len(graph.edges) > bounds.max_edges:
        raise DataflowError('BUDGET_EXCEEDED', 'Schema graph exceeds metadata bounds')
    sources = {source.source_id: source for source in physical.sources} if physical is not None else {}
    nodes = {node.occurrence_id: node for node in graph.nodes}
    incoming, outgoing = {key: [] for key in nodes}, {key: [] for key in nodes}
    for edge in graph.edges:
        incoming[edge.consumer].append(edge.producer)
        outgoing[edge.producer].append(edge.consumer)
    indegree = {key: len(values) for key, values in incoming.items()}
    ready = deque(key for key, count in indegree.items() if not count)
    known = {}
    preserved = {'FilterOp', 'LimitOp', 'SortOp', 'RandomShuffleOp',
        'RandomizeBlockOrderOp', 'RepartitionOp', 'RandomSampleOp'}
    metadata_bytes = 0
    import pyarrow as pa
    while ready:
        key = ready.popleft()
        node = nodes[key]
        schema = None
        if node.kind == 'source':
            value = node.kernel.configuration.get(KEY)
            snapshot = sources.get(key)
            if snapshot is not None:
                sealed = snapshot.immutable_identity.get(KEY)
                if value is not None and sealed is not None and value != sealed:
                    raise DataflowError('SOURCE_CHANGED', 'Sealed source schema differs from graph')
                value = sealed if sealed is not None else value
            if value is not None:
                schema = decode(value, bounds=bounds)
                metadata_bytes += len(value)
        else:
            parents = [known[parent] for parent in incoming[key]]
            base = parents[0] if len(parents) == 1 else None
            kind, cfg = node.kernel.kind, node.kernel.configuration
            if node.kind == 'terminal' and KEY in cfg.get('arguments', {}):
                schema = decode(cfg['arguments'][KEY], bounds=bounds)
                metadata_bytes += len(cfg['arguments'][KEY])
            elif node.kind == 'terminal' or kind in preserved:
                schema = base
            elif base is not None and kind == 'SaveLanceOp':
                schema = stream_lance_output_schema(base, cfg.get('output_ref'))
                metadata_bytes += len(encode_schema(schema, bounds=bounds))
            elif base is not None and kind in ('SelectColumnsOp', 'DropColumnsOp', 'RenameColumnsOp'):
                if kind == 'SelectColumnsOp':
                    fields = [base.field(name) for name in cfg['columns']]
                elif kind == 'DropColumnsOp':
                    fields = [field for field in base if field.name not in cfg['columns']]
                else:
                    names = cfg['names']
                    if isinstance(names, Mapping):
                        names = [names.get(field.name, field.name) for field in base]
                    if len(names) != len(base):
                        raise DataflowError('INVALID_CONTRACT', 'Renamed schema field count differs')
                    fields = [field.with_name(name) for field, name in zip(base, names)]
                schema = pa.schema(fields, metadata=base.metadata)
                metadata_bytes += len(encode_schema(schema, bounds=bounds))
            elif kind == 'union' and parents and all(p is not None for p in parents):
                schema = parents[0]
                if any(not schema.equals(p, check_metadata=True) for p in parents[1:]):
                    schema = None
        if metadata_bytes > bounds.max_control_bytes:
            raise DataflowError('BUDGET_EXCEEDED', 'Derived schema metadata exceeds control budget')
        known[key] = schema
        for child in outgoing[key]:
            indegree[child] -= 1
            if not indegree[child]:
                ready.append(child)
    if len(known) != len(nodes):
        raise DataflowError('INVALID_CONTRACT', 'Cannot interpret schema in a cyclic graph')
    return known

def terminal_schema(graph, physical, *, terminal, bounds):
    """Interpret immutable type metadata; preserve the original terminal API."""
    return node_schemas(graph, physical, bounds=bounds).get(terminal)
