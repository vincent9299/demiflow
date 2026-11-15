"""Bounded type facts from sealed sources, independent of row arrival.

Only metadata from trusted native input handles or a pinned source snapshot is
accepted. Propagation follows schema-preserving kernels; UDF outputs are unknown.
"""
import base64
from collections import deque
from collections.abc import Mapping

from .contracts.dataflow_errors import DataflowError
from .record_schema import RecordSchemas, encode_schema

KEY = 'declared_arrow_schema'


def encode(schema, *, bounds):
    return base64.b64encode(encode_schema(schema, bounds=bounds)).decode('ascii')


def decode(value, *, bounds):
    maximum = min(65536, bounds.max_control_bytes, bounds.max_batch_bytes)
    if type(value) is not str or len(value) > 4 * ((maximum + 2) // 3):
        raise DataflowError('CORRUPT_ARTIFACT', 'Sealed Arrow schema exceeds metadata bounds')
    import pyarrow as pa
    try:
        raw = base64.b64decode(value, validate=True)
        if len(raw) > maximum:
            raise ValueError('schema byte bound')
        schema = pa.ipc.read_schema(pa.BufferReader(raw))
        encode_schema(schema, bounds=bounds)
        return schema
    except (ValueError, pa.ArrowException) as exc:
        raise DataflowError('CORRUPT_ARTIFACT', 'Invalid sealed Arrow schema') from exc


def literal_schema(source, *, bounds):
    """Inspect native already-owned data metadata, without consuming any rows."""
    from ..data.sources import MaterializedSource
    from .dataflow_inputs import InputMaterializedHandle
    from .dataflow_handles import StoredMaterializedHandle
    if type(source) is not MaterializedSource:
        return None
    handle = source.handle
    if type(handle) is StoredMaterializedHandle:
        return handle.declared_schema
    if type(handle) is not InputMaterializedHandle:
        return None
    handle.check_open()
    if handle._format not in ('arrow', 'pandas'):
        return None
    import pyarrow as pa
    if len(handle._value) > bounds.max_nodes:
        raise DataflowError('BUDGET_EXCEEDED', 'Input schema list exceeds metadata bounds')
    schema = None
    for value in handle._value:
        current = value.schema if handle._format == 'arrow' else pa.Schema.from_pandas(value, preserve_index=False)
        encode_schema(current, bounds=bounds)
        if schema is not None and not schema.equals(current, check_metadata=True):
            # Heterogeneous inputs retain per-record types. Do not invent a
            # uniform declaration or reject a previously valid row collection.
            return None
        schema = current
    return schema


def stream_lance_output_schema(schema, output_ref):
    """A native publication passes input fields through and may add a receipt."""
    if schema is None or output_ref is None:
        return schema
    if output_ref in schema.names:
        raise ValueError('output_ref would overwrite an input field')
    import pyarrow as pa
    return schema.append(pa.field(output_ref,
        pa.struct([pa.field('uri', pa.string()), pa.field('version', pa.int64())]), nullable=True))


def terminal_schema(graph, physical, *, terminal, bounds):
    """Interpret immutable type metadata; never invoke a source or user function."""
    if len(graph.nodes) > bounds.max_nodes or len(graph.edges) > bounds.max_edges:
        raise DataflowError('BUDGET_EXCEEDED', 'Schema graph exceeds metadata bounds')
    sources = {source.source_id: source for source in physical.sources}
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
            if node.kind == 'terminal' or kind in preserved:
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
    return known.get(terminal)


def result_schema_ref(result, *, services):
    """Read type metadata from the original committed result publication."""
    from .dataflow_sinks import _read_json, PUBLICATION_MEDIA, _artifact_from_json
    if result.result_ref is None:
        return None
    publication = _read_json(services.store, result.result_ref,
        max_bytes=services.bounds.max_control_bytes, media_type=PUBLICATION_MEDIA)
    if publication.get('schema') != 'demiflow.dataflow_sink_publication.v3':
        raise DataflowError('CORRUPT_ARTIFACT', 'Invalid result publication for schema')
    value = publication['manifest'].get('schema_ref')
    ref = _artifact_from_json(value) if value is not None else None
    if ref is not None:
        RecordSchemas(services).read(ref)
    return ref


def validate_literal_schema_replay(request, graph, *, bounds):
    """Compare only explicit new-seal type facts, without rewriting old identity.

    Historical literal action identities intentionally retain their old bytes.
    New schema facts close the empty-literal ambiguity at the original seal gate.
    Old graphs lacking these facts continue through the existing history checks.
    """
    expected = {node.occurrence_id: node.kernel.configuration[KEY]
        for node in graph.nodes if node.kind == 'source' and KEY in node.kernel.configuration}
    if not expected:
        return
    from ..planning.dataflow_compile import lower_action
    current = lower_action(request, bounds=bounds).graph
    actual = {node.occurrence_id: node.kernel.configuration.get(KEY)
        for node in current.nodes if node.kind == 'source'}
    for occurrence, value in expected.items():
        current_value = actual.get(occurrence)
        if current_value is None or not decode(value, bounds=bounds).equals(
                decode(current_value, bounds=bounds), check_metadata=True):
            raise DataflowError('SOURCE_CHANGED', 'Literal source schema differs from its original sealed plan')
