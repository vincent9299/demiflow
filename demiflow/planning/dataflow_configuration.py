"""The keys of ``KernelSpec.configuration``: who writes each one, where, and in what shape.

``KernelSpec.configuration`` is the one part of a compiled plan that has no
schema of its own: a mapping the compiler fills in several stages and the
runtime reads by key. This module is that schema.

A key reaches a node from exactly one of these writers, in the order they run:

``declaration``  a field of the typed operation, copied by the generic lowering.
                 The allowed keys are the operation's own dataclass fields, so
                 they are derived here, never listed a second time.
``source``       the structural facts of a source or relational node.
``model``        the declaration frozen by a model lowering (prompt, embedding,
                 agent, and the two kinds only direct callers of the model
                 factory produce: fetch and document).
``lowering``     what the generic lowering adds: the shuffle order version,
                 stream options, the keys of a recognised service declaration.
``capture``      what the capture adds once a node has a place in the graph:
                 identities, executor defaults, task grouping, relational links.
``budget``       memory contracts decided by ``dataflow_budget``.
``graph``        marks of the routed-graph adapter.
``shared``       same-instance actor ownership.

``configuration_findings(graph)`` checks a newly compiled graph against this
schema and returns what does not fit. It is report-only: nothing here rejects a
plan, and ``report_configuration`` does nothing unless the
``demiflow.planning.configuration`` logger is enabled for DEBUG. A sealed plan
is never checked; it keeps the keys it was sealed with.

The key names owned by other modules (``publication_workspace``,
``reducer_memory_v1``, ``shuffle_order_version``, ``declared_arrow_schema``,
``managed_http_devices``) are spelled out here so that importing this module
initialises nothing; a test holds them equal to the constants they mirror.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, fields, is_dataclass
from typing import Any, Callable, Mapping

from ..execution.contracts.dataflow_kinds import kinds_with

_LOG = logging.getLogger('demiflow.planning.configuration')

# Operation fields the generic lowering does not copy into the configuration.
NOT_COPIED = frozenset({'runtime_contract', 'label', 'node_id', 'source_dataset',
                        'callable', 'native_options', 'stream_options'})
TASK_KINDS = kinds_with('task')
# Column projections never start a task group; they stay in the one they follow.
TASK_FOLLOWERS = frozenset({'SelectColumnsOp', 'DropColumnsOp', 'RenameColumnsOp', 'RandomSampleOp'})
TASK_KEYS = ('task_batch_rows', 'task_group', 'task_origin')
NATIVE_OPERATIONS = frozenset({'acquire', 'finalize', 'producer', 'search'})
PUBLICATION_SCHEMAS = frozenset({'demiflow.native_publication_workspace.v1',
                                 'demiflow.native_publication_workspace.v2'})


def _mapping(value) -> bool:
    return isinstance(value, Mapping)


def _sequence(value) -> bool:
    return isinstance(value, (list, tuple))


def _text(value) -> bool:
    return type(value) is str and bool(value)


def _count(value) -> bool:
    return type(value) is int and value >= 1


def _flag(value) -> bool:
    return type(value) is bool


def _true(value) -> bool:
    return value is True


def _terminal_memory(value) -> bool:
    if not _mapping(value):
        return False
    keys = set(value)
    if keys == {'schema', 'resident_bytes'}:                 # bounded terminal workspace
        return _text(value['schema']) and _count(value['resident_bytes'])
    if keys in ({'payload_bytes', 'metadata_bytes'}, {'payload_bytes', 'metadata_bytes', 'carrier'}):
        return (type(value['payload_bytes']) is int and _count(value['metadata_bytes'])
                and ('carrier' not in value or _text(value['carrier'])))
    return False


def _publication(value) -> bool:
    return (_mapping(value) and set(value) == {'schema', 'resident_bytes'}
            and value['schema'] in PUBLICATION_SCHEMAS and _count(value['resident_bytes']))


def _shared_actor(value) -> bool:
    return (_mapping(value) and set(value) == {'owner', 'members', 'state_owner'} and _sequence(value['members'])
            and value['owner'] in value['members'] and value['state_owner'] in value['members'])


@dataclass(frozen=True)
class ConfigurationKey:
    name: str
    stage: str
    shape: Callable[[Any], bool]
    meaning: str


def _key(name, stage, shape, meaning):
    return ConfigurationKey(name, stage, shape, meaning)


# Keys the compiler itself writes. Operation fields, source facts and model
# declarations are admitted by ``_admitted`` below and carry no shape here:
# their values are whatever the declaration said.
COMPILER_KEYS: Mapping[str, ConfigurationKey] = {key.name: key for key in (
    _key('occurrence_id', 'capture', _text, 'the occurrence this kernel was captured as; equals the node identity'),
    _key('program', 'capture', _text, 'the occurrence whose sealed program this transform runs; equals the node identity'),
    _key('reducer_program', 'capture', _text, 'the occurrence whose sealed reducer a reduce node folds with'),
    _key('lance_key_source', 'capture', _text, 'the key-only source occurrence an exclude_keys node reads its left side from'),
    _key('payload_columns', 'capture', _sequence, 'the columns the excluded side originally selected'),
    _key('default_batch_size', 'capture', _count, 'map_batches batch size taken from the executor when none is declared; rows'),
    _key('task_batch_rows', 'capture', _count, 'rows per task of a task-scoped kernel; from the executor, else 8192'),
    _key('task_group', 'capture', _text, 'the task group a kernel runs in; a kernel either starts one or joins its producer'),
    _key('task_origin', 'capture', _flag, 'whether this kernel started its task group'),
    _key('shuffle_order_version', 'lowering', _text, 'shuffle formula of a newly compiled random_shuffle; absent in older sealed plans'),
    _key('streaming_map', 'lowering', _true, 'the map declared stream options and runs as a streaming actor'),
    _key('callable_scope', 'lowering', _text, 'stream option: one callable per stage or per worker'),
    _key('native_operation', 'lowering', lambda v: v in NATIVE_OPERATIONS, 'the service a recognised declaration performs'),
    _key('native_key', 'lowering', _text, 'declaration key of an acquire or search service'),
    _key('terminal_memory', 'budget', _terminal_memory, 'terminal index and payload windows, or one bounded workspace; bytes'),
    _key('publication_workspace', 'budget', _publication, 'resident window of a native publication; bytes'),
    _key('input_carrier', 'budget', _text, 'the terminal consumes stored row references from its producer'),
    _key('output_carrier', 'budget', _text, 'the join publishes stored row references to its terminal'),
    _key('reducer_memory_v1', 'budget', _mapping, 'declared memory contract of a reducer'),
    _key('graph_route', 'graph', _mapping, 'routing declaration of a routed graph, on its router'),
    _key('graph_reader', 'graph', _text, 'the reader a branch belongs to, on the first node of the branch'),
    _key('graph_observe', 'graph', _true, 'completion of this node is reported in the graph result'),
    _key('graph_private_tag', 'graph', _mapping, 'the platform tag closing a branch and the producer it follows'),
    _key('graph_literal_prefix', 'graph', _sequence, 'the literal source and plain maps that may share one worker'),
    _key('graph_result', 'graph', _mapping, 'ancestor, router and target occurrences, on the terminal'),
    _key('actor_binding_version', 'shared', _count, 'this node runs an actor instance whose ownership is declared'),
    _key('actor_recovery_rows', 'shared', _true, 'the actor declares recovery rows'),
    _key('shared_actor', 'shared', _shared_actor, 'owner, members and state owner of one shared actor instance'),
)}

# Stream options are copied onto a map that declares them; on the operations that
# declare the same names as fields they are ordinary declaration keys.
STREAM_KEYS = frozenset({'streaming_map', 'concurrency', 'callable_scope', 'execution', 'queue_depth', 'catch'})
VARIANT_KEYS = frozenset({'native_operation', 'native_key'})
GRAPH_MARKS = frozenset({'graph_route', 'graph_reader', 'graph_observe', 'graph_private_tag', 'graph_literal_prefix'})
SHARED_KEYS = frozenset({'actor_binding_version', 'actor_recovery_rows', 'shared_actor'})

_KEYED = frozenset({'kind', 'op', 'on', 'right_on', 'how', 'suffix', 'chunk_bytes', 'max_rows', 'output'})
SOURCE_KEYS: Mapping[str, frozenset] = {
    'items': frozenset({'kind', 'rows', 'items'}),
    'range': frozenset({'kind', 'count'}),
    'file': frozenset({'kind', 'format', 'paths', 'options'}),
    'sql': frozenset({'kind', 'sql', 'options', 'connection_factory'}),
    'lance': frozenset({'kind', 'query', 'key_only'}),
    'materialized': frozenset({'kind', 'row_count', 'reference'}),
    'iterable': frozenset({'kind', 'factory'}),
    'datasource': frozenset({'kind', 'datasource'}),
    'queue': frozenset({'kind', 'datasource'}),
    'queue_history': frozenset({'kind', 'datasource'}),
    'literal_empty': frozenset({'kind'}),
    'join': _KEYED | {'output_carrier'},
    'exclude_keys': _KEYED | {'lance_key_source', 'payload_columns'},
    'reduce': _KEYED | {'reducer_program', 'reducer_memory_v1'},
    'groups': _KEYED,
    'union': frozenset({'kind', 'inputs'}),
}
_JOURNAL = frozenset({'private_journal', 'collect_journal_totals'})
MODEL_KEYS: Mapping[str, frozenset] = {
    'prompt': frozenset({'prompt_name', 'inputs', 'output', 'outputs', 'options', 'max_requests', 'response_format',
                         'model', 'content_hash', 'node_id', 'concurrency', 'catch', 'managed_http_service_digest',
                         'managed_http_devices', 'route', 'routes', 'route_limits', 'isolate_route_failures'}) | _JOURNAL,
    'embedding': frozenset({'inputs', 'output', 'max_batch', 'flush_interval', 'concurrency', 'model_contract',
                            'managed_http_devices', 'max_requests', 'node_id'}) | _JOURNAL,
    'agent': frozenset({'concurrency', 'queue_depth', 'label', 'runtime', 'resources', 'node_id', 'catch'}),
    'fetch': frozenset({'output', 'concurrency', 'requests', 'checkpoint', 'checkpoint_operator', 'query',
                        'search_spec', 'node_id'}),
    'document': frozenset({'request', 'output', 'node_id'}),
}
TERMINAL_KEYS = frozenset({'kind', 'arguments', 'terminal_memory', 'publication_workspace', 'input_carrier',
                           'graph_result'})
SCHEMA_KEY = 'declared_arrow_schema'

# Written after compilation, when a plan is sealed for thread programs. A newly
# compiled graph does not carry it yet.
SEAL_KEYS = frozenset({'thread_attempt'})
# Keys a kernel reads that no compile stage writes today. Each is a reader without
# a writer in this tree: an older sealed plan may carry it, or the read is dead.
# They are listed so that the gap is visible; a newly compiled graph that carried
# one would be reported.
READ_WITHOUT_WRITER: Mapping[str, str] = {
    'initial': 'reduce kernel: inline initial accumulator; a new reduce names reducer_program instead',
    'initial_program': 'reduce kernel: program holding the initial accumulator',
    'reducer_kwargs': 'reduce kernel: inline reducer keyword arguments',
    'aggregates': 'aggregate kernel: inline aggregate declarations; a new aggregate terminal uses action programs',
    'cache': 'model binding: result-cache declaration',
    'cache_namespace': 'kernel factory: result-cache namespace of a non-aggregate kernel',
}


def declared_fields(kind: str):
    """The configuration keys a typed operation declares, or ``None`` when ``kind`` is not one."""
    from ..data import plan
    operation = getattr(plan, kind, None)
    if not (isinstance(operation, type) and is_dataclass(operation)):
        return None
    return frozenset(field.name for field in fields(operation)) - NOT_COPIED


def _has_field(kind: str, name: str) -> bool:
    from ..data import plan
    operation = getattr(plan, kind, None)
    return isinstance(operation, type) and is_dataclass(operation) and any(f.name == name for f in fields(operation))


def _admitted(node) -> frozenset | None:
    """Every key this node may carry, or ``None`` when its kind is not known to the schema."""
    kind = node.kernel.kind
    if node.kind == 'terminal':
        return TERMINAL_KEYS | {'occurrence_id'}
    if node.kind in ('source', 'relational'):
        keys = SOURCE_KEYS.get(kind)
        return None if keys is None else keys | {'occurrence_id', SCHEMA_KEY, 'graph_observe'}
    common = {'occurrence_id', 'program'} | GRAPH_MARKS | SHARED_KEYS
    if kind in MODEL_KEYS:
        return MODEL_KEYS[kind] | common
    declared = declared_fields(kind)
    if declared is None:
        return None
    keys = declared | common
    if _has_field(kind, 'stream_options'):
        keys |= STREAM_KEYS
    if _has_field(kind, 'callable'):
        keys |= VARIANT_KEYS
    if kind == 'MapBatchesOp':
        keys |= {'default_batch_size'}
    if kind == 'RandomShuffleOp':
        keys |= {'shuffle_order_version'}
    if kind in TASK_KINDS:
        keys |= set(TASK_KEYS)
    if kind in TASK_FOLLOWERS:
        keys |= {'task_group'}
    return frozenset(keys)


def _node_findings(node, by_occurrence):
    configuration, kind = node.kernel.configuration, node.kernel.kind
    admitted = _admitted(node)
    if admitted is None:
        yield 'kind is not described by the configuration schema'
        return
    for name in sorted(set(configuration) - admitted):
        yield 'key ' + name + ' is not admitted on this node'
    # A shape belongs to a key the compiler writes. Where a typed operation declares
    # a field of the same name, the value on this node is the declaration's.
    declared = (declared_fields(kind) or frozenset()) if node.kind == 'transform' else frozenset()
    for name, value in configuration.items():
        described = COMPILER_KEYS.get(name)
        if described is None or name not in admitted or name in declared:
            continue
        if not described.shape(value):
            yield 'key ' + name + ' does not have the shape its writer gives it'
    if configuration.get('occurrence_id') != node.occurrence_id:
        yield 'occurrence_id does not name this node'
    for name in ('program', 'reducer_program'):
        if name in configuration and configuration[name] != node.occurrence_id:
            yield name + ' does not name this node'
    if node.kind == 'terminal' and configuration.get('kind') != kind:
        yield 'terminal kind differs from its kernel kind'
    if node.kind != 'transform':
        if 'lance_key_source' in configuration:
            source = by_occurrence.get(configuration['lance_key_source'])
            if source is None or source.kind != 'source':
                yield 'lance_key_source does not name a source of this graph'
        return
    present = [name for name in TASK_KEYS if name in configuration]
    if kind in TASK_KINDS and not configuration.get('streaming_map'):
        if len(present) != len(TASK_KEYS):
            yield 'a task-scoped kernel needs task_batch_rows, task_group and task_origin together'
    elif kind in TASK_FOLLOWERS:
        if present not in ([], ['task_group']):
            yield 'a projection may only join a task group'
    elif present:
        yield 'task keys on a kernel that is not task-scoped'
    if 'native_key' in configuration and configuration.get('native_operation') not in ('acquire', 'search'):
        yield 'native_key without an acquire or search declaration'
    if 'default_batch_size' in configuration and configuration.get('batch_size'):
        yield 'default_batch_size beside a declared batch_size'
    shared = configuration.get('shared_actor')
    if _mapping(shared) and _sequence(shared.get('members')) and node.occurrence_id not in shared['members']:
        yield 'shared_actor does not list this node'


def configuration_findings(graph) -> tuple[str, ...]:
    """Everything in ``graph`` that does not fit the configuration schema. Empty when it fits."""
    by_occurrence = {node.occurrence_id: node for node in graph.nodes}
    found = []
    for node in graph.nodes:
        for finding in _node_findings(node, by_occurrence):
            found.append(node.kernel.kind + ' ' + node.occurrence_id[:12] + ': ' + finding)
    consumers = {edge.producer: by_occurrence.get(edge.consumer) for edge in graph.edges}
    for node in graph.nodes:
        carrier = node.kernel.configuration.get('output_carrier')
        if carrier is not None:
            consumer = consumers.get(node.occurrence_id)
            if consumer is None or consumer.kernel.configuration.get('input_carrier') != carrier:
                found.append(node.kernel.kind + ' ' + node.occurrence_id[:12] + ': output_carrier has no matching consumer')
    return tuple(found)


def report_configuration(graph) -> tuple[str, ...]:
    """Log what does not fit, at DEBUG, and return it. Never raises and never rejects.

    It does nothing at all unless the ``demiflow.planning.configuration`` logger is
    enabled for DEBUG, so an ordinary run pays nothing and cannot be affected.
    """
    if not _LOG.isEnabledFor(logging.DEBUG):
        return ()
    try:
        found = configuration_findings(graph)
    except Exception:  # report-only: a defect in this check must never fail a compile
        _LOG.debug('configuration schema check failed for action %s', getattr(graph, 'action_id', None), exc_info=True)
        return ()
    _LOG.debug('configuration schema: action %s: %d nodes checked, %d findings',
               graph.action_id, len(graph.nodes), len(found))
    for finding in found:
        _LOG.debug('configuration schema: action %s: %s', graph.action_id, finding)
    return found
