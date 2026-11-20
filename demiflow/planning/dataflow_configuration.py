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
schema and returns what does not fit. Nothing here rejects a plan:
``report_configuration`` logs each finding as a warning on the
``demiflow.planning.configuration`` logger and returns. A sealed plan is never
checked; it keeps the keys it was sealed with.

Enforcement is staged. Tests can already fail on a finding (the opt-in
``configuration_schema_gate`` pytest plugin). Production warns. Refusing a plan
is a later step, for the invariants only, once real pipelines have run clean.

The key names owned by other modules (``publication_workspace``,
``reducer_memory_v1``, ``shuffle_order_version``, ``declared_arrow_schema``,
``managed_http_devices``) are spelled out here so that importing this module
initialises nothing; a test holds them equal to the constants they mirror.
"""
from __future__ import annotations

import hashlib
import logging
import os
from dataclasses import dataclass, fields, is_dataclass
from functools import lru_cache
from typing import Any, Callable, Mapping

from ..execution.contracts.dataflow_kinds import kinds_with

_LOG = logging.getLogger('demiflow.planning.configuration')
_MAX_REPORTED = 20          # findings logged per graph; the rest are counted

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
            and _text(value['schema']) and value['schema'] in PUBLICATION_SCHEMAS
            and _count(value['resident_bytes']))


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
    _key('native_operation', 'lowering', lambda v: _text(v) and v in NATIVE_OPERATIONS, 'the service a recognised declaration performs'),
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
# Keys a kernel reads that no compile stage writes. They are two different things.
#
# The earlier sealed form of a reduce node. A reduce is sealed today as one
# reducer program that carries its initial accumulator and keyword arguments.
# Before that, the node carried them inline. The reduce kernel still reads the
# inline form so that an unfinished run sealed in it is validated against its
# original grant (kernels/relational.py, "Legacy sealed plans keep their original
# identity and grant"; pinned by the reducer memory tests).
HISTORICAL_KEYS: Mapping[str, str] = {
    'initial': 'inline initial accumulator of a reduce sealed in the earlier form',
    'initial_program': 'program holding the initial accumulator of a reduce sealed in the earlier form',
    'reducer_kwargs': 'inline reducer keyword arguments of a reduce sealed in the earlier form',
}
# The declaration of a kernel that exists and has unit tests, but that no compile
# stage produces. No compiled plan can reach these kernels:
# - the kernel-level aggregate (kernels/aggregate.py): the aggregate terminal of a
#   compiled plan is computed in the worker terminal from its action program;
# - the cached-map kernel (dataflow_models.bind_cached_map): map_cached is lowered
#   as a cache declaration on an ordinary kernel.
UNPRODUCED_KEYS: Mapping[str, str] = {
    'aggregates': 'aggregate declarations of the kernel-level aggregate',
    'cache': 'cache declaration of the cached-map kernel',
    'cache_namespace': 'what makes the kernel factory choose the cached-map kernel',
}
# A newly compiled graph that carried one of either group would be reported.
READ_WITHOUT_WRITER: Mapping[str, str] = {**HISTORICAL_KEYS, **UNPRODUCED_KEYS}


@lru_cache(maxsize=None)
def declared_fields(kind: str):
    """The configuration keys a typed operation declares, or ``None`` when ``kind`` is not one."""
    from ..data import plan
    operation = getattr(plan, kind, None)
    if not (isinstance(operation, type) and is_dataclass(operation)):
        return None
    return frozenset(field.name for field in fields(operation)) - NOT_COPIED


@lru_cache(maxsize=None)
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


@dataclass(frozen=True)
class _Finding:
    """Process-local diagnostics, never part of a compiled or sealed plan."""
    code: str
    category: str
    stage: str
    node_kind: str
    kernel_kind: str
    occurrence_id: str
    key: str | None
    message: str

    def text(self):
        return self.kernel_kind + ' ' + self.occurrence_id[:12] + ': ' + self.message


def _identity(value):
    # Identity fields come from DTOs. Unexpected user-controlled text is hashed,
    # not logged, including an exception's message or an unknown key/kind.
    if type(value) is str and len(value) <= 64 and all(c in '0123456789abcdef' for c in value):
        return value
    raw = value if type(value) is str else type(value).__name__
    return hashlib.sha256(raw.encode('utf-8', errors='replace')).hexdigest()[:16]


def _key_label(name, admitted):
    known = name in COMPILER_KEYS or name in admitted or name in TERMINAL_KEYS
    known = known or any(name in keys for keys in (*SOURCE_KEYS.values(), *MODEL_KEYS.values()))
    return name if known else 'unknown#' + _identity(name)


def _finding(node, code, message, *, stage, key=None, category='invariant'):
    kind = node.kernel.kind
    known = kind in SOURCE_KEYS or kind in MODEL_KEYS or declared_fields(kind) is not None
    # Terminal names are validated by the compiler, but a diagnostic must not
    # reflect arbitrary text supplied by a standalone caller.
    if node.kind == 'terminal':
        from .dataflow_registry import terminal_registry
        known = kind in terminal_registry()
    return _Finding(code, category, stage, node.kind if node.kind in (
        'source', 'relational', 'transform', 'terminal') else 'unknown',
        kind if known else 'unknown#' + _identity(kind), _identity(node.occurrence_id), key, message)


def _check_error(stage):
    return _Finding('check_error', 'check_error', stage, '', '', '', None,
                    'configuration schema check failed; inspection is incomplete')


def _node_findings(node, by_occurrence, *, stage='final'):
    configuration, kind = node.kernel.configuration, node.kernel.kind
    admitted = _admitted(node)
    def finding(code, message, *, key=None, category='invariant'):
        return _finding(node, code, message, stage=stage, key=key, category=category)
    if admitted is None:
        yield finding('unknown_kind', 'kind is not described by the configuration schema', category='observation')
        return
    for name in configuration:
        if name not in admitted:
            label = _key_label(name, admitted)
            yield finding('unknown_key', 'key ' + label + ' is not admitted on this node',
                          key=label, category='observation')
    declared = (declared_fields(kind) or frozenset()) if node.kind == 'transform' else frozenset()
    invalid = set()
    for name, value in configuration.items():
        described = COMPILER_KEYS.get(name)
        if described is None or name not in admitted or name in declared:
            continue
        if not described.shape(value):
            invalid.add(name)
            yield finding('compiler_key_shape', 'key ' + name + ' does not have the shape its writer gives it', key=name)
    if 'occurrence_id' not in invalid and configuration.get('occurrence_id') != node.occurrence_id:
        yield finding('occurrence_binding', 'occurrence_id does not name this node', key='occurrence_id')
    for name, code in (('program', 'program_binding'), ('reducer_program', 'reducer_binding')):
        if name in configuration and name not in invalid and configuration[name] != node.occurrence_id:
            yield finding(code, name + ' does not name this node', key=name)
    if node.kind == 'terminal' and configuration.get('kind') != kind:
        yield finding('terminal_kind', 'terminal kind differs from its kernel kind', key='kind')
    if node.kind != 'transform':
        reference = configuration.get('lance_key_source')
        # Hashability is insufficient: [] freezes to a hashable tuple, [{}] to
        # an unhashable tuple. Neither is an occurrence reference.
        if _text(reference):
            source = by_occurrence.get(reference)
            if source is None or source.kind != 'source':
                yield finding('source_reference', 'lance_key_source does not name a source of this graph',
                              key='lance_key_source')
        return
    present = [name for name in TASK_KEYS if name in configuration]
    if 'streaming_map' not in invalid:
        if kind in TASK_KINDS and not configuration.get('streaming_map'):
            if len(present) != len(TASK_KEYS):
                yield finding('task_complete', 'a task-scoped kernel needs task_batch_rows, task_group and task_origin together')
        elif kind in TASK_FOLLOWERS:
            if present not in ([], ['task_group']):
                yield finding('task_follower', 'a projection may only join a task group')
        elif present:
            yield finding('task_outside_scope', 'task keys on a kernel that is not task-scoped')
    if ('native_key' in configuration and not invalid.intersection(('native_key', 'native_operation'))
            and configuration.get('native_operation') not in ('acquire', 'search')):
        yield finding('native_key_owner', 'native_key without an acquire or search declaration', key='native_key')
    if ('default_batch_size' in configuration and 'default_batch_size' not in invalid
            and configuration.get('batch_size')):
        yield finding('batch_default_conflict', 'default_batch_size beside a declared batch_size', key='default_batch_size')
    shared = configuration.get('shared_actor')
    if ('shared_actor' not in invalid and _mapping(shared) and _sequence(shared.get('members'))
            and node.occurrence_id not in shared['members']):
        yield finding('shared_membership', 'shared_actor does not list this node', key='shared_actor')


def _findings(nodes, connections, *, stage):
    """One rule source for whole graphs and mature capture stages.

    ``connections`` yields occurrence pairs; no public graph DTO is fabricated
    for intermediate stages. A checker defect preserves earlier findings and
    does not prevent inspection of other nodes. No exception text is exposed.
    """
    by_occurrence = {node.occurrence_id: node for node in nodes}
    for node in nodes:
        try:
            yield from _node_findings(node, by_occurrence, stage=stage)
        except Exception:
            yield _check_error(stage)
    consumers = {}
    for producer, consumer in connections:
        consumers.setdefault(producer, []).append(by_occurrence.get(consumer))
    for node in nodes:
        carrier = node.kernel.configuration.get('output_carrier')
        if not _text(carrier):
            continue                 # already a shape finding, or no carrier
        try:
            targets = consumers.get(node.occurrence_id, ())
            # All outgoing consumers must understand this representation. A
            # consumer's bad input shape has its own finding; do not guess it.
            mismatch = not targets or any(target is None or (
                target.kernel.configuration.get('input_carrier') is None or (
                    _text(target.kernel.configuration.get('input_carrier')) and
                    target.kernel.configuration.get('input_carrier') != carrier)) for target in targets)
            if mismatch:
                yield _finding(node, 'carrier_pair', 'output_carrier has no matching consumer',
                               stage=stage, key='output_carrier', category='observation')
        except Exception:
            yield _check_error(stage)


def _scan(nodes, connections, *, stage):
    try:
        yield from _findings(nodes, connections, stage=stage)
    except Exception:
        yield _check_error(stage)    # retain partial results even if indexing fails


def _graph_findings(graph, *, stage='final'):
    try:
        yield from _scan(graph.nodes, ((edge.producer, edge.consumer) for edge in graph.edges), stage=stage)
    except Exception:
        yield _check_error(stage)


def configuration_findings(graph) -> tuple[str, ...]:
    """All diagnostics, including a checker failure; an empty tuple means a complete clean scan."""
    return tuple(finding.text() for finding in _graph_findings(graph))


def _report(findings, *, action_id, stage, node_count, edge_count, collect_all):
    samples, all_text = [], [] if collect_all else None
    counts = {}
    categories = {}
    total = 0
    for finding in findings:
        total += 1
        counts[finding.code] = counts.get(finding.code, 0) + 1
        categories[finding.category] = categories.get(finding.category, 0) + 1
        if len(samples) < _MAX_REPORTED:
            samples.append(finding)
        if all_text is not None:
            all_text.append(finding.text())
    action = _identity(action_id)
    for finding in samples:
        _LOG.warning('configuration schema: action %s: %s', action, finding.text())
    if total > len(samples):
        _LOG.warning('configuration schema: action %s: and %d more findings', action, total - len(samples))
    summary = {'version': 1, 'pid': os.getpid(), 'action': action, 'stage': stage,
               'nodes': node_count, 'edges': edge_count, 'findings': total,
               'codes': counts, 'categories': categories, 'check_completed': 'check_error' not in counts}
    _LOG.debug('configuration schema: action %s: %d nodes checked, %d findings',
               action, node_count, total, extra={'configuration_schema': summary})
    return tuple(all_text) if all_text is not None else None


def report_configuration(graph) -> tuple[str, ...]:
    """Warn, never reject; retain partial diagnostics on an incomplete inspection.

    This compatibility API returns every diagnostic. The compiler uses the
    bounded stage reporter instead; it does not retain a full finding list.
    """
    return _report(_graph_findings(graph), action_id=getattr(graph, 'action_id', None), stage='final',
                   node_count=len(getattr(graph, 'nodes', ())), edge_count=len(getattr(graph, 'edges', ())),
                   collect_all=True)
