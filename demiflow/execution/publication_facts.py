"""Read-only publication facts for hosts of a native run.

A host (the workbench today) must be able to answer "what did this action
publish?" and "may this prepared publication enter original recovery?" without
knowing how the driver builds its commit scope. These projections previously
lived in the host, which had to rebuild an observation-only run context, name
private sink classes and query ``runtime_commit_scopes`` directly.

Everything here is observation only: no ACK, receipt, scope or journal row is
written, no candidate code is imported and no native commit/recover method is
called. Errors are native ``RecoveryContractError``; hosts map them to their own
error type.

The bodies of ``inspect_write_receipt`` and ``admit_prepared_stage`` were moved
from ``workbench/runs/manager.py`` and ``workbench/runs/native_stages.py``
without changing their logic.
"""
import json
from types import SimpleNamespace

from .recovery_contract import RecoveryContractError

__all__ = [
    'PublicationMismatch', 'PreparedPublicationUnresolved', 'observation_context', 'observation_action',
    'commit_write_context', 'inspect_write_receipt', 'admit_prepared_stage',
    'object_namespace',
]

_MAX_SCOPE_DECLARATION_BYTES = 65536
_STREAM_LANCE_SCHEMA = 'demiflow.stream_lance.v3'


class PublicationMismatch(RecoveryContractError):
    """A prepared native publication contradicts its sealed output identity."""


class PreparedPublicationUnresolved(RecoveryContractError):
    """Native sink inspection could not conclude a prepared publication."""


def observation_context(journal, binding=None, *, directory=None, attempt=None):
    """The one observation-only stand-in for a driver ``RunContext``.

    ``fence='observation-only'`` can never satisfy a journal write guard, so an
    action built on this context can inspect sinks but cannot advance progress.
    Returns ``None`` when the journal has no unified recovery identity.
    """
    if binding is None:
        binding = journal.recovery_identity()
    if binding is None:
        return None
    if attempt is None:
        attempt = journal.execution_status().get('attempt')
    return SimpleNamespace(
        directory=journal.directory if directory is None else directory,
        limits=binding['limits'], journal=journal,
        ref=SimpleNamespace(run_id=binding['run_id']),
        attempt=attempt, fence='observation-only')


def observation_action(journal, action_id, declaration, binding=None, *, directory=None,
                       attempt=None):
    """A ``RunAction`` over :func:`observation_context`, or ``None``."""
    from .run_context import RunAction
    context = observation_context(journal, binding, directory=directory, attempt=attempt)
    if context is None:
        return None
    return RunAction(context, action_id, declaration)


def commit_write_context(action, commit_id):
    """The write context a sink's ``inspect`` expects for one recorded commit."""
    from .commit import CommitCoordinator
    return CommitCoordinator(action)._context(commit_id)


def inspect_write_receipt(journal, action_id):
    """Read-only projection using the same native sink inspection as the driver.

    Covers both publication generations: a v3 ``write_lance`` terminal is
    delegated to ``dataflow_sinks.inspect_native_write_receipt``; an original
    ``lance`` sink (optionally composed with object rows) is inspected here.
    Returns the native receipt data, or ``None`` when nothing conclusive exists.
    """
    from .lance_sink import LanceSink, spec_from_identity
    binding = journal.recovery_identity()
    if binding is None:
        return None
    action = journal.execution_action(action_id)
    if action is None:
        return None
    facts = journal.execution_commits(action_id)
    if facts and facts[-1]['intent']['sinks'] == ['write_lance']:
        from .dataflow_sinks import inspect_native_write_receipt
        return inspect_native_write_receipt(journal, action_id)
    if not facts or facts[-1]['prepared'] is None or facts[-1]['intent']['sinks'] != ['lance']:
        return None
    fact = facts[-1]
    # The immutable native declaration holds the original spec. Schema is
    # serialized as Arrow IPC, so no candidate code is imported to inspect it.
    spec = spec_from_identity(action['declaration']['sink'])
    native_action = observation_action(journal, action_id, action['declaration'], binding,
                                       attempt=journal.execution_status()['attempt'])
    prepared = fact['prepared']['sinks']['lance']
    sink = LanceSink(native_action, spec)
    composite = type(prepared) is dict and {'objects', 'rows'} <= set(prepared)
    if composite:
        from .object_sink import ObjectRowsSink
        sink = ObjectRowsSink.for_inspection(native_action, sink, prepared)
    receipt = sink.inspect(prepared, commit_write_context(native_action, fact['commit_id']))
    if receipt is None:
        return None
    return receipt['data']['rows'] if composite else receipt['data']


def _latest_stream_lance_scope(journal, parent, node):
    """Newest ``stream_lance.v3`` commit scope of one occurrence, or ``None``.

    Old journals have no ``runtime_commit_scopes`` table; that is absence, not
    an error.
    """
    with journal._connection() as db:
        if not db.execute("SELECT 1 FROM sqlite_master WHERE name='runtime_commit_scopes'").fetchone():
            return None
        return db.execute(
            "SELECT scope_id,declaration FROM runtime_commit_scopes WHERE parent_action_id=? "
            "AND json_extract(declaration,'$.node_id')=? AND json_extract(declaration,'$.schema')=? "
            "ORDER BY CAST(json_extract(declaration,'$.request.ordinal') AS INTEGER) DESC,scope_id DESC LIMIT 1",
            (parent, node, _STREAM_LANCE_SCHEMA)).fetchone()


def admit_prepared_stage(journal, directory, *, parents, report_node_id, uri,
                         schema_identity, report_digest):
    """Prove a prepared native stage publication may enter original recovery.

    This is only admission. No ACK, UI result, scope or receipt is written, and
    no native commit/recover method is called. Unknown ownership stays closed.

    ``parents`` are the checked report's action ids; ``report_node_id``, ``uri``
    and ``schema_identity`` identify the sealed output being admitted. Returns
    the committed dataset reference, or ``None`` when no prepared publication of
    that output is provable. Raises ``PublicationMismatch`` when a prepared
    publication exists but differs from the sealed output or has more than one
    owner, and ``PreparedPublicationUnresolved`` when sink inspection itself is
    inconclusive. Other ``RecoveryContractError`` come from the sealed bridge and
    stage binding checks, exactly as before the move.
    """
    from .sealed_plans import read_plan_artifact
    from .dataflow_stage_facts import stage_binding
    from .dataflow_stream_lance import _BatchSink
    binding = journal.recovery_identity()
    if binding is None:
        return None
    reference = None
    for parent in parents:
        bridge_ref = journal.source_binding(parent + '-identity-bridge')
        if bridge_ref is None:
            continue
        bridge = read_plan_artifact(directory, bridge_ref, max_bytes=1024 * 1024)
        for entry in bridge.get('entries', ()):
            if entry.get('report_node_id') != report_node_id:
                continue
            node = entry['occurrence_id']
            stage_binding(journal, directory, parent, node, report_digest)
            row = _latest_stream_lance_scope(journal, parent, node)
            if row is None or len(row[1].encode()) > _MAX_SCOPE_DECLARATION_BYTES:
                continue
            scope_id, raw = row
            declaration = json.loads(raw)
            fact = journal.latest_execution_commit(scope_id)
            if fact is None or fact['prepared'] is None:
                continue
            action = observation_action(journal, scope_id, declaration, binding,
                                        directory=directory)
            try:
                receipt = _BatchSink(action, declaration).inspect(
                    fact['prepared']['sinks']['lance'], commit_write_context(action, fact['commit_id']))
            except RecoveryContractError as exc:
                raise PreparedPublicationUnresolved(str(exc)) from exc
            if receipt is None:
                continue
            candidate = receipt['data']['reference']
            if candidate is None:
                continue
            if (receipt.get('commit_id') != fact['commit_id'] or receipt.get('status') != 'committed'
                    or candidate['uri'] != uri
                    or declaration['schema_identity'] != schema_identity):
                raise PublicationMismatch('Prepared native publication differs from its sealed output')
            if reference is not None:
                raise PublicationMismatch('Prepared output has ambiguous native owners')
            reference = candidate
    return reference


def object_namespace(run_id, logical_root):
    """The object-root record a run with ``run_id`` owns under ``logical_root``.

    Hosts compare journal ``object_roots`` records against this instead of
    fabricating a context for ``object_resources.namespace_record``.
    """
    from .object_resources import namespace_record
    return namespace_record(SimpleNamespace(ref=SimpleNamespace(run_id=run_id)), logical_root)
