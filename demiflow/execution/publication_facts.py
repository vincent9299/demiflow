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
    'object_namespace', 'action_recovery_facts', 'verify_write_receipt', 'verify_published_output',
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
    from demiflow.execution.outputs.lance_sink import LanceSink, spec_from_identity
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
        from demiflow.execution.outputs.object_sink import ObjectRowsSink
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
            "SELECT scope_id,substr(declaration,1,65537) FROM runtime_commit_scopes WHERE parent_action_id=? "
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
    from demiflow.execution.storage.reading import read_plan_artifact
    from demiflow.execution.journal.dataflow_stage_facts import stage_binding
    from .dataflow_stream_lance import _BatchSink
    from demiflow.execution.journal.run_reader import _ReadonlyJournal
    journal = _ReadonlyJournal(directory, attempt=getattr(journal, 'attempt', None))
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
    from demiflow.execution.storage.object_resources import namespace_record
    return namespace_record(SimpleNamespace(ref=SimpleNamespace(run_id=run_id)), logical_root)


def _check_deadline(deadline):
    import time
    import math
    if deadline is not None and (type(deadline) not in (int, float) or not math.isfinite(deadline)):
        raise RecoveryContractError('Invalid publication read deadline')
    if deadline is not None and time.monotonic() >= deadline:
        raise RecoveryContractError('Publication read deadline exceeded')


def _publication_snapshot(directory, action_id, attempt, deadline):
    from demiflow.execution.journal.run_reader import read_run
    _check_deadline(deadline)
    with read_run(directory, deadline=deadline) as reader:
        state, _ = reader.state(attempt)
        selected = state.get('attempt', 1)
        projection = reader.output_projection(selected, action_id)
        current, effective, _ = reader._output_selection(selected, action_id)
        return projection, current, effective


def _recheck(directory, action_id, attempt, deadline, before):
    after, _, _ = _publication_snapshot(directory, action_id, attempt, deadline)
    if any(after[k] != before[k] for k in ('attempt','current_attempt','revision','action_sequence','evidence','retained_from_attempt')):
        raise PublicationMismatch('Publication identity changed while reading')
    _check_deadline(deadline)


def action_recovery_facts(directory, action_id, *, attempt=None, deadline=None):
    """Journal-only recovery eligibility; never proof that a target is absent."""
    from demiflow.execution.journal.run_reader import read_run, _ReadonlyJournal
    from .run_context import create_target_absent_binding
    _check_deadline(deadline)
    # The readonly protocol shares this single transaction for the entire fact.
    with read_run(directory, deadline=deadline) as reader:
        from contextlib import contextmanager
        journal = _ReadonlyJournal(directory, attempt=attempt, deadline=deadline)
        @contextmanager
        def connection():
            yield reader.db
        journal._connection = connection
        state, current = reader.state(attempt)
        selected = state.get('attempt', 1)
        facts, _, _ = reader._output_selection(selected, action_id)
        native = journal.execution_action(action_id)
        identity = journal.recovery_identity()
        binding = (create_target_absent_binding(identity['run_id'], action_id, native['declaration'], facts.get('intent'))
                   if native is not None and identity is not None else None)
        absence = journal.source_binding(action_id + '-create-target-absent')
        if absence is not None:
            from demiflow.execution.outputs.lance_sink import spec_from_identity
            if native is None or identity is None or type(absence) is not dict:
                raise PublicationMismatch('Create absence has no original native action')
            spec = spec_from_identity(native['declaration']['sink'])
            recorded = create_target_absent_binding(identity['run_id'], action_id, native['declaration'],
                dict(kind='write_table', spec=spec.to_dict(), request_hash=absence.get('request_hash'),
                     operation_id=absence.get('operation_id')))
            if recorded is None or absence != recorded:
                raise PublicationMismatch('Create absence binding differs from original action')
        restricted = journal.source_binding(action_id + '-state-recovery-restriction')
        return dict(action_id=action_id, attempt=selected, current_attempt=current,
                    revision=(reader.document('execution_state', 65536) or {}).get('revision', 0),
                    native_action_present=native is not None,
                    native_completed=native is not None and native['result'] is not None,
                    recovery_restricted=native is not None and native['result'] is None and restricted is not None,
                    absence_eligible=binding is not None and journal.latest_execution_commit(action_id) is None,
                    absence_recorded=absence is not None)


def _recording(action_id, attempt, kind, value):
    from .recovery_contract import document
    if kind not in ('recovered', 'stage_recovered'):
        raise PublicationMismatch('Unsupported native recording')
    document(value, 256 * 1024)
    return dict(action_id=action_id, attempt=attempt, kind=kind, value=value)


def _verified(projection, action_id, uri, version, actual, *, schema_identity=None,
              checkpoint_node=None, result_kind='result', recording=None):
    return dict(action_id=action_id, attempt=projection['retained_from_attempt'] or projection['attempt'],
                current_attempt=projection['current_attempt'], revision=projection['revision'],
                action_sequence=projection['action_sequence'], uri=uri, version=version,
                schema_hash=actual['schema_hash'], row_count=actual['row_count'],
                schema_identity=schema_identity, checkpoint_node=checkpoint_node,
                reference=dict(uri=uri, version=version), retained_from_attempt=projection['retained_from_attempt'],
                result_kind=result_kind, recording=recording)


def verify_write_receipt(directory, action_id, *, expected_uri, attempt=None, recover=False,
                         require_current_head=False, deadline=None, metadata_reader=None):
    """Explicit fixed-version verification. Safe inside a running record callback."""
    from demiflow.execution.journal.run_reader import _ReadonlyJournal
    from demiflow.execution.outputs.lance_sink import spec_from_identity
    from ..lance.model import LanceWriteReceipt
    from ..lance.preview import snapshot_metadata
    from ..lance.storage import inspect_lance, open_lance_dataset
    before, _, selected = _publication_snapshot(directory, action_id, attempt, deadline)
    journal = _ReadonlyJournal(directory, attempt=before['attempt'], deadline=deadline)
    receipt = selected.get('recovered') or selected.get('receipt')
    inspected = False
    if (not receipt or receipt.get('status') != 'committed') and recover:
        receipt = inspect_write_receipt(journal, action_id)
        inspected = receipt is not None
    if not receipt or receipt.get('status') != 'committed':
        return None
    parsed = LanceWriteReceipt.from_dict(receipt)
    native = journal.execution_action(action_id)
    intent = selected.get('intent')
    if native is not None:
        if native['declaration'].get('kind') != 'write_table':
            raise PublicationMismatch('Output is not an original native write action')
        request_hash = spec_from_identity(native['declaration']['sink']).content_hash
        if intent is not None and intent.get('request_hash') != request_hash:
            raise PublicationMismatch('Write observation differs from original declaration')
    elif intent is not None:
        request_hash = intent['request_hash']
    else:
        raise PublicationMismatch('Write receipt has no original request identity')
    if parsed.uri != expected_uri or parsed.request_hash != request_hash:
        raise PublicationMismatch('Write receipt identity differs')
    _check_deadline(deadline)
    actual = (metadata_reader or snapshot_metadata)(expected_uri, parsed.committed_version)
    _validate_metadata(actual, parsed.committed_version)
    _check_deadline(deadline)
    if actual['schema_hash'] != parsed.schema_hash or actual['row_count'] != parsed.written_rows:
        raise PublicationMismatch('Committed table differs from receipt')
    if require_current_head:
        if inspect_lance(expected_uri).resolved_version != parsed.committed_version:
            raise PublicationMismatch('Output head differs from owned version')
        if intent is None:
            raise PublicationMismatch('Committed write has no original intent')
        dataset = open_lance_dataset(expected_uri, parsed.committed_version, ())
        transaction = dataset.read_transaction(parsed.committed_version)
        if transaction.transaction_properties.get('__lance_commit_message') != 'demiflow-lance:' + intent['operation_id']:
            raise PublicationMismatch('Output operation marker differs')
    if journal.execution_action(action_id) != native:
        raise PublicationMismatch('Native write identity changed while reading')
    _recheck(directory, action_id, attempt, deadline, before)
    recording = _recording(action_id, before['attempt'], 'recovered', receipt) if inspected else None
    return _verified(before, action_id, expected_uri, parsed.committed_version, actual, recording=recording)


def verify_published_output(directory, action_id, *, expected_uri, expected_schema_identity,
                            publication=None, expected_checkpoint_node=None, report_digest=None,
                            parents=None, attempt=None, recover=False, require_current_head=False,
                            deadline=None, metadata_reader=None):
    """Verify a checkpoint or original native child ACK and its fixed table."""
    from demiflow.execution.journal.run_reader import _ReadonlyJournal
    from demiflow.execution.journal.dataflow_stage_facts import native_stage_commit, iter_native_stage_commits
    from ..lance.preview import snapshot_metadata
    from ..lance.storage import inspect_lance
    if publication not in (None, 'native_stage_v3'):
        raise PublicationMismatch('Unsupported publication contract')
    before, _, selected = _publication_snapshot(directory, action_id, attempt, deadline)
    journal = _ReadonlyJournal(directory, attempt=before['attempt'], deadline=deadline)
    commit = selected.get('stage_recovered') or selected.get('stage_commit')
    recording = None
    if publication == 'native_stage_v3' and recover:
        if type(parents) not in (tuple, list) or len(parents) > 32:
            raise PublicationMismatch('Native recovery needs bounded original parents')
        latest = None
        for fact in iter_native_stage_commits(journal, directory, parents=parents, report_digest=report_digest):
            _check_deadline(deadline)
            if fact['report_node_id'] != action_id:
                continue
            if latest is not None and latest['parent_action_id'] != fact['parent_action_id']:
                raise PublicationMismatch('Native output has multiple parent authorities')
            if latest is None or fact['ordinal'] >= latest['ordinal']:
                latest = fact
        if latest is not None:
            commit = latest
            recording = _recording(action_id, before['attempt'], 'stage_recovered', latest)
    if commit is None:
        return None
    if publication == 'native_stage_v3':
        fact = native_stage_commit(journal, directory, scope_id=commit.get('scope_id'),
                                   commit_id=commit.get('commit_id'), report_digest=report_digest)
        if fact is None or fact != commit or fact['report_node_id'] != action_id:
            raise PublicationMismatch('Native stage differs from original ACK and sealed identity')
    elif commit.get('checkpoint_node') != expected_checkpoint_node:
        raise PublicationMismatch('Checkpoint identity differs')
    reference = commit.get('reference')
    if (type(reference) is not dict or reference.get('uri') != expected_uri
            or type(reference.get('version')) is not int or reference['version'] < 1
            or commit.get('schema_identity') != expected_schema_identity):
        raise PublicationMismatch('Stage publication differs from sealed output')
    _check_deadline(deadline)
    actual = (metadata_reader or snapshot_metadata)(expected_uri, reference['version'])
    _validate_metadata(actual, reference['version'])
    _check_deadline(deadline)
    if actual['schema_identity'] != expected_schema_identity:
        raise PublicationMismatch('Committed stage schema differs')
    if require_current_head and inspect_lance(expected_uri).resolved_version != reference['version']:
        raise PublicationMismatch('Stage head differs from owned version')
    _recheck(directory, action_id, attempt, deadline, before)
    return _verified(before, action_id, expected_uri, reference['version'], actual,
                     schema_identity=expected_schema_identity, checkpoint_node=expected_checkpoint_node,
                     result_kind='recovered_result' if recording or selected.get('stage_recovered') else 'result',
                     recording=recording)


def _validate_metadata(actual, version):
    if (type(actual) is not dict or type(actual.get('version')) is not int or actual['version'] != version
            or type(actual.get('schema_hash')) is not str or not actual['schema_hash']
            or type(actual.get('schema_identity')) is not str or not actual['schema_identity']
            or type(actual.get('row_count')) is not int or actual['row_count'] < 0):
        raise PublicationMismatch('Invalid fixed-version metadata response')
