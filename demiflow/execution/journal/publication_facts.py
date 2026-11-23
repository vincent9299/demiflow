"""Publication facts implementation."""
from __future__ import annotations

import json
from types import SimpleNamespace
from demiflow.execution.recovery_contract import RecoveryContractError
from demiflow.execution.contracts.errors import PublicationMismatch, PreparedPublicationUnresolved
from demiflow.execution.commit import commit_write_context
from demiflow.execution.journal.run_reader import _check_deadline, _publication_snapshot, _latest_stream_lance_scope
_MAX_SCOPE_DECLARATION_BYTES = 65536


def inspect_native_write_receipt(journal, action_id):
    """Project a v3 native publication without starting code or changing history."""
    from demiflow.execution.seal.sealed_plans import sealed_read_bounds
    from demiflow.execution.storage.objects import readonly_run_store
    from demiflow.execution.seal.plan import load_dataflow
    binding = journal.recovery_identity()
    action = journal.execution_action(action_id)
    facts = journal.execution_commits(action_id)
    if binding is None or action is None or not facts:
        return None
    fact = facts[-1]
    if fact['prepared'] is None or fact['intent']['sinks'] != ['write_lance']:
        return None
    seal = journal.source_binding(action_id + '-dataflow-sealed')
    if seal is None:
        return None
    bounds = sealed_read_bounds()
    store = readonly_run_store(journal.directory, store_id=seal['physical_ref']['store_id'], bounds=bounds)
    graph, physical, _ = load_dataflow(journal.directory, action_id, store=store, bounds=bounds, _reader=journal)
    native_action = observation_action(journal, action_id, action['declaration'], binding)
    prepared = fact['prepared']['sinks']['write_lance']
    from demiflow.execution.outputs.sinks import inspect_native_publication
    return inspect_native_publication(native_action, prepared, store=store, bounds=physical.bounds,
        make_context=lambda: commit_write_context(native_action, fact['commit_id']))


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
    from demiflow.execution.pipeline.run_context import RunAction
    context = observation_context(journal, binding, directory=directory, attempt=attempt)
    if context is None:
        return None
    return RunAction(context, action_id, declaration)

def inspect_write_receipt(journal, action_id):
    """Read-only projection using the same native sink inspection as the driver.

    Covers both publication generations: a v3 ``write_lance`` terminal is
    delegated to ``dataflow_sinks.inspect_native_write_receipt``; an original
    ``lance`` sink (optionally composed with object rows) is inspected here.
    Returns the native receipt data, or ``None`` when nothing conclusive exists.
    """
    from demiflow.execution.outputs.lance_sink import inspect_prepared_write, spec_from_identity
    binding = journal.recovery_identity()
    if binding is None:
        return None
    action = journal.execution_action(action_id)
    if action is None:
        return None
    facts = journal.execution_commits(action_id)
    if facts and facts[-1]['intent']['sinks'] == ['write_lance']:
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
    receipt = inspect_prepared_write(native_action, spec, prepared,
        make_context=lambda: commit_write_context(native_action, fact['commit_id']))
    composite = type(prepared) is dict and {'objects', 'rows'} <= set(prepared)
    if receipt is None:
        return None
    return receipt['data']['rows'] if composite else receipt['data']

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
    from demiflow.execution.outputs.lance_sink import _BatchSink
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

def _recheck(directory, action_id, attempt, deadline, before):
    after, _, _ = _publication_snapshot(directory, action_id, attempt, deadline)
    if any(after[k] != before[k] for k in ('attempt','current_attempt','revision','action_sequence','evidence','retained_from_attempt')):
        raise PublicationMismatch('Publication identity changed while reading')
    _check_deadline(deadline)

def _recording(action_id, attempt, kind, value):
    from demiflow.execution.recovery_contract import document
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
    from demiflow.lance.model import LanceWriteReceipt
    from demiflow.execution.outputs.lance_sink import verify_target_write_receipt
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
    actual = verify_target_write_receipt(parsed, native['declaration'] if native is not None else None,
        intent, expected_uri=expected_uri, require_current_head=require_current_head,
        deadline=deadline, metadata_reader=metadata_reader, check_deadline=_check_deadline)
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
    from demiflow.execution.outputs.lance_sink import verify_target_stage
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
    actual = verify_target_stage(expected_uri=expected_uri, version=reference['version'],
        expected_schema_identity=expected_schema_identity, require_current_head=require_current_head,
        deadline=deadline, metadata_reader=metadata_reader, check_deadline=_check_deadline)
    _recheck(directory, action_id, attempt, deadline, before)
    return _verified(before, action_id, expected_uri, reference['version'], actual,
                     schema_identity=expected_schema_identity, checkpoint_node=expected_checkpoint_node,
                     result_kind='recovered_result' if recording or selected.get('stage_recovered') else 'result',
                     recording=recording)
