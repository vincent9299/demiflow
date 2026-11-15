"""Checked result projection for native SaveLance child commits."""
from demiflow.forge.schema import ForgeError
from demiflow.execution.dataflow_stage_facts import native_stage_commit, iter_native_stage_commits


def verify(target, journal, output, commit, *, report_digest):
    if type(commit) is not dict:
        raise ForgeError('Native stage output has no commit evidence')
    from demiflow.execution.recovery_contract import RecoveryContractError
    try:
        fact = native_stage_commit(journal, target, scope_id=commit.get('scope_id'),
                                  commit_id=commit.get('commit_id'), report_digest=report_digest)
    except RecoveryContractError as exc:
        raise ForgeError('Native stage evidence is invalid: ' + str(exc)) from exc
    if (fact is None or fact != commit or fact['report_node_id'] != output['action_id']
            or fact['reference']['uri'] != output['uri']
            or fact['schema_identity'] != output['schema_identity']):
        raise ForgeError('Native stage output differs from its original ACK and sealed identity')
    return fact['reference']


def recover(target, journal, manifest):
    outputs = {item['action_id']: item for item in manifest['outputs'].values()
               if item.get('publication') == 'native_stage_v3'}
    if not outputs:
        return
    from .manager import _report
    report = _report(target, manifest)
    latest = {}
    for fact in iter_native_stage_commits(journal, target, parents=report['actions'],
                                         report_digest=manifest['report_digest']):
        node = fact['report_node_id']
        if node in outputs:
            verify(target, journal, outputs[node], fact, report_digest=manifest['report_digest'])
            previous = latest.get(node)
            if previous is not None and previous['parent_action_id'] != fact['parent_action_id']:
                raise ForgeError('Native output has more than one parent execution authority')
            if previous is None or fact['ordinal'] >= previous['ordinal']:
                latest[node] = fact
    for node, fact in latest.items():
        journal.action_event(node, 'stage_recovered', fact)


def admit_prepared(target, journal, manifest, output):
    """Prove a prepared native publication may enter original recovery.

    This is only admission. No ACK, UI result, scope or receipt is written, and
    no native commit/recover method is called. Unknown ownership stays closed.
    """
    from types import SimpleNamespace
    from demiflow.execution.sealed_plans import read_plan_artifact
    from demiflow.execution.dataflow_stage_facts import stage_binding
    from demiflow.execution.dataflow_stream_lance import _BatchSink
    from demiflow.execution.run_context import RunAction
    from demiflow.execution.commit import CommitCoordinator
    from demiflow.execution.recovery_contract import RecoveryContractError
    from .manager import _report
    import json
    binding = journal.recovery_identity()
    if binding is None:
        return None
    reference = None
    for parent in _report(target, manifest)['actions']:
        bridge_ref = journal.source_binding(parent + '-identity-bridge')
        if bridge_ref is None:
            continue
        bridge = read_plan_artifact(target, bridge_ref, max_bytes=1024 * 1024)
        for entry in bridge.get('entries', ()):
            if entry.get('report_node_id') != output['action_id']:
                continue
            node = entry['occurrence_id']
            stage_binding(journal, target, parent, node, manifest['report_digest'])
            with journal._connection() as db:
                if not db.execute("SELECT 1 FROM sqlite_master WHERE name='runtime_commit_scopes'").fetchone():
                    continue
                row = db.execute("SELECT scope_id,declaration FROM runtime_commit_scopes WHERE parent_action_id=? "
                    "AND json_extract(declaration,'$.node_id')=? AND json_extract(declaration,'$.schema')=? "
                    "ORDER BY CAST(json_extract(declaration,'$.request.ordinal') AS INTEGER) DESC,scope_id DESC LIMIT 1",
                    (parent,node,'demiflow.stream_lance.v3')).fetchone()
            if row is None or len(row[1].encode()) > 65536:
                continue
            scope_id, raw = row
            declaration = json.loads(raw)
            fact = journal.latest_execution_commit(scope_id)
            if fact is None or fact['prepared'] is None:
                continue
            context = SimpleNamespace(directory=target, limits=binding['limits'], journal=journal,
                ref=SimpleNamespace(run_id=binding['run_id']),
                attempt=journal.execution_status().get('attempt'), fence='observation-only')
            action = RunAction(context, scope_id, declaration)
            try:
                receipt = _BatchSink(action, declaration).inspect(fact['prepared']['sinks']['lance'],
                    CommitCoordinator(action)._context(fact['commit_id']))
            except RecoveryContractError as exc:
                raise ForgeError('Native prepared publication is unresolved: ' + str(exc)) from exc
            if receipt is None:
                continue
            candidate = receipt['data']['reference']
            if candidate is None:
                continue
            if (receipt.get('commit_id') != fact['commit_id'] or receipt.get('status') != 'committed'
                    or candidate['uri'] != output['uri']
                    or declaration['schema_identity'] != output['schema_identity']):
                raise ForgeError('Prepared native publication differs from its sealed output')
            if reference is not None:
                raise ForgeError('Prepared output has ambiguous native owners')
            reference = candidate
    return reference
