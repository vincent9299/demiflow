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
    """Check native publication evidence; permission and presentation stay here."""
    from demiflow.execution.publication_facts import (
        admit_prepared_stage, PreparedPublicationUnresolved, PublicationMismatch)
    from .manager import _report
    if journal.recovery_identity() is None:
        return None
    try:
        return admit_prepared_stage(journal, target,
            parents=_report(target, manifest)['actions'], report_node_id=output['action_id'],
            uri=output['uri'], schema_identity=output['schema_identity'],
            report_digest=manifest['report_digest'])
    except PreparedPublicationUnresolved as exc:
        raise ForgeError('Native prepared publication is unresolved: ' + str(exc)) from exc
    except PublicationMismatch as exc:
        raise ForgeError(str(exc)) from exc
