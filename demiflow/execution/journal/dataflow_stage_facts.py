"""Read-only projection of original native stage ACKs and sealed identities.

No table-head lookup, reconciliation, publication or independent ledger occurs
here. These facts are views of RunJournal's child scopes and CommitCoordinator
receipts, suitable for the checked-product adapter and historical result reads.
"""
import json
from demiflow.execution.contracts.recovery_contract import RecoveryContractError
from demiflow.execution.storage.reading import read_plan_artifact

SCHEMA = 'demiflow.native_stage_commit.v3'
_NATIVE = 'demiflow.stream_lance.v3'


def stage_binding(journal, directory, parent, node, report_digest):
    seal = journal.source_binding(parent + '-dataflow-sealed')
    original = journal.recovery_identity()
    if (not seal or not original or seal.get('schema') not in {'demiflow.dataflow_seal.v3', 'demiflow.dataflow_seal.v4'}
            or seal.get('action_id') != parent
            or seal.get('run_id') != original['run_id']):
        raise RecoveryContractError('Native stage has no original sealed action')
    ref = journal.source_binding(parent + '-identity-bridge')
    if ref is None:
        raise RecoveryContractError('Native stage has no sealed report identity bridge')
    bridge = read_plan_artifact(directory, ref, max_bytes=1024 * 1024)
    if (bridge.get('schema') != 'demiflow.identity_bridge.v1'
            or bridge.get('action_id') != parent or bridge.get('dag_id') != seal['dag_id']
            or bridge.get('report_digest') != report_digest):
        raise RecoveryContractError('Native stage report identity bridge differs')
    entries = bridge.get('entries')
    if type(entries) is not list or len(entries) > 1024:
        raise RecoveryContractError('Native stage bridge exceeds its bounded contract')
    matches = [entry for entry in entries if entry.get('occurrence_id') == node]
    if len(matches) != 1:
        raise RecoveryContractError('Native stage has no unique report occurrence')
    return seal, matches[0]


def native_stage_commit(journal, directory, *, scope_id, commit_id, report_digest):
    with journal._connection() as db:
        row = db.execute('SELECT parent_action_id,substr(CAST(declaration AS BLOB),1,65537) FROM runtime_commit_scopes '
                         'WHERE scope_id=?', (scope_id,)).fetchone()
    if row is None or len(row[1]) > 65536:
        raise RecoveryContractError('Native stage scope is missing or oversized')
    parent, raw = row
    declaration = json.loads(raw)
    if declaration.get('schema') != _NATIVE:
        raise RecoveryContractError('Scope is not a native stream publication')
    node = declaration['node_id']
    seal, binding = stage_binding(journal, directory, parent, node, report_digest)
    fact = journal.execution_commit(commit_id)
    if (not fact or fact['action_id'] != scope_id or fact['sequence'] != 1
            or fact['status'] != 'acked'):
        raise RecoveryContractError('Native stage publication has no original ACK')
    receipt = fact['receipts'].get('lance', {})
    data = receipt.get('data', {})
    if (receipt.get('commit_id') != commit_id or receipt.get('sink_id') != 'lance'
            or receipt.get('status') != 'committed'
            or data.get('request_key') != declaration['request_key']):
        raise RecoveryContractError('Native stage receipt differs from its original scope')
    reference = data.get('reference')
    if reference is None:
        return None
    if (type(reference) is not dict or set(reference) != {'uri', 'version'}
            or type(reference['uri']) is not str or type(reference['version']) is not int
            or reference['version'] < 1):
        raise RecoveryContractError('Native stage receipt has no fixed table reference')
    return dict(schema=SCHEMA, parent_action_id=parent, node_id=node,
                scope_id=scope_id, commit_id=commit_id, dag_id=seal['dag_id'],
                report_node_id=binding['report_node_id'], report_occurrence=binding['report_occurrence'],
                report_digest=report_digest, schema_identity=declaration['schema_identity'],
                ordinal=declaration['request']['ordinal'], reference=reference)


def iter_native_stage_commits(journal, directory, *, parents, report_digest):
    """Yield original ACKs in source ordinal order, using bounded pages."""
    if len(parents) > 32:
        raise RecoveryContractError('Native stage parent action budget exceeded')
    count = 0
    limit = (journal.recovery_identity() or {}).get('limits', {}).get('max_commits', 16384)
    with journal._connection() as db:
        if not db.execute("SELECT 1 FROM sqlite_master WHERE name='runtime_commit_scopes'").fetchone():
            return
    for parent in parents:
        previous = (-1, '')
        while True:
            with journal._connection() as db:
                rows = db.execute("SELECT s.scope_id,c.commit_id,CAST(json_extract(s.declaration,'$.request.ordinal') AS INTEGER) ord "
                    "FROM runtime_commit_scopes s JOIN runtime_commits c ON c.action_id=s.scope_id "
                    "WHERE s.parent_action_id=? AND json_extract(s.declaration,'$.schema')=? AND c.status='acked' "
                    "AND (ord>? OR (ord=? AND s.scope_id>?)) ORDER BY ord,s.scope_id LIMIT 64",
                    (parent,_NATIVE,previous[0],previous[0],previous[1])).fetchall()
            if not rows:
                break
            for scope, commit, ordinal in rows:
                count += 1
                if type(ordinal) is not int or ordinal < 0 or count > limit:
                    raise RecoveryContractError('Native stage history exceeds bounded scope contract')
                previous = (ordinal, scope)
                fact = native_stage_commit(journal, directory, scope_id=scope,
                    commit_id=commit, report_digest=report_digest)
                if fact is not None:
                    yield fact
