"""Training publication implementation."""
from __future__ import annotations

import json
from demiflow.execution.contracts.training import FitRequest, ProjectObject
from demiflow.execution.contracts.recovery_contract import identity
from demiflow.execution.journal.run_reader import read_run
from demiflow.execution.journal.training_index import FitConflict, TrainingIndex
from demiflow.execution.storage.model_store import ModelStore


def consumer_id(action, occurrence, kind, key=None):
    return identity([action.context.ref.run_id, action.action_id, occurrence, kind, key])

def confirm_table_consumption(action, occurrence, binding, receipt):
    """Finish the retention transaction even if the driver died after final ACK."""
    from demiflow.execution.journal.run_reader import fit_table_prefix
    run = action.context
    owner = consumer_id(action, occurrence, 'table', action.action_id)
    intent = run.journal.source_binding(owner)
    if not intent or intent.get('producer') is None:
        raise FitConflict("No original pending training reference for this scoring table")
    if type(receipt) is not dict or receipt.get('status') != 'committed':
        raise FitConflict("Scoring table lacks a committed native receipt")
    data = receipt.get('data') or {}
    version = data.get('committed_version')
    if type(version) is not int or version < 1 or data.get('uri') != intent['provenance']['uri']:
        raise FitConflict("Scoring table version or URI differs from its original training reference")
    fact = run.journal.execution_commit(receipt['commit_id'])
    if not fact or fact['status'] != 'acked' or fact['action_id'] != action.action_id:
        raise FitConflict("No confirmed original action commit for scoring-table retention")
    # A native Lance receipt may be wrapped by the dataflow or object-row sink.
    found = False
    for outer in fact['receipts'].values():
        publication = outer['data']
        rows = publication.get('rows')
        if (outer == receipt or publication.get('native_receipt') == receipt
                or type(rows) is dict and rows.get('native_receipt') == receipt):
            found = True
            break
    if not found:
        raise FitConflict("Scoring table receipt differs from the original sink transaction")
    with run.journal.fit_guard(run.fence, action.action_id, occurrence):
        index = TrainingIndex(binding)
        index.hold(owner, intent['instance'],kind='table',
                   target=dict(provenance=intent['provenance'], receipt=receipt))
    summary = dict(action_id=action.action_id, attempt=run.attempt, occurrence=occurrence,
                   instance=intent['instance'], fit_key=intent['fit_key'], producer=intent['producer'],
                   uri=data['uri'], version=version, commit_id=receipt['commit_id'],
                   manifest_digest=intent['manifest'], base_model_digest=intent['base_model'],
                   provenance_id=owner, selection_digest=identity(intent['provenance']['selection']),
                   selection_count=len(intent['provenance']['selection']),
                   selected_steps=[s['step'] for s in intent['provenance']['selection'][:8]])
    run.journal.bind_source(run.fence, fit_table_prefix(run.attempt, action.action_id)+occurrence, summary)

def model_scope(row):
    return identity(['model_manifest', row['action'], row['occurrence'], row['instance']])

def _receipt(row, manifest, commit_id):
    return dict(commit_id=commit_id, sink_id='model_manifest', status='committed',
                data=dict(instance=row['instance'], fit_key=row['fit_key'],
                          request_key=row['request_key'], manifest=manifest.to_dict()))

def confirmed_manifest(row):
    ref = ProjectObject.from_dict(json.loads(row['manifest']))
    scope = model_scope(row)
    expected = identity([row['run_id'], scope, 1])
    if expected != row['commit_id']:
        raise FitConflict("Project index has an inconsistent producer commit ID")
    with read_run(row['run_dir']) as reader:
        receipt = reader.fit_publication(row['run_id'], row['action'], scope, expected)
    if receipt is None:
        return None
    if receipt != _receipt(row, ref, expected):
        raise FitConflict("Published model differs from its producer receipt")
    return ref

def model_selection(result, step, predictor_digest, manifest_digest):
    snapshot, = result.select((step,))
    data = snapshot.to_dict()
    del data['resume_files']
    if type(predictor_digest) is not str or len(predictor_digest) != 64 or any(
            c not in '0123456789abcdef' for c in predictor_digest):
        raise FitConflict("Invalid sealed predictor digest")
    return dict(instance_id=result.instance_id, fit_key=result.fit_key,
                manifest_digest=manifest_digest, predictor_digest=predictor_digest,
                step=step, snapshot=data)

def read_consumed_fit(action, occurrence, project_id, binding):
    """Verify a final row cut against the original model owner, without a live trainer."""
    source = action.context.journal.source_binding(consumer_id(action,occurrence,'confirmed'))
    if not source or source.get('schema') != 'demiflow.fit_consumption.v1' or (
            source['action'],source['occurrence']) != (action.action_id,occurrence):
        raise FitConflict("No original model consumption proof for the final row checkpoint")
    index = TrainingIndex(binding,readonly=True)
    row = index.read(source['instance'])
    if (not row or row['project'] != project_id or row['state'] != 'published'
            or (row['fit_key'],row['run_id'],row['action'],row['commit_id']) != (
                source['fit_key'],source['producer_run'],source['producer_action'],source['producer_commit'])):
        raise FitConflict("Final row checkpoint references a different model producer")
    manifest = confirmed_manifest(row)
    if manifest is None or manifest.to_dict() != source['manifest']:
        raise FitConflict("Final row checkpoint lacks confirmed model evidence")
    request = FitRequest.from_dict(json.loads(row['request']))
    result = ModelStore(index, check_stop=action.context.check_stop).load(manifest,request)
    return result, manifest

def consumed_model_digest(result, manifest, step, predictor_digest):
    return identity(model_selection(result,step,predictor_digest,manifest.sha256))

def read_parent(project_id, binding, manifest_digest, step, *, consumer_instance=None, check_stop=None):
    """Resolve an explicit, confirmed parent within the same project authority."""
    index = TrainingIndex(binding, readonly=True)
    row = index.find_manifest(project_id, manifest_digest)
    if not row:
        raise FitConflict("No published parent manifest in this project")
    ref = confirmed_manifest(row)
    if not ref or ref.sha256 != manifest_digest:
        raise FitConflict("Parent manifest lacks the original producer's confirmation")
    request = FitRequest.from_dict(json.loads(row['request']))
    result = ModelStore(index, check_stop=check_stop).load(ref, request)
    snapshot, = result.select((step,))
    if not snapshot.resume_files:
        raise FitConflict("Selected parent has no resumable optimizer state")
    if consumer_instance is not None:
        TrainingIndex(binding).hold(identity(['parent',consumer_instance,step]), result.instance_id, kind='parent',
                   target=dict(consumer=consumer_instance, step=step, manifest=manifest_digest))
    return snapshot

class ModelManifestSink:
    def __init__(self, index, row):
        self.index = index
        self.row = row

    def prepare(self, blocks, context):
        manifest, = blocks
        return _receipt(self.row, manifest, context.commit_id)['data']

    def _validate(self, prepared, context):
        row = self.row
        manifest = ProjectObject.from_dict(prepared['manifest'])
        expected = _receipt(row, manifest, context.commit_id)
        if (prepared != expected['data'] or context.action_id != model_scope(row)
                or context.run_id != row['run_id']):
            raise FitConflict("Prepared model manifest belongs to a different producer")
        return manifest, expected

    def inspect(self, prepared, context):
        manifest, receipt = self._validate(prepared, context)
        row = self.index.read(self.row['instance'])
        if row and row['state'] == 'published' and (row['commit_id'] == context.commit_id
                and json.loads(row['manifest']) == manifest.to_dict()):
            return receipt
        if row and row['state'] == 'claimed' and row['request_key'] == self.row['request_key']:
            return None
        raise FitConflict("Cannot establish the original model publication outcome")

    def commit(self, prepared, context):
        manifest, receipt = self._validate(prepared, context)
        row = self.row
        self.index.publish(row['instance'], row['epoch'], row['request_key'], context.commit_id, manifest)
        return receipt

    def abort(self, prepared, context):
        # Imported content belongs to the project and may have multiple users.
        pass
