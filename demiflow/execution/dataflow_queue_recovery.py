"""Bounded native queue references in the original acknowledged dataflow cut.

This is an adapter, not a lease or commit authority. QueueChannel and the
original acquisition/final commits alone decide ownership and paid effects.
"""
from dataclasses import asdict
import json
from types import SimpleNamespace

from .contracts.dataflow import ArtifactRef
from .contracts.dataflow_errors import DataflowError
from .recovery_contract import identity

SCHEMA = 'demiflow.native_queue_cut.v1'


def snapshot_claims(service):
    if service._source is None:
        return None
    with service._lock:
        claims = tuple(service._claims.values())
        if len(claims) > service._max_inflight:
            raise DataflowError('BUDGET_EXCEEDED', 'Native queue cut exceeds claim window')
        entries = [dict(delivery_ref=asdict(c.delivery_ref), row_ref=asdict(c.row_ref),
                        receipt_ref=None if c.receipt_row_ref is None else asdict(c.receipt_row_ref))
                   for c in sorted(claims, key=lambda c: c.delivery.task_id)]
    value = dict(schema=SCHEMA, binding=asdict(service.binding), claims=entries)
    _check_size(service, value)
    return value


def _check_size(service, value):
    maximum = min(service._services.bounds.max_state_bytes,
                  service._services.bounds.max_control_bytes)
    if len(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()) > maximum:
        raise DataflowError('BUDGET_EXCEEDED', 'Native queue cut exceeds state/control budget')


def restore_claims(service, value, *, has_cut):
    source = service._source
    if source is None:
        if value is not None:
            raise DataflowError('INCOMPATIBLE_HISTORY', 'Queue cut belongs to a different source')
        return
    if value is None:
        if has_cut:
            raise DataflowError('INCOMPATIBLE_HISTORY', 'Historical dataflow cut lacks native queue ownership references')
        return
    _check_size(service, value)
    if (type(value) is not dict or set(value) != {'schema', 'binding', 'claims'}
            or value['schema'] != SCHEMA or value['binding'] != asdict(service.binding)
            or type(value['claims']) is not list or len(value['claims']) > service._max_inflight):
        raise DataflowError('INCOMPATIBLE_HISTORY', 'Native queue cut differs from sealed binding/window')
    from .dataflow_queue import _Claim
    from .queue_commit import QueueCommitStore
    from .queue_history import inspect_result
    restored = {}
    for entry in value['claims']:
        if type(entry) is not dict or set(entry) != {'delivery_ref', 'row_ref', 'receipt_ref'}:
            raise DataflowError('CORRUPT_ARTIFACT', 'Invalid native queue cut entry')
        ref, row_ref = ArtifactRef(**entry['delivery_ref']), ArtifactRef(**entry['row_ref'])
        descriptor = service._read_json(ref, max_bytes=service._services.bounds.max_control_bytes)
        if (type(descriptor) is not dict or set(descriptor) !=
                {'generation','task_id','fingerprint','epoch','worker_id','lease_until','domain'}
                or descriptor['generation'] != source.generation
                or descriptor['worker_id'] != source.worker_id
                or descriptor['domain'] != source.namespace.domain
                or descriptor['task_id'] in restored):
            raise DataflowError('FENCED', 'Native queue cut changed original ownership')
        row = service._read_json(row_ref, max_bytes=service._services.bounds.max_row_bytes)
        display = dict(task_id=descriptor['task_id'], fingerprint=descriptor['fingerprint'], attempt=descriptor['epoch'])
        if type(row) is not dict or row.get('_queue_delivery') != display:
            raise DataflowError('CORRUPT_ARTIFACT', 'Native queue cut row lost original delivery')
        proof = source.channel.completion(source.generation, descriptor['task_id'])
        if proof is not None:
            if proof['fingerprint'] != descriptor['fingerprint'] or proof['consumer_contract'] != identity(source.contract):
                raise DataflowError('FENCED', 'Native completion differs from cut ownership')
            # Original bounded verifier authenticates the raw object and both
            # immutable commits. No task is re-claimed or ACKed here.
            inspect_result(source.channel, source.channel.snapshot(source.pool, kind='results'),
                           descriptor['task_id'], check_stop=source.parent.context.check_stop)
            delivery = SimpleNamespace(**descriptor)
        else:
            delivery = source.channel.restore_delivery(source.worker_id,
                generation=source.generation, task_id=descriptor['task_id'],
                fingerprint=descriptor['fingerprint'], epoch=descriptor['epoch'], lease_s=source.lease_s)
            source._record_reference(delivery)
            with source.lock:
                source.active[delivery.task_id] = delivery
        claim = _Claim(delivery, identity([source.generation, descriptor['task_id'], descriptor['fingerprint']]),
                       SimpleNamespace(allocation_id='', boot_id=''), ref, row_ref)
        claim.descriptor = descriptor
        if entry['receipt_ref'] is not None:
            claim.receipt_row_ref = ArtifactRef(**entry['receipt_ref'])
            claim.receipt_row = service._read_json(claim.receipt_row_ref, max_bytes=service._services.bounds.max_row_bytes)
            claim.acquisition_fact = service._acquisition_fact(claim.receipt_row)
        if proof is not None:
            with source.channel.database.connection() as db:
                claim.final_fact = QueueCommitStore._fact(db, proof['final_commit'])
                claim.acquisition_fact = QueueCommitStore._fact(db, proof['acquisition_commit'])
            if not claim.final_fact or claim.final_fact['status'] != 'acked' or not claim.acquisition_fact or claim.acquisition_fact['status'] != 'acked':
                raise DataflowError('UNKNOWN_EFFECT', 'Original native completion is not acknowledged')
            if claim.receipt_row is None:
                claim.receipt_row = {**row, service._fetch.output: claim.acquisition_fact['prepared']['sinks']['raw']['result']}
                claim.receipt_row_ref = service._put_json(claim.receipt_row, media_type='application/json', max_bytes=service._services.bounds.max_row_bytes)
            claim.finalized = True
        restored[descriptor['task_id']] = claim
    with service._lock:
        service._claims.update(restored)


def current_delivery_row(row, claim):
    """Rebind only the reserved display; business data and receipt stay exact."""
    expected = dict(task_id=claim.descriptor['task_id'], fingerprint=claim.descriptor['fingerprint'], attempt=claim.descriptor['epoch'])
    if type(row) is not dict or row.get('_queue_delivery') != expected:
        raise DataflowError('INVALID_CONTRACT', 'Restored row changed its original native delivery display')
    return {**row, '_queue_delivery': dict(task_id=claim.delivery.task_id,
        fingerprint=claim.delivery.fingerprint, attempt=claim.delivery.epoch)}
