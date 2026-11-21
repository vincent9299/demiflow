"""Retire a stopped driver's original resource scopes before business replay.

The caller holds the original runtime lease and has claimed its native fence.
This only coordinates existing Journal intents and provider exit proofs. It
never admits workers, scans a deployment pool, or changes a business receipt.
"""
import json
import time
import uuid

from ..resources.contracts import RESOURCE_PROTOCOL_VERSION, MutationContext, ResourceError
from ..resources.manager import ResourceManager
from .resource_state import RunResourceState


def _remaining(deadline):
    value = int((deadline - time.monotonic()) * 1000)
    if value <= 0:
        raise ResourceError('CLEANUP_UNCONFIRMED', 'original resource cleanup deadline expired')
    return value


def _original_path(path):
    if path.resolve() != path:
        raise ResourceError('ARTIFACT_MISMATCH', 'original resource path changed or contains a symlink')
    return path


def _record(path):
    # Reuse the original provider evidence's bounded, no-symlink reader.
    from ..resources.evidence import read_evidence_file
    raw = read_evidence_file(path)
    if raw is None:
        return None
    value = json.loads(raw)
    if type(value) is not dict:
        raise ResourceError('INVALID_SPEC', 'original resource evidence must be an object')
    return value


class _OriginalLocalExit:
    """Validate original coordinates, then delegate to the original provider."""
    def __init__(self, journal, state, binding, deadline):
        self.state, self.binding, self.deadline = state, binding, deadline
        self.root = _original_path(journal.directory.with_name(journal.directory.name + '-driver-resources'))
        self._pool = None

    def _original_pool(self):
        if self._pool is not None:
            return self._pool
        from .dataflow_capacity import local_capacity
        from ..resources.capacity import LocalCapacityPool
        domain, capacity = local_capacity()
        root = _original_path(domain / 'capacity')
        recorded = self.binding.get('capacity_root')
        if recorded is not None and recorded != str(root):
            raise ResourceError('ARTIFACT_MISMATCH', 'original capacity domain differs from the selected domain')
        if not root.is_dir():
            raise ResourceError('CLEANUP_UNCONFIRMED', 'original capacity ledger is unavailable')
        self._pool = LocalCapacityPool(root, capacity=capacity, deadline=self.deadline)
        return self._pool

    def ensure_absent(self, exact_identity, terminate_deadline_ms):
        remaining = min(terminate_deadline_ms, _remaining(self.deadline))
        scope, allocation = exact_identity['scope_id'], exact_identity['allocation_id']
        row = self.state.read_cleanup_allocation(scope, allocation)
        if row['effective'].get('accelerators') and 'job_authority' not in self.binding:
            raise ResourceError('UNSUPPORTED_CAPABILITY', 'original device cleanup binding is unavailable')
        if self.binding.get('provider') != 'local':
            raise ResourceError('UNSUPPORTED_CAPABILITY', 'original provider is unavailable for stopped cleanup')
        if set(self.binding) - {'provider', 'worker_mode', 'capacity_root', 'job_authority', 'device_authority'}:
            raise ResourceError('INVALID_SPEC', 'unknown original resource binding')
        modern = 'worker_mode' in self.binding and 'capacity_root' in self.binding
        if ('worker_mode' in self.binding) != ('capacity_root' in self.binding):
            raise ResourceError('INVALID_SPEC', 'incomplete original resource binding')
        pool = self._original_pool()
        reservation = pool.reservation_view(allocation)
        process_dir = _original_path(self.root / 'units' / scope / allocation)
        thread_dir = _original_path(self.root / 'thread-units' / scope / allocation)
        if process_dir.exists() and thread_dir.exists():
            raise ResourceError('ARTIFACT_MISMATCH', 'allocation has conflicting physical provider paths')
        prepared = _record(process_dir / 'identity.json')
        process = _record(process_dir / 'process.json')
        thread = _record(thread_dir / 'thread.json')
        mode = self.binding.get('worker_mode') if modern else ('thread' if thread is not None else 'process')
        if mode not in {'process', 'thread'}:
            raise ResourceError('INVALID_SPEC', 'unknown original worker mode')
        if (mode == 'thread' and process_dir.exists()) or (mode == 'process' and thread_dir.exists()):
            raise ResourceError('ARTIFACT_MISMATCH', 'worker mode differs from original unit evidence')
        evidence = thread if mode == 'thread' else prepared
        physical = thread if mode == 'thread' else process
        if not modern and (reservation is None or evidence is None):
            raise ResourceError('CLEANUP_UNCONFIRMED', 'legacy resource lacks its original allocation evidence')
        if evidence is not None and (evidence.get('scope_id') != scope
                or evidence.get('allocation_id') != allocation or evidence.get('spec_hash') != row['spec_hash']):
            raise ResourceError('ARTIFACT_MISMATCH', 'original allocation evidence differs from Journal')
        if physical is not None and physical.get('spec_hash') != row['spec_hash']:
            raise ResourceError('ARTIFACT_MISMATCH', 'physical allocation spec differs from Journal')
        if physical is not None and evidence is None:
            raise ResourceError('CLEANUP_UNCONFIRMED', 'physical allocation lost its original identity record')
        if reservation is not None:
            expected = {key: row['effective'][key] for key in ('cpu_millis', 'memory_bytes', 'scratch_bytes')
                        if key in row['effective']}
            if reservation['spec_hash'] != row['spec_hash'] or reservation['effective'] != expected:
                raise ResourceError('ARTIFACT_MISMATCH', 'original capacity reservation differs from Journal')
            if reservation['identity'] is not None and (physical is None
                    or physical.get('identity') != reservation['identity']):
                raise ResourceError('ARTIFACT_MISMATCH', 'capacity reservation physical identity differs')
        elif physical is not None or evidence is not None or row['phase'] not in {'REQUESTED', 'PENDING'}:
            raise ResourceError('CLEANUP_UNCONFIRMED', 'original capacity reservation is missing')
        if physical is None and (not modern or row['phase'] not in {'REQUESTED', 'PENDING'}
                or process_dir.exists() or thread_dir.exists()
                or (reservation is not None and (reservation['state'] not in {'RESERVED', 'RELEASED'}
                    or reservation['identity'] is not None))):
            # A prepared unit may already have forked before process.json was
            # persisted. No original inode was recorded yet: even a free lock
            # at this path could be a replacement while the old child is live.
            # Only matching, never-spawned request facts allow absent cleanup.
            raise ResourceError('CLEANUP_UNCONFIRMED', 'resource lacks original physical exit or never-spawned proof')
        if mode == 'thread':
            if thread is not None and thread.get('effective') != row['effective']:
                raise ResourceError('ARTIFACT_MISMATCH', 'thread effective resource evidence differs')
            from ..resources.thread import ThreadResourceProvider
            from .dataflow_thread_host import DataflowThreadConnector
            provider = ThreadResourceProvider(pool, self.root / 'thread-units', control=DataflowThreadConnector())
            return provider.ensure_absent(exact_identity, min(remaining, _remaining(self.deadline)))
        from ..resources.local import LocalResourceProvider
        from ..resources.process_unit import ManagedProcessUnit
        devices = None
        if row['effective'].get('accelerators'):
            from .dataflow_devices import device_configuration
            devices, _ = device_configuration('local')
            from .recovery_contract import identity
            if devices is not None and self.binding.get('device_authority') != identity(devices.authority()):
                raise ResourceError('ARTIFACT_MISMATCH', 'original training device authority changed')
            if devices is None or devices.reservation_view(allocation) is None:
                raise ResourceError('CLEANUP_UNCONFIRMED', 'original job device reservation cannot be verified')
        provider = LocalResourceProvider(pool, ManagedProcessUnit(self.root / 'units'), device_pool=devices)
        return provider.ensure_absent(exact_identity, min(remaining, _remaining(self.deadline)))


def retained_process_units(journal, *, expected_attempt):
    """Identify separately journaled, live allocations after their driver exits.

    The supervisor still owns all other descendants, including ordinary row
    workers. A recorded group whose physical identity is ambiguous leaves
    attempt cleanup unconfirmed until that identity can be established.
    """
    configuration = journal.recovery_identity()
    if configuration is None:
        return ()
    if journal.execution_status()['attempt'] != expected_attempt:
        raise ResourceError('FENCED', 'driver attempt changed before process handoff')
    state = RunResourceState(journal)
    scopes = state.unfinished_scopes(configuration['run_id'])
    retained = []
    while scopes:
        for scope in scopes:
            document = scope['document']
            job = document['provider_binding'].get('job_authority')
            if job is None:
                continue
            unit = job.get('inspection_key', job['request_key'])
            name = scope['scope_id']
            if (name != 'fit-' + unit or document['authority_ref'] != name
                    or journal.source_binding('fit-resource-' + unit) != job):
                raise ResourceError('ARTIFACT_MISMATCH', 'original job scope membership differs')
            allocation = state.read_allocation_identity(name, unit)
            if allocation is None:
                continue
            root = journal.directory.with_name(journal.directory.name + '-driver-resources')
            directory = _original_path(root / 'units' / name / unit)
            recorded = _record(directory / 'process.json')
            if recorded is None:
                if directory.exists():
                    raise ResourceError('CLEANUP_UNCONFIRMED', 'original job process birth is unconfirmed')
                continue
            if recorded['spec_hash'] != allocation['spec_hash']:
                raise ResourceError('ARTIFACT_MISMATCH', 'original job process spec differs')
            physical = recorded['identity']
            original = allocation['observation'].get('identity')
            if original is not None and physical != original:
                raise ResourceError('ARTIFACT_MISMATCH', 'original job birth identity differs')
            from ..resources.process_observation import unit_lock_status
            lock = unit_lock_status(directory / 'unit.lock', physical['unit'])
            if lock == 'unknown':
                raise ResourceError('CLEANUP_UNCONFIRMED', 'original job unit lock identity changed')
            if lock == 'held':
                retained.append(physical)
            if len(retained) > 128:
                raise ResourceError('QUOTA_EXCEEDED', 'too many retained process units')
        scopes = state.unfinished_scopes(configuration['run_id'], after=scopes[-1]['scope_id'])
    return tuple(retained)


def recover_execution_resources(journal, configuration, fence, *, preserve_jobs=False):
    """Called only by an owner claim made while holding the original run lease."""
    reader = RunResourceState(journal)
    scopes = reader.unfinished_scopes(configuration['run_id'])
    if not scopes:
        return
    deadline = time.monotonic() + configuration['limits']['cancel_grace_s']
    if journal._maintenance_deadline is not None:
        deadline = min(deadline, journal._maintenance_deadline)
    attempt = journal.execution_status()['attempt']
    while scopes:
        for scope in scopes:
            remaining = _remaining(deadline)
            original = scope['document']
            if original.get('run_id') != configuration['run_id'] or original.get('scope_id') != scope['scope_id']:
                raise ResourceError('ARTIFACT_MISMATCH', 'resource scope identity differs from original run')
            job = original['provider_binding'].get('job_authority')
            if job is not None:
                key = job['request_key']
                unit = job.get('inspection_key', key)
                if unit != key:
                    from .recovery_contract import identity
                    if unit != identity(['fit.snapshot_inspection.v1', key]):
                        raise ResourceError('ARTIFACT_MISMATCH', 'inspection does not name its original fit')
                if (scope['scope_id'] != 'fit-' + unit or original['authority_ref'] != scope['scope_id']
                        or journal.source_binding('fit-resource-' + unit) != job):
                    raise ResourceError('ARTIFACT_MISMATCH', 'job scope has no matching original launch')
                fact = journal.fit_submission(fence, job['action'], job['occurrence'], instance=job['instance'],
                            request_key=key, digest=job['digest'], scope=job['provider_scope'])
                if fact['state'] not in {'sent', 'unknown', 'confirmed'}:
                    raise ResourceError('ARTIFACT_MISMATCH', 'job has no recorded submission')
                if preserve_jobs:
                    # Rows retire with their old attempt. A job has its own
                    # original scope and will be observed by its fenced owner.
                    continue
            state = RunResourceState(journal, cleanup_scope=scope['scope_id'])
            provider = _OriginalLocalExit(journal, state, original['provider_binding'], deadline)
            manager = ResourceManager(provider, state)
            if scope['state'] == 'OPEN':
                context = MutationContext(RESOURCE_PROTOCOL_VERSION, scope['scope_id'], attempt, fence,
                                          'recover-' + uuid.uuid4().hex, scope['desired_revision'])
                manager.close(context, dict(mode='ABANDON', engine_receipt_ref='stopped-resource-recovery',
                    terminate_deadline_ms=remaining, reason='original_driver_stopped'))
            while not state.cleanup_complete(scope['scope_id']):
                _remaining(deadline)
                manager.reconcile_once(fence, deadline, 1)
                if not state.cleanup_complete(scope['scope_id']):
                    time.sleep(min(.01, _remaining(deadline) / 1000))
        scopes = reader.unfinished_scopes(configuration['run_id'], after=scopes[-1]['scope_id'])
