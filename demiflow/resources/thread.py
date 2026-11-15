"""Admitted same-process thread units using the original local capacity pool.

The execution-side connector owns the actual worker shell. This provider owns
only physical lifetime and evidence; it never executes Dataset code itself.
Unknown or failed cleanup retains CPU/memory until positively confirmed.
"""
import os
from pathlib import Path

from .contracts import ResourceError, identifier
from .local import LocalResourceProvider, _now_ms
from .process_unit import process_identity, birth_start, _save


class ThreadResourceProvider(LocalResourceProvider):
    def __init__(self, pool, root, *, control):
        super().__init__(pool, None, control=control)
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, scope, allocation):
        identifier(scope); identifier(allocation)
        return self.root / scope / allocation / 'thread.json'

    def _evidence(self, scope, allocation):
        from .evidence import evidence_reference
        return evidence_reference(self.root, scope, allocation, 'thread.json')

    def resolve_evidence(self, scope, allocation, reference):
        from .evidence import resolve_evidence
        return resolve_evidence(self.root, scope, allocation, reference)

    def _read(self, scope, allocation):
        import json
        path = self._path(scope, allocation)
        if not path.exists():
            return None
        with path.open('rb') as stream:
            raw = stream.read(8193)
        if len(raw) > 8192:
            raise ResourceError('INVALID_SPEC', 'thread unit evidence exceeds bound')
        value = json.loads(raw)
        if value['scope_id'] != scope or value['allocation_id'] != allocation:
            raise ResourceError('INVALID_SPEC', 'thread unit evidence identity differs')
        return value

    def _view(self, record):
        allocation, scope = record['allocation_id'], record['scope_id']
        identity = record['identity']
        unit = self._control.units.get(allocation)
        if record.get('cleanup') == 'CONFIRMED':
            phase, freshness = 'TERMINATED', 'FRESH'
        elif unit is not None and unit.thread.is_alive():
            phase, freshness = 'RUNNING', 'FRESH'
        elif birth_start(identity['pid']) != identity['start']:
            phase, freshness = 'TERMINATED', 'FRESH'
        else:
            phase, freshness = 'RUNNING', 'STALE_UNKNOWN'
        result = self._observation(scope, allocation, phase, freshness, identity=identity,
            effective=record['effective'], evidence_ref=self._evidence(scope, allocation),
            cleanup=record.get('cleanup', 'NONE'))
        result['boot_id'] = record['boot_id']
        return result

    def ensure_present(self, allocation_id, immutable_spec):
        scope = immutable_spec['scope_id']
        normalized = self.describe_and_normalize(immutable_spec.get('binding', {}),
            immutable_spec.get('requirements'), immutable_spec['request'])
        if normalized['spec_hash'] != immutable_spec['spec_hash']:
            raise ResourceError('ARTIFACT_MISMATCH', 'thread unit immutable spec differs')
        old = self._read(scope, allocation_id)
        if old is not None:
            if old['spec_hash'] != immutable_spec['spec_hash']:
                raise ResourceError('ARTIFACT_MISMATCH', 'thread allocation reused with different spec')
            return self._view(old)
        effective = normalized['effective']
        try:
            self.pool.reserve(allocation_id, immutable_spec['spec_hash'], effective,
                              immutable_spec['request']['provision_deadline_ms'])
        except ResourceError as exc:
            if exc.code == 'CAPACITY_PENDING':
                return self._observation(scope, allocation_id, 'PENDING', 'FRESH', effective=effective,
                                         conditions=['capacity_pending'])
            raise
        launch = self._control.launch_spec(allocation_id)
        identity = {**process_identity(os.getpid()), 'unit': 'thread-' + allocation_id}
        path = self._path(scope, allocation_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        record = {'scope_id': scope, 'allocation_id': allocation_id,
                  'spec_hash': immutable_spec['spec_hash'], 'boot_id': launch['boot_id'],
                  'identity': identity, 'effective': effective, 'cleanup': 'NONE'}
        _save(path, record)
        self.pool.bind_identity(allocation_id, identity)
        envelope = {'allocation_id': allocation_id, 'spec_hash': immutable_spec['spec_hash'],
                    'cpu_millis': effective['cpu_millis'], 'memory_bytes': effective['memory_bytes'],
                    'resource_directory': str(path.parent / 'native')}
        self._control.start(allocation_id, spec_hash=immutable_spec['spec_hash'], envelope=envelope)
        return self._view(record)

    def inspect(self, scope_id, page_token):
        identifier(scope_id)
        root = self.root / scope_id
        names = sorted(p.name for p in root.iterdir() if p.is_dir()) if root.exists() else []
        if len(names) > 4096:
            raise ResourceError('INVALID_SPEC', 'thread unit inventory exceeds bound')
        names = [name for name in names if page_token is None or name > page_token]
        page = names[:64]
        observations = []
        for name in page:
            record = self._read(scope_id, name)
            observations.append(self._view(record) if record is not None else
                self._observation(scope_id, name, 'REQUESTED', 'FRESH',
                                  evidence_ref=self._evidence(scope_id, name)))
        return {'allocations': observations,
                'next_page_token': page[-1] if len(names) > 64 else None,
                'sampled_at_ms': _now_ms()}

    def ensure_absent(self, exact_identity, terminate_deadline_ms):
        scope, allocation = exact_identity['scope_id'], exact_identity['allocation_id']
        record = self._read(scope, allocation)
        unit = self._control.units.get(allocation)
        confirmed = False
        if record is None:
            confirmed = unit is None
            identity = None
        else:
            identity = record['identity']
            if record.get('cleanup') == 'CONFIRMED':
                confirmed = True
            elif unit is not None:
                confirmed = unit.shutdown(timeout_ms=terminate_deadline_ms)
            elif birth_start(identity['pid']) != identity['start']:
                confirmed = True  # The actual hosting process no longer exists.
        if confirmed:
            if record is not None and record.get('cleanup') != 'CONFIRMED':
                _save(self._path(scope, allocation), {**record, 'cleanup': 'CONFIRMED'})
            # Freeze the real terminal proof before referencing it. Later
            # cleanup calls reuse it without timestamps or phase rewrites.
            evidence = self._evidence(scope, allocation)
            reservation = self.pool.reservation_view(allocation)
            if reservation is not None and reservation['state'] != 'RELEASED':
                self.pool.release(allocation, {'confirmed': True, 'identity': identity, 'evidence_ref': evidence})
            self._control.retire(allocation)
        return {'allocation_id': allocation, 'cleanup': 'CONFIRMED' if confirmed else 'UNCONFIRMED',
                'evidence_ref': evidence if confirmed else self._evidence(scope, allocation),
                'observed_at_ms': _now_ms()}
