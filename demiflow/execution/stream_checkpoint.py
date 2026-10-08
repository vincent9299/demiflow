"""Atomic commit vector for a single-writer local streaming graph.

Every participating sink publishes before forwarding rows. One bounded write
intent covers the gap between a Lance commit and publication of the vector.
Recovery verifies that intent; it never picks unrelated latest table heads.
Business code supplies identity and pending-row relations, not storage logic.
"""
import hashlib
import json
import os
from pathlib import Path
import threading
import uuid

import lance

from .lance_predicate import scalar_equal


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


class StreamCheckpoint:
    """Caller must own its pipeline run lock throughout recovery and execution."""
    def __init__(self, path, *, identity, initial=None, max_bytes=1024**2,
                 accepted_identities=(), allow_identity_migration=False):
        self.path = Path(path).resolve()
        if type(max_bytes) is not int or not 1024 <= max_bytes <= 16*1024**2:
            raise ValueError('Invalid checkpoint max_bytes')
        self.max_bytes = max_bytes
        self.identity = hashlib.sha256(canonical(identity).encode()).hexdigest()
        self.lock = threading.RLock()
        self.failed = False
        if self.path.exists():
            if self.path.stat().st_size > max_bytes:
                raise ValueError('Checkpoint exceeds max_bytes')
            self.state = json.loads(self.path.read_text())
            accepted = set()
            for value in accepted_identities or ():
                if isinstance(value, str):
                    accepted.add(value)
                else:
                    accepted.add(hashlib.sha256(canonical(value).encode()).hexdigest())
            identity_known = self.state.get('identity') in ({self.identity} | accepted)
            if self.state.get('format') != 1 or (not identity_known and not allow_identity_migration):
                raise ValueError('Checkpoint pipeline identity changed')
            if self.state.get('identity') != self.identity:
                self.state['identity'] = self.identity
                self.publish()
            self.recover()
        else:
            self.state = {'format': 1, 'identity': self.identity, 'revision': 0,
                          'outputs': initial or {}, 'pending': None}
            self.publish()

    def publish(self):
        payload = canonical(self.state).encode()
        if len(payload) > self.max_bytes:
            raise ValueError('Checkpoint exceeds max_bytes')
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp = self.path.with_name(self.path.name + '.' + uuid.uuid4().hex + '.tmp')
        try:
            with temp.open('xb') as out:
                out.write(payload); out.flush(); os.fsync(out.fileno())
            os.replace(temp, self.path)
            descriptor = os.open(self.path.parent, os.O_RDONLY)
            try: os.fsync(descriptor)
            finally: os.close(descriptor)
        finally:
            temp.unlink(missing_ok=True)

    def recover(self):
        pending = self.state.get('pending')
        if pending:
            current = lance.dataset(pending['uri'])
            if current.version == pending['base'] + 1:
                values = []
                for key in pending['keys']:
                    predicate = ' AND '.join(scalar_equal(k, v) for k, v in zip(pending['fields'], key))
                    found = []
                    for batch in current.scanner(filter=predicate, limit=2, batch_size=1,
                            batch_readahead=1, fragment_readahead=1).to_batches():
                        if batch.nbytes > pending['max_row_bytes']:
                            raise ValueError('Checkpoint receipt exceeds row budget')
                        found.extend(batch.to_pylist())
                    if len(found) != 1:
                        raise ValueError('Checkpoint write intent does not match committed rows')
                    values.append(found[0])
                if hashlib.sha256(canonical(values).encode()).hexdigest() != pending['digest']:
                    raise ValueError('Checkpoint write intent content mismatch')
                self.state['outputs'][pending['stage']] = {'uri': pending['uri'], 'version': current.version}
            elif current.version != pending['base']:
                raise ValueError('Checkpoint has an unrecognized table head')
            self.state['pending'] = None
            self.state['revision'] += 1
            self.publish()
        for ref in self.state['outputs'].values():
            if lance.dataset(ref['uri']).version != ref['version']:
                raise ValueError('Table head changed outside the checkpoint writer')

    def register(self, stage, ref):
        with self.lock:
            old = self.state['outputs'].get(stage)
            if old is not None and old != ref:
                raise ValueError('Sink does not match the global checkpoint')
            if old is None and (ref['version'] != 1 or lance.dataset(**ref).count_rows()):
                raise ValueError('Existing stage requires an explicit initial checkpoint reference')
            self.state['outputs'][stage] = ref
            self.state['revision'] += 1
            self.publish()

    def begin(self, stage, uri, base, fields, rows, max_row_bytes):
        if self.failed:
            raise ValueError('Checkpoint writer failed; recovery is required')
        if self.state['pending'] is not None:
            raise ValueError('Unresolved checkpoint write intent')
        if self.state['outputs'].get(stage) != {'uri': uri, 'version': base}:
            raise ValueError('Sink write is not based on the global checkpoint')
        self.state['pending'] = {'stage': stage, 'uri': uri, 'base': base, 'fields': fields,
            'keys': [[r[k] for k in fields] for r in rows], 'max_row_bytes': max_row_bytes,
            'digest': hashlib.sha256(canonical(rows).encode()).hexdigest()}
        self.publish()

    def committed(self, stage, ref):
        self.state['outputs'][stage] = ref
        self.state['pending'] = None
        self.state['revision'] += 1
        self.publish()
