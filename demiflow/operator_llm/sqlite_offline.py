"""Offline model contexts and bound responses in the native SQLite journal.

Binary inputs are content addressed ordinary files; SQLite stores their digest
and location. Reading a request reconstructs the exact original model context.
"""
import base64
import hashlib
import json
from pathlib import Path
from ..execution.artifacts import resolve_local_artifact

from .call_ref import PromptRecordRef
from .journal import canonical, request_key
from .model import OperatorLLMResponse
from .offline import request_record, PromptResponsePending
from .sqlite_journal import SQLitePromptJournal


def externalize(value, directory):
    if isinstance(value, dict):
        return {key: externalize(item, directory) for key, item in value.items()}
    if isinstance(value, list):
        return [externalize(item, directory) for item in value]
    if isinstance(value, str) and value.startswith('data:') and ';base64,' in value:
        header, encoded = value.split(',', 1)
        payload = base64.b64decode(encoded, validate=True)
        sha = hashlib.sha256(payload).hexdigest()
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / sha
        import os
        import uuid
        temporary = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
        try:
            with temporary.open('xb') as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            try:
                os.link(temporary, path)
            except FileExistsError:
                if path.read_bytes() != payload:
                    raise ValueError('Offline input content differs: ' + sha)
        finally:
            temporary.unlink(missing_ok=True)
        return {'_demiflow_offline_input': {'path': str(path), 'sha256': sha, 'header': header}}
    return value


def restore(value):
    if isinstance(value, dict):
        if set(value) == {'_demiflow_offline_input'}:
            ref = value['_demiflow_offline_input']
            payload = resolve_local_artifact(ref['path']).read_bytes()
            if hashlib.sha256(payload).hexdigest() != ref['sha256']:
                raise ValueError('Offline input digest mismatch')
            return ref['header'] + ',' + base64.b64encode(payload).decode()
        return {key: restore(item) for key, item in value.items()}
    if isinstance(value, list):
        return [restore(item) for item in value]
    return value


def materialize(request, options):
    journal = SQLitePromptJournal(**options)
    try:
        record = request_record(request)
        key = record['request_sha256']
        stored = canonical(externalize(record, journal.path.with_suffix('.inputs')))
        with journal._lock:
            db = journal._connect()
            with db:
                db.execute('BEGIN IMMEDIATE')
                old = db.execute('SELECT request_json FROM calls WHERE request_key=?', (key,)).fetchone()
                if old is None:
                    db.execute('INSERT INTO calls(request_key, request_json) VALUES (?, ?)', (key, stored))
                elif old[0] is None:
                    db.execute('UPDATE calls SET request_json=? WHERE request_key=?', (stored, key))
                elif old[0] != stored and restore(json.loads(old[0])) != record:
                    raise ValueError('Offline request changed')
        return record, PromptRecordRef(str(journal.path), key, 'request')
    finally:
        journal.close()


def submit_response(root, request_ref, content, *, model, metadata=None):
    ref = PromptRecordRef.from_dict(request_ref) if isinstance(request_ref, dict) else request_ref
    request = ref.read(root)
    key = request['request_sha256']
    if ref.kind != 'request' or ref.request_id != key or request_key({k: v for k, v in request.items() if k != 'request_sha256'}) != key:
        raise ValueError('Offline request changed')
    if model != request['model']:
        raise ValueError('Offline response model differs from the requested model')
    payload = canonical({'request_sha256': key, 'model': model, 'content': content, 'metadata': dict(metadata or {})})
    journal = SQLitePromptJournal(ref.journal_path)
    try:
        with journal._lock:
            db = journal._connect()
            with db:
                db.execute('BEGIN IMMEDIATE')
                old = db.execute('SELECT response_json FROM calls WHERE request_key=?', (key,)).fetchone()
                if old is None:
                    raise ValueError('Offline request missing')
                if old[0] is not None and old[0] != payload:
                    raise ValueError('Immutable prompt record differs: ' + key)
                db.execute('UPDATE calls SET response_json=? WHERE request_key=?', (payload, key))
        return PromptRecordRef(str(journal.path), key, 'response')
    finally:
        journal.close()


class SQLiteOfflinePromptClient:
    def __init__(self, model, options):
        if set(options) != {'offline_store'}:
            raise ValueError('Only offline_store is accepted')
        self.model, self.options = model, options['offline_store']
        self.journal = SQLitePromptJournal(**self.options)

    def lookup(self, request):
        record, ref = materialize(request, self.options)
        key = record['request_sha256']
        trace = {'mode': 'offline', 'request_ref': ref.to_dict(), 'request_sha256': key,
                 'request_id': key, 'journal_path': str(self.journal.path),
                 'model': request.model, 'provider_called': False, 'reused': True}
        response = self.journal.read(key)
        if response is None:
            error = PromptResponsePending('Waiting for a bound offline response: ' + key)
            error.call = {**trace, 'reused': False}
            raise error
        if response.get('request_sha256') != key or response.get('model') != request.model:
            raise ValueError('Offline response belongs to another request or model')
        trace.update(response_ref=PromptRecordRef(str(self.journal.path), key, 'response').to_dict(),
                     response_sha256=request_key(response), offline_metadata=response.get('metadata', {}))
        return OperatorLLMResponse(response['content'], metadata=trace)

    async def execute(self, request):
        from .client import _journal_io
        return await _journal_io(self.lookup, request)

    async def aclose(self):
        from .client import _journal_io
        await _journal_io(self.journal.close)
