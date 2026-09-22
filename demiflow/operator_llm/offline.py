"""File transport for native prompt actors, using the HTTP model context verbatim.

Materializing a request is not a provider call. Missing responses remain pending;
submitted responses still pass through the normal actor's parser and schema.
"""
import hashlib
import json
from pathlib import Path

from .client import request_messages
from .errors import PromptError
from .journal import canonical, immutable
from .model import OperatorLLMResponse


class PromptResponsePending(PromptError):
    pass


def request_record(request):
    value = {"stage": request.prompt_name, "prompt_version": request.prompt_version,
             "model": request.model, "messages": request_messages(request),
             "response_schema": dict(request.response_schema),
             "response_format": request.response_format, "schema_attempt": request.schema_attempt}
    return {**value, "request_sha256": hashlib.sha256(canonical(value).encode()).hexdigest()}


def materialize(request, directory):
    record = request_record(request)
    root = Path(directory)
    key = record['request_sha256']
    path = root / 'requests' / f'{key}.json'
    immutable(path, record)
    return record, path, root / 'responses' / f'{key}.json'


def submit_response(request_path, response_path, content, *, model, metadata=None):
    request = json.loads(Path(request_path).read_text())
    key = request.pop('request_sha256')
    if hashlib.sha256(canonical(request).encode()).hexdigest() != key:
        raise ValueError('Offline request changed')
    if model != request['model']:
        raise ValueError('Offline response model differs from the requested model')
    immutable(response_path, {'request_sha256': key, 'model': model,
                              'content': content, 'metadata': dict(metadata or {})})
    return Path(response_path)


class OfflinePromptClient:
    def __init__(self, model, options):
        if set(options) != {'offline_dir'}:
            raise ValueError('Offline prompt options only accept offline_dir')
        self.model, self.directory = model, Path(options['offline_dir'])

    def lookup(self, request):
        record, path, response_path = materialize(request, self.directory)
        trace = {'mode': 'offline', 'request_path': str(path), 'response_path': str(response_path),
                 'request_sha256': record['request_sha256'], 'model': request.model,
                 'provider_called': False, 'reused': True}
        if not response_path.exists():
            error = PromptResponsePending('Waiting for a bound offline response: ' + str(path))
            error.call = {**trace, 'reused': False}
            raise error
        result = json.loads(response_path.read_text())
        if result.get('request_sha256') != record['request_sha256'] or result.get('model') != request.model:
            raise ValueError('Offline response belongs to another request or model')
        trace['offline_metadata'] = result.get('metadata', {})
        trace['response_sha256'] = hashlib.sha256(response_path.read_bytes()).hexdigest()
        return OperatorLLMResponse(result['content'], metadata=trace)

    async def execute(self, request):
        return self.lookup(request)

    async def aclose(self):
        pass
