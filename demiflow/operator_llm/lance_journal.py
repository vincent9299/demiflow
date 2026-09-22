"""Lance-backed native prompt transports and durable call reservations."""
import hashlib
import fcntl
from pathlib import Path
from .journal import canonical, UncertainPromptCall
from .errors import PromptBudgetExceededError
from .offline import request_record, PromptResponsePending
from .model import OperatorLLMResponse
from ..lance.records import LanceRecordStore, RecordRef


def key_for(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


class LancePromptJournal:
    def __init__(self, options, max_requests=None):
        self.store = LanceRecordStore(**options)
        self.limit = max_requests

    def references(self, request):
        key = key_for(request)
        return {name + '_ref': self.store.reference(name + '/' + key).to_dict()
                for name in ('request', 'response') if self.store.get(name + '/' + key) is not None}

    def lookup(self, request):
        key = key_for(request)
        source = self.store.get('request/' + key)
        result = self.store.get('response/' + key)
        if result is not None:
            if source != request: raise ValueError('Prompt request record mismatch')
            return result
        if source is not None:
            raise UncertainPromptCall('Request reserved without complete response: ' + key)
        return None

    def reserve(self, request):
        self.store.path.parent.mkdir(parents=True, exist_ok=True)
        with self.store.path.with_suffix('.reservation.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            if self.lookup(request) is not None: return False
            if self.limit is not None and len(self.store.keys(prefix='request/')) >= self.limit:
                raise PromptBudgetExceededError('Persistent prompt request budget exhausted')
            self.store.put('request/' + key_for(request), request)
            return True

    def response(self, request, record):
        self.store.put('response/' + key_for(request), record)

    def failed(self, request, error, elapsed):
        self.store.put('transport_error/' + key_for(request),
                       {'type': type(error).__name__, 'detail': str(error), 'elapsed_s': elapsed})


def materialize(request, options):
    store = LanceRecordStore(**options)
    record = request_record(request)
    ref = store.put('request/' + record['request_sha256'], record)
    return record, ref


def submit_response(root, request_ref, content, *, model, metadata=None):
    ref = RecordRef.from_dict(request_ref) if isinstance(request_ref, dict) else request_ref
    request = ref.read(root)
    key = request['request_sha256']
    if key_for({k: v for k, v in request.items() if k != 'request_sha256'}) != key:
        raise ValueError('Offline request changed')
    if model != request['model']:
        raise ValueError('Offline response model differs from the requested model')
    return LanceRecordStore(root, ref.relative_uri).put('response/' + key,
        {'request_sha256': key, 'model': model, 'content': content, 'metadata': dict(metadata or {})})


class LanceOfflinePromptClient:
    def __init__(self, model, options):
        if set(options) != {'offline_store'}: raise ValueError('Only offline_store is accepted')
        self.model, self.options = model, options['offline_store']
        self.store = LanceRecordStore(**self.options)

    def lookup(self, request):
        record, ref = materialize(request, self.options)
        key = record['request_sha256']
        trace = {'mode': 'offline', 'request_ref': ref.to_dict(), 'request_sha256': key,
                 'model': request.model, 'provider_called': False, 'reused': True}
        response = self.store.get('response/' + key)
        if response is None:
            error = PromptResponsePending('Waiting for a bound offline response: ' + key)
            error.call = {**trace, 'reused': False}
            raise error
        if response.get('request_sha256') != key or response.get('model') != request.model:
            raise ValueError('Offline response belongs to another request or model')
        trace.update(response_ref=self.store.reference('response/' + key).to_dict(),
                     response_sha256=key_for(response), offline_metadata=response.get('metadata', {}))
        return OperatorLLMResponse(response['content'], metadata=trace)

    async def execute(self, request): return self.lookup(request)
    async def aclose(self): pass
