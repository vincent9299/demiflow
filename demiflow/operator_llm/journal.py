"""Optional durable request ledger for local prompt actors; never retries uncertainty."""
import fcntl
import hashlib
import json
import os
import time
import uuid
from pathlib import Path
from .errors import PromptBudgetExceededError, PromptError

class UncertainPromptCall(PromptError):pass


def canonical(value):return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'))


class RequestRecord(dict):
    """Exact request payload with a once-computed key; scoped to one call."""
    @classmethod
    def _from_canonical_body(cls, *, protocol, model_contract, body, encoded_body):
        """Internal fast path: caller already serialized body with canonical().

        Hash the same sorted JSON envelope without re-serializing/copying a large
        inline-image body. encoded_body MUST be the canonical UTF-8 bytes of body.
        Kept internal because verifying that invariant would repeat the work.
        """
        result = cls(protocol=protocol, model_contract=model_contract, body=body)
        digest = hashlib.sha256()
        for part in (b'{"body":', encoded_body, b',"model_contract":',
                     canonical(model_contract).encode(), b',"protocol":',
                     canonical(protocol).encode(), b'}'):
            digest.update(part)
        result._request_key = digest.hexdigest()
        return result

    @property
    def request_key(self):
        if not hasattr(self, '_request_key'):
            self._request_key = hashlib.sha256(canonical(self).encode()).hexdigest()
        return self._request_key


def request_key(value):
    return (value.request_key if isinstance(value, RequestRecord)
            else hashlib.sha256(canonical(value).encode()).hexdigest())


def immutable(path,value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    data=canonical(value)
    if path.exists():
        if path.read_text()!=data:raise ValueError(f'Immutable prompt record differs: {path}')
        return
    tmp=path.with_name(path.name+'.'+uuid.uuid4().hex+'.tmp')
    with tmp.open('w') as f:f.write(data);f.flush();os.fsync(f.fileno())
    try:os.link(tmp,path)
    finally:tmp.unlink(missing_ok=True)


class PromptJournal:
    def __init__(self,path,max_requests=None):
        self.path=Path(path);self.path.mkdir(parents=True,exist_ok=True);self.limit=max_requests

    def paths(self,request):
        key=request_key(request)
        return {name:self.path/f'{key}.{name}.json' for name in ['request','response','transport_error']}

    def lookup(self,request):
        paths=self.paths(request)
        if paths['response'].exists():
            if not paths['request'].exists():raise ValueError('Prompt response has no request record')
            if json.loads(paths['request'].read_text())!=request:raise ValueError('Prompt request record mismatch')
            return json.loads(paths['response'].read_text())
        if paths['request'].exists():raise UncertainPromptCall(f'Request reserved without complete response: {paths["request"]}')
        return None

    def reserve(self,request):
        with (self.path/'requests.lock').open('a') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX)
            # Recheck under a short cross-process lock; never await while holding it.
            if self.lookup(request) is not None:return False
            if self.limit is not None and len(list(self.path.glob('*.request.json')))>=self.limit:
                raise PromptBudgetExceededError('Persistent prompt request budget exhausted')
            immutable(self.paths(request)['request'],request)
            return True

    def response(self,request,record):immutable(self.paths(request)['response'],record)
    def failed(self,request,error,elapsed):
        immutable(self.paths(request)['transport_error'],{'type':type(error).__name__,'detail':str(error),'elapsed_s':elapsed,
            **({'call':error.call} if getattr(error,'call',None) else {})})
