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
        key=hashlib.sha256(canonical(request).encode()).hexdigest()
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
        immutable(self.paths(request)['transport_error'],{'type':type(error).__name__,'detail':str(error),'elapsed_s':elapsed})
