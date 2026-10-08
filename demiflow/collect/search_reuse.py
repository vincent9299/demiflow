"""Reuse exact evidence and preserve unknown attempts across a proxy change.

Sources, request semantics and bundled runtime must match. Old failures are
retained under their original identities; no receipt is copied or overwritten.
"""
import asyncio
import copy
import json
import time

from .native_search import NativeSearchSession, SearchConfig
from .native_search.config import digest
from .search_errors import RouteLeaseExpired


def completed_reuse_configs(current, previous):
    if not isinstance(previous, (list, tuple)):
        raise TypeError('search_reuse_configs must be a list')
    result = []
    for value in previous:
        prior = SearchConfig.from_mapping(value) if isinstance(value, dict) else value
        if not isinstance(prior, SearchConfig):
            raise TypeError('Previous source configuration must be SearchConfig')
        now, before = current.snapshot(), prior.snapshot()
        now.pop('proxy'); before.pop('proxy')
        if now != before:
            raise ValueError('Completed search reuse permits only an outgoing proxy change')
        result.append(copy.deepcopy(prior))
    return result


class ReceiptReuseSearchSession(NativeSearchSession):
    def __init__(self, *, cache_path, config, reuse_configs):
        super().__init__(cache_path=cache_path, config=config)
        self.reuse_configs = completed_reuse_configs(config, reuse_configs)
        self.reuse_profiles = None
        self.reuse_lock = asyncio.Lock()
        self.preferred_reuse_profiles = {}

    async def initialize(self):
        await super().initialize()
        async with self.reuse_lock:
            if self.reuse_profiles is None:
                profiles = []
                for config in self.reuse_configs:
                    prior = NativeSearchSession(cache_path=self.path, config=config)
                    # Only derive the old salted identity; no workers, sockets,
                    # relay or HTTP client are started by _initialize.
                    await asyncio.to_thread(prior._initialize)
                    profiles.append(prior.profile)
                self.reuse_profiles = profiles

    def record_reuse(self, key, previous_key):
        with self._db() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS native_search_reuse (
                request_key TEXT PRIMARY KEY, source_key TEXT NOT NULL, observed_at REAL NOT NULL)''')
            db.execute('INSERT OR IGNORE INTO native_search_reuse VALUES (?,?,?)',
                       (key, previous_key, time.time()))

    def first_completed(self, keys, *, preserve_interrupted=False):
        # Renewable pools can accumulate many historical profiles. Batch the
        # indexed lookups with one connection and bounded bind parameters,
        # preserving the declared precedence and exact original receipt.
        uncertain = None
        with self._db() as db:
            for start in range(0,len(keys),400):
                chunk=keys[start:start+400]
                rows={k:json.loads(v) for k,v in db.execute(
                    'SELECT key,value FROM native_search WHERE key IN ('+','.join('?' for _ in chunk)+')',chunk)}
                for key in chunk:
                    saved=rows.get(key)
                    if saved is not None and saved.get('status') in {'ok','no_results'}:
                        return key,saved
                    if (preserve_interrupted and uncertain is None and saved is not None
                            and saved.get('status') == 'interrupted'):
                        # Keep at most one unknown receipt in addition to the
                        # existing bounded lookup chunk. A later exact success
                        # remains usable, but absence of one cannot buy a retry.
                        uncertain = key,saved
        return uncertain

    async def source(self, source, query, parameters):
        key = digest([self.profile, source['name'], query, parameters])
        # A route migration must preserve the last successfully selected
        # evidence, even if another declared route has a different good copy.
        # This preference is installed by the pool from its own durable events.
        preferred=self.preferred_reuse_profiles.get(digest([query,parameters]))
        if preferred in {self.profile,*(self.reuse_profiles or [])}:
            previous_key=digest([preferred,source['name'],query,parameters])
            saved=await self.database(previous_key)
            if saved is None:
                def original_key():
                    with self._db() as db:
                        exists=db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='native_search_reuse'").fetchone()
                        row=db.execute('SELECT source_key FROM native_search_reuse WHERE request_key=?',(previous_key,)).fetchone() if exists else None
                        return row[0] if row else None
                original=await asyncio.to_thread(original_key)
                if original:saved=await self.database(original)
            if saved is not None and saved.get('status') in {'ok','no_results'}:
                self.metrics['reused']+=1
                self.metrics['reused_selected_receipt']=self.metrics.get('reused_selected_receipt',0)+1
                return {'engine':source['name'],**copy.deepcopy(saved)}
        # Existing current-profile reservations/errors retain normal semantics.
        if await self.database(key) is None:
            found=await asyncio.to_thread(self.first_completed,
                [digest([profile,source['name'],query,parameters]) for profile in self.reuse_profiles or []],
                preserve_interrupted=True)
            if found is not None:
                previous_key,saved=found
                await asyncio.to_thread(self.record_reuse, key, previous_key)
                self.metrics['reused'] += 1
                self.metrics['reused_previous_proxy'] = self.metrics.get('reused_previous_proxy',0)+1
                # The original receipt_id and HTTP attempts remain intact;
                # do not insert a second receipt or count a new HTTP attempt.
                return {'engine':source['name'], **copy.deepcopy(saved)}
        before_http = self.metrics.get('http_requests', 0)
        started = time.monotonic()
        try:
            return await super().source(source,query,parameters)
        except RouteLeaseExpired as exc:
            observed = getattr(exc, 'native_http_receipts', [])
            saved = await self.database(key)
            attempts = (saved or {}).get('attempts') or []
            if observed or attempts:
                # Local expiry after external work belongs to this source.
                # Settle its original reservation, preserving unknown HTTP;
                # renewing the lease must not purchase another attempt.
                interrupted = {'attempt':len(attempts)+1,'status':'interrupted',
                    'http_status':(observed or [{}])[-1].get('http_status'),
                    'reason':'Local route lease expired after HTTP admission',
                    'elapsed_s':max(0.,time.monotonic()-started-sum(a.get('elapsed_s',0) for a in attempts)),
                    'http':observed}
                result = self.scrub({'status':'interrupted','reason':interrupted['reason'],
                    'local_lease_expired':True,'results':[],'attempts':[*attempts,interrupted],
                    'receipt_id':key},record=True,protocol=True)
                await self.database(key,result)
                return {'engine':source['name'],**result}
            # The native once() reservation precedes local admission. Settle
            # only a fresh first attempt with positive evidence of zero HTTP;
            # an old unknown receipt returns from once() without raising here.
            # Concurrent HTTP makes this check conservative, never permissive.
            if (self.metrics.get('http_requests', 0) == before_http
                    and not observed):
                if saved and saved.get('status') == 'interrupted' and not saved.get('attempts'):
                    await self.database(key, {'status':'suspended',
                        'reason':'Local route lease expired before any HTTP admission',
                        'results':[], 'attempts':[], 'resume_at':time.time(), 'receipt_id':key})
            raise
