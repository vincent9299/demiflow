"""Execution of native Dataset search/fetch nodes. Request bindings are opaque IDs."""
import asyncio
from .session import bounded
from .connection_manager import WebOperatorLifecycle


def unique(values):
    return list(dict.fromkeys(values))


class SearchWeb(WebOperatorLifecycle):
    concurrency = 1
    def __init__(self, requests, output, session, when, max_candidates, request_concurrency,
                 checkpoint=None, checkpoint_operator='search_web', checkpoint_lease_s=300.0):
        self.requests, self.output, self.session, self.when = requests, output, session, when
        self.max_candidates, self.request_concurrency = max_candidates, request_concurrency
        self.resources = (session,)
        self._checkpoint = None
        if checkpoint is not None:
            from ..execution.operator_checkpoint import OperatorCheckpoint
            self._checkpoint = OperatorCheckpoint(checkpoint, operator=checkpoint_operator,
                                                  lease_s=checkpoint_lease_s,
                                                  payload_policy='key_authoritative')

    async def _checkpointed(self, request, call, row=None):
        """Run one opaque request under the platform task ledger.

        The request id is supplied by the caller; the ledger does not inspect
        query fields or impose search policy. Successful results are replayed
        verbatim. A failed task remains retryable and is raised after sibling
        tasks finish, so a cancelled stream can resume at request granularity.
        """
        if self._checkpoint is None:
            return await call()
        key = str(request.get('request_id') or '')
        if not key:
            raise ValueError('checkpointed search requests require request_id')
        await asyncio.to_thread(self._checkpoint.register, key, {'row': row, 'request': request})
        while True:
            claimed = await asyncio.to_thread(self._checkpoint.claim, key)
            if claimed is None:
                await asyncio.sleep(0.05)
                continue
            if claimed['state'] == 'completed':
                return claimed['result']
            try:
                value = await call()
            except BaseException as exc:
                await asyncio.to_thread(self._checkpoint.fail, key,
                                        {'type': type(exc).__name__, 'message': str(exc)},
                                        retryable=True)
                raise
            if value.get('retryable') is True:
                await asyncio.to_thread(self._checkpoint.fail, key,
                    {'type': 'SearchDeferred', 'message': value.get('reason', value['status'])}, retryable=True)
            else:
                await asyncio.to_thread(self._checkpoint.complete, key, value)
            return value

    def recovery_rows(self):
        if self._checkpoint is None:
            return []
        recovered = []
        for item in self._checkpoint.pending():
            payload = item.get('payload') or {}
            request = payload.get('request') if isinstance(payload, dict) else None
            row = payload.get('row') if isinstance(payload, dict) else None
            if request is None:
                request = payload
            if not isinstance(row, dict) or not isinstance(request, dict):
                recovered.append({self.requests: [request]})
                continue
            recovered.append({**row, self.requests: [request]})
        return recovered

    async def __call__(self, row):
        if self.when is not None and not self.when(row):
            return row
        async def one(request):
            parameters = {key: request[key] for key in ('language','pageno','safesearch','time_range',
                'engines','categories','engine_data','network','fallback_parameters') if key in request}
            async def call():
                result = await self.session.search(request['query'], **parameters)
                return {**request, **result, 'candidates':result.get('candidates', [])[:self.max_candidates]}
            return await self._checkpointed(request, call, row=row)
        # Gather with return_exceptions so an individual failed request cannot
        # cancel sibling requests that may already be paid/in flight. The row
        # is committed only when every child is complete; retryable failures
        # leave their task ledger entries available for the next run.
        values = await bounded(row[self.requests], one, self.request_concurrency,
                              return_exceptions=True)
        errors = [value for value in values if isinstance(value, BaseException)]
        if errors:
            raise errors[0]
        return {**row, self.output:values}


class FetchDocuments(WebOperatorLifecycle):
    concurrency = 1
    def __init__(self, requests, output, session, when, known, per_request,
                 max_attempts, max_new_documents, request_concurrency, url_concurrency):
        self.requests, self.output, self.session, self.when = requests, output, session, when
        self.known, self.per_request = known, per_request
        self.max_attempts, self.max_new_documents = max_attempts, max_new_documents
        self.request_concurrency, self.url_concurrency = request_concurrency, url_concurrency
        self.resources = (session,)

    async def __call__(self, row):
        if self.when is not None and not self.when(row):
            return row
        requests = row[self.requests]
        docs = {d['url']:dict(d) for d in (row.get(self.known, []) if self.known else [])}
        initial = set(docs)
        tasks, attempted, added, touched = {}, set(), set(), set()
        page_gate = asyncio.Semaphore(self.url_concurrency)

        async def get(url, request):
            if url not in docs or docs[url]['status'] != 'ok':
                if url not in tasks:
                    async def fetch():
                        async with page_gate:
                            return await self.session.fetch(url)
                    attempted.add(url)
                    tasks[url] = asyncio.create_task(fetch())
                result = await asyncio.shield(tasks[url])
                previous = docs.get(url, {})
                docs[url] = {**result, 'url':url,
                             'request_ids':previous.get('request_ids', []),
                             'bindings':previous.get('bindings', [])}
                if result['status'] == 'ok':
                    added.add(url)
            doc = docs[url]
            doc['request_ids'] = unique(doc.get('request_ids', [])+[request['request_id']])
            doc['bindings'] = unique(doc.get('bindings', [])+request.get('bindings', []))
            touched.add(url)
            return doc['status'] == 'ok'

        async def group(request):
            urls = unique(request['urls'])
            successes = 0
            reused = 0
            reason = 'no_available_candidate'
            # With a row-wide cap, requests execute in declared order and reserve
            # before dispatch. Without it, independent groups/pages may overlap.
            cap = self.max_attempts is not None or self.max_new_documents is not None
            offset = 0
            while offset < len(urls) and (self.per_request is None or successes < self.per_request):
                size = 1 if cap else min(self.url_concurrency, (self.per_request or len(urls))-successes)
                batch = urls[offset:offset+size]; offset += size
                admitted = []
                for url in batch:
                    available = url in docs and docs[url]['status'] == 'ok'
                    if not available and url not in tasks and (
                        self.max_attempts is not None and len(attempted) >= self.max_attempts or
                        self.max_new_documents is not None and len(added) >= self.max_new_documents):
                        reason = 'fetch_budget'
                        continue
                    reused += int(available)
                    admitted.append(url)
                if admitted:
                    successes += sum(await asyncio.gather(*(get(url, request) for url in admitted)))
            return {'request_id':request['request_id'],
                    'status':'executed' if successes else 'no_new_document',
                    'reason':('already_available' if reused == successes else '') if successes else reason}
        try:
            capped = self.max_attempts is not None or self.max_new_documents is not None
            receipts = await bounded(requests, group, 1 if capped else self.request_concurrency)
        finally:
            for task in tasks.values():
                if not task.done(): task.cancel()
            await asyncio.gather(*tasks.values(), return_exceptions=True)
        order = unique([d['url'] for d in (row.get(self.known, []) if self.known else [])]+
                       [url for r in requests for url in r['urls']])
        request_order = unique([i for d in docs.values() if d['url'] in initial for i in d.get('request_ids', [])]+
                               [r['request_id'] for r in requests])
        binding_order = unique([i for d in docs.values() if d['url'] in initial for i in d.get('bindings', [])]+
                               [i for r in requests for i in r.get('bindings', [])])
        for doc in docs.values():
            doc['request_ids'] = sorted(unique(doc.get('request_ids', [])), key=request_order.index)
            doc['bindings'] = sorted(unique(doc.get('bindings', [])), key=binding_order.index)
        return {**row, self.output:{'documents':[docs[url] for url in order if url in docs],
                                  'receipts':receipts, 'touched_urls':[url for url in order if url in touched]}}
