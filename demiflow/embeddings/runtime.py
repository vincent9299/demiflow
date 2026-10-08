"""Action-owned batched HTTP inference, journals, admission and bounded I/O."""
import asyncio
from dataclasses import replace
from datetime import datetime, timezone
from functools import partial
import json
import os
from pathlib import Path
import time
import uuid

from demiflow.execution.request_limits import RequestGate, drain_on_service_stop
from demiflow.execution.blocking_io import BlockingIOPool
from demiflow.execution.stream_resources import CallMetrics
from demiflow.operator_llm.errors import PromptBudgetExceededError
from demiflow.operator_llm.sqlite_journal import SQLitePromptJournal
from demiflow.services import ManagedHTTPService, VLLMService
from .model import EmbeddingModel
from .payload import EmbeddingProtocolError, _add, prepare, prepare_batches, response_record, vectors


# Compatibility for callers that imported the former internal validator.
from .config import embedding_options as runtime_options


class EmbeddingMetrics(CallMetrics):
    """Constant-size phase totals; overlapping work is not additive wall time."""
    def reset(self):
        super().reset()
        self.phases = {}
        self.batches = 0

    def observe_phases(self, timings):
        if timings is None:
            return
        self.batches += 1
        for key, value in timings.items():
            item = self.phases.setdefault(key, {'count': 0, 'total': 0., 'max': 0.})
            item['count'] += 1
            item['total'] += value
            item['max'] = max(item['max'], value)

    def summary(self):
        return {**super().summary(), 'profiled_batches': self.batches, 'phases': self.phases}


class EmbeddingActor:
    def __init__(self, *, model, inputs, output, call_output, error_output,
                 concurrency, label, options, max_requests, service, request_gate):
        if not isinstance(model, EmbeddingModel):
            raise TypeError('model must be EmbeddingModel')
        if (not isinstance(inputs, dict) or len(inputs) != 1 or
                next(iter(inputs)) not in {'image', 'text', 'document'} or
                any(not isinstance(v, str) or not v for v in inputs.values())):
            raise ValueError('inputs must map exactly one of image/text/document to a row column')
        if 'image' in inputs and model.input_format != 'chat':
            raise ValueError('Image embeddings require input_format=chat')
        destinations = [v for v in (output, call_output, error_output) if v is not None]
        if (not output or any(not isinstance(v, str) or not v for v in destinations)
                or len(set(destinations)) != len(destinations)):
            raise ValueError('Embedding output columns must be nonempty and distinct')
        if max_requests is not None and (type(max_requests) is not int or max_requests < 1):
            raise ValueError('max_requests must be a positive integer or None')
        if service is not None and not isinstance(service, (VLLMService, ManagedHTTPService)):
            raise TypeError('service must be VLLMService, ManagedHTTPService or None')
        if request_gate is not None and not isinstance(request_gate, RequestGate):
            raise TypeError('request_gate must be RequestGate or AdaptiveRequestGate')
        self.model = replace(model)  # Copy nested declaration mappings at plan time.
        self.inputs, self.output = dict(inputs), output
        self.call_output, self.error_output = call_output, error_output
        self.concurrency, self.label = concurrency, label or 'map_embeddings'
        self.options, self.max_requests = runtime_options(options), max_requests
        self.service = service
        if service is not None:
            service.bind(self.model)  # Validate endpoint binding without acquiring resources.
        self.request_gate = request_gate if request_gate is not None else RequestGate(concurrency)
        self.call_metrics = EmbeddingMetrics()
        self._runtime = self  # StreamResources' shared native-model observation protocol.
        self._started = False
        self._pool = self._prepare_pool = self._client = self._owner = self._journal = None
        self._response_pool = None
        self._document_reader = None

    @property
    def cancel_drain_timeout_s(self):
        journal = self.options['sqlite_journal']
        return self.options['timeout_s'] + 3 * journal.get('timeout_s', 30) + 1 if journal else 0

    async def astart(self):
        import httpx
        if self._started:
            raise RuntimeError('Embedding node is already executing another action')
        self._started = True
        self._started_at = time.perf_counter()
        self.call_metrics.reset()
        if 'document' in self.inputs:
            from .documents import DocumentInputReader
            self._document_reader = DocumentInputReader(self.options)
        self._calls, self._locks = 0, {}
        self._pool = BlockingIOPool(self.options['io_workers'], name='demiflow-embeddings-journal')
        self._prepare_pool = BlockingIOPool(self.options['prepare_workers'], name='demiflow-embeddings-prepare')
        self._response_pool = BlockingIOPool(self.options['response_workers'], name='demiflow-embeddings-response')
        self._request_slots = asyncio.Semaphore(self.concurrency)
        self._journal = (SQLitePromptJournal(max_requests=self.max_requests,
                          **self.options['sqlite_journal']) if self.options['sqlite_journal'] else None)
        self._owner = self.service.bind(self.model) if self.service is not None else None
        headers = {'Content-Type': 'application/json'}
        self._api_key = os.environ.get(self.model.api_key_env) if self.model.api_key_env else None
        if self._api_key:
            headers['Authorization'] = 'Bearer ' + self._api_key
        self._client = httpx.AsyncClient(headers=headers, trust_env=self.options['trust_env'],
            limits=httpx.Limits(max_connections=self.concurrency, max_keepalive_connections=self.concurrency,
                                keepalive_expiry=self.options['keepalive_expiry_s']),
            timeout=httpx.Timeout(connect=self.options['connect_timeout_s'], read=self.options['read_timeout_s'],
                                  write=self.options['write_timeout_s'], pool=self.options['pool_timeout_s']))

    async def _io(self, fn, *args, _phase=None, _timings=None, _prepare=False, _response=False, **kwargs):
        # Admission precedes submission: the executor cannot accumulate unbounded jobs.
        started = time.perf_counter()
        pool = self._prepare_pool if _prepare else self._response_pool if _response else self._pool
        phases = [_phase] if _phase else []
        if _phase == 'journal':
            phases.append('journal_' + fn.__name__)
        def execute():
            began = time.perf_counter()
            for phase in phases:
                _add(_timings, phase + '_queue_s', began - started)
            try:
                return fn(*args, **kwargs)
            finally:
                elapsed = time.perf_counter() - began
                for phase in phases:
                    _add(_timings, phase + '_s', elapsed)
        return await pool.run(execute)

    async def _http(self, encoded):
        return await asyncio.wait_for(self._read_http(encoded), self.options['timeout_s'])

    async def _read_http(self, encoded):
        async with self._client.stream('POST', self.model.base_url.rstrip('/') + '/embeddings',
                                       content=encoded) as response:
            content = bytearray()
            async for chunk in response.aiter_bytes(chunk_size=65536):
                content.extend(chunk)
                if len(content) > self.options['max_response_bytes']:
                    raise EmbeddingProtocolError('Embedding response exceeds max_response_bytes')
            return response.status_code, bytes(content).decode('utf-8')

    async def _exchange(self, request, encoded, timings):
        journal = self._journal
        io = partial(self._io, _phase='journal', _timings=timings)
        record = await io(journal.lookup, request) if journal is not None else None
        references = None
        reused = record is not None
        if record is None:
            if journal is not None and journal.read_only:
                raise LookupError('Embedding request missing from read-only journal: ' + request.request_key)
            if self.model.api_key_env and not self._api_key:
                raise ValueError('Missing embedding API key environment variable: ' + self.model.api_key_env)
            if self._owner is not None:
                started = time.perf_counter()
                await self._owner.ensure_ready()
                _add(timings, 'service_wait_s', time.perf_counter() - started)
            started = time.perf_counter()
            async with self._request_slots, self.request_gate.enter():
                _add(timings, 'admission_wait_s', time.perf_counter() - started)
                if journal is not None:
                    reserved = await io(journal.reserve, request)
                    if not reserved:
                        record = await io(journal.lookup, request)
                        reused = True
                elif self.max_requests is not None and self._calls >= self.max_requests:
                    raise PromptBudgetExceededError('Embedding request budget exhausted')
                if record is None:
                    self._calls += 1
                    record, references = await drain_on_service_stop(
                        self._fresh_exchange(request, encoded, timings), enabled=journal is not None)
        call = {key: record[key] for key in ('computed_at', 'elapsed_s')}
        call.update(request_id=request.request_key, reused=reused)
        if journal is not None:
            call.update(references if references is not None else await io(journal.references, request))
        if not 200 <= record['status_code'] < 300:
            import httpx
            self.call_metrics.observe(call, error=True)
            response = httpx.Response(record['status_code'],
                request=httpx.Request('POST', self.model.base_url.rstrip('/') + '/embeddings'))
            response.raise_for_status()
        return record, call

    async def _fresh_exchange(self, request, encoded, timings):
        journal = self._journal
        started = time.monotonic()
        try:
            status, raw = await self._http(encoded)
            _add(timings, 'http_s', time.monotonic() - started)
            record = await self._io(response_record, status, raw,
                                    _phase='response_parse', _timings=timings, _response=True)
        except BaseException as error:
            import httpx
            elapsed = time.monotonic() - started
            if journal is not None:
                await self._io(journal.failed, request, error, elapsed, _phase='journal', _timings=timings)
            self.call_metrics.observe({'elapsed_s': elapsed}, error=True)
            status = getattr(getattr(error, 'response', None), 'status_code', None)
            self.request_gate.result(
                transient=isinstance(error, (httpx.TransportError, TimeoutError))
                          or status in {408, 429} or (status is not None and status >= 500),
                fatal=f'embedding_http_{status}' if status in {400, 401, 403, 404} else '',
                elapsed_s=elapsed)
            raise
        record.update(elapsed_s=time.monotonic() - started,
                      computed_at=datetime.now(timezone.utc).isoformat())
        # Preserve the complete provider response before interpreting vectors.
        references = None
        if journal is not None:
            references = await self._io(journal.response, request, record, _phase='journal', _timings=timings)
        self.request_gate.result(success=200 <= status < 300,
            transient=status in {408, 429} or status >= 500,
            fatal=f'embedding_http_{status}' if status in {400, 401, 403, 404} else '',
            elapsed_s=record['elapsed_s'])
        return record, references

    async def __call__(self, rows):
        timings = {} if self.options['profile_path'] is not None else None
        started = time.perf_counter()
        try:
            return await self._encode(rows, timings)
        finally:
            _add(timings, 'batch_wall_s', time.perf_counter() - started)
            self.call_metrics.observe_phases(timings)

    async def _encode(self, rows, timings):
        if self.options['batch_request_bytes'] is None and self.options['batch_decode_pixels'] is None:
            prepared = await self._io(
                prepare, rows, model=self.model, inputs=self.inputs, options=self.options,
                error_output=self.error_output, timings=timings, document_reader=self._document_reader,
                _phase='prepare', _timings=timings, _prepare=True)
            return await self._encode_prepared(prepared, timings)
        preparation_timings = {} if timings is not None else None
        batches = prepare_batches(rows, model=self.model, inputs=self.inputs, options=self.options,
                                  error_output=self.error_output, timings=preparation_timings,
                                  document_reader=self._document_reader)
        output = []
        try:
            while True:
                prepared = await self._io(next, batches, None,
                    _phase='prepare', _timings=preparation_timings, _prepare=True)
                call_timings = dict(preparation_timings) if timings is not None else None
                if preparation_timings is not None:
                    preparation_timings.clear()
                try:
                    if prepared is None:
                        break
                    output.extend(await self._encode_prepared(prepared, call_timings))
                    del prepared  # Release encoded request before preparing its successor.
                finally:
                    for key, value in (call_timings or {}).items():
                        _add(timings, key, value)
        finally:
            await self._io(batches.close, _prepare=True)
        return output

    async def _encode_prepared(self, prepared, timings):
        valid, invalid, metadata, encoded, request = prepared
        output = []
        for row, error in invalid:
            item = {**row, self.output: None, self.error_output: error}
            if self.call_output:
                item[self.call_output] = None
            output.append(item)
        if not valid:
            return output
        # Single-flight identical batches inside this action; entries live only while in use.
        key = request.request_key
        entry = self._locks.setdefault(key, [asyncio.Lock(), 0])
        entry[1] += 1
        try:
            async with entry[0]:
                record, call = await self._exchange(request, encoded, timings)
        finally:
            entry[1] -= 1
            if not entry[1]:
                del self._locks[key]
        try:
            response = (await self._io(json.loads, record['raw_response'],
                                      _phase='response_parse', _timings=timings, _response=True)
                        if 'raw_response' in record else record['body'])
            embeddings = await self._io(vectors, response, len(valid), self.model,
                                         _phase='vector_validation', _timings=timings, _response=True)
        except (ValueError, TypeError):
            self.call_metrics.observe(call, error=True)
            raise
        call['usage'] = response.get('usage') or {}
        if timings is not None:
            call['timings'] = timings
        self.call_metrics.observe(call)
        for index, (row, vector, meta) in enumerate(zip(valid, embeddings, metadata)):
            item = {**row, self.output: vector}
            if self.call_output:
                item[self.call_output] = {**call, **meta, 'index': index}
            if self.error_output:
                item[self.error_output] = None
            output.append(item)
        return output

    async def aclose(self):
        errors = []
        for resource in (self._client, self._owner):
            if resource is not None:
                try:
                    await resource.aclose()
                except Exception as error:
                    errors.append(error)
        if self._pool is not None:
            try:
                for pool in (self._pool, self._prepare_pool, self._response_pool):
                    await pool.drain()
                if self._journal is not None:
                    await self._io(self._journal.close)
                if self.options['profile_path']:
                    report = {**self.call_metrics.summary(),
                              'action_wall_s': time.perf_counter() - self._started_at,
                              'concurrency': self.concurrency,
                              'prepare_workers': self.options['prepare_workers'],
                              'response_workers': self.options['response_workers'],
                              'io_workers': self.options['io_workers']}
                    def write_profile():
                        path = Path(self.options['profile_path'])
                        path.parent.mkdir(parents=True, exist_ok=True)
                        temporary = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
                        try:
                            temporary.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
                            temporary.replace(path)
                        finally:
                            temporary.unlink(missing_ok=True)
                    await self._io(write_profile)
            except Exception as error:
                errors.append(error)
            finally:
                for pool in (self._pool, self._prepare_pool, self._response_pool):
                    await pool.aclose()
        self._client = self._owner = self._journal = self._pool = self._prepare_pool = None
        self._response_pool = None
        if self._document_reader is not None:
            self._document_reader.clear()
        self._document_reader = None
        self._started = False
        if errors:
            raise ExceptionGroup('Embedding resource cleanup failed', errors)
