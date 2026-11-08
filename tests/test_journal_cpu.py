"""Snapshot refs, bounded ownership reuse, and no-op handoff regressions."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
import threading

import httpx
import pytest

from demiflow.operator_llm.call_ref import PromptRecordRef, read_call
from demiflow.operator_llm.journal import UncertainPromptCall, request_key
from demiflow.operator_llm.sqlite_journal import SQLitePromptJournal


@pytest.mark.parametrize('mode', ['lookup', 'save'])
def test_attempt_ref_stays_with_observed_response_during_another_client_retry(tmp_path, monkeypatch, mode):
    path = tmp_path/'calls.sqlite'; request = {'row':1}
    first = SQLitePromptJournal(path, max_requests=3)
    second = SQLitePromptJournal(path, max_requests=3)
    rejected = {'status_code':429,'body':{'error':'retry later'},'elapsed_s':1}
    success = {'status_code':200,'body':{'result':'ok'},'elapsed_s':2}
    first.reserve(request)
    if mode == 'lookup': first.response(request, rejected)
    observed, release = threading.Event(), threading.Event()
    shard = first._shards[0]; original = shard._references_at
    def references(key, attempt):
        observed.set()
        assert release.wait(5)
        return original(key, attempt)
    monkeypatch.setattr(shard, '_references_at', references)
    def action():
        return (first.lookup_with_references(request) if mode=='lookup'
                else (rejected, first.response(request, rejected)))
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(action)
            try:
                assert observed.wait(5)
                second.reserve_http_retry(request, expected_attempt=1, expected_status=429, max_attempts=3)
                current_refs = second.response(request, success)
            finally: release.set()
            saved, refs = future.result(5)
        assert saved == rejected and 'attempt' not in refs['response_ref']
        assert current_refs['response_ref']['attempt'] == 2
        assert read_call(refs['response_ref']) == rejected
        assert read_call(current_refs['response_ref']) == success
    finally:
        release.set(); first.close(); second.close()


@pytest.mark.parametrize('throughput', [None, {}, {'shards':2}])
def test_combined_lookup_matches_public_contract_and_does_not_cache_rows(tmp_path, throughput):
    journal = SQLitePromptJournal(tmp_path/'calls.sqlite', max_requests=4, throughput=throughput)
    request = {'row':'one'}; record = {'status_code':200,'body':{'text':'中文'},'elapsed_s':.1}
    try:
        assert journal.lookup_with_references(request) == (None, None)
        journal.reserve(request)
        with pytest.raises(UncertainPromptCall) as error: journal.lookup_with_references(request)
        assert error.value.call['request_id'] == request_key(request)
        refs = journal.response(request, record)
        saved, got = journal.lookup_with_references(request)
        assert saved == record == journal.lookup(request)
        assert got == refs == journal.references(request)
        got['request_ref']['kind'] = 'changed'; saved['body']['text'] = 'changed'
        assert journal.lookup_with_references(request) == (record, refs)
        reader = SQLitePromptJournal(journal.path, read_only=True, throughput=throughput)
        try: assert reader.lookup_with_references(request) == (record, refs)
        finally: reader.close()
    finally: journal.close()


def test_native_refs_are_fresh_and_other_dataclass_values_keep_deep_copy():
    for attempt in (1,2):
        ref = PromptRecordRef('/tmp/中文.sqlite','f'*64,'response',attempt)
        expected = asdict(ref)
        if attempt == 1: expected.pop('attempt')
        assert ref.to_dict() == expected
        value = ref.to_dict(); value['kind'] = 'changed'
        assert ref.to_dict() == expected
    @dataclass(frozen=True)
    class Extended(PromptRecordRef):
        extra: object = None
    ref = Extended('p','key','response',extra=[{'a':[]}])
    value = ref.to_dict(); value['extra'][0]['a'].append(1)
    assert ref.extra == [{'a':[]}]
    unusual = PromptRecordRef(['p'],'key','response')
    unusual.to_dict()['journal_path'].append('changed')
    assert unusual.journal_path == ['p']


def test_process_identity_is_scoped_to_pid_and_failed_read_is_not_cached(tmp_path, monkeypatch):
    from demiflow.operator_llm import sqlite_journal as module
    journal = SQLitePromptJournal(tmp_path/'calls.sqlite', throughput={})
    shard = journal._shards[0]; pid = [10]; reads = []
    monkeypatch.setattr(module.os, 'getpid', lambda:pid[0])
    def start(number): reads.append(number); return None if len(reads)==1 else 'start-'+str(number)
    monkeypatch.setattr(module, '_process_start', start)
    try:
        for _ in range(4): lease = shard._lease('f'*64)
        assert reads == [10,10] and (lease.pid,lease.start) == (10,'start-10')
        pid[0] = 11
        lease = shard._lease('e'*64)
        assert reads == [10,10,11] and (lease.pid,lease.start) == (11,'start-11')
    finally: journal.close()


@pytest.mark.asyncio
async def test_prepared_send_skips_empty_handoff_but_direct_send_still_prepares(tmp_path, monkeypatch):
    from demiflow.operator_llm.client import AsyncOperatorLLMClient
    from demiflow.operator_llm.parser import parse_prompt_pack
    from demiflow.operator_llm.runtime import AsyncOperatorLLMRuntime, InProcessOperatorLLMCoordinator
    from test_map_prompt_async import PACK
    monkeypatch.setenv('TEST_PROMPT_KEY', 'fixture')
    pack = parse_prompt_pack(PACK.replace('base_url_env: TEST_PROMPT_URL','base_url: http://fixture.invalid/v1'))
    model = pack.prompt_definitions['enrich'].model
    client = AsyncOperatorLLMClient(model, {'trust_env':False,'sqlite_journal':{'path':str(tmp_path/'calls.sqlite')}})
    sends = []
    def handle(request):
        sends.append(request.content)
        return httpx.Response(200,json={'choices':[{'message':{'content':'{"result":"ok"}'}}]})
    client.client = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    submissions = []
    original = client._prepare_pool.run
    async def counted(fn,*a,**kw):
        submissions.append(fn.__name__); return await original(fn,*a,**kw)
    monkeypatch.setattr(client._prepare_pool,'run',counted)
    runtime = AsyncOperatorLLMRuntime(pack,InProcessOperatorLLMCoordinator())
    try:
        result = await runtime.call_with_trace('enrich',{'payload':1},client_override=client)
        assert result[0] == {'result':'ok'}
        assert submissions == ['prepare'] and len(sends)==1
        result = await runtime.call_with_trace('enrich',{'payload':1},client_override=client)
        assert result[1]['reused'] and len(sends)==1
        # Direct execute callers have not passed through prepare_lookup.
        exchange = runtime._exchange(pack.prompt_definitions['enrich'], [])
        request = next(exchange)
        await client.execute(request)
        assert submissions[-1] == 'record'
        # Another client may complete between prepare_lookup and reserve.
        from dataclasses import replace
        request = replace(request, prompt_version='race')
        prepared, saved = await client.prepare_lookup(request)
        assert saved is None
        writer = SQLitePromptJournal(tmp_path/'calls.sqlite')
        try:
            writer.reserve(prepared.source)
            writer.response(prepared.source, {'status_code':200,'elapsed_s':.1,
                'body':{'choices':[{'message':{'content':'{"result":"ok"}'}}]}})
        finally: writer.close()
        io_calls = []; run = client._journal_pool.run
        async def counted_io(fn,*a,**kw):
            io_calls.append(fn.__name__); return await run(fn,*a,**kw)
        monkeypatch.setattr(client._journal_pool,'run',counted_io)
        calls_before = len(sends)
        response = await client.execute(prepared)
        assert response.metadata['reused'] and len(sends) == calls_before
        assert len(io_calls) == 1
    finally:
        await client.aclose(); await runtime.aclose()
