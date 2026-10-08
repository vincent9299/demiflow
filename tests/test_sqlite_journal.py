"""Durability, legacy replay, concurrency and bounded request metadata."""
import asyncio
import json
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from demiflow.operator_llm.errors import PromptBudgetExceededError
from demiflow.operator_llm.journal import UncertainPromptCall, canonical, request_key
from demiflow.operator_llm.sqlite_journal import SQLitePromptJournal
from test_map_prompt_async import PACK, prompt, server


def test_registered_key_snapshot_preserves_paid_errors_and_unknowns(tmp_path):
    path=tmp_path/'history.sqlite'
    journal=SQLitePromptJournal(path,max_requests=4)
    requests=[{'item':i} for i in range(4)]
    for request in requests:journal.reserve(request)
    journal.response(requests[0],{'status_code':200,'body':'saved success'})
    journal.response(requests[1],{'status_code':500,'body':'saved error'})
    journal.failed(requests[2],RuntimeError('unknown transport outcome'),1)
    journal.close()
    before=path.read_bytes()
    history=SQLitePromptJournal(path,read_only=True)
    try:
        keys=history.registered_keys(max_keys=4)
        with pytest.raises(ValueError,match='exceeds key budget'):
            history.registered_keys(max_keys=3)
    finally:history.close()
    assert keys==frozenset(request_key(r) for r in requests)
    assert path.read_bytes()==before
    writer=sqlite3.connect(path)
    try:
        writer.execute('BEGIN EXCLUSIVE')
        # Membership stays usable without touching a contended live journal.
        assert all(request_key(r) in keys for r in requests)
        assert request_key({'item':'new'}) not in keys
    finally:writer.rollback();writer.close()


def test_registered_key_snapshot_does_not_create_a_new_journal(tmp_path):
    path=tmp_path/'absent.sqlite';history=SQLitePromptJournal(path,read_only=True)
    try:assert history.registered_keys(max_keys=0)==frozenset()
    finally:history.close()
    assert not path.exists()


def test_reservations_survive_reopen_and_budget_is_atomic(tmp_path):
    path = tmp_path / 'calls.sqlite'
    def reserve(i):
        j = SQLitePromptJournal(path, max_requests=3)
        try:
            return j.reserve({'item': i})
        except PromptBudgetExceededError:
            return False
        finally:
            j.close()
    with ThreadPoolExecutor(max_workers=8) as pool:
        assert sum(pool.map(reserve, range(16))) == 3
    journal = SQLitePromptJournal(path, max_requests=3)
    assert journal.stats() == {'requests': 3, 'responses': 0, 'uncertain': 3}
    with sqlite3.connect(path) as db:
        request = json.loads(db.execute('SELECT request_json FROM calls LIMIT 1').fetchone()[0])
    with pytest.raises(UncertainPromptCall):
        journal.lookup(request)
    with pytest.raises(UncertainPromptCall, match='no longer owns'):
        journal.response(request, {'body': 'ok'})
    journal.requeue_uncertain([request_key(request)], actor='test', reason='dead writer')
    journal.limit = 4
    journal.reserve(request)
    journal.response(request, {'body': 'ok'})
    assert journal.lookup(request) == {'body': 'ok'}
    assert journal.reserve(request) is False
    with pytest.raises(ValueError, match='Immutable'):
        journal.response(request, {'body': 'changed'})
    journal.close()


def test_same_request_is_not_sent_twice(tmp_path):
    path = tmp_path / 'calls.sqlite'
    def reserve(_):
        j = SQLitePromptJournal(path)
        try:
            return j.reserve({'item': 'same'})
        except UncertainPromptCall:
            return False
        finally:
            j.close()
    with ThreadPoolExecutor(max_workers=8) as pool:
        assert sum(pool.map(reserve, range(16))) == 1


def test_recovery_preserves_attempt_references_usage_and_budget(tmp_path):
    from demiflow.operator_llm.call_ref import read_call, journal_totals, journal_observation
    path=tmp_path/'recover.sqlite';request={'item':'same exact request'};key=request_key(request)
    journal=SQLitePromptJournal(path,max_requests=3)
    journal.reserve(request)
    first=journal.references(request)
    journal.failed(request,RuntimeError('original unknown outcome'),1)
    old_error={**first['request_ref'],'kind':'error'}
    assert 'attempt' not in old_error  # Existing references remain readable.
    journal.requeue_uncertain([key],actor='test',reason='one bounded recovery')
    assert journal_totals(path)['requests']==1
    assert journal_observation(path)['pending_reservations']==0
    journal.reserve(request)
    second=journal.references(request)
    assert second['request_ref']['attempt']==2
    error=RuntimeError('second failure with known usage')
    error.call={'usage':{'prompt_tokens':5,'completion_tokens':3}}
    journal.failed(request,error,2)
    second_error={**second['request_ref'],'kind':'error'}
    journal.requeue_uncertain([key],actor='test',reason='bounded test retry')
    journal.reserve(request)
    third=journal.response(request,{'status_code':200,'body':{'usage':{'prompt_tokens':10,'completion_tokens':4}},'elapsed_s':3})
    assert third['response_ref']['attempt']==3
    assert read_call(old_error)['detail']=='original unknown outcome'
    assert read_call(second_error)['detail']=='second failure with known usage'
    assert read_call(first['request_ref'])==request
    assert read_call(third['response_ref'])['status_code']==200
    with pytest.raises(KeyError):read_call(first['response_ref'])
    with pytest.raises(KeyError):read_call(second['response_ref'])
    totals=journal_totals(path)
    assert totals=={'requests':3,'responses':1,'transport_errors':2,'provider_responses':1,
                   'usage_records':2,'input_tokens':15,'output_tokens':7,'http_successes':1,'http_errors':0}
    observation=journal_observation(path)
    assert observation['pending_reservations']==0 and observation['errors_in_window']==2
    with pytest.raises(PromptBudgetExceededError):journal.reserve({'item':'new'})
    journal.close()


def test_saved_http_quota_error_is_not_a_success_or_known_usage(tmp_path):
    from demiflow.operator_llm.call_ref import journal_totals
    path=tmp_path/'quota.sqlite'
    journal=SQLitePromptJournal(path,max_requests=2)
    request={'item':'quota rejection'}
    journal.reserve(request)
    journal.response(request,{'status_code':429,'body':{'error':{'message':'MONTHLY_LIMIT_EXCEEDED'}},'elapsed_s':.5})
    totals=journal_totals(path)
    assert totals['requests']==totals['responses']==totals['http_errors']==1
    assert totals['http_successes']==totals['transport_errors']==totals['usage_records']==0
    journal.close()
    assert journal_totals(tmp_path/'missing.sqlite')['http_errors']==0


def test_cache_observation_distinguishes_zero_missing_errors_and_window(tmp_path):
    from demiflow.operator_llm.call_ref import journal_observation
    path=tmp_path/'cache_usage.sqlite'
    journal=SQLitePromptJournal(path,max_requests=5)
    for i,(status,usage) in enumerate([
            (200,{'prompt_tokens':100,'prompt_tokens_details':{'cached_tokens':0}}),
            (200,{'input_tokens':200,'input_tokens_details':{'cached_tokens':150}}),
            (200,{'prompt_tokens':300}),
            (500,{'prompt_tokens':100,'prompt_tokens_details':{'cached_tokens':50}})],1):
        request={'item':i};journal.reserve(request)
        journal.response(request,{'status_code':status,'elapsed_s':i,
                                 'body':{'created':i,'usage':usage}})
    journal.reserve({'item':'pending'})
    observed=journal_observation(path,since=2)
    total=observed['provider_cache_total'];window=observed['provider_cache_window']
    assert observed['pending_reservations']==1
    assert total['successful_responses']==3 and total['reported_responses']==2
    assert total['missing_or_invalid_responses']==1
    assert total['cached_tokens_reported']==150 and total['input_tokens']==600
    assert total['token_hit_rate_on_reported_inputs']==.5
    assert total['reported_cached_share_all_inputs']==.25
    assert window['reported_responses']==1 and window['missing_or_invalid_responses']==1
    assert window['token_hit_rate_on_reported_inputs']==.75
    assert window['reported_cached_share_all_inputs']==.3
    journal.close()
    assert journal_observation(tmp_path/'missing.sqlite')['provider_cache_total']['token_hit_rate_on_reported_inputs'] is None


def test_http_image_identity_replay_and_no_binary_copy(server, tmp_path, monkeypatch):
    from demiflow.data.api import DataAPI
    from demiflow.operator_llm import journal as module
    path = tmp_path / 'calls.sqlite'
    encoded = 'data:image/png;base64,' + 'YWJj' * 400000
    pack = PACK.replace('{{ payload | json }}', '{{ payload | image }}')
    original = module.canonical
    hashes = []
    def counted(value):
        if isinstance(value, module.RequestRecord):
            hashes.append(threading.get_ident())
        return original(value)
    monkeypatch.setattr(module, 'canonical', counted)
    for limit in (1, 0):
        rows = prompt(DataAPI().from_items([{'item': encoded}]), pack=pack,
                      options={'sqlite_journal': {'path': str(path)}}, max_requests=limit,
                      call_output='call').materialize().take_all()
        assert rows[0]['answer'] == 'ok'
        assert rows[0]['call']['reused'] is (limit == 0)
    assert len(server['requests']) == 1
    assert encoded in json.dumps(server['requests'][0]['body'])
    assert len(hashes) == 2  # one exact-request hash per attempt, including replay
    assert path.stat().st_size < 100000
    with sqlite3.connect(path) as db:
        saved = db.execute('SELECT request_json FROM calls').fetchone()[0]
        assert encoded not in saved and 'data_uri_sha256' in saved
    assert rows[0]['call']['request_id']


def legacy_snapshot(root, name, records):
    import lance
    import pyarrow as pa
    rows = [{'key': kind + '/' + key, 'payload': canonical(value), 'written_version': 1}
            for kind, key, value in records]
    schema = pa.schema([('key', pa.string()), ('payload', pa.large_string()), ('written_version', pa.int64())])
    lance.write_dataset(pa.Table.from_pylist(rows, schema=schema), str(root / name))
    return {'root': root, 'relative_uri': name, 'version': 1}


def test_legacy_import_copies_responses_not_images_and_preserves_refs(tmp_path, monkeypatch):
    import lance
    from demiflow.operator_llm.call_ref import read_call
    request = {'image': 'data:image/png;base64,' + 'YWJj' * 10000}
    key = request_key(request)
    source = legacy_snapshot(tmp_path, 'legacy.lance', [
        ('request', key, request), ('response', key, {'body':'ok'}),
        ('request', request_key({'pending':1}), {'pending':1})])
    journal = SQLitePromptJournal(tmp_path / 'calls.sqlite')
    original = lance.LanceDataset.to_batches
    scans = []
    def scan(ds, *args, **kwargs):
        scans.append(kwargs)
        return original(ds, *args, **kwargs)
    monkeypatch.setattr(lance.LanceDataset, 'to_batches', scan)
    assert journal.import_lance(**source) == {'requests':2,'responses':1,'uncertain':1}
    assert all('payload' not in scan['columns'] for scan in scans if 'request/' in scan.get('filter',''))
    result = journal.lookup(request)
    assert result['body'] == 'ok' and result['_legacy_refs']['request_ref']['version']==1
    assert read_call(result['_legacy_refs']['request_ref'], tmp_path)==request
    assert read_call(journal.references(request)['response_ref'])=={'body':'ok'}
    with pytest.raises(UncertainPromptCall):journal.lookup({'pending':1})
    journal.reserve({'new':1})
    assert journal.import_lance(**source)['requests']==3
    assert len(scans)==2
    assert lance.dataset(str(tmp_path/'legacy.lance')).version==1
    journal.close()


def test_interrupted_import_rolls_back(tmp_path):
    source=legacy_snapshot(tmp_path,'old.lance',[('request',request_key({'a':1}),{'a':1})])
    journal=SQLitePromptJournal(tmp_path/'calls.sqlite')
    def stop(_):raise RuntimeError('interrupted')
    with pytest.raises(RuntimeError,match='interrupted'):journal.import_lance(**source,log=stop)
    assert journal.stats()['requests']==0
    assert journal.import_lance(**source)['uncertain']==1
    journal.close()


def test_real_http_legacy_replay_keeps_identity_and_makes_zero_new_calls(server,tmp_path):
    from demiflow.data.api import DataAPI
    from demiflow.operator_llm.call_ref import read_call
    before=prompt(DataAPI().from_items([{'item':1}]),options={'sqlite_journal':{'path':str(tmp_path/'initial.sqlite')}},
                  call_output='call').materialize().take_all()[0]
    request=read_call(before['call']['request_ref'])
    response=read_call(before['call']['response_ref'])
    key=request_key(request)
    source=legacy_snapshot(tmp_path,'old.lance',[('request',key,request),('response',key,response)])
    journal=SQLitePromptJournal(tmp_path/'new.sqlite');journal.import_lance(**source);journal.close()
    after=prompt(DataAPI().from_items([{'item':1}]),options={'sqlite_journal':{'path':str(tmp_path/'new.sqlite')}},
                 call_output='call',max_requests=0).materialize().take_all()[0]
    assert after['answer']==before['answer']
    assert after['call']['reused'] and len(server['requests'])==1
    assert read_call(after['call']['response_ref'])==response


def test_slow_cache_does_not_block_event_loop(monkeypatch):
    from demiflow.operator_llm import runtime
    from demiflow.operator_llm.model import OperatorLLMResponse
    from demiflow.operator_llm.parser import parse_prompt_pack
    class Client:
        def lookup(self, request):
            time.sleep(.12)
            return OperatorLLMResponse('{"result":"ok"}')
        async def execute(self, request):
            raise AssertionError('cache hit must not call provider')
        async def aclose(self):
            pass
    monkeypatch.setattr(runtime, 'create_async_operator_llm_client', lambda _: Client())
    async def run():
        rt = runtime.AsyncOperatorLLMRuntime(parse_prompt_pack(PACK), runtime.InProcessOperatorLLMCoordinator(0))
        task = asyncio.create_task(rt.call('enrich', {'payload': 1}))
        ticks = 0
        while not task.done():
            await asyncio.sleep(.01)
            ticks += 1
        assert await task == {'result': 'ok'}
        assert ticks >= 5
        await rt.aclose()
    asyncio.run(run())


def test_cancel_waits_for_pending_journal_io():
    from demiflow.operator_llm.client import _journal_io
    entered, done = threading.Event(), threading.Event()
    def commit():
        entered.set()
        time.sleep(.08)
        done.set()
    async def run():
        task = asyncio.create_task(_journal_io(commit))
        while not entered.is_set():
            await asyncio.sleep(.001)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert done.is_set()
    asyncio.run(run())


def test_concurrent_first_requests_verify_model_once(server, monkeypatch):
    import httpx
    from demiflow.data.api import DataAPI
    checks = []
    async def models(self, url, **kwargs):
        checks.append(url)
        await asyncio.sleep(.04)
        return httpx.Response(200, json={'data': [{'id': 'mock-model'}]},
                              request=httpx.Request('GET', url))
    monkeypatch.setattr(httpx.AsyncClient, 'get', models)
    prompt(DataAPI().from_items([{'item': i} for i in range(8)]),
           options={'verify_model': True}, concurrency=8).materialize()
    assert len(checks) == 1 and len(server['requests']) == 8
