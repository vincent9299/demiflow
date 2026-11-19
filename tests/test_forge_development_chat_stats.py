"""Whole-session native metrics: public evidence, bounded replay, no model calls."""
import json
from pathlib import Path
import subprocess
from urllib.parse import urlencode

import pytest

from demiflow.forge.schema import ForgeError
from demiflow.workbench.conversations import stats
from demiflow.workbench.conversations.common import root_identity
from demiflow.workbench.conversations.policy import Limits
from demiflow.workbench.conversations.store import Store
from test_forge_deepseek_stream import Frames, NODE, PLUGIN, adapter
from test_forge_development_chat_http import app, request


def metric(kind, sequence, **values):
    return dict(format=stats.FORMAT, kind=kind, event_sequence=sequence, native_turn=1, **values)


def model(sequence=7, **changes):
    value=metric('model', sequence, native_step=0, attempt_id='session:1', message_id='message:session:1',
        usage=dict(inputTokens=400, outputTokens=50, cacheReadTokens=600),
        timing=dict(llm_ms=500, ttft_ms=100, decode_ms=400))
    value.update(changes)
    return value


def history(tmp_path, provider='deepseek'):
    root=tmp_path/'project';root.mkdir()
    store=Store(tmp_path/'state', Limits(), initialize=True)
    sid=store.create('workspace', root_identity(root), provider, 'session-request', 'policy')['id']
    return store, sid, root


def submit(store, sid, root, revision, operation='message'):
    return store.submit('workspace', sid, 'request-'+str(revision), revision, 'Request',
                        root_identity(root), 'policy', operation=operation)[0]


def finish(store, turn):
    store.finish_provider(turn['id'], state='completed', cleanup='complete', result={})


def native_run(tmp_path, operations):
    # Exercise the real fixed plugin with public Session and stream events.
    # The existing protocol fixture owns no credentials or external process.
    source=(Path(__file__).parent/'fixtures/deepseek_stream.cjs').read_text()
    source=source.replace("if(op.frame){", "if(op.native){const a=agents[op.agent||'root'];handlers.get('session/event')(a.session,op.native);}\n    if(op.frame){")
    path=tmp_path/'native-metrics.cjs';path.write_text(source)
    result=subprocess.run([NODE, str(path), str(PLUGIN)], input=json.dumps(dict(operations=operations)),
                          text=True, capture_output=True, timeout=10, check=True)
    return json.loads(result.stdout)


def native(type_, sequence, time, **data):
    return dict(native=dict(type=type_, seq=sequence, time=time, data=data))


def test_native_public_metrics_keep_text_order_and_report_native_units(tmp_path):
    f=Frames()
    frame_start=f.start();block_start=f.chunk('block-start', index=0, blockType='text')
    chunk=f.chunk('text-delta', index=0, text='Visible');chunk['frame']['time']=1100
    end=f.end([dict(type='text', text='Visible')])
    end['event'].update(time=1500)
    end['event']['data'].update(turn=1, step=0, usage=dict(inputTokens=400, outputTokens=50,
        cacheReadTokens=600, private='PRIVATE_USAGE'))
    start=native('step/start', 2, 1000, turn=1, step=0)
    foreign={**start, 'agent':'foreign'}
    output=native_run(tmp_path, [dict(prompt='session'), foreign, start, frame_start,
        block_start, chunk, end,
        native('tool/call', 8, 1600, turn=1, step=0, callId='call-one', arguments='PRIVATE_TOOL'),
        native('tool/result', 9, 2000, message=dict(source=dict(callId='call-one'), content='PRIVATE_RESULT')),
        native('step/end', 10, 2100, turn=1, step=0), native('turn/end', 11, 2200, turn=1)])
    assert output['cancellations']==[]
    notifications=output['output']
    assert 'PRIVATE' not in json.dumps(notifications)
    assert [n['params']['phase'] for n in notifications if n['method']=='demiflow/assistant-stream']==[
        'start','delta','completed','settled']
    projected=[n['params']['metrics'] for n in notifications if n['method']=='demiflow/metrics']
    assert [m['kind'] for m in projected]==['step_started','model','tool','step_ended','turn_ended']
    assert projected[1]['timing']==dict(llm_ms=500, ttft_ms=100, decode_ms=400)
    assert projected[2]['duration_ms']==400
    provider=adapter();normalized=[event for n in notifications for event in provider.event(n)]
    assert provider.event(next(n for n in notifications if n['method']=='demiflow/metrics'))==[]
    store,sid,root=history(tmp_path)
    turn=submit(store,sid,root,0)
    for kind,data in normalized:
        if kind!='activity':store.event(turn['id'],kind,data)
    store.event(turn['id'],'usage.observed',dict(used=1200,size=32000))
    finish(store,turn)
    value=stats.read(store,'workspace',sid)
    assert value['turns']==value['steps']==1
    assert value['turns_basis']=='conversation_requests'
    assert value['llm_ms']==500 and value['tool_ms']==400
    assert value['tokens']==dict(input=1000,uncached_input=400,output=50,total=1050,cache_read=600,cache_write=0)
    assert value['cache_hit_ratio']==.6 and value['decode_tokens_per_second']==125
    assert value['context']==dict(used=1200,size=32000)
    assert value['coverage']['state']==value['coverage']['metrics_state']=='complete'


def test_full_history_paging_dedup_and_compaction_do_not_fabricate_totals(tmp_path):
    store,sid,root=history(tmp_path)
    old=submit(store,sid,root,0)
    for i in range(65):store.event(old['id'],'message.delta',dict(item_id='old',text='old-'+str(i)))
    store.event(old['id'],'usage.observed',dict(used=80,size=100,_meta=dict(billing=dict(inputTokens=999))))
    finish(store,old)
    current=submit(store,sid,root,1)
    for value in [metric('step_started',2,native_step=0),model(),model(),metric('step_ended',10,native_step=0)]:
        store.event(current['id'],'usage.observed',value)
    finish(store,current)
    compact=submit(store,sid,root,2,'compact')
    store.event(compact['id'],'usage.observed',model(70))
    store.event(compact['id'],'usage.observed',dict(used=1,size=100))
    finish(store,compact)
    result=stats.read(store,'workspace',sid)
    assert result['turns']==2 and result['steps']==1 and result['tokens']['total']==1050
    assert result['coverage']['events_scanned']>50
    assert result['coverage']['metrics_state']=='partial'
    assert result['coverage']['reported_turns']==1
    assert result['context']==dict(used=80,size=100)
    # A new process/cache rebuild uses the durable whole history, not a UI page.
    restored=stats.read(Store(store.root,store.limits),'workspace',sid)
    assert restored==result


def test_bounded_incremental_scan_and_cache_hit_are_read_only(tmp_path,monkeypatch):
    store,sid,root=history(tmp_path)
    turn=submit(store,sid,root,0)
    for i in range(10):store.event(turn['id'],'message.delta',dict(item_id='body',text='x'*300))
    store.event(turn['id'],'usage.observed',dict(used=3,size=100))
    finish(store,turn)
    monkeypatch.setattr(stats,'ROWS_PER_READ',3)
    monkeypatch.setattr(stats,'BYTES_PER_READ',1400)
    result=stats.read(store,'workspace',sid)
    assert result['coverage']['state']=='partial' and result['context']['used'] is None
    assert result['coverage']['events_scanned']<=3 and result['coverage']['bytes_scanned']<=1400
    for _ in range(10):
        old=result['coverage'];result=stats.read(store,'workspace',sid)
        assert result['coverage']['events_scanned']-old['events_scanned']<=3
        assert result['coverage']['bytes_scanned']-old['bytes_scanned']<=1400
        if result['coverage']['state']=='complete':break
    assert result['context']==dict(used=3,size=100)
    before=store.path.read_bytes()
    def no_decode(*args,**kwargs):raise AssertionError('Cache hit decoded saved history again')
    monkeypatch.setattr(stats.json,'loads',no_decode)
    assert stats.read(store,'workspace',sid)==result
    assert store.path.read_bytes()==before
    with pytest.raises(ForgeError,match='not found'):stats.read(store,'another-workspace',sid)


@pytest.mark.parametrize('bad',[True,-1,float('inf'),'50',{'untrusted':'number'}])
def test_invalid_native_usage_never_becomes_a_number(tmp_path,bad):
    value=model();value['usage']['outputTokens']=bad
    provider=adapter()
    with pytest.raises(ForgeError,match='usage'):
        provider.event(dict(method='demiflow/metrics',params=dict(sessionId='session',metrics=value)))
    store,sid,root=history(tmp_path)
    turn=submit(store,sid,root,0)
    if bad!=float('inf'):
        store.event(turn['id'],'usage.observed',value)
        result=stats.read(store,'workspace',sid)
        assert result['tokens']['total'] is None and result['steps'] is None
        assert result['coverage']['invalid_metrics']==1


def test_old_usage_and_foreign_nested_usage_do_not_imply_tokens_or_speed(tmp_path):
    store,sid,root=history(tmp_path)
    turn=submit(store,sid,root,0)
    store.event(turn['id'],'usage.observed',dict(used=15,size=100))
    store.event(turn['id'],'item.updated',dict(id='tool',type='tool',usage=model()['usage']))
    result=stats.read(store,'workspace',sid)
    assert result['context']==dict(used=15,size=100)
    assert result['steps'] is result['llm_ms'] is result['tool_ms'] is result['decode_tokens_per_second'] is None
    assert all(v is None for v in result['tokens'].values())
    assert result['coverage']['metrics_state']=='unavailable'


def test_missing_usage_does_not_break_stream_and_cancelled_step_is_counted(tmp_path):
    f=Frames();start=f.start();block=f.chunk('block-start',index=0,blockType='text')
    delta=f.chunk('text-delta',index=0,text='Visible');delta['frame']['time']=1100
    end=f.end([dict(type='text',text='Visible')])
    end['event'].update(time=1500)
    end['event']['data'].update(turn=1,step=0,usage=dict(inputTokens=400,outputTokens='PRIVATE_BAD_USAGE'))
    retry=Frames('session:2',f.revision)
    cancelled=[retry.start(),retry.chunk('block-start',index=0,blockType='text'),
        retry.chunk('text-delta',index=0,text='Interrupted'),retry.end(outcome=dict(kind='abandoned'))]
    value=native_run(tmp_path,[dict(prompt='session'),native('step/start',2,1000,turn=1,step=0),
        start,block,delta,end,native('step/end',8,1600,turn=1,step=0),
        native('step/start',9,1700,turn=1,step=0),*cancelled,native('step/end',10,2000,turn=1,step=0)])
    assert value['cancellations']==[] and 'PRIVATE' not in json.dumps(value['output'])
    provider=adapter();store,sid,root=history(tmp_path);turn=submit(store,sid,root,0)
    for notification in value['output']:
        for kind,data in provider.event(notification):
            if kind!='activity':store.event(turn['id'],kind,data)
    result=stats.read(store,'workspace',sid)
    assert result['steps']==2 and result['llm_ms']==500
    assert result['tokens']['total'] is result['decode_tokens_per_second'] is None
    assert result['coverage']['metrics_state']=='partial' and result['coverage']['models']==1


def test_successful_compaction_invalidates_old_context_without_adding_tokens(tmp_path):
    store,sid,root=history(tmp_path);turn=submit(store,sid,root,0)
    store.event(turn['id'],'usage.observed',dict(used=90,size=100));finish(store,turn)
    assert stats.read(store,'workspace',sid)['context']['used']==90
    compact=submit(store,sid,root,1,'compact')
    store.event(compact['id'],'usage.observed',model())
    store.event(compact['id'],'compaction.completed',dict(status='compacted'))
    finish(store,compact)
    result=stats.read(store,'workspace',sid)
    assert result['turns']==1 and result['context']['used'] is result['tokens']['total'] is None


def test_native_attempt_usage_and_retry_fence_follow_token_meter(tmp_path):
    first=Frames();first_start=first.start();first_block=first.chunk('block-start',index=0,blockType='text')
    first_text=first.chunk('text-delta',index=0,text='Interrupted');first_text['frame']['time']=1100
    first_usage=first.chunk('usage',usage=dict(inputTokens=200,outputTokens=10,private='PRIVATE_FIRST'))
    first_end=first.end(outcome=dict(kind='committed',eventType='assistant/attempt',seq=7))
    second=Frames('session:2',first.revision);second_start=second.start();second_block=second.chunk('block-start',index=0,blockType='text')
    second_text=second.chunk('text-delta',index=0,text='Done');second_text['frame']['time']=1300
    second_end=second.end([dict(type='text',text='Done')]);second_end['frame']['outcome']['seq']=17
    second_end['event'].update(seq=17,time=1500)
    second_end['event']['data'].update(turn=1,step=0,usage=model()['usage'])
    result=native_run(tmp_path,[dict(prompt='session'),native('step/start',2,1000,turn=1,step=0),
        first_start,first_block,first_text,first_usage,first_end,native('llm/retry-started',8,1200,turn=1,step=0),
        second_start,second_block,second_text,second_end,native('step/end',18,1600,turn=1,step=0)])
    assert result['cancellations']==[] and 'PRIVATE' not in json.dumps(result['output'])
    provider=adapter();store,sid,root=history(tmp_path);turn=submit(store,sid,root,0)
    for notification in result['output']:
        for kind,data in provider.event(notification):
            if kind!='activity':store.event(turn['id'],kind,data)
    value=stats.read(store,'workspace',sid)
    assert value['steps']==1 and value['tokens']['total']==1260
    assert value['tokens']['uncached_input']==600 and value['tokens']['output']==60
    assert value['llm_ms']==500 and value['decode_tokens_per_second']==125
    assert value['coverage']['attempts_with_usage']==1 and value['coverage']['metrics_state']=='complete'


def test_same_step_usage_replaces_and_message_identity_cannot_bill_twice(tmp_path):
    store,sid,root=history(tmp_path);turn=submit(store,sid,root,0)
    store.event(turn['id'],'usage.observed',model())
    # A repeated message identity cannot add usage even with a later event seq.
    store.event(turn['id'],'usage.observed',model(8))
    replacement=model(9,attempt_id='session:2',message_id='message:2',usage=dict(inputTokens=100,outputTokens=20))
    store.event(turn['id'],'usage.observed',replacement)
    value=stats.read(store,'workspace',sid)
    assert value['tokens']['total']==120 and value['coverage']['models']==2
    store.event(turn['id'],'usage.observed',metric('retry_started',10,native_step=0))
    store.event(turn['id'],'usage.observed',model(11,attempt_id='session:3',message_id='message:3'))
    assert stats.read(store,'workspace',sid)['tokens']['total']==1170


def test_stats_http_auth_session_scope_and_only_configured_effort(tmp_path):
    with app(tmp_path) as (server,workspace,root):
        status,session=request(server,workspace,'dev-session-create',dict(request_id='stats-fixture',provider_id='codex'))
        assert status==200
        query='?'+urlencode(dict(session_id=session['id']))
        assert request(server,workspace,'dev-stats',query=query,token='wrong')[0]==403
        status,value=request(server,workspace,'dev-stats',query=query)
        assert status==200 and value['turns']==0 and value['tokens']['total'] is None
        assert request(server,workspace,'dev-stats',query='?session_id=missing')[0]==404
        assert request(server,workspace,'dev-stats',query=query+'&limit=100')[0]==400
        caps=request(server,workspace,'dev-capabilities')[1]
        assert caps['conversation_stats'] is True and caps['providers'][0]['effort']=='xhigh'
        del server.development.deployment.providers['codex']['effort']
        assert 'effort' not in request(server,workspace,'dev-capabilities')[1]['providers'][0]
