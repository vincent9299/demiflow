"""Native public stream projection, bounded forwarding and ACP normalization."""
import json
from pathlib import Path
import shutil
import subprocess
import sys
import time

import pytest

from demiflow.forge.schema import ForgeError
from demiflow.workbench.conversations.deepseek import DeepSeek
from demiflow.workbench.conversations.policy import Limits
from demiflow.workbench.conversations.transport import Transport
from test_forge_development_chat_providers import Wire

ROOT=Path(__file__).parents[1]
PLUGIN=ROOT/'demiflow/workbench/conversations/deepseek_stream.cjs'
NODE=shutil.which('node') or '/yzp/zhaozy/yangzepeng/0905/.dsh/node/bin/node'


class Frames:
    def __init__(self, attempt='session:1', revision=0):
        self.attempt=attempt;self.revision=revision;self.index=0

    def frame(self, type_, **kwargs):
        self.revision+=1
        return dict(type=type_,attemptId=self.attempt,revision=self.revision,**kwargs)

    def start(self):return dict(frame=self.frame('start',turn=1,step=0))

    def chunk(self,type_,**kwargs):
        value=dict(frame=self.frame('chunk',index=self.index,time=1,chunk=dict(type=type_,**kwargs)))
        self.index+=1
        return value

    def end(self,content=None,*,interrupted=False,outcome=None):
        if outcome is not None:return dict(frame=self.frame('end',index=self.index,outcome=outcome))
        return dict(frame=self.frame('end',index=self.index,outcome=dict(kind='committed',eventType='assistant/message',seq=7)),
            event=dict(type='assistant/message',seq=7,data=dict(message=dict(id='message:'+self.attempt,
                content=content,replayState='PRIVATE_REPLAY',source='PRIVATE_SOURCE'),interrupted=interrupted)))


def run(operations):
    if not Path(NODE).is_file():pytest.skip('Node is not available')
    result=subprocess.run([NODE,str(Path(__file__).parent/'fixtures/deepseek_stream.cjs'),str(PLUGIN)],
        input=json.dumps(dict(operations=operations)),text=True,capture_output=True,timeout=10,check=True)
    value=json.loads(result.stdout)
    assert value['remainingListeners']==0
    return value


def adapter(submission='request-one'):
    value=DeepSeek(Wire(),dict(model='fixture'),dict(root='/project'),saved_session='session')
    value.start('Human request',submission)
    return value


def test_first_actual_token_precedes_end_coalesces_and_canonical_completion_replaces():
    f=Frames()
    result=run([dict(prompt='session'),f.start(),f.chunk('block-start',index=0,blockType='text'),
        f.chunk('text-delta',index=0,text='Hello'),dict(checkpoint='first-token'),
        f.chunk('text-delta',index=0,text=' '),f.chunk('text-delta',index=0,text='world'),
        dict(checkpoint='batched'),dict(wait=180),dict(checkpoint='timer'),
        f.chunk('block-end',index=0,block=dict(type='text',text='Hello world!',metadata='PRIVATE_BLOCK')),
        f.chunk('finish',reason=dict(kind='stop'),replayState='PRIVATE_FINISH'),
        f.end([dict(type='text',text='Hello world!')])])
    output=result['output'];checkpoints={c['name']:c['count'] for c in result['checkpoints']}
    assert checkpoints['first-token']==2 and checkpoints['batched']==2 and checkpoints['timer']==3
    assert [o['params']['text'] for o in output if o['params']['phase']=='delta']==['Hello',' world']
    provider=adapter();normalized=[event for notification in output for event in provider.event(notification)]
    bodies=[data for kind,data in normalized if kind=='message.completed']
    assert [b['text'] for b in bodies]==['Hello world!','Hello world!']
    assert bodies[-1]['status']=='complete' and not bodies[-1]['interrupted']
    assert len({data['item_id'] for kind,data in normalized if kind.startswith('message.')})==1
    assert 'PRIVATE' not in json.dumps(output)
    duplicate=dict(method='session/update',params=dict(sessionId='session',update=dict(
        sessionUpdate='agent_message_chunk',messageId='message:session:1',content=dict(type='text',text='Hello world!'))))
    assert provider.event(duplicate)==[]
    assert provider.event(dict(method='session/update',params=dict(sessionId='session',update=dict(
        sessionUpdate='usage_update',used=4))))[0][0]=='usage.observed'
    assert provider.event(dict(method='session/update',params=dict(sessionId='session',update=dict(
        sessionUpdate='tool_call',toolCallId='tool',title='Inspect',status='in_progress'))))[1][0]=='item.updated'


def test_repeated_text_in_different_blocks_attempts_and_submissions_is_not_deduplicated():
    f=Frames();ops=[dict(prompt='session'),f.start()]
    for index,type_ in [(0,'reasoning'),(2,'text'),(3,'text')]:
        ops += [f.chunk('block-start',index=index,blockType=type_),f.chunk(type_+'-delta',index=index,text='same')]
    ops += [f.end([dict(type='reasoning',text='same'),dict(type='text',text='same'),dict(type='text',text='same')])]
    second=Frames('session:2',f.revision)
    ops += [second.start(),second.chunk('block-start',index=0,blockType='text'),
        second.chunk('text-delta',index=0,text='same'),second.end([dict(type='text',text='same')])]
    output=run(ops)['output'];one=adapter();two=adapter('request-two')
    first=[data for notification in output for kind,data in one.event(notification) if kind=='message.delta']
    other=[data for notification in output for kind,data in two.event(notification) if kind=='message.delta']
    assert len(first)==4 and len({d['item_id'] for d in first})==4
    assert {d['item_id'] for d in first}.isdisjoint(d['item_id'] for d in other)
    assert [d['channel'] for d in first]==['process','answer','answer','answer']


def test_fast_native_tokens_are_coalesced_before_the_transport_queue():
    f=Frames();ops=[dict(prompt='session'),f.start(),f.chunk('block-start',index=0,blockType='text')]
    ops += [f.chunk('text-delta',index=0,text='x') for _ in range(1000)]
    ops += [f.chunk('block-end',index=0,block=dict(type='text',text='x'*1000)),f.end([dict(type='text',text='x'*1000)])]
    result=run(ops)
    assert len(result['output'])==6 and result['cancellations']==[]
    assert ''.join(event['params']['text'] for event in result['output'] if event['params']['phase']=='delta')=='x'*1000


@pytest.mark.parametrize('outcome',[dict(kind='abandoned'),dict(kind='committed',eventType='assistant/attempt',seq=5)])
def test_failed_attempt_retains_incomplete_prefix_then_retry_has_new_identity(outcome):
    f=Frames();ops=[dict(prompt='session'),f.start(),f.chunk('block-start',index=0,blockType='text'),
        f.chunk('text-delta',index=0,text='Partial'),f.end(outcome=outcome)]
    retry=Frames('session:2',f.revision)
    ops += [retry.start(),retry.chunk('block-start',index=0,blockType='text'),
        retry.chunk('text-delta',index=0,text='Partial'),retry.end([dict(type='text',text='Partial')])]
    provider=adapter();normalized=[event for notification in run(ops)['output'] for event in provider.event(notification)]
    completed=[data for kind,data in normalized if kind=='message.completed']
    assert [d['interrupted'] for d in completed]==[True,False]
    assert completed[0]['text']=='Partial' and completed[0]['item_id']!=completed[1]['item_id']


def test_cancel_filters_empty_and_tool_blocks_without_wrong_mapping_or_hidden_output():
    f=Frames();ops=[dict(prompt='session'),f.start(),f.chunk('block-start',index=0,blockType='text'),
        f.chunk('text-delta',index=0,text=' '),f.chunk('block-start',index=1,blockType='tool-call'),
        f.chunk('tool-call-delta',index=1,arguments='PRIVATE_TOOL'),
        f.chunk('block-start',index=2,blockType='reasoning'),f.chunk('reasoning-delta',index=2,text='Public reasoning'),
        f.chunk('block-start',index=3,blockType='text'),f.chunk('text-delta',index=3,text='Partial answer'),
        f.end([dict(type='reasoning',text='Public reasoning'),dict(type='text',text='Partial answer')],interrupted=True)]
    output=run(ops)['output'];provider=adapter()
    completed=[data for notification in output for kind,data in provider.event(notification) if kind=='message.completed']
    assert {(d['item_id'].rsplit(':',1)[-1],d['text']) for d in completed}=={('0',' '),('2','Public reasoning'),('3','Partial answer')}
    assert all(d['interrupted'] for d in completed) and 'PRIVATE' not in json.dumps(output)


def test_exact_prompt_agent_session_binding_ignores_other_agents_and_replacements():
    a=Frames();b=Frames('foreign:1');c=Frames('helper:1')
    ops=[dict(frame=a.start()['frame']),dict(prompt='session'),
        dict(agent='foreign',frame=b.start()['frame']),dict(agent='helper',frame=c.start()['frame']),
        dict(agent='replacement',frame=a.start()['frame']),a.start(),
        dict(registry='replacement'),a.chunk('block-start',index=0,blockType='text')]
    result=run(ops)
    assert [o['params']['phase'] for o in result['output']]==['start']
    assert result['cancellations']==[]


@pytest.mark.parametrize('cause',['block_bytes','stdout_backpressure','input_bytes','bad_sequence'])
def test_resource_and_protocol_failures_cancel_once_without_an_unbounded_queue(cause):
    f=Frames();ops=[dict(prompt='session'),f.start(),f.chunk('block-start',index=0,blockType='text')]
    if cause=='block_bytes':ops.append(f.chunk('text-delta',index=0,text='x'*(128*1024+1)))
    elif cause=='stdout_backpressure':ops += [dict(pending=2*1024*1024),f.chunk('text-delta',index=0,text='x')]
    elif cause=='input_bytes':ops.append(dict(rawInput='x'*(1024*1024+1)))
    else:
        broken=f.chunk('text-delta',index=0,text='x');broken['frame']['index']=55;ops.append(broken)
    ops += [f.chunk('text-delta',index=0,text='ignored')]*10
    result=run(ops)
    assert len(result['cancellations'])==1
    assert [o['params']['phase'] for o in result['output']]==['start','error']
    provider=adapter();provider.event(result['output'][0])
    with pytest.raises(ForgeError,match='boundary'):provider.event(result['output'][1])


def test_only_normal_acp_profile_gets_the_fixed_observer(tmp_path):
    config=dict(executable='/usr/bin/true',version='0.2.1-rc.2',model='fixture')
    for mode in ('normal','_compaction','_review','_model_broker'):
        home=tmp_path/mode
        DeepSeek.launch({**config,**({mode:True} if mode!='normal' else {})},home,'/project')
        overlay=next(home.glob('dsh/profiles/*/cordis.patch.yml')).read_text()
        assert ('deepseek_stream.cjs' in overlay)==(mode=='normal')
        if mode=='normal':
            assert next(row for row in json.loads(overlay) if row.get('id')=='acp')['inject']==[
                'acpAppStartup','demiflowAssistantStreamReady']


def transport_source(tmp_path, frames, limits):
    path=tmp_path/'wire.jsonl'
    path.write_text(''.join(json.dumps(frame,separators=(',',':'))+'\n' for frame in frames))
    process=subprocess.Popen([sys.executable,'-c',
        'import pathlib,sys,time;sys.stdout.buffer.write(pathlib.Path(sys.argv[1]).read_bytes());sys.stdout.flush();time.sleep(10)',
        str(path)],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    return process,Transport(process,limits)


def test_real_transport_native_64_block_burst_preserves_all_text_and_rpc_response(tmp_path):
    f=Frames();ops=[dict(prompt='session'),f.start()]
    content=[]
    for index in range(64):
        content.append(dict(type='text',text='corrected'))
        ops += [f.chunk('block-start',index=index,blockType='text'),f.chunk('text-delta',index=index,text='prefix'),
                f.chunk('block-end',index=index,block=dict(type='text',text='corrected'))]
    ops.append(f.end(content))
    native=run(ops)['output']
    assert len(native)>128
    legacy=[dict(jsonrpc='2.0',method='session/update',params=dict(sessionId='session',update=dict(
        sessionUpdate='agent_message_chunk',messageId='message:session:1',content=dict(type='text',text='corrected')))) for _ in range(64)]
    frames=native+legacy+[dict(jsonrpc='2.0',id='client-1',result=dict(stopReason='end_turn'))]
    process,transport=transport_source(tmp_path,frames,Limits())
    transport.pending.add('client-1')
    provider=adapter();received=[];peak=0
    try:
        deadline=time.monotonic()+5
        while 'client-1' not in transport.responses and time.monotonic()<deadline:
            transport.poll(.05)
            peak=max(peak,len(transport.events))
            assert len(transport.events)<=128 and len(transport.buffer)<=transport.limits.frame_bytes+1
            assert transport.queued_bytes<=2*transport.limits.frame_bytes
            while transport.events:received.extend(provider.event(transport.pop()))
        assert transport.take('client-1')==dict(stopReason='end_turn')
        complete=[data for kind,data in received if kind=='message.completed' and data['status']=='complete']
        assert len(complete)==64 and len({d['item_id'] for d in complete})==64
        assert all(d['text']=='corrected' for d in complete)
        assert peak>0 and transport.frame_count==len(frames)
    finally:
        process.kill();process.wait();transport.close()


def test_real_transport_byte_backpressure_and_pre_ack_wait_remain_bounded(tmp_path):
    frames=[dict(method='fixture/text',params=dict(index=i,text='x'*700)) for i in range(30)]
    frames.append(dict(id='client-1',result=dict(ok=True)))
    process,transport=transport_source(tmp_path,frames,Limits(frame_bytes=1024))
    transport.pending.add('client-1')
    try:
        deadline=time.monotonic()+2
        while transport.stdout_registered and time.monotonic()<deadline:transport.poll(.02)
        assert not transport.stdout_registered and transport.queued_bytes<=2048 and len(transport.buffer)<=1025
        before=(transport.output_bytes,transport.frame_count,len(transport.events),bytes(transport.buffer))
        # This is what pre-ack wait() does: retain notifications, poll boundedly,
        # and eventually let its existing acknowledgement deadline terminate.
        for _ in range(3):transport.poll(.01)
        assert before==(transport.output_bytes,transport.frame_count,len(transport.events),bytes(transport.buffer))
        values=[];deadline=time.monotonic()+3
        while 'client-1' not in transport.responses and time.monotonic()<deadline:
            while transport.events:values.append(transport.pop()['params']['index'])
            transport.poll(.02)
            assert transport.queued_bytes<=2048 and len(transport.buffer)<=1025
        while transport.events:values.append(transport.pop()['params']['index'])
        assert values==list(range(30)) and transport.take('client-1')==dict(ok=True)
    finally:
        process.kill();process.wait();transport.close()


def test_transport_drains_buffered_frames_and_final_rpc_before_reporting_eof(tmp_path):
    frames=[dict(method='notice',params=dict(index=i)) for i in range(300)]
    frames.append(dict(id='client-1',result=dict(done=True)))
    path=tmp_path/'finished-wire.jsonl'
    path.write_text(''.join(json.dumps(frame)+'\n' for frame in frames))
    process=subprocess.Popen([sys.executable,'-c',
        'import pathlib,sys;sys.stdout.buffer.write(pathlib.Path(sys.argv[1]).read_bytes());sys.stdout.flush()',str(path)],
        stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    transport=Transport(process,Limits());transport.pending.add('client-1')
    try:
        process.wait(timeout=2)
        received=[];deadline=time.monotonic()+3
        while not transport.closed and time.monotonic()<deadline:
            transport.poll(.02)
            assert len(transport.events)<=128
            if transport.buffer:assert not transport.closed
            while transport.events:received.append(transport.pop()['params']['index'])
        assert transport.closed and received==list(range(300))
        assert transport.take('client-1')==dict(done=True) and not transport.buffer
    finally:
        if process.poll() is None:process.kill()
        process.wait();transport.close()


@pytest.mark.parametrize('limits,frames,reason',[
    (Limits(frame_bytes=1024),[dict(method='wide',params=dict(text='x'*1100))],'frame'),
    (Limits(output_bytes=1000),[dict(method='many',params=dict(text='x'*100)) for _ in range(30)],'total stdout'),
    (Limits(events_per_turn=5),[dict(method='many',params={}) for _ in range(10)],'frame budget'),
])
def test_transport_backpressure_does_not_relax_original_limits(tmp_path,limits,frames,reason):
    process,transport=transport_source(tmp_path,frames,limits)
    try:
        with pytest.raises(ForgeError,match=reason):
            deadline=time.monotonic()+3
            while time.monotonic()<deadline:
                transport.poll(.02)
                while transport.events:transport.pop()
    finally:
        process.kill();process.wait();transport.close()
