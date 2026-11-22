/* Public native Web stream -> bounded ACP-side notifications. No model loop,
 * raw provider frames, replay metadata, new service or tool is exposed. */
'use strict';
const {StringDecoder} = require('node:string_decoder');
exports.name = 'demiflow-assistant-stream';
exports.inject = ['acpAppStartup', 'agents'];
exports.apply = function(ctx, config) {
  const FRAME=1024*1024, TOTAL=16*1024*1024, BLOCK=128*1024, ATTEMPT=1024*1024;
  let input='', decoder=new StringDecoder('utf8'), agent, session, current, timer;
  let closed=false, failed=false, sent=0, frames=0, attempts=0, revision=0;
  let metricStep, metricSequence=-1;
  const calls=new Map();
  const identity=value=>typeof value==='string' && value.length>0 && Buffer.byteLength(value)<=256;
  const validIndex=value=>Number.isSafeInteger(value) && value>=0 && value<65536;
  const count=value=>Number.isSafeInteger(value) && value>=0;
  const time=value=>typeof value==='number' && Number.isFinite(value) && value>=0 && value<=Number.MAX_SAFE_INTEGER;
  function write(value,method='demiflow/assistant-stream') {
    const raw=JSON.stringify({jsonrpc:'2.0',method,params:{sessionId:session.id,...value}})+'\n';
    const size=Buffer.byteLength(raw);
    if(size>FRAME || process.stdout.writableLength+size>2*FRAME)
      throw new Error('stream budget');
    sent+=size;
    frames++;
    process.stdout.write(raw);
  }
  function metric(event,kind,turn,fields={}) {
    if(!count(event.seq) || !count(turn)) return;
    write({metrics:{format:'demiflow_native_metrics_v1',kind,event_sequence:event.seq,native_turn:turn,...fields}},
      'demiflow/metrics');
  }
  function usage(value) {
    if(!value || typeof value!=='object' || !count(value.inputTokens) || !count(value.outputTokens)) return null;
    const result={inputTokens:value.inputTokens,outputTokens:value.outputTokens};
    for(const key of ['totalTokens','cacheReadTokens','cacheWriteTokens']) {
      if(value[key]!==undefined) {if(!count(value[key])) return null;result[key]=value[key];}
    }
    return result;
  }
  function modelMetric(event) {
    const data=event.data;
    if(!count(data?.turn) || !count(data.step) || data.turn!==current.turn || data.step!==current.step) return;
    const timing={};
    if(metricStep?.turn===data.turn && metricStep.step===data.step && time(event.time)) {
      timing.llm_ms=Math.max(0,event.time-metricStep.start);
      if(metricStep.first!==null) {
        timing.ttft_ms=Math.max(0,metricStep.first-metricStep.start);
        timing.decode_ms=Math.max(0,event.time-metricStep.first);
      }
      // Match native sessionStats: an assembled message consumes step timing.
      metricStep=undefined;
    }
    metric(event,'model',data.turn,{native_step:data.step,attempt_id:current.id,
      message_id:data.message.id,usage:data.usage===undefined?current.usage??null:usage(data.usage),timing});
  }
  function sessionEvent(source,event) {
    if(closed || failed || !agent || source!==session || agent.session!==session || ctx.agents.get(session.id)!==agent) return;
    if(!['step/start','step/end','llm/retry-started','tool/call','tool/result','turn/end'].includes(event?.type)) return;
    try {
      if(!count(event.seq) || event.seq<=metricSequence) return;
      metricSequence=event.seq;
      const data=event.data;
      if(event.type==='step/start' && count(data?.turn) && count(data.step)) {
        metricStep=time(event.time)?{turn:data.turn,step:data.step,start:event.time,first:null}:undefined;
        metric(event,'step_started',data.turn,{native_step:data.step});
      } else if(event.type==='step/end' && count(data?.turn) && count(data.step)) {
        metric(event,'step_ended',data.turn,{native_step:data.step});metricStep=undefined;
      } else if(event.type==='llm/retry-started' && count(data?.turn) && count(data.step)) {
        metric(event,'retry_started',data.turn,{native_step:data.step});
      } else if(event.type==='tool/call' && identity(data?.callId) && count(data.turn) && time(event.time)) {
        if(calls.size>=512 && !calls.has(data.callId)) calls.delete(calls.keys().next().value);
        calls.set(data.callId,{turn:data.turn,time:event.time});
      } else if(event.type==='tool/result') {
        const id=data?.message?.source?.callId, call=calls.get(id);
        if(call) {
          calls.delete(id);
          if(time(event.time)) metric(event,'tool',call.turn,{call_id:id,duration_ms:Math.max(0,event.time-call.time)});
        }
      } else if(event.type==='turn/end' && count(data?.turn)) {
        metric(event,'turn_ended',data.turn);metricStep=undefined;calls.clear();
      }
    } catch {fail();}
  }
  function fail() {
    if(failed || closed) return;
    failed=true;clearTimeout(timer);timer=undefined;current=undefined;
    // One small terminal notification, no deferred Promise/write queue. The
    // host drains/cancels the owned CLI upon this resource/protocol failure.
    if(session && process.stdout.writableLength<2*FRAME+1024)
      process.stdout.write(JSON.stringify({jsonrpc:'2.0',method:'demiflow/assistant-stream',
        params:{sessionId:session.id,phase:'error',code:'stream_boundary'}})+'\n');
    agent?.cancel({kind:'hook',reason:'Public stream boundary failed'});
  }
  function flush() {
    clearTimeout(timer);timer=undefined;
    if(!current || closed || failed) return;
    for(const [index,block] of current.blocks) {
      if(block.pending) {
        write({phase:'delta',attemptId:current.id,blockIndex:index,channel:block.channel,text:block.pending});
        block.pending='';block.published=true;
      }
    }
  }
  function schedule() {
    if(!timer) timer=setTimeout(()=>{try{flush();}catch{fail();}},16);
  }
  function complete(index,block,status='complete') {
    write({phase:'completed',attemptId:current.id,blockIndex:index,channel:block.channel,
      text:block.text,status,interrupted:status==='incomplete'});
  }
  function assign(block,value) {
    if(typeof value!=='string' || Buffer.byteLength(value)>BLOCK) throw new Error('public text budget');
    const bytes=Buffer.byteLength(value);
    current.bytes+=bytes-block.bytes;
    if(current.bytes>ATTEMPT) throw new Error('attempt budget');
    block.text=value;block.bytes=bytes;
  }
  function receive({agent:source,frame}) {
    if(closed || failed || !agent || source!==agent || source.session!==session ||
        ctx.agents.get(session.id)!==agent) return;
    try {
      if(!frame || !identity(frame.attemptId) || !Number.isSafeInteger(frame.revision) || frame.revision<=revision)
        throw new Error('invalid frame identity');
      revision=frame.revision;
      if(frame.type==='start') {
        if(current) throw new Error('attempt boundary');
        ++attempts;
        current={id:frame.attemptId,turn:frame.turn,step:frame.step,index:0,blocks:new Map(),bytes:0};
        write({phase:'start',attemptId:current.id});return;
      }
      if(!current || frame.attemptId!==current.id || frame.index!==current.index)
        throw new Error('attempt sequence');
      if(frame.type==='chunk') {
        current.index++;
        if(current.index>65536) throw new Error('chunk budget');
        const chunk=frame.chunk;
        if(chunk?.type==='usage') current.usage=usage(chunk.usage);
        // Native isTokenDelta's timestamp rule, without retaining or exporting
        // tool arguments, replay metadata, or private model content.
        if(metricStep && metricStep.turn===current.turn && metricStep.step===current.step && metricStep.first===null && time(frame.time) &&
          ((['text-delta','reasoning-delta'].includes(chunk?.type) && typeof chunk.text==='string' && chunk.text!=='') ||
           (chunk?.type==='tool-call-delta' && (typeof chunk.argumentsDelta==='string' && chunk.argumentsDelta!=='' || chunk.name!==undefined))))
          metricStep.first=frame.time;
        if(chunk?.type==='block-start' && ['text','reasoning'].includes(chunk.blockType)) {
          if(!validIndex(chunk.index) || current.blocks.has(chunk.index) || current.blocks.size>=64)
            throw new Error('block identity');
          current.blocks.set(chunk.index,{channel:chunk.blockType==='text'?'answer':'process',
            type:chunk.blockType,text:'',bytes:0,pending:'',published:false,ended:false});
        } else if(chunk?.type==='text-delta' || chunk?.type==='reasoning-delta') {
          const block=current.blocks.get(chunk.index);
          if(!block || block.ended || block.type!==(chunk.type==='text-delta'?'text':'reasoning') ||
              typeof chunk.text!=='string' || Buffer.byteLength(chunk.text)>BLOCK) throw new Error('public block delta');
          assign(block,block.text+chunk.text);block.pending+=chunk.text;
          // First actual text is delivered immediately; subsequent tokens share
          // one short timer. There is no simulated typing or unbounded queue.
          if(!block.published || Buffer.byteLength(block.pending)>=16384) flush();else schedule();
        } else if(chunk?.type==='block-end') {
          const block=current.blocks.get(chunk.index);
          if(block) {
            if(block.ended || chunk.block?.type!==block.type) throw new Error('public block end');
            flush();assign(block,chunk.block.text);block.ended=true;complete(chunk.index,block,'streaming');
          }
        }
        // All other chunk fields/types (including finish/replay/source/tool
        // arguments) stay on their existing native/ACP paths. Usage retains
        // only the whitelisted numeric billing buckets above.
        return;
      }
      if(frame.type!=='end') throw new Error('unknown stream phase');
      flush();
      const outcome=frame.outcome;
      let messageId, committed, incomplete=true;
      if(outcome?.kind==='committed' && outcome.eventType==='assistant/message') {
        if(!Number.isSafeInteger(outcome.seq) || outcome.seq<0) throw new Error('settlement sequence');
        const events=session.snapshotEvents(outcome.seq,outcome.seq+1);
        const event=events[0];
        if(events.length!==1 || event.seq!==outcome.seq || event.type!=='assistant/message' ||
            !identity(event.data?.message?.id) || !Array.isArray(event.data.message.content)) throw new Error('settlement identity');
        messageId=event.data.message.id;incomplete=event.data.interrupted===true;
        committed=event;
        const publicBlocks=event.data.message.content.filter(block=>block?.type==='text' || block?.type==='reasoning');
        // Cancellation removes whitespace/tool blocks. Match ordered public
        // block identities, never matching repeated text or string prefixes.
        const visible=[...current.blocks].filter(([,b])=>!incomplete || b.text.trim()!=='');
        if(publicBlocks.length!==visible.length) throw new Error('settlement blocks');
        for(let i=0;i<visible.length;i++) {
          const [index,block]=visible[i], canonical=publicBlocks[i];
          if(canonical.type!==block.type) throw new Error('settlement block type');
          assign(block,canonical.text);complete(index,block,incomplete?'incomplete':'complete');
        }
        for(const [index,block] of current.blocks)
          if(incomplete && !block.text.trim()) complete(index,block,'incomplete');
      } else if(outcome?.kind==='abandoned' || outcome?.kind==='committed' && outcome.eventType==='assistant/attempt') {
        for(const [index,block] of current.blocks) complete(index,block,'incomplete');
      } else throw new Error('unknown settlement');
      write({phase:'settled',attemptId:current.id,...(messageId===undefined?{}:{messageId}),
        status:incomplete?'incomplete':'complete'});
      if(committed) modelMetric(committed);
      else if(outcome?.kind==='committed' && outcome.eventType==='assistant/attempt' && count(outcome.seq) && count(current.turn) && count(current.step))
        metric({seq:outcome.seq},'attempt',current.turn,{native_step:current.step,attempt_id:current.id,usage:current.usage??null});
      current=undefined;
    } catch {fail();}
  }
  // ACP itself owns parsing/dispatch. Observing only a prompt's session identity
  // binds this plugin to that exact registry Agent and Session before streaming.
  // Do not setEncoding: the official ACP reader expects Buffer chunks.
  const data=chunk=>{
    if(closed || failed) return;
    try {
      input+=typeof chunk==='string'?chunk:decoder.write(chunk);
      if(Buffer.byteLength(input)>FRAME) throw new Error('input frame budget');
      let end;
      while((end=input.indexOf('\n'))>=0) {
        const raw=input.slice(0,end);input=input.slice(end+1);
        if(!raw.trim()) continue;
        const value=JSON.parse(raw);
        if(value.method!=='session/prompt') continue;
        const id=value.params?.sessionId;
        if(!identity(id)) throw new Error('prompt identity');
        const target=ctx.agents.get(id), bound=target?.session;
        if(!target || target.id!==id || bound?.id!==id || bound.header?.cwd!==config.cwd ||
            bound.header?.origin==='subagent' || bound.header?.parentSession!==undefined ||
            agent && (agent!==target || session!==bound)) throw new Error('prompt root identity');
        if(agent && !current) {sent=0;frames=0;attempts=0;}
        agent=target;session=bound;
      }
    } catch {fail();}
  };
  process.stdin.on('data',data);
  ctx.on('agent/assistant-stream',receive,{global:true});
  ctx.on('session/event',sessionEvent,{global:true});
  ctx.effect(()=>()=>{closed=true;clearTimeout(timer);current=undefined;metricStep=undefined;calls.clear();input='';process.stdin.off('data',data);});
  // The ACP profile waits for this fixed readiness service before owning stdio,
  // so even its first prompt cannot race installation of the observer.
  ctx.provide('demiflowAssistantStreamReady',{ready:true});
};
