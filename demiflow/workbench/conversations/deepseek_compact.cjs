/* Native maintenance endpoint for the pinned Harness. Runs inside its CLI and
 * private session home; it does not implement a model loop or delete history. */
'use strict';
const {realpath} = require('node:fs/promises');
exports.name = 'demiflow-compaction';
exports.inject = ['acpAppStartup','agents','llm','sessionPersistence','sessions','compaction'];
exports.apply = function(ctx, config) {
  let handle, sessionId, busy=false, operation, buffer='', closed=false;
  const send = value => { if(!closed) process.stdout.write(JSON.stringify({jsonrpc:'2.0',...value})+'\n'); };
  function own(params) { if(!handle || params.sessionId!==sessionId) throw new Error('Session identity mismatch'); }
  async function dispatch(method, params) {
    if(method==='initialize') return {protocolVersion:1,agentInfo:{name:'demiflow-deepseek-compaction',version:'1'}};
    if(method==='session/resume') {
      if(handle || typeof params.sessionId!=='string' || params.sessionId.length>128) throw new Error('Invalid resume');
      if(await realpath(params.cwd)!==await realpath(config.cwd)) throw new Error('Workspace mismatch');
      const saved=await ctx.sessionPersistence.stat(params.sessionId,{signal:operation.signal});
      if(!saved?.header || saved.header.origin==='subagent' || saved.header.parentSession!==undefined ||
         await realpath(saved.header.cwd)!==await realpath(config.cwd)) throw new Error('Session is not resumable here');
      handle=await ctx.agents.resume({resumeSessionId:params.sessionId,
        agentOptions:{provider:config.provider,model:config.model},signal:operation.signal});
      sessionId=params.sessionId;
      return {sessionId,configOptions:[{id:'model',currentValue:JSON.stringify([config.provider,config.model])}]};
    }
    own(params);
    if(method==='session/compact') {
      const result=await ctx.compaction.compactNow(handle.agent,operation.signal);
      return result===null ? {status:'noop'} : {status:'compacted',compaction_id:result.compactionId,
        shadowed_items:result.shadowedSeqs.length,estimated_tokens_before:result.shadowedTokenCount,
        summary_sequence:result.summarySeq};
    }
    if(method==='session/close') { await handle.dispose();handle=null;return {}; }
    throw new Error('Unsupported maintenance method');
  }
  function line(raw) {
    let value;
    try {value=JSON.parse(raw);} catch {send({id:null,error:{code:-32700,message:'Invalid JSON'}});return;}
    if(value.method==='session/cancel') {
      if(value.params?.sessionId===sessionId) operation?.abort();
      return;
    }
    if(!Object.hasOwn(value,'id')) return;
    if(busy) {send({id:value.id,error:{code:-32000,message:'Maintenance request in progress'}});return;}
    busy=true;operation=new AbortController();
    dispatch(value.method,value.params||{}).then(result=>send({id:value.id,result}),
      error=>send({id:value.id,error:{code:-32603,message:String(error.message).slice(0,1024)}})).finally(()=>{busy=false;});
  }
  const data = chunk => {
    // One small control frame at a time. Large or pipelined inputs are rejected
    // before JSON decoding; the owning Python worker bounds stdout and lifetime.
    if(buffer.length+chunk.length>16384) {operation?.abort();process.stdin.pause();send({id:null,error:{code:-32600,message:'Control frame budget exceeded'}});return;}
    buffer+=chunk;
    let end;
    while((end=buffer.indexOf('\n'))>=0) {const raw=buffer.slice(0,end);buffer=buffer.slice(end+1);if(raw.trim())line(raw);}
  };
  process.stdin.setEncoding('utf8');process.stdin.on('data',data);
  ctx.effect(()=>()=>{closed=true;operation?.abort();process.stdin.off('data',data);handle?.dispose();});
};
