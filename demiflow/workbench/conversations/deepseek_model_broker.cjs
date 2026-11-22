/* Trusted fixed-provider LLM service. This process never creates an agent,
 * session or tool executor. Tools in GenerateOptions are schemas sent to the
 * model; the developer CLI, in its separate sandbox, owns their execution. */
'use strict';
const {once}=require('node:events');
exports.name='demiflow-model-broker';
exports.inject=['acpAppStartup','llm'];
exports.apply=function(ctx,config) {
  let active=null,busy=false,buffer='',closing=false;
  const maximum=4*1024*1024;
  function safeError(err) {
    const status=Number(err?.status??err?.statusCode);
    if(Number.isInteger(status)&&status>=400&&status<600)return 'Upstream model HTTP '+status;
    const code=typeof err?.code==='string'&&/^[A-Za-z0-9_.-]{1,40}$/.test(err.code)?err.code:'';
    if(code)return 'Upstream model error ('+code+')';
    const name=typeof err?.name==='string'&&/^[A-Za-z0-9_]{1,40}$/.test(err.name)?err.name:'';
    if(name==='AbortError')return 'Model request timed out or aborted';
    const msg=typeof err?.message==='string'?err.message:'';
    if(/^(Model |Unsupported |Native )/.test(msg))return msg.slice(0,160);
    return 'Configured model request failed'+(name?' ('+name+')':'');
  }
  async function send(value) {
    if(closing)return;
    const raw=JSON.stringify({jsonrpc:'2.0',...value})+'\n';
    if(Buffer.byteLength(raw)>maximum)throw new Error('Model response frame exceeds budget');
    if(!process.stdout.write(raw))await once(process.stdout,'drain');
  }
  async function dispatch(id,method,params) {
    if(method==='initialize')return {protocol:'demiflow-model-channel-v1',provider:config.provider,model:config.model};
    if(params.provider!==config.provider||params.model!==config.model)throw new Error('Model route differs');
    if(method==='model/resolve')return ctx.llm.resolveModelInfo(config.provider,config.model,active.signal);
    if(method!=='model/stream')throw new Error('Unsupported model method');
    const options={...params,provider:config.provider,model:config.model,
      reasoningEffort:config.reasoningEffort,maxTokens:Math.min(params.maxTokens||16384,16384),signal:active.signal};
    let finished=false,pending=null,lastSent=0;
    async function flushPending() {
      if(!pending)return;
      const chunk=pending;pending=null;lastSent=Date.now();
      await send({method:'model/chunk',params:{requestId:id,chunk}});
    }
    for await(const chunk of ctx.llm.stream(options)) {
      if(finished)throw new Error('Model emitted data after completion');
      const delta=(chunk?.type==='text-delta'||chunk?.type==='reasoning-delta')&&typeof chunk.text==='string';
      if(delta) {
        if(pending&&pending.type===chunk.type&&pending.index===chunk.index&&pending.text.length+chunk.text.length<=4096) {
          pending.text+=chunk.text;
          if(Date.now()-lastSent>=16||pending.text.length>=512) await flushPending();
          continue;
        }
        await flushPending();
        if(lastSent===0) {
          lastSent=Date.now();
          await send({method:'model/chunk',params:{requestId:id,chunk}});
        } else {
          pending={...chunk};
          if(Date.now()-lastSent>=16||pending.text.length>=512) await flushPending();
        }
        continue;
      }
      await flushPending();
      await send({method:'model/chunk',params:{requestId:id,chunk}});
      if(chunk.type==='finish')finished=true;
    }
    await flushPending();
    if(!finished)throw new Error('Model stream ended without completion');
    return {complete:true};
  }
  function line(raw) {
    let message;try{message=JSON.parse(raw);}catch{return;}
    if(message.method==='model/cancel'){active?.abort();return;}
    if(typeof message.id!=='string'||busy){closing=true;active?.abort();process.stdin.pause();return;}
    busy=true;active=new AbortController();
    const timer=setTimeout(()=>active?.abort(),180000);
    dispatch(message.id,message.method,message.params||{}).then(result=>send({id:message.id,result}),
      err=>send({id:message.id,error:{code:-32000,message:safeError(err)}}))
      .finally(()=>{clearTimeout(timer);active=null;busy=false;});
  }
  function data(chunk) {
    if(Buffer.byteLength(buffer)+Buffer.byteLength(chunk)>maximum){closing=true;active?.abort();process.stdin.pause();return;}
    buffer+=chunk;let end;
    while((end=buffer.indexOf('\n'))>=0){const raw=buffer.slice(0,end);buffer=buffer.slice(end+1);if(raw.trim())line(raw);}
  }
  process.stdin.setEncoding('utf8');process.stdin.on('data',data);
  process.stdin.on('end',()=>{closing=true;active?.abort();});
  ctx.effect(()=>()=>{closing=true;active?.abort();process.stdin.off('data',data);});
};
