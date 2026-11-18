/* Trusted one-shot service. It never creates an agent, session, or tool loop. */
'use strict';
exports.name = 'demiflow-review';
exports.inject = ['acpAppStartup', 'llm'];
exports.apply = function(ctx, config) {
  let initialized=false, used=false, busy=false, closed=false, buffer='', operation;
  const maximum=65536, inputMaximum=600*1024;
  if (!config.provider || !config.model || config.maxOutputBytes!==maximum ||
      !Number.isInteger(config.timeoutMs) || config.timeoutMs<1 || config.timeoutMs>180000)
    throw new Error('Invalid fixed review configuration');
  const send = value => {if(!closed) process.stdout.write(JSON.stringify({jsonrpc:'2.0',...value})+'\n');};
  async function dispatch(method, params) {
    if (method==='initialize') {
      if(initialized) throw new Error('Already initialized');
      initialized=true;
      return {protocolVersion:1,agentInfo:{name:'demiflow-deepseek-review',version:'1'},
        provider:config.provider,model:config.model,mode:'no-tools',singleRequest:true};
    }
    if(method!=='review/generate' || !initialized || used) throw new Error('Unsupported review operation');
    if(typeof params.prompt!=='string' || !params.prompt.trim() || Buffer.byteLength(params.prompt)>inputMaximum ||
       typeof params.request_id!=='string' || !/^[a-zA-Z0-9_-]{1,80}$/.test(params.request_id) ||
       typeof params.input_digest!=='string' || !/^[0-9a-f]{64}$/.test(params.input_digest))
      throw new Error('Invalid bounded review input');
    used=true;operation=new AbortController();
    const timer=setTimeout(()=>operation.abort(),config.timeoutMs);
    let text='', bytes=0, observed=0, finish=false;
    const blocks=new Map();
    try {
      // The profile disables llm-retry. One native model request, tools empty,
      // no developer-session identity and no model-triggered execution surface.
      for await (const chunk of ctx.llm.stream({provider:config.provider,model:config.model,
        reasoningEffort:config.reasoningEffort,tools:[],maxTokens:16384,
        messages:[{role:'user',content:[{type:'text',text:params.prompt}]}],signal:operation.signal})) {
        if(finish || ++observed>32768) throw new Error('Invalid or excessive model stream');
        if(chunk.type==='block-start') {
          if(!['text','reasoning'].includes(chunk.blockType) || blocks.has(chunk.index) || blocks.size>=64)
            throw new Error('Reviewer attempted a tool or unsupported block');
          blocks.set(chunk.index,{type:chunk.blockType,text:'',ended:false});
        } else if(chunk.type==='text-delta' || chunk.type==='reasoning-delta') {
          const block=blocks.get(chunk.index);
          if(!block || block.ended || block.type!==(chunk.type==='text-delta'?'text':'reasoning') || typeof chunk.text!=='string')
            throw new Error('Invalid model block');
          bytes+=Buffer.byteLength(chunk.text);
          if(bytes>256*1024) throw new Error('Model stream byte budget exceeded');
          if(block.type==='text') {
            block.text+=chunk.text;text+=chunk.text;
            if(Buffer.byteLength(text)>maximum) throw new Error('Review output budget exceeded');
          }
        } else if(chunk.type==='block-end') {
          const block=blocks.get(chunk.index);
          if(!block || block.ended || chunk.block?.type!==block.type ||
             (block.type==='text' && chunk.block.text!==block.text)) throw new Error('Invalid completed model block');
          block.ended=true;
        } else if(chunk.type==='usage') {
          // Usage is deliberately not part of the review verdict or receipt.
        } else if(chunk.type==='finish') {
          if(chunk.reason?.kind==='max-tokens' && [...blocks.values()].every(block=>block.ended))
            throw Object.assign(new Error('Independent review output token budget exhausted'),{reviewFailure:'max-tokens'});
          if(chunk.reason?.kind!=='stop' || [...blocks.values()].some(block=>!block.ended))
            throw new Error('Review did not finish normally');
          finish=true;
        } else throw new Error('Reviewer attempted tools or unsupported stream output');
      }
      if(!finish || !text.trim() || operation.signal.aborted) throw new Error('Review completion was not confirmed');
      return {request_id:params.request_id,input_digest:params.input_digest,provider:config.provider,
        model:config.model,mode:'no-tools',finish:'stop',text};
    } finally {clearTimeout(timer);operation.abort();}
  }
  function line(raw) {
    let value;
    try {value=JSON.parse(raw);} catch {send({id:null,error:{code:-32700,message:'Invalid JSON'}});return;}
    if(!Object.hasOwn(value,'id')) return;
    if(busy) {send({id:value.id,error:{code:-32000,message:'Review request in progress'}});return;}
    busy=true;
    dispatch(value.method,value.params||{}).then(result=>send({id:value.id,result}),
      error=>send({id:value.id,error:error?.reviewFailure==='max-tokens'
        ? {code:-32001,message:'Independent review output token budget exhausted'}
        : {code:-32603,message:'Independent review did not complete safely'}}))
      .finally(()=>{busy=false;});
  }
  const data=chunk=>{
    if(Buffer.byteLength(buffer)+Buffer.byteLength(chunk)>inputMaximum+4096) {
      operation?.abort();process.stdin.pause();send({id:null,error:{code:-32600,message:'Review input budget exceeded'}});return;
    }
    buffer+=chunk;let end;
    while((end=buffer.indexOf('\n'))>=0) {const raw=buffer.slice(0,end);buffer=buffer.slice(end+1);if(raw.trim())line(raw);}
  };
  process.stdin.setEncoding('utf8');process.stdin.on('data',data);
  ctx.effect(()=>()=>{closed=true;operation?.abort();process.stdin.off('data',data);});
};
