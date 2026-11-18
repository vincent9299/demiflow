/* Fixed model-only transport inside the external execution boundary.
 * The CLI retains its agent/tool loop. No upstream credential or host address
 * is available here; the one task endpoint is supplied by the launcher. */
'use strict';
const net = require('node:net');
const {pathToFileURL} = require('node:url');
exports.name = 'demiflow-model-proxy';
exports.inject = ['llm'];
exports.apply = async function(ctx, config) {
  const {LlmAdapter} = await import(pathToFileURL(config.llmModule).href);
  const maximum = 1024 * 1024;
  async function* exchange(method, options, signal) {
    const endpoint = process.env.DEMIFLOW_MODEL_ENDPOINT;
    if (!/^127\.0\.0\.1:[0-9]{1,5}$/.test(endpoint || '')) throw new Error('Fixed model channel is unavailable');
    const port = Number(endpoint.split(':')[1]);
    const payload = {token:config.token, method, options};
    const raw = JSON.stringify(payload)+'\n';
    if (Buffer.byteLength(raw)>maximum) throw new Error('Model request exceeds its byte budget');
    if (signal?.aborted) throw new Error('Model request cancelled');
    const socket = net.createConnection({host:'127.0.0.1',port});
    const abort = () => socket.destroy(new Error('Model request cancelled'));
    signal?.addEventListener('abort',abort,{once:true});
    socket.setTimeout(180000,()=>socket.destroy(new Error('Model channel timed out')));
    let buffer=Buffer.alloc(0),finished=false;
    try {
      socket.write(raw);
      for await (const chunk of socket) {
        if(buffer.length+chunk.length>maximum) throw new Error('Model response frame exceeds budget');
        buffer=Buffer.concat([buffer,chunk]);
        let end;
        while((end=buffer.indexOf(10))>=0) {
          const value=JSON.parse(buffer.subarray(0,end).toString('utf8'));buffer=buffer.subarray(end+1);
          if(value.error) throw new Error(String(value.error).slice(0,256));
          if(value.done===true) {finished=true;return;}
          if(!Object.hasOwn(value,'value')) throw new Error('Invalid model response');
          yield value.value;
        }
      }
      if(!finished) throw new Error('Model channel closed without a completion receipt');
    } finally {signal?.removeEventListener('abort',abort);socket.destroy();}
  }
  class FixedAdapter extends LlmAdapter {
    async resolveModel(provider,model,signal) {
      if(provider!==config.provider||model!==config.model) throw new Error('Model route is fixed for this task');
      let result;
      for await(const value of exchange('resolve',{provider,model},signal)) {
        if(result!==undefined) throw new Error('Duplicate model metadata');result=value;
      }
      if(!result || result.provider!==provider || result.id!==model) throw new Error('Model metadata identity differs');
      return result;
    }
    async listModels(provider) {return [await this.resolveModel(provider,config.model)];}
    async *stream(options) {
      if(options.provider!==config.provider||options.model!==config.model) throw new Error('Model route is fixed for this task');
      const plain={};
      for(const name of ['provider','model','reasoningEffort','messages','system','tools','toolHistory','temperature','maxTokens','stop','sessionId','purpose'])
        if(options[name]!==undefined) plain[name]=options[name];
      yield* exchange('stream',plain,options.signal);
    }
  }
  ctx.effect(()=>ctx.llm.registerAdapter([config.provider],new FixedAdapter()));
};
