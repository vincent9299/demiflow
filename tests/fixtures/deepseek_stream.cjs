/* Executes the production observer against native public event shapes. No LLM. */
'use strict';
const fs=require('node:fs'), vm=require('node:vm'), {EventEmitter}=require('node:events');
const request=JSON.parse(fs.readFileSync(0,'utf8'));
const stdin=new EventEmitter(), output=[], checkpoints=[], cleanups=[], handlers=new Map(), cancellations=[];
const make=(id,header={})=>({id,session:{id,header:{cwd:'/project',...header},events:new Map(),
  snapshotEvents(start,end){return [...this.events.values()].filter(e=>e.seq>=start&&e.seq<end);}},
  cancel(cause){cancellations.push({id,cause});}});
const agents={root:make('session'),foreign:make('foreign'),helper:make('helper',{origin:'subagent'}),replacement:make('session')};
const registry=new Map(Object.values(agents).filter(a=>a!==agents.replacement).map(a=>[a.id,a]));
const stdout={writableLength:0,write(raw){output.push(JSON.parse(raw));return true;}};
const context={agents:{get:id=>registry.get(id)},on(name,fn,options){handlers.set(name,fn);},
  provide(name,value){if(name!=='demiflowAssistantStreamReady'||!value.ready||stdin.listenerCount('data')!==1||
    !handlers.has('agent/assistant-stream'))throw new Error('Observer must be installed before ACP readiness');},
  effect(fn){cleanups.push(fn());}};
const plugin={exports:{}};
vm.runInNewContext(fs.readFileSync(process.argv[2],'utf8'),{
  exports:plugin.exports,module:plugin,require,Buffer,setTimeout,clearTimeout,
  process:{stdin,stdout},console
},{filename:process.argv[2]});
plugin.exports.apply(context,{cwd:'/project'});
(async()=>{
  for(const op of request.operations){
    if(op.prompt){
      const raw=Buffer.from(JSON.stringify({jsonrpc:'2.0',id:'client-3',method:'session/prompt',params:{sessionId:op.prompt,prompt:[]}})+'\n');
      stdin.emit('data',raw.subarray(0,17));stdin.emit('data',raw.subarray(17));
    }
    if(op.rawInput)stdin.emit('data',Buffer.from(op.rawInput));
    if(op.pending!==undefined)stdout.writableLength=op.pending;
    if(op.registry)registry.set('session',agents[op.registry]);
    if(op.frame){
      const agent=agents[op.agent||'root'];
      if(op.event)agent.session.events.set(op.event.seq,op.event);
      handlers.get('agent/assistant-stream')({agent,frame:op.frame});
    }
    if(op.wait)await new Promise(resolve=>setTimeout(resolve,op.wait));
    if(op.checkpoint)checkpoints.push({name:op.checkpoint,count:output.length});
  }
  for(const cleanup of cleanups)cleanup?.();
  process.stdout.write(JSON.stringify({output,checkpoints,cancellations,remainingListeners:stdin.listenerCount('data')}));
})().catch(error=>{console.error(error);process.exitCode=1;});
