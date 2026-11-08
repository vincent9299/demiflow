"""Actual JS admission/API code with controlled transport, no kernel/runtime."""
import json
from pathlib import Path
import shutil
import subprocess

import pytest


def test_bounded_queue_cancellation_priority_stop_and_read_only_busy_retries():
    node=shutil.which('node')
    if node is None:pytest.skip('Node required for actual JavaScript protocol check')
    web=Path(__file__).resolve().parents[1]/'demiflow/workbench/web'
    app=(web/'app.js').read_text();api=app[app.index('  async function api('):app.index('  function clearSelection()')]
    script=r'''
const assert=require('node:assert/strict'),vm=require('node:vm');
const tick=()=>new Promise(resolve=>setImmediate(resolve));
async function main(){
  const c={DOMException,AbortController,AbortSignal,Promise,setTimeout,clearTimeout,URLSearchParams,JSON,Date,TypeError,Error};
  c.globalThis=c;vm.createContext(c);vm.runInContext(TRANSPORT,c);
  const q=c.ForgeAnalysisTransport;let release,order=[],cancelled=false;
  const first=q.schedule(()=>new Promise(resolve=>release=resolve));await tick();
  const low=q.schedule(()=>order.push('background'),{priority:0});
  const current=q.schedule(()=>order.push('selection'),{priority:1});
  const cleanup=q.schedule(()=>order.push('cleanup'),{priority:2});
  const abort=new AbortController(),old=q.schedule(()=>{cancelled=true;},{signal:abort.signal});
  const rejected=assert.rejects(old,error=>error.name==='AbortError');abort.abort();await rejected;
  release();await Promise.all([first,low,current,cleanup]);await tick();
  assert.deepEqual(order,['cleanup','selection','background']);assert.equal(cancelled,false);
  assert.equal(q.requestStats().active,0);assert.equal(q.requestStats().queued,0);
  let done,dispatched=0;const held=q.schedule(()=>new Promise(resolve=>done=resolve));await tick();
  const full=Array.from({length:32},()=>q.schedule(()=>dispatched++));
  assert.equal(q.requestStats().queued,32);
  await assert.rejects(q.schedule(()=>{throw Error('must not dispatch overflow');}),error=>error.code==='busy'&&error.operationStarted===false);
  done();await Promise.all([held,...full]);await tick();assert.equal(dispatched,32);
  Object.assign(c,{offline:false,workspaceId:'fixture',viewEpoch:1,readController:new AbortController(),activeReads:0,activeWrites:0,token:'fixture-token',authFailed:false,
    V:{boundedJSON:JSON.stringify},staleRequest:()=>new DOMException('stale','AbortError')});
  vm.runInContext(API+'\nglobalThis.actualApi=api;',c);
  const calls=[];const ok=value=>({ok:true,status:200,json:async()=>value});
  const busy=()=>({ok:false,status:503,json:async()=>({code:'busy',error:'Analysis request capacity is busy; no operation was dispatched'})});
  for(const path of ['analysis-cell-submit','analysis-session-start','analysis-view-save','analysis-annotations-save','analysis-media-capture']){
    calls.length=0;c.fetch=async(url,init)=>{calls.push({url,init});return busy();};
    await assert.rejects(c.actualApi(path,{}, {request_id:'original-request',generation:'fixed-generation'}));
    assert.equal(calls.length,1,path+' must not retry a write/execution');
    assert.equal(JSON.parse(calls[0].init.body).request_id,'original-request');await tick();
  }
  let clock=0;c.Date={now:()=>clock};c.setTimeout=(callback,ms)=>{clock+=ms;return setTimeout(callback,0);};
  calls.length=0;c.fetch=async(url,init)=>{calls.push({url,init});return calls.length<3?busy():ok({id:'fixed-view',revision:1});};
  const read=await c.actualApi('analysis-view',{view_id:'fixed-view'});assert.equal(read.id,'fixed-view');assert.equal(calls.length,3);
  assert(calls.every(item=>item.init.method==='GET'&&item.init.headers['X-Forge-Token']==='fixture-token'));await tick();
  calls.length=0;clock=0;c.fetch=async(url,init)=>{calls.push({url,init});return busy();};
  await assert.rejects(c.actualApi('analysis-view',{view_id:'still-fixed'}));
  assert(calls.length<=13&&clock<=22000,'read retry budget');await tick();
  let resume;calls.length=0;c.fetch=(url,init)=>{calls.push({url,init});return url.endsWith('/analysis-view')?new Promise(resolve=>resume=()=>resolve(ok({id:'held'}))):Promise.resolve(ok({accepted:true}));};
  const block=c.actualApi('analysis-view');await tick();
  const body={request_id:'queued-original',target:{source:'fixed'}};
  const write=c.actualApi('analysis-annotations-save',{},body);body.request_id='changed-after-click';
  const stop=c.actualApi('analysis-stop-generation',{}, {request_id:'stop-original',session_id:'session',generation:'original-generation'});
  await stop;assert(calls.some(item=>item.url.endsWith('/analysis-stop-generation')),'stop bypasses metadata queue');
  assert(!calls.some(item=>item.url.endsWith('/analysis-annotations-save')));
  resume();await block;await write;await tick();
  const sent=calls.find(item=>item.url.endsWith('/analysis-annotations-save'));assert.equal(JSON.parse(sent.init.body).request_id,'queued-original');
  let resume2;calls.length=0;c.fetch=(url,init)=>{calls.push({url,init});return new Promise(resolve=>resume2=()=>resolve(ok({id:'last'})));};
  const currentRead=c.actualApi('analysis-view');await tick();
  const oldRead=c.actualApi('analysis-case-media',{}, {digest:'old-source',case_key:'0',field:'image'});
  const oldRejected=assert.rejects(oldRead,error=>error.name==='AbortError');c.readController.abort();c.viewEpoch++;c.readController=new AbortController();
  await oldRejected;resume2();await assert.rejects(currentRead,error=>error.name==='AbortError');await tick();
  assert.equal(calls.length,1,'cancelled queued source never dispatches');
  assert.equal(q.requestStats().active,0);assert.equal(q.requestStats().queued,0);assert.equal(c.activeReads,0);assert.equal(c.activeWrites,0);
  process.stdout.write('actual queue/API protocol passed\n');
}
main().catch(error=>{process.stderr.write(String(error.stack));process.exitCode=1;});
'''
    script='const TRANSPORT='+json.dumps((web/'analysis-transport.js').read_text())+';\nconst API='+json.dumps(api)+';\n'+script
    result=subprocess.run([node,'-'],input=script,text=True,capture_output=True,timeout=20)
    assert result.returncode==0,result.stdout+'\n'+result.stderr
