/* Deterministic subscription lifecycle checks; no native execution is mocked as passed. */
const fs=require('node:fs'),vm=require('node:vm'),assert=require('node:assert/strict'),path=require('node:path');
let timerId=0;const timers=new Map(),listeners=new Map(),document={hidden:false,
  addEventListener:(kind,fn)=>listeners.set(kind,fn),removeEventListener:kind=>listeners.delete(kind)};
const context=vm.createContext({AbortController,document,Math,
  setTimeout:fn=>{timers.set(++timerId,fn);return timerId;},clearTimeout:id=>timers.delete(id)});
vm.runInContext(fs.readFileSync(path.join(__dirname,'../../demiflow/workbench/web/workbench-observer.js'),'utf8'),context);
const project='w'+'a'.repeat(32),run='r'+'a'.repeat(24),digest='a'.repeat(64);
let selected=1;const calls=[],snapshots=[],failures=[];let groups=0;
const api=(name,query,body,pid,options)=>new Promise((resolve,reject)=>calls.push({name,query,pid,options,resolve,reject}));
const observer=context.ForgeWorkbenchObserver.create({project,api,isCurrent:t=>selected===t.attempt,
  onSnapshot:(value,target)=>snapshots.push({value,target}),onFailure:(error,target)=>failures.push({error,target})});
const value=attempt=>({run_id:run,attempt,current_attempt:attempt,digest,revision:'v'+attempt,status:'running',state:{}});
(async()=>{
  const old=observer.set({run_id:run,attempt:1,digest});assert.equal(calls.length,1);
  selected=2;const latest=observer.set({run_id:run,attempt:2,digest});assert(calls[0].options.signal.aborted);groups++;
  calls[1].resolve(value(2));await latest;assert.equal(snapshots.length,1);assert.equal(snapshots[0].target.attempt,2);groups++;
  calls[0].reject(new Error('late old-attempt error'));await old;assert.equal(failures.length,0);assert.equal(snapshots.length,1);groups++;
  const refresh=observer.refresh();calls[2].resolve({...value(2),run_id:'r'+'b'.repeat(24)});await assert.rejects(refresh);assert.equal(failures.length,1);groups++;
  const held=observer.refresh();document.hidden=true;listeners.get('visibilitychange')();assert(calls[3].options.signal.aborted);
  calls[3].resolve(value(2));await held;assert.equal(snapshots.length,1);groups++;
  document.hidden=false;listeners.get('visibilitychange')();assert.equal(calls.length,5);
  calls[4].resolve({...value(2),current_attempt:3});await new Promise(resolve=>setImmediate(resolve));
  assert.equal(snapshots.length,2);assert.equal(snapshots.at(-1).target.attempt,2);assert.equal(timers.size,0);groups++;
  assert(calls.every(call=>call.pid===project&&call.query.run_id===run&&call.query.attempt<=2));groups++;
  observer.dispose();assert.equal(listeners.size,0);assert.equal(timers.size,0);groups++;
  console.log(JSON.stringify({unit_groups:groups,status:'passed',scope:'subscription identity/late success and error/visibility/disposal, fake bounded API replies only'}));
})().catch(error=>{console.error(error);process.exitCode=1;});
