/* Protocol/state unit checks. Fake API replies do not prove native execution. */
const fs=require('node:fs'),vm=require('node:vm'),assert=require('node:assert/strict');
const {webcrypto}=require('node:crypto');
const path=require('node:path');
const saved=new Map();
const context=vm.createContext({TextEncoder,TextDecoder,Date,crypto:webcrypto,
  sessionStorage:{getItem:key=>saved.get(key)||null,setItem:(key,value)=>saved.set(key,value)}});
for(const name of ['workbench-state.js','workbench-commands.js'])vm.runInContext(fs.readFileSync(path.join(__dirname,'../../demiflow/workbench/web',name),'utf8'),context);
const clean=value=>JSON.parse(JSON.stringify(value));
const A='w'+'a'.repeat(32),B='w'+'b'.repeat(32),id='r'+'1'.repeat(24),digest='a'.repeat(64);
let groups=0;
(async()=>{
  const state=context.ForgeWorkbenchState.create(A);
  const one=state.transition({entry:'flows/test.py:run',params:'flows/a.json'});
  assert(state.acceptGraph(one,{binding:{workspace_id:A,basis_kind:'source',entry:'flows/test.py:run',params:'flows/a.json'},nodes:[{id:'n1'},{id:'n2'}]}));
  const detail=state.chooseNode('n1');state.chooseNode('n2');assert(!state.valid(detail));assert(state.valid(one));groups++;
  const old=state.transition({run:id,attempt:1});state.transition({run:id,attempt:2});assert(!state.valid(old));groups++;
  assert.throws(()=>state.acceptGraph(state.capture(),{binding:{workspace_id:B},nodes:[]}));groups++;
  state.dispose();assert(!state.valid(state.capture()));groups++;
  const calls=[];let phase='prepare_lost';
  const api=async(name,query,body,project)=>{
    calls.push(clean({name,query,body,project}));
    if(name==='run-prepare'){
      if(phase==='prepare_lost')throw new Error('lost prepare response');
      return {run_id:id,digest,attempt:2,status:'running'};
    }
    if(name==='run-start')throw new Error('lost start response');
    if(name==='run')return {run_id:id,digest,attempt:query.attempt,status:phase==='prepared'?'prepared':'running'};
    if(name==='parameter-preview')return {base_sha256:body.expected_sha256,proposed_sha256:digest,no_change:false};
    if(name==='parameters-update'){const error=new Error('save response lost');error.code=phase==='conflict'?'conflict':'operation_failed';throw error;}
    if(name==='parameter-state')return {observed_sha256:phase==='base'?'b'.repeat(64):phase==='other'?'c'.repeat(64):digest};
    throw new Error('Unexpected fake API '+name);
  };
  const commands=context.ForgeWorkbenchCommands.create({project:A,api});
  const body={entry:'flows/test.py:run',params:'flows/a.json',inputs:{},limits:{timeout_s:30}};
  const first=await commands.identity(body);body.params='flows/changed.json';
  await assert.rejects(commands.prepare(first));assert.equal(commands.pending().length,1);groups++;
  const same=await commands.identity({...body,params:'flows/a.json'});assert.equal(same.request_id,first.request_id);groups++;
  phase='prepared';const prepared=await commands.prepare(first);
  assert.equal(prepared.attempt,1);assert(calls.some(c=>c.name==='run'&&c.query.attempt===1));groups++;
  await assert.rejects(commands.start(prepared));assert.equal(commands.pending()[0].run_id,id);groups++;
  phase='running';const inspected=await commands.inspect(prepared);assert.equal(inspected.run.attempt,1);assert.equal(commands.pending().length,0);groups++;
  assert(calls.every(c=>c.project===A));assert(calls.filter(c=>c.name==='run-start').every(c=>c.body.expected_attempt===1));groups++;
  const parameterBody={path:'flows/a.json',expected_sha256:'b'.repeat(64),graph_binding:{basis_kind:'source'},replace:[{op:'replace',path:'/arguments/minimum',value:.7}]};
  await assert.rejects(commands.saveParameters(parameterBody));
  const intent=commands.files()[0];assert.equal(intent.phase,'unknown');assert.equal(intent.body.expected_sha256,parameterBody.expected_sha256);
  assert(calls.filter(c=>c.name==='parameters-update').every(c=>!Object.hasOwn(c.body,'request_id')));groups++;
  assert.equal((await commands.inspectFile(intent)).state,'content_matches');assert.equal(commands.files().length,1);groups++;
  phase='base';assert.equal((await commands.inspectFile(intent)).state,'base_observed');groups++;
  phase='other';assert.equal((await commands.inspectFile(intent)).state,'conflict');groups++;
  await assert.rejects(commands.retryFile(intent));assert.deepEqual(calls.at(-1).body,clean(intent.body));groups++;
  commands.forgetFile(intent.path);assert.equal(commands.files().length,0);groups++;
  phase='conflict';await assert.rejects(commands.saveParameters(parameterBody));assert.equal(commands.files()[0].phase,'conflict');groups++;
  commands.forgetFile(intent.path);
  for(let i=0;i<16;i++)await commands.identity({entry:'flows/test.py:run',params:'flows/'+i+'.json'});
  await assert.rejects(commands.identity({entry:'flows/test.py:run',params:'flows/17.json'}));assert.equal(commands.pending().length,16);groups++;
  console.log(JSON.stringify({unit_groups:groups,status:'passed',scope:'pure identity/storage/command protocol with fake responses; real browser and native acceptance pending'}));
})().catch(error=>{console.error(error);process.exitCode=1;});
