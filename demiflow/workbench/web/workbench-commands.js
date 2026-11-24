/* Durable browser intents complement the existing server run_requests ledger. */
(() => {
  'use strict';
  const KEY='demiflow-workbench-run-intents-v1';
  const fileKey='demiflow-workbench-parameter-intents-v1';
  const clone=value=>JSON.parse(JSON.stringify(value));
  const bytes=text=>new TextEncoder().encode(text).byteLength;
  const canonical=value=>Array.isArray(value)?value.map(canonical):value&&typeof value==='object'?
    Object.fromEntries(Object.keys(value).sort().map(key=>[key,canonical(value[key])])):value;
  function read(key,maximum) {
    const raw=sessionStorage.getItem(key)||'[]';
    if(bytes(raw)>maximum)throw new Error('本浏览器的未决操作记录超限，请先处理已有记录。');
    const value=JSON.parse(raw);
    if(!Array.isArray(value)||value.length>16)throw new Error('本浏览器的未决操作记录无法读取。');
    return value;
  }
  function write(key,rows,maximum) {
    const raw=JSON.stringify(rows);
    if(rows.length>16||bytes(raw)>maximum)throw new Error('未决操作已满，请先核对已有操作；未创建新请求。');
    sessionStorage.setItem(key,raw);
  }
  function put(key,row,match,maximum) {
    const rows=read(key,maximum),index=rows.findIndex(match);
    if(index<0)rows.push(clone(row));else rows[index]=clone(row);
    write(key,rows,maximum);
  }
  function remove(key,match,maximum) {write(key,read(key,maximum).filter(row=>!match(row)),maximum);}
  function create({project,api}) {
    if(!/^[wl][0-9a-f]{32}$/.test(project))throw new Error('项目身份不合法');
    const call=(name,query,body)=>api(name,query,body,project);
    const pending=()=>read(KEY,512*1024).filter(row=>row.project===project);
    const persist=row=>put(KEY,row,v=>v.project===project&&v.request_id===row.request_id,512*1024);
    async function identity(body) {
      const captured=clone(body),raw=JSON.stringify(canonical(captured));
      if(bytes(raw)>65536)throw new Error('运行配置超过 64 KiB');
      const hash=await crypto.subtle.digest('SHA-256',new TextEncoder().encode(raw));
      const fingerprint=Array.from(new Uint8Array(hash),v=>v.toString(16).padStart(2,'0')).join('');
      const old=pending().find(row=>row.fingerprint===fingerprint);
      if(old)return clone(old);
      const id='p'+String(Date.now()).padStart(13,'0')+'-'+Array.from(crypto.getRandomValues(new Uint8Array(16)),v=>v.toString(16).padStart(2,'0')).join('');
      const intent={project,request_id:id,fingerprint,body:captured,phase:'prepare',created_at:Date.now()};
      persist(intent);return intent;
    }
    function recordRun(intent,run) {
      if(!/^r[0-9a-f]{24}$/.test(run?.run_id)||!/^[0-9a-f]{64}$/.test(run.digest)||!Number.isInteger(run.attempt))throw new Error('服务返回的运行身份无法核对');
      const next={...intent,run_id:run.run_id,digest:run.digest,attempt:intent.attempt??1,phase:'start'};
      persist(next);return next;
    }
    const finish=intent=>remove(KEY,v=>v.project===project&&v.request_id===intent.request_id,512*1024);
    async function prepare(intent) {
      let result=await call('run-prepare',{}, {...intent.body,request_id:intent.request_id});
      if(result.attempt!==1)result=await call('run',{run_id:result.run_id,attempt:1,compact:'1'});
      return recordRun(intent,result);
    }
    async function start(intent) {
      const result=await call('run-start',{}, {run_id:intent.run_id,expected:intent.digest,expected_attempt:intent.attempt});
      if(result.run_id!==intent.run_id||result.attempt!==intent.attempt)throw new Error('启动响应的运行或尝试身份不一致，请核对原目标。');
      finish(intent);return result;
    }
    async function inspect(intent) {
      if(intent.run_id) {
        const run=await call('run',{run_id:intent.run_id,attempt:intent.attempt,compact:'1'});
        if(run.run_id!==intent.run_id||run.attempt!==intent.attempt)throw new Error('核对响应身份不一致');
        if(run.status!=='prepared'||(run.state?.execution_stopped===true&&run.state?.cleanup==='complete'))finish(intent);
        return {intent,run};
      }
      const receipt=await call('run-request',{request_id:intent.request_id});
      if(receipt.request_id!==intent.request_id)throw new Error('准备回执身份不一致');
      if(receipt.status==='ready') {
        const run=await call('run',{run_id:receipt.run_id,attempt:1,compact:'1'});
        return {intent:recordRun(intent,run),run,receipt};
      }
      return {intent,receipt};
    }
    const files=()=>read(fileKey,512*1024).filter(row=>row.project===project);
    const persistFile=row=>put(fileKey,row,v=>v.project===project&&v.path===row.path,512*1024);
    const forgetFile=path=>remove(fileKey,v=>v.project===project&&v.path===path,512*1024);
    async function saveParameters(body) {
      const captured=clone(body);
      const preview=await call('parameter-preview',{},captured);
      if(preview.base_sha256!==captured.expected_sha256)throw new Error('配置预览基线不一致');
      if(preview.no_change)return {no_change:true};
      const intent={project,path:captured.path,body:{...captured,proposed_sha256:preview.proposed_sha256},phase:'saving'};
      if(bytes(JSON.stringify(intent))>32768)throw new Error('保存意图超过 32 KiB，请减少本次修改');
      persistFile(intent);
      try {
        const result=await call('parameters-update',{},intent.body);
        if(!result.applied||result.new_sha256!==preview.proposed_sha256)throw new Error('配置保存结果无法核对');
        forgetFile(intent.path);return result;
      } catch(error) {
        const rejected=['forbidden','unauthorized','invalid_request','operation_rejected','resource_limit'].includes(error.code);
        persistFile({...intent,phase:error.code==='conflict'?'conflict':rejected?'rejected':'unknown',error:String(error.message).slice(0,1024)});
        throw error;
      }
    }
    async function inspectFile(intent) {
      const value=await call('parameter-state',{path:intent.path});
      const observed=value.observed_sha256;
      return {state:observed===intent.body.proposed_sha256?'content_matches':observed===intent.body.expected_sha256?'base_observed':'conflict',value};
    }
    return Object.freeze({identity,pending,prepare,start,inspect,finish,
      retryPrepare:prepare,
      reconcile:intent=>call('run-request-reconcile',{}, {request_id:intent.request_id}),
      cancel:target=>call('run-cancel',{}, {run_id:target.run_id,expected_attempt:target.attempt,compact:true}),
      reconcileRun:target=>call('run-reconcile',{}, {run_id:target.run_id,expected_attempt:target.attempt}),
      resume:target=>call('run-resume',{}, {run_id:target.run_id,expected:target.digest,expected_attempt:target.attempt}),
      files,saveParameters,inspectFile,forgetFile,
      retryFile:intent=>call('parameters-update',{},clone(intent.body)),
    });
  }
  globalThis.ForgeWorkbenchCommands=Object.freeze({create});
})();
