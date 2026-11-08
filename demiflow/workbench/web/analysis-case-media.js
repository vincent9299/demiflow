/* Fixed Case-field media UI. Trusted adapters own capture, authority and URLs. */
(() => {
  'use strict';
  const node=(tag,text,kind)=>{const value=document.createElement(tag);if(text!==undefined)value.textContent=text;if(kind)value.className=kind;return value;};
  const refKeys=['source_snapshot','case_key','field','artifact_id','artifact_generation','expected_binding','expected_sha256','mime'];
  const canonical=value=>JSON.stringify(value,(_key,item)=>item&&typeof item==='object'&&!Array.isArray(item)?Object.fromEntries(Object.keys(item).sort().map(key=>[key,item[key]])):item);
  const sha=value=>typeof value==='string'&&/^[a-f0-9]{64}$/.test(value);
  const released=new WeakSet();
  const release=value=>{if(!value||typeof value!=='object'||released.has(value))return;released.add(value);try{Promise.resolve(value.dispose?.()).catch(()=>{});}catch{}};
  const once=callback=>{let used=false;return ()=>{if(!used){used=true;callback();}};};
  const identity=value=>({source_snapshot:value.source_snapshot,case_key:value.case_key,field:value.field});
  const key=value=>canonical(identity(value));
  function reference(value,target){
    if(!value||typeof value!=='object'||Array.isArray(value)||Object.keys(value).length!==refKeys.length||refKeys.some(name=>!Object.hasOwn(value,name))||!sha(value.source_snapshot)||!sha(value.expected_binding)||!sha(value.expected_sha256)||typeof value.case_key!=='string'||!/^(0|[1-9][0-9]{0,19})$/.test(value.case_key)||BigInt(value.case_key)>=2n**64n||typeof value.field!=='string'||!value.field||!/^o[a-f0-9]{32}$/.test(value.artifact_id)||!Number.isSafeInteger(value.artifact_generation)||value.artifact_generation<1||typeof value.mime!=='string')throw Error('固定媒体引用不符合闭合协议');
    if(target&&key(value)!==key(target))throw Error('媒体引用与当前固定来源、Case 或字段不一致');
    ForgeAnalysisState.bounded(value,2048);return value;
  }
  function ready(value,target){
    if(value?.status!=='ready')throw Error('媒体尚未原子提交为 ready');
    reference(value.reference,target);const descriptor=value.descriptor,ref=value.reference;
    if(!descriptor||descriptor.artifact_id!==ref.artifact_id||descriptor.sha256!==ref.expected_sha256||descriptor.mime!==ref.mime||!Number.isSafeInteger(descriptor.bytes)||descriptor.bytes<1||descriptor.bytes>64*1024*1024||'output_index' in descriptor)throw Error('媒体 ready 元数据缺失或与固定引用不一致');
    ForgeAnalysisState.bounded(descriptor,4096);
    return {status:'ready',reference:ForgeAnalysisState.clone(ref),descriptor:ForgeAnalysisState.clone(descriptor),dispose:once(()=>release(value))};
  }
  function basis(value,target){
    if(!Array.isArray(value)||value.length>16)throw Error('每个答案最多绑定 16 个固定媒体字段');
    const names=new Set();for(const item of value){reference(item);if(target&&(item.source_snapshot!==target.source||item.case_key!==target.case_key))throw Error('答案媒体依据属于其他固定来源或 Case');if(names.has(item.field))throw Error('答案媒体依据包含重复字段');names.add(item.field);}
    ForgeAnalysisState.bounded(value,24*1024);return value;
  }
  function create({project,store,adapter,permissions,renderField,resolveMedia,pendingRoot,isCurrent=()=>true,onNotice=()=>{},onEvidence=()=>{}}){
    if(!adapter||typeof adapter.lookup!=='function'||typeof adapter.capture!=='function'||typeof adapter.receipt!=='function')throw Error('字段媒体适配器需要 lookup/capture/receipt');
    const S=ForgeAnalysisState,fields=new Set(),queue=[],rootAbort=new AbortController();let dead=false,inFlight=0,capturing=false;
    const allowed=()=>!dead&&permissions()?.analysis_write===true;
    const intents=()=>store.pending(project).filter(item=>item.scope.kind==='case'&&item.operation==='media-capture');
    const targetFor=intent=>({source_snapshot:intent.payload.digest,case_key:intent.payload.case_key,field:intent.payload.field});
    const notice=(text,error,target)=>{if(!dead&&isCurrent())onNotice(text,error,target);};
    function button(label,run,parent,disabled=false){const item=node('button',label);item.type='button';item.disabled=disabled;item.onclick=()=>Promise.resolve().then(run).catch(error=>{if(!dead&&error.name!=='AbortError')notice(error.message,true);});parent.append(item);return item;}
    function emit(){if(!dead){onEvidence();paintPending();}}
    function pendingFor(target){return intents().some(item=>key(targetFor(item))===key(target));}
    function pump(){
      while(!dead&&inFlight<2&&queue.length){
        const job=queue.shift();if(!job.alive()){job.done();continue;}inFlight++;
        Promise.resolve().then(job.run).catch(error=>{if(job.alive())job.fail(error);}).finally(()=>{inFlight--;job.done();pump();});
      }
    }
    function enqueue(job){if(queue.length>=32){job.fail(Error('媒体核对队列已达 32 个字段预算'));job.done();return;}queue.push(job);pump();}
    function evidence(source,caseKey,allowed=null){
      if(allowed!==null&&(!(allowed instanceof Set)||allowed.size>16))throw Error('媒体依据字段范围无效或超限');
      const selected=new Map();for(const field of fields){if(!field.alive()||!field.fixed||field.target.source_snapshot!==source||field.target.case_key!==caseKey||allowed&&!allowed.has(field.target.field))continue;
        const ref=field.fixed.reference,previous=selected.get(ref.field);if(previous&&canonical(previous)!==canonical(ref))throw Error('同一字段出现不同固定媒体版本，请重新核对后选择评测依据');selected.set(ref.field,ref);}
      return basis([...selected.values()].sort((a,b)=>a.field<b.field?-1:a.field>b.field?1:0),{source,case_key:caseKey}).map(S.clone);
    }
    function acknowledge(intent,value){
      const target=targetFor(intent);
      if(value?.status==='pending'){release(value);paintPending();return false;}
      if(value?.status==='unavailable'){release(value);store.finish(project,intent.payload.request_id);notice(value.message||value.reason||'媒体固定不可用，原字段值保留。',true,target);emit();return true;}
      const fixed=ready(value,target);store.finish(project,intent.payload.request_id);
      const candidates=[...fields].filter(field=>field.alive()&&key(field.target)===key(target));
      if(candidates.length){candidates[0].accept(fixed);for(const field of candidates.slice(1))field.lookup();}else fixed.dispose();
      notice('媒体已固定；评测答案仍需单独保存。',false,target);emit();return true;
    }
    async function dispatch(intent,signal){
      if(!allowed())throw Error('当前没有固定媒体权限；原请求与字段值保留');
      store.durableIntents();capturing=true;updatePermissions();paintPending();
      try{
        const p=intent.payload,value=await adapter.capture({project,request_id:p.request_id,source_snapshot:p.digest,case_key:p.case_key,field:p.field,signal});
        try{acknowledge(intent,value);}catch(error){release(value);throw error;}
      }catch(error){
        if(error.operationStarted===false||error.definitive===true){store.finish(project,intent.payload.request_id);throw error;}
        throw Error(error.message+'；媒体原请求 '+intent.payload.request_id+' 已保留，请先核对结果。');
      }finally{capturing=false;updatePermissions();paintPending();}
    }
    async function capture(field){
      if(!allowed())throw Error('当前没有固定媒体权限');if(capturing)throw Error('已有媒体固定请求正在提交，请稍后核对');
      if(!field.alive())return;if(pendingFor(field.target))throw Error('此字段仍有未确认的媒体原请求，请先核对');
      const target=field.target,payload={request_id:'m'+crypto.randomUUID().replaceAll('-',''),digest:target.source_snapshot,case_key:target.case_key,field:target.field};
      const intent=store.prepare(project,'media-capture',payload,{kind:'case',purpose:'field-media-capture',source:target.source_snapshot,case_key:target.case_key,field:target.field});
      paintPending();await dispatch(intent,field.signal);
    }
    function paintPending(){
      if(dead||!pendingRoot)return;pendingRoot.replaceChildren();let values;
      try{values=intents();}catch(error){pendingRoot.append(node('p',error.message,'error-box'));return;}
      for(const intent of values){const target=targetFor(intent),line=node('div',undefined,'analysis-pending-row');pendingRoot.append(line);
        line.append(node('span','媒体待核对 · 来源 '+target.source_snapshot.slice(0,12)+' · Case '+target.case_key+' · '+target.field+' · '+intent.payload.request_id,'muted small'));
        button('核对媒体原请求',async()=>{
          const value=await adapter.receipt({project,request_id:intent.payload.request_id,signal:rootAbort.signal});
          try{acknowledge(intent,value);}catch(error){release(value);throw error;}
        },line,capturing);
        button('以原媒体请求重试',()=>{if(capturing)return;return dispatch(intent,rootAbort.signal);},line,capturing||!allowed());
      }
    }
    function updatePermissions(){for(const field of fields)field.permissions();if(!dead)paintPending();}
    function mountField({parent,source_snapshot,case_key,field,format,value,label,compact=false,mountText,preview,disposeReaders,isCurrent:visible=()=>parent.isConnected}){
      if(fields.size>=32)throw Error('单个 Case 最多挂载 32 个媒体字段组件');
      const target={source_snapshot,case_key,field},host=node('div',undefined,'case-media-field'),status=node('p',undefined,'muted small'),controls=node('div',undefined,'case-media-controls'),view=node('div');
      if(compact)host.dataset.compact='true';
      const original=node('details',undefined,'case-media-original');original.append(node('summary',compact?'原始引用与媒体操作':'原字段值'));host.append(status,controls,view,original);parent.append(host);
      const abort=new AbortController(),owned=new Set(),originalOwned=new Set();let disposed=false,fixed=null,lookupAbort=null,lookupVersion=0,queued=false,phase='loading',resolveReady;
      const promise=new Promise(resolve=>{resolveReady=resolve;});
      const alive=()=>!dead&&!disposed&&!abort.signal.aborted&&visible()&&host.isConnected;
      const use=handle=>{if(handle?.dispose){if(alive())owned.add(handle);else handle.dispose();}};
      const clear=()=>{for(const handle of owned)handle.dispose();owned.clear();disposeReaders?.(view);globalThis.ForgeAnalysisRender?.disposeWithin?.(view);view.replaceChildren();if(fixed){fixed.dispose();fixed=null;}emit();};
      function show(text,error=false){if(alive()){status.className=error?'error-box':'muted small';status.textContent=text;status.title='';}}
      function permissionsChanged(){pin.disabled=!allowed()||capturing||pendingFor(target);pin.hidden=phase!=='capture_required';}
      function unavailable(reason){clear();phase='unavailable';show(reason||'此字段媒体不可用，原值仍可查看。',true);permissionsChanged();}
      function accept(value){
        if(!alive()){release(value);return;}clear();
        try{
          const accepted=ready(value,target);
          if(!accepted.descriptor.mime.startsWith(format+'/')){accepted.dispose();throw Error('实际媒体为 '+accepted.descriptor.mime+'，与所选预览格式不匹配；请保留原值或更改格式。');}
          fixed=accepted;phase='ready';const size=fixed.descriptor.bytes<1024?fixed.descriptor.bytes+' 字节':fixed.descriptor.bytes<1024*1024?(fixed.descriptor.bytes/1024).toFixed(1)+' KiB':(fixed.descriptor.bytes/1024/1024).toFixed(1)+' MiB';show(compact?'已固定媒体':'已固定 · '+fixed.descriptor.mime+' · '+fixed.descriptor.bytes+' 字节 · SHA '+fixed.reference.expected_sha256.slice(0,12));status.title=fixed.descriptor.mime+' · '+size+' · 内容 SHA '+fixed.reference.expected_sha256;
          if(typeof resolveMedia!=='function'){show('媒体已固定；当前播放适配器尚未接通，原值仍可查看。',true);}
          else {
            const captured=fixed,current=()=>alive()&&fixed===captured;
            const rendered=renderField({parent:view,format,value:captured.descriptor,label,originalLabel:'媒体描述信息',mountText,preview,disposeReaders,isCurrent:current,
              resolveMedia:request=>resolveMedia({...request,project,reference:S.clone(captured.reference),descriptor:S.clone(captured.descriptor),source_snapshot,case_key,field})});
            Promise.resolve(rendered).then(handle=>{if(!current()){handle?.dispose?.();return;}use(handle);handle?.ready?.catch?.(error=>{if(current())show(error.message,true);});}).catch(error=>{if(current())show(error.message,true);});
          }
          emit();permissionsChanged();
        }catch(error){release(value);unavailable(error.message);}
      }
      async function lookup(){
        if(!alive()||queued)return;queued=true;phase='loading';show('正在核对固定字段媒体…');permissionsChanged();
        const version=++lookupVersion;lookupAbort?.abort();lookupAbort=new AbortController();
        const signal=AbortSignal.any([abort.signal,lookupAbort.signal]);
        enqueue({alive:()=>alive()&&version===lookupVersion,done(){queued=false;resolveReady();},fail:error=>unavailable(error.message),run:async()=>{
          let result;
          try{
            result=await adapter.lookup({project,source_snapshot,case_key,field,signal});
            if(!alive()||signal.aborted||version!==lookupVersion){release(result);return;}
            if(result?.status==='ready'){accept(result);result=null;}
            else if(result?.status==='capture_required'){clear();phase='capture_required';show(result.message||'此字段尚未固定为可保存的媒体。可明确选择固定媒体供评测。');permissionsChanged();release(result);result=null;}
            else if(result?.status==='unavailable'){unavailable(result.message||result.reason||'未声明授权媒体资源根，无法读取；原值保留。');release(result);result=null;}
            else throw Error('媒体 lookup 返回了未知状态；未挂载预览');
          }catch(error){release(result);if(alive()&&version===lookupVersion&&error.name!=='AbortError')unavailable(error.message);}
        }});
      }
      const refresh=button('重新核对媒体',lookup,compact?original:controls);
      const pin=button('固定媒体供评测',()=>capture(entry),controls,!allowed());pin.hidden=true;
      original.ontoggle=()=>{if(original.open&&original.children.length===(compact?2:1)){const box=node('div');original.append(box);Promise.resolve(renderField({parent:box,format:typeof value==='string'?'text':'json',value,label:'原字段值',mountText,preview,disposeReaders,isCurrent:alive})).then(handle=>{if(handle?.dispose){if(alive())originalOwned.add(handle);else handle.dispose();}}).catch(error=>{if(alive())box.append(node('p',error.message,'error-box'));});}};
      const entry={target,signal:abort.signal,alive,get fixed(){return fixed;},accept(value){lookupVersion++;lookupAbort?.abort();accept(value);},lookup,permissions:permissionsChanged,
        element:host,ready:promise,dispose(){if(disposed)return;disposed=true;lookupVersion++;abort.abort();lookupAbort?.abort();clear();for(const handle of originalOwned)handle.dispose();originalOwned.clear();disposeReaders?.(original);globalThis.ForgeAnalysisRender?.disposeWithin?.(original);fields.delete(entry);refresh.onclick=null;pin.onclick=null;original.ontoggle=null;resolveReady();}};
      fields.add(entry);lookup();return entry;
    }
    paintPending();return {mountField,evidence,paintPending,updatePermissions,dispose(){if(dead)return;dead=true;rootAbort.abort();for(const field of [...fields])field.dispose();for(const job of queue.splice(0))job.done();pendingRoot?.replaceChildren();}};
  }
  globalThis.ForgeCaseMedia=Object.freeze({create,reference,basis,canonical});
})();
