/* Trusted output transport. Credentials remain in app.js and HttpOnly cookies. */
(() => {
  'use strict';
  // One metadata request leaves capacity for native playback byte requests.
  // The existing HTTP and worker limits remain the final authority. This queue
  // only schedules the original call, never replays a write or an execution.
  const pendingRequests=[];let activeRequests=0;
  function requestAbort(){return new DOMException('页面读取已取消','AbortError');}
  function drainRequests(){
    if(activeRequests||!pendingRequests.length)return;
    pendingRequests.sort((a,b)=>b.priority-a.priority);
    const item=pendingRequests.shift();item.signal?.removeEventListener('abort',item.cancel);
    if(item.signal?.aborted){item.reject(requestAbort());drainRequests();return;}
    activeRequests=1;
    Promise.resolve().then(()=>{if(item.signal?.aborted)throw requestAbort();return item.run();})
      .then(item.resolve,item.reject).finally(()=>{activeRequests=0;drainRequests();});
  }
  function schedule(run,{signal,priority=1}={}){
    if(typeof run!=='function'||![0,1,2].includes(priority))return Promise.reject(new TypeError('无效的分析请求'));
    if(signal?.aborted)return Promise.reject(requestAbort());
    if(pendingRequests.length>=32){const error=new Error('分析读取较多，请稍后重试；该请求尚未发送。');error.code='busy';error.operationStarted=false;return Promise.reject(error);}
    return new Promise((resolve,reject)=>{
      const item={run,signal,priority,resolve,reject,cancel:null};
      item.cancel=()=>{const at=pendingRequests.indexOf(item);if(at<0)return;pendingRequests.splice(at,1);signal?.removeEventListener('abort',item.cancel);reject(requestAbort());};
      signal?.addEventListener('abort',item.cancel,{once:true});pendingRequests.push(item);drainRequests();
    });
  }
  const sameDescriptor=(a,b)=>a&&b&&Object.keys(a).length===Object.keys(b).length&&Object.keys(a).every(key=>Object.hasOwn(b,key)&&a[key]===b[key]);
  const fixed = request => {
    const artifact=request.artifact,ref=request.reference;
    if(!artifact||!request.execution_id||ref?.artifact_id!==artifact.id||!Number.isSafeInteger(ref.output_index))throw Error('此字段尚未关联经登记的固定媒体输出');
    return {execution_id:request.execution_id,expected_output_id:artifact.id,expected_binding:artifact.digest,
      output_index:ref.output_index,mime:ref.mime,expected_sha256:ref.sha256};
  };
  const caseFixed = request => {
    const value=request.reference,keys=['source_snapshot','case_key','field','artifact_id','artifact_generation','expected_binding','expected_sha256','mime'];
    if(!value||Object.keys(value).length!==keys.length||keys.some(key=>!Object.hasOwn(value,key))||
      !/^[a-f0-9]{64}$/.test(value.source_snapshot)||!/^[a-f0-9]{64}$/.test(value.expected_binding)||!/^[a-f0-9]{64}$/.test(value.expected_sha256)||
      !/^o[a-f0-9]{32}$/.test(value.artifact_id)||!Number.isSafeInteger(value.artifact_generation)||value.artifact_generation<1||value.artifact_generation>2147483647||
      typeof value.case_key!=='string'||!/^(0|[1-9][0-9]{0,19})$/.test(value.case_key)||BigInt(value.case_key)>=2n**64n||typeof value.field!=='string'||!value.field||typeof value.mime!=='string')throw Error('Case 媒体固定引用无效');
    ForgeAnalysisState.bounded(value,2048);return ForgeAnalysisState.clone(value);
  };
  function create({project,api,isCurrent=()=>true}){
    let dead=false,active=null,queued=0,chain=Promise.resolve();
    const alive=()=>!dead&&isCurrent();
    const action=(name,reference,{signal,cleanup=false}={})=>{
      if(queued>=4)return Promise.reject(Error('媒体凭证操作繁忙，请稍后重试'));
      queued++;
      const result=chain.catch(()=>{}).then(()=>{
        if(!cleanup&&(!alive()||signal?.aborted))throw new DOMException('页面已变化','AbortError');
        return api('analysis-media',{}, {action:name,reference},project,{signal,cleanup});
      });
      chain=result.finally(()=>{queued--;});chain.catch(()=>{});return result;
    };
    const inspect=async request=>{
      if(!alive())throw new DOMException('页面已变化','AbortError');
      return api('analysis-output-inspect',fixed(request),undefined,project,{signal:request.signal});
    };
    const readRange=request=>api('analysis-output-media',fixed(request),undefined,project,
      {signal:request.signal,range:`bytes=${request.start}-${request.end}`,outputRange:true});
    async function playback(request,reference=fixed(request)){
      request.beforePrepare?.();
      if(active&&!active.closed)await active.release();
      if(!alive()||request.signal?.aborted)throw new DOMException('页面已变化','AbortError');
      const owner={reference,closed:false,timer:null,release:null};
      const release=()=>{
        if(owner.closed)return;owner.closed=true;clearTimeout(owner.timer);
        request.signal?.removeEventListener('abort',release);if(active===owner)active=null;
        return action('revoke',reference,{cleanup:true}).catch(()=>{});
      };
      owner.release=release;active=owner;
      try{
        const value=await action('prepare',reference,{signal:request.signal});
        if(owner.closed||!alive()||request.signal?.aborted){await release();throw new DOMException('页面已变化','AbortError');}
        const base='/api/workspaces/'+project+'/analysis-media',target=new URL(value.url,location.href);
        if(target.origin!==location.origin||target.pathname!==base||target.username||target.password||target.hash||!/^\?binding=[a-f0-9]{64}$/.test(target.search))throw Error('视频固定引用 URL 无效');
        const expected=target.pathname+target.search;
        if(value.binding!==reference.expected_binding||!sameDescriptor(value.descriptor,request.descriptor))throw Error('视频凭证与固定输出不一致');
        const renew=()=>{
          clearTimeout(owner.timer);
          const seconds=value.expires_in;
          if(!Number.isInteger(seconds)||seconds<1||seconds>300)throw Error('视频凭证有效期无效');
          owner.timer=setTimeout(async()=>{
            if(owner.closed||!alive()){release();return;}
            try{const renewed=await action('renew',reference,{signal:request.signal});
              if(renewed.url!==expected||renewed.binding!==reference.expected_binding)throw Error('视频续期身份变化');
              if(!owner.closed)renew();
            }catch{release();}
          },Math.max(1,seconds-30)*1000);
        };
        request.signal?.addEventListener('abort',release,{once:true});renew();
        return {url:expected,authorizeURL:url=>alive()&&!owner.closed&&url.origin===location.origin&&url.pathname+url.search===expected&&!url.username&&!url.password&&!url.hash,dispose:release};
      }catch(error){await release();throw error;}
    }
    const resolveMedia=request=>ForgeAnalysisMedia.createTransport({inspect,readRange,playback})(request);
    const resolveCaseMedia=request=>{
      const reference=caseFixed(request);
      return ForgeAnalysisMedia.createTransport({
        inspect:()=>api('analysis-artifact-media-current',{}, {reference},project,{signal:request.signal}),
        readRange:item=>api('analysis-artifact-media',{reference:JSON.stringify(reference)},undefined,project,
          {signal:item.signal,range:`bytes=${item.start}-${item.end}`,outputRange:true}),
        playback:item=>playback(item,reference)
      })({...request,reference:request.descriptor});
    };
    const caseIdentity=value=>{
      if(!alive()||value.project!==project||value.signal?.aborted)throw new DOMException('Case 页面已变化','AbortError');
      return {digest:value.source_snapshot,case_key:value.case_key,field:value.field};
    };
    const fieldMedia=Object.freeze({
      lookup:value=>api('analysis-case-media',{},caseIdentity(value),project,{signal:value.signal}),
      capture:value=>api('analysis-media-capture',{}, {request_id:value.request_id,...caseIdentity(value)},project,{signal:value.signal}),
      receipt:value=>{
        if(!alive()||value.project!==project)throw new DOMException('Case 页面已变化','AbortError');
        return api('analysis-receipt',{request_id:value.request_id},undefined,project,{signal:value.signal});
      }
    });
    return {resolveMedia,resolveCaseMedia,fieldMedia,dispose(){if(dead)return;dead=true;active?.release();}};
  }
  globalThis.ForgeAnalysisTransport=Object.freeze({create,schedule,requestStats:()=>({active:activeRequests,queued:pendingRequests.length,limit:32})});
})();
