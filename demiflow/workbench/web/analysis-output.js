/* Notebook MIME pages and immutable provenance; no output executes UI code. */
(() => {
  'use strict';
  const node=(tag,text,kind)=>{const item=document.createElement(tag);if(text!==undefined)item.textContent=text;if(kind)item.className=kind;return item;};
  const render=()=>globalThis.ForgeAnalysisRender;
  const sha=value=>typeof value==='string'&&/^[a-f0-9]{64}$/.test(value);
  const notebookOrigin=value=>value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).length===2&&value.kind==='notebook_output'&&/^no[a-f0-9]{64}$/.test(value.reference);
  function caseEvidence(rep,output,index){
    const proof=rep.definition_witness,binding=proof?.binding,origin=rep.origin,descriptor=rep.value;
    // This is a consistency gate on an authenticated server projection, not a
    // browser registration/signature scheme. Saving still rechecks the store.
    const registered=Number.isSafeInteger(index)&&index>=0&&index<256&&sha(output.artifact.digest)&&sha(descriptor?.source_snapshot)&&rep.mime==='application/vnd.demiflow.case-view+json'&&rep.available===true&&proof?.registered===true&&notebookOrigin(origin)&&notebookOrigin(proof.origin)&&origin.reference===proof.origin.reference&&
      binding?.artifact_id===output.artifact.id&&binding.manifest_sha256===output.artifact.digest&&binding.output_index===index&&binding.execution_id===output.provenance.execution_id&&binding.kernel_generation===output.provenance.kernel_generation&&binding.source_snapshot===descriptor?.source_snapshot&&sha(binding.descriptor_sha256)&&
      Array.isArray(descriptor?.questions)&&Array.isArray(binding.question_sha256)&&binding.question_sha256.length===descriptor.questions.length&&binding.question_sha256.length<=16&&binding.question_sha256.every(sha);
    return {origin:registered?structuredClone(origin):null,definition_witness:registered?structuredClone(proof):null,annotation_origin_registered:!!registered};
  }
  function mount({parent,output,project,mountText,preview,resolveMedia,disposeReaders,isCurrent=()=>parent.isConnected,onCase,onTable}){
    const root=node('section',undefined,'analysis-output');parent.append(root);let dead=false;const owned=new Set();
    const current=()=>!dead&&root.isConnected&&isCurrent();
    const dispose=()=>{if(dead)return;dead=true;for(const item of owned)item.dispose();owned.clear();render()?.disposeWithin(root);disposeReaders?.(root);root.replaceChildren();};
    if(!output||output.version!=='output_v1'||output.provenance?.project!==project||!Array.isArray(output.items)||output.items.length>16||!output.artifact?.digest){root.append(node('p','Notebook 输出身份或协议不匹配，未挂载。','error-box'));return {element:root,dispose};}
    // Keep the descriptor/origin together, independent of later caller/UI mutations.
    output=structuredClone(output);const p=output.provenance;
    root.append(node('p','执行 '+p.execution_id+' · Notebook '+p.notebook_id+'@'+p.document_revision+' · 单元 '+p.cell_id+' · kernel '+p.kernel_generation+' · 输出代次 '+p.output_generation,'analysis-output-provenance muted small'));
    const facts=node('details');facts.append(node('summary','固定来源与输出依据'),node('pre',JSON.stringify({source_sha256:p.source_sha256,document_sha256:p.document_sha256,source_snapshots:p.source_snapshots,artifact:output.artifact},null,2),'analysis-raw'));root.append(facts);
    root.append(node('p',(output.coverage?.complete?'Jupyter 消息已完成':'输出未完成')+' · '+output.coverage?.status+' · 未采集原生 FD 直写输出','analysis-output-coverage muted small'));
    for(const item of output.items){
      const article=node('article',undefined,'analysis-output-item'),bar=node('div',undefined,'analysis-output-controls'),host=node('div');root.append(article);article.append(bar,host);
      bar.append(node('strong',item.output_type==='stream'?item.name:item.output_type==='error'?'执行错误':item.execution_count!==undefined?'Out['+item.execution_count+']':'输出 '+item.index));
      if(item.output_type==='error')article.classList.add('analysis-output-error');
      const representations=item.representations;
      if(!Array.isArray(representations)||representations.length>16){host.append(node('p','输出 MIME 集合无效','error-box'));continue;}
      const select=node('select');select.setAttribute('aria-label','输出 '+item.index+' MIME 格式');bar.append(select);
      for(const [index,rep]of representations.entries()){const option=node('option',rep.mime+(rep.available===false?' · 不可用/原值':''));option.value=index;select.append(option);}
      select.value=Math.max(0,representations.findIndex(rep=>rep.available!==false));let field=null;
      const paint=()=>{
        field?.dispose();field=null;render()?.disposeWithin(host);disposeReaders?.(host);host.replaceChildren();
        const rep=representations[Number(select.value)];if(!rep)return;
        if(rep.truncated||rep.available===false)host.append(node('p',(rep.truncated?'仅有界预览；':'')+(rep.reason||'当前格式不可用')+' · 原始值保留于固定 artifact '+output.artifact.id+' / 输出 '+item.index,'muted small'));
        if(['case','table'].includes(rep.format)&&rep.available!==false){
          const callback=rep.format==='case'?onCase:onTable,button=node('button',rep.format==='case'?'在 Case 工作台查看此描述':'查看固定来源表');button.type='button';button.disabled=typeof callback!=='function';host.append(button);
          host.append(node('p','这是 Notebook 产生的展示描述；保存的视图与评审答案由各自版本管理。','muted small'));
          const evidence=rep.format==='case'?caseEvidence(rep,output,item.index):{origin:null,definition_witness:null,annotation_origin_registered:false};
          if(rep.format==='case'&&!evidence.annotation_origin_registered)host.append(node('p','此输出尚未登记评审来源，可查看描述。','muted small'));
          button.onclick=()=>{if(current()&&button.isConnected&&host.contains(button))callback(structuredClone(rep.value),{...structuredClone(evidence),provenance:structuredClone(p),output_index:item.index});};
          field=render()?.field({parent:host,format:'json',value:rep.value,isCurrent:current});
        }else if(typeof rep.raw_json==='string'){host.append(node('p','JSON 整数精确原文（浏览器数值范围外）','muted small'));field=render()?.field({parent:host,format:'code',language:'json',value:rep.raw_json,isCurrent:current});
        }else if(Object.hasOwn(rep,'value')){
          field=render()?.field({parent:host,format:rep.available===false?'json':rep.format,value:rep.value,label:rep.mime,mountText,
            preview:rep.preview?async()=>rep.preview:preview,resolveMedia:resolveMedia?request=>resolveMedia({...request,artifact:output.artifact,execution_id:p.execution_id}):null,disposeReaders,isCurrent:current});
        }else host.append(node('p','该表示未传入当前页；使用受控原始 artifact 读取。','muted small'));
        if(field)owned.add(field);
      };
      select.onchange=()=>{if(field)owned.delete(field);paint();};paint();
    }
    return {element:root,dispose};
  }
  function mountExecution({parent,execution,project,api,mountText,disposeReaders,resolveMedia,onCase,onTable,isCurrent=()=>parent.isConnected}){
    const root=node('details',undefined,'analysis-output-slot');root.append(node('summary','Notebook 输出'+(execution.output_id?' · '+execution.output_id:' · 尚未提交')));parent.append(root);
    let dead=false,epoch=0,currentMount=null,page=null,busy=false;let abort=new AbortController();
    const content=node('div');root.append(content);
    const current=ticket=>!dead&&ticket===epoch&&root.isConnected&&isCurrent();
    async function load(offset=0){
      if(busy||!execution.output_id)return;busy=true;const ticket=epoch;content.setAttribute('aria-busy','true');
      try{
        const params={execution_id:execution.id,expected_output_id:execution.output_id,offset,limit:8};if(page)params.expected_binding=page.artifact.digest;
        const value=await api('analysis-output',params,null,project,{signal:abort.signal});if(!current(ticket))return;
        if(value.provenance?.execution_id!==execution.id||value.provenance?.kernel_generation!==execution.generation||value.artifact?.id!==execution.output_id||page&&value.artifact?.digest!==page.artifact.digest)throw Error('输出属于另一 execution/generation/binding');
        page=value;currentMount?.dispose();render()?.disposeWithin(content);disposeReaders?.(content);content.replaceChildren();
        currentMount=mount({parent:content,output:value,project,mountText,disposeReaders,resolveMedia,onCase,onTable,isCurrent:()=>current(ticket),preview:(format,value)=>api('analysis-field-preview',{}, {format,value},project)});
        const controls=node('div',undefined,'analysis-output-controls');content.append(controls);
        for(const [label,next]of [['上一页',Math.max(0,offset-8)],['下一页',value.next_offset]]){const button=node('button',label);button.type='button';button.disabled=label==='上一页'?offset===0:next===null;button.onclick=()=>load(next);controls.append(button);}
      }catch(error){if(current(ticket)){const failure=node('p',error.message,'error-box');content.append(failure);const retry=node('button','重试读取输出');retry.type='button';retry.onclick=()=>{failure.remove();retry.remove();load(offset);};content.append(retry);}}
      finally{busy=false;if(current(ticket))content.removeAttribute('aria-busy');}
    }
    root.ontoggle=()=>{if(!root.open){epoch++;abort.abort();currentMount?.dispose();currentMount=null;render()?.disposeWithin(content);disposeReaders?.(content);content.replaceChildren();page=null;busy=false;}else{abort=new AbortController();load();}};
    return {element:root,dispose(){if(dead)return;dead=true;epoch++;abort.abort();root.ontoggle=null;currentMount?.dispose();render()?.disposeWithin(content);disposeReaders?.(content);content.replaceChildren();}};
  }
  globalThis.ForgeAnalysisOutput=Object.freeze({mount,mountExecution});
})();
