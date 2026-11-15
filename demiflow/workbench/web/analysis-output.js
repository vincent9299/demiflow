/* Notebook MIME pages and immutable provenance; no output executes UI code. */
(() => {
  'use strict';
  const node=(tag,text,kind)=>{const item=document.createElement(tag);if(text!==undefined)item.textContent=text;if(kind)item.className=kind;return item;};
  const render=()=>globalThis.ForgeAnalysisRender;
  const sha=value=>typeof value==='string'&&/^[a-f0-9]{64}$/.test(value);
  const bytesOf=value=>new TextEncoder().encode(JSON.stringify(value)).length;
  const PRIMITIVE=new Set(['bool','int64','float64','utf8']);
  const FORMAT_NAMES={table:'固定表',case:'Case 描述',text:'文本',code:'代码',md:'Markdown',json:'JSON',tag:'标签',html:'HTML',message:'消息',dialogue:'对话',image:'图片',audio:'音频',video:'视频'};
  const formatLabel=rep=>FORMAT_NAMES[rep.format]||rep.format||'输出';
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
  function toScalar(value){
    if(value===null||value===undefined)return null;
    if(typeof value==='boolean'||typeof value==='string')return value;
    if(typeof value==='number')return Number.isFinite(value)?value:String(value);
    if(typeof value==='object'&&value.type==='float'&&typeof value.value==='string')return value.value;
    return JSON.stringify(value);
  }
  async function readTableSlice(handle,{project,executionId,outputId,outputIndex,artifactDigest,api,signal,isCurrent=()=>true}={}){
    if(typeof api!=='function')throw Error('未提供分析读取接口；保留固定来源表跳转。');
    if(!handle||typeof handle!=='object'||typeof handle.id!=='string'||!Number.isSafeInteger(handle.generation)||handle.generation<1||!sha(handle.digest))throw Error('此表输出未携带可内联读取的固定 handle；保留固定来源表跳转。');
    const current=()=>!signal?.aborted&&isCurrent();
    const source=await api('analysis-source-from-handle',{}, {handle:{id:handle.id,generation:handle.generation,digest:handle.digest}},project,{signal});
    if(!current())throw Error('读取已取消。');
    if(!source||!sha(source.digest)||!sha(source.binding)||!Array.isArray(source.schema)||!source.case_key)throw Error('固定来源响应身份无效；保留固定来源表跳转。');
    const selected=source.schema.filter(column=>PRIMITIVE.has(column?.type)).slice(0,16);
    if(!selected.length)throw Error('当前固定表没有可呈现的基本类型列；保留固定来源表跳转。');
    const columns=selected.map(column=>column.name);
    const descriptor={version:1,source_snapshot:source.digest,case_key:source.case_key,fields:selected.map((column,index)=>({id:'col'+index,field:column.name,label:column.name,role:'context',format:'text'})),grouping:[],filters:null,sort:[],layout:'card',questions:[]};
    const view={id:null,revision:0,descriptor};
    const query={kind:'cases',columns:[...columns],grouping:[],filters:null,sort:[],scope:'sample',limit:20};
    const page=await api('analysis-case-page',{}, {digest:source.digest,query,view},project,{signal});
    if(!current())throw Error('读取已取消。');
    if(!page||page.source_snapshot!==source.digest||page.kind!=='cases'||!Array.isArray(page.rows)||page.rows.length>20||!Array.isArray(page.columns)||page.columns.join('\u0000')!==columns.join('\u0000')||!page.coverage)throw Error('Case 页与固定来源或投影不一致；保留固定来源表跳转。');
    const forgeCase=globalThis.ForgeCase;
    if(!forgeCase||typeof forgeCase.hash!=='function')throw Error('Case 摘要组件未加载；保留固定来源表跳转。');
    const viewDigest=await forgeCase.hash(view);
    const queryDigest=await forgeCase.hash({source:source.digest,view:viewDigest,query});
    if(!current())throw Error('读取已取消。');
    if(page.view_digest!==viewDigest||page.query_digest!==queryDigest||!sha(page.scope_digest)||!Number.isSafeInteger(page.annotation_watermark)||page.annotation_watermark<0)throw Error('Case 页视图/查询/快照依据不匹配；保留固定来源表跳转。');
    const rows=page.rows.map(row=>{const record={};for(const name of columns)record[name]=toScalar(row[name]);return record;});
    const slice={columns:selected.map(column=>({name:column.name,type:column.type})),rows,
      coverage:{label:'当前输出的前20行预览',complete:page.coverage.complete===true,totalRows:Number.isInteger(page.coverage.total_rows)?page.coverage.total_rows:null},
      identity:{project,binding:source.binding,artifactDigest,executionId,outputId,outputIndex,sourceSnapshot:source.digest}};
    if(bytesOf({columns:slice.columns,rows:slice.rows})>256*1024)throw Error('内联表切片超过 256KiB 预算；保留固定来源表跳转。');
    return slice;
  }
  function mount({parent,output,project,mountText,preview,resolveMedia,disposeReaders,isCurrent=()=>parent.isConnected,onCase,onTable,api}){
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
      const select=node('select');select.setAttribute('aria-label','输出 '+item.index+' 格式');bar.append(select);
      for(const [index,rep]of representations.entries()){const option=node('option',formatLabel(rep)+(rep.available===false?' · 不可用/原值':''));option.value=index;option.title=rep.mime;select.append(option);}
      select.value=Math.max(0,representations.findIndex(rep=>rep.available!==false));let field=null,releaseInline=null;
      const paint=()=>{
        field?.dispose();field=null;releaseInline?.();releaseInline=null;
        render()?.disposeWithin(host);disposeReaders?.(host);host.replaceChildren();
        const rep=representations[Number(select.value)];if(!rep)return;
        if(rep.truncated||rep.available===false)host.append(node('p',(rep.truncated?'仅有界预览；':'')+(rep.reason||'当前格式不可用')+' · 原始值保留于固定 artifact '+output.artifact.id+' / 输出 '+item.index,'muted small'));
        if(rep.format==='case'&&rep.available!==false){
          const button=node('button','在 Case 工作台查看此描述');button.type='button';button.disabled=typeof onCase!=='function';host.append(button);
          host.append(node('p','这是 Notebook 产生的展示描述；保存的视图与评审答案由各自版本管理。','muted small'));
          const evidence=caseEvidence(rep,output,item.index);
          if(!evidence.annotation_origin_registered)host.append(node('p','此输出尚未登记评审来源，可查看描述。','muted small'));
          button.onclick=()=>{if(current()&&button.isConnected&&host.contains(button))onCase(structuredClone(rep.value),{...structuredClone(evidence),provenance:structuredClone(p),output_index:item.index});};
          field=render()?.field({parent:host,format:'json',value:rep.value,isCurrent:current});
        }else if(rep.format==='table'&&rep.available!==false){
          const button=node('button','查看固定来源表');button.type='button';button.disabled=typeof onTable!=='function';host.append(button);
          host.append(node('p','这是 Notebook 产生的展示描述；保存的视图与评审答案由各自版本管理。','muted small'));
          button.onclick=()=>{if(current()&&button.isConnected&&host.contains(button))onTable(structuredClone(rep.value),{origin:null,definition_witness:null,annotation_origin_registered:false,provenance:structuredClone(p),output_index:item.index});};
          const handle=rep.value,inlineHost=node('div',undefined,'analysis-output-inline');host.append(inlineHost);
          if(typeof api==='function'&&handle&&typeof handle==='object'&&typeof handle.id==='string'&&globalThis.ForgeAnalysisCharts){
            const readButton=node('button','内联预览前20行（表格/图表）');readButton.type='button';host.append(readButton);
            const inline={abort:null,chart:null};
            const release=()=>{inline.abort?.abort();inline.abort=null;inline.chart?.dispose();inline.chart=null;inlineHost.replaceChildren();};
            releaseInline=release;owned.add({dispose:release});
            readButton.onclick=async()=>{
              if(!current())return;release();
              inline.abort=new AbortController();readButton.disabled=true;
              inlineHost.append(node('p','正在沿固定来源读取前20行…','muted small'));
              try{
                const slice=await readTableSlice(handle,{project,executionId:p.execution_id,outputId:output.artifact.id,outputIndex:item.index,artifactDigest:output.artifact.digest,api,signal:inline.abort.signal,isCurrent:current});
                if(!current())return;
                inlineHost.replaceChildren();
                const numeric=slice.columns.filter(column=>column.type==='int64'||column.type==='float64');
                inline.chart=globalThis.ForgeAnalysisCharts.mount({parent:inlineHost,slice,spec:{kind:'table',x:slice.columns[0]?.name||null,y:numeric[0]?.name||null},isCurrent:current});
                owned.add(inline.chart);
              }catch(error){if(current())inlineHost.replaceChildren(node('p',error.message,'error-box'));}
              finally{if(current())readButton.disabled=false;}
            };
          }else host.append(node('p','此表输出未携带可内联读取的固定 handle；可使用上方固定来源表跳转。','muted small'));
          field=render()?.field({parent:host,format:'json',value:rep.value,isCurrent:current});
        }else if(typeof rep.raw_json==='string'){host.append(node('p','JSON 整数精确原文（浏览器数值范围外）','muted small'));field=render()?.field({parent:host,format:'code',language:'json',value:rep.raw_json,isCurrent:current});}
        else if(Object.hasOwn(rep,'value')){
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
        currentMount=mount({parent:content,output:value,project,api,mountText,disposeReaders,resolveMedia,onCase,onTable,isCurrent:()=>current(ticket),preview:(format,value)=>api('analysis-field-preview',{}, {format,value},project)});
        const controls=node('div',undefined,'analysis-output-controls');content.append(controls);
        for(const [label,next]of [['上一页',Math.max(0,offset-8)],['下一页',value.next_offset]]){const button=node('button',label);button.type='button';button.disabled=label==='上一页'?offset===0:next===null;button.onclick=()=>load(next);controls.append(button);}
      }catch(error){if(current(ticket)){const failure=node('p',error.message,'error-box');content.append(failure);const retry=node('button','重试读取输出');retry.type='button';retry.onclick=()=>{failure.remove();retry.remove();load(offset);};content.append(retry);}}
      finally{busy=false;if(current(ticket))content.removeAttribute('aria-busy');}
    }
    root.ontoggle=()=>{if(!root.open){epoch++;abort.abort();currentMount?.dispose();currentMount=null;render()?.disposeWithin(content);disposeReaders?.(content);content.replaceChildren();page=null;busy=false;}else{abort=new AbortController();load();}};
    return {element:root,dispose(){if(dead)return;dead=true;epoch++;abort.abort();root.ontoggle=null;currentMount?.dispose();render()?.disposeWithin(content);disposeReaders?.(content);content.replaceChildren();}};
  }
  globalThis.ForgeAnalysisOutput=Object.freeze({mount,mountExecution,readTableSlice});
})();