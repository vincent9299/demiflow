/* One source Notebook; no code runs on navigation, editing or restoration. */
(() => {
  'use strict';
  const $=(tag,text,kind)=>{const node=document.createElement(tag);if(text!==undefined)node.textContent=text;if(kind)node.className=kind;return node;};
  const uid=prefix=>prefix+crypto.randomUUID().replaceAll('-','');
  const same=(a,b)=>JSON.stringify(a)===JSON.stringify(b);
  const localID=id=>/^d[0-9a-f]{32}$/.test(id);
  const names={admitted:'准备数据',starting:'启动中',idle:'就绪',executing:'运行中',stop_requested:'停止中',cleanup_pending:'清理中',stopped:'已停止',queued:'排队',dispatched:'已派发',running:'运行中',finished:'已结束',succeeded:'完成',error:'代码错误',unknown:'结果待核对',cancelled:'已取消',timeout:'超时'};
  const executionLabel=item=>names[item.state==='finished'?item.status:item.state]||names[item.state]||item.state;
  const helpers="from demiflow.workbench.analysis.client import source, table, sql, publish, case_view";
  const modeName=mode=>mode==='trusted_local'?'受信开发模式':mode==='oci'?'隔离执行模式':'运行模式未记录';
  const sha256=async text=>[...new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(text)))].map(value=>value.toString(16).padStart(2,'0')).join('');
  const rejectedCode=code=>['conflict','busy','resource_limit','operation_rejected','forbidden','unauthorized','not_found'].includes(code);
  function mount({parent,api,project,connected,permissions={},features={},executionProfile=null,mountText,disposeReaders,media,fieldMedia,resolveMedia,caseResolveMedia,authorizeURL,onCase,onTable,onCaseContext,toolbarHost=null}) {
    const S=ForgeAnalysisState,store=new S.Store(),root=$('section',undefined,'analysis-notebook');parent.append(root);
    const cases=connected&&globalThis.ForgeCase?ForgeCase.mount({parent,api,project,connected,permissions,features,mountText,disposeReaders,media,fieldMedia,resolveMedia:caseResolveMedia,authorizeURL,onContext:onCaseContext,toolbarHost:toolbarHost?.case}):null;
    const bar=$('div',undefined,'analysis-bar'),notice=$('div',undefined,'analysis-notice'),pending=$('div',undefined,'analysis-pending');
    const actions=$('div',undefined,'analysis-toolbar-actions');(toolbarHost?.notebook||bar).append(actions);
    const sourceBox=$('details',undefined,'analysis-sources'),kernel=$('div',undefined,'analysis-kernel'),body=$('div',undefined,'analysis-cells'),history=$('details',undefined,'analysis-history');
    const context=$('div',undefined,'analysis-context'),more=$('details',undefined,'analysis-file-actions'),fileActions=$('div',undefined,'analysis-file-controls');more.append($('summary','导入 / 导出'),fileActions);context.append(sourceBox,kernel);root.append(bar,notice,pending,context,body,history);
    const historySummary=$('summary','单元执行记录'),historySelectors=$('div',undefined,'analysis-cell-tools analysis-history-selectors'),historyRows=$('div'),historyControls=$('div',undefined,'analysis-cell-tools'),historyOutput=$('div');
    sourceBox.append($('summary','Notebook 数据源'));history.append(historySummary,historySelectors,historyRows,historyControls,historyOutput);
    let disposed=false,epoch=0,busy=false,draftTimer=null,pollTimer=null,polling=false,currentSource=null,sourceEpoch=0,sourceAlias='data',sourceProjection='';
    const outputMounts=new Map(),explicitExecutions=new Set(),cellNodes=new Map(),runChains=new Set();
    let originalSourcesRun=false;
    function clearNotebookOutputs(){for(const item of outputMounts.values()){item.handle.dispose();item.handle.element.remove();}outputMounts.clear();globalThis.ForgeAnalysisRender?.disposeWithin(body);}
    function paintOutput(section,recent){
      const key=section.dataset.analysisCell,old=outputMounts.get(key),identity=recent?.output_id?recent.id+'@'+recent.generation+'@'+recent.output_id:null;
      if(old?.identity===identity&&old.handle.element.parentElement===section)return;
      if(old){old.handle.dispose();old.handle.element.remove();outputMounts.delete(key);}
      if(!identity||!globalThis.ForgeAnalysisOutput||root.hidden)return;
      const outputEpoch=epoch,handle=ForgeAnalysisOutput.mountExecution({parent:section,execution:recent,project,api,mountText,disposeReaders,resolveMedia,onCase,onTable,
        isCurrent:()=>!disposed&&epoch===outputEpoch&&!root.hidden&&section.isConnected});
      outputMounts.set(key,{identity,handle});
      // Only the result of an explicit run in this editor opens inline. Restored
      // executions stay closed; the renderer still requires a media preview click.
      if(explicitExecutions.delete(recent.id))handle.element.open=true;
    }
    let touched=false,discardForLeave=false;
    let identifier=uid('d'),revision=0,base=null,draft={title:'未命名 Notebook',cells:[{id:uid('c'),language:'python',source:'# Python 与 SQL 单元共享同一内核\n'+helpers+'\nprint("Hello, DemiFlow")'}]},bindings={},session=null,executions=[];
    let historyItems=[],historyAfter=0,historyNext=null;
    let historyScope=null,historySessions=[],historyGenerations=[],historyRequest=0;
    const abort=new AbortController();
    const live=ticket=>!disposed&&ticket===epoch&&root.isConnected;
    function info(text,error=false){notice.replaceChildren($('p',text,error?'error-box':'muted small'));}
    function action(label,run,parent,disabled=false){const button=$('button',label);button.type='button';button.disabled=disabled;button.onclick=()=>Promise.resolve().then(run).catch(error=>{if(!disposed&&error.name!=='AbortError')info(error.message,true);});parent.append(button);return button;}
    function input(label,value,parent,maximum=256){const field=$('label',label),element=$('input');element.value=value;element.maxLength=maximum;element.setAttribute('aria-label',label);field.append(element);parent.append(field);return element;}
    const read=(name,query={})=>api('analysis-'+name,query,undefined,project,{signal:abort.signal});
    const dirty=()=>!base||!same(base,draft);
    const beforeUnload=event=>{if(!flushDraft()||store.hasVolatileSources()){event.preventDefault();event.returnValue='';}};
    globalThis.addEventListener('beforeunload',beforeUnload);
    function flushDraft(){
      clearTimeout(draftTimer);draftTimer=null;
      if(discardForLeave)return true;
      if(!touched&&!session&&!Object.keys(bindings).length)return true;
      try{
        const reference={bindings,session:session?{id:session.id,generation:session.generation}:null};
        // Source + original revision/bindings/session are captured together
        // before any optional small-reference bookkeeping or view departure.
        store.saveDraft(project,identifier,{revision,document:S.clone(draft),...reference});
        let referenceCaptured=true;try{store.saveReference(project,identifier,reference);}catch{referenceCaptured=false;}
        if(base&&!dirty()&&referenceCaptured)store.removeDraft(project,identifier);
        persistence.textContent=store.hasVolatileSources()?'本窗口暂存 · 重开前请导出':'';
        discardButton.hidden=true;
        return true;
      }
      catch(error){discardButton.hidden=false;info('草稿未能安全暂存，已保留原编辑内容并暂停离开：'+error.message+'。可先导出草稿，或明确放弃当前未保存编辑。',true);return false;}
    }
    function changed(){touched=true;discardForLeave=false;status.textContent=dirty()?'有未保存修改':'已保存 · v'+revision;clearTimeout(draftTimer);draftTimer=setTimeout(flushDraft,250);}
    const picker=$('select');picker.setAttribute('aria-label','选择 Notebook');const localOption=$('option','本地草稿');localOption.value=identifier;picker.append(localOption);bar.append(picker);
    const title=input('Notebook 名称',draft.title,bar),status=$('span','本地草稿','muted small');bar.append(status);
    const persistence=$('span',store.hasVolatileSources()?'本窗口暂存 · 重开前请导出':'','muted small');bar.append(persistence);
    if(!connected)bar.append($('span','分析环境未接入','muted small'));
    title.oninput=()=>{draft.title=title.value;changed();};
    const canWrite=()=>connected&&!!permissions.analysis_write;
    async function refreshList(){
      const ticket=epoch,value=connected?await read('documents',{kind:'notebook',limit:32}):{items:[]};if(!live(ticket))return;
      picker.replaceChildren();const locals=store.drafts(project).filter(item=>localID(item.id));
      if(localID(identifier)&&!locals.some(item=>item.id===identifier))locals.push({id:identifier,document:draft});
      for(const item of locals){const local=$('option','草稿 · '+item.document.title);local.value=item.id;picker.append(local);}
      for(const item of value.items){const option=$('option',(item.title||item.id)+' · v'+item.revision);option.value=item.id;picker.append(option);}
      picker.value=identifier;
    }
    async function loadDocument(id,wantedRevision){
      const cells=[];let offset=0,header=null;
      for(let page=0;page<64;page++){
        const value=await read('notebook',{notebook_id:id,...(wantedRevision?{revision:wantedRevision}:{}),offset,limit:8});
        if(header&&value.revision!==header.revision)throw new Error('Notebook 版本已变化，请重新读取');
        header=value;wantedRevision=value.revision;cells.push(...value.document.cells);
        if(cells.length>64)throw new Error('Notebook 单元数量超限');
        if(value.next_offset===null)return {...header,document:S.document({title:header.document.title,cells})};
        if(!Number.isInteger(value.next_offset)||value.next_offset<=offset)throw new Error('Notebook 翻页标识无效');
        offset=value.next_offset;
      }
      throw new Error('Notebook 翻页超过上限');
    }
    async function select(id,wantedRevision){
      if(!flushDraft()){picker.value=identifier;return;}const ticket=++epoch;
      const saved=store.draft(project,id),reference=saved||store.reference(project,id);let loaded=null;
      if(!localID(id))loaded=await loadDocument(id,wantedRevision||(saved?.revision||undefined));
      if(!live(ticket))return;
      identifier=id;revision=loaded?.revision||0;base=loaded?.document||null;
      touched=!!saved;discardForLeave=false;
      draft=S.clone(saved?.document||base||{title:'未命名 Notebook',cells:[]});bindings=S.clone(reference?.bindings||{});
      session=reference?.session||null;executions=[];historyItems=[];historyAfter=0;historyNext=null;historyScope=null;historySessions=[];historyGenerations=[];historyRequest++;originalSourcesRun=false;title.value=draft.title;
      paint();info(saved?'已恢复该 Notebook 的本地草稿；代码未执行。':'已读取源单元，代码未执行。');if(session&&connected)await poll();
    }
    picker.onchange=()=>{select(picker.value).catch(error=>info(error.message,true));};
    async function mutate(operation,payload,scope){
      if(busy){const error=new Error('当前操作正在提交，请先等待或核对原请求');error.code='busy';error.operationStarted=false;throw error;}
      payload={request_id:uid('u'),...payload};let intent;
      try{intent=store.prepare(project,operation,payload,scope);}catch(error){error.operationStarted=false;throw error;}
      const ticket=epoch;
      busy=true;paintPending();
      try{
        const value=await api('analysis-'+operation,{},payload,project);
        if(live(ticket))store.finish(project,payload.request_id);return {value,intent};
      }catch(error){
        // A transport failure/conflict does not invent a second request ID or
        // discard source. Reconciliation is an explicit read of this receipt.
        const wrapped=new Error(error.message+'；原请求 '+payload.request_id+' 已保留，可在待核对操作中检查。');
        wrapped.code=error.code;wrapped.operationStarted=error.operationStarted;wrapped.requestId=payload.request_id;
        throw wrapped;
      }finally{busy=false;if(!disposed)paintPending();}
    }
    function acknowledge(intent,value){
      if(intent.scope.notebook!==identifier)return;
      if(intent.operation==='notebook-create'){
        const previous=identifier;identifier=value.id;revision=value.revision;base=S.clone(intent.payload.document);
        store.removeDraft(project,previous);flushDraft();
      }else if(intent.operation==='notebook-patch'){
        if(revision!==intent.payload.expected_revision)throw new Error('保存已确认，当前草稿基线不同；请重新选择该 Notebook 核对版本');
        base=S.apply(base,intent.payload.patch);revision=value.revision;flushDraft();
      }else if(intent.operation==='session-start'||intent.operation==='session-control'){
        session=value;flushDraft();
      }
      paint();
    }
    function paintPending(){
      pending.replaceChildren();let items=[];
      try{items=store.pending(project).filter(item=>item.scope.kind!=='case');}catch(error){info(error.message,true);return;}
      for(const intent of items){
        const line=$('div',undefined,'analysis-pending-row');line.append($('span','待核对 · '+intent.operation+' · '+intent.payload.request_id,'muted small'));pending.append(line);
        action('核对原请求',async()=>{
          const value=await read('receipt',{request_id:intent.payload.request_id});
          if(disposed)return;acknowledge(intent,value);store.finish(project,intent.payload.request_id);paintPending();
          info('原操作已确认；代码未自动执行。');
        },line,busy||!connected);
        action('以原请求重试',async()=>{
          if(busy)return;store.durableIntents();busy=true;
          try{const value=await api('analysis-'+intent.operation,{},intent.payload,project);
            if(disposed)return;acknowledge(intent,value);store.finish(project,intent.payload.request_id);paintPending();info('原请求已确认，未自动重放单元执行。');
          }finally{busy=false;}
        },line,busy||!connected);
      }
    }
    async function save(onlyCell=null){
      if(!canWrite())throw new Error('保存 Notebook 需要项目保存权限和已接入的分析环境');
      const ticket=epoch,notebookId=identifier,startRevision=revision;
      S.document(draft);let operation,payload;
      if(!base){operation='notebook-create';let document=S.clone(draft);try{S.bounded(document,56*1024);}catch{document={title:draft.title,cells:[]};}
        payload={document};
      }else{const change=S.patch(base,draft,onlyCell);if(!change){info('所选内容已保存。');return {kind:'saved',notebookId,revision};}operation='notebook-patch';payload={notebook_id:identifier,expected_revision:revision,patch:change};}
      let value,intent;
      try{({value,intent}=await mutate(operation,payload,{notebook:identifier}));}
      catch(error){
        if(!disposed)info(error.message,true);
        const rejected=error.code==='conflict'||error.operationStarted===false||rejectedCode(error.code);
        return {kind:rejected?'conflict':'pending',notebookId,revision:startRevision};
      }
      if(live(ticket)){acknowledge(intent,value);await refreshList();info(dirty()?'保存已确认，仍有未保存单元；可逐单元保存。':'已保存为 v'+revision+'；未执行代码。');return {kind:'saved',notebookId,revision};}
      return {kind:'pending',notebookId,revision:startRevision};
    }
    const saveNotebookButton=action('保存 Notebook',()=>save(),actions,!canWrite());saveNotebookButton.classList.add('primary');
    action('新建',()=>{
      if(!flushDraft())return;const next=uid('d'),document={title:'未命名 Notebook',cells:[]};
      store.saveDraft(project,next,{revision:0,document,bindings:{},session:null});
      epoch++;identifier=next;revision=0;base=null;draft=document;bindings={};session=null;executions=[];touched=true;discardForLeave=false;originalSourcesRun=false;title.value=draft.title;
      paint();flushDraft();refreshList().catch(error=>info(error.message,true));
    },actions);
    actions.append(more);
    action('导出 .ipynb',()=>{
      const raw=S.bounded(S.exported(draft),2*1024*1024),blob=new Blob([raw],{type:'application/x-ipynb+json'}),url=URL.createObjectURL(blob),anchor=$('a');
      anchor.href=url;anchor.download='analysis.ipynb';anchor.click();setTimeout(()=>URL.revokeObjectURL(url),1000);
    },fileActions);
    action('导出草稿',()=>{
      const raw=S.bounded({format:'demiflow_notebook_draft_v1',project,notebook_id:identifier,base_revision:revision,
        document:S.draftDocument(draft),bindings,session:session?{id:session.id,generation:session.generation}:null},2*1024*1024+16384);
      const url=URL.createObjectURL(new Blob([raw],{type:'application/json'})),anchor=$('a');anchor.href=url;anchor.download='notebook-draft.json';anchor.click();setTimeout(()=>URL.revokeObjectURL(url),1000);
    },fileActions);
    const discardButton=action('放弃当前未保存编辑',()=>{
      discardForLeave=true;touched=false;clearTimeout(draftTimer);draftTimer=null;
      try{store.removeDraft(project,identifier);}catch{ /* preserve unknown storage; only this editor is abandoned */ }
      draft=S.clone(base||{title:'未命名 Notebook',cells:[]});discardButton.hidden=true;paint();
      info('已按你的选择放弃当前未保存编辑；已保存的 Notebook、其他草稿和运行记录保留。');
    },actions);discardButton.hidden=true;
    const file=$('input');file.type='file';file.accept='.ipynb,.json,application/json';file.hidden=true;bar.append(file);
    action('导入源单元',()=>file.click(),fileActions);
    file.onchange=async()=>{
      const ticket=epoch;
      try{const selected=file.files?.[0];if(!selected)return;if(selected.size>2*1024*1024+16384)throw new Error('Notebook 文件超过导入字节上限');
        const value=JSON.parse(await selected.text()),rawDraft=value?.format==='demiflow_notebook_draft_v1';
        const imported=rawDraft?S.draftDocument(value.document):S.imported(value,selected.name.replace(/\.ipynb$/i,'').slice(0,80)||'Imported Notebook');
        if(!live(ticket)||!flushDraft())return;
        const next=uid('d');store.saveDraft(project,next,{revision:0,document:imported,bindings:{},session:null});
        epoch++;identifier=next;revision=0;base=null;draft=imported;bindings={};session=null;executions=[];touched=true;discardForLeave=false;originalSourcesRun=false;title.value=draft.title;paint();flushDraft();refreshList().catch(error=>info(error.message,true));info(rawDraft?'已恢复草稿源码；原项目、版本和绑定保留在草稿文件中，需明确核对后重新绑定；未接管内核。':'已导入源单元；原输出和内核状态未导入，未执行代码。');
      }catch(error){info(error.message,true);}finally{file.value='';}
    };
    function paintSources(){
      sourceBox.replaceChildren($('summary','Notebook 数据源 · '+Object.keys(bindings).length));
      const line=$('div',undefined,'analysis-source-row');sourceBox.append(line);
      const alias=input('内核中的表名',sourceAlias,line,80);alias.oninput=()=>{sourceAlias=alias.value;};
      const projection=input('字段投影',sourceProjection,line,8192);projection.oninput=()=>{sourceProjection=projection.value;};projection.parentElement.classList.add('analysis-source-projection');
      projection.placeholder=currentSource?.locator?.columns?.join(', ')||'留空沿用目录字段；多个字段以逗号分隔';
      projection.title='留空沿用当前目录投影；也可显式填写字段或结构体叶字段，例如 case_id, a_evaluation.score。';
      line.append($('span',currentSource?.label||'在数据目录选择固定版本后，可添加为内核数据源。','muted small'));
      sourceBox.append($('p',"同一表可添加不同字段投影。Python 可通过 source('data').sample()['rows'] 读取复杂字段；SQL 使用基本类型字段。",'muted small'));
      const example=$('details');example.append($('summary','SQL 与 Python 互用示例'),$('pre',helpers+"\n\n# 当前绑定表的有界 JSON 样本，保留消息等复杂字段\nrows = source('data').sample(limit=20)['rows']\n\n# Python 生成的表，显式命名后可在后续 SQL 单元查询\nimport pyarrow as pa\nnumbers = publish(pa.table({'value': [1, 2, 3]})).bind('numbers')\n# SQL 单元：SELECT SUM(value) FROM numbers\n\n# SQL 单元的结果变量可在后续 Python 单元使用\nresult = sql('SELECT * FROM data LIMIT 20')\nresult.to_pandas()\n\n# 替换已生成的同名表时，明确给出旧值；原数据源名称保留\nnew_numbers = publish(pa.table({'value': [4, 5]}))\nnumbers = new_numbers.bind('numbers', expected=numbers)"));sourceBox.append(example);
      const sourceTicket=sourceEpoch;
      action('添加当前表',async()=>{
        if(!connected||!currentSource)throw new Error('请先选择固定版本数据源');
        if(!/^[A-Za-z_][A-Za-z0-9_]{0,79}$/.test(alias.value))throw new Error('内核表名需要普通标识符');
        if(!Object.hasOwn(bindings,alias.value)&&Object.keys(bindings).length>=16)throw new Error('一本 Notebook 最多绑定 16 个来源');
        const locator=S.clone(currentSource.locator),ticket=epoch,name=alias.value,selectedProjection=projection.value;
        const columns=selectedProjection.trim()?selectedProjection.split(',').map(item=>item.trim()):locator.columns;
        if(!Array.isArray(columns)||!columns.length||columns.length>32||new Set(columns).size!==columns.length||columns.some(item=>typeof item!=='string'||!item.trim()||item.includes('\0')||S.bytes(item)>256))throw new Error('请选择 1–32 个互不重复的字段，每个字段最多 256 字节');
        S.bounded(columns,16384);locator.columns=[...columns];
        const value=await api('analysis-source-register',{}, {locator},project);
        if(!live(ticket)||sourceTicket!==sourceEpoch||name!==alias.value||selectedProjection!==projection.value)throw new Error('目录、字段投影或表名已变化，来源未加入当前草稿，请重新选择');
        bindings={...bindings,[name]:value.digest};flushDraft();paintSources();info('已绑定固定来源；已启动的内核仍保持原来源，需停止后显式启动新内核才使用变更。');
      },line,!connected||!currentSource);
      for(const [name,digest]of Object.entries(bindings)){
        const item=$('div',undefined,'analysis-source-row');item.append($('code',name),$('span',digest,'muted small'));sourceBox.append(item);
        action('移除绑定',()=>{delete bindings[name];flushDraft();paintSources();},item);
      }
      if(session?.sources)sourceBox.append($('p','当前内核固定来源：'+Object.entries(session.sources).map(([name,digest])=>name+' · '+digest.slice(0,12)).join('；'),'muted small'));
    }
    function paintKernel(){
      const liveKernel=!!session&&session.state!=='stopped';
      const stateLabel=session?(names[session.state]||session.state||'读取中'):'内核未启动';
      const kernelStatus=$('span','Python / SQL · '+stateLabel,'muted small');
      if(session)kernelStatus.title='内核 '+session.id+' · generation '+session.generation;
      kernel.replaceChildren(kernelStatus);
      if(session?.reason)kernel.append($('span','停止原因：'+session.reason,'muted small'));
      const mismatch=liveKernel&&!same(session.sources||{},bindings);
      if(mismatch){
        kernel.append($('span','内核固定来源与当前编辑器绑定不同；运行默认停止，活内核未被改变。','muted small'));
        action('继续原内核运行',()=>{originalSourcesRun=true;info('下一次运行将沿用原内核固定来源；如需使用新绑定，请先停止内核再运行。');paintKernel();},kernel,!permissions.analysis_execute);
      }
      if(executionProfile?.mode){
        const profile=$('details',undefined,'analysis-execution-profile');profile.append($('summary','当前读取环境 · '+modeName(executionProfile.mode)));
        profile.append($('p',executionProfile.mode==='trusted_local'?'使用当前容器账号权限；停止与清理覆盖本服务管理的进程、通道和临时文件。':'使用已接入的隔离执行环境。','muted small'));
        profile.append($('pre',JSON.stringify(executionProfile,null,2)));kernel.append(profile);
      }
      if(session?.environment){const original=$('details',undefined,'analysis-generation-environment');original.append($('summary','本代运行来源 · '+modeName(session.environment.body?.mode)),$('pre',JSON.stringify(session.environment,null,2)));kernel.append(original);}
      const start=action(liveKernel?'启动内核':'启动并运行内核',async()=>{
        if(dirty()||!base)throw new Error('请先保存 Notebook，再显式启动内核');
        const ticket=epoch,{value,intent}=await mutate('session-start',{notebook_id:identifier,revision,sources:S.clone(bindings)},{notebook:identifier});
        if(live(ticket)){acknowledge(intent,value);info('内核已受理，准备数据后可运行单元。');}
      },kernel,!connected||!permissions.analysis_execute||!base||liveKernel);
      start.hidden=liveKernel;
      const stop=action('立即停止内核',async()=>{
        if(!session||session.state==='stopped')return;
        const original={id:session.id,generation:session.generation},ticket=epoch;
        const value=await api('analysis-stop-generation',{}, {request_id:uid('u'),session_id:original.id,generation:original.generation},project);
        if(live(ticket)&&session?.id===original.id&&session?.generation===original.generation){info(value.accepted?(value.execution_mode==='trusted_local'?'停止已受理；正在核对受管进程、通道和临时文件清理。':'停止已受理；正在核对进程、挂载和文件清理。'):'停止尚未确认。');await poll();}
      },kernel,!connected||!permissions.analysis_execute||!session||session.state==='stopped');
      stop.hidden=!session||session.state==='stopped';
      stop.title='停止本代内核并清理其进程、通道与临时文件；不影响已保存 Notebook 与历史输出。';
    }
    function paintExecutionStatus(section,recent){
      if(!recent)return;
      const status=$('div',undefined,'analysis-cell-status muted small'),basis=$('details');
      status.append($('span',executionLabel(recent)+' · 已保存代码 v'+recent.notebook_revision));
      basis.append($('summary','执行依据'),$('pre','执行 '+recent.id+'\n内核代次 '+recent.generation+'\n代码 SHA256 '+recent.source_sha256+(recent.output_id?'\n输出 '+recent.output_id:'')));
      basis.append($('p','该次执行来源 · '+modeName(recent.environment?.mode)+(recent.environment?.digest?' · '+recent.environment.digest.slice(0,12):''),'muted small'));
      let loaded=false;basis.ontoggle=async()=>{if(!basis.open||loaded)return;loaded=true;const ticket=epoch;try{
        const value=await read('execution-environment',{execution_id:recent.id});if(!live(ticket)||!basis.isConnected)return;
        if(value.execution_id!==recent.id||value.generation!==recent.generation)throw Error('执行来源与原执行身份不一致');
        basis.append(value.environment?$('pre',JSON.stringify(value.environment,null,2)):$('p','该次执行未记录环境来源；按原历史保留。','muted small'));
      }catch(error){if(live(ticket)&&basis.isConnected){loaded=false;basis.append($('p',error.message,'error-box'));}}};
      status.append(basis);section.append(status);
    }
    const submittableStates=['admitted','starting','idle','executing'];
    function runFailure(error){
      const rejected=error.operationStarted===false||rejectedCode(error.code);
      if(rejected)return {kind:'blocked',requestId:error.requestId||null,executionId:null,message:error.message};
      if(error.requestId)return {kind:'pending',requestId:error.requestId,executionId:null,message:error.message};
      return {kind:'blocked',requestId:null,executionId:null,message:error.message};
    }
    async function ensureSessionForRun(capture){
      if(!live(capture.epoch)||identifier!==capture.notebookId)return {kind:'blocked',requestId:null,message:'Notebook 已切换，运行链停止。'};
      if(session&&session.state!=='stopped'){
        if(!submittableStates.includes(session.state))return {kind:'blocked',requestId:null,message:'内核正在停止或清理（'+(names[session.state]||session.state)+'），完成后可重新运行。'};
        if(!same(session.sources||{},capture.sources)){
          if(originalSourcesRun){originalSourcesRun=false;return {kind:'ready',sessionId:session.id,generation:session.generation};}
          return {kind:'blocked',requestId:null,message:'当前内核固定来源与编辑器绑定不同。可在内核行选择“继续原内核运行”，或停止内核后重新运行以使用新绑定；活内核未被改变。'};
        }
        return {kind:'ready',sessionId:session.id,generation:session.generation};
      }
      if(!connected||!permissions.analysis_execute)return {kind:'blocked',requestId:null,message:'当前环境未授权启动内核；只读页面不会启动。'};
      let value,intent;
      try{({value,intent}=await mutate('session-start',{notebook_id:capture.notebookId,revision:capture.revision,sources:S.clone(capture.sources)},{notebook:capture.notebookId}));}
      catch(error){
        if(!disposed)info(error.message,true);
        const rejected=error.operationStarted===false||rejectedCode(error.code);
        return {kind:rejected?'blocked':'pending',requestId:error.requestId||null,message:error.message};
      }
      if(!live(capture.epoch)||identifier!==capture.notebookId)return {kind:'pending',requestId:intent.payload.request_id,message:'内核启动已发出但页面已切换；请在待核对操作中确认原请求。'};
      acknowledge(intent,value);
      const deadline=Date.now()+30000;
      while(session&&session.id===value.id&&session.generation===value.generation&&!submittableStates.includes(session.state)&&live(capture.epoch)&&Date.now()<deadline){
        await new Promise(resolve=>setTimeout(resolve,1000));
        if(!live(capture.epoch))break;
        await poll();
      }
      if(!live(capture.epoch)||identifier!==capture.notebookId)return {kind:'blocked',requestId:intent.payload.request_id,message:'Notebook 已切换，运行链停止。'};
      if(!session||session.id!==value.id||session.generation!==value.generation)return {kind:'blocked',requestId:intent.payload.request_id,message:'内核代次已变化，未自动重发；请重新运行。'};
      if(!submittableStates.includes(session.state))return {kind:'pending',requestId:intent.payload.request_id,message:'内核仍在准备数据（'+(names[session.state]||session.state)+'）；原启动请求已受理，可稍后重试运行。'};
      return {kind:'ready',sessionId:session.id,generation:session.generation};
    }
    async function requestCellRun(cellId,{advance=false}={}){
      const blocked=message=>({kind:'blocked',requestId:null,executionId:null,message});
      if(runChains.has(cellId))return blocked('该单元已有在途的保存/运行操作；请等待完成或先取消。');
      const cell=draft.cells.find(item=>item.id===cellId);
      if(!cell)return blocked('单元已不存在。');
      if(cell.language==='markdown')return blocked('Markdown 单元仅预览，不执行。');
      if(!connected||!permissions.analysis_execute)return blocked('当前环境未授权执行；只读页面不会启动内核。');
      if(!canWrite())return blocked('运行需要先保存单元；当前没有保存权限或未接入分析环境。');
      let snapshot;
      try{snapshot=S.cell(cell);}catch(error){return blocked(error.message);}
      runChains.add(cellId);
      const chainEpoch=epoch;
      try{
        const cellSha=await sha256(snapshot.source);
        let capture={project,epoch:chainEpoch,notebookId:identifier,revision,cell:snapshot,cellSha,sources:S.clone(bindings),sessionId:session?.id||null,generation:session?.generation??null};
        if(!live(chainEpoch))return {kind:'cancelled',requestId:null,executionId:null,message:'页面已切换，运行链停止。'};
        const savedCell=base?.cells.find(item=>item.id===cellId);
        if(!base||!same(savedCell,snapshot)){
          const outcome=await save(cellId);
          if(outcome.kind==='conflict')return blocked('保存被拒绝或冲突，运行链已停止；草稿保留。');
          if(outcome.kind==='pending')return {kind:'pending',requestId:null,executionId:null,message:'保存回执待核对，运行链已停止；请在待核对操作中确认原请求。'};
          if(!live(chainEpoch))return {kind:'cancelled',requestId:null,executionId:null,message:'保存期间页面已切换，运行链停止。'};
          capture={...capture,notebookId:identifier,revision:outcome.revision};
        }
        const nowCell=draft.cells.find(item=>item.id===cellId);
        if(!nowCell||!same(nowCell,snapshot))return blocked('运行期间单元代码已修改；已保留新编辑，未提交执行。');
        if(!same(base?.cells.find(item=>item.id===cellId),snapshot)||revision!==capture.revision)return blocked('保存基线与捕获内容不一致，未提交执行；请重新运行。');
        const ready=await ensureSessionForRun(capture);
        if(ready.kind!=='ready'){if(!disposed&&ready.message)info(ready.message,ready.kind==='blocked');return {kind:ready.kind,requestId:ready.requestId,executionId:null,message:ready.message};}
        if(!live(chainEpoch)||identifier!==capture.notebookId)return {kind:'cancelled',requestId:null,executionId:null,message:'页面已切换，运行链停止。'};
        const finalCell=draft.cells.find(item=>item.id===cellId);
        if(!finalCell||!same(finalCell,snapshot))return blocked('运行期间单元代码已修改；已保留新编辑，未提交执行。');
        if(revision!==capture.revision||!same(base?.cells.find(item=>item.id===cellId),snapshot))return blocked('保存基线已变化，未提交执行；请重新运行。');
        if(!session||session.id!==ready.sessionId||session.generation!==ready.generation)return blocked('内核代次已变化，未自动重发；请重新运行。');
        let submitted;
        try{submitted=await mutate('cell-submit',{session_id:ready.sessionId,expected_generation:ready.generation,revision:capture.revision,cell_id:cellId,source_sha256:capture.cellSha},{notebook:capture.notebookId,cell_id:cellId});}
        catch(error){if(!disposed)info(error.message,true);return runFailure(error);}
        const requestId=submitted.intent.payload.request_id,executionId=typeof submitted.value?.id==='string'?submitted.value.id:null;
        if(!live(chainEpoch))return {kind:'pending',requestId,executionId,message:'提交已发出但页面已切换；请在待核对操作中确认原请求。'};
        if(executionId){if(explicitExecutions.size>=64)explicitExecutions.delete(explicitExecutions.values().next().value);explicitExecutions.add(executionId);}
        info('单元已受理。');await poll();
        if(advance){const index=draft.cells.findIndex(item=>item.id===cellId),next=draft.cells[index+1];if(next)cellNodes.get(next.id)?.textarea.focus();}
        return {kind:'submitted',requestId,executionId,message:'单元已受理。'};
      }catch(error){
        if(!disposed&&error.name!=='AbortError')info(error.message,true);
        return runFailure(error);
      }finally{runChains.delete(cellId);}
    }
    function insertTemplate({language,source,title}={}){
      if(disposed)return false;
      if(language!=='python'&&language!=='sql')return false;
      if(typeof source!=='string'||!source.length||source.length>16384)return false;
      if(draft.cells.length>=64)return false;
      let body=source;
      if(typeof title==='string'&&title.trim())body=(language==='sql'?'-- ':'# ')+title.trim().replace(/[\r\n]+/g,' ')+'\n'+source;
      if(body.length>16384)return false;
      const cell={id:uid('c'),language,source:body};
      draft.cells.push(cell);changed();
      if(!flushDraft()){draft.cells.pop();changed();flushDraft();return false;}
      paintCells();info('已插入模板单元；未执行代码，未启动内核。');return true;
    }
    const autoRows=area=>{area.rows=Math.min(18,Math.max(4,area.value.split('\n').length+1));};
    function runLabel(value){
      if(value.language==='markdown')return '运行';
      if(!base||!same(base.cells.find(item=>item.id===value.id),value))return '保存并运行';
      if(!session||session.state==='stopped')return '启动并运行';
      return '运行';
    }
    function updateCellExecution(entry){
      const recent=executions.filter(item=>item.cell_id===entry.id).at(-1);
      const identity=recent?recent.id+'@'+recent.generation+'@'+recent.state+'@'+(recent.status||'')+'@'+(recent.output_id||''):null;
      if(entry.statusIdentity!==identity){entry.section.querySelector('.analysis-cell-status')?.remove();entry.statusIdentity=identity;paintExecutionStatus(entry.section,recent);}
      paintOutput(entry.section,recent);
      refreshCellCancel(entry.section,entry.id);
      const cell=draft.cells.find(item=>item.id===entry.id),savedCell=base?.cells.find(item=>item.id===entry.id);
      const stale=!!recent&&!!cell&&cell.source!==(savedCell?.source);
      const marker=entry.section.querySelector('.analysis-cell-stale');
      if(stale&&!marker)entry.section.append($('p','当前代码已编辑；下方输出来自上次保存/运行的代码，未丢失。','analysis-cell-stale muted small'));
      else if(!stale&&marker)marker.remove();
    }
    function createCellNode(id){
      const section=$('article',undefined,'analysis-cell');section.dataset.analysisCell=id;
      const tools=$('div',undefined,'analysis-cell-tools'),language=$('select');language.setAttribute('aria-label','单元语言');
      for(const [key,label]of [['python','Python'],['sql','SQL'],['markdown','Markdown']]){const option=$('option',label);option.value=key;language.append(option);}
      const indexCode=$('code','单元','muted small');
      const report=outcome=>{if(!disposed&&outcome.kind!=='submitted'&&outcome.message)info(outcome.message,outcome.kind==='blocked');};
      const run=action('运行',()=>{requestCellRun(id).then(report);},tools);
      run.classList.add('primary','analysis-cell-run');
      const cancel=action('取消单元',async()=>{
        const recent=executions.filter(item=>item.cell_id===id).at(-1);
        if(!recent||!session)return;
        const original={id:session.id,generation:session.generation},ticket=epoch;
        const reply=await api('analysis-cell-cancel',{}, {request_id:uid('u'),execution_id:recent.id,expected_generation:original.generation},project);
        if(live(ticket)&&session?.id===original.id&&session?.generation===original.generation){info(reply.accepted?(reply.generation_will_stop?'取消已受理；本代内核将停止，清理完成后可重新启动。':'排队单元已取消。'):'取消尚未确认。');await poll();}
      },tools,true);
      cancel.classList.add('analysis-cell-cancel');
      const more=$('details',undefined,'analysis-cell-more');more.append($('summary','更多'));
      action('保存此单元',()=>save(id),more,!canWrite());
      action('删除单元',()=>{draft.cells=draft.cells.filter(item=>item.id!==id);changed();paintCells();},more);
      more.append($('code','单元 '+id,'muted small'));
      tools.append(language,indexCode,run,cancel,more);section.append(tools);
      const textarea=$('textarea');textarea.maxLength=16384;textarea.spellcheck=false;textarea.setAttribute('aria-label','单元代码');section.append(textarea);
      textarea.oninput=()=>{const cur=draft.cells.find(item=>item.id===id);if(!cur)return;cur.source=textarea.value;autoRows(textarea);changed();};
      textarea.onkeydown=event=>{
        if(event.key!=='Enter')return;
        if(event.ctrlKey||event.metaKey){event.preventDefault();requestCellRun(id).then(report);}
        else if(event.shiftKey){event.preventDefault();requestCellRun(id,{advance:true}).then(report);}
      };
      language.onchange=()=>{const cur=draft.cells.find(item=>item.id===id);if(!cur)return;cur.language=language.value;if(cur.language!=='sql')delete cur.result_name;changed();paintCells();};
      return {id,section,tools,language,indexCode,run,cancel,more,textarea,nameInput:null,preview:null,statusIdentity:undefined};
    }
    function updateCellNode(entry,value,index){
      entry.indexCode.textContent='单元 '+(index+1);
      if(entry.language.value!==value.language)entry.language.value=value.language;
      entry.textarea.setAttribute('aria-label',value.language+' 单元代码');
      if(value.language==='sql'){
        if(!entry.nameInput){entry.nameInput=input('结果变量名',value.result_name||'',entry.tools,80);entry.nameInput.placeholder='可留空';entry.nameInput.oninput=()=>{const cur=draft.cells.find(item=>item.id===entry.id);if(!cur)return;if(entry.nameInput.value)cur.result_name=entry.nameInput.value;else delete cur.result_name;changed();};entry.tools.insertBefore(entry.nameInput.closest('label'),entry.run);}
        else if(document.activeElement!==entry.nameInput&&entry.nameInput.value!==(value.result_name||''))entry.nameInput.value=value.result_name||'';
      }else if(entry.nameInput){entry.nameInput.closest('label').remove();entry.nameInput=null;}
      if(document.activeElement!==entry.textarea&&entry.textarea.value!==value.source){entry.textarea.value=value.source;autoRows(entry.textarea);}
      entry.run.textContent=runLabel(value);
      entry.run.disabled=!connected||!permissions.analysis_execute||!canWrite()||value.language==='markdown'||!!session&&['stop_requested','cleanup_pending'].includes(session.state);
      entry.run.title=value.language==='markdown'?'Markdown 单元仅预览，不执行。':'保存当前单元（如需）→ 启动内核（如需）→ 提交执行；运行期间修改代码会停止本次提交。';
      const saveCellButton=entry.more.querySelector('button');if(saveCellButton)saveCellButton.disabled=!canWrite();
      if(value.language==='markdown'){
        if(!entry.preview){entry.preview=$('details');entry.preview.append($('summary','预览 Markdown'));entry.preview.ontoggle=()=>{if(entry.preview.open){disposeReaders(entry.preview);entry.preview.replaceChildren($('summary','预览 Markdown'));const cur=draft.cells.find(item=>item.id===entry.id);mountText(entry.preview,cur?.source||'');}};entry.section.append(entry.preview);}
      }else if(entry.preview){disposeReaders(entry.preview);entry.preview.remove();entry.preview=null;}
      updateCellExecution(entry);
    }
    let addRow=null;
    function paintCells(){
      const seen=new Set();let anchor=null;
      for(const [index,value]of draft.cells.entries()){
        seen.add(value.id);
        let entry=cellNodes.get(value.id);
        if(!entry){entry=createCellNode(value.id);cellNodes.set(value.id,entry);}
        updateCellNode(entry,value,index);
        const expectedNext=anchor?anchor.nextSibling:body.firstChild;
        if(entry.section!==expectedNext)body.insertBefore(entry.section,expectedNext);
        anchor=entry.section;
      }
      for(const [id,entry]of [...cellNodes]){
        if(seen.has(id))continue;
        const old=outputMounts.get(id);if(old){old.handle.dispose();old.handle.element.remove();outputMounts.delete(id);}
        disposeReaders(entry.section);globalThis.ForgeAnalysisRender?.disposeWithin(entry.section);
        entry.section.remove();cellNodes.delete(id);
      }
      if(!addRow){addRow=$('div',undefined,'analysis-cell-tools analysis-cell-add');for(const [language,label]of [['python','＋ Python'],['sql','＋ SQL'],['markdown','＋ Markdown']])action(label,()=>{if(draft.cells.length>=64)throw new Error('每本 Notebook 最多 64 个单元');draft.cells.push({id:uid('c'),language,source:language==='sql'?'SELECT * FROM data LIMIT 20':language==='python'?helpers+'\n':''});changed();paintCells();},addRow);}
      for(const button of addRow.querySelectorAll('button'))button.disabled=draft.cells.length>=64;
      body.append(addRow);
    }
    function paintHistory(){
      const items=historyScope||historyAfter?historyItems:[...executions].reverse();historySummary.textContent='单元执行记录 · '+(historyScope?'所选历史 · ':'')+'当前页 '+items.length;historyRows.replaceChildren();historyControls.replaceChildren();paintHistorySelectors();
      for(const item of items){
        const entry=$('article',undefined,'analysis-execution-history');historyRows.append(entry);entry.append($('p',item.id+' · '+item.cell_id+' · '+executionLabel(item)+' · v'+item.notebook_revision+' · '+item.generation,'muted small'));
        if(item.output_id&&globalThis.ForgeAnalysisOutput)action('查看此次输出',()=>{
          const old=outputMounts.get('history-selected');old?.handle.dispose();old?.handle.element.remove();historyOutput.replaceChildren();
          const ticket=epoch,scope=historyRequest,handle=ForgeAnalysisOutput.mountExecution({parent:historyOutput,execution:item,project,api,mountText,disposeReaders,resolveMedia,onCase,onTable,isCurrent:()=>live(ticket)&&scope===historyRequest&&!root.hidden&&history.open});
          outputMounts.set('history-selected',{identity:item.id,handle});handle.element.open=true;
        },entry);
      }
      if(!items.length)historyRows.append($('p','选择内核后按原 generation 读取；打开 Notebook 不会执行单元。','muted small'));
      action('最新记录',()=>loadHistory(0),historyControls,!(historyScope||session)||!connected);
      action('更早记录',()=>loadHistory(historyNext),historyControls,!(historyScope||session)||historyNext===null||!connected);
      if(historyScope)action('返回当前内核记录',()=>{historyScope=null;historyRequest++;historyAfter=0;historyItems=[];clearHistoryOutput();paintHistory();},historyControls);
    }
    function clearHistoryOutput(){const old=outputMounts.get('history-selected');old?.handle.dispose();old?.handle.element.remove();outputMounts.delete('history-selected');historyOutput.replaceChildren();}
    function paintHistorySelectors(){
      historySelectors.replaceChildren();if(!features.notebook_history||localID(identifier)||!connected)return;
      const label=$('label','Notebook 运行记录'),select=$('select');select.setAttribute('aria-label','选择 Notebook 运行');label.append(select);historySelectors.append(label);
      const empty=$('option','选择服务端保存的运行');empty.value='';select.append(empty);
      for(const [index,item] of historySessions.entries()){const option=$('option',(historySessions.length-index)+' · '+item.created.slice(0,19).replace('T',' ')+' · '+(names[item.state]||item.state)+' · v'+item.notebook_revision);option.value=item.id;select.append(option);}
      select.value=historyScope?.id||'';
      select.onchange=async()=>{
        const id=select.value,ticket=epoch,request=++historyRequest;clearHistoryOutput();historyItems=[];historyAfter=0;historyNext=null;
        if(!id){historyScope=null;historyGenerations=[];paintHistory();return;}
        historyScope={id,generation:null};historyGenerations=[];paintHistory();
        try{const value=await read('session-generations',{session_id:id});if(!live(ticket)||request!==historyRequest)return;
          historyGenerations=value.items;historyScope={id,generation:value.items[0]?.generation};paintHistory();await loadHistory(0);
        }catch(error){if(live(ticket)&&request===historyRequest)info(error.message,true);}
      };
      if(historyScope&&historyGenerations.length){const genLabel=$('label','内核代次'),generations=$('select');generations.setAttribute('aria-label','选择历史内核代次');genLabel.append(generations);historySelectors.append(genLabel);
        for(const [index,item] of historyGenerations.entries()){const option=$('option','代次 '+(historyGenerations.length-index)+' · '+item.execution_count+' 次执行 · '+(item.current?(names[item.state]||item.state):'历史'));option.value=item.generation;generations.append(option);}generations.value=historyScope.generation;
        generations.onchange=async()=>{historyRequest++;clearHistoryOutput();historyScope={id:historyScope.id,generation:generations.value};historyItems=[];historyAfter=0;historyNext=null;paintHistory();try{await loadHistory(0);}catch(error){info(error.message,true);}};
        historySelectors.append($('span','所选运行只读；查看输出不会启动内核或重跑代码。','muted small'));
      }
      action('刷新运行记录',refreshHistorySessions,historySelectors,busy);
    }
    async function refreshHistorySessions(){
      if(!features.notebook_history||localID(identifier)||!connected)return;
      const ticket=epoch,id=identifier,value=await read('notebook-sessions',{notebook_id:id});
      if(live(ticket)&&identifier===id){historySessions=value.items;paintHistorySelectors();}
    }
    history.ontoggle=()=>{if(!history.open)clearHistoryOutput();else refreshHistorySessions().catch(error=>info(error.message,true));};
    async function loadHistory(after){
      const selected=historyScope||session;if(!selected?.generation)return;const original={id:selected.id,generation:selected.generation},ticket=epoch,request=historyRequest;
      const page=await read('executions',{session_id:original.id,generation:original.generation,limit:16,order:'desc',after});
      const current=historyScope||session;if(!live(ticket)||request!==historyRequest||current?.id!==original.id||current?.generation!==original.generation)return;
      historyAfter=after;historyItems=page.items;historyNext=page.next_cursor;paintHistory();
    }
    function paint(){saveNotebookButton.disabled=!canWrite();title.value=draft.title;status.textContent=dirty()?'有未保存修改':'已保存 · v'+revision;persistence.textContent=store.hasVolatileSources()?'本窗口暂存 · 重开前请导出':'';paintPending();paintSources();paintKernel();paintCells();paintHistory();}
    function refreshCellCancel(node,cellId){
      const button=node.querySelector('.analysis-cell-cancel');if(!button)return;
      const recent=executions.filter(item=>item.cell_id===cellId).at(-1);
      const cancellable=!!recent&&!!session&&recent.generation===session.generation&&['queued','dispatched','running'].includes(recent.state);
      button.disabled=!connected||!permissions.analysis_execute||!cancellable;
      button.textContent=recent?.state==='queued'?'取消排队':'取消单元';
      button.title=recent?.state==='queued'?'取消尚未派发的排队单元。':'中断本单元；受信模式下将停止本代内核并清理，之后可重新启动。';
    }
    async function poll(){
      if(polling||!connected||!session||disposed||session.state==='stopped')return;polling=true;
      const ticket=epoch,original={id:session.id,generation:session.generation};
      try{
        const value=await read('session',{session_id:original.id});if(!live(ticket)||session?.id!==original.id||session?.generation!==original.generation)return;
        if(value.generation!==original.generation){info('原内核代次已变化；此页面未接管新的内核，请重新选择。',true);return;}
        session=value;
        const page=await read('executions',{session_id:original.id,generation:original.generation,limit:16,order:'desc'});
        if(!live(ticket)||session?.generation!==original.generation)return;
        executions=[...page.items].reverse();if(!historyScope&&!historyAfter)historyNext=page.next_cursor;
        paintKernel();paintHistory();
        // Preserve editor nodes/cursors while observations change; only execution slots repaint.
        for(const entry of cellNodes.values())updateCellExecution(entry);
      }finally{polling=false;}
    }
    function schedule(){clearTimeout(pollTimer);if(disposed)return;pollTimer=setTimeout(async()=>{
      try{if(!document.hidden&&root.getClientRects().length)await poll();}catch(error){if(!disposed&&error.name!=='AbortError')info(error.message,true);}finally{schedule();}
    },2000);}
    try{const local=store.drafts(project).filter(item=>localID(item.id)).at(-1);if(local){identifier=local.id;draft=local.document;bindings=local.bindings||{};touched=true;title.value=draft.title;}}
    catch(error){info(error.message,true);}
    paint();if(!connected)info('尚未接入分析环境；可以编辑和导出本地源单元。保存、运行和历史读取将在接入后开放。');
    refreshList().catch(error=>info(error.message,true));schedule();
    return {element:root,setPresentation(value){return cases?.setPresentation(value)??false;},canLeave(){return flushDraft()&&(!cases||cases.canLeave());},insertTemplate,
      setMode(notebook){if(!flushDraft()||cases&&!cases.canLeave())return false;root.hidden=!notebook;actions.hidden=!notebook;if(!notebook)clearNotebookOutputs();else for(const entry of cellNodes.values())paintOutput(entry.section,executions.filter(item=>item.cell_id===entry.id).at(-1));cases?.activate(!notebook);return true;},
      setSource(source){if(same(currentSource,source))return;currentSource=source;sourceEpoch++;cases?.setSource(source);if(!disposed)paintSources();},updatePermissions(value){if(same(permissions,value))return;permissions=value;cases?.updatePermissions(value);paint();},
      dispose(){if(disposed)return true;if(!flushDraft()||cases&&!cases.dispose())return false;disposed=true;epoch++;abort.abort();clearTimeout(draftTimer);clearTimeout(pollTimer);globalThis.removeEventListener('beforeunload',beforeUnload);clearNotebookOutputs();actions.remove();globalThis.ForgeAnalysisRender?.disposeWithin(root);disposeReaders(root);return true;}};
  }
  globalThis.ForgeAnalysis=Object.freeze({mount});
})();
