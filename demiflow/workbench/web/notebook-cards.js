import {presentDataFrame} from './notebook-dataframe.js';
import {notebookTranscript} from './notebook-transcript.js';
import {notebookConversation} from './notebook-assistant.js';

const icons={save:'M4 3h13l3 3v15H4zM8 3v6h8V3M8 20v-7h8v7',cell:'M3 4h18v11H3zM8 20h8m-4-4v8',chat:'M5 4h14a1 1 0 0 1 1 1v11a1 1 0 0 1-1 1H9l-5 4V5a1 1 0 0 1 1-1Z',close:'m6 6 12 12M6 18 18 6',run:'m8 5 11 7-11 7z',code:'m8 7-5 5 5 5m8-10 5 5-5 5',up:'M12 19V5m-6 6 6-6 6 6'};
function element(doc,tag,cls,text){
  const node=doc.createElement(tag);if(cls)node.className=cls;if(text!==undefined)node.textContent=text;return node;
}
function icon(doc,name,label,action){
  const button=element(doc,'button','df-card-icon');button.type='button';button.title=label;button.setAttribute('aria-label',label);
  const svg=doc.createElementNS('http://www.w3.org/2000/svg','svg'),path=doc.createElementNS(svg.namespaceURI,'path');
  svg.setAttribute('viewBox','0 0 24 24');svg.setAttribute('aria-hidden','true');path.setAttribute('d',icons[name]);svg.append(path);button.append(svg);button.onclick=action;return button;
}

const NO_PYTHON_MSG='当前项目尚未配置业务 Python 环境（Notebook 内核不继承平台环境），请先在右上角「设置 → 项目设置」中指定独立 Python 路径。';
const TOOL_KINDS={analysis:'分析',tool:'工具',view:'查看',annotation:'标注',annotation_component:'组件'};

function detectCellTools(source){
  if(!source||!source.includes('data_tools'))return [];
  const found=[];
  const seen=new Set();
  const add=name=>{const clean=name.trim().split(/\s+as\s+/)[0].trim();if(clean&&/^[A-Za-z_]\w*$/.test(clean)&&!seen.has(clean)){seen.add(clean);found.push(clean);}};
  for(const match of source.matchAll(/from\s+(?:[\w.]+\.)?data_tools(?:\.([\w.]+))?\s+import\s+\(?([^#\n)]+)/g)){
    const submod=match[1]||'';
    for(const part of match[2].split(','))add(part);
    if(!match[2].trim()&&submod)add(submod.split('.').pop());
  }
  for(const match of source.matchAll(/import\s+(?:[\w.]+\.)?data_tools\.([\w.]+)/g)){
    add(match[1].split('.').pop());
  }
  return found;
}

export function mountCards({desk,controls,panel,storage,app,api,project,alive,permissions,hasProjectPython,loadTools,openTool,writeDataTool,tables,catalogCommand,layout}){
  const book=panel.content,doc=book.node.ownerDocument,win=doc.defaultView;
  const frameTables='.jp-RenderedHTMLCommon table.dataframe';
  const renderTables=node=>{
    if(node.matches?.(frameTables))presentDataFrame(node);
    node.querySelectorAll?.(frameTables).forEach(presentDataFrame);
  };
  const aside=element(document,'aside','df-card-assistant');aside.setAttribute('aria-label','卡片助手');
  const head=element(document,'header','df-assistant-head'),heading=element(document,'strong',null,'卡片助手'),targetName=element(document,'small','df-target-name');
  const thread=element(document,'div','df-card-thread');const flow=element(document,'div'), original=element(document,'section','df-card-original'), actions=element(document,'div');
  original.hidden=true;thread.append(flow,original,actions);
  let follow=true;
  thread.onscroll=()=>{follow=thread.scrollHeight-thread.scrollTop-thread.clientHeight<48;};
  const transcript=notebookTranscript(flow,original,api,()=>{if(follow)thread.scrollTop=thread.scrollHeight;});
  const form=element(document,'form','df-card-composer'),reference=element(document,'div','df-reference');
  const input=element(document,'textarea');input.rows=2;input.placeholder='描述你想做的分析或标注…';input.setAttribute('aria-label','卡片要求');
  const providerSelect=element(document,'select','df-card-provider');
  providerSelect.hidden=true;providerSelect.setAttribute('aria-label','卡片助手模型');providerSelect.title='卡片助手模型';
  let providers=[],canPrewarm=false,prewarmKey='';
  function requestNotebookPrewarm(conversation,providerId){
    if(!valid()||!assistantOpen||!canPrewarm||!providerId)return;
    const sid=conversation?.sessionId||null;
    const key=(sid||'new')+':'+providerId;
    if(key===prewarmKey)return;
    prewarmKey=key;
    api('dev-prewarm',{},{session_kind:'notebook',...(sid?{session_id:sid}:{}),provider_id:providerId}).catch(()=>{if(prewarmKey===key)prewarmKey='';});
  }
  providerSelect.onchange=()=>{
    if(selectedId){
      const conv=assistantFor(selectedId);
      conv.chooseProvider(providerSelect.value);
      requestNotebookPrewarm(conv,providerSelect.value);
    }
  };
  async function loadProviders(){
    try{
      const caps=await api('dev-capabilities');
      if(!valid()||!assistantOpen)return;
      canPrewarm=caps.prewarm_api===true&&caps.notebook_cells===true&&caps.enabled===true&&caps.start===true;
      providers=(caps.providers||[]).filter(p=>p.available);
      providerSelect.replaceChildren(...providers.map(p=>new Option(p.label||p.id,p.id)));
      renderThread();
    }catch{} // Sending checks capabilities again and displays the service error.
  }
  const executionLabel=element(document,'label','df-run-consent','允许自动运行');
  const execution=element(document,'input');execution.type='checkbox';
  const autoRunKey='demiflow.notebook.auto_run';
  try{execution.checked=localStorage.getItem(autoRunKey)!=='false';}catch{execution.checked=true;}
  execution.onchange=()=>{try{localStorage.setItem(autoRunKey,String(execution.checked));}catch{}};
  executionLabel.prepend(execution);
  const composerFooter=element(document,'div','df-composer-footer');
  const toolPickerBtn=element(document,'button','df-tool-picker-btn','@ 工具');
  toolPickerBtn.type='button';toolPickerBtn.title='查看或显式指定项目数据工具';
  toolPickerBtn.setAttribute('aria-expanded','false');
  const toolPopover=element(document,'div','df-tool-popover');toolPopover.hidden=true;
  const toolSearch=element(document,'input','df-tool-search');
  toolSearch.type='search';toolSearch.placeholder='搜索项目或内置工具…';toolSearch.setAttribute('aria-label','搜索工具');
  const toolList=element(document,'div','df-tool-popover-list');
  toolPopover.append(toolSearch,toolList);
  let toolDefs=null;
  async function ensureTools(){
    if(toolDefs||!loadTools)return toolDefs||[];
    try{
      const index=await loadTools();
      if(valid()){toolDefs=index?.definitions||[];schedulePaint();}
    }catch{toolDefs=[];}
    return toolDefs||[];
  }
  function findToolDef(name){
    if(!toolDefs)return null;
    return toolDefs.find(item=>item.symbol===name||item.module?.endsWith('.'+name)||item.module===name)||null;
  }
  function renderToolPopover(){
    toolList.replaceChildren();
    const q=toolSearch.value.trim().toLocaleLowerCase();
    const items=(toolDefs||[]).filter(item=>!q||[item.title,item.symbol,item.summary,item.when_to_use,item.path,item.description].join(' ').toLocaleLowerCase().includes(q));
    if(!items.length){
      toolList.append(element(document,'p','df-chat-status',q?'没有匹配的工具。':'项目尚未声明数据工具。'));
      return;
    }
    for(const item of items.slice(0,40)){
      const row=element(document,'div','df-tool-popover-row');
      const pick=element(document,'button','df-tool-popover-item');pick.type='button';
      const headLine=element(document,'div','df-tool-popover-head');
      headLine.append(element(document,'span','df-tool-popover-title',item.title||item.symbol),
        element(document,'small',null,TOOL_KINDS[item.tool_kind]||'工具'));
      pick.append(headLine);
      if(item.summary)pick.append(element(document,'span','df-tool-popover-summary',item.summary));
      pick.onclick=()=>{
        const cell=selected()||newCell();
        if(cell){
          const current=refs.get(cell.model.id)||[];
          if(current.length<16&&!current.some(i=>i.id===item.id))refs.set(cell.model.id,[...current,item]);
          renderThread();
        }
        closeToolPopover();input.focus();
      };
      row.append(pick);
      if(openTool){
        const info=element(document,'button','df-tool-popover-info','···');info.type='button';
        info.title='查看 '+item.symbol+' 说明与源码';info.setAttribute('aria-label',info.title);
        info.onclick=()=>{closeToolPopover();openTool(item);};
        row.append(info);
      }
      toolList.append(row);
    }
  }
  function closeToolPopover(){toolPopover.hidden=true;toolPickerBtn.setAttribute('aria-expanded','false');}
  toolPickerBtn.onclick=async()=>{
    if(!toolPopover.hidden){closeToolPopover();return;}
    toolPopover.hidden=false;toolPickerBtn.setAttribute('aria-expanded','true');
    toolList.replaceChildren(element(document,'p','df-chat-status','正在读取工具…'));
    await ensureTools();
    if(valid()&&!toolPopover.hidden){renderToolPopover();toolSearch.focus();}
  };
  toolSearch.oninput=renderToolPopover;
  toolPopover.onkeydown=e=>{if(e.key==='Escape'){e.preventDefault();closeToolPopover();input.focus();}};
  const onDocClick=e=>{if(!toolPopover.hidden&&!toolPopover.contains(e.target)&&e.target!==toolPickerBtn)closeToolPopover();};
  document.addEventListener('click',onDocClick,true);
  composerFooter.append(executionLabel,toolPickerBtn);
  const send=icon(document,'up','发送要求');send.type='submit';
  const messages=new Map(),drafts=new Map(),refs=new Map(),decorated=new Map();
  const conversations=new Map(),inFlight=new Set();
  const resizeKeys=new WeakMap();
  // Jupyter handles arrows in capture phase, before a focused cell child.
  const onResizeKey=event=>resizeKeys.get(event.target)?.(event);
  win.addEventListener('keydown',onResizeKey,true);
  let selectedId=null,dead=false,assistantOpen=false,paintFrame=0;
  let path=panel.context.path;
  const viewKey=()=>JSON.stringify(['demiflow.notebook.cards',project,path]);
  const valid=()=>!dead&&alive()&&!panel.isDisposed;
  const writable=()=>!!permissions().analysis_write&&!panel.model?.readOnly;
  const cells=()=>book.widgets;
  const selected=()=>book.activeCell?.model?.type==='code'?book.activeCell:null;
  const cellById=id=>cells().find(cell=>cell.model.id===id);
  function assistantFor(id){
    if(!conversations.has(id)){
      const boundPath=path;
      conversations.set(id,notebookConversation({api,project,path:boundPath,cell:id,
        active:()=>valid()&&panel.context.path===boundPath,
        onSession:sid=>{if(assistantOpen&&selectedId===id)transcript.select(sid);},
        onEvents:(sid,events)=>transcript.receive(sid,events),
        command:(name,args,pending)=>command(id,name,args,pending)}));
    }
    return conversations.get(id);
  }
  function remember(){
    if(selectedId)drafts.set(selectedId,input.value);
    const recent=map=>[...map].slice(-16);
    const state={messages:recent(messages),drafts:recent(drafts),open:assistantOpen};
    try{
      let raw=JSON.stringify(state);
      while(raw.length>192000&&state.messages.length){state.messages.shift();raw=JSON.stringify(state);}
      if(raw.length<=192000)sessionStorage.setItem(viewKey(),raw);
    }catch{}
  }
  let savedOpen=null;
  try{
    const state=JSON.parse(sessionStorage.getItem(viewKey())||'{}');
    for(const [id,rows] of state.messages||[])if(Array.isArray(rows))messages.set(id,rows);
    for(const [id,draft] of state.drafts||[])if(typeof draft==='string')drafts.set(id,draft);
    if(typeof state.open==='boolean')savedOpen=state.open;
  }catch{}
  function rename(){if(path!==panel.context.path)conversations.clear();path=panel.context.path;}
  function focusCell(cell){book.deselectAll();book.activeCellIndex=cells().indexOf(cell);}
  function newCell(source=''){
    if(!writable())return null;
    const index=Math.max(0,book.activeCellIndex+1);
    book.model.sharedModel.insertCell(index,{id:crypto.randomUUID(),cell_type:'code',source,metadata:{},outputs:[],execution_count:null});
    book.deselectAll();book.activeCellIndex=index;book.activeCell?.node.scrollIntoView({block:'nearest'});return book.activeCell;
  }
  function showAssistant(value){
    assistantOpen=value;desk.dataset.assistant=value?'open':'closed';aside.hidden=!value;divider.setActive(value);
    if(value){renderThread();loadProviders();input.focus();}else transcript.select(null);
    schedulePaint();
    layout();
  }
  const add=icon(document,'cell','新建卡片',()=>{const cell=newCell();cell?.editor?.focus();});
  add.disabled=!writable();controls.append(add);
  head.append(heading,targetName,providerSelect,icon(document,'close','关闭助手',()=>showAssistant(false)));form.append(toolPopover,reference,input,composerFooter,send);aside.append(head,thread,form);desk.append(aside);
  const divider=ForgeNavigation.resizable({layout:desk,sidebar:aside,label:'卡片助手',right:true,active:false});
  function ask(item){
    let cell=selected();if(!cell)cell=newCell();
    if(cell&&item){
      const items=refs.get(cell.model.id)||[];
      if(items.length<16&&!items.some(i=>i.id===item.id))refs.set(cell.model.id,[...items,item]);
    }
    showAssistant(true);
  }
  function sourceOf(cell){return cell.model.sharedModel.getSource();}
  function trackSelection(){
    if(selectedId)drafts.set(selectedId,input.value);
    selectedId=selected()?.model.id||null;follow=true;input.value=drafts.get(selectedId)||'';
    if(assistantOpen)renderThread();
    schedulePaint();
  }
  function renderThread(){
    actions.replaceChildren();reference.replaceChildren();
    const cell=selected(),id=cell?.model.id;
    targetName.textContent=cell?(cell.model.getMetadata('demiflow_card_title')||'当前卡片 '+(book.activeCellIndex+1)):'';
    const items=refs.get(id)||[];
    for(const item of items){
      const chip=element(document,'span','df-tool-chip',item.title||item.symbol);
      chip.append(icon(document,'close','取消引用 '+item.symbol,()=>{refs.set(id,items.filter(i=>i.id!==item.id));renderThread();}));reference.append(chip);
    }
    const conversation=id?assistantFor(id):null;
    transcript.select(conversation?.sessionId||null);
    const pending=conversation?.pending;
    const busy=inFlight.has(id);
    providerSelect.hidden=providers.length<2||!id;
    providerSelect.disabled=!!(busy||pending||conversation?.sessionId);
    providerSelect.value=conversation?.provider||providers.find(p=>p.id==='deepseek')?.id||providers[0]?.id||'';
    if(assistantOpen&&id&&!busy&&!pending)requestNotebookPrewarm(conversation,providerSelect.value);
    executionLabel.hidden=!permissions().analysis_execute;execution.disabled=busy;
    input.disabled=busy||!writable()||!id;send.disabled=input.disabled;
    if(!id){actions.append(element(document,'p','df-chat-status','选中一张代码卡片即可调整。'));return;}
    for(const row of messages.get(id)||[]){
      if(!row.turn)actions.append(element(document,'p','df-card-request',row.request));
      if(row.text&&(!row.turn||row.error))actions.append(element(document,'p','df-card-reply',row.text));
      if(row.code){
        const preview=element(document,'details','df-proposal'),summary=element(document,'summary',null,'查看生成代码');
        preview.append(summary,element(document,'pre',null,row.code));actions.append(preview);
        const apply=element(document,'button','df-apply',sourceOf(cell)===row.source?'应用到这张卡片':'作为新卡片插入');
        apply.disabled=!writable();apply.onclick=()=>applyCode(row,cell);actions.append(apply);
      }
    }
    const last=messages.get(id)?.at(-1);
    const interrupted=last?.retryable||(!last?.turn&&/^本轮未完成。\s*interrupted$/.test(last?.text||''));
    if(interrupted&&!busy&&!pending){
      const retry=element(document,'button','df-apply','重新发送');
      retry.onclick=()=>{input.value=last.request;submit(last.request);};
      actions.append(retry);
    }
    if(busy)actions.append(element(document,'p','df-chat-status','助手正在生成…'));
    else if(pending?.cell===id){
      const resume=element(document,'button','df-apply','取回上次回复');resume.onclick=()=>submit(pending.request);actions.append(resume);
    }
    if(follow)thread.scrollTop=thread.scrollHeight;
  }
  function saveMessage(id,row){const list=messages.get(id)||[];list.push(row);messages.set(id,list.slice(-8));remember();}
  function changed(){
    if(assistantOpen&&messages.get(selectedId)?.some(row=>row.code))renderThread();
  }
  function applyCode(row,cell){
    if(!valid()||!writable()||!row.code)return;
    const target=cellById(cell.model.id);
    let applied;
    if(target&&sourceOf(target)===row.source){target.model.sharedModel.setSource(row.code);applied=target;}
    else applied=newCell(row.code);
    if(!applied)return;
    applied.model.setMetadata('demiflow_card_title',row.request.slice(0,80));
    applied.model.setMetadata('demiflow_card_needs_run',true);
    applied.node.dataset.dfFolded='false';
    if(!win.matchMedia('(prefers-reduced-motion: reduce)').matches)applied.node.animate(
      [{boxShadow:'0 0 0 2px #91a9ed'},{boxShadow:'0 0 0 2px transparent'}],{duration:850,easing:'ease-out'});
    row.code=null;row.text||='已生成卡片。';
    schedulePaint();renderThread();remember();
  }
  async function command(id,name,args,pending){
    if(!valid()||path!==panel.context.path)throw Error('目标 Notebook 已关闭或改名。');
    if(name.startsWith('catalog.'))return catalogCommand(name,args);
    if(name==='tool.write'){
      if(!writable()||!writeDataTool)throw Error('当前项目未开启写权限，无法写入 data_tools。');
      const result=await writeDataTool(args);
      toolDefs=null;await ensureTools();
      return result;
    }
    const cell=cellById(id);
    if(!cell||cell.isDisposed||cell.model.type!=='code')throw Error('目标 cell 已删除。');
    const source=sourceOf(cell);
    if(name==='cell.show'||name==='context.show'){
      const allCells=cells();
      const index=allCells.indexOf(cell);
      const prevCodeCells=index>0
        ?allCells.slice(Math.max(0,index-3),index)
          .map((c,i)=>({cell:c,index:Math.max(0,index-3)+i}))
          .filter(({cell:c})=>c.model.type==='code')
        :[];
      const before=prevCodeCells.map(({cell:c})=>sourceOf(c)).join('\n').slice(0,2000);
      const outputText=cell.outputArea?.node.innerText||'';
      const selectedRefs=(refs.get(id)||[]).map(i=>[i.path,i.symbol]);
      const allowRun=pending.allowRun&&!!permissions().analysis_execute;
      const loadEnv=async()=>{
        try{
          const inv=await api('projects');
          const proj=(inv.projects||[]).find(p=>p.workspace_id===project);
          if(proj)return {kernel_python:proj.python||null,sandbox:proj.notebook_sandbox||{mode:'readonly',write_paths:[]}};
        }catch{}
        return {kernel_python:null,sandbox:{mode:'readonly',write_paths:[]}};
      };
      if(name==='cell.show'){
        const env=await loadEnv();
        return {path,source,before,output:outputText.slice(0,1600),selected:selectedRefs,allow_run:allowRun,...env};
      }
      const topic=args?.topic||null;
      if(topic==='cell'){
        return {path,cell_index:index,source,selected:selectedRefs,allow_run:allowRun};
      }
      if(topic==='before'){
        return {path,cell_index:index,before_cells:prevCodeCells.map(({cell:c,index:ci})=>({index:ci,source:sourceOf(c).slice(0,1200)}))};
      }
      if(topic==='output'){
        return {path,cell_index:index,output:outputText.slice(0,2400)};
      }
      if(topic==='env'){
        const env=await loadEnv();
        return {path,allow_run:allowRun,...env};
      }
      if(topic==='catalog'){
        const [dictList,toolList]=await Promise.all([
          catalogCommand('catalog.list',{group:'dictionary',offset:0}),
          catalogCommand('catalog.list',{group:'tools',offset:0}),
        ]);
        return {dictionary:dictList,tools:toolList};
      }
      // Compact overview (progressive disclosure): expand details via `data context --topic <...>`
      const env=await loadEnv();
      let catalogSummary=null;
      try{
        const [dictList,toolList]=await Promise.all([
          catalogCommand('catalog.list',{group:'dictionary',offset:0}),
          catalogCommand('catalog.list',{group:'tools',offset:0}),
        ]);
        catalogSummary={tables:dictList.total,tools:toolList.total};
      }catch{}
      return {
        path,cell_index:index,total_cells:allCells.length,allow_run:allowRun,...env,
        source:source.slice(0,1200),source_truncated:source.length>1200,
        before_cells_count:prevCodeCells.length,has_output:outputText.trim().length>0,
        selected:selectedRefs,catalog_summary:catalogSummary,
        topics:['cell','before','output','env','catalog'],
      };
    }
    if(source!==(pending.current??pending.source))throw Error('cell 已被手动修改，这条命令未应用。');
    if(!writable())throw Error('当前文档只读。');
    if(name==='cell.edit'){
      applyCode({source,code:args.source,request:pending.request},cell);
      return {state:'edited'};
    }
    if(name!=='cell.run'||!pending.allowRun||!permissions().analysis_execute)throw Error('本轮未获运行许可。');
    if(hasProjectPython&&!hasProjectPython())throw Error(NO_PYTHON_MSG);
    await panel.sessionContext.ready;
    if(!valid()||cell.isDisposed||sourceOf(cell)!==source||!writable()||!permissions().analysis_execute)
      throw Error('cell 或运行权限已变更。');
    if(!panel.sessionContext.session?.kernel){
      await panel.sessionContext.changeKernel({name:'python3'});
    }
    if(!panel.sessionContext.session?.kernel)throw Error('当前 Notebook 内核未连接。');
    const reply=await cell.constructor.execute(cell,panel.sessionContext);
    if(!reply||reply.content.status!=='ok')throw Error([reply?.content?.ename,reply?.content?.evalue].filter(Boolean).join(': ')||'内核未成功执行。');
    if(!cell.isDisposed&&cell.model.outputs.length)cell.node.dataset.dfFolded='true';
    schedulePaint();
    return {state:'executed',execution_count:reply.content.execution_count,output:(cell.outputArea?.node.innerText||'').slice(0,2400)};
  }
  async function submit(request){
    const cell=selected();if(!cell||inFlight.has(cell.model.id)||!request.trim()||!writable())return;
    const id=cell.model.id,source=sourceOf(cell),index=book.activeCellIndex,initialPath=path;
    const context={path,cell:id,source,allowRun:execution.checked&&!!permissions().analysis_execute,before:cells().slice(Math.max(0,index-3),index).filter(c=>c.model.type==='code').map(sourceOf).join('\n'),
      output:cell.outputArea?.node.innerText||'',tables:tables(),selected:refs.get(id)||[]};
    inFlight.add(id);schedulePaint();renderThread();
    try{
      const result=await assistantFor(id).generate(request,context);
      if(!valid()||path!==initialPath)return;
      const row={request:result.request,source:result.source,code:result.code,text:result.message,turn:result.turn};
      drafts.delete(id);if(selectedId===id)input.value='';saveMessage(id,row);
      const target=cellById(id);
      if(row.code&&target&&sourceOf(target)===row.source)applyCode(row,target);
    }catch(error){if(valid())saveMessage(id,{request,text:error.message,error:true,turn:error.turn,retryable:error.retryable});}
    finally{inFlight.delete(id);if(valid()){schedulePaint();renderThread();}}
  }
  form.onsubmit=e=>{e.preventDefault();submit(input.value);};
  input.onkeydown=e=>{if(e.key==='Enter'&&!e.shiftKey&&!e.isComposing){e.preventDefault();form.requestSubmit();}};
  function onExecution(model,change){
    if(change.name==='executionState'&&change.newValue==='running'&&model.getMetadata('demiflow_card_needs_run')){
      model.deleteMetadata('demiflow_card_needs_run');schedulePaint();
    }
  }
  function saveButton(cell){
    return icon(doc,'save','保存这张卡片',()=>{if(writable())storage.save(cell.model).catch(()=>schedulePaint());});
  }
  function saveShortcut(event){
    if(event.defaultPrevented||event.isComposing||!(event.ctrlKey||event.metaKey)||event.shiftKey||event.altKey||event.key.toLowerCase()!=='s')return;
    if(event.currentTarget===window&&(aside.contains(event.target)||!desk.contains(event.target)))return;
    event.preventDefault();event.stopImmediatePropagation();
    const cell=selected();if(cell&&writable())storage.save(cell.model).catch(()=>schedulePaint());
  }
  storage.changes.addEventListener('change',schedulePaint);
  win.addEventListener('keydown',saveShortcut,true);window.addEventListener('keydown',saveShortcut,true);
  function decorate(){
    paintFrame=0;if(!valid())return;
    for(const [cell,bar] of decorated)if(cell.isDisposed){bar.release();bar.remove();decorated.delete(cell);}
    for(const cell of cells()){
      if(!cell.node.isConnected)continue;
      if(cell.model.type!=='code'){
        let bar=decorated.get(cell);
        if(!bar){
          bar=element(doc,'header','df-cell-tools');bar.append(element(doc,'span','df-cell-title'),element(doc,'small','df-save-state'),saveButton(cell));
          bar.release=()=>{};cell.node.prepend(bar);decorated.set(cell,bar);
        }
        bar.children[1].textContent=storage.status(cell.model);bar.lastChild.disabled=!writable();
        continue;
      }
      let bar=decorated.get(cell);
      if(!bar){
        bar=element(doc,'header','df-cell-tools');const name=element(doc,'span','df-cell-title');
        const toolBadge=element(doc,'button','df-cell-tool-badge');toolBadge.type='button';toolBadge.hidden=true;
        toolBadge.onclick=async()=>{
          const names=detectCellTools(sourceOf(cell));
          if(!names.length||!openTool)return;
          await ensureTools();
          const match=names.map(findToolDef).find(Boolean);
          if(match)openTool(match);
        };
        const status=element(doc,'span','df-cell-status');status.setAttribute('role','status');
        const edit=icon(doc,'code','编辑代码',()=>{
          focusCell(cell);
          const fold=cell.model.outputs.length>0&&cell.node.dataset.dfFolded==='false';
          cell.node.dataset.dfFolded=String(fold);
          book.mode=fold?'command':'edit';
          if(!fold)cell.editor?.focus();
          schedulePaint();
        });
        const run=icon(doc,'run','运行卡片',async()=>{
          if(!permissions().analysis_execute)return;
          run.disabled=true;cell.node.querySelector('.df-run-error')?.remove();
          try{
            if(hasProjectPython&&!hasProjectPython())throw Error(NO_PYTHON_MSG);
            await panel.sessionContext.ready;
            if(!valid()||cell.isDisposed)return;
            if(!panel.sessionContext.session?.kernel){
              await panel.sessionContext.changeKernel({name:'python3'});
            }
            if(!valid()||cell.isDisposed)return;
            focusCell(cell);
            const ok=await app.commands.execute('notebook:run-cell',{widgetId:panel.id,activate:false});
            if(ok===undefined)throw Error('未能定位到这张卡片所在的 Notebook。');
            if(!cell.isDisposed&&cell.model.outputs.length)cell.node.dataset.dfFolded='true';
          }catch(error){
            if(!cell.isDisposed){
              const warning=element(doc,'p','df-run-error',error.message||'运行失败');
              warning.setAttribute('role','status');cell.node.querySelector('.df-run-error')?.remove();bar.after(warning);
            }
          }finally{run.disabled=!permissions().analysis_execute;schedulePaint();}
        });
        run.disabled=!permissions().analysis_execute;const assistant=icon(doc,'chat','卡片助手',()=>{
          focusCell(cell);showAssistant(true);
        });
        bar.append(name,toolBadge,status,assistant,edit,run,element(doc,'small','df-save-state'),saveButton(cell));
        const viewport=cell.outputArea.node;
        const grip=element(doc,'div','df-output-grip');grip.tabIndex=0;
        grip.setAttribute('role','separator');grip.setAttribute('aria-orientation','horizontal');
        grip.setAttribute('aria-label','调整结果区高度');grip.title='拖动调整高度，双击恢复自动高度';
        const maximum=()=>Math.max(120,Math.round(win.innerHeight*.85));
        function height(px){
          if(px===null)viewport.style.removeProperty('--df-output-height');
          else viewport.style.setProperty('--df-output-height',Math.max(48,Math.min(maximum(),Math.round(px)))+'px');
        }
        let drag=null;
        grip.onpointerdown=event=>{
          if(event.button!==0||!event.isPrimary)return;
          event.preventDefault();event.stopPropagation();grip.focus();
          drag={y:event.clientY,height:viewport.getBoundingClientRect().height};
          grip.setPointerCapture(event.pointerId);
        };
        grip.onpointermove=event=>{if(drag)height(drag.height+event.clientY-drag.y);};
        grip.onlostpointercapture=()=>{drag=null;};
        grip.ondblclick=()=>height(null);
        resizeKeys.set(grip,event=>{
          if(!['ArrowUp','ArrowDown','Home'].includes(event.key))return;
          event.preventDefault();event.stopPropagation();
          height(event.key==='Home'?null:viewport.getBoundingClientRect().height+(event.key==='ArrowUp'?-24:24));
        });
        const size=new win.ResizeObserver(()=>{
          grip.setAttribute('aria-valuemin','48');grip.setAttribute('aria-valuemax',String(maximum()));
          grip.setAttribute('aria-valuenow',String(Math.max(48,Math.round(viewport.getBoundingClientRect().height))));
        });
        size.observe(viewport);cell.node.append(grip);
        bar.release=()=>{size.disconnect();height(null);grip.remove();};
        cell.model.stateChanged.connect(onExecution);
        cell.node.prepend(bar);decorated.set(cell,bar);
        cell.node.dataset.dfFolded=String(cell.model.outputs.length>0&&!cell.model.getMetadata('demiflow_card_needs_run'));
      }
      if(bar.parentNode!==cell.node)cell.node.prepend(bar);
      const expanded=cell.node.dataset.dfFolded==='false';
      const source=sourceOf(cell);
      const toolBadge=bar.children[1];
      const usedTools=detectCellTools(source);
      if(usedTools.length){
        if(!toolDefs&&loadTools)ensureTools();
        const labels=usedTools.map(sym=>{const def=findToolDef(sym);return def?.title||sym;});
        toolBadge.textContent='工具 · '+labels.slice(0,2).join('、')+(labels.length>2?' 等'+labels.length+'项':'');
        toolBadge.title='此卡片已调用数据工具：'+labels.join('、')+'（点击查看说明与源码）';
        toolBadge.hidden=false;
      }else{
        toolBadge.hidden=true;
      }
      const status=bar.children[2];
      const state=inFlight.has(cell.model.id)?'正在更新…':cell.model.getMetadata('demiflow_card_needs_run')?'代码已更新 · 待运行':'';
      if(status.textContent!==state)status.textContent=state;
      bar.children[3].setAttribute('aria-expanded',String(assistantOpen&&selectedId===cell.model.id));
      const edit=bar.children[4],label=expanded&&cell.model.outputs.length?'收起代码':'编辑代码';
      edit.setAttribute('aria-expanded',String(expanded));edit.setAttribute('aria-label',label);edit.title=label;
      bar.children[6].textContent=storage.status(cell.model);bar.lastChild.disabled=!writable();
      bar.firstChild.textContent=cell.model.getMetadata('demiflow_card_title')||(source.startsWith('# ')?source.slice(2,98).split('\n')[0]:'');
    }
  }
  function schedulePaint(){if(!paintFrame)paintFrame=win.requestAnimationFrame(decorate);}
  const observer=new win.MutationObserver(records=>{
    if(records.some(r=>[...r.addedNodes].some(n=>n.nodeType===1&&(n.matches('.jp-CodeCell')||n.querySelector('.jp-CodeCell')))))schedulePaint();
  });
  const outputObserver=new win.MutationObserver(records=>{
    for(const record of records)for(const node of record.addedNodes)
      if(node.nodeType===1)renderTables(node);
  });
  outputObserver.observe(book.node,{subtree:true,childList:true});renderTables(book.node);
  observer.observe(book.node,{subtree:true,childList:true});
  book.activeCellChanged.connect(trackSelection);panel.model.contentChanged.connect(changed);panel.context.pathChanged.connect(rename);
  input.maxLength=8192;input.onchange=remember;win.addEventListener('pagehide',remember);
  rename();showAssistant(savedOpen===true);trackSelection();decorate();
  return {ask,pending:()=>inFlight.size>0,dispose(){
    storage.persistDrafts();storage.changes.removeEventListener('change',schedulePaint);
    document.removeEventListener('click',onDocClick,true);
    win.removeEventListener('keydown',saveShortcut,true);window.removeEventListener('keydown',saveShortcut,true);
    remember();dead=true;outputObserver.disconnect();book.node.querySelectorAll('.df-dataframe-view').forEach(view=>view.remove());win.removeEventListener('pagehide',remember);observer.disconnect();win.cancelAnimationFrame(paintFrame);book.activeCellChanged.disconnect(trackSelection);panel.model?.contentChanged.disconnect(changed);panel.context.pathChanged.disconnect(rename);
    win.removeEventListener('keydown',onResizeKey,true);divider.dispose();for(const [cell,bar] of decorated){cell.model.stateChanged.disconnect(onExecution);bar.release();bar.remove();}transcript.dispose();aside.remove();add.remove();delete desk.dataset.assistant;
  }};
}
