import {presentDataFrame} from './notebook-dataframe.js';
import {notebookConversation} from './notebook-assistant.js';

const icons={plus:'M12 5v14M5 12h14',chat:'M5 4h14a1 1 0 0 1 1 1v11a1 1 0 0 1-1 1H9l-5 4V5a1 1 0 0 1 1-1Z',close:'m6 6 12 12M6 18 18 6',run:'m8 5 11 7-11 7z',code:'m8 7-5 5 5 5m8-10 5 5-5 5',up:'M12 19V5m-6 6 6-6 6 6'};
function element(doc,tag,cls,text){
  const node=doc.createElement(tag);if(cls)node.className=cls;if(text!==undefined)node.textContent=text;return node;
}
function icon(doc,name,label,action){
  const button=element(doc,'button','df-card-icon');button.type='button';button.title=label;button.setAttribute('aria-label',label);
  const svg=doc.createElementNS('http://www.w3.org/2000/svg','svg'),path=doc.createElementNS(svg.namespaceURI,'path');
  svg.setAttribute('viewBox','0 0 24 24');svg.setAttribute('aria-hidden','true');path.setAttribute('d',icons[name]);svg.append(path);button.append(svg);button.onclick=action;return button;
}

export function mountCards({desk,documentPane,panel,app,api,project,alive,permissions,tables,layout}){
  const book=panel.content,doc=book.node.ownerDocument,win=doc.defaultView;
  const style=element(doc,'link');style.rel='stylesheet';style.href='/notebook-cards.css';doc.head.append(style);
  const frameTables='.jp-RenderedHTMLCommon table.dataframe';
  const renderTables=node=>{
    if(node.matches?.(frameTables))presentDataFrame(node);
    node.querySelectorAll?.(frameTables).forEach(presentDataFrame);
  };
  const top=element(document,'header','df-book-header'),title=element(document,'strong');
  const nativeMode=element(document,'button','df-view-switch','完整 Notebook');nativeMode.type='button';
  const aside=element(document,'aside','df-card-assistant');aside.setAttribute('aria-label','卡片助手');
  const head=element(document,'header','df-assistant-head'),heading=element(document,'strong',null,'卡片助手'),targetName=element(document,'small','df-target-name');
  const thread=element(document,'div','df-card-thread');thread.setAttribute('aria-live','polite');
  const form=element(document,'form','df-card-composer'),reference=element(document,'div','df-reference');
  const input=element(document,'textarea');input.rows=2;input.placeholder='描述你想做的分析或标注…';input.setAttribute('aria-label','卡片要求');
  const send=icon(document,'up','发送要求');send.type='submit';
  const messages=new Map(),drafts=new Map(),refs=new Map(),decorated=new Map();
  const conversations=new Map(),inFlight=new Set();
  let selectedId=null,dead=false,classic=false,assistantOpen=false,paintFrame=0;
  let path=panel.context.path;
  const viewKey=()=>JSON.stringify(['demiflow.notebook.cards',project,path]);
  const valid=()=>!dead&&alive()&&!panel.isDisposed;
  const writable=()=>!!permissions().analysis_write&&!panel.model?.readOnly;
  const cells=()=>book.widgets;
  const selected=()=>book.activeCell?.model?.type==='code'?book.activeCell:null;
  const cellById=id=>cells().find(cell=>cell.model.id===id);
  function assistantFor(id){
    if(!conversations.has(id))conversations.set(id,notebookConversation({api,project,path,cell:id,active:valid}));
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
  function rename(){if(path!==panel.context.path)conversations.clear();path=panel.context.path;title.textContent=path.split('/').pop().replace(/\.ipynb$/,'');}
  function focusCell(cell){book.deselectAll();book.activeCellIndex=cells().indexOf(cell);}
  function newCell(source=''){
    if(!writable())return null;
    const index=Math.max(0,book.activeCellIndex+1);
    book.model.sharedModel.insertCell(index,{id:crypto.randomUUID(),cell_type:'code',source,metadata:{},outputs:[],execution_count:null});
    book.deselectAll();book.activeCellIndex=index;book.activeCell?.node.scrollIntoView({block:'nearest'});return book.activeCell;
  }
  function showAssistant(value){
    assistantOpen=value;desk.dataset.assistant=value?'open':'closed';aside.hidden=!value;toggle.setAttribute('aria-expanded',String(value));divider.setActive(value);
    if(value){renderThread();input.focus();}
    layout();
  }
  function setMode(value){
    classic=value;doc.body.classList.toggle('df-card-mode',!classic);
    nativeMode.textContent=classic?'卡片视图':'完整 Notebook';
    if(classic)showAssistant(false);
    win.dispatchEvent(new win.Event('resize'));
  }
  const add=icon(document,'plus','新建卡片',()=>{const cell=newCell();cell?.editor?.focus();});
  const toggle=icon(document,'chat','卡片助手',()=>showAssistant(!assistantOpen));
  add.disabled=!writable();top.append(title,add,nativeMode,toggle);documentPane.prepend(top);
  head.append(heading,targetName,icon(document,'close','关闭助手',()=>showAssistant(false)));form.append(reference,input,send);aside.append(head,thread,form);desk.append(aside);
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
    selectedId=selected()?.model.id||null;input.value=drafts.get(selectedId)||'';
    if(assistantOpen)renderThread();
    schedulePaint();
  }
  function renderThread(){
    thread.replaceChildren();reference.replaceChildren();
    const cell=selected(),id=cell?.model.id;
    targetName.textContent=cell?(cell.model.getMetadata('demiflow_card_title')||'当前卡片 '+(book.activeCellIndex+1)):'';
    const items=refs.get(id)||[];
    for(const item of items){
      const chip=element(document,'span','df-tool-chip',item.symbol);
      chip.append(icon(document,'close','取消引用 '+item.symbol,()=>{refs.set(id,items.filter(i=>i.id!==item.id));renderThread();}));reference.append(chip);
    }
    const pending=id?assistantFor(id).pending:null;
    const busy=inFlight.has(id);
    input.disabled=busy||!writable()||!id;send.disabled=input.disabled;
    if(!id){thread.append(element(document,'p','df-chat-status','选中一张代码卡片即可调整。'));return;}
    for(const row of messages.get(id)||[]){
      thread.append(element(document,'p','df-card-request',row.request));
      if(row.text)thread.append(element(document,'p','df-card-reply',row.text));
      if(row.code){
        const preview=element(document,'details','df-proposal'),summary=element(document,'summary',null,'查看生成代码');
        preview.append(summary,element(document,'pre',null,row.code));thread.append(preview);
        const apply=element(document,'button','df-apply',sourceOf(cell)===row.source?'应用到这张卡片':'作为新卡片插入');
        apply.disabled=!writable();apply.onclick=()=>applyCode(row,cell);thread.append(apply);
      }
    }
    if(busy)thread.append(element(document,'p','df-chat-status','助手正在生成…'));
    else if(pending?.cell===id){
      const resume=element(document,'button','df-apply','取回上次回复');resume.onclick=()=>submit(pending.request);thread.append(resume);
    }
    thread.scrollTop=thread.scrollHeight;
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
  async function submit(request){
    const cell=selected();if(!cell||inFlight.has(cell.model.id)||!request.trim()||!writable())return;
    const id=cell.model.id,source=sourceOf(cell),index=book.activeCellIndex,initialPath=path;
    const context={path,cell:id,source,before:cells().slice(Math.max(0,index-3),index).filter(c=>c.model.type==='code').map(sourceOf).join('\n'),
      output:cell.outputArea?.node.innerText||'',tables:tables(),selected:refs.get(id)||[]};
    inFlight.add(id);schedulePaint();renderThread();
    try{
      const result=await assistantFor(id).generate(request,context);
      if(!valid()||path!==initialPath)return;
      const row={request:result.request,source:result.source,code:result.code,text:result.message};
      drafts.delete(id);if(selectedId===id)input.value='';saveMessage(id,row);
      const target=cellById(id);
      if(row.code&&target&&sourceOf(target)===row.source)applyCode(row,target);
    }catch(error){if(valid())saveMessage(id,{request,text:error.message});}
    finally{inFlight.delete(id);if(valid()){schedulePaint();renderThread();}}
  }
  form.onsubmit=e=>{e.preventDefault();submit(input.value);};
  input.onkeydown=e=>{if(e.key==='Enter'&&!e.shiftKey&&!e.isComposing){e.preventDefault();form.requestSubmit();}};
  function onExecution(model,change){
    if(change.name==='executionState'&&change.newValue==='running'&&model.getMetadata('demiflow_card_needs_run')){
      model.deleteMetadata('demiflow_card_needs_run');schedulePaint();
    }
  }
  function decorate(){
    paintFrame=0;if(!valid())return;
    for(const [cell,bar] of decorated)if(cell.isDisposed){bar.remove();decorated.delete(cell);}
    for(const cell of cells()){
      if(cell.model.type!=='code'||!cell.node.isConnected)continue;
      let bar=decorated.get(cell);
      if(!bar){
        bar=element(doc,'header','df-cell-tools');const name=element(doc,'span','df-cell-title');
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
            await panel.sessionContext.ready;
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
        run.disabled=!permissions().analysis_execute;bar.append(name,status,edit,run);
        cell.model.stateChanged.connect(onExecution);
        cell.node.prepend(bar);decorated.set(cell,bar);
        cell.node.dataset.dfFolded=String(cell.model.outputs.length>0&&!cell.model.getMetadata('demiflow_card_needs_run'));
      }
      if(bar.parentNode!==cell.node)cell.node.prepend(bar);
      const expanded=cell.node.dataset.dfFolded==='false';
      const status=bar.children[1];
      const state=inFlight.has(cell.model.id)?'正在更新…':cell.model.getMetadata('demiflow_card_needs_run')?'代码已更新 · 待运行':'';
      if(status.textContent!==state)status.textContent=state;
      const edit=bar.children[2],label=expanded&&cell.model.outputs.length?'收起代码':'编辑代码';
      edit.setAttribute('aria-expanded',String(expanded));edit.setAttribute('aria-label',label);edit.title=label;
      const source=sourceOf(cell);
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
  book.activeCellChanged.connect(trackSelection);panel.model.contentChanged.connect(changed);panel.context.pathChanged.connect(rename);nativeMode.onclick=()=>setMode(!classic);
  input.maxLength=8192;input.onchange=remember;win.addEventListener('pagehide',remember);
  rename();setMode(false);showAssistant(savedOpen===true);trackSelection();decorate();
  return {ask,dispose(){
    remember();dead=true;outputObserver.disconnect();book.node.querySelectorAll('.df-dataframe-view').forEach(view=>view.remove());win.removeEventListener('pagehide',remember);observer.disconnect();win.cancelAnimationFrame(paintFrame);book.activeCellChanged.disconnect(trackSelection);panel.model?.contentChanged.disconnect(changed);panel.context.pathChanged.disconnect(rename);
    divider.dispose();for(const [cell,bar] of decorated){cell.model.stateChanged.disconnect(onExecution);bar.remove();}style.remove();doc.body.classList.remove('df-card-mode');aside.remove();top.remove();delete desk.dataset.assistant;
  }};
}
