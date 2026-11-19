/* One data dictionary alongside the real Notebook 7 application. */
(() => {
  'use strict';
  const node = (tag, text, cls) => { const el=document.createElement(tag);if(text!==undefined)el.textContent=text;if(cls)el.className=cls;return el; };
  const sessions = new Map();
  const notebookPanel = session => {
    const app=session.frame.contentWindow?.jupyterapp;
    if(!app)return null;
    const current=app.shell.currentWidget;
    if(current?.content?.model?.sharedModel)return current;
    try{return [...app.shell.widgets('main')].find(item=>item.content?.model?.sharedModel)||null;}catch{return null;}
  };
  const dirty = session => !!notebookPanel(session)?.context?.model?.dirty;
  window.addEventListener('beforeunload', event => {
    if([...sessions.values()].some(dirty)){event.preventDefault();event.returnValue='';}
  });
  function position(session) {
    const visible=session.host?.isConnected&&!session.host.hidden;
    session.frame.hidden=!visible;
    if(!visible)return;
    const rect=session.host.getBoundingClientRect();
    Object.assign(session.frame.style,{left:rect.left+'px',top:rect.top+'px',width:rect.width+'px',height:rect.height+'px'});
  }
  function prepareFrame(session) {
    const frame=session.frame;
    frame.addEventListener('load',()=>{
      const win=frame.contentWindow;
      // Notebook's own File → Open/New actions remain in this native pane.
      // Every destination must remain under this project's proxy prefix.
      const follow=url=>{
        const target=new URL(url,win.location.href);
        if(target.origin===location.origin&&target.pathname.startsWith(session.base)){
          win.location.assign(target.href);return win;
        }
        return window.open(target.href,'_blank','noopener,noreferrer');
      };
      win.open=follow;
      win.document.addEventListener('click',event=>{
        const link=event.target.closest?.('a[href]');
        if(link?.target==='_blank'){
          const target=new URL(link.href,win.location.href);
          if(target.origin===location.origin&&target.pathname.startsWith(session.base)){event.preventDefault();follow(target.href);}
        }
      },true);
      // Remember the native document URL; a reload reopens it with Jupyter's
      // persisted content and existing session, without replaying any cell.
      if(win.location.pathname.startsWith(session.base)){
        try{sessionStorage.setItem('demiflow-jupyter:'+session.project,win.location.pathname);}catch{}
      }
      const epoch=(session.loadEpoch||0)+1;session.loadEpoch=epoch;
      (async()=>{
        // Notebook exposes its application after the iframe load event. Wait
        // for that native lifecycle before subscribing to document renames.
        const until=Date.now()+30000;
        while(!win.jupyterapp&&Date.now()<until&&session.loadEpoch===epoch&&frame.isConnected)
          await new Promise(resolve=>setTimeout(resolve,100));
        if(!win.jupyterapp||session.loadEpoch!==epoch||!frame.isConnected)return;
        await win.jupyterapp.restored;
        const panel=notebookPanel(session);
        if(!panel)return;
        await panel.context.ready;
        const remember=()=>{
          const path=panel.context.path.split('/').map(encodeURIComponent).join('/');
          try{sessionStorage.setItem('demiflow-jupyter:'+session.project,session.base+'notebooks/'+path);}catch{}
        };
        panel.context.pathChanged.connect(remember);remember();
      })().catch(()=>{});
    });
  }
  async function attach(host, project, api, alive) {
    if(!sessions.has(project)&&sessions.size>=4){
      const old=[...sessions.values()].find(item=>!item.host?.isConnected&&!dirty(item)
        &&!['busy','starting','restarting'].includes(notebookPanel(item)?.sessionContext?.kernelDisplayStatus));
      if(!old)throw Error('已有四个活动 Notebook 工作区，请先保存并等待运行结束。');
      // Disconnect only our clean inactive pane. The backend can retire its
      // server only when no other browser is connected and no kernel is busy.
      old.frame.remove();sessions.delete(old.project);
      await new Promise(resolve=>setTimeout(resolve,100));
    }
    let opened;
    for(let attempt=0;;attempt++){
      try{opened=await api('notebook-open',{},{});break;}
      catch(error){
        if(error.code!=='resource_limit'||attempt>=3||!alive())throw error;
        await new Promise(resolve=>setTimeout(resolve,200*(attempt+1)));
      }
    }
    if(!alive())return null;
    try{await recoverDrafts(project,opened);}
    catch(error){
      // A malformed old draft must remain recoverable without blocking the
      // independent native workspace. Its original localStorage is untouched.
      if(alive())host.parentElement.querySelector('.data-dictionary').append(node('p','旧草稿未自动导入，原记录已保留。'+error.message,'dictionary-meta'));
    }
    if(!alive())return null;
    let session=sessions.get(project);
    if(!session){
      const frame=node('iframe',undefined,'native-notebook-frame');
      frame.title='Jupyter Notebook';frame.referrerPolicy='same-origin';
      session={frame,project,base:opened.base_url,host:null};prepareFrame(session);
      let path=opened.url;
      try{const previous=sessionStorage.getItem('demiflow-jupyter:'+project);if(previous?.startsWith(opened.base_url)&&!previous.includes('..'))path=previous;}catch{}
      frame.src=path;frame.hidden=true;document.body.append(frame);sessions.set(project,session);
    }
    session.host=host;position(session);return session;
  }
  async function recoverDrafts(project,opened) {
    if(!opened.permissions.analysis_write)return;
    let raw;
    try{raw=localStorage.getItem('demiflow-analysis-drafts-v1');}catch{return;}
    if(!raw)return;
    if(raw.length>4*1024*1024)throw Error('旧 Notebook 草稿超出恢复上限，原草稿已保留。');
    const envelope=JSON.parse(raw);
    if(envelope.v!==1||!Array.isArray(envelope.items)||envelope.items.length>32)throw Error('旧 Notebook 草稿格式无法识别，原草稿已保留。');
    const drafts=envelope.items.filter(item=>item.project===project);
    if(drafts.length>4)throw Error('旧 Notebook 草稿数量超过恢复上限，原草稿已保留。');
    for(const draft of drafts){
      if(!Array.isArray(draft.document?.cells)||draft.document.cells.length>64)throw Error('旧 Notebook 草稿单元无法恢复。');
      const hash=[...new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(JSON.stringify(draft))))].map(x=>x.toString(16).padStart(2,'0')).join('').slice(0,16);
      const path='恢复草稿-'+hash+'.ipynb';
      const url=opened.base_url+'api/contents/'+encodeURIComponent(path);
      const exists=await fetch(url+'?content=0',{credentials:'same-origin'});
      if(exists.ok)continue;
      if(exists.status!==404)throw Error('旧草稿恢复暂不可用，原草稿仍保留在浏览器中。');
      const cells=draft.document.cells.map((cell,index)=>({id:'restored-'+index,
        cell_type:cell.language==='markdown'?'markdown':cell.language==='sql'?'raw':'code',
        metadata:{demiflow:{language:cell.language}},source:cell.source,
        ...(cell.language==='python'?{execution_count:null,outputs:[]}:{} )}));
      const content={nbformat:4,nbformat_minor:5,metadata:{demiflow:{legacy_draft:{id:draft.id,revision:draft.revision},title:draft.document.title}},cells};
      const response=await fetch(url,{method:'PUT',credentials:'same-origin',headers:{'Content-Type':'application/json'},body:JSON.stringify({type:'notebook',format:'json',content})});
      if(!response.ok)throw Error('旧草稿未能导入 Jupyter，原草稿仍保留在浏览器中。');
      // Keep the original browser record. A later edited native copy is never
      // overwritten, and returning to the old release remains recoverable.
    }
  }
  async function insert(session, source) {
    const panel=notebookPanel(session);
    if(!panel)throw Error('请先在右侧打开一个 Notebook。');
    const notebook=panel.content,model=notebook.model.sharedModel;
    const active=notebook.activeCell;
    if(active?.model?.type==='code'&&!active.model.sharedModel.getSource().trim()){
      active.model.sharedModel.setSource(source);
    }else{
      const index=Math.max(0,notebook.activeCellIndex+1);
      model.insertCell(index,{id:crypto.randomUUID(),cell_type:'code',source,metadata:{},outputs:[],execution_count:null});
      notebook.activeCellIndex=index;
    }
    notebook.activeCell?.editor?.focus();
  }
  async function mount({parent,api,project,context,request,originalPath,onBinding}) {
    parent.classList.add('native-data-active');
    const root=node('div',undefined,'native-data-workspace');parent.append(root);
    const dictionary=node('aside',undefined,'data-dictionary');dictionary.setAttribute('aria-label','数据字典');
    const header=node('div',undefined,'dictionary-heading');header.append(node('h2','数据字典'));
    const search=node('input');search.type='search';search.placeholder='搜索表或字段';search.setAttribute('aria-label','搜索数据字典');search.maxLength=128;
    dictionary.append(header,search);
    const list=node('div',undefined,'dictionary-list');dictionary.append(list);
    const native=node('div',undefined,'native-notebook-host');native.setAttribute('aria-label','Notebook 工作区');
    const status=node('p','正在连接 Jupyter…','native-notebook-status');native.append(status);root.append(dictionary,native);
    let disposed=false,session=null;
    const alive=()=>!disposed&&root.isConnected;
    const resize=()=>{root.style.height=Math.max(240,innerHeight-root.getBoundingClientRect().top)+'px';if(session)position(session);};
    const observer=new ResizeObserver(resize);observer.observe(native);window.addEventListener('resize',resize);window.addEventListener('scroll',resize,true);
    const fail=error=>{if(alive()){status.textContent=error.message||String(error);status.classList.add('error-box');}};
    const tables=new Map();
    function add(item) {
      if(tables.has(item.key))return;
      const row=node('details',undefined,'dictionary-table');row.dataset.table=item.key;
      const summary=node('summary'),name=node('span',item.label,'dictionary-name');name.title=item.label;
      const version=node('span',item.version?'v'+item.version:'','dictionary-version');summary.append(name,version);row.append(summary);
      const content=node('div',undefined,'dictionary-fields');row.append(content);list.append(row);
      const entry={row,item,content,loaded:false,fieldNames:[]};tables.set(item.key,entry);
      row.addEventListener('toggle',async()=>{
        if(!row.open||entry.loaded||entry.loading)return;
        entry.loading=true;content.replaceChildren(node('p','读取字段…','dictionary-meta'));
        try{
          const expected=context.project?.digest;
          const expectedBinding=request.alias===item.alias
            ?(request.kind==='table_request'?request.expected_binding:request.binding_digest)||item.binding
            :item.binding;
          const result=item.original
            ?await api('project-table',{id:item.original.id,expected,...(item.original.version?{version:item.original.version}:{})})
            :await api('table',{alias:item.alias,...(expectedBinding?{expected_binding:expectedBinding}:{}),
                ...(request.alias===item.alias&&request.version?{version:request.version,version_binding:request.binding_digest,expected_binding:request.alias_binding}:{})});
          if(!alive()||tables.get(item.key)!==entry||!row.isConnected)return;
          const fields=item.original?result.fields:result.metadata?.fields||{};
          const fixedVersion=item.original?result.version:result.binding.reference.ref.lance_version;
          const rows=item.original?result.row_count:result.binding.reference.ref.row_count;
          const binding=item.original?result.binding:result.binding.digest;
          version.textContent='v'+fixedVersion;
          entry.fieldNames=Object.keys(fields);entry.loaded=true;content.replaceChildren();
          content.append(node('p',rows+' 行 · '+entry.fieldNames.length+' 字段','dictionary-meta'));
          for(const field of entry.fieldNames){
            const line=node('div',undefined,'dictionary-field');line.append(node('span',field),node('small',fields[field]?.type?.kind||fields[field]?.type||'—'));content.append(line);
          }
          const actions=node('div',undefined,'dictionary-actions');content.append(actions);
          const insertButton=node('button','插入读取代码','dictionary-insert');insertButton.type='button';
          insertButton.title='插入固定版本的读取代码，不自动运行';insertButton.disabled=!context.capabilities?.analysis_write;
          insertButton.onclick=async()=>{
            try{
              const columns=entry.fieldNames.slice(0,16);
              const encoded=JSON.stringify;
              let code;
              if(item.original){
                code='from demiflow.workbench.notebooks.data import project_sample\n\ndf = project_sample('+encoded(item.original.id)+', expected='+encoded(expected)+', version='+fixedVersion+', binding='+encoded(binding)+', columns='+encoded(columns)+', limit=20)\ndf';
              }else{
                const historical=request.alias===item.alias&&!!request.version;
                code='from demiflow.workbench.notebooks.data import sample\n\ndf = sample('+encoded(item.alias)+', binding='+encoded(historical?request.alias_binding:binding)+(historical?', version='+fixedVersion+', version_binding='+encoded(binding):'')+', columns='+encoded(columns)+', limit=20)\ndf';
              }
              if(!session)throw Error('Jupyter 尚未连接。');
              await insert(session,code);
            }catch(error){const message=node('p',error.message,'dictionary-meta');content.append(message);setTimeout(()=>message.remove(),5000);}
          };
          actions.append(insertButton);
          if(request.kind==='table_request'&&request.alias===item.alias){
            const resolved={v:2,workspace:project,kind:'table',alias:item.alias,binding_digest:binding,
              ...(request.version?{version:request.version,alias_binding:request.alias_binding}:{}),
              ...(request.columns?{columns:request.columns}:{}),...(request.offset!==undefined?{offset:request.offset}:{})};
            request=resolved;onBinding?.(resolved);
          }
        }catch(error){if(alive())content.replaceChildren(node('p',error.message,'dictionary-meta'));}
        finally{entry.loading=false;}
      });
      if(request.alias===item.alias||originalPath===item.original?.path)row.open=true;
    }
    function populate(fresh) {
      const entries=[...(fresh.tables||[]).map(table=>({key:'bound:'+table.alias,label:table.alias,alias:table.alias,binding:table.binding_digest,version:table.reference.ref.lance_version})),
        ...(fresh.project?.tables||[]).filter(ref=>ref.status==='available').map(ref=>({key:'original:'+ref.id,label:ref.path,original:ref,version:ref.version,expected:fresh.project.digest}))];
      const current=new Map(entries.map(item=>[item.key,item]));
      for(const [key,entry] of tables){
        if(!current.has(key)||JSON.stringify(current.get(key))!==JSON.stringify(entry.item)){
          entry.row.remove();tables.delete(key);
        }
      }
      for(const item of entries)add(item);
      list.querySelector('.dictionary-empty')?.remove();
      if(!tables.size)list.append(node('p','尚未登记数据表。其他输入文件可以直接在 Notebook 中读取。','dictionary-empty'));
    }
    populate(context);
    search.oninput=()=>{
      const value=search.value.trim().toLocaleLowerCase();
      for(const {row,item,fieldNames} of tables.values())row.hidden=![item.label,...fieldNames].some(name=>name.toLocaleLowerCase().includes(value));
    };
    if(context.native_notebook){
      attach(native,project,api,alive).then(value=>{if(!alive())return;session=value;status.hidden=!!value;resize();}).catch(fail);
    }else{status.textContent='此服务尚未启用原生 Jupyter Notebook。';}
    return {
      canLeave:()=>true,
      updateContext(fresh){if(alive()){context=fresh;populate(fresh);}},
      dispose(){if(disposed)return;disposed=true;observer.disconnect();window.removeEventListener('resize',resize);window.removeEventListener('scroll',resize,true);if(session){session.host=null;position(session);}parent.classList.remove('native-data-active');}
    };
  }
  window.ForgeNotebooks=Object.freeze({mount});
})();
