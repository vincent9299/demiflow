/* One data dictionary alongside the real Notebook 7 application. */
(() => {
  'use strict';
  const node = (tag, text, cls) => { const el=document.createElement(tag);if(text!==undefined)el.textContent=text;if(cls)el.className=cls;return el; };
  const sessions = new Set();
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
  function connectWidgets(win,panel){
    const config=JSON.parse(win.document.getElementById('jupyter-config-data').textContent);
    if(!config.demiflowDeferredWidgets)return;
    const extension=win._JUPYTERLAB?.['@jupyter-widgets/jupyterlab-manager'];
    if(!extension)return;
    const context=panel.sessionContext;
    async function activate(){
      if(!context.session?.kernel)return;
      context.kernelChanged.disconnect(activate);
      const factory=await extension.get('./extension');
      const plugins=factory().default,app=win.jupyterapp;
      for(const plugin of plugins)if(!app.hasPlugin(plugin.id))app.registerPlugin(plugin);
      await Promise.all(plugins.filter(plugin=>plugin.autoStart).map(plugin=>app.activatePlugin(plugin.id)));
    }
    context.kernelChanged.connect(activate);
    activate();
  }
  function prepareFrame(session) {
    const frame=session.frame;
    frame.addEventListener('load',()=>{
      session.readyPanel=null;session.onPanel?.(null);
      const win=frame.contentWindow;
      frame.toggleAttribute('data-loading',win.location.pathname.startsWith(session.base+'notebooks/'));
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
        session.url=win.location.pathname;
        if(session.host)try{sessionStorage.setItem('demiflow-jupyter:'+session.project,session.url);}catch{}
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
        await panel.sessionContext.ready;
        connectWidgets(win,panel);
        const remember=()=>{
          const path=panel.context.path.split('/').map(encodeURIComponent).join('/');
          const previous=session.url;session.url=session.base+'notebooks/'+path;
          if(session.host)try{sessionStorage.setItem('demiflow-jupyter:'+session.project,session.url);}catch{}
          session.onPath?.(previous,session.url);
        };
        panel.context.pathChanged.connect(remember);remember();
        session.readyPanel=panel;session.onPanel?.(panel);
      })().catch(()=>{}).finally(()=>{
        if(session.loadEpoch!==epoch)return;
        frame.removeAttribute('data-loading');
        session.onReady?.();
      });
    });
  }
  function makeRoom(){
    if(sessions.size<4)return;
    const old=[...sessions].find(item=>!item.host?.isConnected&&item.readyPanel&&!dirty(item)
      &&!['busy','starting','restarting'].includes(item.readyPanel.sessionContext?.kernelDisplayStatus));
    if(!old)throw Error('打开的 Notebook 已满，请先保存修改并等待运行完成。');
    old.frame.remove();sessions.delete(old);
  }
  function attach(host,project,base,url){
    let session=[...sessions].find(item=>item.project===project&&item.url===url);
    if(!session){
      makeRoom();
      const frame=node('iframe',undefined,'native-notebook-frame');
      frame.title='Jupyter Notebook';frame.referrerPolicy='same-origin';
      session={frame,project,base,url,host:null};prepareFrame(session);
      frame.toggleAttribute('data-loading',url.startsWith(base+'notebooks/'));
      frame.src=url;frame.hidden=true;document.body.append(frame);sessions.add(session);
    }
    session.host=host;position(session);
    try{sessionStorage.setItem('demiflow-jupyter:'+project,url);}catch{}
    return session;
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
    const panel=session&&notebookPanel(session);
    if(!panel)throw Error('请先在右侧打开一个 Notebook。');
    const notebook=panel.content,model=notebook.model.sharedModel;
    const active=notebook.activeCell;
    if(active?.model?.type==='code'&&!active.model.sharedModel.getSource().trim()){
      active.model.sharedModel.setSource(source);
    }else{
      const index=Math.max(0,notebook.activeCellIndex+1);
      model.insertCell(index,{id:crypto.randomUUID(),cell_type:'code',source,metadata:{},outputs:[],execution_count:null});
      notebook.deselectAll();
      notebook.activeCellIndex=index;
    }
    notebook.activeCell?.editor?.focus();
  }
  async function mount({parent,api,project,context,request,originalPath,onBinding,mountText,disposeReaders,notebookAccess}) {
    parent.classList.add('native-data-active');
    const root=node('div',undefined,'native-data-workspace');parent.append(root);
    const dictionary=node('aside',undefined,'data-dictionary');dictionary.setAttribute('aria-label','数据导航');
    const navigation=ForgeNavigation.header({title:'数据',searchLabel:'搜索数据字典',placeholder:'表名或已读取字段',onSearch:()=>filterLists()});
    const {search}=navigation;
    const list=node('div',undefined,'dictionary-list');
    const panes={dictionary:list,tools:node('div')};
    const labels={dictionary:'数据字典',tools:'工具'};
    const filters={dictionary:'',tools:''};
    let activeTab='dictionary',toolIndex=null,loadingTools=false;
    const tabs=ForgeNavigation.categoryTabs('数据分类',Object.entries(labels),key=>{
      filters[activeTab]=search.value;activeTab=key;search.value=filters[key];
      navigation.searchScope('搜索'+labels[key],key==='dictionary'?'表名或已读取字段':'名称、说明或文件名');
      for(const [name,pane] of Object.entries(panes))pane.hidden=name!==key;
      filterLists();if(key!=='dictionary')loadTools();
    });
    for(const tab of tabs.element.children){
      const key=tab.dataset.kind,pane=panes[key];tab.id='data-tab-'+key;pane.id='data-pane-'+key;
      pane.setAttribute('role','tabpanel');pane.setAttribute('aria-labelledby',tab.id);
      tab.setAttribute('aria-controls',pane.id);pane.hidden=key!==activeTab;
    }
    dictionary.append(navigation.element,tabs.element,...Object.values(panes));
    const desk=node('section',undefined,'notebook-desk'),documentPane=node('div',undefined,'notebook-document');
    const native=node('div',undefined,'native-notebook-host');native.setAttribute('aria-label','Notebook 工作区');
    const status=node('p','正在连接 Jupyter…','native-notebook-status');native.append(status);
    const header=node('header',undefined,'df-book-header');
    const strip=node('div',undefined,'df-book-strip'),bookTabs=node('div',undefined,'df-book-tabs');
    bookTabs.setAttribute('role','tablist');bookTabs.setAttribute('aria-label','Notebook');
    const create=node('button','＋','df-new-book');create.type='button';create.title='新建 Notebook';create.setAttribute('aria-label',create.title);
    const enableRun=node('button','启用运行','df-view-switch');
    enableRun.title='允许此项目的 Notebook 运行 Python 代码';
    const controls=node('div',undefined,'df-book-controls');
    strip.append(bookTabs,create);header.append(strip,enableRun,controls);
    const notice=node('p',undefined,'df-book-notice');notice.setAttribute('role','status');notice.hidden=true;
    documentPane.append(header,notice,native);desk.append(documentPane);root.append(dictionary,desk);
    const dictionaryNavigation=ForgeNavigation.collapsible({layout:root,sidebar:dictionary,header:navigation,label:'数据字典目录'});
    let disposed=false,session=null;
    const alive=()=>!disposed&&root.isConnected;
    const resize=()=>{root.style.height=Math.max(240,innerHeight-root.getBoundingClientRect().top)+'px';if(session)position(session);};
    const observer=new ResizeObserver(resize);observer.observe(native);window.addEventListener('resize',resize);window.addEventListener('scroll',resize,true);
    const fail=error=>{if(alive()){notice.textContent=error.message||String(error);notice.hidden=false;}};
    let toolsDialog,cards,cardsEpoch=0;
    async function openTool(item){
      try{
        const {showTool}=await import('./data-tools.js');
        if(alive()&&!toolsDialog?.open)toolsDialog=showTool({root,item,api,mountText,disposeReaders,project,reference:cards?()=>cards.ask(item):null,
          canInsert:!!context.native_notebook&&!!context.capabilities?.analysis_write,insert:code=>insert(session,code)});
      }catch(error){fail(error);}
    }
    const expandedTools=new Set();
    function paintTools(host,items,query){
      host.replaceChildren();
      const files=new Map();
      for(const item of items){
        if(![item.symbol,item.path,item.description].join(' ').toLocaleLowerCase().includes(query))continue;
        if(!files.has(item.path))files.set(item.path,[]);
        files.get(item.path).push(item);
      }
      for(const [path,entries] of files){
        const file=node('details',undefined,'dictionary-table');file.open=!!query||expandedTools.has(path);
        file.ontoggle=()=>{if(!query&&file.isConnected){if(file.open)expandedTools.add(path);else expandedTools.delete(path);}};
        const label=entries[0].origin==='platform'?'内置 · '+entries[0].module:path;
        const heading=node('summary');heading.title=path;heading.append(node('span',label,'dictionary-name'));file.append(heading);
        for(const item of entries){
          const button=node('button',undefined,'data-tool-leaf');button.type='button';
          const type={analysis:'分析',tool:'工具',view:'查看',annotation:'标注',annotation_component:'组件'}[item.tool_kind];
          button.append(node('span',item.symbol),node('small',type));
          button.title=item.description||item.path;
          button.onclick=()=>cards?cards.ask(item):openTool(item);
          const row=node('div',undefined,'data-tool-row'),info=node('button','···','data-tool-info');
          info.type='button';info.title='查看 '+item.symbol+' 定义';info.setAttribute('aria-label',info.title);
          info.onclick=()=>openTool(item);row.append(button,info);file.append(row);
        }
        host.append(file);
      }
    }
    async function loadTools(){
      if(!toolIndex)panes[activeTab].textContent='正在读取'+labels[activeTab]+'…';
      if(loadingTools)return;
      loadingTools=true;
      try{
        const index=await api('project-data-tools');
        if(!alive())return;
        toolIndex=index;filterLists();
      }catch(error){
        if(alive()&&activeTab!=='dictionary'){
          const pane=panes[activeTab];pane.replaceChildren(node('p',error.message,'op-error'));delete pane.dataset.revision;
        }
      }finally{loadingTools=false;}
    }
    const tables=new Map();
    let directory={tables:[],diagnostics:[]},scan=0,scanning;
    async function scanDirectory(){
      const epoch=++scan;
      try{const result=await api('dictionary-sources');if(!alive()||epoch!==scan)return;directory=result;}
      catch(error){if(!alive()||epoch!==scan)return;directory={tables:[],diagnostics:[error.message]};}
      populate(context);
    }
    async function tableDetail(item){
      const expected=item.expected||context.project?.digest;
      const expectedBinding=request.alias===item.alias
        ?(request.kind==='table_request'?request.expected_binding:request.binding_digest)||item.binding
        :item.binding;
      const result=item.original
        ?await api('project-table',{id:item.original.id,expected,...(item.original.version?{version:item.original.version}:{})})
        :await api('table',{alias:item.alias,...(expectedBinding?{expected_binding:expectedBinding}:{}),
            ...(request.alias===item.alias&&request.version?{version:request.version,version_binding:request.binding_digest,expected_binding:request.alias_binding}:{})});
      const fields=item.original?result.fields:result.metadata?.fields||{};
      const fixedVersion=item.original?result.version:result.binding.reference.ref.lance_version;
      const rows=item.original?result.row_count:result.binding.reference.ref.row_count;
      const binding=item.original?result.binding:result.binding.digest;
      const columns=Object.keys(fields).slice(0,16);
      const encoded=JSON.stringify;
      let code;
      if(item.original){
        code='from demiflow.workbench.notebooks.data import project_sample\n\ndf = project_sample('+encoded(item.original.id)+', expected='+encoded(expected)+', version='+fixedVersion+', binding='+encoded(binding)+', columns='+encoded(columns)+', limit=20)\ndf';
      }else{
        const historical=request.alias===item.alias&&!!request.version;
        code='from demiflow.workbench.notebooks.data import sample\n\ndf = sample('+encoded(item.alias)+', binding='+encoded(historical?request.alias_binding:binding)+(historical?', version='+fixedVersion+', version_binding='+encoded(binding):'')+', columns='+encoded(columns)+', limit=20)\ndf';
      }
      return {fields,fixedVersion,rows,binding,code};
    }
    async function catalogCommand(command,args){
      const dictionary=args.group==='dictionary';
      if(dictionary)await scanning;
      const tools=dictionary?[]:(await api('project-data-tools')).definitions;
      if(command==='catalog.list'){
        const values=dictionary?[...tables.values()].map(({item})=>({id:item.key,name:item.label,version:item.version})):
          tools.map(item=>({id:item.id,name:item.symbol,module:item.module,origin:item.origin,kind:item.tool_kind}));
        const offset=args.offset,items=values.slice(offset,offset+12);
        return {items,next_offset:offset+items.length<values.length?offset+items.length:null};
      }
      if(dictionary){
        const entry=tables.get(args.id);if(!entry)throw Error('数据目录已变化，请重新查询。');
        const {fields,fixedVersion,rows,binding,code}=await tableDetail(entry.item);
        const names=Object.keys(fields),offset=args.offset||0;
        return {version:fixedVersion,rows,binding,read:code,fields:names.slice(offset,offset+12).map(name=>[name,fields[name]?.type?.kind||fields[name]?.type||'']),
          next_offset:offset+12<names.length?offset+12:null};
      }
      const item=tools.find(i=>i.id===args.id);if(!item)throw Error('未找到此工具。');
      const info=await api('data-tool-definition',{id:item.id});
      return {symbol:info.symbol,module:info.module,origin:info.origin,kind:info.tool_kind,
        signature:info.signature,description:(info.description||'').slice(0,2000),
        import:info.module?'from '+info.module+' import '+info.symbol:null};
    }
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
          const {fields,fixedVersion,rows,binding,code}=await tableDetail(item);
          if(!alive()||tables.get(item.key)!==entry||!row.isConnected)return;
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
        finally{entry.loading=false;if(alive())filterLists();}
      });
      if((request.alias && request.alias===item.alias)||(originalPath && originalPath===item.original?.path))row.open=true;
    }
    function populate(fresh) {
      const covered=new Set([...(fresh.tables||[]).map(t=>t.uri),...(fresh.project?.tables||[]).filter(t=>t.status==='available').map(t=>fresh.workspace+'/'+t.path)]);
      const entries=[...(fresh.tables||[]).map(table=>({key:'bound:'+table.alias,label:table.alias,alias:table.alias,binding:table.binding_digest,version:table.reference.ref.lance_version})),
        ...(fresh.project?.tables||[]).filter(ref=>ref.status==='available').map(ref=>({key:'original:'+ref.id,label:ref.path,original:ref,version:ref.version,expected:fresh.project.digest})),
        ...directory.tables.filter(ref=>!covered.has(ref.uri)).map(ref=>({key:'directory:'+ref.id,label:ref.path,original:ref,version:ref.version,expected:ref.expected}))];
      const current=new Map(entries.map(item=>[item.key,item]));
      for(const [key,entry] of tables){
        if(!current.has(key)||JSON.stringify(current.get(key))!==JSON.stringify(entry.item)){
          entry.row.remove();tables.delete(key);
        }
      }
      for(const item of entries)add(item);
      for(const button of list.querySelectorAll('.dictionary-insert'))button.disabled=!fresh.capabilities?.analysis_write;
      list.querySelectorAll('.dictionary-empty,.dictionary-warning').forEach(n=>n.remove());
      for(const issue of directory.diagnostics)list.append(node('p',issue,'dictionary-warning'));
      if(!tables.size)list.append(node('p','尚未登记数据表。其他输入文件可以直接在 Notebook 中读取。','dictionary-empty'));
      filterLists();
    }
    populate(context);scanning=scanDirectory();
    function filterLists(){
      const value=search.value.trim().toLocaleLowerCase();
      if(activeTab==='dictionary'){
        for(const {row,item,fieldNames} of tables.values())row.hidden=![item.label,...fieldNames].some(name=>name.toLocaleLowerCase().includes(value));
      }else if(toolIndex){
        const toolPane=panes[activeTab];
        if(toolPane.dataset.revision===toolIndex.revision&&toolPane.dataset.query===value)return;
        toolPane.dataset.revision=toolIndex.revision;toolPane.dataset.query=value;
        paintTools(toolPane,toolIndex.definitions,value);
        if(!toolPane.childElementCount)toolPane.append(node('p',value?'没有匹配的结果。':'项目尚未声明'+labels[activeTab]+'。','dictionary-empty'));
        for(const issue of toolIndex.diagnostics)toolPane.append(node('p',issue.path+' · '+issue.detail,'op-error'));
      }
    }
    let base='',documents=[],busy=false;
    function documentUrl(path){return base+'notebooks/'+path.split('/').map(encodeURIComponent).join('/');}
    function pathFrom(url){
      const prefix=base+'notebooks/';
      return url?.startsWith(prefix)?decodeURIComponent(url.slice(prefix.length)):null;
    }
    function paintTabs(focus=false){
      bookTabs.replaceChildren();
      const activePath=pathFrom(session?.url);
      for(const path of documents){
        const tab=node('button',path.replace(/\.ipynb$/,''));tab.type='button';tab.title=path;
        tab.setAttribute('role','tab');const selected=session?.url===documentUrl(path);
        tab.setAttribute('aria-selected',String(selected));tab.tabIndex=selected||(!activePath&&path===documents[0])?0:-1;
        tab.onclick=()=>{if(!busy)try{activate(documentUrl(path),true);}catch(error){fail(error);}};
        tab.onkeydown=event=>{
          const offsets={ArrowLeft:-1,ArrowRight:1,Home:-documents.indexOf(path),End:documents.length-1-documents.indexOf(path)};
          if(!(event.key in offsets))return;
          event.preventDefault();
          const index=(documents.indexOf(path)+offsets[event.key]+documents.length)%documents.length;
          bookTabs.children[index].click();
        };
        bookTabs.append(tab);if(selected&&focus){tab.focus();tab.scrollIntoView({block:'nearest',inline:'nearest'});}
      }
    }
    function accessState(){
      const caps=context.capabilities||{};
      create.disabled=busy||!context.native_notebook||(!caps.analysis_write&&!notebookAccess.canGrant('analysis_write'));
      create.title=!caps.analysis_write&&notebookAccess.canGrant('analysis_write')?'新建 Notebook 并开启项目保存权限':'新建 Notebook';
      enableRun.hidden=!context.native_notebook||!session||caps.analysis_execute||!notebookAccess.canGrant('analysis_execute');
      enableRun.disabled=busy;
    }
    function detach(){
      ++cardsEpoch;cards?.dispose();cards=null;
      if(session){session.onPanel=null;session.onPath=null;session.onReady=null;session.host=null;position(session);session=null;}
    }
    function activate(url,focus=false){
      if(cards?.pending()&&url!==session?.url)throw Error('当前卡片正在更新，请在更新完成后切换。');
      if(session?.url!==url){
        const next=attach(native,project,base,url);
        detach();session=next;
      }
      notice.hidden=true;status.textContent='正在打开 Notebook…';status.hidden=!session.frame.hasAttribute('data-loading');resize();
      session.onReady=()=>{status.hidden=true;};
      session.onPath=(previous,current)=>{
        const old=pathFrom(previous),path=pathFrom(current);
        if(old!==path&&old)documents=documents.map(item=>item===old?path:item);
        if(path&&!documents.includes(path))documents.push(path);
        paintTabs();
      };
      session.onPanel=async panel=>{
        const epoch=++cardsEpoch;cards?.dispose();cards=null;
        if(!panel?.content?.activeCellChanged)return;
        try{
          const {mountCards}=await import('./notebook-cards.js');
          if(!alive()||epoch!==cardsEpoch)return;
          const kernel=panel.sessionContext;
          if(context.capabilities?.analysis_execute && kernel.kernelPreference.shouldStart===false)
            kernel.kernelPreference={...kernel.kernelPreference,shouldStart:true,autoStartDefault:true};
          cards=mountCards({desk,controls,panel,app:session.frame.contentWindow.jupyterapp,api,project,alive,
            permissions:()=>context.capabilities||{},tables:()=>[...tables.values()].map(({item,fieldNames})=>({
              table:item.original?item.original.id:item.alias,version:item.version,binding:item.binding,
              expected:item.expected,fields:fieldNames,
              reader:item.original?'demiflow.workbench.notebooks.data.project_sample':'demiflow.workbench.notebooks.data.sample'})),
            catalogCommand,layout:resize});
        }catch(error){fail(error);}
      };
      if(!cards&&session.readyPanel)session.onPanel(session.readyPanel);
      paintTabs(focus);accessState();
    }
    async function connect(createNew=false){
      if(busy)return;
      busy=true;accessState();notice.hidden=true;
      if(createNew&&!session){status.textContent='正在新建 Notebook…';status.hidden=false;}
      try{
        if(createNew||![...sessions].some(item=>item.project===project))makeRoom();
        if(createNew&&!context.capabilities?.analysis_write){
          context=await notebookAccess.enable({analysis_write:true});
          if(!alive())return;
          populate(context);
        }
        let opened;
        for(let attempt=0;;attempt++){
          try{opened=await api('notebook-open',{},createNew?{create:true}:{});break;}
          catch(error){
            if(createNew||error.code!=='resource_limit'||attempt>=3||!alive())throw error;
            await new Promise(resolve=>setTimeout(resolve,200*(attempt+1)));
          }
        }
        if(!alive())return;
        if(!opened.url){
          status.textContent=notebookAccess.canGrant('analysis_write')?'点击上方 ＋ 新建第一份 Notebook。':'此项目尚无可查看的 Notebook。请联系项目管理员开启保存权限。';
          return;
        }
        base=opened.base_url;
        const available=new Set(opened.documents);
        documents=[...new Set([...documents.filter(path=>available.has(path)),...opened.documents])];
        if(!session)try{await recoverDrafts(project,opened);}catch(error){if(alive())dictionary.append(node('p',error.message,'dictionary-meta'));}
        if(!alive())return;
        let url=opened.url;
        if(!createNew)try{
          const previous=sessionStorage.getItem('demiflow-jupyter:'+project);
          const path=pathFrom(previous);
          if(path&&(documents.includes(path)||(path.includes('/')&&!path.split('/').includes('..'))))url=previous;
        }catch{}
        activate(url,createNew);
      }catch(error){if(!session)status.hidden=true;paintTabs();fail(error);}
      finally{busy=false;if(alive())accessState();}
    }
    create.onclick=()=>{
      if(busy)return;
      if(cards?.pending()){fail(Error('当前卡片正在更新，请在更新完成后新建。'));return;}
      connect(true);
    };
    enableRun.onclick=async()=>{
      if(busy)return;
      busy=true;accessState();notice.hidden=true;
      try{
        context=await notebookAccess.enable({analysis_execute:true});
        if(!alive())return;
        if(session?.readyPanel)await session.onPanel(session.readyPanel);
      }catch(error){fail(error);}
      finally{busy=false;if(alive())accessState();}
    };
    accessState();
    if(context.native_notebook)connect();else status.textContent='此服务尚未启用原生 Jupyter Notebook。';
    return {
      canLeave:()=>true,
      updateContext(fresh){if(alive()){context=fresh;accessState();populate(fresh);scanning=scanDirectory();}},
      dispose(){if(disposed)return;disposed=true;if(toolsDialog?.open)toolsDialog.close();detach();dictionaryNavigation.dispose();observer.disconnect();window.removeEventListener('resize',resize);window.removeEventListener('scroll',resize,true);parent.classList.remove('native-data-active');}
    };
  }
  window.ForgeNotebooks=Object.freeze({mount});
})();
