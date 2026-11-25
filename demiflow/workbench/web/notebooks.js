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
  function relayout(session) {
    const win=session.frame.contentWindow;
    if(!win||session.frame.hidden)return;
    const kick=()=>{
      if(session.frame.hidden||!session.frame.isConnected)return;
      try{win.dispatchEvent(new win.Event('resize'));}catch{}
      try{
        win.jupyterapp?.shell?.update();
        const panel=notebookPanel(session);
        panel?.update();
        panel?.content?.update();
      }catch{}
    };
    kick();
    try{win.requestAnimationFrame(kick);}catch{}
  }
  function position(session) {
    const visible=session.host?.isConnected&&!session.host.hidden;
    const wasHidden=session.frame.hidden;
    session.frame.hidden=!visible;
    if(!visible)return;
    const rect=session.host.getBoundingClientRect();
    Object.assign(session.frame.style,{left:rect.left+'px',top:rect.top+'px',width:rect.width+'px',height:rect.height+'px'});
    if(wasHidden)relayout(session);
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
      frame.setAttribute('data-loading','');
      // Document links stay within their project's existing notebook frame.
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
        if(!win.location.pathname.startsWith(session.base+'notebooks/'))throw Error('请从文档标签打开卡片。');
        const style=win.document.createElement('link');
        style.rel='stylesheet';style.href='/notebook-cards.css';
        const styled=new Promise((resolve,reject)=>{
          style.onload=resolve;style.onerror=()=>reject(Error('卡片样式加载失败，请刷新重试。'));
        });
        win.document.body.classList.add('df-card-mode');win.document.head.append(style);
        await styled;
        const until=Date.now()+60000;
        while(!win.jupyterapp&&Date.now()<until&&session.loadEpoch===epoch&&frame.isConnected)
          await new Promise(resolve=>setTimeout(resolve,100));
        if(session.loadEpoch!==epoch||!frame.isConnected)return;
        if(!win.jupyterapp)throw Error('文档加载超时，请刷新后重试。');
        await win.jupyterapp.restored;
        let panel=notebookPanel(session);
        while(!panel&&Date.now()<until&&session.loadEpoch===epoch&&frame.isConnected){
          await new Promise(resolve=>setTimeout(resolve,100));panel=notebookPanel(session);
        }
        if(session.loadEpoch!==epoch||!frame.isConnected)return;
        if(!panel)throw Error('未能打开卡片文档，请刷新后重试。');
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
        if(session.loadEpoch!==epoch||!frame.isConnected)return;
        session.readyPanel=panel;await session.onPanel?.(panel);
      })().catch(error=>{if(session.loadEpoch===epoch)session.onReady?.(error);});
    });
  }
  function makeRoom(){
    if(sessions.size<16)return;
    const old=[...sessions].find(item=>!item.host?.isConnected&&item.readyPanel&&!dirty(item)
      &&!['busy','starting','restarting'].includes(item.readyPanel.sessionContext?.kernelDisplayStatus));
    if(!old)throw Error('打开的 Notebook 已满，请先保存修改并等待运行完成。');
    try{old.readyPanel.sessionContext?.shutdown?.().catch(()=>{});}catch{}
    old.frame.remove();sessions.delete(old);
  }
  function attach(host,project,base,url){
    let session=[...sessions].find(item=>item.project===project&&item.url===url);
    if(!session){
      makeRoom();
      const frame=node('iframe',undefined,'native-notebook-frame');
      frame.title='Jupyter Notebook';frame.referrerPolicy='same-origin';
      session={frame,project,base,url,host:null};prepareFrame(session);
      frame.setAttribute('data-loading','');
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
      if(opened.deleted_drafts?.includes(path))continue;
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
    const notebookList=node('div',undefined,'notebook-directory');
    const panes={dictionary:list,tools:node('div'),notebooks:notebookList};
    const labels={dictionary:'数据字典',tools:'工具',notebooks:'Notebook'};
    const filters={dictionary:'',tools:'',notebooks:''};
    let activeTab='dictionary',toolIndex=null,loadingTools=false;
    const tabs=ForgeNavigation.categoryTabs('数据分类',Object.entries(labels),key=>{
      filters[activeTab]=search.value;activeTab=key;search.value=filters[key];
      navigation.searchScope('搜索'+labels[key],key==='dictionary'?'表名或已读取字段':key==='notebooks'?'Notebook 文件名':'名称、说明或文件名');
      for(const [name,pane] of Object.entries(panes))pane.hidden=name!==key;
      filterLists();if(key==='tools')loadTools();
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
    const pythonBanner=node('div','尚未配置项目业务 Python 环境（Notebook 内核不继承平台环境），请先在右上角「设置 → 项目设置」中指定独立的 venv / conda Python 路径。','df-python-banner');
    pythonBanner.setAttribute('role','alert');pythonBanner.hidden=true;
    const notice=node('p',undefined,'df-book-notice');notice.setAttribute('role','status');notice.hidden=true;
    const confirmation=node('div',undefined,'df-book-confirm');confirmation.hidden=true;
    const question=node('span'),cancel=node('button','取消'),confirm=node('button','确认删除');
    confirm.className='df-confirm-delete';cancel.type=confirm.type='button';
    confirmation.append(question,cancel,confirm);
    documentPane.append(header,pythonBanner,confirmation,notice,native);desk.append(documentPane);root.append(dictionary,desk);
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
      const code='from demiflow.workbench.notebooks.data import sample\n\ndf = sample('+JSON.stringify(item.label)+', version='+fixedVersion+', limit=20)\ndf';
      return {fields,fixedVersion,rows,binding,code};
    }
    async function catalogCommand(command,args){
      const dictionary=args.group==='dictionary';
      if(dictionary)await scanning;
      const tools=dictionary?[]:(await api('project-data-tools')).definitions;
      if(command==='catalog.list'){
        const q=(args.query||'').trim().toLocaleLowerCase();
        const values=dictionary
          ?[...tables.values()].filter(({item,fieldNames})=>!q||[item.label,...fieldNames].join(' ').toLocaleLowerCase().includes(q))
              .map(({item})=>({name:item.label,version:item.version}))
          :tools.filter(item=>!q||[item.symbol,item.module,item.path,item.description].join(' ').toLocaleLowerCase().includes(q))
              .map(item=>({name:item.module?item.module+'.'+item.symbol:item.path+'::'+item.symbol,origin:item.origin,kind:item.tool_kind}));
        const offset=args.offset,items=values.slice(offset,offset+12);
        return {items,total:values.length,next_offset:offset+items.length<values.length?offset+items.length:null};
      }
      if(dictionary){
        const matches=[...tables.values()].filter(({item})=>item.label===args.id&&(args.version==null||item.version==null||item.version===args.version));
        const entry=matches.find(({item})=>args.version!=null&&item.version===args.version)||matches[0];
        if(!entry)throw Error('数据字典中没有这张表。');
        if(matches.length>1&&args.version==null)throw Error('这张表有多个版本，请指定 --version。');
        const {fields,fixedVersion,rows,code}=await tableDetail(entry.item);
        const names=Object.keys(fields),offset=args.offset||0;
        const countCode='from demiflow.workbench.notebooks.data import count\n\ncount('+JSON.stringify(entry.item.label)+', version='+fixedVersion+')';
        return {version:fixedVersion,rows,sample_limit:100,read:code,count:countCode,fields:names.slice(offset,offset+12).map(name=>[name,fields[name]?.type?.kind||fields[name]?.type||'']),
          next_offset:offset+12<names.length?offset+12:null};
      }
      const item=tools.find(i=>(i.module?i.module+'.'+i.symbol:i.path+'::'+i.symbol)===args.id);if(!item)throw Error('未找到此工具。');
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
      }else if(activeTab==='notebooks'){
        paintSidebarNotebooks();
      }else if(toolIndex){
        const toolPane=panes[activeTab];
        if(toolPane.dataset.revision===toolIndex.revision&&toolPane.dataset.query===value)return;
        toolPane.dataset.revision=toolIndex.revision;toolPane.dataset.query=value;
        paintTools(toolPane,toolIndex.definitions,value);
        if(!toolPane.childElementCount)toolPane.append(node('p',value?'没有匹配的结果。':'项目尚未声明'+labels[activeTab]+'。','dictionary-empty'));
        for(const issue of toolIndex.diagnostics)toolPane.append(node('p',issue.path+' · '+issue.detail,'op-error'));
      }
    }
    let base='',documents=[],openDocuments=[],busy=false;
    const openKey=()=>'demiflow-jupyter-open:'+project;
    function saveOpenDocuments(){try{sessionStorage.setItem(openKey(),JSON.stringify(openDocuments));}catch{}}
    function loadOpenDocuments(available){
      try{
        const raw=JSON.parse(sessionStorage.getItem(openKey())||'[]');
        if(Array.isArray(raw))return [...new Set(raw.filter(p=>typeof p==='string'&&available.has(p)))];
      }catch{}
      return [];
    }
    function ensureOpen(path){
      if(path&&!openDocuments.includes(path)){openDocuments.push(path);saveOpenDocuments();}
    }
    function documentUrl(path){return base+'notebooks/'+path.split('/').map(encodeURIComponent).join('/');}
    function pathFrom(url){
      const prefix=base+'notebooks/';
      return url?.startsWith(prefix)?decodeURIComponent(url.slice(prefix.length)):null;
    }
    function sessionForPath(path){
      if(!base||!path)return null;
      const targetUrl=documentUrl(path);
      return [...sessions].find(item=>item.project===project&&item.url===targetUrl)||null;
    }
    function kernelState(path){
      if(!openDocuments.includes(path))return 'closed';
      const target=sessionForPath(path);
      const ctx=target?.readyPanel?.sessionContext;
      if(ctx?.session?.kernel&&!['dead','terminating'].includes(ctx.kernelDisplayStatus))return 'running';
      return 'open';
    }
    function trashIcon(){
      const svg=document.createElementNS('http://www.w3.org/2000/svg','svg'),line=document.createElementNS(svg.namespaceURI,'path');
      svg.setAttribute('viewBox','0 0 24 24');svg.setAttribute('aria-hidden','true');line.setAttribute('d','M4 7h16M9 7V4h6v3M6 7l1 13h10l1-13M10 10v7m4-7v7');
      svg.append(line);return svg;
    }
    function paintSidebarNotebooks(){
      notebookList.replaceChildren();
      const query=activeTab==='notebooks'?search.value.trim().toLocaleLowerCase():'';
      const activePath=pathFrom(session?.url);
      for(const path of documents){
        const title=path.replace(/\.ipynb$/,'');
        if(query&&!path.toLocaleLowerCase().includes(query)&&!title.toLocaleLowerCase().includes(query))continue;
        const row=node('div',undefined,'df-nb-row');
        const active=activePath===path;
        const isOpen=openDocuments.includes(path);
        const state=kernelState(path);
        row.dataset.active=String(active);row.dataset.path=path;row.dataset.state=state;
        const item=node('button',undefined,'df-nb-item');item.type='button';item.title=path;
        if(active)item.setAttribute('aria-current','true');
        const dot=node('span',undefined,'df-nb-dot');dot.dataset.state=state;
        dot.title=state==='running'?'内核运行中':isOpen?'已打开（内核未运行）':'未打开';
        item.append(dot,node('span',title,'df-nb-name'));
        item.onclick=()=>{if(!busy)try{ensureOpen(path);activate(documentUrl(path),true);}catch(error){fail(error);}};
        row.append(item);
        if(isOpen){
          const closeBtn=node('button','×','df-nb-action');closeBtn.type='button';
          closeBtn.title='关闭「'+title+'」（停止内核，不删除文件）';closeBtn.setAttribute('aria-label',closeBtn.title);
          closeBtn.onclick=()=>askClose(path);
          row.append(closeBtn);
        }
        if(context.capabilities?.analysis_write){
          const delBtn=node('button',undefined,'df-nb-action df-nb-del');delBtn.type='button';
          delBtn.title='删除「'+title+'」';delBtn.setAttribute('aria-label',delBtn.title);
          delBtn.append(trashIcon());delBtn.onclick=()=>askDelete(path);
          row.append(delBtn);
        }
        notebookList.append(row);
      }
      if(!notebookList.childElementCount){
        notebookList.append(node('p',query?'没有匹配的 Notebook。':'尚未创建 Notebook。','dictionary-empty'));
      }
    }
    let pendingAction=null;
    function cancelAction(){pendingAction=null;confirmation.hidden=true;}
    cancel.onclick=()=>{cancelAction();bookTabs.querySelector('[aria-selected=true]')?.focus();};
    confirmation.onkeydown=e=>{if(e.key==='Escape'){e.preventDefault();cancel.click();}};
    async function performClose(path,target){
      busy=true;accessState();
      const idx=openDocuments.indexOf(path);
      if(session===target)detach();
      if(target){
        try{await target.readyPanel?.sessionContext?.shutdown?.();}catch{}
        target.frame.remove();sessions.delete(target);
      }
      openDocuments=openDocuments.filter(item=>item!==path);saveOpenDocuments();
      try{if(sessionStorage.getItem('demiflow-jupyter:'+project)===documentUrl(path))sessionStorage.removeItem('demiflow-jupyter:'+project);}catch{}
      busy=false;
      if(!alive())return;
      if(!session){
        if(openDocuments.length)activate(documentUrl(openDocuments[Math.min(Math.max(0,idx),openDocuments.length-1)]),true);
        else{paintTabs();accessState();status.hidden=false;status.textContent='从左侧「Notebook」目录打开已有文档，或点击上方 ＋ 新建 Notebook。';}
      }else{
        paintTabs();accessState();
      }
    }
    function askClose(path){
      if(busy)return;
      const target=sessionForPath(path);
      if(target&&((session===target&&cards?.pending())||target.readyPanel?.sessionContext?.kernelDisplayStatus==='busy')){
        fail(Error('请等待当前分析运行结束后再关闭。'));return;
      }
      const hasUnsaved=target&&(target.storage?.hasDrafts?target.storage.hasDrafts():dirty(target));
      if(hasUnsaved){
        pendingAction={kind:'close',path,owner:target};
        question.textContent='「'+path.replace(/\.ipynb$/,'')+'」有未保存的修改，仍要关闭并停止内核？';
        confirm.textContent='确认关闭';
        confirmation.hidden=false;cancel.focus();
        return;
      }
      performClose(path,target);
    }
    function askDelete(path){
      if(busy||!context.capabilities?.analysis_write)return;
      const owner=sessionForPath(path);
      if(owner&&((session===owner&&(!owner.readyPanel||cards?.pending()))||['busy','starting','restarting'].includes(owner.readyPanel?.sessionContext?.kernelDisplayStatus))){
        fail(Error('请等当前卡片准备好后再删除。'));return;
      }
      pendingAction={kind:'delete',path,owner};
      question.textContent='删除「'+path.replace(/\.ipynb$/,'')+'」及其中的全部卡片？';
      confirm.textContent='确认删除';
      confirmation.hidden=false;cancel.focus();
    }
    confirm.onclick=async()=>{
      if(!pendingAction||busy)return;
      const action=pendingAction;cancelAction();
      if(action.kind==='close'){
        await performClose(action.path,action.owner);
        return;
      }
      const {path,owner}=action;
      if(!context.capabilities?.analysis_write)return;
      if(owner&&((session===owner&&cards?.pending())||['busy','starting','restarting'].includes(owner.readyPanel?.sessionContext?.kernelDisplayStatus))){
        fail(Error('请等待当前分析运行结束后再删除。'));return;
      }
      const openIdx=openDocuments.indexOf(path);
      const wasActive=session&&session===owner;
      busy=true;accessState();
      if(wasActive)detach();
      try{
        if(owner?.readyPanel?.sessionContext)try{await owner.readyPanel.sessionContext.shutdown();}catch{}
        const app=owner?.frame?.contentWindow?.jupyterapp;
        if(app){
          await app.commands.execute('docmanager:delete-file',{path});
        }else{
          const resp=await fetch(base+'api/contents/'+path.split('/').map(encodeURIComponent).join('/'),{method:'DELETE',credentials:'same-origin'});
          if(!resp.ok&&resp.status!==404)throw Error('删除 Notebook 失败（HTTP '+resp.status+'）。');
        }
      }catch(error){
        busy=false;if(alive()){if(wasActive&&owner)activate(owner.url,true);fail(error);}return;
      }
      if(owner){owner.storage?.forget();owner.frame.remove();sessions.delete(owner);}
      documents=documents.filter(item=>item!==path);
      openDocuments=openDocuments.filter(item=>item!==path);saveOpenDocuments();
      try{if(sessionStorage.getItem('demiflow-jupyter:'+project)===documentUrl(path))sessionStorage.removeItem('demiflow-jupyter:'+project);}catch{}
      busy=false;
      if(!alive())return;
      if(wasActive){
        if(openDocuments.length)activate(documentUrl(openDocuments[Math.min(Math.max(0,openIdx),openDocuments.length-1)]),true);
        else{paintTabs();accessState();status.hidden=false;status.textContent=documents.length?'从左侧「Notebook」目录打开已有文档，或点击上方 ＋ 新建 Notebook。':'点击上方 ＋ 新建第一份 Notebook。';create.focus();}
      }else{
        paintTabs();accessState();
      }
    };
    function paintTabs(focus=false){
      bookTabs.replaceChildren();
      const activePath=pathFrom(session?.url);
      for(const path of openDocuments){
        const title=path.replace(/\.ipynb$/,'');
        const tab=node('button',title);tab.type='button';tab.title=path;
        tab.setAttribute('role','tab');const selected=session?.url===documentUrl(path);
        tab.setAttribute('aria-selected',String(selected));tab.tabIndex=selected||(!activePath&&path===openDocuments[0])?0:-1;
        tab.onclick=()=>{if(!busy)try{activate(documentUrl(path),true);}catch(error){fail(error);}};
        tab.onkeydown=event=>{
          const offsets={ArrowLeft:-1,ArrowRight:1,Home:-openDocuments.indexOf(path),End:openDocuments.length-1-openDocuments.indexOf(path)};
          if(!(event.key in offsets))return;
          event.preventDefault();
          const index=(openDocuments.indexOf(path)+offsets[event.key]+openDocuments.length)%openDocuments.length;
          bookTabs.querySelectorAll('[role=tab]')[index].click();
        };
        bookTabs.append(tab);
        const close=node('button','×','df-close-book');close.type='button';
        close.title='关闭「'+title+'」（不删除文件）';close.setAttribute('aria-label','关闭「'+title+'」');
        close.onclick=()=>askClose(path);bookTabs.append(close);
        if(selected&&focus){tab.focus();tab.scrollIntoView({block:'nearest',inline:'nearest'});}
      }
      paintSidebarNotebooks();
    }
    function accessState(){
      const caps=context.capabilities||{};
      create.disabled=busy||!context.native_notebook||(!caps.analysis_write&&!notebookAccess.canGrant('analysis_write'));
      create.title=!caps.analysis_write&&notebookAccess.canGrant('analysis_write')?'新建 Notebook 并开启项目保存权限':'新建 Notebook';
      enableRun.hidden=!context.native_notebook||!session||caps.analysis_execute||!notebookAccess.canGrant('analysis_execute');
      enableRun.disabled=busy;
      pythonBanner.hidden=!context.native_notebook||!!context.project_python;
    }
    let disconnectKernel=null;
    function detach(){
      cancelAction();++cardsEpoch;cards?.dispose();cards=null;
      disconnectKernel?.();disconnectKernel=null;
      if(session){session.onPanel=null;session.onPath=null;session.onReady=null;session.host=null;position(session);session=null;}
    }
    function activate(url,focus=false){
      if(cards?.pending()&&url!==session?.url)throw Error('当前卡片正在更新，请在更新完成后切换。');
      const targetPath=pathFrom(url);
      if(targetPath)ensureOpen(targetPath);
      if(session?.url!==url){
        const next=attach(native,project,base,url);
        detach();session=next;
      }
      notice.hidden=true;status.textContent='正在打开 Notebook…';status.hidden=!session.frame.hasAttribute('data-loading');resize();
      session.onReady=error=>{status.hidden=true;if(error)fail(error);paintSidebarNotebooks();};
      session.onPath=(previous,current)=>{
        const old=pathFrom(previous),path=pathFrom(current);
        if(old!==path&&old){
          documents=documents.map(item=>item===old?path:item);
          openDocuments=openDocuments.map(item=>item===old?path:item);saveOpenDocuments();
        }
        if(path&&!documents.includes(path))documents.push(path);
        if(path&&!openDocuments.includes(path)){openDocuments.push(path);saveOpenDocuments();}
        paintTabs();
      };
      session.onPanel=async panel=>{
        const epoch=++cardsEpoch;cards?.dispose();cards=null;
        disconnectKernel?.();disconnectKernel=null;
        session.frame.setAttribute('data-loading','');
        if(!panel?.content?.activeCellChanged)return;
        try{
          const {mountCards}=await import('./notebook-cards.js');
          const {cellStorage}=await import('./notebook-cell-save.js');
          const storage=await cellStorage(panel,project,base);
          if(!alive()||epoch!==cardsEpoch)return;
          const kernel=panel.sessionContext;
          if(context.capabilities?.analysis_execute&&context.project_python){
            if(kernel.kernelPreference.shouldStart===false)
              kernel.kernelPreference={...kernel.kernelPreference,shouldStart:true,autoStartDefault:true,name:'python3'};
          }else if(!context.project_python){
            kernel.kernelPreference={...kernel.kernelPreference,shouldStart:false,autoStartDefault:false};
          }
          const onKernelUpdate=()=>{if(alive())paintSidebarNotebooks();};
          kernel.kernelChanged.connect(onKernelUpdate);
          kernel.statusChanged.connect(onKernelUpdate);
          disconnectKernel=()=>{
            try{kernel.kernelChanged.disconnect(onKernelUpdate);}catch{}
            try{kernel.statusChanged.disconnect(onKernelUpdate);}catch{}
          };
          session.storage=storage;
          cards=mountCards({desk,controls,panel,storage,app:session.frame.contentWindow.jupyterapp,api,project,alive,
            permissions:()=>context.capabilities||{},hasProjectPython:()=>!!context.project_python,
            tables:()=>[...tables.values()].map(({item,fieldNames})=>({
              table:item.label,version:item.version,fields:fieldNames,
              reader:'demiflow.workbench.notebooks.data.sample'})),
            catalogCommand,layout:resize});
          session.frame.removeAttribute('data-loading');relayout(session);session.onReady?.();
        }catch(error){status.hidden=true;fail(error);}
      };
      if(!cards&&session.readyPanel)session.onPanel(session.readyPanel);
      paintTabs(focus);accessState();
    }
    async function connect(createNew=false){
      if(busy)return;
      cancelAction();busy=true;accessState();notice.hidden=true;
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
        if(!openDocuments.length)openDocuments=loadOpenDocuments(available);
        else openDocuments=openDocuments.filter(path=>available.has(path));
        if(!session)try{await recoverDrafts(project,opened);}catch(error){if(alive())dictionary.append(node('p',error.message,'dictionary-meta'));}
        if(!alive())return;
        let url=opened.url;
        if(!createNew)try{
          const previous=sessionStorage.getItem('demiflow-jupyter:'+project);
          const path=pathFrom(previous);
          if(path&&(documents.includes(path)||(path.includes('/')&&!path.split('/').includes('..'))))url=previous;
        }catch{}
        const initialPath=pathFrom(url);
        if(!initialPath){
          status.textContent=notebookAccess.canGrant('analysis_write')?'点击上方 ＋ 新建第一份 Notebook。':'此项目尚无可查看的 Notebook。请联系项目管理员开启保存权限。';
          return;
        }
        ensureOpen(initialPath);
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
      cancelAction();busy=true;accessState();notice.hidden=true;
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
      updateContext(fresh){
        if(!alive())return;
        const hadPython=!!context.project_python;
        context=fresh;
        if(!fresh.capabilities?.analysis_write)cancelAction();
        paintTabs();accessState();populate(fresh);scanning=scanDirectory();
        if(!hadPython&&fresh.project_python&&fresh.capabilities?.analysis_execute&&session?.readyPanel){
          const kernel=session.readyPanel.sessionContext;
          kernel.kernelPreference={...kernel.kernelPreference,shouldStart:true,autoStartDefault:true,name:'python3'};
          if(!kernel.session?.kernel)kernel.changeKernel({name:'python3'}).catch(()=>{});
        }
      },
      dispose(){if(disposed)return;disposed=true;if(toolsDialog?.open)toolsDialog.close();detach();dictionaryNavigation.dispose();observer.disconnect();window.removeEventListener('resize',resize);window.removeEventListener('scroll',resize,true);parent.classList.remove('native-data-active');}
    };
  }
  window.ForgeNotebooks=Object.freeze({mount});
})();
