/* The UI consumes the same immutable inspection and case APIs as the CLI. */
(() => {
  'use strict';
  const $ = (tag, text, className) => { const n = document.createElement(tag); if (text !== undefined) n.textContent = text; if (className) n.className = className; return n; };
  const json = value => JSON.stringify(value, null, 2);
  const offline = window.FORGE_EXPORT;
  const V = window.ForgeView;
  let token = '', initialView = null, initialError = null;
  if (!offline) {
    try {
      if(location.hash.length>16385)throw new Error('视图链接超出长度限制');
      const fragment = new URLSearchParams(location.hash.slice(1));
      if(fragment.getAll('token').length>1)throw new Error('重复会话参数');
      token = fragment.get('token') || sessionStorage.getItem('forge-token') || '';
      if (token) sessionStorage.setItem('forge-token', token);
      fragment.delete('token');
      history.replaceState(null, '', location.pathname + (fragment.size ? '#' + fragment : ''));
      initialView = V.decodeView(location.hash);
    } catch (err) { initialError = err; }
  }
  let sessionReloadPending = false;
  function reloadSessionEntry() {
    if (offline || !new URLSearchParams(location.hash.slice(1)).has('token')) return false;
    // Opening an authorization link in the same tab can change only the hash.
    // Re-enter startup so the new token and any requested view are read together.
    if (!sessionReloadPending) { sessionReloadPending = true; location.reload(); }
    return true;
  }
  window.addEventListener('hashchange', reloadSessionEntry);
  let workspaceId = '', projectList = [], projectNav, projectDigest = '', projectRemovalEnabled = false, projectManageEnabled = false;
  let pendingOriginalPath = null;
  let developmentView = null, workbenchView = null, analysisView = null, analysisTransport = null;
  const readers = new Set();
  // Cleanup registry only: view-scoped DOM handles (A07 profile, charts) disposed
  // after a successful canLeave and before the DOM is cleared. No business state.
  const currentViewHandles = new Set();
  function registerViewHandle(handle) {
    if (!handle || typeof handle.dispose !== 'function') return handle;
    if (currentViewHandles.size >= 128) {
      const oldest = currentViewHandles.values().next().value;
      currentViewHandles.delete(oldest);
      try { oldest.dispose(); } catch { /* a stale handle must not block registration */ }
    }
    currentViewHandles.add(handle); return handle;
  }
  function disposeViewHandles() {
    for (const handle of currentViewHandles) { try { handle.dispose(); } catch { /* disposal is best-effort */ } }
    currentViewHandles.clear();
  }
  function canLeaveViews() {
    for (const controller of [developmentView, workbenchView, analysisView]) if (controller?.canLeave && !controller.canLeave()) return false;
    return true;
  }
  function disposeReaders(root) {
    if (!root) { disposeViewHandles(); developmentView?.dispose(); developmentView = null; workbenchView?.dispose(); workbenchView=null; analysisView?.dispose();analysisView=null;analysisTransport?.dispose();analysisTransport=null; }
    if(root)for(const img of root.querySelectorAll('img.case-image')){const url=img.getAttribute('src');if(blobs.delete(url)){mediaBytes-=blobSizes.get(url)||0;blobSizes.delete(url);URL.revokeObjectURL(url);}img.remove();}
    for (const reader of readers) if (!root || root.contains(reader.element)) {reader.dispose(); readers.delete(reader);}
  }
  function mountText(parent, text, {sourceOnly=false, compactSource=false, continuousSource=false, resolveLink, defaultMode='preview'}={}) {
    const reader=globalThis.ForgeReading.createTextReader({text, compactSource, continuousSource, defaultMode:sourceOnly?'source':defaultMode,
      identity:{epoch:viewEpoch, selection:selectionEpoch}, resolveLink, offline:!!offline});
    if(sourceOnly)reader.element.querySelector('.reading-toolbar').hidden=true;
    readers.add(reader);parent.append(reader.element);return reader;
  }
  function closeNavigation() {
    const wasOpen=document.querySelector('.shell')?.classList.contains('nav-open');
    document.querySelector('.shell')?.classList.remove('nav-open');
    const toggle=document.getElementById('navigation-toggle');
    updateNavigation();
    if(wasOpen&&toggle?.getClientRects().length)toggle.focus();
  }
  function updateNavigation() {
    const shell=document.querySelector('.shell');if(!shell)return;
    const narrow=matchMedia('(max-width:760px)').matches,expanded=narrow?shell.classList.contains('nav-open'):!shell.classList.contains('sidebar-collapsed');
    shell.classList.toggle('nav-narrow',narrow);
    document.getElementById('navigation-toggle')?.setAttribute('aria-expanded',String(expanded));
    const sidebar=document.getElementById('project-sidebar');
    if(sidebar){if(narrow)sidebar.setAttribute('aria-hidden',String(!expanded));else sidebar.removeAttribute('aria-hidden');}
    const control=document.getElementById('sidebar-toggle');
    if(control){const label=narrow?'收起项目导航':expanded?'收起侧栏':'展开侧栏';control.setAttribute('aria-label',label);control.title=label;control.setAttribute('aria-expanded',String(expanded));}
  }
  function revealTab() {
    const current=nav?.querySelector('[aria-current=page]');if(!current)return;
    const tab=current.getBoundingClientRect(),bounds=nav.getBoundingClientRect();
    if(tab.left<bounds.left+16)nav.scrollLeft-=bounds.left+16-tab.left;
    else if(tab.right>bounds.right-16)nav.scrollLeft+=tab.right-bounds.right+16;
  }
  const projectTabs = new Map();
  let context, main, nav, view = null, pendingView = null, selection = null, viewState = 'loading';
  let auditEnabled=false, auditState=null, auditPanel, auditFetchedAt=0, auditRequest=0, auditError=null;
  let viewEpoch = 0, selectionEpoch = 0;
  let continueInput = '', refreshing = null;
  let syncTimer, changes = null, lastObserved = null, forcePending = false;
  let serviceRunPermissions={},serviceWorkbenchPermissions={},workbenchContract=null;
  const canRun=permission=>context?.run_permissions?.[permission]??!!context?.runs_enabled;
  let updates = {}, activeWrites = 0, authFailed = false;
  let readController = new AbortController(), activeReads = 0;
  const blobs = new Set(),blobSizes=new Map();
  let mediaBytes = 0;
  const staleRequest = () => new DOMException('页面选择已变化', 'AbortError');
  function pageKind(reference = pendingView || view) {
    const kind = reference?.kind || 'overview';
    if(reference?.panel)return reference.panel;
    return ({development_session:'development',table:'tables',table_request:'tables',case:'tables',inspection:'flows',run:'monitor',run_compare:'monitor',runs:'monitor',checks:'flows'})[kind] || kind;
  }
  const pipelinePage=page=>['flows','operators','monitor'].includes(page);
  function markFunction(page=pageKind()) {
    for(const b of nav.querySelectorAll('[data-page]')){
      const selected=b.dataset.page===page;b.classList.toggle('active',selected);b.setAttribute('aria-pressed',String(selected));
      if(selected)b.setAttribute('aria-current','page');else b.removeAttribute('aria-current');
    }
    revealTab();
  }
  function target(kind, fields = {}) { return V.validateView({v:2, workspace:workspaceId, kind, ...fields}); }
  // At most six validated locators per registered project. Drafts stay in
  // their existing Stores; only a confirmed current view is remembered here.
  function rememberTab() {
    if (viewState !== 'ready') return;
    const reference = selection || view;
    if (!reference || reference.v !== 2 || reference.workspace !== workspaceId) return;
    const page = pageKind(reference);
    if (!['development','flows','operators','monitor','tables','overview'].includes(page)) return;
    const saved = projectTabs.get(workspaceId) || {page, views:new Map()};
    saved.page = page; saved.views.set(page, V.validateView(reference));
    if(pipelinePage(page))saved.pipeline=V.validateView(reference);
    projectTabs.set(workspaceId, saved);
  }
  function tabTarget(page) {
    const current = selection || view;
    if (viewState === 'ready' && current?.workspace === workspaceId && pageKind(current) === page) return current;
    const saved=projectTabs.get(workspaceId)?.views;
    if(pipelinePage(page)){
      const shared=projectTabs.get(workspaceId)?.pipeline||[saved?.get(page),saved?.get('flows'),saved?.get('operators'),saved?.get('monitor')].find(Boolean);
      return shared?V.validateView({...shared,panel:page}):target(page==='monitor'?'runs':'flows',{panel:page});
    }
    return saved?.get(page) || target(page);
  }
  function revokeImages() { for (const url of blobs) URL.revokeObjectURL(url); blobs.clear();blobSizes.clear(); mediaBytes = 0; }
  async function api(path, query = {}, body, projectId = workspaceId, options = {}) {
    if (offline) throw new Error('离线报告不能读取工作区或重新检查');
    const write=!!body&&!['plan-preview','plan-node-detail','plan-pipeline-source','analysis-source-register','analysis-case-group-bindings','analysis-case-page','analysis-annotation-heads','analysis-annotation-definitions','analysis-field-preview','analysis-source-from-handle','analysis-case-media','analysis-artifact-media-current','analysis-media'].includes(path);
    const cleanup=options.cleanup===true&&path==='analysis-media'&&body?.action==='revoke';
    if(options.range&&!['analysis-output-media','analysis-artifact-media'].includes(path))throw new Error('不支持的媒体 Range 路由');
    const epoch = viewEpoch;
    const url = (['projects','capabilities','project-create','project-register','project-remove','project-permissions','project-capabilities'].includes(path) ? '/api/' + path : '/api/workspaces/' + projectId + '/' + path) + (Object.keys(query).length ? '?' + new URLSearchParams(query) : '');
    if (url.length > 8192) throw new Error('查询链接超出长度限制，请缩小字段投影');
    const analysis=path.startsWith('analysis-'),payload=body?V.boundedJSON(body,65536):null;
    const signal=cleanup?AbortSignal.timeout(5000):write?AbortSignal.timeout(65000):AbortSignal.any([readController.signal,...(options.signal?[options.signal]:[]),AbortSignal.timeout(65000)]);
    const run=async()=>{
    if (!write && activeReads >= 8) throw new Error('busy：页面读取已达并发上限');
    if (!write) activeReads++;
    else activeWrites++;
    try {
      const started=Date.now();let attempts=0;
      for(;;){
      const response = await fetch(url, {
        method: body ? 'POST' : 'GET', headers: {'X-Forge-Token': token, ...(body ? {'Content-Type':'application/json'} : {}),...(options.range?{Range:options.range}:{})},
        ...(body ? {body: payload} : {}),
        ...(cleanup?{keepalive:true}:{}),
        signal
      }).catch(err => {
        if(!write&&!cleanup&&(epoch!==viewEpoch||options.signal?.aborted))throw staleRequest();
        if (err.name === 'AbortError') throw err;
        throw new Error(write ? '未收到服务响应，请先检查连接并核对操作结果。' : '无法连接工作区服务，请确认服务和连接可用后重试。');
      });
      if(!write&&!cleanup&&(epoch!==viewEpoch||options.signal?.aborted))throw staleRequest();
      if (!response.ok) {
        let value = {}; try { value = await response.json(); } catch { /* proxies may return an empty or HTML error */ }
        if(!write&&!cleanup&&(epoch!==viewEpoch||options.signal?.aborted))throw staleRequest();
        if (!value || typeof value !== 'object' || Array.isArray(value)) value = {};
        const err = new Error(value.error || ('HTTP ' + response.status + '：服务暂未提供可读取的响应。'));
        err.code = value.code || 'transport_error';
        if(value.operation_started===false)err.operationStarted=false;
        err.resetRequired = value.reset_required === true;
        if (response.status === 403 && path === 'capabilities') authFailed = true;
        if (err.code === 'unauthorized') authFailed = true;
        // Keep transient admission contention inside the original project read.
        // Result-source selection and all writes retain their explicit recovery.
        const projectRead=!body&&['projects','capabilities','context','changes','graph','project-operators','operator-definition','operator-history','flow-arguments'].includes(path)&&err.operationStarted===false;
        if((analysis||projectRead)&&!write&&!cleanup&&err.code==='busy'&&!signal.aborted&&attempts<12&&Date.now()-started<20000){
          const wait=Math.min(2000,100*2**Math.min(attempts++,5));
          await new Promise((resolve,reject)=>{let timer;const cancel=()=>{clearTimeout(timer);signal.removeEventListener('abort',cancel);reject(staleRequest());};signal.addEventListener('abort',cancel,{once:true});if(signal.aborted){cancel();return;}timer=setTimeout(()=>{signal.removeEventListener('abort',cancel);resolve();},wait);});
          continue;
        }
        if (authFailed) err.message = '会话授权已失效，请使用当前服务的有效授权入口重新打开。';
        else if (err.code === 'conflict') err.message += '；对象已变化，请刷新并核对后再操作。';
        else if(write&&err.code==='busy'&&err.operationStarted===false&&['inspect','project-check'].includes(path))err.message='检查未受理，尚未开始。服务繁忙，请稍后再次点击检查。已有检查结果及其原依据保留。';
        else if (write && err.operationStarted !== false && response.status >= 500) err.message += ' 写入结果可能尚未确认，请先查询，不要直接重复提交。';
        throw err;
      }
      if(options.outputRange){
        if(!['analysis-output-media','analysis-artifact-media'].includes(path)||response.status!==206||response.headers.get('X-Forge-Binding')!==(path==='analysis-artifact-media'?JSON.parse(query.reference).expected_binding:query.expected_binding))throw new Error('媒体字节绑定身份不一致');
        return response;
      }
      let value;
      try { value = path === 'media' ? await response.blob() : await response.json(); }
      catch { if(!write&&(epoch!==viewEpoch||options.signal?.aborted))throw staleRequest();throw new Error(write ? '未能读取操作响应，请先核对操作结果，不要直接重复提交。' : '服务响应无法读取，请刷新后重试。'); }
      if (!write && !cleanup && epoch !== viewEpoch) throw staleRequest();
      if (path === 'media' && response.headers.get('X-Forge-Binding') !== query.expected_binding) throw new Error('图片绑定身份不一致');
      return value;
      }
    } finally { if (!write) activeReads--; else activeWrites--; }
    };
    if(!analysis||path==='analysis-stop-generation')return run();
    const priority=cleanup?2:['analysis-case-media','analysis-artifact-media-current','analysis-output-inspect'].includes(path)?0:1;
    return ForgeAnalysisTransport.schedule(run,{signal,priority});
  }
  function clearSelection() {
    selection = null;
  }

  function error(parent, err) { if (err?.name !== 'AbortError' && parent?.isConnected) { const box=$('div', err.message || String(err), 'error-box'); parent.append(box); return box; } }
  function renderLoadFailure(parent, err, {retryRead, scope, isCurrent = () => true} = {}) {
    if (!parent?.isConnected || err?.name === 'AbortError' || !isCurrent()) return;
    const box=$('div',undefined,'load-failure');box.setAttribute('role','alert');
    let advice;
    if (err.operationStarted === true) advice='操作可能已经开始，结果尚未确认。请回到原操作的待核对回执查询，不要直接重复提交。';
    else if (err.code === 'unauthorized' || authFailed) advice='会话授权已失效。请通过当前服务的有效授权入口重新打开；已显示的内容保留。';
    else if (err.code === 'busy') advice='服务读取容量暂时不足。稍后可重试读取；已有内容保留。';
    else if (err.code === 'conflict') advice='来源身份已变化。请重新选择或刷新固定来源后核对；未确认前不覆盖已有内容。';
    else if (err.code === 'not_found') advice='对象已不可读取，可能已移除或改名。请返回目录重新选择。';
    else advice='读取未完成，已有内容保留。可重试读取，或返回重新选择来源。';
    box.append($('strong',(scope?.label||'内容')+'读取失败'),$('p',advice));
    if (scope?.project) box.append($('p','项目：'+(projectList.find(p=>p.workspace_id===scope.project)?.name||scope.project),'muted small'));
    if (retryRead && err.operationStarted !== true) {
      const bar=$('div',undefined,'toolbar');box.append(bar);
      button('重试读取',async()=>{
        if(!isCurrent())return;
        box.remove();
        try{await retryRead();}
        catch(retryError){if(retryError?.name!=='AbortError'&&isCurrent()&&parent.isConnected)renderLoadFailure(parent,retryError,{retryRead,scope,isCurrent});}
      },bar);
    }
    const details=$('details');details.append($('summary','原始错误'),$('pre',(err.code?'['+err.code+'] ':'')+(err.message||String(err))));box.append(details);
    parent.append(box);
    err.presented=true;err.presentedIn=box;
  }
  function button(label, action, parent, disabled = false) { const b = $('button', label); let failure; b.disabled = disabled; b.onclick = async () => { failure?.remove(); failure=null; b.disabled = true; try { await action(); } catch (err) { if(!err?.presented)failure=error(parent || main, err); } finally { b.disabled = disabled; } }; if (parent) parent.append(b); return b; }
  function icon(paths) {
    const ns='http://www.w3.org/2000/svg',svg=document.createElementNS(ns,'svg');
    for(const [key,value] of Object.entries({viewBox:'0 0 24 24',fill:'none',stroke:'currentColor','stroke-width':'1.5','stroke-linecap':'round','stroke-linejoin':'round','aria-hidden':'true',focusable:'false'}))svg.setAttribute(key,value);
    for(const d of paths){const path=document.createElementNS(ns,'path');path.setAttribute('d',d);svg.append(path);}
    return svg;
  }
  function iconButton(label, action, parent, paths) {
    const b=button(label,action);b.className='icon-button';b.setAttribute('aria-label',label);b.title=label;
    b.replaceChildren(icon(paths));parent?.append(b);return b;
  }
  function panel(title, parent = main) { const p = $('section', undefined, 'panel'); if (title) p.append($('h2', title)); parent.append(p); return p; }
  function badge(status) { const label = {checked:'静态检查通过', invalid:'不符合契约', incomplete:'检查不完整', stale:'依据已变化', not_rechecked:'尚未复核', not_live:'离线快照',prepared:'已准备 · 尚未执行',starting:'正在启动',running:'运行中',stopping:'正在停止',succeeded:'运行成功',failed:'运行失败',cancelled:'已取消',interrupted:'运行中断'}[status] || status; return $('span', label, 'badge' + (['invalid','failed','interrupted'].includes(status) ? ' error' : ['checked','succeeded'].includes(status) ? '' : ' warn')); }
  function heading(title, subtitle) { main.append($('h1', title), $('p', subtitle, 'muted')); }
  function raw(parent, title, value) {
    const d=$('details'),host=$('div');d.append($('summary',title),host);parent.append(d);
    d.addEventListener('toggle',()=>{disposeReaders(host);host.replaceChildren();if(d.open&&d.isConnected)mountText(host,json(value),{sourceOnly:true});});return d;
  }
  function select(values, selected) { const s = $('select'); for (const item of values) { const v = typeof item === 'string' ? item : item.value; const o = $('option', typeof item === 'string' ? item : item.label); o.value = v; s.append(o); } if (selected) s.value = selected; return s; }
  function table(parent, columns, rows, action) { const wrap = $('div', undefined, 'table-wrap'), t = $('table'), head = $('tr'); for (const c of columns) head.append($('th', c)); const th = $('thead'); th.append(head); t.append(th); const body = $('tbody'); for (const row of rows) { const tr = $('tr'); for (const cell of row.cells) tr.append($('td', cell)); if (action) { tr.className = 'clickable'; tr.tabIndex=0; tr.setAttribute('aria-label',row.cells.filter(v=>v!==undefined&&v!==null).join(' · ').slice(0,240)); tr.onclick = async () => { try { await action(row.value, tr); } catch (err) { if(!err?.presented)error(parent,err); } }; tr.onkeydown=event=>{if(event.key==='Enter'||event.key===' '){event.preventDefault();tr.click();}}; } body.append(tr); } t.append(body); wrap.append(t); parent.append(wrap); return t; }
  function putHistory(value, mode) {
    if (mode === 'none') return;
    history[mode === 'push' ? 'pushState' : 'replaceState'](null, '', location.pathname + V.encodeView(value));
  }
  function choose(reference, route = false) {
    const value = V.validateView(reference);
    selection = value;
    if (route && viewState === 'ready') { view = value; putHistory(value, 'replace'); }
  }
  function contextActions(parent, reference) {
    if (offline) return;
    const value = V.validateView(reference), bar = $('div', undefined, 'toolbar context-actions');
    parent.append(bar);
    button('复制定位链接', async () => {
      const link = location.origin + location.pathname + V.encodeView(value);
      try { await navigator.clipboard.writeText(link); }
      catch { const text = $('textarea'); text.readOnly = true; text.value = link; text.setAttribute('aria-label','定位链接'); bar.append(text); text.select(); }
    }, bar);
    raw(bar, '完整定位引用', value);
  }
  async function openView(value, {historyMode = 'push'} = {}) {
    if(!canLeaveViews())return;
    value = V.validateView(value);
    if (value.workspace !== (value.v === 2 ? workspaceId : context.workspace)) throw new Error('workspace_mismatch：此链接属于另一个工作区');
    if(value.v === 1)value={...value,v:2,workspace:workspaceId};
    rememberTab();
    readController.abort();readController=new AbortController();
    const epoch = ++viewEpoch; selectionEpoch++; revokeImages();disposeReaders();closeNavigation();updates={};
    pendingView = value; clearSelection(); viewState = 'loading'; main.replaceChildren();main.classList.remove('bound-case-active','original-case-active','original-table-active','workbench-active','data-active','data-notebook-active','data-case-config-active','data-catalog-expanded','data-catalog-collapsed');projectHeading();
    if(value.kind==='table_request'){
      putHistory(value,historyMode);historyMode='replace';
      main.append($('p','正在读取请求的数据引用…','busy view-loading'));
    }
    markFunction();
    try {
      const page = pageKind();
      if (page === 'development') {
        developmentView = await ForgeDevelopment.mount({main, api, workspaceId, view:value, mountText, disposeReaders,
          openSession:id=>openView(target('development_session',{session_id:id})), error, register:handle=>{developmentView=handle;}});
        if(epoch===viewEpoch&&developmentView?.sessionId)value=target('development_session',{session_id:developmentView.sessionId});
      }
      else if (page === 'overview') overview();
      else if (page === 'tables') await tables(value,reference=>{
        if(epoch!==viewEpoch)throw staleRequest();
        const resolved=V.validateView(reference);
        if(resolved.v!==2||resolved.workspace!==workspaceId||resolved.kind!=='table')throw new Error('workspace_mismatch：数据引用属于另一个工作区');
        value=resolved;pendingView=value;main.querySelector('.view-loading')?.remove();
        putHistory(value,'replace');projectHeading();
      });
      else if (pipelinePage(page)) {main.classList.add('workbench-active');const mounted=await pipeline(value);value=mounted.reference;}
      if (epoch !== viewEpoch) throw staleRequest();
      view = value; pendingView = null; viewState = 'ready';
      if (!selection) clearSelection();
      putHistory(value, historyMode);
    } catch (err) {
      if (epoch === viewEpoch) {
        main.querySelector('.view-loading')?.remove();view = value; pendingView = null; putHistory(value, historyMode);clearSelection();viewState = 'error';
        const localVisible=err.presentedIn&&err.presentedIn.isConnected&&err.presentedIn.offsetParent!==null;
        if(!localVisible)renderLoadFailure(main,err,{retryRead:()=>openView(value,{historyMode:'replace'}),scope:{project:workspaceId,operation:value.kind,label:'页面内容'},isCurrent:()=>epoch===viewEpoch&&workspaceId===value.workspace});
        if(['table_request','table','case'].includes(value.kind))main.append($('p','未打开的数据引用：'+value.alias,'muted small'));
      }
      throw err;
    }
  }
  async function navigate(page) {
    if(pipelinePage(page)&&workbenchView&&['ready','loading'].includes(viewState)&&pipelinePage(pageKind())){
      rememberTab();await workbenchView.showPage(page);closeNavigation();return;
    }
    return openView(tabTarget(page));
  }
  function syncStatus(message, failed=false) {
    const box=document.getElementById('sync-status');if(!box)return;
    box.className='nav-note'+(failed?' sync-error':'');
    const text=message+(lastObserved?' · 最近同步：'+lastObserved.toLocaleTimeString():'');
    box.querySelector('.sync-description').textContent=text;
    document.getElementById('workspace-sync-detail').textContent=text;
    updateWorkspaceStatus();
  }
  function updateWorkspaceStatus() {
    const status=document.getElementById('workspace-status');if(!status)return;
    const box=document.getElementById('sync-status'),failed=box.classList.contains('sync-error'),attention=auditPanel?.classList.contains('audit-attention');
    const state=failed?'error':attention?'attention':document.hidden?'paused':lastObserved?'ready':'pending';
    status.dataset.state=state;
    box.querySelector('.sync-caption').textContent={error:'同步异常',attention:'检查需关注',paused:'同步暂停',ready:'已同步',pending:'正在连接'}[state];
    status.querySelector('summary').title=[box.querySelector('.sync-description').textContent,auditPanel?.hidden?'':auditPanel?.querySelector('summary')?.textContent].filter(Boolean).join('\n');
  }
  function renderAudit() {
    if(!auditPanel)return;
    const expanded=auditPanel.open;
    auditPanel.replaceChildren();auditPanel.hidden=!auditEnabled||!workspaceId;auditPanel.classList.remove('audit-attention');
    if(!auditEnabled||!workspaceId){updateWorkspaceStatus();return;}
    const now=(auditState?.observed_at||0)+(performance.now()-auditFetchedAt)/1000;
    const rows=(auditState?.observations||[]).map(row=>({...row,freshness:row.freshness==='current'&&now>row.due_at?'overdue':row.freshness})),scope=rows.find(r=>r.kind==='scope');
    const labels={unconfigured:'未启用',unknown:'尚未核验',within_scope:'范围内',violated:'存在越界修改',checked:'依据有效',stale:'依据已变化',invalid:'不符合契约',unverifiable:'无法完成核验'};
    const label=row=>row.freshness==='pending'?'发现变化，待核验':row.freshness==='overdue'?'核验已超期':labels[row.result.status]||row.result.status;
    auditPanel.append($('summary','工作区检查 · 修改范围：'+(scope?label(scope):'正在读取')));
    if(auditError)auditPanel.append($('p','检查信息暂未更新；保留上次结果。 '+auditError,'sync-error small'));
    if(!auditState){updateWorkspaceStatus();return;}
    auditPanel.append($('p',`已绑定 ${auditState.coverage.tracked} / ${auditState.coverage.registered} 份入口配置的检查依据。未绑定项不代表已通过。`,'muted small'));
    auditPanel.append($('p',auditState.watcher.status==='observed'&&now-auditState.watcher.heartbeat<=180?'后台巡检最近有响应；下面分别显示内容核验时间。':'后台巡检状态不可用；执行前仍会现场核验。','muted small'));
    for(const row of rows){
      const item=$('div',undefined,'audit-row');
      const title=row.kind==='scope'?'允许修改范围':(row.result.entry||('检查报告 '+row.identity.slice(0,12)))+(row.result.parameter_path?' · '+row.result.parameter_path:'');
      item.append($('strong',title+'：'+label(row)));
      item.append($('span',row.verified_at?'上次内容核验：'+new Date(row.verified_at*1000).toLocaleString():'尚无内容核验结果','muted small'));
      if(row.result.error)item.append($('p',row.result.error.message,'sync-error'));
      const changes=row.result.changes||[];
      if(changes.length){
        const list=$('ul');
        for(const change of changes.slice(0,20))list.append($('li',typeof change==='string'?change:((change.allowed?'范围内':'越界')+' · '+change.path+' · '+({added:'新增',deleted:'删除',modified:'修改',type_changed:'类型改变'}[change.change]||change.change))));
        item.append(list);
        if(changes.length>20||row.result.truncated)item.append($('p','仅显示部分明细；完整计数：'+(row.result.change_count??changes.length),'muted small'));
      }
      if(row.freshness!=='unconfigured'&&auditState.enabled)button(row.kind==='scope'?'核验修改范围':'核验这份报告',async()=>{
        const id=workspaceId,request={kind:row.kind,expected_policy:auditState.policy_digest};
        if(row.kind==='report')request.digest=row.identity;
        await api('audit-check',{},request);
        if(id===workspaceId)await refreshAudit();
      },item);
      auditPanel.append(item);
    }
    auditPanel.open=expanded||rows.some(r=>r.result.status==='violated');
    auditPanel.classList.toggle('audit-attention',rows.some(r=>['violated','stale','unverifiable','invalid'].includes(r.result.status)||['overdue','pending'].includes(r.freshness)));
    const attention=rows.filter(r=>['violated','stale','unverifiable','invalid'].includes(r.result.status)||['overdue','pending'].includes(r.freshness));
    if(attention.length)auditPanel.querySelector('summary').textContent='工作区检查 · '+attention.map(label).filter((v,i,a)=>a.indexOf(v)===i).join(' / ');
    updateWorkspaceStatus();
  }
  async function refreshAudit() {
    if(!auditEnabled)return true;
    const request=++auditRequest;
    try {
      const result=await api('audit');
      if(request!==auditRequest)return false;
      auditState=result;auditError=null;auditFetchedAt=performance.now();renderAudit();return true;
    } catch(err) {
      if(err.name==='AbortError')throw err;
      if(request===auditRequest){auditError=err.message;renderAudit();}
      return false;
    }
  }
  function scheduleSync() {
    clearTimeout(syncTimer);
    if(!offline&&!document.hidden&&!authFailed)syncTimer=setTimeout(()=>syncView().catch(()=>{}),3000);
  }
  async function syncView(force=false) {
    if(authFailed)return;
    forcePending ||= force;
    if(refreshing)return refreshing;
    clearTimeout(syncTimer);
    const task=(async()=>{
      do {
        if(viewState==='loading'||activeWrites){scheduleSync();return;}
        const forced=forcePending;forcePending=false;
        const epoch=viewEpoch, selected=selectionEpoch;
        await refreshProjects();
        if(!workspaceId){
          if(projectList.length)await switchProject(projectList[0].workspace_id,null,'replace');
          else syncStatus('尚未登记项目');
          return;
        }
        const next=await api('changes');
        if(context&&next.workspace!==context.workspace)throw new Error('工作区身份不一致');
        const changed=key=>!changes||json(next[key])!==json(changes[key]);
        const graphChanged=['workspace_stamp','sources','tables','discovery'].some(changed);
        const catalogChanged=graphChanged||changed('reports');
        const project=projectList.find(p=>p.workspace_id===workspaceId);
        const permissionsChanged=!!project&&(json(project.permissions)!==json(context?.run_permissions)||json(project.capabilities)!==json(context?.capabilities));
        if(!context||forced||catalogChanged||permissionsChanged){
          const fresh=await api('context');
          if(fresh.workspace!==next.workspace)throw new Error('工作区身份不一致');
          updates.context?.(fresh);
          context=fresh;
          projectHeading();
          for(const b of nav.querySelectorAll('[data-page]'))b.disabled=false;
        }
        if(epoch!==viewEpoch||selected!==selectionEpoch)throw staleRequest();
        if(activeWrites)return;
        const current=view||initialView||target('overview');
        const position=window.scrollY;
        if(viewState==='error'){
          if(forced||!['table_request','table','case'].includes(current.kind))await openView(current,{historyMode:'replace'});
        } else if(pipelinePage(pageKind())&&workbenchView){
          await workbenchView.sync(context,{force:forced,graphChanged,catalogChanged,runsChanged:changed('runs')||changed('run_revision')});
        } else if(forced||viewState==='error') {
          await openView(current,{historyMode:'replace'});
        } else if(pageKind()==='overview'&&catalogChanged) {
          await openView(current,{historyMode:'replace'});
        } else if(pageKind()==='tables') {
          if(catalogChanged)await updates.tables?.();
          if(changed('runs'))await updates.results?.();
        }
        const checkAudit=auditEnabled&&(forced||auditError||changed('audit')||changed('workspace_stamp')||!auditState);
        // Advance the project baseline independently of optional audit reads.
        // A failed audit must not cause the same graph to rebuild every tick.
        changes=next;lastObserved=new Date();window.scrollTo(0,position);
        if(checkAudit)await refreshAudit();else renderAudit();
        syncStatus(auditError?'项目已同步；工作区检查暂未更新。':'自动同步已开启',!!auditError);
      } while(forcePending&&!document.hidden);
    })();
    refreshing=task;
    try{await task;}catch(err){if(err.name!=='AbortError')syncStatus('暂未更新：'+err.message+(authFailed?'':'\n保留上次内容，将自动重试。'),true);throw err;}
    finally{if(refreshing===task)refreshing=null;scheduleSync();}
  }
  function refreshView(){return syncView(true);}
  function originalProject(parent=main) {
    const project=context.project;if(!project)return;
    const p=panel('原项目识别',parent);
    p.append($('p','读取已有声明和文档引用；识别结果不代表规范通过，也不会授权执行。','muted'));
    for(const d of project.declarations||[])p.append($('p',d.pipeline+' · '+d.standard+' · '+d.path,'muted small'));
    const viewer=$('div');
    table(p,['入口','协议 / 规范','依据'],project.entries.map(e=>({value:e,cells:[e.entry,(e.protocol==='native_flow'?'原生 Flow':'config → run_pipeline')+' · '+(e.standard||'平台工作区声明'),e.evidence]})),async e=>{
      const source=await api('source',{path:e.path,...(e.sha256?{expected_sha256:e.sha256}:{})});
      if(viewer.isConnected)viewer.replaceChildren($('h3',e.entry),$('pre',source.text));
    });
    if(!project.entries.length)p.append($('p','尚未识别入口。具体原因见下面的问题列表。','notice'));
    if(project.operators.length)table(p,['算子声明','可用性','依据'],project.operators.map(o=>({cells:[o.path,o.status==='available'?'路径存在 · 尚不代表规范通过':'路径缺失',o.evidence]})));
    if(project.configurations.length)table(p,['配置（点击查看原文）','识别结果','依据'],project.configurations.map(c=>({value:c,cells:[c.path,({native_parameters:'原生参数',original_configuration:'原项目配置',referenced:'文档引用',unreadable:'无法读取'}[c.status]||c.status),c.evidence]})),async c=>{
      if(!c.sha256)throw new Error('配置未能读取；请查看具体诊断。');
      const value=await api('project-document',{path:c.path,expected:c.sha256});
      if(viewer.isConnected)viewer.replaceChildren($('h3',c.path),$('pre',value.text));
    });
    p.append(viewer);
    for(const d of project.diagnostics)p.append($('p',d.path+' · '+d.detail,'notice'));
    raw(p,'识别范围与内容摘要',{coverage:project.coverage,digest:project.digest,evidence:project.evidence});
  }
  function originalRuns(parent=main) {
    const p=panel('原项目运行声明',parent);
    function render(){
      p.replaceChildren($('h2','原项目运行声明'));
      const refs=context.project?.runs||[];
      p.append($('p',refs.length?'以下是文档中的产物引用，未核验为受支持的运行记录；不能据此判断成功、失败或可恢复。':'未识别到受支持的原项目运行状态声明；这不表示原项目从未运行。','notice'));
      if(refs.length)table(p,['产物引用','依据'],refs.map(r=>({cells:[r.path,r.evidence]})));
    }
    render();
  }
  function originalData(onOpen=()=>{},onSource=()=>{},{catalogParent=main,viewerParent=null}={}) {
    const p=panel('原项目声明的数据',catalogParent),listing=$('div',undefined,'original-listing'),viewer=$('div',undefined,'original-viewer'),stale=$('p','原项目声明已变化。已打开的内容保留；请返回当前表并重新选择数据，核对最新依据。','notice');stale.hidden=true;p.classList.add('original-data','table-catalog');p.append(listing,stale);(viewerParent||p).append(viewer);
    let selectedDigest=null,sequence=0,profileHandle=null;
    function render(){
      listing.replaceChildren($('p','读取已有引用，不新增数据登记。未声明版本的表在打开时固定版本，翻页与 Case 详情保持该版本。','muted'));
      const refs=context.project?.tables||[];
      table(listing,['数据位置','声明版本','可用性','依据'],refs.map(t=>({value:t,cells:[t.path,t.version===null?'文档未声明 · 打开时固定':'v'+t.version,({available:'可查看',missing:'路径不存在',outside_project:'项目外 · 未授权读取'}[t.status]||t.status),t.evidence.join('\n')]})),open);
      if(!refs.length)listing.append($('p','尚未识别到可解析的数据引用；不代表项目没有数据。','notice'));
      stale.hidden=!selectedDigest||selectedDigest===context.project?.digest;
    }
    async function open(ref){
      if(ref.status!=='available')throw new Error('此声明无法读取：'+ref.status+'。平台不会扩大数据访问范围。');
      const seq=++sequence,expected=context.project.digest;selectionEpoch++;
      const metadata=await api('project-table',{id:ref.id,expected});
      if(seq!==sequence||!viewer.isConnected)throw staleRequest();
      selectedDigest=expected;stale.hidden=true;
      onOpen();
      profileHandle?.dispose();disposeReaders(viewer);viewer.replaceChildren($('h3',ref.path),$('p','本次查看固定为 v'+metadata.version+' · '+metadata.row_count+' 行','notice'));
      raw(viewer,'字段与来源',metadata);
      const controls=$('div',undefined,'toolbar'),columns=$('input');columns.placeholder='字段投影（逗号分隔；默认前 16 列）';columns.setAttribute('aria-label','原项目数据字段');columns.className='wide';controls.append(columns);viewer.append(controls);
      const rows=$('div'),pager=$('div',undefined,'toolbar'),detail=$('div',undefined,'original-case-body');viewer.append(rows,pager,detail);
      let offset=0,pageSequence=0;
      const query={id:ref.id,expected,version:metadata.version,binding:metadata.binding};
      profileHandle=registerViewHandle(catalogProfile(viewer,'project-table-profile',()=>({...query,columns:columns.value.trim()?V.validateColumns(columns.value.split(',').map(value=>value.trim()).filter(Boolean)):Object.keys(metadata.fields||{}).slice(0,16)})));
      async function load(){
        const page=++pageSequence;selectionEpoch++;rows.hidden=false;pager.hidden=false;detail.hidden=false;
        const projection=columns.value.trim()?V.validateColumns(columns.value.split(',').map(v=>v.trim()).filter(Boolean)):null;
        const result=await api('project-sample',{...query,offset,limit:20,...(projection?{columns:JSON.stringify(projection)}:{})});
        if(seq!==sequence||page!==pageSequence||!rows.isConnected)throw staleRequest();
        if(result.columns.length)onSource({label:ref.path+' · v'+metadata.version,title:ref.path,version:metadata.version,rows:metadata.row_count,fields:Object.keys(metadata.fields||{}).length,
          locator:{kind:'project_table',id:ref.id,discovery:expected,version:metadata.version,binding:metadata.binding,columns:result.columns.filter(name=>name!=='_rowid')}});
        disposeReaders(detail);rows.replaceChildren();pager.replaceChildren();detail.replaceChildren();
        table(rows,['_rowid',...result.columns],result.rows.map(r=>({value:r,cells:['_rowid',...result.columns].map(k=>{const text=typeof r[k]==='string'?r[k]:JSON.stringify(r[k]);return text?.length>140?text.slice(0,140)+'…':text;})})),async row=>{
          if(typeof row._rowid==='number'?!Number.isSafeInteger(row._rowid):typeof row._rowid!=='string'||!/^\d{1,20}$/.test(row._rowid))throw new Error('此行定位无法精确表示；请使用 CLI 读取。');
          const chosen=++selectionEpoch;
          const value=await api('project-sample',{...query,row_id:row._rowid,columns:JSON.stringify(result.columns)});
          if(chosen!==selectionEpoch||page!==pageSequence||seq!==sequence||!detail.isConnected)throw staleRequest();
          if(value.rows.length!==1||String(value.rows[0]._rowid)!==String(row._rowid))throw new Error('指定 Case 已不可读取');
          disposeReaders(detail);const bound=document.getElementById('case-browser');if(bound){disposeReaders(bound);bound.querySelector('.case-detail')?.replaceChildren();}detail.replaceChildren($('h1','Case · '+row._rowid+' · v'+metadata.version),$('p',context.name+' → '+ref.path+' → Case '+row._rowid,'muted small'));
          button('返回当前表',()=>{main.classList.remove('original-case-active');detail.hidden=true;disposeReaders(detail);detail.replaceChildren();rows.hidden=false;pager.hidden=false;rows.querySelector('tbody tr')?.focus();},detail);
          detail.append($('p','来源：原项目声明 · 固定版本 v'+metadata.version+'；未读取的字段与外部资源不作推断。','notice'));
          renderCase(detail,null,value.rows[0],{});raw(detail,'字段、版本与来源',metadata);detail.hidden=false;rows.hidden=true;pager.hidden=true;main.classList.add('original-case-active');main.classList.remove('bound-case-active');detail.scrollIntoView({block:'start'});
        });
        pager.append($('span',(result.rows.length?offset+1:0)+'–'+(offset+result.rows.length)+' / '+result.row_count+' 行','muted'));
        button('上一页',async()=>{offset=Math.max(0,offset-20);await load();},pager,offset===0);
        button('下一页',async()=>{offset+=20;await load();},pager,!result.has_more);
      }
      button('读取字段',async()=>{offset=0;await load();},controls);
      button('重新查看声明版本',()=>open(ref),controls);
      await load();
    }
    render();
    if(pendingOriginalPath){const path=pendingOriginalPath;pendingOriginalPath=null;const ref=context.project?.tables.find(t=>t.path===path&&t.status==='available');if(ref)open(ref).catch(err=>error(viewer,err));}
    return render;
  }
  function sourceInventory(parent=main) {
    if(!context.source_files?.length&&!context.source_diagnostics?.length)return;
    const files=panel('原项目源码',parent);
    for(const message of context.source_diagnostics||[])files.append($('p',message,'notice'));
    const viewer=$('div');
    table(files,['文件'],(context.source_files||[]).map(path=>({value:path,cells:[path]})),async path=>{
      const source=await api('source',{path});if(!viewer.isConnected)return;
      disposeReaders(viewer);viewer.replaceChildren($('h3',source.path),$('p','当前源码 · '+source.sha256,'muted small'));
      mountText(viewer,source.text,{sourceOnly:true});
    });files.append(viewer);
  }
  function documentDialog(title, text) {
    const dialog=$('dialog');dialog.className='document-dialog';dialog.append($('h2',title));
    const host=$('div');dialog.append(host);mountText(host,text,{sourceOnly:true});
    button('关闭',()=>dialog.close(),dialog);document.body.append(dialog);
    dialog.addEventListener('close',()=>{disposeReaders(dialog);dialog.remove();},{once:true});dialog.showModal();
  }
  function resolveProjectLink(href) {
    const path=href.replace(/^\.\//,''),epoch=viewEpoch,id=workspaceId,project=context.project;
    const current=()=>{if(id!==workspaceId||epoch!==viewEpoch)throw staleRequest();};
    const config=project?.configurations.find(c=>c.path===path&&c.sha256);
    if(config)return {label:'查看配置原文 · '+path,open:async()=>{current();const value=await api('project-document',{path,expected:config.sha256});current();documentDialog(path,value.text);}};
    if(context.source_files?.includes(path)){
      const declared=project?.entries.find(e=>e.path===path&&e.sha256);
      return {label:'查看源码 · '+path,open:async()=>{current();const value=await api('source',{path,...(declared?{expected_sha256:declared.sha256}:{})});current();documentDialog(path,value.text);}};
    }
    const bound=context.tables.filter(t=>t.uri===context.workspace.replace(/\/+$/,'')+'/'+path&&t.binding_digest);
    if(bound.length===1)return {label:'查看固定版本表 · '+bound[0].alias,open:async()=>{current();await openTable(bound[0].alias,bound[0].binding_digest);}};
    const refs=project?.tables.filter(t=>t.path===path&&t.status==='available')||[];
    if(refs.length===1)return {label:'查看数据 · '+path,open:async()=>{current();pendingOriginalPath=path;await openView(target('tables'));}};
    return null;
  }
  function overview() {
    const intro=panel('README.md');intro.id='project-readme';intro.tabIndex=-1;
    if(!context.readme){intro.append($('p','尚无 README.md。可在开发中补充项目说明。','notice'));button('进入开发',()=>navigate('development'),intro);}
    else if(context.readme.startsWith('项目说明无法读取：'))intro.append($('p',context.readme,'error-box'));
    else mountText(intro,context.readme,{resolveLink:resolveProjectLink});
    const info=$('details');info.append($('summary','项目内容、能力与依据'));main.append(info);
    info.append($('p',context.flows.length+' 个登记流程 · '+context.tables.length+' 个固定数据引用 · '+context.report_total+' 份报告；报告数量不代表全部通过。','muted small'));
    button('查看管道与检查',()=>navigate('flows'),info);button('查看数据',()=>navigate('tables'),info);
    if(Object.keys(context.locations).length)table(info,['位置','根目录','权限'],Object.entries(context.locations).map(([name,value])=>({cells:[name,value.root,value.writable?'可声明输出':'只读']})));
    raw(info,'原生能力覆盖',context.native_api_coverage);raw(info,'版本化工程规范',context.standard);
  }

  async function projectChecks(parent=main,{project=workspaceId,isCurrent=()=>true,onBegin=()=>{},onEnd=()=>{}}={}) {
    const controls=$('div',undefined,'toolbar');parent.append(controls);
    const area=panel('当前项目规范检查',parent);
    const names={passed:'通过',checked:'静态检查通过',missing:'缺少证据',invalid:'不符合规范',incomplete:'尚未完成',stale:'依据已变化'};
    function render(value) {
      if(!area.isConnected||!isCurrent())return;
      area.replaceChildren($('h2','检查结果'));
      area.append($('p','整体：'+(names[value.status]||value.status)+'；静态检查：'+(names[value.static_status]||value.static_status)));
      if(value.standard)area.append($('p','统一规范版本：'+value.standard.version,'muted'));
      if(value.coverage)area.append($('p','已检查 '+value.coverage.inspected+' / '+value.coverage.declared+' 组已声明入口与配置；未声明代码不计为通过。'));
      if(value.coverage?.recognized_entries!==undefined)area.append($('p','已识别 '+value.coverage.recognized_entries+' 个原入口；其中 '+value.coverage.source_inspected+' 个完成或尝试了源码前置检查，'+value.coverage.unbound_entries+' 个缺少原生协议或参数绑定，不能完成 Flow 语义检查。','notice'));
      if(value.source_checks?.length)table(area,['原入口','源码前置检查','实际读取的源码'],value.source_checks.map(s=>({cells:[s.entry,names[s.status]||s.status,s.sources.join('\n')||'未读取']})));
      area.append($('p','行为验证和语义审查的缺失证据会单独列出。修改项目需要你的明确确认。','notice'));
      if(value.changes?.length)area.append($('p',value.changes.join('\n'),'notice'));
      for(const item of value.diagnostics||[])area.append($('p',[item.rule,(item.path||'')+(item.path&&item.line?':'+item.line:''),item.detail].filter(Boolean).join(' · '),'error-box'));
      if(value.entries?.length)table(area,['入口','配置','结果'],value.entries.map(item=>({value:item,cells:[item.entry,item.params,names[item.status]||item.status]})),item=>item.digest?openView(target('inspection',{report_digest:item.digest})):undefined);
      if(value.rules?.length){const rules=$('details');rules.append($('summary','查看全部规则与证据'));area.append(rules);
        table(rules,['规则','要求','证据'],value.rules.map(rule=>({cells:[rule.id,rule.requirement,Object.entries(rule.methods).map(([m,s])=>({static:'静态',behavior:'行为',review:'审查'}[m])+': '+(names[s]||s)).join('；')]})));}
    }
    let checking=false;
    button('检查当前项目',async()=>{
      if(checking||!controls.isConnected||!controls.getClientRects().length||!isCurrent())return;
      checking=true;onBegin();
      try{const value=await api('project-check',{},{},project);render(value);}
      finally{checking=false;onEnd();}
    },controls);
    render(await api('project-status',{},undefined,project));
  }

  function projectDialog(create) {
    const dialog=$('dialog'),form=$('form'),title=create?'新建项目':'注册已有项目';
    form.append($('h2',title),$('p',create?'填写服务所在机器上的目标目录。目录必须不存在或为空。':'填写原 pipeline 的目录。原文件保持不变；平台只添加必要管理信息。'));
    const root=$('input'),name=$('input');root.required=true;root.placeholder='服务器上的绝对目录';root.setAttribute('aria-label','项目目录');root.maxLength=4096;
    name.placeholder=create?'项目名称':'名称（可留空）';name.setAttribute('aria-label','项目名称');name.required=create;name.maxLength=128;
    form.append(root,name);
    const save=$('button',title);save.type='submit';form.append(save);
    const close=button('取消',()=>dialog.close(),form);close.type='button';
    let failure;
    form.onsubmit=async event=>{event.preventDefault();save.disabled=true;failure?.remove();try{
      const body={root:root.value.trim()};if(name.value.trim())body.name=name.value.trim();
      const result=await api(create?'project-create':'project-register',{},body);
      dialog.close();await refreshProjects();await switchProject(result.workspace_id,null,'replace');
    }catch(err){failure=error(form,err);}finally{save.disabled=false;}};
    dialog.append(form);document.body.append(dialog);dialog.addEventListener('close',()=>dialog.remove(),{once:true});dialog.showModal();root.focus();
  }

  function removeProjectDialog(project) {
    if(activeWrites)throw new Error('正在提交操作，完成后再移除项目。');
    const expected=projectDigest,dialog=$('dialog');
    dialog.append($('h2','移除项目'),$('p',project.name),$('p',project.root,'muted small'),
      $('p','仅取消本控制台的登记和后续巡检，项目将不再展示。项目文件、管理信息和历史数据全部保留，之后可重新注册。'),
      $('p','已启动的任务继续运行；本页面中该项目尚未提交的参数草稿会清除。','muted'));
    button('取消',()=>dialog.close(),dialog);
    button('确认移除',async()=>{
      if(activeWrites)throw new Error('正在提交操作，请稍后重试。');
      await api('project-remove',{},{workspace_id:project.workspace_id,expected});
      dialog.close();await refreshProjects();
      if(!workspaceId&&projectList.length)await switchProject(projectList[0].workspace_id,null,'replace');
    },dialog);
    document.body.append(dialog);dialog.addEventListener('close',()=>dialog.remove(),{once:true});dialog.showModal();
  }
  async function openTable(alias, expected) {
    await openView(target('table_request',{alias,...(expected?{expected_binding:expected}:{})}));
  }
  function insertAnalysisTemplate({language,sourceAlias,field,filter},analysisHandle){
    if(!analysisHandle||typeof analysisHandle.insertTemplate!=='function')return false;
    if(language!=='sql'&&language!=='python')return false;
    if(typeof sourceAlias!=='string'||!/^[A-Za-z_][A-Za-z0-9_]{0,79}$/.test(sourceAlias))return false;
    const ident=name=>typeof name==='string'&&name.length>0&&name.length<=256&&!/[\u0000-\u001f]/.test(name)?'"'+name.replace(/"/g,'""')+'"':null;
    const literal=value=>{
      if(typeof value==='boolean')return {sql:value?'TRUE':'FALSE',py:value?'True':'False'};
      if(typeof value==='number')return Number.isFinite(value)?{sql:String(value),py:String(value)}:null;
      if(typeof value==='string')return value.includes('\u0000')?null:{sql:"'"+value.replace(/'/g,"''")+"'",py:JSON.stringify(value)};
      return null;
    };
    let where='',pyWhere='',title='查询模板 · '+sourceAlias;
    if(filter!==undefined&&filter!==null){
      if(typeof filter!=='object'||Array.isArray(filter))return false;
      const column=ident(filter.field),key=JSON.stringify(filter.field);
      if(!column||typeof filter.field!=='string')return false;
      if(filter.op==='is_null'){where=column+' IS NULL';pyWhere='row.get('+key+') is None';}
      else if(filter.op==='eq'){const v=literal(filter.value);if(!v)return false;where=column+' = '+v.sql;pyWhere='row.get('+key+') == '+v.py;}
      else if(filter.op==='contains'){if(typeof filter.value!=='string')return false;const v=literal(filter.value);if(!v)return false;where='strpos('+column+', '+v.sql+') > 0';pyWhere='isinstance(row.get('+key+'), str) and '+v.py+' in row['+key+']';}
      else if(filter.op==='in'){
        if(!Array.isArray(filter.value)||!filter.value.length||filter.value.length>64)return false;
        const parts=filter.value.map(literal);if(parts.some(part=>!part))return false;
        where=column+' IN ('+parts.map(part=>part.sql).join(', ')+')';pyWhere='row.get('+key+') in ['+parts.map(part=>part.py).join(', ')+']';
      }
      else if(filter.op==='range'){
        const lower=filter.value?.lower,upper=filter.value?.upper;
        if(typeof lower!=='number'||typeof upper!=='number')return false;
        const a=literal(lower),b=literal(upper);if(!a||!b)return false;
        where=column+' >= '+a.sql+' AND '+column+' <= '+b.sql;pyWhere='isinstance(row.get('+key+'), (int, float)) and '+a.py+' <= row['+key+'] <= '+b.py;
      }
      else return false;
      title='筛选模板 · '+sourceAlias+'.'+filter.field;
    }
    let source;
    if(language==='sql'){
      const projection=filter?'*':field?ident(field):'*';
      if(!projection)return false;
      source='SELECT '+projection+' FROM '+ident(sourceAlias)+(where?' WHERE '+where:'')+' LIMIT 20';
    }else{
      const key=field===undefined||field===null?null:JSON.stringify(field);
      if(field!==undefined&&field!==null&&(typeof field!=='string'||!field||field.length>256))return false;
      source=filter
        ?'# 有界样例筛选草稿：只过滤已读取的样例行，不代表全表结果。\nrows = source('+JSON.stringify(sourceAlias)+").sample(limit=100)['rows']\nfiltered = [row for row in rows if "+pyWhere+']\nfiltered[:20]'
        :'rows = source('+JSON.stringify(sourceAlias)+").sample(limit=20)['rows']\n"+(key?'[row.get('+key+') for row in rows]':'rows');
    }
    if(source.length>16384)return false;
    return analysisHandle.insertTemplate({language,source,title:title.slice(0,120)})===true;
  }
  function catalogProfile(parent,endpoint,parameters){
    const details=$('details',undefined,'catalog-profile'),output=$('div');details.append($('summary','字段分布'),output);parent.append(details);
    const controls=$('div',undefined,'toolbar profile-controls'),fieldChoice=select([],'');fieldChoice.setAttribute('aria-label','分布字段');
    output.append($('p','按当前固定版本与字段投影读取统计；默认前 16 列。最多扫描前 1000 行 / 8 MiB 投影数据。只有真实枚举计数画成条形，不是完整直方图。','muted small'),controls);
    const failureHost=$('div'),results=$('div'),draftHost=$('div');output.append(failureHost,results,draftHost);
    let controller=null,disposed=false,loaded=false,lastFields=[],lastField='';
    const handle={element:details,ready:null,dispose(){disposed=true;controller?.abort();controller=null;},select(name){details.open=true;if(loaded&&lastFields.includes(name)){fieldChoice.value=name;openField(name);}else read(name).then(()=>openField(name));}};
    function openField(name){const item=results.querySelector('[data-field="'+CSS.escape(name)+'"]');if(item){item.open=true;item.scrollIntoView({block:'nearest'});}}
    function showDraft(field,entry){
      const filter=entry===null?{op:'is_null',field}:{op:'eq',field,value:entry.value};
      draftHost.replaceChildren();
      const box=$('div',undefined,'filter-draft');draftHost.append(box);
      box.append($('h4','筛选草稿 · '+field),$('p','这是可见草稿，样例并未筛过全表。应用到 Case 请把 typed_filter 粘贴到其筛选器；插入 Notebook 的是显式模板，由你运行。','muted small'),$('pre',json(filter)));
      const bar=$('div',undefined,'toolbar');box.append(bar);
      const aliasInput=$('input');aliasInput.value='data';aliasInput.maxLength=80;aliasInput.setAttribute('aria-label','Notebook 数据源绑定名');
      const aliasLabel=$('label','绑定名');aliasLabel.append(aliasInput);bar.append(aliasLabel);
      button('复制 typed_filter',async()=>{
        try{await navigator.clipboard.writeText(json(filter));}
        catch{const text=$('textarea');text.readOnly=true;text.value=json(filter);text.setAttribute('aria-label','typed_filter 草稿');box.append(text);text.select();}
      },bar);
      const status=$('p',undefined,'muted small');
      const insert=language=>{
        const accepted=insertAnalysisTemplate({language,sourceAlias:aliasInput.value.trim(),field,filter},analysisView);
        status.textContent=accepted?'已插入 Notebook 草稿单元（未运行）。':'Notebook 未接受该模板：空间不足或当前不可插入。草稿保留，可复制 typed_filter。';
        status.className=accepted?'notice':'notice';
      };
      const sqlButton=button('插入 SQL 模板',()=>insert('sql'),bar),pyButton=button('插入 Python 模板',()=>insert('python'),bar);
      if(!analysisView||typeof analysisView.insertTemplate!=='function'){sqlButton.disabled=true;pyButton.disabled=true;status.textContent='当前 Notebook 尚未提供模板插入；可复制 typed_filter 到 Case 筛选器。';}
      box.append(status);
    }
    async function read(fieldName){
      if(disposed)return;
      controller?.abort();controller=new AbortController();
      const epoch=viewEpoch,project=workspaceId;
      failureHost.replaceChildren();
      handle.ready=(async()=>{
        const payload=parameters();
        if(!payload.columns?.length)throw new Error('请先选择统计字段');
        const value=await api(endpoint,{...payload,columns:JSON.stringify(payload.columns)},undefined,project,{signal:controller.signal});
        if(disposed||epoch!==viewEpoch||workspaceId!==project||!results.isConnected)throw staleRequest();
        const expected=payload.version_binding||payload.expected_binding||payload.binding;
        if(value.binding!==expected)throw new Error('统计来源与当前固定版本不一致');
        loaded=true;lastFields=value.fields.map(field=>field.name);
        const chosen=fieldName&&lastFields.includes(fieldName)?fieldName:lastFields.includes(fieldChoice.value)?fieldChoice.value:lastFields[0];
        lastField=chosen||'';
        fieldChoice.replaceChildren(...lastFields.map(name=>{const option=$('option',name);option.value=name;return option;}));
        if(lastField)fieldChoice.value=lastField;
        const coverage=value.coverage;
        results.replaceChildren($('p',(coverage.complete?'全表覆盖':'当前结果页抽样')+' · '+coverage.rows_scanned+' / '+coverage.total_rows+' 行 · '+coverage.logical_bytes_scanned+' 投影字节'+(coverage.stopped_by?' · 停止原因 '+coverage.stopped_by:''),'notice'));
        for(const field of value.fields){
          const item=$('details',undefined,'profile-field');item.dataset.field=field.name;
          item.append($('summary',field.name+' · 空值 '+field.null_count+' · 其他短值 '+field.other_short_primitive_count));
          item.append($('p','最小 '+JSON.stringify(field.minimum)+' · 最大 '+JSON.stringify(field.maximum)+' · 非有限 '+field.nonfinite_count+' · 复杂值 '+field.complex_count+' · 长值 '+field.long_or_unrepresentable_count,'profile-facts'));
          const bars=$('div',undefined,'profile-bars');item.append(bars);
          const entries=[...(field.null_count?[{value:null,count:field.null_count}]:[]),...field.enumeration];
          const max=Math.max(1,...entries.map(entry=>entry.count));
          for(const entry of entries){
            const row=$('button',undefined,'profile-bar'+(entry.value===null?' profile-bar-null':''));row.type='button';
            row.setAttribute('aria-label','生成筛选草稿：'+field.name+(entry.value===null?' 为空':' 等于 '+JSON.stringify(entry.value))+'，'+entry.count+' 行');
            const label=$('span',entry.value===null?'null（空值）':JSON.stringify(entry.value),'profile-bar-label');label.title=entry.value===null?'null':JSON.stringify(entry.value);
            const track=$('span',undefined,'profile-bar-track'),fill=$('span',undefined,'profile-bar-fill');fill.style.width=Math.max(2,Math.round(entry.count/max*100))+'%';track.append(fill);
            row.append(label,track,$('span',String(entry.count),'profile-bar-count'));
            row.onclick=()=>showDraft(field.name,entry.value===null?null:entry);
            bars.append(row);
          }
          if(!entries.length)bars.append($('p','该字段在此覆盖内没有可画的枚举计数。','muted small'));
          if(field.name===lastField)item.open=true;
          results.append(item);
        }
        draftHost.replaceChildren();
        return value;
      })();
      try{return await handle.ready;}
      catch(err){
        handle.ready=null;
        if(err?.name==='AbortError'||disposed||epoch!==viewEpoch||!failureHost.isConnected)return;
        renderLoadFailure(failureHost,err,{retryRead:()=>read(fieldName),scope:{project,operation:endpoint,label:'字段分布'},isCurrent:()=>!disposed&&epoch===viewEpoch&&failureHost.isConnected});
      }
    }
    controls.append(fieldChoice);
    button('读取分布',()=>read(),controls);
    fieldChoice.onchange=()=>read(fieldChoice.value);
    return handle;
  }
  async function tables(request,onBinding=()=>{}) {
    main.classList.add('data-active');
    const openingOriginal=!!pendingOriginalPath,dataHeader=$('section',undefined,'data-header'),catalogTools=$('div',undefined,'data-toolbar'),modeTools=$('div',undefined,'data-analysis-modes');dataHeader.append(catalogTools,modeTools);main.append(dataHeader);
    const notebookActions=$('div',undefined,'data-mode-actions'),caseActions=$('div',undefined,'data-mode-actions');
    let catalogExpanded=!context.tables.length||openingOriginal;
    const catalogToggle=button('',()=>setCatalog(!catalogExpanded),catalogTools);
    const dataBody=$('div',undefined,'data-body'),directory=$('aside',undefined,'data-directory'),dataContent=$('div',undefined,'data-content');dataBody.append(directory,dataContent);main.append(dataBody);
    const activeSource=$('div',undefined,'data-source-context'),tableChoice=select([],''),sourceFacts=$('span','选择数据表','muted small');tableChoice.setAttribute('aria-label','当前数据表');activeSource.append(tableChoice,sourceFacts);catalogTools.append(activeSource);
    const samplePanel=$('section',undefined,'data-samples');samplePanel.hidden=!!context.analysis_connected;dataContent.append(samplePanel);let samplesRequested=false,readSamples=null;
    const samplesToggle=button('数据样例与字段分布',async()=>{samplesRequested=samplePanel.hidden;samplePanel.hidden=!samplesRequested;samplesToggle.setAttribute('aria-expanded',String(!samplePanel.hidden));if(samplesRequested&&readSamples)await readSamples();},catalogTools);samplesToggle.setAttribute('aria-expanded',String(!samplePanel.hidden));
    const analysisHost=$('div',undefined,'analysis-workspace');dataContent.append(analysisHost);analysisHost.hidden=true;
    const analysisProject=workspaceId,analysisEpoch=viewEpoch;
    let notebookActive=false,catalogSource=null,caseContext=null,alias=request.alias,fixed=null;
    function paintDataContext(){
      const activeCase=!notebookActive&&context.analysis_connected,selected=activeCase?(caseContext||{phase:'loading',source:null,label:'Case 固定来源'}):null;
      const matches=selected?.source?.kind==='fixed_lance'&&catalogSource&&selected.source.binding===catalogSource.locator.binding&&selected.source.rows===catalogSource.rows;
      tableChoice.replaceChildren();
      const option=$('option',selected?(matches?catalogSource.title:selected.label||'Case 固定来源'):(catalogSource?.title||'选择数据表'));option.value='current';tableChoice.append(option);tableChoice.value='current';
      for(const item of context.tables.filter(item=>!item.run_id)){const choice=$('option',item.alias+' · v'+item.reference.ref.lance_version);choice.value=item.alias;tableChoice.append(choice);}
      const facts=selected?(matches?catalogSource:{rows:selected.source?.rows,fields:selected.source?.schema?.length}):catalogSource;
      const parts=[];
      if(activeCase&&selected?.source&&!matches)parts.push('当前分析固定来源');
      else if(catalogSource)parts.push('目录表');
      if(facts?.version!==undefined)parts.push('固定版本 v'+facts.version+((!selected||matches)&&catalogSource?.locator?.version!==undefined?'（历史固定）':''));
      else if(selected?.source?.digest)parts.push('固定快照 '+selected.source.digest.slice(0,12));
      if(Number.isSafeInteger(facts?.rows))parts.push(facts.rows+' 行');
      if(Number.isSafeInteger(facts?.fields)&&facts.fields>0)parts.push(facts.fields+' 字段');
      if(activeCase&&!caseContext)parts.push('固定来源见下方 Case 分析');
      else if(activeCase&&caseContext?.phase==='loading')parts.push('Case 来源待核对');
      if(activeCase&&caseContext?.phase==='unavailable')parts.push('Case 来源暂不可用');
      if(notebookActive)parts.push('当前表 · 可添加为 Notebook 数据源');
      sourceFacts.textContent=parts.join(' · ')||'在数据目录选择固定版本表';
      sourceFacts.title=selected?.source?.digest||catalogSource?.label||'';
      // The sample reader belongs to the catalog table. An adopted Notebook
      // snapshot must never relabel that old sample as the new Case source.
      // An explicit historical pin keeps its own labelled reader reachable.
      const historicalPin=fixed?.version!==null&&fixed?.version!==undefined;
      samplesToggle.disabled=!!selected&&(!matches||selected.phase!=='ready')&&!historicalPin;
      if(samplesToggle.disabled){samplePanel.hidden=true;samplesToggle.setAttribute('aria-expanded','false');samplesToggle.title='此 Case 使用独立固定来源；材料与字段在下方按该来源读取。';}
      else samplesToggle.title=historicalPin?'查看历史固定版本的数据样例与字段分布':'查看当前目录表的数据样例与字段分布';
    }
    tableChoice.onchange=()=>{const alias=tableChoice.value;tableChoice.value='current';if(alias!=='current'){const item=context.tables.find(item=>item.alias===alias);if(item)openTable(item.alias,item.binding_digest).catch(err=>{if(!err?.presented)error(dataContent,err);});}};
    function selectDataSource(source){if(!analysisView?.canLeave())return false;const sameLocator=!!catalogSource&&json(catalogSource.locator)===json(source.locator);catalogSource=source;if(!sameLocator)caseContext=null;analysisView?.setSource(source);paintDataContext();return true;}
    analysisTransport=ForgeAnalysisTransport.create({project:analysisProject,api,isCurrent:()=>workspaceId===analysisProject&&viewEpoch===analysisEpoch&&analysisHost.isConnected});
    analysisView=ForgeAnalysis.mount({parent:analysisHost,api,project:workspaceId,connected:!!context.analysis_connected,
      executionProfile:context.analysis_execution_profile||null,toolbarHost:{notebook:notebookActions,case:caseActions},
      permissions:context.capabilities||{},features:context.analysis_features||{},mountText,disposeReaders,resolveMedia:analysisTransport.resolveMedia,
      fieldMedia:context.analysis_features?.case_media===true?analysisTransport.fieldMedia:undefined,caseResolveMedia:context.analysis_features?.case_media===true?analysisTransport.resolveCaseMedia:undefined,
      onCaseContext:value=>{if(viewEpoch!==analysisEpoch||workspaceId!==analysisProject||!analysisHost.isConnected)return;caseContext=value;analysisHost.classList.toggle('analysis-case-source-shared',value?.phase==='ready');paintDataContext();},
      onCase:(descriptor,evidence)=>{try{if(analysisView?.setPresentation({descriptor,...evidence,label:'Notebook 固定输出'}))dataMode(false);}catch(err){error(analysisHost,err);}},
      onTable:async value=>{try{
        const fixedSource=value?.source_snapshot?await api('analysis-source',{digest:value.source_snapshot},undefined,analysisProject):
          await api('analysis-source-from-handle',{}, {handle:{id:value.id,generation:value.generation,digest:value.digest}},analysisProject);
        if(viewEpoch!==analysisEpoch||!analysisHost.isConnected)return;
        const columns=(value.columns||fixedSource.schema.map(field=>field.name)).slice(0,16);
        const descriptor={version:1,source_snapshot:fixedSource.digest,case_key:fixedSource.case_key,
          fields:columns.map((field,index)=>({id:'f'+index,field,label:field,role:'context',format:fixedSource.schema.find(item=>item.name===field)?.type==='utf8'?'text':'json'})),
          filters:null,grouping:[],sort:[],layout:'card',questions:[]};
        if(analysisView?.setPresentation({descriptor,label:'Notebook 固定表'}))dataMode(false);
      }catch(err){if(analysisHost.isConnected&&err.name!=='AbortError')error(analysisHost,err);}},
      media:async(host,reference,isCurrent)=>{
        if(!isCurrent())throw staleRequest();
        const historical=Object.hasOwn(reference,'version');
        const blob=await api('media',{alias:reference.alias,expected_binding:historical?reference.alias_binding:reference.binding_digest,row_id:reference.row_id,field:reference.field,...(historical?{version:reference.version,version_binding:reference.binding_digest}:{})});
        if(!isCurrent()||!host.isConnected)throw staleRequest();
        if(mediaBytes+blob.size>32*1024*1024||blobs.size>=64)throw new Error('图片缓存已满，请重新打开数据页释放');
        const url=URL.createObjectURL(blob);mediaBytes+=blob.size;blobs.add(url);blobSizes.set(url,blob.size);
        const image=$('img');image.src=url;image.alt=reference.field;image.className='case-image';host.append(image,$('p','图片来自固定行引用；外部图片内容由已有图片读取规则核对。','muted small'));
      }});
    const notebookMode=button('Notebook',()=>dataMode(true),modeTools),caseMode=button('Case 分析',()=>dataMode(false),modeTools);modeTools.append(notebookActions,caseActions);
    function dataMode(notebook){if(analysisView&&!analysisView.canLeave())return;const configuredCase=!notebook&&!!context.analysis_connected;if(analysisView&&!analysisView.setMode(notebook))return;notebookActive=notebook;notebookActions.hidden=!notebook;caseActions.hidden=notebook||!configuredCase;analysisHost.hidden=!notebook&&!configuredCase;samplePanel.hidden=notebook||configuredCase?!samplesRequested:false;samplesToggle.setAttribute('aria-expanded',String(!samplePanel.hidden));main.classList.toggle('data-notebook-active',notebook);main.classList.toggle('data-case-config-active',configuredCase);notebookMode.setAttribute('aria-pressed',String(notebook));caseMode.setAttribute('aria-pressed',String(!notebook));paintDataContext();}
    dataMode(false);
    function setCatalog(expanded){catalogExpanded=expanded;directory.hidden=!expanded;main.classList.toggle('data-catalog-expanded',expanded);main.classList.toggle('data-catalog-collapsed',!expanded);catalogToggle.textContent=expanded?'收起数据目录':'展开数据目录';catalogToggle.setAttribute('aria-expanded',String(expanded));}
    setCatalog(catalogExpanded);
    const listing = panel('已绑定的数据表',directory);listing.classList.add('table-catalog');
    function catalogRow(name,meta,title,active,action){
      const row=$('button',undefined,'catalog-row'+(active?' active':''));row.type='button';row.title=title;
      if(active)row.setAttribute('aria-current','true');
      const nameSpan=$('span',name,'catalog-row-name');nameSpan.title=title;
      row.append(nameSpan,$('span',meta,'catalog-row-meta'));
      row.onclick=()=>action().catch(err=>{if(err?.name!=='AbortError'&&!err?.presented&&dataContent.isConnected)renderLoadFailure(dataContent,err,{scope:{project:workspaceId,operation:'table',label:name},isCurrent:()=>dataContent.isConnected});});
      return row;
    }
    function listTables(){
      listing.replaceChildren($('h2','已绑定的数据表'));
      const rowsHost=$('div',undefined,'catalog-rows');listing.append(rowsHost);
      const items=context.tables.filter(t=>!t.run_id);
      for(const t of items){
        const title=t.alias+' · '+t.reference.location_id+' / '+t.reference.ref.dataset_id+' · v'+t.reference.ref.lance_version;
        rowsHost.append(catalogRow(t.alias,'v'+t.reference.ref.lance_version+' · '+t.reference.ref.row_count+' 行',title,t.alias===alias,()=>openTable(t.alias,t.binding_digest)));
      }
      if(!items.length)listing.append($('p','尚未绑定数据表。','catalog-empty'));
    }
    const refreshOriginal=originalData(()=>{
      main.classList.remove('bound-case-active','original-case-active');main.classList.add('original-table-active');
      const bound=document.getElementById('case-browser');if(bound){disposeReaders(bound);bound.querySelector('.case-detail')?.replaceChildren();}
      samplesRequested=true;samplePanel.hidden=false;samplesToggle.setAttribute('aria-expanded','true');
      setCatalog(false);
      if(!pendingView){view=target('tables');choose(view);putHistory(view,'replace');projectHeading();}
    },source=>selectDataSource(source),{catalogParent:directory,viewerParent:samplePanel});
    listTables();updates.tables=()=>{listTables();refreshOriginal();};
    const results = panel('运行产物 · 已核验的固定版本',directory);results.classList.add('table-catalog'); let resultOffset = 0, resultRequest = 0, resultPage = [];
    async function loadResults() {
        const sequence=++resultRequest;
        const value = await api('runs',{offset:resultOffset,limit:20}); if (sequence!==resultRequest||!results.isConnected) throw staleRequest();
        resultPage=value.runs;
        results.replaceChildren($('h2','运行产物 · 已核验的固定版本'));
        const rowsHost=$('div',undefined,'catalog-rows');results.append(rowsHost);let rowCount=0;
        for (const run of value.runs) for (const output of run.outputs) if (output.result) {
          addResultTable(run,output);rowCount++;
          const title=(run.parameter_path||run.entry)+' · '+run.run_id+' · '+output.name+' · v'+output.result.ref.lance_version;
          rowsHost.append(catalogRow(output.name+' · '+run.run_id,(run.parameter_path||run.entry)+' · v'+output.result.ref.lance_version+' · '+output.result.ref.row_count+' 行',title,false,()=>openResult(run,output)));
        }
        if (!rowCount) results.append($('p','这页运行尚无已核验产物。','catalog-empty'));
        const bar=$('div',undefined,'toolbar');results.append(bar);
        button('较新运行产物',async()=>{resultOffset=Math.max(0,resultOffset-20);await loadResults();},bar,resultOffset===0);
        button('较早运行产物',async()=>{resultOffset+=20;await loadResults();},bar,resultOffset+20>=value.total);
    }
    updates.context=fresh=>{for(const run of resultPage)for(const output of run.outputs)if(output.result)addResultTable(run,output,fresh);analysisView?.updatePermissions(fresh.capabilities||{});};
    updates.results=loadResults;await loadResults();
    if(request.view_id){
      if(!context.analysis_connected)throw new Error('当前项目尚未接入 Case 视图读取；链接和已保存配置保持原值。');
      analysisView.setPresentation({view_ref:{id:request.view_id,revision:request.revision},label:'已保存的 Case 视图 · v'+request.revision});
      return;
    }
    if(openingOriginal&&!request.alias)return;
    const holder=$('div');holder.id='case-browser';samplePanel.append(holder);
    let expected=request.kind==='table_request'?request.expected_binding:request.binding_digest;
    if (!alias) { const first=context.tables[0]; if (!first) return; alias=first.alias;expected=first.binding_digest; }
    // Fixed catalog context: aliasBinding is the alias's current preview binding;
    // binding/version describe the version actually read (possibly historical).
    let profileHandle=null,shownFixed=null,offset=request.offset||0,pageRequest=0,versionsLoaded=false,versionsPending=false;
    const p=panel(null,holder);p.classList.add('bound-table');
    const headerHost=$('div'),pinnedBar=$('div',undefined,'version-pinned');pinnedBar.hidden=true;
    const versionDetails=$('details',undefined,'bound-versions'),versionHost=$('div');versionDetails.append($('summary','版本'),versionHost);versionDetails.hidden=true;
    versionDetails.addEventListener('toggle',()=>{if(versionDetails.open)loadVersions();});
    const tools=$('div',undefined,'toolbar'),projection=$('input');projection.placeholder='字段投影（逗号分隔；默认前 16 列）';projection.className='wide';projection.setAttribute('aria-label','字段投影');projection.maxLength=4096;projection.value=(request.columns||[]).join(',');
    const pageSize=select(['20','5','50','100'],'20');pageSize.setAttribute('aria-label','每页行数');tools.append(projection,pageSize);
    const profileHost=$('div'),caseRail=$('div',undefined,'case-record-selector');
    const loadingNote=$('p','正在按固定版本读取…','busy');loadingNote.hidden=true;
    const failureHost=$('div'),rowsSourceNote=$('p',undefined,'muted small rows-source-note');rowsSourceNote.hidden=true;
    const rowsBox=$('div'),pager=$('div',undefined,'toolbar end'),details=$('div',undefined,'case-detail');
    p.append(headerHost,versionDetails,tools,profileHost,caseRail,loadingNote,failureHost,rowsSourceNote,rowsBox,pager);holder.append(details);
    button('读取',async()=>{offset=0;await load();},tools);
    const columns=()=>projection.value.trim()?V.validateColumns(projection.value.split(',').map(v=>v.trim()).filter(Boolean)):null;
    const versionQueryFor=context=>context.version!==null?{version:context.version,version_binding:context.binding.digest}:{};
    const tableView=(extra={})=>target('table',{alias,binding_digest:fixed.binding.digest,...(fixed.version!==null?{version:fixed.version,alias_binding:fixed.aliasBinding}:{}),...extra});
    function rebuildProfile(){
      profileHandle?.dispose();profileHost.replaceChildren();
      profileHandle=registerViewHandle(catalogProfile(profileHost,'table-profile',()=>({alias,expected_binding:fixed.aliasBinding,...versionQueryFor(fixed),columns:columns()||Object.keys(fixed.description.metadata?.fields||{}).slice(0,16)})));
    }
    function paintVersionMarkers(){
      for(const row of versionHost.querySelectorAll('.version-row')){
        const active=fixed.version===Number(row.dataset.version);
        row.classList.toggle('active',active);row.setAttribute('aria-pressed',String(active));
      }
    }
    function paintBoundHeader(){
      disposeReaders(headerHost);headerHost.replaceChildren();
      const binding=fixed.binding,display=fixed.display,description=fixed.description;
      if(!context.analysis_connected)headerHost.append($('h2',display.title||alias));
      raw(headerHost,'当前表的固定来源',{alias,uri:binding.uri,reference:binding.reference,...(fixed.version!==null?{version:fixed.version,alias_binding:fixed.aliasBinding}:{})});
      if(description.reference.provenance?.kind==='historical')headerHost.append($('p','历史结果 · '+(description.reference.provenance.note||'只读固定引用；没有补造新 Flow 的执行记录。'),'notice'));
      if(alias.startsWith('run:'))headerHost.append($('p','实际运行结果 · '+alias,'notice'));
      headerHost.append($('p','样例与分布按'+(fixed.version!==null?'历史固定版本':'绑定版本')+' v'+binding.reference.ref.lance_version+'；查看历史版本不改变目录绑定，也不调用 attach。','muted small'));
      pinnedBar.replaceChildren();pinnedBar.hidden=fixed.version===null;
      if(fixed.version!==null){
        pinnedBar.append($('span','目录表 '+alias+' · 正在查看历史固定 v'+fixed.version));
        button('回到目录当前版本',()=>pinCurrentVersion(),pinnedBar);
      }
      headerHost.append(pinnedBar);
      const fieldsMeta=description.metadata?.fields||{};
      const fieldDetails=$('details',undefined,'bound-fields'),fieldHost=$('div');fieldDetails.append($('summary','字段 · '+Object.keys(fieldsMeta).length),fieldHost);headerHost.append(fieldDetails);
      fieldDetails.addEventListener('toggle',()=>{
        if(!fieldDetails.open||fieldHost.children.length)return;
        const names=(description.columns?.length?description.columns:Object.keys(fieldsMeta)).filter(name=>name!=='_rowid');
        const t=table(fieldHost,['字段','类型','可空','分布'],names.map(name=>{const type=fieldsMeta[name]?.type||{};return {value:name,cells:[name,(type.kind||'未知')+(type.physical?' · '+type.physical:''),type.nullable?'是':'否','']};}));
        for(const tr of t.querySelectorAll('tbody tr')){
          const name=tr.firstChild.textContent,b=$('button','分布');b.type='button';b.className='field-profile-button';b.setAttribute('aria-label','查看字段 '+name+' 的分布');
          b.onclick=()=>profileHandle?.select(name);
          tr.lastChild.append(b);
        }
      });
    }
    function applyFixed(next){
      fixed=next;offset=0;
      clearSelection();disposeReaders(details);details.replaceChildren();caseRail.replaceChildren();main.classList.remove('bound-case-active');
      failureHost.replaceChildren();
      paintBoundHeader();rebuildProfile();paintVersionMarkers();paintDataContext();
    }
    async function selectTableVersion({alias:versionAlias,aliasBinding,version,columns:desiredColumns},ticket,{present=true}={}){
      const stale=()=>ticket.epoch!==viewEpoch||ticket.project!==workspaceId||!p.isConnected;
      try{
        if(stale())throw staleRequest();
        if(versionAlias!==alias)throw new Error('版本选择属于另一个数据表');
        if(!Number.isSafeInteger(version)||version<1)throw new Error('版本必须是正整数');
        const chosen=await api('table-versions',{alias,expected_binding:aliasBinding,version});
        if(stale())throw staleRequest();
        const selected=chosen.selected;
        if(!selected||selected.version!==version)throw new Error('该版本没有可固定的登记事实');
        if(selected.alias_binding!==aliasBinding)throw new Error('目录别名绑定已变化，请重新打开当前表');
        const versionBinding=selected.binding;
        if(!versionBinding||!/^[0-9a-f]{64}$/.test(versionBinding.digest||''))throw new Error('版本绑定身份缺失');
        const described=await api('table',{alias,expected_binding:aliasBinding,version,version_binding:versionBinding.digest});
        if(stale())throw staleRequest();
        if(described.binding?.digest!==versionBinding.digest||described.binding?.alias!==alias)throw new Error('版本读取与登记绑定不一致');
        if(described.metadata?.version!==version||selected.metadata?.version!==version)throw new Error('版本登记事实与实际读取不一致');
        if(selected.metadata&&described.metadata&&(selected.metadata.schema_hash!==described.metadata.schema_hash||selected.metadata.row_count!==described.metadata.row_count))throw new Error('版本登记事实与实际读取不一致');
        const projectionColumns=(desiredColumns?.length?desiredColumns:(described.columns||[]).filter(name=>name!=='_rowid').slice(0,16));
        applyFixed({alias,aliasBinding,binding:versionBinding,version,description:described,display:described.reference.display||{}});
        projection.value=projectionColumns.join(',');
        await load(false);
        return {project:workspaceId,locator:{kind:'workspace_table',alias,binding:versionBinding.digest,columns:projectionColumns,version,alias_binding:aliasBinding},
          label:alias+' · v'+version,title:fixed.display.title||alias,version,rows:described.metadata?.row_count??null,fields:Object.keys(described.metadata?.fields||{}).length||null};
      }catch(err){
        if(err?.name==='AbortError'||stale())return null;
        if(present&&!err.presented&&p.isConnected){failureHost.replaceChildren();renderLoadFailure(failureHost,err,{scope:{project:workspaceId,operation:'table-versions',label:'历史版本'},isCurrent:()=>!stale()});}
        return null;
      }
    }
    async function pinCurrentVersion(){
      const ticket={epoch:viewEpoch,project:workspaceId};
      try{
        const described=await api('table',{alias,expected_binding:fixed.aliasBinding});
        if(ticket.epoch!==viewEpoch||ticket.project!==workspaceId||!p.isConnected)throw staleRequest();
        applyFixed({alias,aliasBinding:described.binding.digest,binding:described.binding,version:null,description:described,display:described.reference.display||{}});
        projection.value=(request.columns||described.columns||[]).filter(name=>name!=='_rowid').slice(0,16).join(',');
        const t={alias,uri:described.binding.uri,reference:described.binding.reference,binding_digest:described.binding.digest};
        const at=context.tables.findIndex(item=>item.alias===alias);
        if(at<0)context.tables.push(t);else context.tables[at]={...context.tables[at],...t};
        await load(false);
      }catch(err){
        if(err?.name==='AbortError'||ticket.epoch!==viewEpoch)return;
        if(!err.presented&&p.isConnected)renderLoadFailure(pinnedBar.isConnected?pinnedBar:p,err,{scope:{project:workspaceId,operation:'table',label:'目录当前版本'},isCurrent:()=>ticket.epoch===viewEpoch&&p.isConnected});
      }
    }
    async function loadVersions(){
      if(!fixed||versionsLoaded||versionsPending)return;
      const epoch=viewEpoch,aliasBinding=fixed.aliasBinding;versionsPending=true;versionHost.replaceChildren($('p','正在读取登记版本…','busy'));
      try{
        const value=await api('table-versions',{alias,expected_binding:aliasBinding});
        if(epoch!==viewEpoch||!versionHost.isConnected)throw staleRequest();
        if(value.binding?.digest!==aliasBinding||fixed.aliasBinding!==aliasBinding)throw new Error('版本来源与目录别名绑定不一致');
        versionHost.replaceChildren();
        const aliasVersion=value.binding?.reference?.ref?.lance_version;
        if(value.reason==='run_output_sealed_version')versionHost.append($('p','运行产物固定于单一 sealed 版本，没有登记版本历史。','muted small'));
        else if(!value.versions.length)versionHost.append($('p','此表尚未在数据目录登记版本；仅绑定版本可预览。','muted small'));
        if(value.versions.length){
          const rowsHost=$('div',undefined,'catalog-rows');versionHost.append(rowsHost);
          for(const item of value.versions){
            const row=$('button',undefined,'catalog-row version-row');row.type='button';row.dataset.version=String(item.lance_version);
            row.title='固定查看 v'+item.lance_version+' 的样例与分布；不改变目录绑定';
            row.append($('span','v'+item.lance_version+(item.lance_version===aliasVersion?' · 目录当前绑定':''),'catalog-row-name'),
              $('span',String(item.row_count)+' 行 · '+(item.content_digest||'schema '+item.schema_name+'@'+item.schema_version),'catalog-row-meta'));
            row.onclick=async()=>{row.disabled=true;try{await selectTableVersion({alias,aliasBinding,version:item.lance_version},{epoch:viewEpoch,project:workspaceId});}finally{row.disabled=false;}};
            rowsHost.append(row);
          }
          versionHost.append($('p','点击版本固定查看该版本的样例与分布；不会改变目录绑定。'+(value.truncated?'仅显示前 128 个登记版本（共 '+value.total_registered+' 个）。':''),'muted small'));
        }
        paintVersionMarkers();versionsLoaded=true;
      }catch(err){if(err?.name!=='AbortError'&&epoch===viewEpoch&&versionHost.isConnected){versionHost.replaceChildren();renderLoadFailure(versionHost,err,{retryRead:loadVersions,scope:{project:workspaceId,operation:'table-versions',label:'登记版本'},isCurrent:()=>epoch===viewEpoch&&versionHost.isConnected});}}
      finally{versionsPending=false;}
    }
    async function initFixed(){
      if(request.kind==='table_request'&&Object.hasOwn(request,'version')){
        const selection=await selectTableVersion({alias,aliasBinding:request.alias_binding,version:request.version,columns:request.columns},{epoch:viewEpoch,project:workspaceId},{present:false});
        if(!selection)throw new Error('历史版本链接未能固定；请返回目录重新选择版本。');
        onBinding(tableView({columns:selection.locator.columns,offset:0}));
        return true;
      }
      const historical=Object.hasOwn(request,'version');
      const description=await api('table',{alias,...(historical?{expected_binding:request.alias_binding,version:request.version,version_binding:request.binding_digest}:expected?{expected_binding:expected}:{})});
      if(historical&&(description.metadata?.version!==request.version||description.binding?.digest!==request.binding_digest))throw new Error('历史版本读取与链接身份不一致');
      fixed={alias,aliasBinding:historical?request.alias_binding:description.binding.digest,binding:description.binding,version:historical?request.version:null,description,display:description.reference.display||{}};
      if(request.kind==='table_request'){
        request=target('table',{alias,binding_digest:description.binding.digest,...(request.columns?{columns:request.columns}:{}),...(request.offset!==undefined?{offset:request.offset}:{})});
        onBinding(request);
      }
      return false;
    }
    async function showCase(rowId,shownColumns,route=false) {
      const seq=++selectionEpoch,caseFixed=fixed;clearSelection();viewState='loading';
      disposeReaders(details);details.replaceChildren($('p','正在读取 Case…','busy'));
      try {
        const detail=await api('sample',{alias,expected_binding:caseFixed.aliasBinding,...versionQueryFor(caseFixed),row_id:String(rowId),columns:JSON.stringify(shownColumns)});
        if(seq!==selectionEpoch||!details.isConnected)throw staleRequest();
        if(detail.binding.digest!==caseFixed.binding.digest||detail.rows.length!==1||String(detail.rows[0]._rowid)!==String(rowId))throw new Error('指定 Case 已不可读取');
        const row=detail.rows[0],reference=target('case',{alias,binding_digest:caseFixed.binding.digest,row_id:String(rowId),columns:detail.columns,...(caseFixed.version!==null?{version:caseFixed.version,alias_binding:caseFixed.aliasBinding}:{})});
        disposeReaders(details);details.replaceChildren();main.classList.add('bound-case-active');main.classList.remove('original-case-active');
        for(const b of caseRail.children)b.setAttribute('aria-pressed',String(b.dataset.rowId===String(rowId)));
        const left=panel('单条 case · '+rowId,details);left.classList.add('case-reading');left.querySelector('h2').replaceWith($('h1','单条 case · '+rowId));
        left.prepend($('p',context.name+' → '+alias+' → Case '+rowId,'muted small'));
        button('返回当前表',async()=>{disposeReaders(details);details.replaceChildren();main.classList.remove('bound-case-active');rowsBox.hidden=false;pager.hidden=false;view=tableView({columns:shownColumns,offset});choose(view);putHistory(view,'replace');projectHeading();rowsBox.scrollIntoView({block:'start'});rowsBox.querySelector('tbody tr')?.focus();},left);
        left.append($('p','表别名：'+alias+' · 固定版本 v'+caseFixed.binding.reference.ref.lance_version+(caseFixed.version!==null?'（历史固定）':'')+' · 来源：'+caseFixed.binding.uri,'notice'));
        const right=$('details',undefined,'panel case-comparison');right.append($('summary','按明确业务键对照'));details.append(right);
        rowsBox.classList.add('case-record-list');rowsBox.setAttribute('aria-label','当前固定版本记录');
        renderCase(left,reference,row,caseFixed.display);if(!pendingView)viewState='ready';choose(reference,route);projectHeading();
        right.append($('p','选择业务键匹配另一张表；不会按行号假定一一对应。','muted small'));
        const options=context.tables.map(x=>x.alias),other=select(options,options.find(x=>x!==alias)),key=select([{value:'',label:'选择业务键…'},...detail.columns]);
        const targetKey=$('input');targetKey.placeholder='对侧键（默认同名）';targetKey.setAttribute('aria-label','对侧业务键');key.setAttribute('aria-label','业务键');other.setAttribute('aria-label','对照表');
        const bar=$('div',undefined,'toolbar');bar.append(other,key,targetKey);right.append(bar);const output=$('div');right.append(output);right.addEventListener('toggle',()=>{if(!right.open){disposeReaders(output);output.replaceChildren();}});
        button('查找对应 case',async()=>{
          if(!key.value)throw new Error('请先明确选择业务键');
          const selected=context.tables.find(t=>t.alias===other.value);disposeReaders(output);output.replaceChildren($('p','读取对应记录…','busy'));
          const described=await api('table',{alias:other.value,...(selected?.binding_digest?{expected_binding:selected.binding_digest}:{})});
          const result=await api('sample',{alias:described.binding.alias,expected_binding:described.binding.digest,match:JSON.stringify({field:targetKey.value||key.value,value:row[key.value]}),limit:20});
          if(seq!==selectionEpoch||!output.isConnected)throw staleRequest();
          output.replaceChildren();output.append($('p',result.rows.length===0?'未找到匹配记录':result.rows.length===1?'匹配 1 条记录':`匹配 ${result.rows.length}${result.has_more?' 条或更多':' 条'}记录；业务键不唯一`,'notice'));
          for(const match of result.rows){
            const item=$('details'),host=$('div');item.append($('summary','Case '+match._rowid+' · '+described.binding.alias),host);output.append(item);
            item.addEventListener('toggle',()=>{disposeReaders(host);host.replaceChildren();if(item.open){for(const other of output.querySelectorAll(':scope > details'))if(other!==item){other.open=false;disposeReaders(other);}
              renderCase(host,target('case',{alias:described.binding.alias,binding_digest:result.binding.digest,row_id:String(match._rowid),columns:result.columns}),match,described.reference.display||{});}});
          }
        },bar);
      } catch(err) {if(seq===selectionEpoch){viewState='error';clearSelection();details.replaceChildren();if(err?.name!=='AbortError'&&!err.presented)renderLoadFailure(details,err,{retryRead:()=>showCase(rowId,shownColumns,route),scope:{project:workspaceId,operation:'sample',label:'Case '+rowId},isCurrent:()=>seq===selectionEpoch&&details.isConnected});}throw err;}
    }
    async function load(initial=false) {
      const page=++pageRequest,seq=++selectionEpoch,readFixed=fixed;
      clearSelection();viewState='loading';disposeReaders(details);
      rowsBox.hidden=false;pager.hidden=false;details.replaceChildren();
      failureHost.replaceChildren();loadingNote.hidden=false;
      try {
        const result=await api('sample',{alias,expected_binding:readFixed.aliasBinding,...versionQueryFor(readFixed),offset,limit:pageSize.value,...(columns()?{columns:JSON.stringify(columns())}:{})});
        if(page!==pageRequest||seq!==selectionEpoch||!rowsBox.isConnected)throw staleRequest();
        if(result.binding?.digest!==readFixed.binding.digest)throw new Error('样例来源与当前固定版本不一致');
        shownFixed=readFixed;samplesLoaded=true;
        rowsSourceNote.hidden=false;
        rowsSourceNote.textContent='当前样例：'+alias+' · v'+readFixed.binding.reference.ref.lance_version+(readFixed.version!==null?'（历史固定）':'（目录绑定）')+' · '+readFixed.binding.digest.slice(0,12);
        if(result.columns.length)selectDataSource({label:alias+' · v'+readFixed.binding.reference.ref.lance_version,title:readFixed.display.title||alias,version:readFixed.binding.reference.ref.lance_version,rows:readFixed.binding.reference.ref.row_count,fields:Object.keys(readFixed.description.metadata?.fields||{}).length||result.columns.filter(name=>name!=='_rowid').length,
          locator:{kind:'workspace_table',alias,binding:readFixed.binding.digest,columns:result.columns.filter(name=>name!=='_rowid'),...(readFixed.version!==null?{version:readFixed.version,alias_binding:readFixed.aliasBinding}:{})}});
        rowsBox.replaceChildren();pager.replaceChildren();caseRail.replaceChildren();
        for(const row of result.rows){const label=result.columns.filter(k=>k!=='_rowid').slice(0,2).map(k=>typeof row[k]==='string'?row[k].slice(0,55):String(row[k]).slice(0,25)).join(' · ');const b=button('Case '+row._rowid+' · '+label,()=>showCase(row._rowid,result.columns,true),caseRail);b.dataset.rowId=String(row._rowid);b.setAttribute('aria-pressed','false');}
        table(rowsBox,['_rowid',...result.columns],result.rows.map(row=>({value:row,cells:['_rowid',...result.columns].map(k=>{const v=row[k],text=typeof v==='string'?v:JSON.stringify(v);return text&&text.length>140?text.slice(0,140)+'…':text;})})),async(row,tr)=>{
          if(pendingView){await openView(target('case',{alias,binding_digest:readFixed.binding.digest,row_id:String(row._rowid),columns:result.columns,...(readFixed.version!==null?{version:readFixed.version,alias_binding:readFixed.aliasBinding}:{})}));return;}
          for(const r of rowsBox.querySelectorAll('tr'))r.classList.remove('selected');tr.classList.add('selected');await showCase(row._rowid,result.columns,true);
        });
        if(!result.rows.length)rowsBox.append($('p','当前页没有记录。','muted'));
        pager.append($('span',`${result.rows.length?offset+1:0}–${offset+result.rows.length} / ${result.row_count} 行 · 展示文本可缩略，详情读取不静默截断`,'muted small'));
        const controls=$('div',undefined,'toolbar');pager.append(controls);
        button('上一页',async()=>{offset=Math.max(0,offset-Number(pageSize.value));await load();},controls,offset===0);
        button('下一页',async()=>{offset+=Number(pageSize.value);await load();},controls,!result.has_more);
        if(!initial){view=tableView({columns:result.columns,offset});putHistory(view,'replace');}
        const rowId=initial&&request.kind==='case'?request.row_id:result.rows[0]?._rowid;
        if(rowId!==undefined&&(!context.analysis_connected||request.kind==='case'))await showCase(rowId,result.columns);
        else {if(!pendingView)viewState='ready';choose(tableView({columns:result.columns,offset}));}
      }catch(err){
        if(err?.name!=='AbortError'&&page===pageRequest&&rowsBox.isConnected){
          viewState='error';clearSelection();
          if(!err.presented)renderLoadFailure(failureHost,err,{retryRead:()=>load(),scope:{project:workspaceId,operation:'sample',label:shownFixed?'样例（保留 v'+shownFixed.binding.reference.ref.lance_version+' 旧页）':'样例'},isCurrent:()=>page===pageRequest&&rowsBox.isConnected});
        }
        throw err;
      }finally{if(page===pageRequest)loadingNote.hidden=true;}
    }
    // The fixed schema is sufficient to register a Case/Notebook source.
    // Keep the separate catalog sample reader behind its explicit disclosure.
    let samplesLoaded=false,samplePending=null;
    readSamples=()=>{
      if(samplesLoaded)return Promise.resolve();
      if(samplePending)return samplePending;
      samplePending=load(true).then(()=>{samplesLoaded=true;}).finally(()=>{samplePending=null;});
      return samplePending;
    };
    const resolved=await initFixed();
    if(!resolved){paintBoundHeader();rebuildProfile();}
    versionDetails.hidden=false;if(versionDetails.open)loadVersions();
    if(fixed.version!==null){samplesRequested=true;samplePanel.hidden=false;samplesToggle.setAttribute('aria-expanded','true');}
    if(fixed.version===null){
      const t={alias,uri:fixed.binding.uri,reference:fixed.binding.reference,binding_digest:fixed.binding.digest};
      const at=context.tables.findIndex(item=>item.alias===alias);
      if(at<0)context.tables.push(t);else context.tables[at]={...context.tables[at],...t};
    }
    updates.tables=async()=>{
      listTables();refreshOriginal();if(main.classList.contains('original-table-active'))return;
      if(fixed.version!==null){
        try{
          const latest=await api('table',{alias,expected_binding:fixed.aliasBinding,version:fixed.version,version_binding:fixed.binding.digest});
          if(latest.binding?.digest!==fixed.binding.digest)throw new Error('固定版本来源已变化');
        }catch(err){if(err?.name!=='AbortError'&&!err.presented&&p.isConnected&&fixed.version!==null){failureHost.replaceChildren();renderLoadFailure(failureHost,err,{scope:{project:workspaceId,operation:'table',label:'历史固定来源'},isCurrent:()=>p.isConnected&&fixed.version!==null});}}
        return;
      }
      const latest=await api('table',{alias,expected_binding:fixed.aliasBinding});
      if(latest.binding.digest!==fixed.binding.digest)await openView(view||request,{historyMode:'replace'});
    };
    listTables();
    const declared=[...new Set((fixed.display.fields||[]).map(field=>field.path[0]))];
    const projected=request.columns||(declared.length?declared:fixed.description.columns?.slice(0,16));
    // Older servers lack ordered columns: retain their native sample path
    // instead of deriving a new source identity from sorted object keys.
    if(resolved){/* historical table_request already loaded and registered */}
    else if(context.analysis_connected&&request.kind!=='case'&&!samplesRequested&&projected){
      const selected=V.validateColumns(projected.filter(name=>name!=='_rowid'));
      selectDataSource({label:alias+' · v'+fixed.binding.reference.ref.lance_version,title:fixed.display.title||alias,version:fixed.binding.reference.ref.lance_version,rows:fixed.binding.reference.ref.row_count,fields:Object.keys(fixed.description.metadata?.fields||{}).length||selected.length,
        locator:{kind:'workspace_table',alias,binding:fixed.binding.digest,columns:selected,...(fixed.version!==null?{version:fixed.version,alias_binding:fixed.aliasBinding}:{})}});
    }else await readSamples();
  }
  function renderCase(parent,reference,row,display={}) {
    if(!row){parent.append($('p','记录不存在或已不可读取'));return;}
    const historical=!!reference&&Object.hasOwn(reference,'version');
    const caseVersionQuery=historical?{version:reference.version,version_binding:reference.binding_digest}:{};
    const caseExpectedBinding=historical?reference.alias_binding:reference?.binding_digest;
    const body=$('div',undefined,'case-fields');parent.append(body);
    const entries=[],fieldModes=new Map(),structureOpen=new Map();let visits=0,fieldGeneration=0,activeIndex=-1,activeReader=null;
    function walk(value,parts,path,spec){
      if(++visits>2048||entries.length>=64)throw new Error('展示字段展开超过上限，请缩小展示声明或读取范围。');
      if(!parts.length){entries.push({key:spec.label||path.join('.'),value,path,spec});return;}
      const [part,...rest]=parts;
      if(part==='*'){if(Array.isArray(value))value.forEach((item,index)=>walk(item,rest,[...path,index],spec));}
      else if(value!==null&&typeof value==='object'&&Object.hasOwn(value,part))walk(value[part],rest,[...path,part],spec);
    }
    if(display.fields?.length)for(const spec of display.fields)walk(row,spec.path,[],spec);
    else for(const [key,value] of Object.entries(row))entries.push({key,value,path:[key],spec:null});
    const isPath=value=>typeof value==='string'&&!/\s/.test(value)&&(/^(?:\/|file:|\.\.?\/)/.test(value)||/\.(?:py|json|ya?ml|toml|log|txt|png|jpe?g|webp|gif)$/i.test(value));
    function structuredString(value){
      if(typeof value!=='string'||value.length>65536||!/^\s*[\[{]/.test(value))return false;
      try{const parsed=JSON.parse(value);return parsed!==null&&typeof parsed==='object';}catch{return false;}
    }
    const readable=entry=>typeof entry.value==='string'&&(entry.spec?.kind==='text'||!entry.spec&&!isPath(entry.value)&&!structuredString(entry.value)&&(entry.value.length>=240||/[\r\n]/.test(entry.value)));
    const identity=$('dl',undefined,'case-identity details');
    for(const entry of entries.filter(e=>!e.spec&&e.path[0]!=='_rowid'&&e.value!==null&&typeof e.value!=='object'&&!readable(e))){
      const text=String(entry.value);if(text.length>160)continue;
      identity.append($('dt',entry.key),$('dd',text));
    }
    if(identity.children.length)body.append(identity);
    const tabs=$('div',undefined,'field-tabs');tabs.setAttribute('role','tablist');tabs.setAttribute('aria-label','Case 字段');
    const host=$('div',undefined,'case-field');host.setAttribute('role','tabpanel');host.id='field-panel-'+(++renderCase.serial);body.append(tabs,host);
    function activate(index,focus=false){
      if(index===activeIndex)return;
      if(activeReader)fieldModes.set(activeIndex,activeReader.element.dataset.mode);
      if(activeIndex>=0)structureOpen.set(activeIndex,!!host.querySelector('details')?.open);
      activeIndex=index;activeReader=null;
      const generation=++fieldGeneration;const entry=entries[index];disposeReaders(host);host.replaceChildren($('h3',entry.key));
      for(const [i,tab]of [...tabs.children].entries()){tab.setAttribute('aria-selected',String(i===index));tab.tabIndex=i===index?0:-1;if(i===index){host.setAttribute('aria-labelledby',tab.id);if(focus)tab.focus();}}
      const {key,value,path,spec}=entry;
      if(readable(entry))activeReader=mountText(host,value,{defaultMode:fieldModes.get(index)||'preview'});
      else if(value&&typeof value==='object'){const d=raw(host,'展开结构化内容',value);d.open=structureOpen.get(index)||false;}
      else if(typeof value==='string'&&(value.length>=240||/[\r\n]/.test(value)))mountText(host,value,{sourceOnly:true});
      else host.append($('pre',value===null?'null':String(value),'scalar-value'));
      if(reference&&spec?.kind==='evidence'&&value!==null)button('查看历史调用证据',async()=>{const detail=await api('table-evidence',{alias:reference.alias,expected_binding:caseExpectedBinding,row_id:reference.row_id,field_path:JSON.stringify(path),...caseVersionQuery});if(host.isConnected&&generation===fieldGeneration)raw(host,'历史调用记录 · 不代表本次 Flow 已验证',detail.document);},host);
      const runId=reference?.alias.startsWith('run:')?reference.alias.split(':')[1]:null;
      if(runId&&value&&typeof value==='object'){
        let visited=0,found=0;const references=new Set();const walk=(v,depth=0)=>{if(!v||typeof v!=='object'||depth>8||++visited>256||found>=16)return;
          if(typeof v.journal_path==='string'&&typeof v.request_id==='string'&&['request','response','error'].includes(v.kind)){const key=json([v.journal_path,v.request_id,v.kind,v.attempt||1]);if(references.has(key))return;references.add(key);found++;button('查看调用证据 · '+v.kind,async()=>{const detail=await api('run-evidence',{run_id:runId,reference:JSON.stringify(v)});if(host.isConnected&&generation===fieldGeneration)raw(host,'原生调用证据 · '+v.kind,detail.document);},host);return;}
          for(const child of Object.values(v))walk(child,depth+1);};walk(value);
      }
      if(reference&&(spec?.kind==='image'&&value!==null||!spec&&typeof value==='string'&&/\.(png|jpe?g|webp|gif)$/i.test(value)))button('查看本地图片',async()=>{
        const epoch=selectionEpoch,fieldIndex=index;
        const blob=await api('media',{alias:reference.alias,expected_binding:caseExpectedBinding,row_id:reference.row_id,...(spec?{field_path:JSON.stringify(path)}:{field:key}),...caseVersionQuery});
        if(!host.isConnected||epoch!==selectionEpoch||tabs.children[fieldIndex]?.getAttribute('aria-selected')!=='true')throw staleRequest();
        if(mediaBytes+blob.size>32*1024*1024||blobs.size>=64)throw new Error('图片缓存预算已满，请重新打开数据表页释放');
        mediaBytes+=blob.size;const url=URL.createObjectURL(blob);blobs.add(url);blobSizes.set(url,blob.size);const img=$('img');img.src=url;img.alt=key;img.className='case-image';host.append(img,$('p',spec?'图片读取已核对固定行中的 SHA256。':'图片来自该表位置内的文件；表版本本身不固定外部图片内容。','muted small'));
      },host);
    }
    entries.forEach((entry,index)=>{const tab=$('button',entry.key);tab.type='button';tab.setAttribute('role','tab');tab.id=host.id+'-'+index;tab.setAttribute('aria-controls',host.id);tab.onclick=()=>activate(index);tab.onkeydown=event=>{let next;if(event.key==='ArrowRight')next=(index+1)%entries.length;if(event.key==='ArrowLeft')next=(index+entries.length-1)%entries.length;if(event.key==='Home')next=0;if(event.key==='End')next=entries.length-1;if(next!==undefined){event.preventDefault();activate(next,true);}};tabs.append(tab);});
    const first=entries.findIndex(readable);if(entries.length)activate(first>=0?first:Math.max(0,entries.findIndex(e=>e.path[0]!=='_rowid')));
    const identityInfo={row_id:String(row._rowid),...(reference?{alias:reference.alias,binding_digest:reference.binding_digest}:{}),...(historical?{version:reference.version,alias_binding:reference.alias_binding}:{}),note:'row_id 是当前固定版本内的行定位，不是跨表业务键。'};
    raw(body,'行定位与来源明细',identityInfo);raw(body,'原始记录',row);if(reference)contextActions(body,reference);
  }
  renderCase.serial=0;
  function addResultTable(run,output,destination=context){
    const alias='run:'+run.run_id+':'+output.name;
    if(!output.result)return alias;
    const t={alias,run_id:run.run_id,output_name:output.name,uri:output.uri,reference:{location_id:output.result.location_id,ref:output.result.ref}};
    const at=destination.tables.findIndex(t=>t.alias===alias);if(at<0)destination.tables.push(t);else destination.tables[at]=t;return alias;
  }
  async function openResult(run,output){const alias=addResultTable(run,output);await openTable(alias);}
  async function pipeline(request){
    if(workbenchContract!=='demiflow_workbench_graph_v1')throw new Error('当前服务尚未提供图工作台契约');
    const id=workspaceId,epoch=viewEpoch;let reference=request,mountedHandle=null,checkVersion=0,checksPending=0;
    const isCurrent=()=>epoch===viewEpoch&&workspaceId===id;
    const handle=await ForgeWorkbench.mount({main,api,context,workspaceId:id,view:request,mountText,disposeReaders,openResult,navigate,
      register:handle=>{mountedHandle=handle;if(isCurrent())workbenchView=handle;else handle.dispose();},
      projectCheckState:()=>({version:checkVersion,active:checksPending>0}),
      continueInput,
      continueWithResult:async(run,output)=>{continueInput=addResultTable(run,output);await openView(target('flows'));},
      onContext:fresh=>{if(epoch===viewEpoch&&workspaceId===id&&fresh.workspace_id===id){context=fresh;projectHeading();}},
      onPageChange:page=>{if(isCurrent())markFunction(page);},
      rememberView:fields=>{if(epoch!==viewEpoch||workspaceId!==id)return;reference=V.validateView({v:2,workspace:id,...fields});
        if(viewState==='ready'){view=reference;selection=reference;selectionEpoch++;putHistory(reference,'replace');projectHeading();}},
      projectSettings:{get:()=>({manage:projectManageEnabled,project:projectList.find(p=>p.workspace_id===id),digest:projectDigest,
        service:serviceRunPermissions,workbench:serviceWorkbenchPermissions}),refresh:refreshProjects},
      renderProjectInfo:async parent=>{await projectChecks(parent,{project:id,isCurrent,
        onBegin:()=>{checkVersion++;checksPending++;},
        onEnd:()=>{checkVersion++;checksPending--;if(isCurrent())mountedHandle?.projectCheckSettled?.();}});
        if(parent.isConnected&&isCurrent()){originalProject(parent);sourceInventory(parent);originalRuns(parent);}},
    });
    if(epoch!==viewEpoch){handle.dispose();throw staleRequest();}
    workbenchView=handle;return {handle,reference};
  }
  function renderOffline(report,parent){
    const box=panel(report.entry||'固定检查报告',parent);
    box.append(badge('not_live'),$('p','固定快照 · 当前源码、数据和运行状态尚未复核。','muted small'));
    for(const d of report.diagnostics||[])box.append($('p',(d.rule||d.code)+': '+d.detail,'error-box'));
    const host=$('div',undefined,'wb-graph'),detail=panel('节点契约',box);box.insertBefore(host,detail);
    const visible=(report.nodes||[]).slice(0,128),ids=new Set(visible.map(n=>n.id));
    const edges=[
      ...(report.edges||[]).map(e=>({...e,type:'data'})),...(report.value_edges||[]).map(e=>({...e,type:'value'})),...(report.action_edges||[]).map(e=>({...e,type:'action'}))];
    const graph={binding:{basis_kind:'report',basis_digest:report.digest},nodes:visible,edges:edges.filter(e=>ids.has(e.from)&&ids.has(e.to)).slice(0,256)};
    if(visible.length<(report.nodes||[]).length||graph.edges.length<edges.length)box.insertBefore($('p','显示已裁剪：'+visible.length+' / '+(report.nodes||[]).length+' 个节点。离线文件保留完整报告；显示范围不改变原始检查结论。','notice'),host);
    const renderer=ForgeWorkbenchGraph.mount(host,{onSelect:selection=>{const node=graph.nodes.find(n=>n.id===selection.reportNodeId);detail.replaceChildren($('h2',node.api),$('p',node.source+':'+node.line,'muted small'));raw(detail,'实际调用参数',node.parameters);raw(detail,'输出字段',node.fields);}});
    const presentation=ForgeWorkbenchPresentation;
    renderer.update(presentation.buildGraphModel(graph,{choice:presentation.chooseGraphView(graph)}));raw(box,'原始覆盖与绑定',{coverage:report.coverage,binding:report.binding});
  }
  function projectHeading() {
    const name=document.getElementById('workspace-name');name.textContent=context?.name||projectList.find(p=>p.workspace_id===workspaceId)?.name||'选择项目';name.title=name.textContent;
    const mode=document.getElementById('workspace-mode');
    mode.textContent=context?(context.runs_enabled?'可查看、检查与运行':canRun('run_control')?'可查看、检查与停止已有运行':'可查看、检查 · 运行未开启'):'';
    mode.hidden=!context;
    const path=document.getElementById('workspace-path');
    if(path){const current=pendingView||view;path.textContent=({development:'开发',overview:'Readme',tables:'数据',flows:'管道',operators:'算子',monitor:'运行监控'})[pageKind()]+(current?.alias?' / '+current.alias:'')+(current?.row_id?' / Case '+current.row_id:'');}
    const settings=document.getElementById('project-settings');if(settings)settings.disabled=!workspaceId;
    const status=document.getElementById('workspace-status');if(status){status.hidden=!workspaceId;if(!workspaceId)status.open=false;}
  }
  function markProject() {
    if(!projectNav)return;
    nav.hidden=!workspaceId;
    for(const item of projectNav.querySelectorAll('[data-workspace]')){
      const current=item.dataset.workspace===workspaceId;
      item.classList.toggle('active',current);
      if(current)item.setAttribute('aria-current','true');else item.removeAttribute('aria-current');
    }
  }
  function settingsDialog() {
    const project=projectList.find(p=>p.workspace_id===workspaceId);if(!project)return;
    const dialog=$('dialog');dialog.append($('h2','项目设置'),$('p',project.name),$('p',project.root,'muted small'));
    if(projectRemovalEnabled)button('移除项目 '+project.name,()=>{dialog.close();removeProjectDialog(project);},dialog);
    else dialog.append($('p','此服务未授予项目管理权限。','notice'));
    button('关闭',()=>dialog.close(),dialog);document.body.append(dialog);dialog.addEventListener('close',()=>dialog.remove(),{once:true});dialog.showModal();
  }
  function clearProject(message='在原项目目录中阅读说明、查看数据与 Case、核对流程和检查结果。') {
    if(!canLeaveViews())return;
    readController.abort();readController=new AbortController();
    viewEpoch++;selectionEpoch++;auditRequest++;revokeImages();disposeReaders();closeNavigation();
    workspaceId='';context=null;view=null;pendingView=null;initialView=null;selection=null;
    viewState='ready';changes=null;lastObserved=null;updates={};
    continueInput='';auditState=null;auditError=null;pendingOriginalPath=null;
    for(const b of nav.querySelectorAll('[data-page]')){b.disabled=true;b.classList.remove('active');b.removeAttribute('aria-current');}
    markProject();projectHeading();renderAudit();main.replaceChildren();main.classList.remove('bound-case-active','original-case-active','original-table-active','workbench-active','data-active','data-notebook-active','data-case-config-active','data-catalog-expanded','data-catalog-collapsed');
    heading('开始使用 DemiFlow',message);
    const welcome=panel(null);welcome.classList.add('next-step');welcome.dataset.onboarding='empty';
    welcome.append($('h2','让原项目更容易阅读与核对'),$('p','DemiFlow 将说明、数据、流程与检查结果放在一起。先选择一个项目目录，再从已有内容开始。'));
    if(projectManageEnabled){const actions=$('div',undefined,'toolbar');welcome.append(actions);button('注册已有项目',()=>projectDialog(false),actions).className='primary';button('新建项目',()=>projectDialog(true),actions);
      welcome.append($('p','选择服务器绝对目录 → 阅读项目内容 → 核对规范检查。新建只接受空目录或不存在的目录；注册仅补必要管理信息。','muted small'));}
    else welcome.append($('p','此服务未授予项目管理权限。请从已授权的单项目服务进入，或由服务管理员登记项目。','notice'));
    history.replaceState(null,'',location.pathname);syncStatus(projectList.length?'请选择项目':'尚未登记项目');
  }
  async function refreshProjects(){
    const inventory=await api('projects'),rows=inventory.projects;projectDigest=inventory.digest;
    if(json(rows)!==json(projectList)||!projectNav.childElementCount){
      projectList=rows;
      const existing=new Map([...projectNav.querySelectorAll('[data-workspace]')].map(item=>[item.dataset.workspace,item]));
      projectNav.querySelector('.project-empty')?.remove();
      for(const [index,p] of rows.entries()){
        let item=existing.get(p.workspace_id);
        if(!item){
          item=button('',()=>p.workspace_id===workspaceId?closeNavigation():switchProject(p.workspace_id));
          item.className='project-item';item.dataset.workspace=p.workspace_id;
          const mark=icon(['M3 7V5h6l2 2h10v13H3z']);mark.classList.add('project-mark');
          const avatar=$('span',undefined,'project-avatar');avatar.setAttribute('aria-hidden','true');
          item.append(mark,avatar,$('span',undefined,'project-name'),$('span','暂不可用','project-unavailable'));
        }
        item.querySelector('.project-avatar').textContent=Array.from(p.name.split('/').filter(Boolean).at(-1)||p.name).slice(0,2).join('').toUpperCase();
        item.querySelector('.project-name').textContent=p.name;
        item.querySelector('.project-unavailable').hidden=p.status==='available';
        item.dataset.unavailable=String(p.status!=='available');item.setAttribute('aria-label',p.name+(p.status==='available'?'':' · 暂不可用'));
        item.title=p.name+'\n'+p.root;
        if(projectNav.children[index]!==item)projectNav.insertBefore(item,projectNav.children[index]||null);
        existing.delete(p.workspace_id);
      }
      for(const item of existing.values())item.remove();
      if(!rows.length)projectNav.append($('p','尚未登记项目','project-empty'));
      markProject();
    }
    const manage=document.getElementById('project-actions');if(manage)manage.hidden=!rows.length;
    for(const id of projectTabs.keys())if(!rows.some(p=>p.workspace_id===id))projectTabs.delete(id);
    if(workspaceId&&!rows.some(p=>p.workspace_id===workspaceId))clearProject('项目已移出 DemiFlow 登记清单，项目文件和历史数据均已保留。可以重新注册，或选择其他项目。');
  }
  async function switchProject(id, requested=null, historyMode='push') {
    if(!canLeaveViews())return;
    if(activeWrites)throw new Error('正在提交操作，完成后再切换项目。');
    if(!projectList.some(p=>p.workspace_id===id))throw new Error('此工作区尚未登记');
    rememberTab();
    clearTimeout(syncTimer);readController.abort();readController=new AbortController();
    document.getElementById('workspace-status').open=false;
    const epoch=++viewEpoch;selectionEpoch++;revokeImages();disposeReaders();closeNavigation();
    workspaceId=id;context=null;view=null;pendingView=null;selection=null;viewState='loading';
    changes=null;lastObserved=null;updates={};continueInput='';forcePending=false;
    auditState=null;auditError=null;renderAudit();

    markProject();projectHeading();main.replaceChildren($('p','正在读取项目…','busy view-loading'));
    syncStatus('正在连接工作区…');
    for(const b of nav.querySelectorAll('[data-page]')){b.disabled=true;b.classList.remove('active');b.removeAttribute('aria-current');}
    try{
      const fresh=await api('context');
      if(epoch!==viewEpoch)throw staleRequest();
      if(fresh.workspace_id!==id)throw new Error('工作区身份不一致');
      context=fresh;
      projectHeading();
      for(const b of nav.querySelectorAll('[data-page]'))b.disabled=false;
      const initialPage=context.flows?.length||context.project?.entries?.length?'flows':'overview';
      await openView(requested||tabTarget(projectTabs.get(id)?.page||initialPage),{historyMode});
      changes=await api('changes');
      lastObserved=new Date();
      await refreshAudit();
      syncStatus(auditError?'项目已同步；工作区检查暂未更新。':'自动同步已开启',!!auditError);
    }catch(err){if(epoch===viewEpoch){viewState='error';main.querySelector('.view-loading')?.remove();renderLoadFailure(main,err,{retryRead:()=>switchProject(id,requested,'replace'),scope:{project:id,operation:'context',label:'项目'},isCurrent:()=>epoch===viewEpoch&&workspaceId===id});syncStatus('暂未更新：'+err.message,true);}throw err;}
    finally{scheduleSync();}
  }
  function projectFor(value){
    const row=projectList.find(p=>value.v===2?p.workspace_id===value.workspace:p.root===value.workspace);
    if(!row)throw new Error('链接工作区未登记；不能通过路径访问任意项目。');
    return row.workspace_id;
  }
  async function start() {
    const app=document.getElementById('app'),shell=$('div',undefined,'shell'),sidebar=$('aside',undefined,'sidebar');
    const sidebarHeader=$('div',undefined,'sidebar-header'),brand=$('div',undefined,'brand');brand.append($('strong','DemiFlow'),$('span','数据工作台'));sidebarHeader.append(brand);sidebar.append(sidebarHeader);
    const content=$('div',undefined,'workspace-content'),top=$('div',undefined,'workspace-top'),header=$('header',undefined,'workspace-header');
    main=$('main');main.id='workspace-content';
    const title=$('div',undefined,'workspace-title'),workspaceName=$('strong');workspaceName.id='workspace-name';
    title.append(workspaceName);header.append(title);
    top.append(header);content.append(top,main);shell.append(sidebar,content);app.append(shell);
    if(offline){workspaceName.textContent='固定快照';sidebar.append($('div','固定快照 · 只读','nav-note'));heading('接线检查报告','此页面可独立查看。当前源码、表数据和运行结果未复核。');renderOffline(offline,main);return;}
    try{shell.classList.toggle('sidebar-collapsed',sessionStorage.getItem('demiflow-sidebar-collapsed')==='1');}catch{ /* Keep navigation usable without preference storage. */ }
    const toggle=iconButton('展开项目导航',()=>{shell.classList.toggle('nav-open');updateNavigation();},null,['M4 6h16M4 12h16M4 18h16']);title.prepend(toggle);
    toggle.id='navigation-toggle';toggle.setAttribute('aria-controls','project-sidebar');toggle.setAttribute('aria-expanded','false');sidebar.id='project-sidebar';
    const fold=$('button',undefined,'icon-button sidebar-toggle');fold.id='sidebar-toggle';fold.setAttribute('aria-controls','project-sidebar');fold.append(icon(['M4 4h16v16H4zM9 4v16']));const railBrand=$('span','DF','rail-brand');railBrand.setAttribute('aria-hidden','true');fold.append(railBrand);sidebarHeader.append(fold);
    fold.onclick=()=>{if(matchMedia('(max-width:760px)').matches)closeNavigation();else{const collapsed=shell.classList.toggle('sidebar-collapsed');try{sessionStorage.setItem('demiflow-sidebar-collapsed',String(Number(collapsed)));}catch{ /* Preference storage is optional. */ }updateNavigation();revealTab();}};
    const projectHeader=$('div',undefined,'project-header'),label=$('h2','项目','sidebar-heading');label.id='projects-heading';projectHeader.append(label);sidebar.append(projectHeader);
    projectNav=$('nav',undefined,'project-list');projectNav.id='project-list';projectNav.setAttribute('aria-labelledby',label.id);sidebar.append(projectNav);
    nav=$('nav',undefined,'function-nav');nav.setAttribute('aria-label','项目功能');nav.hidden=true;
    for(const [page,label]of [['development','开发'],['operators','算子'],['flows','管道'],['monitor','运行监控'],['tables','数据'],['overview','Readme']]){const b=button(label,()=>navigate(page),nav);b.dataset.page=page;b.disabled=true;}
    const actions=$('div',undefined,'header-actions');
    const settings=iconButton('项目设置',settingsDialog,actions,['M4 7h10m4 0h2M4 17h2m4 0h10M14 4v6M10 14v6']);settings.id='project-settings';
    iconButton('刷新当前视图',refreshView,actions,['M20 7v5h-5M4 17v-5h5M6 7a7 7 0 0 1 11-2l3 3M4 16l3 3a7 7 0 0 0 11-2']);
    iconButton('帮助',()=>documentDialog('使用 DemiFlow','DemiFlow 从原项目出发：阅读 README，查看固定版本数据与 Case，核对入口、配置和规范检查，再按权限准备运行。\n\n刷新和自动同步只读取当前状态；检查会保存报告。未识别不表示没有内容，静态检查通过不表示行为验证或语义审查已完成。'),actions,['M21 12a9 9 0 1 1-18 0 9 9 0 0 1 18 0','M9.1 9a3 3 0 0 1 5.8 1c0 2-3 2-3 4M12 17h.01']);header.append(actions);
    const crumb=$('span','项目总览','sr-only');crumb.id='workspace-path';title.append(crumb);
    const status=$('details',undefined,'workspace-status'),statusSummary=$('summary'),statusPanel=$('div',undefined,'workspace-status-panel');status.id='workspace-status';status.hidden=true;
    statusSummary.setAttribute('aria-label','工作区状态');
    const syncState=$('span',undefined,'nav-note');syncState.id='sync-status';syncState.setAttribute('role','status');syncState.append($('span','正在连接','sync-caption'),$('span','正在连接工作区…','sync-description sr-only'));statusSummary.append(syncState);
    const mode=$('span',undefined,'badge');mode.id='workspace-mode';mode.hidden=true;
    const syncDetail=$('p','正在连接工作区…','muted small');syncDetail.id='workspace-sync-detail';
    statusPanel.append($('h2','工作区状态'),mode,syncDetail);status.append(statusSummary,statusPanel);title.append(status);
    auditPanel=$('details',undefined,'audit-panel');auditPanel.id='audit-panel';auditPanel.hidden=true;statusPanel.append(auditPanel);
    header.insertBefore(nav,actions);
    updateNavigation();
    window.addEventListener('resize',()=>{updateNavigation();revealTab();});
    matchMedia('(max-width:760px)').addEventListener('change',()=>{closeNavigation();revealTab();});
    document.addEventListener('click',event=>{if(!status.contains(event.target))status.open=false;});
    document.addEventListener('keydown',event=>{if(event.key==='Escape'&&!document.querySelector('dialog[open]')){if(shell.classList.contains('nav-open')){closeNavigation();toggle.focus();}else if(status.open){status.open=false;statusSummary.focus();}}});
    window.addEventListener('pagehide',()=>{disposeReaders();revokeImages();readController.abort();clearTimeout(syncTimer);});
    projectHeading();
    main.append($('p','正在读取工作区…','busy'));
    try {
      const capability = await api('capabilities');
      if(capability.contract !== 'demiflow_forge_http_v1'){authFailed=true;throw new Error('HTTP 契约不匹配，请更新客户端后重开页面。');}
      serviceRunPermissions=capability.permissions;serviceWorkbenchPermissions=capability.workbench_permissions||{};workbenchContract=capability.workbench_contract;
      auditEnabled=capability.audit_contract==='demiflow_forge_audit_status_v1';
      projectManageEnabled=!!capability.permissions.project_manage;
      projectRemovalEnabled=!!capability.permissions.project_manage&&capability.operations.POST.includes('project-remove');
      if(projectManageEnabled){const manage=$('div',undefined,'project-actions');manage.id='project-actions';manage.hidden=true;projectHeader.append(manage);iconButton('新建项目',()=>projectDialog(true),manage,['M12 5v14M5 12h14']);iconButton('注册已有项目',()=>projectDialog(false),manage,['M3 7V5h6l2 2h10v13H3z','M12 10v7m-3-3 3 3 3-3']);}
      await refreshProjects();
      if(initialError)throw initialError;
      if(!projectList.length)clearProject();
      else await switchProject(initialView?projectFor(initialView):projectList[0].workspace_id,initialView,'replace');
    }catch(err){if(err.name!=='AbortError'){viewState='error';if(!err.presented)error(main,err);syncStatus('暂未更新：'+err.message,true);if(!context)main.append($('p','请从 workbench view 输出的带会话 token 地址打开。','muted'));}}
    window.addEventListener('popstate',async()=>{
      if(reloadSessionEntry())return;
      try{const value=V.decodeView(location.hash)||target('overview');const id=projectFor(value);if(id!==workspaceId)await switchProject(id,value,'none');else await openView(value,{historyMode:'none'});}
      catch(err){if(!err?.presented)error(main,err);}
    });
    document.addEventListener('visibilitychange',()=>{
      if(document.hidden){clearTimeout(syncTimer);syncStatus('页面隐藏，暂停自动同步');return;}
      syncView().catch(()=>{});
    });
    scheduleSync();
  }
  start().catch(err=>{if(!err?.presented)error(main,err);});
})();
