/* The UI consumes the same immutable inspection and case APIs as the CLI. */
(() => {
  'use strict';
  const $ = (tag, text, className) => { const n = document.createElement(tag); if (text !== undefined) n.textContent = text; if (className) n.className = className; return n; };
  // Chromium also emits a click when a text drag starts and ends in a control.
  // Keep the selected text and the current view in that case.
  let selectionStart;
  document.addEventListener('pointerdown', event => {
    selectionStart = event.isPrimary && event.button === 0 ? [event.clientX, event.clientY] : null;
  }, true);
  document.addEventListener('click', event => {
    const start = selectionStart;
    selectionStart = null;
    if (!start || !event.detail || Math.hypot(event.clientX - start[0], event.clientY - start[1]) < 4) return;
    const selection = window.getSelection();
    if (!selection?.isCollapsed && selection?.rangeCount && selection.getRangeAt(0).intersectsNode(event.target)) {
      event.preventDefault();
      event.stopPropagation();
    }
  }, true);
  // Shared navigation chrome; each view keeps ownership of its data and filters.
  function navigationHeader({title,searchLabel,onSearch,placeholder}) {
    const element=$('div',undefined,'side-navigation-tools'),heading=$('div',undefined,'side-navigation-heading');
    heading.append($('h2',title,'side-navigation-title'));
    const toggle=$('button',undefined,'side-navigation-search-toggle');toggle.type='button';toggle.title=searchLabel;toggle.setAttribute('aria-label',searchLabel);toggle.setAttribute('aria-expanded','false');
    toggle.append(icon(['M21 21l-5-5','M18 10a8 8 0 1 1-16 0 8 8 0 0 1 16 0']));
    const actions=$('div',undefined,'side-navigation-actions');actions.append(toggle);heading.append(actions);
    const field=$('div',undefined,'side-navigation-search'),search=$('input');field.hidden=true;
    search.type='search';search.maxLength=128;search.placeholder=placeholder||searchLabel;search.setAttribute('aria-label',searchLabel);
    search.oninput=()=>onSearch(search.value);
    function close(){field.hidden=true;toggle.setAttribute('aria-expanded','false');search.value='';onSearch('');toggle.focus();}
    toggle.onclick=()=>{if(!field.hidden){close();return;}field.hidden=false;toggle.setAttribute('aria-expanded','true');search.focus();};
    field.onkeydown=event=>{if(event.key==='Escape'){event.preventDefault();event.stopPropagation();close();}};
    field.append(search);element.append(heading,field);
    return {element,actions,search};
  }
  let navigationSequence=0;
  function resizableNavigation({layout,sidebar,label,active=true}) {
    const handle=$('div',undefined,'side-navigation-resizer'),key='demiflow.navigation-width.'+label;
    let width=null,drag=null,shield=null,disposed=false;
    try{const saved=Number(sessionStorage.getItem(key));if(saved>=140&&saved<=560)width=saved;}catch{}
    if(!sidebar.id)sidebar.id='side-navigation-'+(++navigationSequence);
    handle.tabIndex=0;handle.setAttribute('role','separator');handle.setAttribute('aria-orientation','vertical');
    handle.setAttribute('aria-label','调整'+label+'宽度');handle.setAttribute('aria-controls',sidebar.id);
    handle.title='拖动调整宽度；方向键微调；双击恢复默认';layout.classList.add('side-navigation-layout');layout.append(handle);
    const bounds=()=>({min:140,max:Math.max(140,Math.min(560,layout.clientWidth-140))});
    function sync(){
      if(disposed)return;
      handle.hidden=!active||sidebar.hidden;if(handle.hidden)return;
      const {min,max}=bounds();
      if(width!==null)layout.style.setProperty('--side-navigation-width',Math.max(min,Math.min(max,width))+'px');
      const rect=sidebar.getBoundingClientRect(),parent=layout.getBoundingClientRect();
      handle.style.left=(rect.right-parent.left-4)+'px';
      handle.setAttribute('aria-valuemin',min);handle.setAttribute('aria-valuemax',max);handle.setAttribute('aria-valuenow',Math.round(rect.width));
    }
    function setWidth(value){const {min,max}=bounds();width=Math.max(min,Math.min(max,value));sync();}
    function save(){try{if(width===null)sessionStorage.removeItem(key);else sessionStorage.setItem(key,String(width));}catch{}}
    function finish(){
      if(!drag)return;const id=drag.id;drag=null;shield?.remove();shield=null;
      if(handle.hasPointerCapture(id))handle.releasePointerCapture(id);save();
    }
    handle.onpointerdown=event=>{
      if(event.button!==0||!active)return;event.preventDefault();handle.focus();
      drag={id:event.pointerId,x:event.clientX,width:sidebar.getBoundingClientRect().width};handle.setPointerCapture(event.pointerId);
      // Native Notebook is an iframe: keep the gesture in the shell while crossing it.
      shield=$('div',undefined,'side-navigation-drag-shield');document.body.append(shield);
    };
    handle.onpointermove=event=>{if(drag?.id===event.pointerId)setWidth(drag.width+event.clientX-drag.x);};
    handle.onpointerup=finish;handle.onpointercancel=finish;handle.onlostpointercapture=finish;
    handle.ondblclick=()=>{width=null;layout.style.removeProperty('--side-navigation-width');save();sync();};
    handle.onkeydown=event=>{
      if(!['ArrowLeft','ArrowRight','Home','End'].includes(event.key))return;
      event.preventDefault();const {min,max}=bounds(),now=sidebar.getBoundingClientRect().width;
      setWidth(event.key==='Home'?min:event.key==='End'?max:now+(event.key==='ArrowRight'?1:-1)*(event.shiftKey?40:10));save();
    };
    const observer=new ResizeObserver(sync);observer.observe(layout);observer.observe(sidebar);sync();
    return {setActive(value){active=value;if(!active)finish();else if(width===null)layout.style.removeProperty('--side-navigation-width');sync();},dispose(){disposed=true;finish();observer.disconnect();handle.remove();}};
  }
  function collapsibleNavigation({layout,sidebar,header,label,initialOpen=true,active=true}) {
    let open=initialOpen;
    const reopen=$('div',undefined,'side-navigation-reopen'),toggle=$('button',undefined,'side-navigation-toggle');toggle.type='button';
    if(!sidebar.id)sidebar.id='side-navigation-'+(++navigationSequence);
    toggle.setAttribute('aria-label',label);toggle.setAttribute('aria-controls',sidebar.id);toggle.append(icon(['M3 4h18v16H3zM9 4v16']));
    layout.prepend(reopen);
    const resize=resizableNavigation({layout,sidebar,label,active:active&&open});
    function render(){
      sidebar.hidden=!active||!open;reopen.hidden=!active||open;
      const target=open?header.actions:reopen;if(toggle.parentElement!==target)target.append(toggle);
      toggle.setAttribute('aria-expanded',String(open));toggle.title=(open?'收起':'展开')+label;
      layout.classList.toggle('side-navigation-closed',!!layout.querySelector(':scope > .side-navigation-reopen:not([hidden])'));
      resize.setActive(active&&open);
    }
    toggle.onclick=()=>{open=!open;render();toggle.focus();};render();
    return {setActive(value){active=value;render();},dispose(){resize.dispose();}};
  }
  globalThis.ForgeNavigation=Object.freeze({header:navigationHeader,collapsible:collapsibleNavigation,resizable:resizableNavigation});
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
  let developmentView = null, workbenchView = null, notebooksView = null;
  const readers = new Set();
  // Cleanup registry only: view-scoped DOM handles disposed
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
    for (const controller of [developmentView, workbenchView, notebooksView]) if (controller?.canLeave && !controller.canLeave()) return false;
    return true;
  }
  function disposeReaders(root) {
    if (!root) { disposeViewHandles(); developmentView?.dispose(); developmentView = null; workbenchView?.dispose(); workbenchView=null; notebooksView?.dispose();notebooksView=null; }
    for (const reader of readers) if (!root || root.contains(reader.element)) {reader.dispose(); readers.delete(reader);}
  }
  function mountText(parent, text, {sourceOnly=false, compactSource=false, continuousSource=false, resolveLink, defaultMode='preview'}={}) {
    const reader=globalThis.ForgeReading.createTextReader({text, compactSource, continuousSource, defaultMode:sourceOnly?'source':defaultMode,
      identity:{epoch:viewEpoch, selection:selectionEpoch}, resolveLink, offline:!!offline});
    if(sourceOnly)reader.element.querySelector('.reading-toolbar').hidden=true;
    readers.add(reader);parent.append(reader.element);return reader;
  }
  function closeProjectPicker({restoreFocus=false}={}) {
    const picker=document.getElementById('project-picker'),toggle=document.getElementById('project-switcher');
    if(!picker||picker.hidden)return;
    const containedFocus=picker.contains(document.activeElement);
    picker.hidden=true;toggle?.setAttribute('aria-expanded','false');
    if(restoreFocus||containedFocus)toggle?.focus();
  }
  function filterProjects() {
    const term=(document.getElementById('project-search')?.value||'').trim().toLocaleLowerCase();let count=0;
    for(const item of projectNav?.querySelectorAll('[data-workspace]')||[]){
      item.hidden=!item.title.toLocaleLowerCase().includes(term);if(!item.hidden)count++;
    }
    const empty=document.getElementById('project-no-match');if(empty)empty.hidden=!projectList.length||count>0;
  }
  function toggleProjectPicker() {
    const picker=document.getElementById('project-picker'),toggle=document.getElementById('project-switcher');
    if(!picker.hidden){closeProjectPicker({restoreFocus:true});return;}
    document.getElementById('workspace-status').open=false;
    picker.hidden=false;toggle.setAttribute('aria-expanded','true');
    const search=document.getElementById('project-search');search.value='';filterProjects();search.focus();
    projectNav.querySelector('[aria-current=true]')?.scrollIntoView({block:'nearest'});
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
  const waitingReads=[];
  function acquireRead(signal){
    if(signal.aborted)return Promise.reject(staleRequest());
    if(activeReads<8){activeReads++;return Promise.resolve();}
    if(waitingReads.length>=32)return Promise.reject(new Error('页面等待读取过多，请稍后再试。'));
    return new Promise((resolve,reject)=>{
      const waiter={resolve,signal,cancel:null};
      waiter.cancel=()=>{const at=waitingReads.indexOf(waiter);if(at>=0)waitingReads.splice(at,1);signal.removeEventListener('abort',waiter.cancel);reject(staleRequest());};
      signal.addEventListener('abort',waiter.cancel,{once:true});waitingReads.push(waiter);
    });
  }
  function releaseRead(){
    const waiter=waitingReads.shift();
    if(waiter){waiter.signal.removeEventListener('abort',waiter.cancel);waiter.resolve();}
    else activeReads--;
  }
  const staleRequest = () => new DOMException('页面选择已变化', 'AbortError');
  function pageKind(reference = pendingView || view) {
    const kind = reference?.kind || 'overview';
    if(reference?.panel)return reference.panel;
    return ({development_new:'development',development_session:'development',table:'tables',table_request:'tables',case:'tables',inspection:'flows',run:'monitor',run_compare:'monitor',runs:'monitor',checks:'flows'})[kind] || kind;
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
  async function api(path, query = {}, body, projectId = workspaceId, options = {}) {
    if (offline) throw new Error('离线报告不能读取工作区或重新检查');
    const write=!!body&&!['plan-preview','plan-node-detail','plan-pipeline-source','analysis-source-register','analysis-case-group-bindings','analysis-case-page','analysis-annotation-heads','analysis-annotation-definitions','analysis-field-preview','analysis-source-from-handle','analysis-case-media','analysis-artifact-media-current','analysis-media'].includes(path);
    const cleanup=false;
    const epoch = viewEpoch;
    const url = (['projects','capabilities','project-create','project-register','project-remove','project-permissions','project-capabilities'].includes(path) ? '/api/' + path : '/api/workspaces/' + projectId + '/' + path) + (Object.keys(query).length ? '?' + new URLSearchParams(query) : '');
    if (url.length > 8192) throw new Error('查询链接超出长度限制，请缩小字段投影');
    const payload=body?V.boundedJSON(body,65536):null;
    const signal=cleanup?AbortSignal.timeout(5000):write?AbortSignal.timeout(65000):AbortSignal.any([readController.signal,...(options.signal?[options.signal]:[]),AbortSignal.timeout(65000)]);
    const run=async()=>{
    if (!write) await acquireRead(signal);
    else activeWrites++;
    try {
      const started=Date.now();let attempts=0;
      for(;;){
      const response = await fetch(url, {
        method: body ? 'POST' : 'GET', headers: {'X-Forge-Token': token, ...(body ? {'Content-Type':'application/json'} : {})},
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
        const projectRead=err.operationStarted===false&&(
          !body&&['projects','capabilities','context','changes','runs','graph','graph-source','table','sample','project-operators','operator-definition','operator-history','flow-arguments','project-documents','project-markdown'].includes(path)
          ||!!body&&['plan-preview','plan-node-detail','plan-pipeline-source'].includes(path));
        if(projectRead&&!write&&!cleanup&&err.code==='busy'&&!signal.aborted&&attempts<12&&Date.now()-started<20000){
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
      let value;
      try { value = await response.json(); }
      catch { if(!write&&(epoch!==viewEpoch||options.signal?.aborted))throw staleRequest();throw new Error(write ? '未能读取操作响应，请先核对操作结果，不要直接重复提交。' : '服务响应无法读取，请刷新后重试。'); }
      if (!write && !cleanup && epoch !== viewEpoch) throw staleRequest();
      return value;
      }
    } finally { if (!write) releaseRead(); else activeWrites--; }
    };
    return run();
  }
  function clearSelection() {
    selection = null;
  }

  function error(parent, err) {
    if (err?.name === 'AbortError' || !parent?.isConnected) return;
    const headerFailure=!!parent.closest('.workspace-header');
    const target=headerFailure?main:parent;
    if(!target?.isConnected)return;
    const box=$('div',err.message||String(err),'error-box');box.setAttribute('role','alert');
    if(headerFailure)target.prepend(box);else target.append(box);
    return box;
  }
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
  async function openView(value, {historyMode = 'push'} = {}) {
    if(!canLeaveViews())return;
    value = V.validateView(value);
    if (value.workspace !== (value.v === 2 ? workspaceId : context.workspace)) throw new Error('workspace_mismatch：此链接属于另一个工作区');
    if(value.v === 1)value={...value,v:2,workspace:workspaceId};
    rememberTab();
    readController.abort();readController=new AbortController();
    const epoch = ++viewEpoch; selectionEpoch++; disposeReaders();updates={};
    pendingView = value; clearSelection(); viewState = 'loading'; main.replaceChildren();main.classList.remove('bound-case-active','original-case-active','original-table-active','workbench-active','data-active','data-notebook-active','data-case-config-active','data-catalog-expanded','data-catalog-collapsed');projectHeading();
    if(value.kind==='table_request'){
      putHistory(value,historyMode);historyMode='replace';
      main.append($('p','正在读取请求的数据引用…','busy view-loading'));
    }
    markFunction();
    try {
      const page = pageKind();
      if (page === 'development') {
        const project=workspaceId,developmentApi=(path,query,body)=>{if(epoch!==viewEpoch)throw staleRequest();return api(path,query,body,project);};
        const mounted = await ForgeDevelopment.mount({main, api:developmentApi, workspaceId:project, view:value, mountText, disposeReaders,
          openNew:()=>epoch===viewEpoch?openView(target('development_new')):undefined,
          openSession:id=>epoch===viewEpoch?openView(target('development_session',{session_id:id})):undefined,
          error, register:handle=>{if(epoch===viewEpoch)developmentView=handle;else handle.dispose();}});
        if(epoch!==viewEpoch){mounted.dispose();throw staleRequest();}
        developmentView=mounted;
        if(developmentView?.sessionId)value=target('development_session',{session_id:developmentView.sessionId});
      }
      else if (page === 'overview') await overview(value);
      else if (page === 'tables') await tables(value,reference=>{
        if(epoch!==viewEpoch)throw staleRequest();
        const resolved=V.validateView(reference);
        if(resolved.v!==2||resolved.workspace!==workspaceId||resolved.kind!=='table')throw new Error('workspace_mismatch：数据引用属于另一个工作区');
        value=resolved;if(viewState==='ready'){view=value;pendingView=null;}else{pendingView=value;}main.querySelector('.view-loading')?.remove();
        putHistory(value,'replace');projectHeading();
      });
      else if (pipelinePage(page)) {main.classList.add('workbench-active');const mounted=await pipeline(value);value=mounted.reference;}
      if (epoch !== viewEpoch) throw staleRequest();
      main.querySelector('.view-loading')?.remove();
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
      rememberTab();await workbenchView.showPage(page);closeProjectPicker();return;
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
    const viewer=$('div');
    table(p,['原生流程入口','依据'],project.entries.map(e=>({value:e,cells:[e.entry,e.evidence]})),async e=>{
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
  async function overview(reference) {
    const epoch=viewEpoch,project=workspaceId,documentPath=reference.document||'README.md';
    const layout=$('div',undefined,'document-workspace'),directory=$('nav',undefined,'document-directory'),viewer=$('div',undefined,'document-viewer');
    directory.id='document-directory';directory.setAttribute('aria-label','Markdown 文档目录');
    const directoryHeader=ForgeNavigation.header({title:'文档',searchLabel:'搜索文档',placeholder:'筛选已展开的目录',onSearch:filterDocuments});
    directory.append(directoryHeader.element);layout.append(directory,viewer);main.append(layout);main.classList.add('readme-main');
    const directoryNavigation=ForgeNavigation.collapsible({layout,sidebar:directory,header:directoryHeader,label:'文档目录',initialOpen:!matchMedia('(max-width:760px)').matches});
    let disposed=false;const current=()=>!disposed&&epoch===viewEpoch&&project===workspaceId;
    registerViewHandle({dispose(){disposed=true;directoryNavigation.dispose();main.classList.remove('readme-main');}});
    const navigateDocument=path=>{if(current())return openView(target('overview',{document:path}));};
    function filterDocuments(){
      const term=directoryHeader.search.value.trim().toLocaleLowerCase();
      for(const item of directory.querySelectorAll('.document-item'))item.hidden=!item.dataset.document.toLocaleLowerCase().includes(term);
      for(const group of [...directory.querySelectorAll('.document-folder')].reverse())group.hidden=!!term&&!group.dataset.document.toLocaleLowerCase().includes(term)&&!group.querySelector('.document-item:not([hidden]),.document-folder:not([hidden])');
    }
    async function folder(host,path='',offset=0){
      if(!current()||host.dataset.loading==='true')return;
      host.dataset.loading='true';
      try{
        const result=await api('project-documents',{...(path?{path}:{}),offset,limit:50});if(!current()||!host.isConnected||(path&&!host.parentElement.open))return;
        if(directory.querySelectorAll('[data-document]').length+result.entries.length>300)throw new Error('展开的目录较多，请先收起部分目录。');
        host.querySelector(':scope > .document-page')?.remove();host.querySelector(':scope > .document-list-error')?.remove();
        for(const row of result.entries){
          if(row.kind==='directory'){
            const group=$('details',undefined,'document-folder'),summary=$('summary'),children=$('div',undefined,'document-children');
            group.dataset.document=row.path;summary.append($('span',row.name));summary.title=row.name;group.append(summary,children);host.append(group);
            group.addEventListener('toggle',()=>{if(group.open)folder(children,row.path);else children.replaceChildren();});
            if(documentPath.startsWith(row.path+'/'))group.open=true;
          }else{
            const item=button(row.name,()=>navigateDocument(row.path),host);item.className='document-item';item.dataset.document=row.path;item.title=row.path;
            if(row.path===documentPath){item.classList.add('active');item.setAttribute('aria-current','page');}
          }
        }
        if(!result.entries.length&&offset===0)host.append($('p','没有 Markdown 文档','document-empty'));
        if(result.next_offset!==null){const next=button('更多文件',()=>folder(host,path,result.next_offset),host);next.className='document-page';}
        filterDocuments();
      }catch(err){if(current()&&err.name!=='AbortError'){const message=$('p',err.message,'document-list-error');host.querySelector(':scope > .document-list-error')?.remove();host.append(message);}}
      finally{delete host.dataset.loading;}
    }
    const catalog=$('div',undefined,'document-tree');directory.append(catalog);folder(catalog);
    const intro=panel(documentPath,viewer);intro.id='project-readme';intro.tabIndex=-1;
    let text=context.readme;
    if(reference.document){try{const doc=await api('project-markdown',{path:documentPath});if(!current())return;text=doc.text;}catch(err){if(current())error(intro,err);return;}}
    if(!text){intro.append($('p',reference.document?'此文档为空。':'尚无 README.md。可在开发中补充项目说明。','notice'));if(!reference.document)button('进入开发',()=>navigate('development'),intro);}
    else if(!reference.document&&text.startsWith('项目说明无法读取：'))intro.append($('p',text,'error-box'));
    else mountText(intro,text,{resolveLink:href=>{
      if(!current()||/^(?:[a-z][a-z0-9+.-]*:|\/|#)/i.test(href))return null;
      let path;try{path=decodeURIComponent(new URL(href,'https://project.invalid/'+documentPath).pathname.slice(1));}catch{return null;}
      if(/\.(?:md|markdown)$/i.test(path)){try{V.validateView(target('overview',{document:path}));}catch{return null;}
        return {label:'打开文档 · '+path,open:()=>navigateDocument(path)};}
      return resolveProjectLink(path);
    }});
    const info=$('details');info.append($('summary','项目内容、能力与依据'));viewer.append(info);
    info.append($('p',context.flows.length+' 个登记流程 · '+context.tables.length+' 个固定数据引用 · '+context.report_total+' 份报告；报告数量不代表全部通过。','muted small'));
    button('查看管道与检查',()=>navigate('flows'),info);button('查看数据',()=>navigate('tables'),info);
    if(Object.keys(context.locations).length)table(info,['位置','根目录','权限'],Object.entries(context.locations).map(([name,value])=>({cells:[name,value.root,value.writable?'可声明输出':'只读']})));
    raw(info,'原生能力覆盖',context.native_api_coverage);raw(info,'版本化工程规范',context.standard);
  }

  async function projectChecks(parent=main,{project=workspaceId,isCurrent=()=>true,onBegin=()=>{},onEnd=()=>{}}={}) {
    const controls=$('div',undefined,'toolbar');parent.append(controls);
    const area=panel('当前项目规范检查',parent);
    const names={checked:'通过',missing:'尚未检查',invalid:'不符合规范',incomplete:'尚未完成',stale:'依据已变化'};
    function render(value) {
      if(!area.isConnected||!isCurrent())return;
      area.replaceChildren($('h2','检查结果'));
      area.append($('p','静态检查：'+(names[value.status]||value.status)));
      if(value.coverage)area.append($('p','已检查 '+value.coverage.inspected+' / '+value.coverage.declared+' 组已登记入口与配置。'));
      if(value.changes?.length)area.append($('p',value.changes.join('\n'),'notice'));
      for(const item of value.diagnostics||[])area.append($('p',[item.rule,(item.path||'')+(item.path&&item.line?':'+item.line:''),item.detail].filter(Boolean).join(' · '),'error-box'));
      if(value.entries?.length)table(area,['入口','配置','结果'],value.entries.map(item=>({value:item,cells:[item.entry,item.params,names[item.status]||item.status]})),item=>item.digest?openView(target('inspection',{report_digest:item.digest})):undefined);
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
  async function tables(request,onBinding=()=>{}) {
    const project=workspaceId,epoch=viewEpoch;
    const localApi=(path,query,body)=>{if(epoch!==viewEpoch)throw staleRequest();return api(path,query,body,project);};
    const originalPath=pendingOriginalPath;pendingOriginalPath=null;
    const mounted=await ForgeNotebooks.mount({parent:main,api:localApi,project,context,request,originalPath,onBinding});
    if(epoch!==viewEpoch){mounted.dispose();throw staleRequest();}
    notebooksView=mounted;
    updates.context=fresh=>mounted.updateContext(fresh);
    updates.tables=()=>mounted.updateContext(context);
  }
  function addResultTable(run,output,destination=context){
    const alias='run:'+run.run_id+':'+output.name+(run.attempt!=null?':'+run.attempt:'');
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
    document.getElementById('project-switcher')?.setAttribute('aria-label','切换项目：'+name.textContent);
    const mode=document.getElementById('workspace-mode');
    mode.textContent=context?(context.runs_enabled?'可查看、检查与运行':canRun('run_control')?'可查看、检查与停止已有运行':'可查看、检查 · 运行未开启'):'';
    mode.hidden=!context;
    const path=document.getElementById('workspace-path');
    if(path){const current=pendingView||view;path.textContent=({development:'开发',overview:'Readme',tables:'数据',flows:'管道',operators:'算子',monitor:'运行'})[pageKind()]+(current?.alias?' / '+current.alias:'')+(current?.row_id?' / Case '+current.row_id:'');}
    const settings=document.getElementById('project-settings');if(settings)settings.disabled=offline||!workspaceId;
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
    const dialog=$('dialog');dialog.setAttribute('aria-label','项目设置');
    const path=$('p',project.root,'muted small');path.style.overflowWrap='anywhere';
    dialog.append($('h2','项目设置'),$('p',project.name),path);
    if(projectRemovalEnabled)button('移除项目 '+project.name,()=>{dialog.close();removeProjectDialog(project);},dialog);
    else dialog.append($('p','此服务未授予项目管理权限。','notice'));
    button('关闭',()=>dialog.close(),dialog);document.body.append(dialog);
    dialog.addEventListener('close',()=>dialog.remove(),{once:true});dialog.showModal();
  }
  function clearProject(message='在项目中阅读说明、用 Notebook 分析数据、核对流程和检查结果。') {
    if(!canLeaveViews())return;
    readController.abort();readController=new AbortController();
    viewEpoch++;selectionEpoch++;auditRequest++;disposeReaders();closeProjectPicker();
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
          item=button('',()=>p.workspace_id===workspaceId?closeProjectPicker():switchProject(p.workspace_id));
          item.className='project-item';item.dataset.workspace=p.workspace_id;
          const mark=icon(['M3 7V5h6l2 2h10v13H3z']);mark.classList.add('project-mark');
          item.append(mark,$('span',undefined,'project-name'),$('span','暂不可用','project-unavailable'));
        }
        item.querySelector('.project-name').textContent=p.name;
        item.querySelector('.project-unavailable').hidden=p.status==='available';
        item.dataset.unavailable=String(p.status!=='available');item.setAttribute('aria-label',p.name+(p.status==='available'?'':' · 暂不可用'));
        item.title=p.name+'\n'+p.root;
        if(projectNav.children[index]!==item)projectNav.insertBefore(item,projectNav.children[index]||null);
        existing.delete(p.workspace_id);
      }
      for(const item of existing.values())item.remove();
      if(!rows.length)projectNav.append($('p','尚未登记项目','project-empty'));
      markProject();filterProjects();
    }
    const manage=document.getElementById('project-actions');if(manage)manage.hidden=false;
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
    const epoch=++viewEpoch;selectionEpoch++;disposeReaders();closeProjectPicker();
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
    const app=document.getElementById('app'),shell=$('div',undefined,'shell');
    const brand=$('div',undefined,'brand');brand.append($('strong','DemiFlow'));
    const content=$('div',undefined,'workspace-content'),top=$('div',undefined,'workspace-top'),header=$('header',undefined,'workspace-header');
    main=$('main');main.id='workspace-content';
    const title=$('div',undefined,'workspace-title'),workspaceName=$('strong');workspaceName.id='workspace-name';
    title.append(brand);header.append(title);
    top.append(header);content.append(top,main);shell.append(content);app.append(shell);
    if(offline){workspaceName.textContent='固定快照 · 只读';title.append(workspaceName);heading('接线检查报告','此页面可独立查看。当前源码、表数据和运行结果未复核。');renderOffline(offline,main);return;}
    const toggle=button('',toggleProjectPicker,title);toggle.className='project-switcher';toggle.id='project-switcher';
    toggle.append(workspaceName,icon(['m8 10 4 4 4-4']));toggle.setAttribute('aria-haspopup','dialog');toggle.setAttribute('aria-controls','project-picker');toggle.setAttribute('aria-expanded','false');
    const picker=$('div',undefined,'project-picker');picker.id='project-picker';picker.hidden=true;picker.setAttribute('role','dialog');picker.setAttribute('aria-label','切换项目');top.append(picker);
    const projectHeader=$('div',undefined,'project-header'),label=$('h2','项目','project-picker-heading');label.id='projects-heading';projectHeader.append(label);picker.append(projectHeader);
    const search=$('input');search.id='project-search';search.type='search';search.maxLength=128;search.placeholder='搜索项目…';search.setAttribute('aria-label','搜索项目');search.oninput=filterProjects;picker.append(search);
    projectNav=$('nav',undefined,'project-list');projectNav.id='project-list';projectNav.setAttribute('aria-labelledby',label.id);picker.append(projectNav);
    const noMatch=$('p','没有匹配的项目','project-empty');noMatch.id='project-no-match';noMatch.hidden=true;picker.append(noMatch);
    picker.addEventListener('keydown',event=>{
      if(!['ArrowDown','ArrowUp','Enter'].includes(event.key))return;
      const rows=[...projectNav.querySelectorAll('[data-workspace]')].filter(item=>!item.hidden),index=rows.indexOf(document.activeElement);
      if(event.target!==search&&index<0)return;
      if(event.key==='Enter'){if(event.target===search&&rows.length){event.preventDefault();rows[0].click();}return;}
      if(rows.length){event.preventDefault();rows[index<0?(event.key==='ArrowDown'?0:rows.length-1):(index+(event.key==='ArrowDown'?1:-1)+rows.length)%rows.length].focus();}
    });
    picker.addEventListener('focusout',event=>{if(event.relatedTarget&&!picker.contains(event.relatedTarget)&&event.relatedTarget!==toggle)closeProjectPicker();});
    nav=$('nav',undefined,'function-nav');nav.setAttribute('aria-label','项目功能');nav.hidden=true;
    for(const [page,label]of [['development','开发'],['operators','算子'],['flows','管道'],['monitor','运行'],['tables','数据'],['overview','Readme']]){const b=button(label,()=>navigate(page),nav);b.dataset.page=page;b.disabled=true;}
    const actions=$('div',undefined,'header-actions');
    const settings=iconButton('设置',settingsDialog,actions,['M4 7h10m4 0h2M4 17h2m4 0h10M14 4v6M10 14v6']);settings.id='project-settings';
    iconButton('刷新当前视图',refreshView,actions,['M20 7v5h-5M4 17v-5h5M6 7a7 7 0 0 1 11-2l3 3M4 16l3 3a7 7 0 0 0 11-2']);
    iconButton('帮助',()=>documentDialog('使用 DemiFlow','DemiFlow 从原项目出发：阅读文档，用原生 Jupyter 分析数据，核对入口、配置和规范检查，再按权限准备运行。\n\n刷新和自动同步只读取当前状态；检查会保存报告。未识别不表示没有内容，静态检查通过不表示行为验证或语义审查已完成。'),actions,['M21 12a9 9 0 1 1-18 0 9 9 0 0 1 18 0','M9.1 9a3 3 0 0 1 5.8 1c0 2-3 2-3 4M12 17h.01']);header.append(actions);
    const crumb=$('span','项目总览','sr-only');crumb.id='workspace-path';title.append(crumb);
    const status=$('details',undefined,'workspace-status'),statusSummary=$('summary'),statusPanel=$('div',undefined,'workspace-status-panel');status.id='workspace-status';status.hidden=true;
    statusSummary.setAttribute('aria-label','工作区状态');
    const syncState=$('span',undefined,'nav-note');syncState.id='sync-status';syncState.setAttribute('role','status');syncState.append($('span','正在连接','sync-caption'),$('span','正在连接工作区…','sync-description sr-only'));statusSummary.append(syncState);
    const mode=$('span',undefined,'badge');mode.id='workspace-mode';mode.hidden=true;
    const syncDetail=$('p','正在连接工作区…','muted small');syncDetail.id='workspace-sync-detail';
    statusPanel.append($('h2','工作区状态'),mode,syncDetail);status.append(statusSummary,statusPanel);title.append(status);
    auditPanel=$('details',undefined,'audit-panel');auditPanel.id='audit-panel';auditPanel.hidden=true;statusPanel.append(auditPanel);
    header.insertBefore(nav,actions);
    window.addEventListener('resize',revealTab);
    document.addEventListener('click',event=>{if(!status.contains(event.target))status.open=false;if(!picker.contains(event.target)&&!toggle.contains(event.target))closeProjectPicker();});
    document.addEventListener('keydown',event=>{if(event.key==='Escape'&&!document.querySelector('dialog[open]')){if(!picker.hidden){event.preventDefault();closeProjectPicker({restoreFocus:true});}else if(status.open){status.open=false;statusSummary.focus();}}});
    window.addEventListener('pagehide',()=>{disposeReaders();readController.abort();clearTimeout(syncTimer);});
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
      // Register before rendering: the first page can be visible while its
      // trailing audit reads are still pending, including after a reload.
      window.addEventListener('popstate',async()=>{
        closeProjectPicker();
        if(reloadSessionEntry())return;
        try{const value=V.decodeView(location.hash)||target('overview');const id=projectFor(value);if(id!==workspaceId)await switchProject(id,value,'none');else await openView(value,{historyMode:'none'});}
        catch(err){if(!err?.presented)error(main,err);}
      });
      if(!projectList.length)clearProject();
      else await switchProject(initialView?projectFor(initialView):projectList[0].workspace_id,initialView,'replace');
    }catch(err){if(err.name!=='AbortError'){viewState='error';if(!err.presented)error(main,err);syncStatus('暂未更新：'+err.message,true);if(!context)main.append($('p','请从 workbench view 输出的带会话 token 地址打开。','muted'));}}
    document.addEventListener('visibilitychange',()=>{
      if(document.hidden){clearTimeout(syncTimer);syncStatus('页面隐藏，暂停自动同步');return;}
      syncView().catch(()=>{});
    });
    scheduleSync();
  }
  start().catch(err=>{if(!err?.presented)error(main,err);});
})();
