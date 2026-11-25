/* Unified Pipeline workspace: collapsible left sidebar (大纲|文件|运行) + single-focus center (DAG|代码) + right docked drawer (节点详情|开发助手). */
(() => {
  'use strict';
  const el = (tag, text, cls) => {
    const n = document.createElement(tag);
    if (text !== undefined) n.textContent = text;
    if (cls) n.className = cls;
    return n;
  };
  const tabSvg = paths => {
    const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
    svg.setAttribute('viewBox', '0 0 24 24');
    svg.setAttribute('aria-hidden', 'true');
    for (const d of paths) {
      const p = document.createElementNS(svg.namespaceURI, 'path');
      p.setAttribute('d', d);
      svg.append(p);
    }
    return svg;
  };
  const runStatusLabel = {
    pending: '未开始', prepared: '已准备', starting: '启动中', running: '运行中',
    stopping: '停止中', succeeded: '成功', failed: '失败', cancelled: '已取消', interrupted: '中断',
  };

  async function mount(options) {
    const {
      main, api, icon, context, workspaceId, view = {}, mountText, disposeReaders,
      openResult, navigate, onContext, rememberView, onPageChange, projectSettings,
      renderProjectInfo, projectCheckState, continueInput, continueWithResult,
      error, openNew, openSession, initialDrawer = null, operatorTarget = null, register,
    } = options;

    let disposed = false, workbenchHandle = null, devHandle = null;
    let sidebarTab = 'outline', drawerOpen = false, drawerTab = 'assistant', codeOpen = false;
    const drawerStorageKey = 'demiflow-pipeline-drawer:' + workspaceId;

    main.classList.add('pipeline-workspace-active');
    const root = el('div', undefined, 'pw-workspace');
    main.append(root);

    // 1. Left collapsible sidebar (大纲 | 文件 | 运行)
    const sidebar = el('aside', undefined, 'pw-sidebar');
    sidebar.setAttribute('aria-label', '管线导航');
    const navigation = ForgeNavigation.header({
      title: '管线',
      searchLabel: '搜索大纲、文件或运行',
      placeholder: '搜索大纲、文件或运行',
      onSearch: () => {
        const q = navigation.search.value;
        devHandle?.files?.setFilter(q);
        if (sidebarTab === 'runs') renderRunsList();
      },
    });
    const sideTabs = el('div', undefined, 'dev-sidebar-tabs');
    const makeSideTab = (label, paths, active = false) => {
      const b = el('button', undefined, 'dev-sidebar-tab' + (active ? ' is-active' : ''));
      b.type = 'button';
      b.setAttribute('aria-label', label);
      b.setAttribute('aria-pressed', String(active));
      b.dataset.tooltip = label;
      b.title = label;
      b.append(tabSvg(paths));
      return b;
    };
    const outlineBtn = makeSideTab('大纲', ['M8 6h13', 'M8 12h13', 'M8 18h13', 'M3 6h.01', 'M3 12h.01', 'M3 18h.01'], true);
    const filesBtn = makeSideTab('文件', ['M3 7a2 2 0 0 1 2-2h4l2 2h8a2 2 0 0 1 2 2v8a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z']);
    const filesBadge = el('span', '0', 'dev-change-badge'); filesBadge.hidden = true;
    filesBtn.append(filesBadge);
    const runsBtn = makeSideTab('运行', ['M3 12a9 9 0 1 0 3-6.7L3 8', 'M3 3v5h5', 'M12 7v5l3 2']);
    sideTabs.append(outlineBtn, filesBtn, runsBtn);

    const outlinePane = el('div', undefined, 'pw-sidebar-pane');
    const filesPane = el('div', undefined, 'pw-sidebar-pane'); filesPane.hidden = true;
    const runsPane = el('div', undefined, 'pw-sidebar-pane'); runsPane.hidden = true;
    const runsHead = el('div', undefined, 'pw-runs-head');
    const runsAllBtn = el('button', '查看运行监控'); runsAllBtn.type = 'button';
    runsAllBtn.onclick = () => { showCode(false); workbenchHandle?.showPage?.('monitor'); };
    const runsRefreshBtn = el('button', '刷新'); runsRefreshBtn.type = 'button';
    runsRefreshBtn.onclick = () => loadRuns(true);
    runsHead.append(runsAllBtn, runsRefreshBtn);
    const runsList = el('div');
    runsPane.append(runsHead, runsList);
    sidebar.append(navigation.element, sideTabs, outlinePane, filesPane, runsPane);

    // 2. Center single-focus canvas (DAG/Run OR Code/Operator detail)
    const center = el('section', undefined, 'pw-center');
    const workbenchHost = el('div', undefined, 'pw-workbench-host');
    const codeHost = el('div', undefined, 'pw-code-host'); codeHost.hidden = true;
    const codeBar = el('div', undefined, 'pw-code-bar');
    const backGraphBtn = el('button', '← 返回执行图', 'pw-back-graph'); backGraphBtn.type = 'button';
    backGraphBtn.onclick = () => { showCode(false); devHandle?.files?.setOpen?.(false, {skipFocus: true}); };
    const codeAssistantBtn = el('button', '开发助手', 'pw-assistant-toggle'); codeAssistantBtn.type = 'button';
    codeAssistantBtn.onclick = () => toggleAssistant();
    codeBar.append(backGraphBtn, codeAssistantBtn);
    const codeBody = el('div', undefined, 'pw-code-body');
    codeHost.append(codeBar, codeBody);
    center.append(workbenchHost, codeHost);

    // 3. Right single docked drawer (节点详情 | 开发助手) — never exceeds 3 columns, zero modals
    const drawer = el('aside', undefined, 'pw-drawer');
    drawer.setAttribute('aria-label', '管线右侧面板');
    drawer.hidden = true;
    const drawerHead = el('header', undefined, 'pw-drawer-head');
    const drawerTabs = el('div', undefined, 'pw-drawer-tabs');
    drawerTabs.setAttribute('role', 'tablist');
    drawerTabs.setAttribute('aria-label', '右侧面板');
    const detailTabBtn = el('button', '节点详情', 'pw-drawer-tab');
    detailTabBtn.type = 'button'; detailTabBtn.setAttribute('role', 'tab'); detailTabBtn.dataset.tab = 'detail';
    const assistantTabBtn = el('button', '开发助手', 'pw-drawer-tab');
    assistantTabBtn.type = 'button'; assistantTabBtn.setAttribute('role', 'tab'); assistantTabBtn.dataset.tab = 'assistant';
    drawerTabs.append(detailTabBtn, assistantTabBtn);
    const drawerClose = el('button', '×', 'pw-drawer-close');
    drawerClose.type = 'button'; drawerClose.title = '收起右侧面板'; drawerClose.setAttribute('aria-label', drawerClose.title);
    drawerHead.append(drawerTabs, drawerClose);

    const detailPane = el('div', undefined, 'pw-drawer-pane'); detailPane.dataset.pane = 'detail';
    const assistantPane = el('div', undefined, 'pw-drawer-pane'); assistantPane.dataset.pane = 'assistant';
    drawer.append(drawerHead, detailPane, assistantPane);
    root.append(sidebar, center, drawer);

    const sidebarCollapse = ForgeNavigation.collapsible({layout: root, sidebar, header: navigation, label: '管线目录'});
    const drawerResize = ForgeNavigation.resizable({layout: root, sidebar: drawer, label: '管线右侧面板', right: true, cssVar: '--pw-drawer-width', minWidth: 300, maxWidth: 680, minMain: 360, active: false});

    const headerAssistantBtn = el('button', '开发助手', 'pw-assistant-toggle');
    headerAssistantBtn.type = 'button';
    headerAssistantBtn.setAttribute('aria-expanded', 'false');
    headerAssistantBtn.onclick = () => toggleAssistant();

    function showCode(open) {
      codeOpen = !!open;
      codeHost.hidden = !codeOpen;
      workbenchHost.hidden = codeOpen;
    }

    function syncDrawer() {
      root.dataset.drawer = drawerOpen ? 'open' : 'closed';
      root.dataset.drawerTab = drawerTab;
      drawer.hidden = !drawerOpen;
      detailPane.hidden = drawerTab !== 'detail';
      assistantPane.hidden = drawerTab !== 'assistant';
      detailTabBtn.setAttribute('aria-selected', String(drawerTab === 'detail'));
      assistantTabBtn.setAttribute('aria-selected', String(drawerTab === 'assistant'));
      const asstExpanded = String(drawerOpen && drawerTab === 'assistant');
      headerAssistantBtn.setAttribute('aria-expanded', asstExpanded);
      codeAssistantBtn.setAttribute('aria-expanded', asstExpanded);
      drawerResize.setActive(drawerOpen && !matchMedia('(max-width:900px)').matches);
      try { sessionStorage.setItem(drawerStorageKey, JSON.stringify({open: drawerOpen, tab: drawerTab})); } catch {}
    }

    function openDrawer(tab) {
      if (tab) drawerTab = tab;
      drawerOpen = true;
      syncDrawer();
    }
    function closeDrawer() {
      drawerOpen = false;
      workbenchHandle?.closeDetail?.();
      syncDrawer();
    }
    function toggleAssistant() {
      if (drawerOpen && drawerTab === 'assistant') closeDrawer();
      else openDrawer('assistant');
    }

    detailTabBtn.onclick = () => openDrawer('detail');
    assistantTabBtn.onclick = () => openDrawer('assistant');
    drawerClose.onclick = () => closeDrawer();

    let runRows = [], runsLoaded = false;
    function renderRunsList() {
      runsList.replaceChildren();
      const q = navigation.search.value.trim().toLocaleLowerCase();
      const items = runRows.filter(r => !q || [r.run_id, r.entry, r.status].join(' ').toLocaleLowerCase().includes(q));
      if (!items.length) {
        runsList.append(el('p', runsLoaded ? (q ? '没有匹配的运行记录。' : '暂无运行记录。') : '正在读取运行记录…', 'op-muted dev-op-pad'));
        return;
      }
      for (const item of items) {
        const btn = el('button', undefined, 'pw-run-item'); btn.type = 'button';
        const top = el('div', undefined, 'pw-run-item-top');
        const name = (item.entry || item.run_id || '').split(':').pop();
        top.append(el('span', name, 'pw-run-item-name'));
        const st = el('span', runStatusLabel[item.status] || item.status || '', 'pw-run-item-status');
        st.dataset.status = item.status || '';
        top.append(st);
        btn.append(top, el('span', item.run_id, 'pw-run-item-meta'));
        btn.onclick = () => {
          showCode(false);
          devHandle?.files?.setOpen?.(false, {skipFocus: true});
          navigate?.({v: 2, workspace: workspaceId, kind: 'run', run_id: item.run_id});
        };
        runsList.append(btn);
      }
    }
    async function loadRuns(force = false) {
      if (runsLoaded && !force) { renderRunsList(); return; }
      renderRunsList();
      try {
        const res = await api('runs', {limit: 20});
        if (disposed) return;
        runRows = res?.runs || [];
        runsLoaded = true;
        renderRunsList();
      } catch (err) {
        if (!disposed) runsList.replaceChildren(el('p', err.message || '读取运行记录失败', 'op-error dev-op-pad'));
      }
    }

    function setSidebarTab(next) {
      sidebarTab = ['outline', 'files', 'runs'].includes(next) ? next : 'outline';
      outlinePane.hidden = sidebarTab !== 'outline';
      filesPane.hidden = sidebarTab !== 'files';
      runsPane.hidden = sidebarTab !== 'runs';
      outlineBtn.classList.toggle('is-active', sidebarTab === 'outline');
      filesBtn.classList.toggle('is-active', sidebarTab === 'files');
      runsBtn.classList.toggle('is-active', sidebarTab === 'runs');
      outlineBtn.setAttribute('aria-pressed', String(sidebarTab === 'outline'));
      filesBtn.setAttribute('aria-pressed', String(sidebarTab === 'files'));
      runsBtn.setAttribute('aria-pressed', String(sidebarTab === 'runs'));
      if (sidebarTab === 'files') { devHandle?.files?.setFilter(navigation.search.value); devHandle?.files?.ensureTreeLoaded?.(); }
      else if (sidebarTab === 'outline') { devHandle?.files?.setFilter(navigation.search.value); devHandle?.files?.ensureOperatorsLoaded?.(); }
      else if (sidebarTab === 'runs') { loadRuns(); }
    }
    outlineBtn.onclick = () => setSidebarTab('outline');
    filesBtn.onclick = () => setSidebarTab('files');
    runsBtn.onclick = () => setSidebarTab('runs');

    const isDevView = ['development', 'development_session', 'development_new'].includes(view?.kind);
    const wbView = isDevView ? {v: 2, workspace: workspaceId, kind: 'flows', panel: 'flows'} : view;
    const devView = isDevView ? view : {v: 2, workspace: workspaceId, kind: 'development'};

    const handle = {
      workspaceId,
      get sessionId() { return devHandle?.sessionId; },
      get workbench() { return workbenchHandle; },
      get development() { return devHandle; },
      canLeave: () => {
        if (workbenchHandle?.dirty?.()) return confirm('当前有未保存的配置草稿，确定离开吗？');
        return true;
      },
      openAssistant: () => openDrawer('assistant'),
      openDetail: () => openDrawer('detail'),
      showPage: async page => {
        showCode(false);
        devHandle?.files?.setOpen?.(false, {skipFocus: true});
        if (workbenchHandle?.showPage) await workbenchHandle.showPage(page);
      },
      navigate: async nextView => {
        if (disposed) return;
        if (['development', 'development_session', 'development_new'].includes(nextView?.kind)) {
          openDrawer('assistant');
          await devHandle?.navigate?.(nextView);
          return;
        }
        if (nextView?.panel === 'operators') {
          setSidebarTab('outline');
          await devHandle?.files?.selectOperatorTarget?.(true);
          return;
        }
        showCode(false);
        devHandle?.files?.setOpen?.(false, {skipFocus: true});
        if (nextView?.kind === 'run' && nextView?.run_id) {
          await workbenchHandle?.sync?.(context, {force: false});
          await workbenchHandle?.showPage?.('monitor');
        } else if (nextView?.panel === 'monitor' || nextView?.kind === 'runs') {
          await workbenchHandle?.showPage?.('monitor');
        } else {
          await workbenchHandle?.showPage?.('flows');
          if (nextView?.entry) await workbenchHandle?.selectFlow?.(nextView.entry);
        }
      },
      sync: async (fresh, opts = {}) => {
        await workbenchHandle?.sync?.(fresh, opts);
        if (opts.catalogChanged || opts.graphChanged || opts.force) {
          devHandle?.files?.ensureOperatorsLoaded?.({force: true});
        }
        if (opts.runsChanged || opts.force) {
          runsLoaded = false;
          if (sidebarTab === 'runs') loadRuns(true);
        }
      },
      projectCheckSettled: () => workbenchHandle?.projectCheckSettled?.(),
      dispose: () => {
        if (disposed) return;
        disposed = true;
        sidebarCollapse.dispose();
        drawerResize.dispose();
        workbenchHandle?.dispose?.();
        devHandle?.dispose?.();
        main.classList.remove('pipeline-workspace-active');
      },
    };
    register?.(handle);

    // Mount ForgeDevelopment in drawerMode first so left sidebar outline/files hosts are wired.
    const devPromise = ForgeDevelopment.mount({
      main: assistantPane,
      api,
      workspaceId,
      view: devView,
      mountText,
      disposeReaders,
      drawerMode: true,
      externalFilesHost: filesPane,
      externalOperatorsHost: outlinePane,
      externalCodeHost: codeBody,
      openOperators: view?.panel === 'operators' || !!operatorTarget,
      operatorTarget,
      openNew,
      openSession,
      error,
      onCodeOpenChange: isOpen => showCode(isOpen),
      onSelectFlow: async entry => {
        showCode(false);
        await workbenchHandle?.selectFlow?.(entry);
      },
      onShowFilesTab: () => setSidebarTab('files'),
      onBadgeChange: count => { filesBadge.textContent = String(count); filesBadge.hidden = !count; },
      onTurnCompleted: async () => {
        try {
          const fresh = await api('context');
          if (!disposed && fresh) {
            onContext?.(fresh);
            await workbenchHandle?.sync?.(fresh, {graphChanged: true, catalogChanged: true, force: true});
            devHandle?.files?.ensureOperatorsLoaded?.({force: true});
          }
        } catch {}
      },
      register: h => { devHandle = h; },
    });

    const wbPromise = ForgeWorkbench.mount({
      main: workbenchHost,
      api,
      icon,
      context,
      workspaceId,
      view: wbView,
      mountText,
      disposeReaders,
      openResult,
      navigate,
      openOperator: async targetOp => {
        setSidebarTab('outline');
        await devHandle?.files?.selectOperatorTarget?.(targetOp || true);
      },
      register: h => { workbenchHandle = h; },
      projectCheckState,
      continueInput,
      continueWithResult,
      onContext,
      onPageChange,
      rememberView,
      projectSettings,
      renderProjectInfo,
      embedded: {
        hideFlowSidebar: true,
        nodeDetailHost: detailPane,
        assistantButton: headerAssistantBtn,
        onDetailOpen: () => openDrawer('detail'),
        onDetailClose: () => { if (drawerTab === 'detail') closeDrawer(); },
        onAskAssistant: target => {
          openDrawer('assistant');
          if (target?.path) {
            devHandle?.insertReference?.(target.path + (target.line ? ':' + target.line : ''));
          } else if (target?.name) {
            devHandle?.insertReference?.(target.name);
          }
        },
      },
    });

    await Promise.all([
      devPromise.catch(err => {
        if (!disposed) assistantPane.replaceChildren(el('p', err?.message || '当前服务未启用开发助手。', 'op-muted dev-op-pad'));
      }),
      wbPromise,
    ]);
    if (disposed) return handle;

    // Load semantic outline right away so counts appear cleanly on the collapsed groups.
    devHandle?.files?.ensureOperatorsLoaded?.();

    // Decide initial right drawer state: explicit dev link -> assistant open; no flows -> assistant open; else restore saved or closed.
    let savedDrawer = null;
    try { savedDrawer = JSON.parse(sessionStorage.getItem(drawerStorageKey) || 'null'); } catch {}
    if (initialDrawer === 'assistant' || isDevView) {
      drawerOpen = true; drawerTab = 'assistant';
    } else if (initialDrawer === 'detail') {
      drawerOpen = true; drawerTab = 'detail';
    } else if (!(context?.flows?.length || context?.project?.entries?.length)) {
      drawerOpen = true; drawerTab = 'assistant';
    } else if (savedDrawer && typeof savedDrawer.open === 'boolean') {
      drawerOpen = savedDrawer.open;
      drawerTab = savedDrawer.tab === 'detail' ? 'detail' : 'assistant';
    }
    syncDrawer();
    return handle;
  }

  globalThis.ForgePipelineWorkspace = Object.freeze({mount});
})();
