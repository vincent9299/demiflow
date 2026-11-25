/* Unified Pipeline workspace:
 * - Left collapsible sidebar: 管线 (result flows) | 会话 (multi-session chat) | 组件 (reusable outline) | 文件 (workspace tree)
 * - Center wide stage: 管线画布 (DAG + pipeline runs) ⇄ 管线开发助手会话 (wide multi-session assistant)
 * - Right collapsible/resizable supplementary panel (补充信息区): auto-switches between 节点详情 and 代码/组件阅读器 based on interaction.
 */
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

  async function mount(options) {
    const {
      main, api, icon, context, workspaceId, view = {}, mountText, disposeReaders,
      openResult, navigate, onContext, rememberView, onPageChange, projectSettings,
      renderProjectInfo, projectCheckState, continueInput, continueWithResult,
      error, openNew, openSession, initialDrawer = null, operatorTarget = null, register,
    } = options;

    let disposed = false, workbenchHandle = null, devHandle = null;
    let currentContext = context;
    let sidebarTab = 'flows', stageMode = 'canvas', rightMode = null, drawerExpanded = false;

    main.classList.add('pipeline-workspace-active');
    const root = el('div', undefined, 'pw-workspace');
    main.append(root);

    // 1. Left collapsible sidebar (管线 | 会话 | 组件 | 文件)
    const sidebar = el('aside', undefined, 'pw-sidebar');
    sidebar.setAttribute('aria-label', '管线导航');
    const navigation = ForgeNavigation.header({
      title: '管线',
      searchLabel: '搜索管线、会话或文件',
      placeholder: '搜索管线、会话或文件',
      onSearch: () => {
        const q = navigation.search.value;
        workbenchHandle?.filterFlows?.(q);
        devHandle?.setSessionFilter?.(q);
        devHandle?.files?.setFilter(q);
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
    const flowsBtn = makeSideTab('管线', ['M6 3v12', 'M18 9a3 3 0 1 0 0-6 3 3 0 0 0 0 6z', 'M6 21a3 3 0 1 0 0-6 3 3 0 0 0 0 6z', 'M18 9a9 9 0 0 1-9 9'], true);
    const sessionsBtn = makeSideTab('会话', ['M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z']);
    const filesBtn = makeSideTab('文件', ['M3 7a2 2 0 0 1 2-2h4l2 2h8a2 2 0 0 1 2 2v8a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z']);
    const filesBadge = el('span', '0', 'dev-change-badge'); filesBadge.hidden = true;
    filesBtn.append(filesBadge);
    sideTabs.append(flowsBtn, sessionsBtn, filesBtn);

    const flowsPane = el('div', undefined, 'pw-sidebar-pane pw-flows-pane');
    const sessionsPane = el('div', undefined, 'pw-sidebar-pane'); sessionsPane.hidden = true;
    const filesPane = el('div', undefined, 'pw-sidebar-pane'); filesPane.hidden = true;
    sidebar.append(navigation.element, sideTabs, flowsPane, sessionsPane, filesPane);

    // 2. Center wide stage (管线画布 ⇄ 管线开发助手会话)
    const center = el('section', undefined, 'pw-center');
    const workbenchHost = el('div', undefined, 'pw-workbench-host');
    const assistantHost = el('div', undefined, 'pw-assistant-host'); assistantHost.hidden = true;
    center.append(workbenchHost, assistantHost);

    // 3. Right collapsible & resizable supplementary panel (补充信息区: 节点详情 | 代码/组件阅读器)
    const drawer = el('aside', undefined, 'pw-drawer');
    drawer.setAttribute('aria-label', '补充信息区');
    drawer.hidden = true;
    const detailPane = el('div', undefined, 'pw-drawer-pane'); detailPane.dataset.pane = 'detail'; detailPane.hidden = true;
    const codePane = el('div', undefined, 'pw-drawer-pane'); codePane.dataset.pane = 'code'; codePane.hidden = true;
    drawer.append(detailPane, codePane);

    root.append(sidebar, center, drawer);

    const sidebarCollapse = ForgeNavigation.collapsible({layout: root, sidebar, header: navigation, label: '管线目录'});
    const drawerResize = ForgeNavigation.resizable({
      layout: root, sidebar: drawer, label: '补充信息区', right: true,
      cssVar: '--pw-drawer-width', minWidth: 300, maxWidth: 760, minMain: 360, active: false,
    });

    const headerSourceBtn = el('button', '源码', 'pw-header-ghost');
    headerSourceBtn.type = 'button';
    headerSourceBtn.title = '查看当前管线源码';
    headerSourceBtn.onclick = () => {
      const entry = workbenchHandle?.currentEntry?.();
      const filePath = entry ? String(entry).split(':')[0] : '';
      if (filePath && devHandle?.files?.openFile) {
        if (rightMode === 'code') {
          devHandle.files.setOpen?.(false, {skipFocus: true});
          setRightMode(null);
        } else {
          devHandle.files.openFile(filePath, 'source');
        }
      }
    };

    function setDrawerExpanded(next) {
      drawerExpanded = !!next && rightMode === 'code';
      root.dataset.drawerExpanded = drawerExpanded ? 'true' : 'false';
      devHandle?.files?.setExpandedState?.(drawerExpanded);
      drawerResize.setActive(!!rightMode && !drawerExpanded && !matchMedia('(max-width:900px)').matches);
      return drawerExpanded;
    }

    function setRightMode(next) {
      rightMode = (next === 'detail' || next === 'code') ? next : null;
      const open = !!rightMode;
      if (rightMode !== 'code' && drawerExpanded) {
        drawerExpanded = false;
        root.dataset.drawerExpanded = 'false';
        devHandle?.files?.setExpandedState?.(false);
      }
      root.dataset.drawer = open ? 'open' : 'closed';
      root.dataset.drawerMode = rightMode || '';
      drawer.hidden = !open;
      detailPane.hidden = rightMode !== 'detail';
      codePane.hidden = rightMode !== 'code';
      headerSourceBtn.setAttribute('aria-pressed', String(rightMode === 'code'));
      drawerResize.setActive(open && !drawerExpanded && !matchMedia('(max-width:900px)').matches);
    }

    function setStageMode(next) {
      stageMode = next === 'assistant' ? 'assistant' : 'canvas';
      root.dataset.stage = stageMode;
      if (drawerExpanded) setDrawerExpanded(false);
      workbenchHost.hidden = stageMode !== 'canvas';
      assistantHost.hidden = stageMode !== 'assistant';
      // When switching away from canvas to assistant, hide node detail if it was open (keep code reader if open).
      if (stageMode === 'assistant' && rightMode === 'detail') {
        workbenchHandle?.closeDetail?.();
        setRightMode(null);
      }
    }

    function setSidebarTab(next) {
      sidebarTab = ['flows', 'sessions', 'files'].includes(next) ? next : 'flows';
      flowsPane.hidden = sidebarTab !== 'flows';
      sessionsPane.hidden = sidebarTab !== 'sessions';
      filesPane.hidden = sidebarTab !== 'files';
      for (const [key, btn] of [['flows', flowsBtn], ['sessions', sessionsBtn], ['files', filesBtn]]) {
        btn.classList.toggle('is-active', sidebarTab === key);
        btn.setAttribute('aria-pressed', String(sidebarTab === key));
      }
      if (sidebarTab === 'files') {
        devHandle?.files?.setFilter(navigation.search.value);
        devHandle?.files?.ensureTreeLoaded?.();
      } else if (sidebarTab === 'sessions') {
        devHandle?.setSessionFilter?.(navigation.search.value);
      } else if (sidebarTab === 'flows') {
        workbenchHandle?.filterFlows?.(navigation.search.value);
      }
    }

    flowsBtn.onclick = () => setSidebarTab('flows');
    sessionsBtn.onclick = () => setSidebarTab('sessions');
    filesBtn.onclick = () => setSidebarTab('files');

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
      openAssistant: () => { setSidebarTab('sessions'); setStageMode('assistant'); },
      openCanvas: () => setStageMode('canvas'),
      showPage: async page => {
        setStageMode('canvas');
        if (workbenchHandle?.showPage) await workbenchHandle.showPage(page);
      },
      navigate: async nextView => {
        if (disposed) return;
        if (['development', 'development_session', 'development_new'].includes(nextView?.kind)) {
          setSidebarTab('sessions');
          setStageMode('assistant');
          await devHandle?.navigate?.(nextView);
          return;
        }
        if (nextView?.panel === 'operators') {
          setSidebarTab('files');
          await devHandle?.files?.selectOperatorTarget?.(true);
          return;
        }
        setStageMode('canvas');
        if (nextView?.kind === 'run' && nextView?.run_id) {
          await workbenchHandle?.sync?.(currentContext, {force: false});
          await workbenchHandle?.showPage?.('monitor');
        } else if (nextView?.panel === 'monitor' || nextView?.kind === 'runs') {
          await workbenchHandle?.showPage?.('monitor');
        } else {
          await workbenchHandle?.showPage?.('flows');
          if (nextView?.entry) await workbenchHandle?.selectFlow?.(nextView.entry);
        }
      },
      sync: async (fresh, opts = {}) => {
        if (fresh) currentContext = fresh;
        await workbenchHandle?.sync?.(fresh, opts);
        if (opts.catalogChanged || opts.graphChanged || opts.force) {
          devHandle?.files?.ensureOperatorsLoaded?.({force: true});
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

    const devPromise = ForgeDevelopment.mount({
      main: assistantHost,
      api,
      workspaceId,
      view: devView,
      mountText,
      disposeReaders,
      drawerMode: true,
      externalSessionsHost: sessionsPane,
      externalFilesHost: filesPane,
      externalCodeHost: codePane,
      excludeFlows: true,
      openOperators: view?.panel === 'operators' || !!operatorTarget,
      operatorTarget,
      openNew,
      openSession,
      error,
      onCodeOpenChange: isOpen => {
        if (isOpen) {
          workbenchHandle?.closeDetail?.();
          setRightMode('code');
        } else if (rightMode === 'code') {
          setRightMode(null);
        }
      },
      onToggleCodeExpand: () => setDrawerExpanded(!drawerExpanded),
      onSelectFlow: async entry => {
        setStageMode('canvas');
        await workbenchHandle?.selectFlow?.(entry);
      },
      onShowFilesTab: () => setSidebarTab('files'),
      onSessionFocus: () => setStageMode('assistant'),
      onBackToCanvas: () => setStageMode('canvas'),
      onBadgeChange: count => { filesBadge.textContent = String(count); filesBadge.hidden = !count; },
      onTurnCompleted: async () => {
        try {
          const fresh = await api('context');
          if (!disposed && fresh) {
            currentContext = fresh;
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
        if (targetOp?.path && devHandle?.files?.openFile) {
          devHandle.files.openFile(targetOp.path, 'source');
        } else {
          await devHandle?.files?.selectOperatorTarget?.(targetOp || true);
        }
      },
      register: h => { workbenchHandle = h; },
      projectCheckState,
      continueInput,
      continueWithResult,
      onContext: fresh => { if (fresh) currentContext = fresh; onContext?.(fresh); },
      onPageChange,
      rememberView,
      projectSettings,
      renderProjectInfo,
      embedded: {
        hideFlowSidebar: true,
        flowListHost: flowsPane,
        nodeDetailHost: detailPane,
        sourceButton: headerSourceBtn,
        onFlowSelect: () => setStageMode('canvas'),
        onDetailOpen: () => {
          devHandle?.files?.setOpen?.(false, {skipFocus: true});
          setRightMode('detail');
        },
        onDetailClose: () => {
          if (rightMode === 'detail') setRightMode(null);
        },
        onAskAssistant: target => {
          workbenchHandle?.closeDetail?.();
          if (rightMode === 'detail') setRightMode(null);
          setSidebarTab('sessions');
          setStageMode('assistant');
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
        if (!disposed) assistantHost.replaceChildren(el('p', err?.message || '当前服务未启用开发助手。', 'op-muted dev-op-pad'));
      }),
      wbPromise,
    ]);
    if (disposed) return handle;

    devHandle?.files?.ensureOperatorsLoaded?.();

    if (initialDrawer === 'assistant' || isDevView) {
      setSidebarTab('sessions');
      setStageMode('assistant');
    } else if (!(context?.flows?.length || context?.project?.entries?.length)) {
      setSidebarTab('sessions');
      setStageMode('assistant');
    } else {
      setSidebarTab('flows');
      setStageMode('canvas');
    }
    return handle;
  }

  globalThis.ForgePipelineWorkspace = Object.freeze({mount});
})();
