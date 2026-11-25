// The DSH components render the rail and tree. DemiFlow owns their data and lifetime.
// CodeMirror 6 (self-hosted MIT bundle) renders the read-only Code Truth & Diff viewer.
(() => {
  'use strict';
  const {createRoot, react:{createElement:h}} = DshRuntime;
  const {TurnNavigator, Level, files} = DshWorkbench;
  const node = (tag, cls, text) => {
    const value = document.createElement(tag);
    if (cls) value.className = cls;
    if (text !== undefined && text !== null) value.textContent = text;
    return value;
  };
  const railText = (key, {turn} = {}) => ({
    'chat.turnNavigation.label':'会话轮次',
    'chat.turnNavigation.jump':'跳到第 '+turn+' 轮',
    'chat.turnNavigation.jumpLoad':'加载第 '+turn+' 轮',
    'chat.turnNavigation.turn':'第 '+turn+' 轮'
  })[key];

  function turnRail({host, timeline, flow, navigate}) {
    const slot = node('div', 'development-rail-slot');
    const picker = node('select', 'development-turn-picker');
    picker.setAttribute('aria-label', '跳到会话轮次');
    picker.hidden = true;
    host.append(slot);
    const root = createRoot(slot);
    let rows = [], frame = 0, disposed = false, lastKey = '', lastRenderKey = '', items = [];
    let controller;
    const land = item => navigate(rows[item.turn - 1]);
    picker.onchange = () => land(items[picker.selectedIndex]);
    function paint() {
      frame = 0;
      if (disposed) return;
      if (!rows.length) {
        if (lastRenderKey !== 'empty') {
          lastRenderKey = 'empty';
          root.render(h(TurnNavigator, {items: [], activeTurn: null, busyTurn: null, onNavigate: land, t: railText}));
        }
        return;
      }
      const top = timeline.getBoundingClientRect().top;
      const loaded = new Map();
      let activeId;
      for (const section of flow.children) {
        if (!section.childElementCount || !section.offsetHeight) continue;
        const id = section.dataset.turnId;
        loaded.set(id, section);
        if (!activeId || section.getBoundingClientRect().top <= top + 12) activeId = id;
      }
      if (timeline.scrollTop > 0 && timeline.scrollHeight - timeline.clientHeight - timeline.scrollTop < 2)
        activeId = [...loaded.keys()].at(-1);
      const key = [...loaded.keys()].join('|');
      if (lastKey !== key || items.length !== rows.length) {
        lastKey = key;
        items = rows.map((row, i) => ({
          turn:i + 1, prompt:row.title, response:'',
          anchor:{kind:loaded.has(row.id) ? 'loaded' : 'unloaded'}
        }));
      }
      const index = rows.findIndex(row => row.id === activeId);
      picker.selectedIndex = index;
      const renderKey = key + ':' + index + ':' + items.length;
      if (renderKey === lastRenderKey) return;
      lastRenderKey = renderKey;
      root.render(h(TurnNavigator, {items, activeTurn:index < 0 ? null : index + 1,
        busyTurn:null, onNavigate:land, t:railText}));
    }
    function update() {
      if (!disposed && !frame) frame = requestAnimationFrame(paint);
    }
    async function load(api, session, scoped) {
      // Each server page contains at most 20 titles. Session policy caps turns at 128.
      const request = controller = {};
      const known = new Map();
      for (let offset = 0; offset !== null && offset < 140;) {
        const page = await api('dev-project-turns', {offset,limit:20,...(scoped ? {session_id:session} : {})});
        if (disposed || request !== controller) return;
        for (const row of page.turns) if (row.session === session && row.sequence) known.set(row.id, row);
        if (!(page.next_offset > offset)) break;
        offset = page.next_offset;
      }
      rows = [...known.values()].sort((a,b) => a.sequence - b.sequence);
      items = []; lastKey = ''; lastRenderKey = '';
      picker.replaceChildren(...rows.map((row, i) => new Option('第 '+(i+1)+' 轮 · '+(row.title || '归档记录'), row.id)));
      picker.hidden = rows.length < 2;
      update();
    }
    function clear() {
      controller = {};
      rows = []; items = []; lastKey = ''; lastRenderKey = '';
      picker.replaceChildren(); picker.hidden = true;
      update();
    }
    return {picker, update, load, clear, dispose() {
      disposed = true; cancelAnimationFrame(frame); root.unmount();
    }};
  }

  function diffStatsFromUnified(text) {
    if (typeof text !== 'string' || !text) return {added: null, removed: null};
    let added = 0, removed = 0;
    for (const line of text.split('\n')) {
      if (line.startsWith('+') && !line.startsWith('+++')) added++;
      else if (line.startsWith('-') && !line.startsWith('---')) removed++;
    }
    return {added, removed};
  }

  function splitPath(rel) {
    const clean = String(rel || '').replace(/^\/+/, '');
    const idx = clean.lastIndexOf('/');
    return idx < 0 ? {name: clean, dir: ''} : {name: clean.slice(idx + 1), dir: clean.slice(0, idx)};
  }

  const IGNORED_DIRS = new Set(['__pycache__', '.pytest_cache', '.mypy_cache', 'node_modules', 'logs']);
  const IGNORED_SUFFIXES = ['.pyc', '.pyo'];
  function ignoredPath(rel) {
    const clean = String(rel || '').replace(/^\/+/, '');
    if (!clean) return true;
    const parts = clean.split('/');
    if (parts.some(p => !p || p.startsWith('.') || IGNORED_DIRS.has(p) || p.endsWith('.lance'))) return true;
    const lower = parts[parts.length - 1].toLowerCase();
    return IGNORED_SUFFIXES.some(ext => lower.endsWith(ext));
  }

  function chevronIcon() {
    const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
    svg.setAttribute('viewBox', '0 0 16 16');
    svg.setAttribute('aria-hidden', 'true');
    svg.classList.add('dev-chevron');
    const p = document.createElementNS(svg.namespaceURI, 'path');
    p.setAttribute('d', 'M5 6l3 3.5L11 6');
    svg.append(p);
    return svg;
  }

  function filePanel({layout, filesHost, operatorsHost, toggle, api, iconButton, mountText, disposeReaders, project, onReference, onRework, onRequestReview, onShowFilesTab, onBadgeChange}) {
    let disposed = false;
    let levels = Object.create(null), expanded = [], treeLoaded = false, projectRootPath = '';
    let gitChanges = [], gitBranch = '', gitIsRepo = false;
    const observedChanges = new Map(); // path -> {path, status, added, removed, diff, sha256}
    let filterTerm = '';
    let paneMode = 'file'; // 'file' | 'operator'

    // Per-project approved review state & preserved history (persisted in localStorage).
    const projectKey = String(project?.workspace_id || project?.root || 'default');
    const approvalStorageKey = 'forge-dev-approvals:' + projectKey;
    const approvedMap = new Map(); // path -> fingerprint
    let approvedHistory = []; // [{path, fingerprint, status, added, removed, approvedAt}]
    let historyOpen = false;
    try {
      const raw = JSON.parse(localStorage.getItem(approvalStorageKey) || 'null');
      if (raw && typeof raw === 'object') {
        if (raw.map && typeof raw.map === 'object') {
          for (const [k, v] of Object.entries(raw.map)) {
            if (typeof k === 'string' && typeof v === 'string') approvedMap.set(k, v);
          }
        }
        if (Array.isArray(raw.history)) {
          approvedHistory = raw.history.filter(r => r && typeof r.path === 'string').slice(0, 100);
        }
      }
    } catch {}
    function persistApprovals() {
      try {
        localStorage.setItem(approvalStorageKey, JSON.stringify({
          map: Object.fromEntries(approvedMap.entries()),
          history: approvedHistory.slice(0, 100),
        }));
      } catch {}
    }
    function changeFingerprint(item) {
      if (!item) return '';
      const cached = fileCache.get(item.path);
      const sha = item.sha256 || cached?.sha256 || '';
      if (sha) return 'sha:' + sha;
      return [item.status || 'M', item.added ?? '', item.removed ?? '', item.diff ? item.diff.length : ''].join(':');
    }
    function isApprovedItem(item) {
      if (!item) return false;
      const fp = changeFingerprint(item);
      return Boolean(fp) && approvedMap.get(item.path) === fp;
    }

    // Open tabs & active code view state
    const openTabs = []; // array of relative paths (no leading slash), max 8
    const fileCache = new Map(); // path -> {text, original, status, added, removed, isGit, unifiedDiff, sha256}
    let activePath = null;
    let viewMode = 'source'; // 'diff' | 'source'
    let viewerHandle = null;
    let loadEpoch = 0;
    let latestChecks = {native_flow: null, project_layout: null, review: null, reviewDetails: []};
    let activeCheckKey = null;

    // === LEFT SIDEBAR: Project tree (top) + Changed files (bottom, collapsible & resizable split) ===
    const explorer = node('div', 'dev-file-explorer');
    let changedOpen = false, changedUserToggled = false, prevPendingCount = 0;
    let treeOpen = true, treeSearchOpen = false;
    let treeSearchTimer = null, treeSearchEpoch = 0;
    let splitRatio = 0.6;
    try {
      const saved = Number(localStorage.getItem('forge-dev-explorer-tree-split'));
      if (Number.isFinite(saved) && saved >= 0.15 && saved <= 0.85) splitRatio = saved;
    } catch {}

    const changedSection = node('div', 'dev-changed-section');
    changedSection.hidden = true;
    const changedHeader = node('div', 'dev-section-header');
    const changedToggle = node('button', 'dev-section-toggle');
    changedToggle.type = 'button';
    changedToggle.setAttribute('aria-expanded', 'false');
    const changedTitle = node('span', 'dev-section-title', '待审核变更');
    const changedCount = node('span', 'dev-section-count', '0');
    changedToggle.append(chevronIcon(), changedTitle, changedCount);
    const changedHeaderActions = node('div', 'dev-section-actions');
    const approveAllBtn = node('button', 'dev-approve-all-btn', '全部通过');
    approveAllBtn.type = 'button';
    approveAllBtn.title = '标记当前全部改动已审核通过（保留修改历史）';
    approveAllBtn.hidden = true;
    const changedRefresh = iconButton('刷新变更列表', 'M20 10a8 8 0 1 0-1 7M20 4v6h-6');
    changedHeaderActions.append(approveAllBtn, changedRefresh);
    changedHeader.append(changedToggle, changedHeaderActions);
    const changedList = node('div', 'dev-changed-list');
    const historyWrap = node('div', 'dev-approved-history');
    historyWrap.hidden = true;
    const historyToggle = node('button', 'dev-history-toggle');
    historyToggle.type = 'button';
    historyToggle.setAttribute('aria-expanded', 'false');
    const historyLabel = node('span', null, '已通过历史');
    const historyCount = node('span', 'dev-section-count', '0');
    historyToggle.append(chevronIcon(), historyLabel, historyCount);
    const historyList = node('div', 'dev-history-list');
    historyList.hidden = true;
    historyToggle.onclick = () => {
      historyOpen = !historyOpen;
      historyToggle.setAttribute('aria-expanded', String(historyOpen));
      historyToggle.classList.toggle('is-open', historyOpen);
      historyList.hidden = !historyOpen;
    };
    historyWrap.append(historyToggle, historyList);
    changedSection.append(changedHeader, changedList, historyWrap);

    const splitHandle = node('div', 'dev-split-resizer');
    splitHandle.setAttribute('role', 'separator');
    splitHandle.setAttribute('aria-orientation', 'horizontal');
    splitHandle.setAttribute('aria-label', '调整项目目录与待审核变更高度');
    splitHandle.tabIndex = 0;
    splitHandle.hidden = true;

    const treeSection = node('div', 'dev-tree-section');
    const treeHeader = node('div', 'dev-section-header');
    const treeToggle = node('button', 'dev-section-toggle');
    treeToggle.type = 'button';
    treeToggle.setAttribute('aria-expanded', 'true');
    const treeTitle = node('span', 'dev-section-title', '项目目录');
    treeToggle.append(chevronIcon(), treeTitle);
    const treeHeaderActions = node('div', 'dev-section-actions');
    const treeCreateToggle = iconButton('新建文件', 'M12 5v14M5 12h14');
    treeCreateToggle.setAttribute('aria-expanded', 'false');
    const treeSearchToggle = iconButton('搜索项目目录', 'M11 4a7 7 0 1 0 0 14a7 7 0 1 0 0-14M16.2 16.2L20 20');
    treeSearchToggle.setAttribute('aria-expanded', 'false');
    const treeRefresh = iconButton('刷新项目目录', 'M20 10a8 8 0 1 0-1 7M20 4v6h-6');
    treeHeaderActions.append(treeCreateToggle, treeSearchToggle, treeRefresh);
    treeHeader.append(treeToggle, treeHeaderActions);
    const treeCreateWrap = node('div', 'dev-tree-create-wrap');
    treeCreateWrap.hidden = true;
    const treeCreateBar = node('div', 'dev-tree-search');
    const treeCreateInput = document.createElement('input');
    treeCreateInput.type = 'text';
    treeCreateInput.className = 'dev-tree-search-input';
    treeCreateInput.placeholder = '新建文件，如 .env 或 configs/a.json';
    treeCreateInput.setAttribute('aria-label', '新建文件路径');
    const treeCreateSubmit = node('button', 'dev-tree-create-submit', '创建');
    treeCreateSubmit.type = 'button';
    const treeCreateCancel = node('button', 'dev-tree-search-clear', '×');
    treeCreateCancel.type = 'button';
    treeCreateCancel.setAttribute('aria-label', '取消新建文件');
    treeCreateBar.append(treeCreateInput, treeCreateSubmit, treeCreateCancel);
    const treeCreateError = node('div', 'dev-tree-create-error');
    treeCreateError.hidden = true;
    treeCreateWrap.append(treeCreateBar, treeCreateError);
    const treeSearchBar = node('div', 'dev-tree-search');
    treeSearchBar.hidden = true;
    const treeSearchInput = document.createElement('input');
    treeSearchInput.type = 'search';
    treeSearchInput.className = 'dev-tree-search-input';
    treeSearchInput.placeholder = '搜索项目文件…';
    treeSearchInput.setAttribute('aria-label', '搜索项目文件');
    const treeSearchClear = node('button', 'dev-tree-search-clear', '×');
    treeSearchClear.type = 'button';
    treeSearchClear.setAttribute('aria-label', '清除目录搜索');
    treeSearchClear.hidden = true;
    treeSearchBar.append(treeSearchInput, treeSearchClear);
    const treeBody = node('div', 'dev-tree-body ' + files.body);
    const treeSearchResults = node('div', 'dev-search-results');
    treeSearchResults.hidden = true;
    const treeHost = node('ul', files.level);
    treeBody.append(treeSearchResults, treeHost);
    treeSection.append(treeHeader, treeCreateWrap, treeSearchBar, treeBody);
    explorer.append(treeSection, splitHandle, changedSection);
    if (filesHost) filesHost.append(explorer);

    let treeCreateOpen = false, creatingFile = false;
    function applyExplorerSplit() {
      const hasChanges = !changedSection.hidden;
      changedToggle.setAttribute('aria-expanded', String(changedOpen));
      treeToggle.setAttribute('aria-expanded', String(treeOpen));
      changedList.hidden = !changedOpen;
      historyWrap.hidden = !changedOpen || approvedHistory.length === 0;
      treeCreateWrap.hidden = !treeOpen || !treeCreateOpen;
      treeSearchBar.hidden = !treeOpen || !treeSearchOpen;
      treeBody.hidden = !treeOpen;
      changedSection.classList.toggle('is-collapsed', !changedOpen);
      treeSection.classList.toggle('is-collapsed', !treeOpen);
      const canResize = hasChanges && changedOpen && treeOpen;
      splitHandle.hidden = !canResize;
      splitHandle.setAttribute('aria-valuenow', String(Math.round(splitRatio * 100)));
      if (!hasChanges) {
        changedSection.style.flex = '';
        treeSection.style.flex = treeOpen ? '1 1 0%' : '0 0 auto';
      } else if (treeOpen && changedOpen) {
        const pct = Math.round(splitRatio * 1000) / 10;
        treeSection.style.flex = '0 0 ' + pct + '%';
        changedSection.style.flex = '1 1 0%';
      } else if (treeOpen && !changedOpen) {
        treeSection.style.flex = '1 1 0%';
        changedSection.style.flex = '0 0 auto';
      } else if (!treeOpen && changedOpen) {
        treeSection.style.flex = '0 0 auto';
        changedSection.style.flex = '1 1 0%';
      } else {
        treeSection.style.flex = '0 0 auto';
        changedSection.style.flex = '0 0 auto';
      }
    }
    function setSplitRatio(next) {
      splitRatio = Math.max(0.15, Math.min(0.85, next));
      try { localStorage.setItem('forge-dev-explorer-tree-split', String(splitRatio)); } catch {}
      applyExplorerSplit();
    }
    changedToggle.onclick = () => { changedUserToggled = true; changedOpen = !changedOpen; applyExplorerSplit(); };
    treeToggle.onclick = () => { treeOpen = !treeOpen; applyExplorerSplit(); };
    splitHandle.ondblclick = () => setSplitRatio(0.6);
    splitHandle.onkeydown = e => {
      if (e.key === 'ArrowUp') { e.preventDefault(); setSplitRatio(splitRatio - 0.05); }
      else if (e.key === 'ArrowDown') { e.preventDefault(); setSplitRatio(splitRatio + 0.05); }
      else if (e.key === 'Home') { e.preventDefault(); setSplitRatio(0.18); }
      else if (e.key === 'End') { e.preventDefault(); setSplitRatio(0.82); }
    };
    splitHandle.onpointerdown = e => {
      if (e.button !== 0) return;
      e.preventDefault();
      splitHandle.setPointerCapture(e.pointerId);
      splitHandle.classList.add('is-dragging');
      const rect = explorer.getBoundingClientRect();
      const move = ev => {
        if (rect.height <= 80) return;
        setSplitRatio((ev.clientY - rect.top) / rect.height);
      };
      const end = () => {
        splitHandle.classList.remove('is-dragging');
        splitHandle.removeEventListener('pointermove', move);
        splitHandle.removeEventListener('pointerup', end);
        splitHandle.removeEventListener('pointercancel', end);
      };
      splitHandle.addEventListener('pointermove', move);
      splitHandle.addEventListener('pointerup', end);
      splitHandle.addEventListener('pointercancel', end);
    };
    applyExplorerSplit();

    const treeRoot = createRoot(treeHost);
    const treeText = (key, args) => ({empty:'空目录',loading:'正在读取…',truncated:'加载更多文件',
      'entry.other':'文件','error.unavailable':args?.message || '读取失败'})[key] || '读取失败';

    // === RIGHT PANE: Code Truth Panel (CodeMirror 6) ===
    const pane = node('aside', 'development-files');
    pane.setAttribute('aria-label', '代码面板');
    pane.hidden = true;

    const tabBar = node('div', 'dev-code-tabbar');
    const tabList = node('div', 'dev-code-tabs');
    const closePanelBtn = iconButton('关闭代码面板', 'M6 6l12 12M18 6L6 18');
    tabBar.append(tabList, closePanelBtn);

    const forgeStrip = node('div', 'dev-forge-strip');
    forgeStrip.hidden = true;
    const forgeDetail = node('div', 'dev-forge-detail');
    forgeDetail.hidden = true;

    const toolbar = node('div', 'dev-code-toolbar');
    toolbar.hidden = true;
    const pathMeta = node('div', 'dev-code-pathmeta');
    const pathText = node('span', 'dev-code-path');
    const statusPill = node('span', 'dev-code-status');
    pathMeta.append(pathText, statusPill);

    const toolActions = node('div', 'dev-code-actions');
    const modeSwitch = node('div', 'dev-code-modes');
    const diffModeBtn = node('button', 'dev-mode-btn', '变更 Diff');
    diffModeBtn.type = 'button';
    const sourceModeBtn = node('button', 'dev-mode-btn', '完整源码');
    sourceModeBtn.type = 'button';
    modeSwitch.append(diffModeBtn, sourceModeBtn);

    const approveFileBtn = node('button', 'dev-action-btn dev-action-approve', '审核通过');
    approveFileBtn.type = 'button';
    approveFileBtn.title = '标记当前文件改动已审核通过（保留修改历史）';
    approveFileBtn.hidden = true;
    const formatJsonBtn = node('button', 'dev-action-btn', '格式化');
    formatJsonBtn.type = 'button';
    formatJsonBtn.title = '格式化 JSON';
    formatJsonBtn.hidden = true;
    const saveBtn = node('button', 'dev-action-btn dev-action-save', '保存');
    saveBtn.type = 'button';
    saveBtn.title = '保存修改 (Ctrl/⌘+S)';
    saveBtn.hidden = true;
    const revertBtn = node('button', 'dev-action-btn', '还原');
    revertBtn.type = 'button';
    revertBtn.title = '放弃未保存的修改';
    revertBtn.hidden = true;
    const refBtn = node('button', 'dev-action-btn dev-action-primary', '引用到对话');
    refBtn.type = 'button';
    const searchBtn = iconButton('在代码中搜索 (Ctrl+F)', 'M11 4a7 7 0 1 0 0 14a7 7 0 1 0 0-14M16.2 16.2L20 20');
    const copyBtn = node('button', 'dev-action-btn', '复制');
    copyBtn.type = 'button';
    const reloadFileBtn = iconButton('刷新当前文件', 'M20 10a8 8 0 1 0-1 7M20 4v6h-6');
    toolActions.append(modeSwitch, formatJsonBtn, saveBtn, revertBtn, approveFileBtn, refBtn, searchBtn, copyBtn, reloadFileBtn);
    toolbar.append(pathMeta, toolActions);

    const viewport = node('div', 'dev-code-viewport');
    const operatorPane = node('div', 'dev-operator-pane');
    operatorPane.hidden = true;
    pane.append(tabBar, forgeStrip, forgeDetail, toolbar, viewport, operatorPane);
    layout.append(pane);
    toggle.setAttribute('aria-expanded', 'false');

    function setPaneMode(mode) {
      paneMode = mode === 'operator' ? 'operator' : 'file';
      const isOp = paneMode === 'operator';
      tabBar.hidden = isOp;
      forgeStrip.hidden = isOp || (!latestChecks.native_flow && !latestChecks.project_layout && !latestChecks.review);
      forgeDetail.hidden = isOp || !activeCheckKey;
      toolbar.hidden = isOp || !activePath;
      viewport.hidden = isOp;
      operatorPane.hidden = !isOp;
      if (isOp) destroyViewer();
    }

    const resize = globalThis.ForgeNavigation?.resizable
      ? ForgeNavigation.resizable({layout, sidebar:pane, label:'代码面板', right:true, cssVar:'--side-detail-width', minWidth:320, maxWidth:860, minMain:320, active:false})
      : null;

    function mergedChanges() {
      const map = new Map();
      for (const item of gitChanges) {
        map.set(item.path, {...item, source: 'git'});
      }
      for (const [p, obs] of observedChanges.entries()) {
        const prev = map.get(p);
        if (prev) {
          map.set(p, {...prev, diff: obs.diff || prev.diff, sha256: prev.sha256 || obs.sha256 || '', source: 'both'});
        } else {
          map.set(p, {...obs, source: 'session'});
        }
      }
      return [...map.values()].sort((a, b) => a.path.localeCompare(b.path));
    }

    function changeFor(rel) {
      const clean = String(rel || '').replace(/^\/+/, '');
      const gitHit = gitChanges.find(item => item.path === clean);
      const obsHit = observedChanges.get(clean);
      if (!gitHit && !obsHit) return null;
      return {...(gitHit || {}), ...(obsHit || {}),
        status: gitHit?.status || obsHit?.status || 'M',
        added: gitHit?.added ?? obsHit?.added ?? null,
        removed: gitHit?.removed ?? obsHit?.removed ?? null,
        sha256: gitHit?.sha256 || obsHit?.sha256 || '',
        diff: obsHit?.diff || null};
    }

    function formatShortTime(ts) {
      if (!Number.isFinite(ts)) return '';
      try {
        const d = new Date(ts);
        return String(d.getHours()).padStart(2, '0') + ':' + String(d.getMinutes()).padStart(2, '0');
      } catch {
        return '';
      }
    }

    function approveItems(items, {advance = false} = {}) {
      if (!Array.isArray(items) || !items.length) return;
      const now = Date.now();
      for (const item of items) {
        if (!item?.path) continue;
        const fp = changeFingerprint(item);
        if (!fp) continue;
        approvedMap.set(item.path, fp);
        approvedHistory = [
          {
            path: item.path,
            fingerprint: fp,
            status: item.status || 'M',
            added: Number.isFinite(item.added) ? item.added : null,
            removed: Number.isFinite(item.removed) ? item.removed : null,
            approvedAt: now,
          },
          ...approvedHistory.filter(r => !(r.path === item.path && r.fingerprint === fp)),
        ].slice(0, 100);
      }
      persistApprovals();
      const remaining = mergedChanges().filter(item => !isApprovedItem(item));
      renderChangedList();
      renderTabs();
      if (advance && remaining.length) {
        openFile(remaining[0].path, 'diff');
      } else if (activePath) {
        const ch = changeFor(activePath);
        approveFileBtn.hidden = !(ch && !isApprovedItem(ch));
      }
    }

    function destroyViewer() {
      if (viewerHandle) {
        try { viewerHandle.destroy(); } catch {}
        viewerHandle = null;
      }
    }

    function renderChangedList() {
      const all = mergedChanges();
      const pending = all.filter(item => !isApprovedItem(item));
      if (typeof onBadgeChange === 'function') onBadgeChange(pending.length);
      changedCount.textContent = String(pending.length);
      approveAllBtn.hidden = pending.length === 0;
      const term = filterTerm.trim().toLocaleLowerCase();
      const visiblePending = term ? pending.filter(item => item.path.toLocaleLowerCase().includes(term)) : pending;
      const visibleHistory = term ? approvedHistory.filter(item => item.path.toLocaleLowerCase().includes(term)) : approvedHistory;
      changedSection.hidden = pending.length === 0 && approvedHistory.length === 0;
      if (pending.length === 0 && prevPendingCount > 0) {
        changedOpen = false;
        changedUserToggled = false;
      } else if (pending.length > 0 && prevPendingCount === 0) {
        changedOpen = true;
        changedUserToggled = false;
      } else if (!changedUserToggled) {
        changedOpen = pending.length > 0;
      }
      prevPendingCount = pending.length;
      historyCount.textContent = String(approvedHistory.length);
      applyExplorerSplit();
      changedList.replaceChildren();
      if (visiblePending.length) {
        for (const item of visiblePending) {
          const btn = node('button', 'dev-changed-item');
          btn.type = 'button';
          btn.dataset.path = item.path;
          btn.title = item.path;
          btn.setAttribute('aria-current', activePath === item.path ? 'true' : 'false');
          const badge = node('span', 'dev-status-tag dev-status-' + (item.status || 'M').toLowerCase(), item.status || 'M');
          const {name, dir} = splitPath(item.path);
          const textWrap = node('span', 'dev-changed-text');
          textWrap.append(node('span', 'dev-changed-name', name));
          if (dir) textWrap.append(node('span', 'dev-changed-dir', dir));
          const stats = node('span', 'dev-changed-stats');
          if (Number.isFinite(item.added) && item.added > 0) stats.append(node('span', 'dev-stat-add', '+' + item.added));
          if (Number.isFinite(item.removed) && item.removed > 0) stats.append(node('span', 'dev-stat-del', '-' + item.removed));
          btn.append(badge, textWrap, stats);
          btn.onclick = () => openFile(item.path, 'diff');
          changedList.append(btn);
        }
      } else if (pending.length === 0 && approvedHistory.length > 0) {
        changedList.append(node('div', 'dev-changed-empty', '当前改动已全部审核通过'));
      }
      historyList.replaceChildren();
      for (const item of visibleHistory) {
        const btn = node('button', 'dev-changed-item dev-history-item');
        btn.type = 'button';
        btn.dataset.path = item.path;
        btn.title = item.path;
        btn.setAttribute('aria-current', activePath === item.path ? 'true' : 'false');
        const badge = node('span', 'dev-status-tag dev-status-' + (item.status || 'M').toLowerCase(), item.status || 'M');
        const {name, dir} = splitPath(item.path);
        const textWrap = node('span', 'dev-changed-text');
        textWrap.append(node('span', 'dev-changed-name', name));
        if (dir) textWrap.append(node('span', 'dev-changed-dir', dir));
        const meta = node('span', 'dev-changed-stats');
        if (Number.isFinite(item.added) && item.added > 0) meta.append(node('span', 'dev-stat-add', '+' + item.added));
        if (Number.isFinite(item.removed) && item.removed > 0) meta.append(node('span', 'dev-stat-del', '-' + item.removed));
        const ts = formatShortTime(item.approvedAt);
        if (ts) meta.append(node('span', 'dev-history-time', ts));
        btn.append(badge, textWrap, meta);
        btn.onclick = () => openFile(item.path, 'diff');
        historyList.append(btn);
      }
      if (!activePath && !pane.hidden) renderEmptyState();
    }

    function statusSvg(tone) {
      const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
      svg.setAttribute('viewBox', '0 0 16 16');
      svg.setAttribute('aria-hidden', 'true');
      svg.classList.add('dev-forge-icon');
      const d = ({
        ok: 'M3.2 8.4l3.1 3.1 6.5-6.8',
        warn: 'M8 4.2v4.6M8 11.4h.01',
        bad: 'M4.5 4.5l7 7M11.5 4.5l-7 7',
        muted: 'M4 8h.01M8 8h.01M12 8h.01',
      })[tone] || 'M8 4.2v4.6M8 11.4h.01';
      const path = document.createElementNS(svg.namespaceURI, 'path');
      path.setAttribute('d', d);
      svg.append(path);
      return svg;
    }

    function renderForgeStrip() {
      const items = [];
      const checkLabel = status => ({passed:'通过', failed:'发现问题', incomplete:'待核对', not_applicable:'不适用'})[status] || status;
      const checkTone = status => ({passed:'ok', failed:'bad', incomplete:'warn', not_applicable:'muted'})[status] || 'muted';
      if (latestChecks.native_flow && latestChecks.native_flow.status !== 'not_applicable') {
        const c = latestChecks.native_flow;
        const diags = c.diagnostics || [];
        const onlyUnregisteredFl10 = c.status === 'incomplete' && (c.inspected_count || 0) > 0 && diags.length > 0 && diags.every(d => d.rule === 'FL-10');
        const lines = diags.map(d => [d.rule, d.path + (d.line ? ':' + d.line : ''), d.detail].filter(Boolean).join(' · '));
        const omitted = (c.diagnostic_count || 0) - lines.length;
        if (omitted > 0) lines.push('另有 ' + omitted + ' 项诊断');
        items.push({key: 'native_flow', short: '数据流', label: '原生数据流检查',
          text: onlyUnregisteredFl10 ? '变更流通过 (有未登记提示)' : checkLabel(c.status),
          tone: onlyUnregisteredFl10 ? 'ok' : checkTone(c.status),
          lines: lines.length ? lines : ['静态数据流与 SDK 契约检查通过。']});
      }
      if (latestChecks.project_layout && latestChecks.project_layout.status !== 'not_applicable') {
        const c = latestChecks.project_layout;
        const lines = (c.diagnostics || []).map(d => [d.rule, d.path + (d.line ? ':' + d.line : ''), d.detail].filter(Boolean).join(' · '));
        items.push({key: 'project_layout', short: '目录', label: '项目目录检查', text: checkLabel(c.status), tone: checkTone(c.status),
          lines: lines.length ? lines : ['业务目录与角色规范检查通过。']});
      }
      if (latestChecks.review) {
        const r = latestChecks.review;
        const rText = ({passed:'已通过', invalid:'需修改', incomplete:'未完成', running:'评审中'})[r] || r;
        const rTone = ({passed:'ok', invalid:'bad', incomplete:'warn', running:'muted'})[r] || 'muted';
        const lines = latestChecks.reviewDetails.length
          ? latestChecks.reviewDetails
          : [r === 'passed' ? '独立只读评审通过。' : '点击聊天流中的「独立评审」查看完整记录。'];
        items.push({key: 'review', short: '评审', label: '独立评审', text: rText, tone: rTone, lines});
      }
      forgeStrip.replaceChildren();
      forgeStrip.hidden = items.length === 0;
      if (!items.length) { forgeDetail.hidden = true; return; }
      if (activeCheckKey && !items.some(i => i.key === activeCheckKey)) activeCheckKey = null;
      forgeStrip.append(node('span', 'dev-forge-strip-label', 'Forge'));
      for (const item of items) {
        const pill = node('button', 'dev-forge-pill dev-forge-' + item.tone + (activeCheckKey === item.key ? ' is-open' : ''));
        pill.type = 'button';
        pill.setAttribute('aria-expanded', String(activeCheckKey === item.key));
        pill.setAttribute('aria-label', item.label + '：' + item.text);
        pill.title = item.label + ' · ' + item.text + '\n点击展开详情';
        pill.append(statusSvg(item.tone), node('span', 'dev-forge-pill-text', item.short));
        pill.onclick = () => {
          activeCheckKey = activeCheckKey === item.key ? null : item.key;
          renderForgeStrip();
        };
        forgeStrip.append(pill);
      }
      const selected = items.find(i => i.key === activeCheckKey);
      forgeDetail.replaceChildren();
      if (selected) {
        const head = node('div', 'dev-forge-detail-head');
        head.append(node('strong', null, selected.label + ' · ' + selected.text));
        const headBtns = node('div', 'dev-forge-detail-actions');
        if (typeof onRework === 'function' && (selected.tone === 'bad' || selected.tone === 'warn') && selected.lines.length) {
          const reworkBtn = node('button', 'dev-forge-rework', selected.tone === 'bad' ? '带问题打回修改' : '交给助手修复');
          reworkBtn.type = 'button';
          reworkBtn.onclick = () => {
            const prompt = '请针对以下 Forge「' + selected.label + '」诊断逐项排查修复，并运行项目 pytest 验证：\n' + selected.lines.map(l => '- ' + l).join('\n');
            onRework(prompt);
          };
          headBtns.append(reworkBtn);
        }
        const closeDetail = node('button', 'dev-forge-detail-close', '收起');
        closeDetail.type = 'button';
        closeDetail.onclick = () => { activeCheckKey = null; renderForgeStrip(); };
        headBtns.append(closeDetail);
        head.append(headBtns);
        const list = node('ul', 'dev-forge-detail-list');
        for (const line of selected.lines) list.append(node('li', null, line));
        forgeDetail.append(head, list);
        forgeDetail.hidden = false;
      } else {
        forgeDetail.hidden = true;
      }
    }

    function hasUnsavedDraft(rel) {
      const entry = rel ? fileCache.get(rel) : null;
      return Boolean(entry && typeof entry.draft === 'string' && entry.draft !== (entry.modified ?? ''));
    }

    function renderTabs() {
      tabList.replaceChildren();
      for (const rel of openTabs) {
        const tab = node('div', 'dev-code-tab' + (rel === activePath ? ' is-active' : ''));
        tab.dataset.path = rel;
        const selectBtn = node('button', 'dev-code-tab-btn');
        selectBtn.type = 'button';
        selectBtn.title = rel;
        const dirty = hasUnsavedDraft(rel);
        const ch = changeFor(rel);
        if (dirty) selectBtn.append(node('span', 'dev-code-tab-dot dev-status-dirty'));
        else if (ch) selectBtn.append(node('span', 'dev-code-tab-dot dev-status-' + (ch.status || 'M').toLowerCase()));
        selectBtn.append(node('span', 'dev-code-tab-name', splitPath(rel).name));
        selectBtn.onclick = () => openFile(rel);
        const closeBtn = node('button', 'dev-code-tab-close', '×');
        closeBtn.type = 'button';
        closeBtn.setAttribute('aria-label', '关闭标签 ' + splitPath(rel).name);
        closeBtn.onclick = e => { e.stopPropagation(); closeTab(rel); };
        tab.append(selectBtn, closeBtn);
        tabList.append(tab);
      }
    }

    function renderEmptyState() {
      destroyViewer();
      toolbar.hidden = true;
      viewport.replaceChildren();
      const empty = node('div', 'dev-code-empty');
      empty.append(node('h3', null, '代码真相视图'));
      empty.append(node('p', null, '查看并轻量编辑项目源码、配置文件与变更 Diff，或围绕待改对象在左侧对话中驱动 Forge 修改。'));
      const changes = mergedChanges();
      if (changes.length) {
        const box = node('div', 'dev-code-empty-changes');
        box.append(node('div', 'dev-code-empty-label', '近期改动 (' + changes.length + ')'));
        for (const item of changes.slice(0, 8)) {
          const btn = node('button', 'dev-changed-item');
          btn.type = 'button';
          btn.append(node('span', 'dev-status-tag dev-status-' + (item.status || 'M').toLowerCase(), item.status || 'M'));
          const {name, dir} = splitPath(item.path);
          const wrap = node('span', 'dev-changed-text');
          wrap.append(node('span', 'dev-changed-name', name));
          if (dir) wrap.append(node('span', 'dev-changed-dir', dir));
          btn.append(wrap);
          btn.onclick = () => openFile(item.path, 'diff');
          box.append(btn);
        }
        empty.append(box);
      }
      if (typeof onShowFilesTab === 'function') {
        const browse = node('button', 'dev-action-btn', '在左侧浏览项目文件');
        browse.type = 'button';
        browse.onclick = () => onShowFilesTab();
        empty.append(browse);
      }
      viewport.append(empty);
    }

    let savingFile = false;
    const MAX_JSON_FORMAT_CHARS = 2 * 1024 * 1024;
    function isJsonPath(rel) {
      return typeof rel === 'string' && /\.json$/i.test(rel.trim());
    }
    function formatJsonText(raw) {
      if (typeof raw !== 'string' || !raw.trim() || raw.length > MAX_JSON_FORMAT_CHARS) return null;
      try {
        return JSON.stringify(JSON.parse(raw), null, 2) + '\n';
      } catch {
        return null;
      }
    }
    function isCompactJsonText(raw) {
      if (typeof raw !== 'string') return false;
      const trimmed = raw.trim();
      if (!trimmed || trimmed.includes('\n')) return false;
      const formatted = formatJsonText(trimmed);
      return formatted !== null && formatted.trim() !== trimmed;
    }
    function displayTextFor(entry) {
      if (!entry) return '';
      if (typeof entry.draft === 'string') return entry.draft;
      const raw = entry.modified ?? '';
      if (!entry.writable && viewMode === 'source' && entry.jsonFormatted && isJsonPath(entry.path)) {
        return formatJsonText(raw) ?? raw;
      }
      return raw;
    }
    function syncJsonFormatButton(entry) {
      const show = Boolean(entry && viewMode === 'source' && isJsonPath(entry.path));
      if (!show) {
        formatJsonBtn.hidden = true;
        return;
      }
      if (entry.writable) {
        formatJsonBtn.hidden = false;
        formatJsonBtn.disabled = savingFile;
        formatJsonBtn.textContent = '格式化';
        formatJsonBtn.title = '格式化 JSON（生成未保存改动，按 Ctrl/⌘+S 保存）';
        formatJsonBtn.classList.remove('is-active');
        formatJsonBtn.removeAttribute('aria-pressed');
        return;
      }
      const raw = entry.modified ?? '';
      const formatted = formatJsonText(raw);
      const canFormat = formatted !== null && formatted.trim() !== raw.trim();
      formatJsonBtn.hidden = !canFormat && !entry.jsonFormatted;
      if (formatJsonBtn.hidden) return;
      formatJsonBtn.disabled = false;
      const active = Boolean(entry.jsonFormatted && formatted !== null);
      formatJsonBtn.textContent = active ? '查看原文' : '格式化';
      formatJsonBtn.title = active ? '当前已格式化展示 JSON，点击切换为原始文本' : '按 2 空格缩进格式化展示 JSON';
      formatJsonBtn.classList.toggle('is-active', active);
      formatJsonBtn.setAttribute('aria-pressed', String(active));
    }
    function handleJsonFormatClick() {
      const rel = activePath;
      const entry = rel ? fileCache.get(rel) : null;
      if (!entry || viewMode !== 'source' || !isJsonPath(entry.path)) return;
      if (!entry.writable) {
        if (formatJsonText(entry.modified ?? '') === null) return;
        entry.jsonFormatted = !entry.jsonFormatted;
        paintActiveCode();
        return;
      }
      if (savingFile) return;
      const current = viewerHandle?.getText ? viewerHandle.getText() : (typeof entry.draft === 'string' ? entry.draft : (entry.modified ?? ''));
      const formatted = formatJsonText(current);
      if (formatted === null) {
        entry.saveError = 'JSON 语法无效，无法格式化';
        syncEditControls();
        return;
      }
      entry.saveError = '';
      if (formatted !== current) {
        entry.draft = formatted === (entry.modified ?? '') ? null : formatted;
        entry.justSaved = false;
        if (viewerHandle?.setText) viewerHandle.setText(formatted);
        else paintActiveCode();
      }
      syncEditControls();
    }

    function syncEditControls() {
      const entry = activePath ? fileCache.get(activePath) : null;
      if (!entry) return;
      const ch = changeFor(activePath);
      const st = entry.status && entry.status !== 'clean' ? entry.status : ch?.status;
      const canEdit = Boolean(viewMode === 'source' && entry.writable);
      const dirty = hasUnsavedDraft(activePath);

      syncJsonFormatButton(entry);
      saveBtn.hidden = !canEdit;
      saveBtn.disabled = savingFile || !dirty;
      saveBtn.textContent = savingFile ? '保存中…' : '保存';
      revertBtn.hidden = !canEdit || !dirty;
      revertBtn.disabled = savingFile;

      if (entry.saveError) {
        statusPill.textContent = entry.saveError;
        statusPill.className = 'dev-code-status dev-status-error';
        statusPill.title = entry.saveError;
        statusPill.hidden = false;
      } else if (dirty) {
        statusPill.textContent = '未保存';
        statusPill.className = 'dev-code-status dev-status-dirty';
        statusPill.title = '有未保存的编辑改动 (Ctrl/⌘+S 保存)';
        statusPill.hidden = false;
      } else if (entry.justSaved) {
        statusPill.textContent = '已保存';
        statusPill.className = 'dev-code-status dev-status-a';
        statusPill.title = '已写入项目文件';
        statusPill.hidden = false;
      } else if (st && st !== 'clean') {
        const added = entry.changed ? entry.added : (ch?.added || (st === 'A' && entry.modified ? entry.modified.split('\n').length : null));
        const removed = entry.changed ? entry.removed : ch?.removed;
        const parts = [{M:'已修改', A:'新增', D:'已删除'}[st] || st];
        if (Number.isFinite(added) && added > 0) parts.push('+' + added);
        if (Number.isFinite(removed) && removed > 0) parts.push('-' + removed);
        statusPill.textContent = parts.join(' ');
        statusPill.className = 'dev-code-status dev-status-' + st.toLowerCase();
        statusPill.title = '';
        statusPill.hidden = false;
      } else if (canEdit) {
        statusPill.textContent = '可编辑';
        statusPill.className = 'dev-code-status dev-status-clean';
        statusPill.title = '可直接编辑并按 Ctrl/⌘+S 保存';
        statusPill.hidden = false;
      } else {
        statusPill.textContent = '只读';
        statusPill.className = 'dev-code-status dev-status-clean';
        statusPill.title = entry.readonlyReason || '只读查看';
        statusPill.hidden = false;
      }
      renderTabs();
    }

    async function saveActiveFile() {
      const rel = activePath;
      const entry = rel ? fileCache.get(rel) : null;
      if (!entry || !entry.writable || viewMode !== 'source' || savingFile) return;
      const text = viewerHandle?.getText ? viewerHandle.getText() : (typeof entry.draft === 'string' ? entry.draft : (entry.modified ?? ''));
      if (text === (entry.modified ?? '')) return;
      savingFile = true;
      entry.saveError = '';
      syncEditControls();
      try {
        const res = await api('project-file-save', {}, {
          path: rel,
          text,
          expected_sha256: entry.sha256 || '',
        });
        if (disposed) return;
        entry.modified = text;
        entry.sha256 = res.sha256 || entry.sha256;
        entry.draft = null;
        entry.justSaved = true;
        entry.saveError = '';
        savingFile = false;
        syncEditControls();
        refreshChanges();
        setTimeout(() => {
          if (!disposed && fileCache.get(rel) === entry && entry.justSaved) {
            entry.justSaved = false;
            if (activePath === rel) syncEditControls();
          }
        }, 1800);
      } catch (err) {
        if (disposed) return;
        savingFile = false;
        entry.saveError = err?.message || '保存失败';
        syncEditControls();
      }
    }

    function revertActiveFile() {
      const rel = activePath;
      const entry = rel ? fileCache.get(rel) : null;
      if (!entry || savingFile) return;
      entry.draft = null;
      entry.saveError = '';
      if (viewerHandle?.setText) viewerHandle.setText(entry.modified ?? '');
      syncEditControls();
    }

    function paintActiveCode() {
      if (!activePath) { renderEmptyState(); return; }
      const entry = fileCache.get(activePath);
      if (!entry) return;
      toolbar.hidden = false;
      pathText.textContent = activePath;
      pathText.title = activePath;

      const ch = changeFor(activePath);
      const st = entry.status && entry.status !== 'clean' ? entry.status : ch?.status;
      const hasGitDiff = entry.isGit && entry.changed && typeof entry.original === 'string';
      const hasAddedDiff = !hasGitDiff && st === 'A' && typeof entry.modified === 'string';
      const hasObsDiff = !hasGitDiff && !hasAddedDiff && typeof entry.unifiedDiff === 'string' && entry.unifiedDiff.trim().length > 0;
      const canDiff = hasGitDiff || hasAddedDiff || hasObsDiff;
      if (viewMode === 'diff' && !canDiff) viewMode = 'source';

      diffModeBtn.disabled = !canDiff;
      diffModeBtn.setAttribute('aria-pressed', String(viewMode === 'diff'));
      sourceModeBtn.setAttribute('aria-pressed', String(viewMode === 'source'));
      diffModeBtn.classList.toggle('is-active', viewMode === 'diff');
      sourceModeBtn.classList.toggle('is-active', viewMode === 'source');
      const pendingHit = ch && !isApprovedItem(ch);
      approveFileBtn.hidden = !pendingHit;

      const canEdit = Boolean(viewMode === 'source' && entry.writable);
      const activeText = displayTextFor(entry);
      const currentRel = activePath;
      syncEditControls();

      destroyViewer();
      viewport.replaceChildren();
      const host = node('div', 'dev-cm-host');
      viewport.append(host);

      if (globalThis.ForgeCodeViewer?.mount) {
        viewerHandle = globalThis.ForgeCodeViewer.mount(host, {
          path: activePath,
          mode: viewMode,
          text: activeText,
          original: hasGitDiff ? entry.original : (hasAddedDiff ? '' : null),
          unifiedDiff: hasObsDiff ? entry.unifiedDiff : null,
          editable: canEdit,
          onChange: canEdit ? nextText => {
            if (disposed || activePath !== currentRel) return;
            entry.draft = nextText === (entry.modified ?? '') ? null : nextText;
            entry.justSaved = false;
            entry.saveError = '';
            syncEditControls();
          } : null,
          onSave: canEdit ? () => { saveActiveFile(); } : null,
        });
      } else {
        const pre = node('pre', 'dev-code-fallback', viewMode === 'diff' && hasObsDiff ? entry.unifiedDiff : activeText);
        host.append(pre);
      }
      renderChangedList();
    }

    async function loadFile(rel, preferredMode, {silent = false} = {}) {
      const clean = String(rel || '').replace(/^\/+/, '');
      if (!clean) return;
      if (silent && hasUnsavedDraft(clean)) return;
      const prevEntry = fileCache.get(clean);
      const epoch = ++loadEpoch;
      activePath = clean;
      if (!openTabs.includes(clean)) {
        openTabs.push(clean);
        while (openTabs.length > 8) openTabs.shift();
      }
      renderTabs();
      renderChangedList();
      if (!silent) {
        destroyViewer();
        toolbar.hidden = false;
        pathText.textContent = clean;
        statusPill.hidden = true;
        viewport.replaceChildren(node('div', 'dev-code-loading', '正在读取源码…'));
      }
      try {
        const data = await api('project-file-diff', {path: clean});
        if (disposed || epoch !== loadEpoch) return;
        const obs = observedChanges.get(clean);
        const keepDraft = prevEntry && prevEntry.sha256 === (data.sha256 || '') && typeof prevEntry.draft === 'string'
          ? prevEntry.draft : null;
        const entry = {
          path: clean,
          isGit: data.is_git === true,
          status: data.status || 'clean',
          sha256: data.sha256 || '',
          original: typeof data.original === 'string' ? data.original : null,
          modified: typeof data.modified === 'string' ? data.modified : '',
          changed: data.changed === true,
          added: data.added || 0,
          removed: data.removed || 0,
          unifiedDiff: obs?.diff || null,
          writable: data.writable === true,
          readonlyReason: data.readonly_reason || '',
          draft: keepDraft,
          jsonFormatted: prevEntry && typeof prevEntry.jsonFormatted === 'boolean'
            ? prevEntry.jsonFormatted
            : (!(data.writable === true) && isJsonPath(clean) && isCompactJsonText(typeof data.modified === 'string' ? data.modified : '')),
          justSaved: false,
          saveError: '',
        };
        fileCache.set(clean, entry);
        const chNow = changeFor(clean);
        const stNow = entry.status && entry.status !== 'clean' ? entry.status : chNow?.status;
        const hasDiff = (entry.isGit && entry.changed && typeof entry.original === 'string')
          || (stNow === 'A' && typeof entry.modified === 'string')
          || Boolean(entry.unifiedDiff);
        if (preferredMode === 'diff' || preferredMode === 'source') {
          viewMode = preferredMode === 'diff' && hasDiff ? 'diff' : 'source';
        } else if (keepDraft !== null) {
          viewMode = 'source';
        } else if (!silent) {
          viewMode = hasDiff ? 'diff' : 'source';
        }
        paintActiveCode();
      } catch (err) {
        if (disposed || epoch !== loadEpoch) return;
        destroyViewer();
        viewport.replaceChildren(node('div', 'dev-code-error', err?.message || '读取文件失败'));
      }
    }

    function openFile(path, preferredMode = null) {
      const clean = String(path || '').replace(/^\/+/, '');
      if (!clean) return;
      setPaneMode('file');
      if (pane.hidden) setOpen(true, {skipFocus: true});
      loadFile(clean, preferredMode);
    }

    function closeTab(rel) {
      const idx = openTabs.indexOf(rel);
      if (idx < 0) return;
      if (hasUnsavedDraft(rel) && !window.confirm('文件 ' + splitPath(rel).name + ' 有未保存的修改，确定放弃并关闭吗？')) {
        return;
      }
      openTabs.splice(idx, 1);
      fileCache.delete(rel);
      if (activePath === rel) {
        activePath = openTabs[Math.min(idx, openTabs.length - 1)] || null;
        if (activePath) loadFile(activePath);
        else { renderTabs(); renderChangedList(); renderEmptyState(); }
      } else {
        renderTabs();
      }
    }

    function renderTree() {
      treeRoot.render(h(Level, {
        path: '/',
        tree: {
          state: {levels, expanded},
          t: treeText,
          onToggle,
          onMore: path => list(path, true),
          onOpen: path => {
            const clean = String(path || '').replace(/^\/+/, '');
            openFile(clean, changeFor(clean) ? 'diff' : 'source');
          },
        },
      }));
    }

    async function list(path, more = false) {
      const cache = levels, previous = cache[path];
      if (more && (!previous?.level || previous.loading)) return;
      if (more) previous.loading = true;
      else cache[path] = {kind: 'loading'};
      const pending = cache[path]; renderTree();
      try {
        const page = await api('project-files', {...(path === '/' ? {} : {path: path.slice(1)}), offset: more ? previous.next : 0, limit: 50});
        if (disposed || levels !== cache || cache[path] !== pending) return;
        const entries = more ? previous.level.entries.slice() : [];
        const count = Object.values(cache).reduce((n, l) => n + (l.level?.entries.length || 0), 0);
        if (count + page.entries.length > 1024) throw new Error('已展开的文件较多，请收起部分目录后重试。');
        for (const row of page.entries) entries.push({name: row.name, type: row.kind === 'directory' ? 'directory' : 'file', preview: row.kind === 'markdown' || row.kind === 'text'});
        cache[path] = {kind: 'ready', level: {entries, truncated: page.next_offset !== null}, next: page.next_offset};
        if (path === '/') {
          treeLoaded = true;
          projectRootPath = page.root || '';
          if (projectRootPath) treeTitle.title = projectRootPath;
        }
      } catch (e) {
        if (disposed || levels !== cache || cache[path] !== pending) return;
        const failure = {message: e.message};
        cache[path] = more ? {...previous, loading: false, failure} : {kind: 'failed', failure};
      }
      renderTree();
    }

    function onToggle(parent, path) {
      if (!expanded.includes(path)) { expanded.push(path); list(path); return; }
      const below = p => p === path || p.startsWith(path + '/');
      expanded = expanded.filter(p => !below(p));
      for (const p of Object.keys(levels)) if (below(p)) delete levels[p];
      renderTree();
    }

    function resetTree() {
      levels = Object.create(null);
      expanded = [];
      treeLoaded = false;
      treeBody.scrollTop = 0;
      list('/');
    }

    let changesInflight = null, changesPending = false;
    async function refreshChanges() {
      if (disposed) return;
      if (changesInflight) { changesPending = true; return changesInflight; }
      changesInflight = (async () => {
        try {
          const data = await api('project-changes');
          if (disposed) return;
          gitIsRepo = data.is_git === true;
          gitBranch = data.branch || '';
          gitChanges = Array.isArray(data.changes) ? data.changes : [];
          renderChangedList();
          if (activePath && !pane.hidden) {
            loadFile(activePath, null, {silent: true});
          }
        } catch {
          // Non-fatal: keep existing view.
        } finally {
          changesInflight = null;
          if (!disposed && changesPending) {
            changesPending = false;
            refreshChanges();
          }
        }
      })();
      return changesInflight;
    }

    function recordObservedChanges(items) {
      if (disposed || !Array.isArray(items) || !items.length) return;
      let activeTouched = false;
      for (const file of items) {
        if (!file || typeof file.path !== 'string') continue;
        const clean = file.path.replace(/^\/+/, '');
        if (ignoredPath(clean)) continue;
        const status = !file.before ? 'A' : !file.after ? 'D' : 'M';
        const stats = diffStatsFromUnified(file.diff);
        observedChanges.set(clean, {
          path: clean,
          status,
          added: stats.added,
          removed: stats.removed,
          sha256: file.after?.sha256 || file.before?.sha256 || '',
          diff: typeof file.diff === 'string' ? file.diff : null,
        });
        if (clean === activePath) activeTouched = true;
      }
      renderChangedList();
      if (activeTouched && !pane.hidden) loadFile(activePath, null, {silent: true});
    }

    function recordChecks(checks) {
      if (disposed || !checks) return;
      if (checks.native_flow) latestChecks.native_flow = checks.native_flow;
      if (checks.project_layout) latestChecks.project_layout = checks.project_layout;
      if (checks.review) latestChecks.review = checks.review;
      if (checks.reviewReset) latestChecks.reviewDetails = [];
      if (checks.reviewDetail) {
        latestChecks.reviewDetails.push(checks.reviewDetail);
        if (latestChecks.reviewDetails.length > 20) latestChecks.reviewDetails.shift();
      }
      renderForgeStrip();
    }

    function clearSessionChanges() {
      observedChanges.clear();
      latestChecks = {native_flow: null, project_layout: null, review: null, reviewDetails: []};
      activeCheckKey = null;
      renderForgeStrip();
      renderChangedList();
    }

    function effectiveTreeQuery() {
      return (treeSearchInput.value || '').trim() || filterTerm.trim();
    }

    function renderTreeSearchState(query, entries, {loading = false, error = '', truncated = false} = {}) {
      treeSearchResults.replaceChildren();
      if (!query) {
        treeSearchResults.hidden = true;
        treeHost.hidden = false;
        return;
      }
      treeHost.hidden = true;
      treeSearchResults.hidden = false;
      if (loading) {
        treeSearchResults.append(node('div', 'dev-search-empty', '正在搜索项目文件…'));
        return;
      }
      if (error) {
        treeSearchResults.append(node('div', 'dev-search-empty', error));
        return;
      }
      if (!entries.length) {
        treeSearchResults.append(node('div', 'dev-search-empty', '未找到匹配文件'));
        return;
      }
      for (const row of entries) {
        const btn = node('button', 'dev-changed-item dev-search-item');
        btn.type = 'button';
        btn.dataset.path = row.path;
        btn.title = row.path;
        btn.setAttribute('aria-current', activePath === row.path ? 'true' : 'false');
        const {name, dir} = splitPath(row.path);
        const textWrap = node('span', 'dev-changed-text');
        textWrap.append(node('span', 'dev-changed-name', name));
        if (dir) textWrap.append(node('span', 'dev-changed-dir', dir));
        btn.append(textWrap);
        btn.onclick = () => openFile(row.path, changeFor(row.path) ? 'diff' : 'source');
        treeSearchResults.append(btn);
      }
      if (truncated) {
        treeSearchResults.append(node('div', 'dev-search-empty', '结果较多，请输入更精确的文件名'));
      }
    }

    function scheduleTreeSearch({immediate = false} = {}) {
      if (treeSearchTimer) { clearTimeout(treeSearchTimer); treeSearchTimer = null; }
      const q = effectiveTreeQuery();
      treeSearchClear.hidden = !treeSearchInput.value;
      if (!q) {
        treeSearchEpoch++;
        renderTreeSearchState('', []);
        return;
      }
      const epoch = ++treeSearchEpoch;
      const run = async () => {
        treeSearchTimer = null;
        renderTreeSearchState(q, [], {loading: true});
        try {
          const data = await api('project-files', {query: q, limit: 50});
          if (disposed || epoch !== treeSearchEpoch) return;
          renderTreeSearchState(q, Array.isArray(data.entries) ? data.entries : [], {truncated: data.truncated === true});
        } catch (err) {
          if (disposed || epoch !== treeSearchEpoch) return;
          renderTreeSearchState(q, [], {error: err?.message || '搜索失败'});
        }
      };
      if (immediate) run();
      else treeSearchTimer = setTimeout(run, 140);
    }

    function setTreeCreateOpen(open) {
      treeCreateOpen = Boolean(open);
      treeCreateToggle.setAttribute('aria-expanded', String(treeCreateOpen));
      treeCreateToggle.classList.toggle('is-active', treeCreateOpen);
      if (treeCreateOpen) {
        if (!treeOpen) treeOpen = true;
        if (treeSearchOpen) setTreeSearchOpen(false);
      } else {
        treeCreateInput.value = '';
        treeCreateError.textContent = '';
        treeCreateError.hidden = true;
      }
      applyExplorerSplit();
      if (treeCreateOpen) treeCreateInput.focus();
    }

    async function submitCreateFile() {
      if (creatingFile) return;
      const raw = (treeCreateInput.value || '').trim().replace(/^\/+/, '');
      if (!raw) {
        treeCreateError.textContent = '请输入相对路径，如 .env 或 configs/a.json';
        treeCreateError.hidden = false;
        treeCreateInput.focus();
        return;
      }
      creatingFile = true;
      treeCreateSubmit.disabled = true;
      treeCreateError.hidden = true;
      try {
        const res = await api('project-file-create', {}, {path: raw});
        if (disposed) return;
        const createdPath = res?.path || raw;
        setTreeCreateOpen(false);
        resetTree();
        // Auto-expand parent directories so the newly created file is visible in the tree.
        const parts = createdPath.split('/');
        if (parts.length > 1) {
          let acc = '';
          for (let i = 0; i < parts.length - 1; i++) {
            acc += '/' + parts[i];
            if (!expanded.includes(acc)) {
              expanded.push(acc);
              list(acc);
            }
          }
        }
        openFile(createdPath, 'source');
        refreshChanges();
      } catch (err) {
        if (disposed) return;
        treeCreateError.textContent = err?.message || '创建文件失败';
        treeCreateError.hidden = false;
      } finally {
        creatingFile = false;
        treeCreateSubmit.disabled = false;
      }
    }

    function setTreeSearchOpen(open) {
      treeSearchOpen = Boolean(open);
      treeSearchToggle.setAttribute('aria-expanded', String(treeSearchOpen));
      treeSearchToggle.classList.toggle('is-active', treeSearchOpen);
      if (treeSearchOpen) {
        if (!treeOpen) treeOpen = true;
        if (treeCreateOpen) setTreeCreateOpen(false);
      }
      applyExplorerSplit();
      if (treeSearchOpen) {
        treeSearchInput.focus();
      } else if (treeSearchInput.value) {
        treeSearchInput.value = '';
        scheduleTreeSearch({immediate: true});
      }
    }

    treeCreateToggle.onclick = () => setTreeCreateOpen(!treeCreateOpen);
    treeCreateSubmit.onclick = () => submitCreateFile();
    treeCreateCancel.onclick = () => setTreeCreateOpen(false);
    treeCreateInput.onkeydown = e => {
      if (e.key === 'Enter') { e.preventDefault(); submitCreateFile(); }
      else if (e.key === 'Escape') { e.preventDefault(); setTreeCreateOpen(false); }
    };
    treeSearchToggle.onclick = () => setTreeSearchOpen(!treeSearchOpen);
    treeSearchInput.oninput = () => scheduleTreeSearch();
    treeSearchInput.onkeydown = e => {
      if (e.key === 'Escape') {
        e.preventDefault();
        if (treeSearchInput.value) {
          treeSearchInput.value = '';
          scheduleTreeSearch({immediate: true});
        } else {
          setTreeSearchOpen(false);
        }
      }
    };
    treeSearchClear.onclick = () => {
      treeSearchInput.value = '';
      scheduleTreeSearch({immediate: true});
      treeSearchInput.focus();
    };

    function setFilter(term) {
      filterTerm = String(term || '');
      renderChangedList();
      scheduleTreeSearch();
      paintOperatorTree();
    }

    // === OUTLINE SIDEBAR (Flows + Configs + Code Ops + Prompt Ops, collapsible & vertically resizable) + TYPE-AWARE RIGHT PANEL ===
    let opIndex = null, opFlows = [], opConfigs = [], opSelected = null, opSelectedFlow = null, opSelectedConfig = null, opRevision = 'current', opDetail = null, opSeq = 0, opLoading = null;
    const opStorageKey = project ? 'demiflow.operator-selection.' + project : '';
    try { if (opStorageKey) opSelected = sessionStorage.getItem(opStorageKey); } catch {}
    const opCodeType = item => ({class: '可调用类', async_function: '异步函数', function: '函数'})[item.definition_type] || 'Python 定义';
    const opVisible = item => item.kind === 'prompt' || item.export_status === 'declared' || (item.export_status === 'legacy' && item.flow_references?.length) || item.id === opSelected;

    const OUTLINE_KEYS = ['flows', 'configs', 'code', 'prompt'];
    let outlineExplorer = null;
    const outlineSections = {}; // key -> {section, toggle, count, list, open}
    const outlineHandles = [];
    let outlineWeights = [0.22, 0.20, 0.32, 0.26];
    try {
      const saved = JSON.parse(localStorage.getItem('forge-dev-outline-splits') || 'null');
      if (Array.isArray(saved) && saved.length === OUTLINE_KEYS.length && saved.every(n => Number.isFinite(n) && n >= 0.08)) outlineWeights = saved;
    } catch {}

    function applyOutlineSplits() {
      if (!outlineExplorer) return;
      const keys = OUTLINE_KEYS;
      const openKeys = keys.filter(k => outlineSections[k]?.open);
      const totalWeight = openKeys.reduce((sum, k) => sum + outlineWeights[keys.indexOf(k)], 0) || 1;
      for (const k of keys) {
        const s = outlineSections[k];
        if (!s) continue;
        s.list.hidden = !s.open;
        s.toggle.setAttribute('aria-expanded', String(s.open));
        s.section.classList.toggle('is-collapsed', !s.open);
        if (s.open) {
          const pct = (outlineWeights[keys.indexOf(k)] / totalWeight) * 100;
          s.section.style.flex = `1 1 ${pct.toFixed(2)}%`;
        } else {
          s.section.style.flex = '0 0 auto';
        }
      }
      // Show each drag handle only when the section above it and at least one section below it are open.
      for (let i = 0; i < outlineHandles.length; i++) {
        const upperOpen = Boolean(outlineSections[keys[i]]?.open);
        const anyLowerOpen = keys.slice(i + 1).some(k => outlineSections[k]?.open);
        outlineHandles[i].hidden = !(upperOpen && anyLowerOpen);
      }
    }

    function wireOutlineDrag(handle, upperIdx, lowerIdx) {
      handle.onpointerdown = e => {
        if (e.button !== 0 || !outlineExplorer) return;
        e.preventDefault();
        handle.setPointerCapture(e.pointerId);
        handle.classList.add('is-dragging');
        const keys = OUTLINE_KEYS;
        let actualLower = -1;
        for (let j = lowerIdx; j < keys.length; j++) {
          if (outlineSections[keys[j]]?.open) { actualLower = j; break; }
        }
        if (actualLower < 0) return;
        const startY = e.clientY;
        const totalH = Math.max(120, outlineExplorer.getBoundingClientRect().height);
        const startUpper = outlineWeights[upperIdx], startLower = outlineWeights[actualLower];
        const sumPair = startUpper + startLower;
        const openSum = keys.reduce((acc, k, idx) => acc + (outlineSections[k]?.open ? outlineWeights[idx] : 0), 0) || 1;
        const move = ev => {
          const delta = ((ev.clientY - startY) / totalH) * openSum;
          const nextUpper = Math.max(0.10, Math.min(sumPair - 0.10, startUpper + delta));
          outlineWeights[upperIdx] = nextUpper;
          outlineWeights[actualLower] = sumPair - nextUpper;
          applyOutlineSplits();
        };
        const end = () => {
          handle.classList.remove('is-dragging');
          handle.removeEventListener('pointermove', move);
          handle.removeEventListener('pointerup', end);
          handle.removeEventListener('pointercancel', end);
          try { localStorage.setItem('forge-dev-outline-splits', JSON.stringify(outlineWeights)); } catch {}
        };
        handle.addEventListener('pointermove', move);
        handle.addEventListener('pointerup', end);
        handle.addEventListener('pointercancel', end);
      };
    }

    if (operatorsHost) {
      outlineExplorer = node('div', 'dev-outline-explorer');
      const specs = [
        ['flows', '管道', '调整管道与配置高度'],
        ['configs', '配置', '调整配置与代码算子高度'],
        ['code', '代码算子', '调整代码算子与 Prompt 算子高度'],
        ['prompt', 'Prompt 算子', null],
      ];
      specs.forEach(([key, label, handleLabel], idx) => {
        const section = node('div', 'dev-outline-section');
        const header = node('div', 'dev-section-header');
        const toggleBtn = node('button', 'dev-section-toggle');
        toggleBtn.type = 'button';
        toggleBtn.setAttribute('aria-expanded', 'true');
        const count = node('span', 'dev-section-count', '0');
        toggleBtn.append(chevronIcon(), node('span', 'dev-section-title', label), count);
        header.append(toggleBtn);
        if (idx === 0) {
          const refBtnOutline = iconButton('刷新大纲', 'M20 10a8 8 0 1 0-1 7M20 4v6h-6');
          refBtnOutline.onclick = () => ensureOperatorsLoaded({force: true});
          header.append(refBtnOutline);
        }
        const list = node('div', 'op-tree-items dev-outline-list');
        section.append(header, list);
        outlineExplorer.append(section);
        outlineSections[key] = {section, toggle: toggleBtn, count, list, open: true};
        toggleBtn.onclick = () => {
          outlineSections[key].open = !outlineSections[key].open;
          applyOutlineSplits();
        };
        if (handleLabel) {
          const handle = node('div', 'dev-split-resizer');
          handle.setAttribute('role', 'separator');
          handle.setAttribute('aria-orientation', 'horizontal');
          handle.setAttribute('aria-label', handleLabel);
          outlineExplorer.append(handle);
          outlineHandles.push(handle);
          wireOutlineDrag(handle, idx, idx + 1);
        }
      });
      operatorsHost.replaceChildren(outlineExplorer);
      applyOutlineSplits();
    }

    function paintOperatorGroup(listEl, countEl, kind) {
      if (!listEl) return;
      listEl.replaceChildren();
      if (!opIndex) {
        countEl.textContent = '0';
        listEl.append(node('p', 'op-muted dev-op-pad', '正在读取…'));
        return;
      }
      const term = filterTerm.trim().toLocaleLowerCase();
      const matches = (opIndex.definitions || []).filter(item => opVisible(item) && item.kind === kind &&
        [item.symbol, item.path, item.description].join(' ').toLocaleLowerCase().includes(term));
      countEl.textContent = String(matches.length);
      const paths = new Map();
      for (const row of matches) {
        if (!paths.has(row.path)) paths.set(row.path, []);
        paths.get(row.path).push(row);
      }
      for (const [path, items] of paths) {
        const folder = node('details', 'op-tree-file');
        folder.open = !!term || items.some(i => i.id === opSelected) || paths.size <= 3;
        const summary = node('summary');
        summary.append(node('span', null, path));
        summary.title = path;
        folder.append(summary);
        for (const item of items) {
          const b = node('button', 'op-tree-leaf');
          b.type = 'button';
          b.dataset.definition = item.id;
          b.setAttribute('aria-pressed', String(paneMode === 'operator' && opSelected === item.id));
          b.title = (kind === 'code' ? (item.export_status === 'declared' ? '已声明算子' : '流程引用 · 未声明') + ' · ' + opCodeType(item) + ' · ' : '') + item.path + ' · ' + item.symbol;
          const glyphCls = 'op-glyph ' + (kind === 'code' ? 'is-code' : 'is-prompt');
          b.append(node('span', glyphCls, kind === 'code' ? (item.definition_type === 'class' ? 'C' : 'ƒ') : 'P'), node('span', null, item.symbol));
          b.onclick = () => openOperator(item.id);
          folder.append(b);
        }
        listEl.append(folder);
      }
      if (!matches.length) {
        listEl.append(node('p', 'op-muted dev-op-pad', term ? '无匹配项' : kind === 'code' ? '暂无代码算子' : '暂无 Prompt 算子'));
      }
    }

    function paintFlowsOutline() {
      const sec = outlineSections.flows;
      if (!sec) return;
      sec.list.replaceChildren();
      const term = filterTerm.trim().toLocaleLowerCase();
      const matches = opFlows.filter(f => [f.title, f.entry].join(' ').toLocaleLowerCase().includes(term));
      sec.count.textContent = String(matches.length);
      for (const f of matches) {
        const filePath = f.entry.split(':')[0];
        const b = node('button', 'op-tree-leaf dev-outline-flow');
        b.type = 'button';
        b.dataset.entry = f.entry;
        b.title = f.entry;
        b.setAttribute('aria-pressed', String(paneMode === 'file' && opSelectedFlow === f.entry && activePath === filePath));
        const textWrap = node('span', 'dev-outline-flow-text');
        textWrap.append(node('span', 'dev-outline-flow-name', f.title));
        if (f.title !== f.entry) textWrap.append(node('span', 'dev-outline-flow-entry', f.entry));
        b.append(node('span', 'op-glyph is-flow', 'F'), textWrap);
        b.onclick = () => {
          opSelectedFlow = f.entry;
          openFile(filePath, 'source');
          paintOperatorTree();
        };
        sec.list.append(b);
      }
      if (!matches.length) {
        sec.list.append(node('p', 'op-muted dev-op-pad', term ? '无匹配管道' : '暂无已登记管道'));
      }
    }

    function paintConfigsOutline() {
      const sec = outlineSections.configs;
      if (!sec) return;
      sec.list.replaceChildren();
      const term = filterTerm.trim().toLocaleLowerCase();
      const matches = opConfigs.filter(c => [c.name, c.path, c.flowTitle].join(' ').toLocaleLowerCase().includes(term));
      sec.count.textContent = String(matches.length);
      for (const c of matches) {
        const b = node('button', 'op-tree-leaf dev-outline-flow');
        b.type = 'button';
        b.dataset.configPath = c.path;
        b.title = c.path + (c.flowTitle ? ' · ' + c.flowTitle : '');
        b.setAttribute('aria-pressed', String(paneMode === 'file' && opSelectedConfig === c.path && activePath === c.path));
        const textWrap = node('span', 'dev-outline-flow-text');
        textWrap.append(node('span', 'dev-outline-flow-name', c.name));
        textWrap.append(node('span', 'dev-outline-flow-entry', c.flowTitle ? c.flowTitle + ' · ' + c.path : c.path));
        b.append(node('span', 'op-glyph is-config', '{}'), textWrap);
        b.onclick = () => {
          opSelectedConfig = c.path;
          opSelectedFlow = null;
          openFile(c.path, changeFor(c.path) ? 'diff' : 'source');
          paintOperatorTree();
        };
        sec.list.append(b);
      }
      if (!matches.length) {
        sec.list.append(node('p', 'op-muted dev-op-pad', term ? '无匹配配置' : '暂无配置文件'));
      }
    }

    function paintOperatorTree() {
      if (!outlineExplorer) return;
      paintFlowsOutline();
      paintConfigsOutline();
      paintOperatorGroup(outlineSections.code?.list, outlineSections.code?.count, 'code');
      paintOperatorGroup(outlineSections.prompt?.list, outlineSections.prompt?.count, 'prompt');
    }

    function paintOperatorCard(parent, title, contentFn) {
      const card = node('section', 'dev-op-card');
      if (title) card.append(node('h4', 'dev-op-card-title', title));
      contentFn(card);
      parent.append(card);
    }

    function paintOperatorDetail() {
      if (typeof disposeReaders === 'function') disposeReaders(operatorPane);
      operatorPane.replaceChildren();
      if (!opDetail) return;
      const d = opDetail, isPrompt = d.kind === 'prompt';
      const head = node('div', 'dev-op-header');
      const headTop = node('div', 'dev-op-header-top');
      const badges = node('div', 'dev-op-badges');
      badges.append(node('span', 'dev-op-badge ' + (isPrompt ? 'is-prompt' : 'is-code'),
        isPrompt ? 'Prompt 算子' : '代码算子 · ' + opCodeType(d)));
      if (!isPrompt) {
        badges.append(node('span', 'dev-op-badge is-subtle', d.export_status === 'declared' ? '已声明' : '流程引用 · 未声明'));
      }
      if (d.declared_version) badges.append(node('span', 'dev-op-badge is-subtle', 'v' + d.declared_version));
      const headBtns = node('div', 'dev-op-actions');
      const refOp = node('button', 'dev-action-btn dev-action-primary', '引用到对话');
      refOp.type = 'button';
      refOp.onclick = () => {
        if (typeof onReference === 'function') {
          onReference(d.path + (d.line ? ':' + d.line : '') + ' (' + d.symbol + ')');
        }
      };
      const openSrc = node('button', 'dev-action-btn', '打开原文件');
      openSrc.type = 'button';
      openSrc.onclick = () => { if (d.path) openFile(d.path, 'source'); };
      const histBtn = node('button', 'dev-action-btn', opRevision === 'current' ? '版本历史' : opRevision.slice(0, 8) + ' · 历史');
      histBtn.type = 'button';
      histBtn.onclick = () => openOperatorHistory(head);
      const closeOp = iconButton('关闭算子面板', 'M6 6l12 12M18 6L6 18');
      closeOp.onclick = () => setOpen(false);
      headBtns.append(refOp, openSrc, histBtn, closeOp);
      headTop.append(badges, headBtns);

      const title = node('h3', 'dev-op-title', d.symbol);
      const loc = node('div', 'dev-op-location', d.path + (d.line ? ' · L' + d.line + (d.end_line ? '–' + d.end_line : '') : ''));
      head.append(headTop, title, loc);

      const body = node('div', 'dev-op-body');
      if (d.historical) {
        const notice = node('div', 'dev-op-notice');
        notice.append(node('span', null, '正在阅读历史版本（只读）；不会修改当前代码。'));
        const back = node('button', 'dev-action-btn', '返回当前版本');
        back.type = 'button';
        back.onclick = () => openOperator(opSelected, 'current');
        notice.append(back);
        body.append(notice);
      }

      if (d.description || d.module_summary) {
        body.append(node('p', 'dev-op-desc', d.description ? d.description.split('\n')[0] : '模块说明 · ' + d.module_summary));
      }

      const refs = opIndex?.definitions?.find(x => x.id === opSelected)?.flow_references;
      if (!d.historical && refs?.length) {
        const refStrip = node('div', 'dev-op-refs');
        refStrip.append(node('span', 'dev-op-refs-label', '引用流程：'));
        for (const r of refs) {
          const name = r.entry.split(':')[0].split('/').pop().replace(/\.py$/, '');
          const chip = node('span', 'dev-op-ref-chip', name + ' · ' + r.api);
          chip.title = r.entry + ' · L' + r.line + ' · ' + r.port;
          refStrip.append(chip);
        }
        body.append(refStrip);
      }

      if (isPrompt) {
        // Prompt-specific presentation: model/format chips -> template -> input vars -> output schema -> defaults
        const modelInfo = d.model && typeof d.model === 'object' ? d.model : (d.model ? {name: d.model} : null);
        const chips = node('div', 'dev-op-chips');
        if (modelInfo) {
          const label = [modelInfo.name || modelInfo.model || modelInfo.id || modelInfo, modelInfo.transport || d.transport].filter(Boolean).join(' · ');
          if (label) chips.append(node('span', 'definition-model', '模型 ' + label));
        }
        if (d.response_format) chips.append(node('span', 'definition-chip', '输出 ' + d.response_format));
        if (d.defaults && Object.keys(d.defaults).length) {
          for (const [k, v] of Object.entries(d.defaults).slice(0, 4)) {
            chips.append(node('span', 'definition-chip', k + ': ' + (typeof v === 'string' ? v : JSON.stringify(v))));
          }
        }
        if (chips.childNodes.length) body.append(chips);

        const tpl = d.template ?? (d.messages ? JSON.stringify(d.messages, null, 2) : d.source);
        paintOperatorCard(body, 'Prompt 模板', card => {
          if (typeof tpl === 'string' && typeof mountText === 'function') {
            mountText(card, tpl, {sourceOnly: true, compactSource: true, continuousSource: true});
            const st = card.querySelector('.reading-status');
            if (st?.textContent === '源码完整显示。') st.hidden = true;
          } else {
            card.append(node('pre', 'dev-op-pre', typeof tpl === 'string' ? tpl : '无可用模板正文。'));
          }
        });

        paintOperatorCard(body, '输入变量', card => {
          if (d.input_variables?.length) {
            const table = node('table', 'op-contract');
            const tr0 = node('tr');
            ['变量', '类型', '要求'].forEach(h => tr0.append(node('th', null, h)));
            table.append(tr0);
            for (const v of d.input_variables) {
              const tr = node('tr');
              [v.name, v.type ?? '未声明', v.required ? '必填' : '可选'].forEach(val => tr.append(node('td', null, val)));
              table.append(tr);
            }
            card.append(table);
          } else {
            card.append(node('p', 'definition-note', d.input_diagnostic || '未声明模板输入变量。'));
          }
        });

        paintOperatorCard(body, '输出 Schema', card => {
          if (d.output_schema) {
            card.append(node('pre', 'dev-op-pre', JSON.stringify(d.output_schema, null, 2)));
          } else {
            card.append(node('p', 'definition-note', d.response_format === 'image' ? '图片对象引用 ObjectRef；字段结构以该 API 的原生图片协议为准。' : '未声明结构化响应 Schema。'));
          }
        });
      } else {
        // Code operator presentation: signature -> Python source -> collapsible contract/defaults
        if (d.signature) {
          paintOperatorCard(body, '接口签名', card => {
            card.append(node('pre', 'dev-op-pre', d.signature));
          });
        }
        paintOperatorCard(body, '算子源码', card => {
          if (typeof d.source === 'string' && typeof mountText === 'function') {
            const wrap = node('div', 'definition-source');
            card.append(wrap);
            mountText(wrap, d.source, {sourceOnly: true, compactSource: true, continuousSource: true, pythonLine: d.line || 1});
            const st = wrap.querySelector('.reading-status');
            if (st?.textContent === '源码完整显示。') st.hidden = true;
          } else {
            card.append(node('pre', 'dev-op-pre', typeof d.source === 'string' ? d.source : '无可用源码正文。'));
          }
        });
        if (globalThis.DemiFlowDefinition?.render) {
          const fold = node('details', 'dev-op-contract-fold');
          fold.append(node('summary', null, '参数与默认值契约'));
          DemiFlowDefinition.render(fold, d, {mountText, mode: 'contract'});
          body.append(fold);
        }
      }

      operatorPane.append(head, body);
    }

    async function openOperatorHistory(anchor) {
      const existing = operatorPane.querySelector('.op-history');
      if (existing) { existing.remove(); return; }
      const ticket = opSeq, id = opSelected;
      if (!id) return;
      try {
        const value = await api('operator-history', {id});
        if (disposed || ticket !== opSeq || id !== opSelected) return;
        const pop = node('aside', 'op-history dev-op-history');
        const heading = node('div', 'op-history-heading');
        heading.append(node('strong', null, '版本历史'));
        const close = node('button', 'op-text-button', '关闭');
        close.type = 'button';
        close.onclick = () => pop.remove();
        heading.append(close);
        pop.append(heading);
        const curBtn = node('button', 'op-version', '当前工作区');
        curBtn.type = 'button';
        curBtn.onclick = () => { pop.remove(); openOperator(id, 'current'); };
        pop.append(curBtn);
        for (const v of value.versions || []) {
          const vb = node('button', 'op-version');
          vb.type = 'button';
          vb.append(node('span', null, new Date(v.timestamp * 1000).toLocaleString()), node('code', null, v.revision.slice(0, 8)));
          vb.onclick = () => { pop.remove(); openOperator(id, v.revision); };
          pop.append(vb);
        }
        if (!(value.versions || []).length) pop.append(node('p', 'op-muted', value.reason || '尚无已保存提交。'));
        anchor.append(pop);
      } catch (err) {
        if (!disposed) operatorPane.append(node('p', 'op-error dev-op-pad', err.message));
      }
    }

    async function openOperator(id, version = 'current') {
      if (!id || disposed) return;
      opSelected = id;
      opRevision = version;
      const ticket = ++opSeq;
      setPaneMode('operator');
      paintOperatorTree();
      try { if (opStorageKey) sessionStorage.setItem(opStorageKey, id); } catch {}
      if (pane.hidden) setOpen(true, {skipFocus: true});
      operatorPane.replaceChildren(node('p', 'op-muted dev-op-pad', '正在读取算子定义…'));
      try {
        const value = await api('operator-definition', {id, revision: version});
        if (disposed || ticket !== opSeq) return;
        opDetail = value;
        paintOperatorDetail();
      } catch (err) {
        if (!disposed && ticket === opSeq) {
          operatorPane.replaceChildren(node('p', 'op-error dev-op-pad', err.message));
        }
      }
    }

    async function ensureOperatorsLoaded({force = false} = {}) {
      if (disposed) return;
      if (opIndex && !force) { paintOperatorTree(); return; }
      if (opLoading && !force) return opLoading;
      opLoading = (async () => {
        try {
          const [data, ctx, cfgSearch] = await Promise.all([
            api('project-operators'),
            api('context').catch(() => null),
            api('project-files', {query: 'configs/', limit: 50}).catch(() => null),
          ]);
          if (disposed) return;
          opIndex = data;
          const seen = new Map();
          for (const e of ctx?.project?.entries || []) seen.set(e.entry, {entry: e.entry, title: e.entry, params: e.params || []});
          for (const f of ctx?.flows || []) {
            const prev = seen.get(f.entry);
            seen.set(f.entry, {
              entry: f.entry,
              title: f.title || f.name || f.description?.split('\n')[0] || f.entry,
              params: f.params || prev?.params || [],
            });
          }
          opFlows = [...seen.values()];
          const cfgMap = new Map();
          for (const f of opFlows) {
            const list = Array.isArray(f.params) ? f.params : (f.params ? [f.params] : []);
            for (const p of list) {
              if (typeof p !== 'string' || !p) continue;
              if (!cfgMap.has(p)) {
                cfgMap.set(p, {path: p, name: splitPath(p).name, flowTitle: f.title !== f.entry ? f.title : '', bound: true});
              }
            }
          }
          for (const row of cfgSearch?.entries || []) {
            if (!row?.path || !row.path.startsWith('configs/')) continue;
            if (!/\.(json|ya?ml|toml)$/i.test(row.path)) continue;
            if (!cfgMap.has(row.path)) {
              cfgMap.set(row.path, {path: row.path, name: splitPath(row.path).name, flowTitle: '', bound: false});
            }
          }
          opConfigs = [...cfgMap.values()].sort((a, b) => (Number(b.bound) - Number(a.bound)) || a.path.localeCompare(b.path));
          if (!opSelected || !opIndex.definitions?.some(d => d.id === opSelected)) {
            const first = opIndex.definitions?.find(opVisible);
            if (first) opSelected = first.id;
          }
          paintOperatorTree();
        } catch (err) {
          if (!disposed && outlineSections.code?.list) {
            outlineSections.code.list.replaceChildren(node('p', 'op-error dev-op-pad', err.message));
          }
        } finally {
          opLoading = null;
        }
      })();
      return opLoading;
    }

    async function selectOperatorTarget(target) {
      await ensureOperatorsLoaded({force: true});
      if (disposed || !opIndex) return false;
      let match = null;
      if (target && typeof target === 'object') {
        if (target.path) {
          const normalized = target.path.replaceAll('\\', '/');
          const hits = (opIndex.definitions || []).filter(item => item.kind === 'code' && item.path === normalized &&
            (!target.sha256 || item.file_sha256 === target.sha256) &&
            (!target.line || item.line <= target.line && target.line <= item.end_line));
          if (hits.length === 1) match = hits[0];
        } else if (target.name) {
          const hits = (opIndex.definitions || []).filter(item => item.kind === 'prompt' &&
            item.symbol === target.name &&
            (!target.version || item.declared_version === target.version) &&
            (!target.template_sha256 || item.template_sha256 === target.template_sha256));
          if (hits.length >= 1) match = hits[0];
        }
      }
      const chosen = match || opIndex.definitions?.find(d => d.id === opSelected) || opIndex.definitions?.find(opVisible);
      if (chosen) { await openOperator(chosen.id); return !!match; }
      return false;
    }

    function setOpen(value, {skipFocus = false} = {}) {
      pane.hidden = !value;
      layout.classList.toggle('development-files-open', value);
      toggle.setAttribute('aria-expanded', String(value));
      resize?.setActive(value && !matchMedia('(max-width:900px)').matches);
      if (value) {
        if (paneMode === 'operator') {
          if (!opDetail && opSelected) openOperator(opSelected);
        } else {
          if (!activePath) renderEmptyState();
          else paintActiveCode();
        }
        if (!skipFocus) (paneMode === 'operator' ? operatorPane.querySelector('button') : closePanelBtn)?.focus();
      } else {
        destroyViewer();
        if (!skipFocus) toggle.focus();
      }
    }

    diffModeBtn.onclick = () => { if (!diffModeBtn.disabled) { viewMode = 'diff'; paintActiveCode(); } };
    sourceModeBtn.onclick = () => { viewMode = 'source'; paintActiveCode(); };
    formatJsonBtn.onclick = () => { handleJsonFormatClick(); };
    saveBtn.onclick = () => { saveActiveFile(); };
    revertBtn.onclick = () => { revertActiveFile(); };
    approveFileBtn.onclick = () => {
      if (!activePath) return;
      const ch = changeFor(activePath);
      if (ch && !isApprovedItem(ch)) approveItems([ch], {advance: true});
    };
    approveAllBtn.onclick = () => {
      const pending = mergedChanges().filter(item => !isApprovedItem(item));
      if (pending.length) approveItems(pending, {advance: false});
    };
    searchBtn.onclick = () => { viewerHandle?.openSearch(); };
    reloadFileBtn.onclick = () => {
      if (!activePath) return;
      if (hasUnsavedDraft(activePath) && !window.confirm('当前文件有未保存的修改，刷新将丢弃未保存内容，确定继续吗？')) return;
      const entry = fileCache.get(activePath);
      if (entry) entry.draft = null;
      loadFile(activePath);
      refreshChanges();
    };
    refBtn.onclick = () => { if (activePath && typeof onReference === 'function') onReference(activePath); };
    copyBtn.onclick = async () => {
      const entry = activePath ? fileCache.get(activePath) : null;
      if (!entry) return;
      const text = viewerHandle?.getText ? viewerHandle.getText() : displayTextFor(entry);
      try {
        await navigator.clipboard.writeText(text);
        copyBtn.textContent = '已复制';
        setTimeout(() => { if (!disposed) copyBtn.textContent = '复制'; }, 1400);
      } catch {
        copyBtn.textContent = '复制失败';
        setTimeout(() => { if (!disposed) copyBtn.textContent = '复制'; }, 1400);
      }
    };
    toggle.onclick = () => setOpen(pane.hidden);
    closePanelBtn.onclick = () => setOpen(false);
    treeRefresh.onclick = () => { resetTree(); refreshChanges(); };
    changedRefresh.onclick = () => { refreshChanges(); };
    pane.onkeydown = e => {
      if (e.key === 'Escape') {
        if (e.target && typeof e.target.closest === 'function' && e.target.closest('.cm-editor')) return;
        e.preventDefault();
        setOpen(false);
      }
    };

    // Load git changes badge on mount so the left "文件" tab shows live change count immediately.
    refreshChanges();

    return {
      openFile,
      openOperator,
      ensureOperatorsLoaded,
      selectOperatorTarget,
      setOpen,
      isOpen: () => !pane.hidden,
      ensureTreeLoaded() { if (!treeLoaded) list('/'); refreshChanges(); },
      resetTree,
      refreshChanges,
      recordObservedChanges,
      recordChecks,
      clearSessionChanges,
      setFilter,
      dispose() {
        disposed = true;
        if (treeSearchTimer) { clearTimeout(treeSearchTimer); treeSearchTimer = null; }
        destroyViewer();
        if (typeof disposeReaders === 'function') disposeReaders(operatorPane);
        resize?.dispose();
        treeRoot.unmount();
      },
    };
  }
  globalThis.DevelopmentNavigation = {turnRail, filePanel};
})();
