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

  const IGNORED_DIRS = new Set(['__pycache__', '.pytest_cache', '.mypy_cache', 'node_modules']);
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

  function filePanel({layout, filesHost, toggle, api, iconButton, onReference, onRework, onRequestReview, onShowFilesTab, onBadgeChange}) {
    let disposed = false;
    let levels = Object.create(null), expanded = [], treeLoaded = false, projectRootPath = '';
    let gitChanges = [], gitBranch = '', gitIsRepo = false;
    const observedChanges = new Map(); // path -> {path, status, added, removed, diff}
    let filterTerm = '';

    // Open tabs & active code view state
    const openTabs = []; // array of relative paths (no leading slash), max 8
    const fileCache = new Map(); // path -> {text, original, status, added, removed, isGit, unifiedDiff, sha256}
    let activePath = null;
    let viewMode = 'source'; // 'diff' | 'source'
    let viewerHandle = null;
    let loadEpoch = 0;
    let latestChecks = {native_flow: null, project_layout: null, review: null, reviewDetails: []};
    let activeCheckKey = null;

    // === LEFT SIDEBAR: Changed files + Project tree (collapsible + resizable split) ===
    const explorer = node('div', 'dev-file-explorer');
    let changedOpen = true, treeOpen = true;
    let splitRatio = 0.4;
    try {
      const saved = Number(localStorage.getItem('forge-dev-explorer-split'));
      if (Number.isFinite(saved) && saved >= 0.15 && saved <= 0.85) splitRatio = saved;
    } catch {}

    const changedSection = node('div', 'dev-changed-section');
    changedSection.hidden = true;
    const changedHeader = node('div', 'dev-section-header');
    const changedToggle = node('button', 'dev-section-toggle');
    changedToggle.type = 'button';
    changedToggle.setAttribute('aria-expanded', 'true');
    const changedTitle = node('span', 'dev-section-title', '已改文件');
    const changedCount = node('span', 'dev-section-count', '0');
    changedToggle.append(chevronIcon(), changedTitle, changedCount);
    const changedRefresh = iconButton('刷新变更列表', 'M20 10a8 8 0 1 0-1 7M20 4v6h-6');
    changedHeader.append(changedToggle, changedRefresh);
    const changedList = node('div', 'dev-changed-list');
    changedSection.append(changedHeader, changedList);

    const splitHandle = node('div', 'dev-split-resizer');
    splitHandle.setAttribute('role', 'separator');
    splitHandle.setAttribute('aria-orientation', 'horizontal');
    splitHandle.setAttribute('aria-label', '调整已改文件与项目目录高度');
    splitHandle.tabIndex = 0;
    splitHandle.hidden = true;

    const treeSection = node('div', 'dev-tree-section');
    const treeHeader = node('div', 'dev-section-header');
    const treeToggle = node('button', 'dev-section-toggle');
    treeToggle.type = 'button';
    treeToggle.setAttribute('aria-expanded', 'true');
    const treeTitle = node('span', 'dev-section-title', '项目目录');
    treeToggle.append(chevronIcon(), treeTitle);
    const treeRefresh = iconButton('刷新项目目录', 'M20 10a8 8 0 1 0-1 7M20 4v6h-6');
    treeHeader.append(treeToggle, treeRefresh);
    const treeBody = node('div', 'dev-tree-body ' + files.body);
    const treeHost = node('ul', files.level);
    treeBody.append(treeHost);
    treeSection.append(treeHeader, treeBody);
    explorer.append(changedSection, splitHandle, treeSection);
    if (filesHost) filesHost.append(explorer);

    function applyExplorerSplit() {
      const hasChanges = !changedSection.hidden;
      changedToggle.setAttribute('aria-expanded', String(changedOpen));
      treeToggle.setAttribute('aria-expanded', String(treeOpen));
      changedList.hidden = !changedOpen;
      treeBody.hidden = !treeOpen;
      changedSection.classList.toggle('is-collapsed', !changedOpen);
      treeSection.classList.toggle('is-collapsed', !treeOpen);
      const canResize = hasChanges && changedOpen && treeOpen;
      splitHandle.hidden = !canResize;
      splitHandle.setAttribute('aria-valuenow', String(Math.round(splitRatio * 100)));
      if (!hasChanges) {
        changedSection.style.flex = '';
        treeSection.style.flex = treeOpen ? '1 1 0%' : '0 0 auto';
      } else if (changedOpen && treeOpen) {
        const pct = Math.round(splitRatio * 1000) / 10;
        changedSection.style.flex = '0 0 ' + pct + '%';
        treeSection.style.flex = '1 1 0%';
      } else if (changedOpen && !treeOpen) {
        changedSection.style.flex = '1 1 0%';
        treeSection.style.flex = '0 0 auto';
      } else if (!changedOpen && treeOpen) {
        changedSection.style.flex = '0 0 auto';
        treeSection.style.flex = '1 1 0%';
      } else {
        changedSection.style.flex = '0 0 auto';
        treeSection.style.flex = '0 0 auto';
      }
    }
    function setSplitRatio(next) {
      splitRatio = Math.max(0.15, Math.min(0.85, next));
      try { localStorage.setItem('forge-dev-explorer-split', String(splitRatio)); } catch {}
      applyExplorerSplit();
    }
    changedToggle.onclick = () => { changedOpen = !changedOpen; applyExplorerSplit(); };
    treeToggle.onclick = () => { treeOpen = !treeOpen; applyExplorerSplit(); };
    splitHandle.ondblclick = () => setSplitRatio(0.4);
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

    const refBtn = node('button', 'dev-action-btn dev-action-primary', '引用到对话');
    refBtn.type = 'button';
    const searchBtn = iconButton('在代码中搜索 (Ctrl+F)', 'M11 4a7 7 0 1 0 0 14a7 7 0 1 0 0-14M16.2 16.2L20 20');
    const copyBtn = node('button', 'dev-action-btn', '复制');
    copyBtn.type = 'button';
    const reloadFileBtn = iconButton('刷新当前文件', 'M20 10a8 8 0 1 0-1 7M20 4v6h-6');
    toolActions.append(modeSwitch, refBtn, searchBtn, copyBtn, reloadFileBtn);
    toolbar.append(pathMeta, toolActions);

    const viewport = node('div', 'dev-code-viewport');
    pane.append(tabBar, forgeStrip, forgeDetail, toolbar, viewport);
    layout.append(pane);
    toggle.setAttribute('aria-expanded', 'false');

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
          map.set(p, {...prev, diff: obs.diff || prev.diff, source: 'both'});
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
        diff: obsHit?.diff || null};
    }

    function destroyViewer() {
      if (viewerHandle) {
        try { viewerHandle.destroy(); } catch {}
        viewerHandle = null;
      }
    }

    function renderChangedList() {
      const all = mergedChanges();
      const total = all.length;
      if (typeof onBadgeChange === 'function') onBadgeChange(total);
      changedCount.textContent = String(total);
      const term = filterTerm.trim().toLocaleLowerCase();
      const visible = term ? all.filter(item => item.path.toLocaleLowerCase().includes(term)) : all;
      changedSection.hidden = total === 0;
      applyExplorerSplit();
      changedList.replaceChildren();
      for (const item of visible) {
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
      if (!activePath && !pane.hidden) renderEmptyState();
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
        items.push({key: 'native_flow', label: '原生数据流检查',
          text: onlyUnregisteredFl10 ? '变更流通过 (有未登记提示)' : checkLabel(c.status),
          tone: onlyUnregisteredFl10 ? 'ok' : checkTone(c.status),
          lines: lines.length ? lines : ['静态数据流与 SDK 契约检查通过。']});
      }
      if (latestChecks.project_layout && latestChecks.project_layout.status !== 'not_applicable') {
        const c = latestChecks.project_layout;
        const lines = (c.diagnostics || []).map(d => [d.rule, d.path + (d.line ? ':' + d.line : ''), d.detail].filter(Boolean).join(' · '));
        items.push({key: 'project_layout', label: '项目目录检查', text: checkLabel(c.status), tone: checkTone(c.status),
          lines: lines.length ? lines : ['业务目录与角色规范检查通过。']});
      }
      if (latestChecks.review) {
        const r = latestChecks.review;
        const rText = ({passed:'已通过', invalid:'需修改', incomplete:'未完成', running:'评审中'})[r] || r;
        const rTone = ({passed:'ok', invalid:'bad', incomplete:'warn', running:'muted'})[r] || 'muted';
        const lines = latestChecks.reviewDetails.length
          ? latestChecks.reviewDetails
          : [r === 'passed' ? '独立只读评审通过。' : '点击聊天流中的「独立评审」查看完整记录。'];
        items.push({key: 'review', label: '独立评审', text: rText, tone: rTone, lines});
      }
      forgeStrip.replaceChildren();
      forgeStrip.hidden = items.length === 0;
      if (!items.length) { forgeDetail.hidden = true; return; }
      if (activeCheckKey && !items.some(i => i.key === activeCheckKey)) activeCheckKey = null;
      forgeStrip.append(node('span', 'dev-forge-strip-label', 'Forge 校验'));
      for (const item of items) {
        const pill = node('button', 'dev-forge-pill dev-forge-' + item.tone + (activeCheckKey === item.key ? ' is-open' : ''), item.label + ' · ' + item.text);
        pill.type = 'button';
        pill.setAttribute('aria-expanded', String(activeCheckKey === item.key));
        pill.title = '点击查看「' + item.label + '」详情';
        pill.onclick = () => {
          activeCheckKey = activeCheckKey === item.key ? null : item.key;
          renderForgeStrip();
        };
        forgeStrip.append(pill);
      }
      if (typeof onRequestReview === 'function' && latestChecks.review !== 'running') {
        const revBtn = node('button', 'dev-forge-trigger', '发起评审');
        revBtn.type = 'button';
        revBtn.title = '手动发起一次独立只读 Forge 评审';
        revBtn.onclick = () => onRequestReview();
        forgeStrip.append(revBtn);
      }
      const selected = items.find(i => i.key === activeCheckKey);
      forgeDetail.replaceChildren();
      if (selected) {
        const head = node('div', 'dev-forge-detail-head');
        head.append(node('strong', null, selected.label + ' · ' + selected.text));
        const headBtns = node('div', 'dev-forge-detail-actions');
        if (typeof onRework === 'function' && selected.tone !== 'ok' && selected.lines.length) {
          const reworkBtn = node('button', 'dev-forge-rework', '带问题打回修改');
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

    function renderTabs() {
      tabList.replaceChildren();
      for (const rel of openTabs) {
        const tab = node('div', 'dev-code-tab' + (rel === activePath ? ' is-active' : ''));
        tab.dataset.path = rel;
        const selectBtn = node('button', 'dev-code-tab-btn');
        selectBtn.type = 'button';
        selectBtn.title = rel;
        const ch = changeFor(rel);
        if (ch) selectBtn.append(node('span', 'dev-code-tab-dot dev-status-' + (ch.status || 'M').toLowerCase()));
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
      empty.append(node('p', null, '只读查看项目源码与变更 Diff，围绕待改对象在左侧对话中驱动 Forge 修改。'));
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

    function paintActiveCode() {
      if (!activePath) { renderEmptyState(); return; }
      const entry = fileCache.get(activePath);
      if (!entry) return;
      toolbar.hidden = false;
      pathText.textContent = activePath;
      pathText.title = activePath;

      const hasGitDiff = entry.isGit && entry.changed && typeof entry.original === 'string';
      const hasObsDiff = !hasGitDiff && typeof entry.unifiedDiff === 'string' && entry.unifiedDiff.trim().length > 0;
      const canDiff = hasGitDiff || hasObsDiff;
      if (viewMode === 'diff' && !canDiff) viewMode = 'source';

      diffModeBtn.disabled = !canDiff;
      diffModeBtn.setAttribute('aria-pressed', String(viewMode === 'diff'));
      sourceModeBtn.setAttribute('aria-pressed', String(viewMode === 'source'));
      diffModeBtn.classList.toggle('is-active', viewMode === 'diff');
      sourceModeBtn.classList.toggle('is-active', viewMode === 'source');

      const ch = changeFor(activePath);
      const st = entry.status && entry.status !== 'clean' ? entry.status : ch?.status;
      if (st && st !== 'clean') {
        const added = entry.changed ? entry.added : ch?.added;
        const removed = entry.changed ? entry.removed : ch?.removed;
        const parts = [{M:'已修改', A:'新增', D:'已删除'}[st] || st];
        if (Number.isFinite(added) && added > 0) parts.push('+' + added);
        if (Number.isFinite(removed) && removed > 0) parts.push('-' + removed);
        statusPill.textContent = parts.join(' ');
        statusPill.className = 'dev-code-status dev-status-' + st.toLowerCase();
        statusPill.hidden = false;
      } else {
        statusPill.textContent = '只读';
        statusPill.className = 'dev-code-status dev-status-clean';
        statusPill.hidden = false;
      }

      destroyViewer();
      viewport.replaceChildren();
      const host = node('div', 'dev-cm-host');
      viewport.append(host);

      if (globalThis.ForgeCodeViewer?.mount) {
        viewerHandle = globalThis.ForgeCodeViewer.mount(host, {
          path: activePath,
          mode: viewMode,
          text: entry.modified ?? '',
          original: hasGitDiff ? entry.original : null,
          unifiedDiff: hasObsDiff ? entry.unifiedDiff : null,
        });
      } else {
        const pre = node('pre', 'dev-code-fallback', viewMode === 'diff' && hasObsDiff ? entry.unifiedDiff : (entry.modified ?? ''));
        host.append(pre);
      }
      renderTabs();
      renderChangedList();
    }

    async function loadFile(rel, preferredMode, {silent = false} = {}) {
      const clean = String(rel || '').replace(/^\/+/, '');
      if (!clean) return;
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
        };
        fileCache.set(clean, entry);
        const hasDiff = (entry.isGit && entry.changed && typeof entry.original === 'string') || Boolean(entry.unifiedDiff);
        if (preferredMode === 'diff' || preferredMode === 'source') {
          viewMode = preferredMode === 'diff' && hasDiff ? 'diff' : 'source';
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
      if (pane.hidden) setOpen(true, {skipFocus: true});
      loadFile(clean, preferredMode);
    }

    function closeTab(rel) {
      const idx = openTabs.indexOf(rel);
      if (idx < 0) return;
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

    function setFilter(term) {
      filterTerm = String(term || '');
      renderChangedList();
    }

    function setOpen(value, {skipFocus = false} = {}) {
      pane.hidden = !value;
      layout.classList.toggle('development-files-open', value);
      toggle.setAttribute('aria-expanded', String(value));
      resize?.setActive(value && !matchMedia('(max-width:900px)').matches);
      if (value) {
        if (!activePath) renderEmptyState();
        else paintActiveCode();
        if (!skipFocus) closePanelBtn.focus();
      } else {
        destroyViewer();
        if (!skipFocus) toggle.focus();
      }
    }

    diffModeBtn.onclick = () => { if (!diffModeBtn.disabled) { viewMode = 'diff'; paintActiveCode(); } };
    sourceModeBtn.onclick = () => { viewMode = 'source'; paintActiveCode(); };
    searchBtn.onclick = () => { viewerHandle?.openSearch(); };
    reloadFileBtn.onclick = () => { if (activePath) loadFile(activePath); refreshChanges(); };
    refBtn.onclick = () => { if (activePath && typeof onReference === 'function') onReference(activePath); };
    copyBtn.onclick = async () => {
      const entry = activePath ? fileCache.get(activePath) : null;
      if (!entry) return;
      try {
        await navigator.clipboard.writeText(entry.modified ?? '');
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
    pane.onkeydown = e => { if (e.key === 'Escape') { e.preventDefault(); setOpen(false); } };

    // Load git changes badge on mount so the left "文件" tab shows live change count immediately.
    refreshChanges();

    return {
      openFile,
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
        destroyViewer();
        resize?.dispose();
        treeRoot.unmount();
      },
    };
  }
  globalThis.DevelopmentNavigation = {turnRail, filePanel};
})();
