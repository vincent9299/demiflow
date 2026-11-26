/* Multi-pipeline cross-project Workflow DAG panel (Level 1 graph + drill-down to Level 2 flows). */
(() => {
  'use strict';

  const el = (tag, text, cls) => {
    const n = document.createElement(tag);
    if (text !== undefined && text !== null) n.textContent = String(text);
    if (cls) n.className = cls;
    return n;
  };

  const STATUS_META = {
    pending: {label: '待执行', tone: 'neutral'},
    pending_resume: {label: '待续跑', tone: 'neutral'},
    waiting_project: {label: '排队中', tone: 'running'},
    preparing: {label: '准备中', tone: 'running'},
    starting: {label: '启动中', tone: 'running'},
    running: {label: '运行中', tone: 'running'},
    stopping: {label: '停止中', tone: 'running'},
    succeeded: {label: '已完成', tone: 'success'},
    reused: {label: '已复用', tone: 'success'},
    failed: {label: '失败', tone: 'failed'},
    skipped: {label: '已跳过', tone: 'neutral'},
    cancelled: {label: '已停止', tone: 'failed'},
    interrupted: {label: '已中断', tone: 'failed'},
  };

  const statusMeta = status => STATUS_META[status] || {label: status || '未知', tone: 'neutral'};

  function fmtTime(ts) {
    if (!ts || !Number.isFinite(ts)) return '';
    try {
      const d = new Date(ts * 1000);
      return d.toLocaleTimeString('zh-CN', {hour12: false});
    } catch { return ''; }
  }

  function fmtDuration(start, end) {
    if (!start || !Number.isFinite(start)) return '';
    const s = Math.max(0, Math.round(((end && Number.isFinite(end)) ? end : Date.now() / 1000) - start));
    if (s < 60) return s + 's';
    const m = Math.floor(s / 60), r = s % 60;
    return m + 'm ' + r + 's';
  }

  function statusPill(status) {
    const m = statusMeta(status);
    const span = el('span', m.label, 'wf-pill wf-pill-' + m.tone);
    span.dataset.status = status || '';
    return span;
  }

  function mount(options) {
    const {
      sidebarHost,
      stageHost,
      detailHost,
      api,
      workspaceId,
      context,
      onActivate,
      onDeactivate,
      onDetailOpen,
      onDetailClose,
      onOpenSource,
      onOpenChild,
      error,
    } = options;

    let disposed = false;
    let active = false;
    let workflows = [];
    let filterQuery = '';
    let selectedPath = null;
    let currentInspect = null;
    let currentRuns = [];
    let selectedRunId = null;
    let currentRun = null;
    let selectedNodeId = null;
    let pollTimer = null;
    let graphRenderer = null;
    let busyAction = false;

    // 1. Sidebar section ("编排" above "单条管线", hidden if project has zero workflows).
    const navSection = el('div', undefined, 'wf-sidebar-section');
    navSection.hidden = true;
    const navHeader = el('div', undefined, 'wf-sidebar-header');
    const navTitle = el('span', '编排', 'wf-sidebar-label');
    const navCount = el('span', '0', 'wf-sidebar-count');
    navHeader.append(navTitle, navCount);
    const navList = el('div', undefined, 'wb-navigation-items wf-nav-list');
    const singleFlowsHeading = el('div', '单条管线', 'wf-sidebar-sublabel');
    navSection.append(navHeader, navList, singleFlowsHeading);
    sidebarHost.prepend(navSection);

    // 2. Center stage layout inside stageHost.
    const root = el('div', undefined, 'wf-stage');
    const header = el('div', undefined, 'wf-header');
    const headerLeft = el('div', undefined, 'wf-header-left');
    const titleEl = el('h2', '多管线编排', 'wf-title');
    const metaRow = el('div', undefined, 'wf-meta-row');
    headerLeft.append(titleEl, metaRow);

    const headerActions = el('div', undefined, 'wf-actions');
    const runPicker = el('select', undefined, 'wf-run-picker');
    runPicker.setAttribute('aria-label', '选择编排运行记录');
    const runBtn = el('button', '运行编排', 'primary wf-btn-run'); runBtn.type = 'button';
    const stopBtn = el('button', '停止编排', 'wf-btn-stop'); stopBtn.type = 'button'; stopBtn.hidden = true;
    const resumeBtn = el('button', '断点续跑', 'wf-btn-resume'); resumeBtn.type = 'button'; resumeBtn.hidden = true;
    const reconcileBtn = el('button', '对账恢复', 'wf-btn-reconcile'); reconcileBtn.type = 'button'; reconcileBtn.hidden = true;
    const sourceBtn = el('button', '源码', 'pw-header-ghost wf-btn-source'); sourceBtn.type = 'button';
    headerActions.append(runPicker, runBtn, stopBtn, resumeBtn, reconcileBtn, sourceBtn);
    header.append(headerLeft, headerActions);

    const bannerBox = el('div', undefined, 'wf-banners');
    bannerBox.hidden = true;
    const graphWrap = el('div', undefined, 'wf-graph-host');
    const footerBar = el('div', undefined, 'wf-footer');
    footerBar.hidden = true;

    root.append(header, bannerBox, graphWrap, footerBar);
    stageHost.replaceChildren(root);

    // 3. Right detail drawer container.
    const detailRoot = el('div', undefined, 'wf-detail');
    detailHost.replaceChildren(detailRoot);

    function clearPoll() {
      if (pollTimer) { clearTimeout(pollTimer); pollTimer = null; }
    }

    function schedulePoll() {
      clearPoll();
      if (disposed || !active || !selectedRunId || !currentRun) return;
      const isLive = ['starting', 'running', 'stopping'].includes(currentRun.status) || currentRun.orchestrator_alive;
      if (!isLive) return;
      pollTimer = setTimeout(async () => {
        if (disposed || !active || !selectedRunId) return;
        try {
          const fresh = await api('workflow-run', {wf_run_id: selectedRunId});
          if (disposed || !active || fresh.wf_run_id !== selectedRunId) return;
          currentRun = fresh;
          renderStage();
          if (selectedNodeId) renderNodeDetail(selectedNodeId);
          refreshListQuiet();
          schedulePoll();
        } catch {
          if (!disposed && active) pollTimer = setTimeout(schedulePoll, 3000);
        }
      }, 1500);
    }

    async function refreshListQuiet() {
      try {
        const res = await api('workflows');
        if (disposed) return;
        workflows = res.workflows || [];
        renderSidebar();
      } catch { /* ignore background list refresh errors */ }
    }

    function renderSidebar() {
      if (sidebarHost.firstElementChild !== navSection) sidebarHost.prepend(navSection);
      const q = filterQuery.trim().toLocaleLowerCase();
      navSection.hidden = workflows.length === 0;
      navCount.textContent = String(workflows.length);
      navList.replaceChildren();
      for (const wf of workflows) {
        const hay = (wf.name + ' ' + wf.path + ' ' + (wf.description || '')).toLocaleLowerCase();
        if (q && !hay.includes(q)) continue;
        const btn = el('button', undefined, 'wb-flow-record wf-nav-item' + (active && selectedPath === wf.path ? ' active' : ''));
        btn.type = 'button';
        btn.dataset.workflowPath = wf.path;
        if (active && selectedPath === wf.path) btn.setAttribute('aria-current', 'page');

        const top = el('div', undefined, 'wf-nav-top');
        const nameEl = el('strong', wf.name || wf.path);
        top.append(nameEl);
        if (wf.latest_run?.status) top.append(statusPill(wf.latest_run.status));
        else if (wf.status === 'failed') top.append(statusPill('failed'));

        const subParts = [(wf.node_count || 0) + ' 步'];
        if (wf.cross_project) {
          const cnt = (wf.involved_projects || []).length;
          subParts.push('跨 ' + cnt + ' 项目');
        }
        if (wf.path) subParts.push(wf.path.replace(/^workflows\//, ''));
        const sub = el('span', subParts.join(' · '));
        btn.append(top, sub);
        btn.onclick = () => openWorkflow(wf.path);
        navList.append(btn);
      }
    }

    function renderBanners() {
      bannerBox.replaceChildren();
      const msgs = [];
      if (currentInspect?.diagnostics?.length) {
        for (const d of currentInspect.diagnostics.slice(0, 4)) {
          msgs.push({tone: d.rule === 'WF-05' ? 'warn' : 'error', text: `[${d.rule}] ${d.detail}`});
        }
      }
      if (currentRun?.needs_reconcile) {
        msgs.push({tone: 'warn', text: '编排调度器进程已停止但状态尚未结算，请点击「对账恢复」核对子管线实际状态后再续跑。'});
      } else if (currentRun?.error && ['failed', 'interrupted', 'cancelled'].includes(currentRun.status)) {
        msgs.push({tone: 'error', text: currentRun.error});
      }
      bannerBox.hidden = msgs.length === 0;
      for (const m of msgs) {
        bannerBox.append(el('div', m.text, 'wf-banner wf-banner-' + m.tone));
      }
    }

    function renderHeader() {
      const spec = currentRun?.spec || currentInspect?.spec;
      const projects = currentRun?.projects || currentInspect?.projects || [];
      titleEl.textContent = spec?.name || selectedPath || '多管线编排';
      metaRow.replaceChildren();
      if (selectedPath) metaRow.append(el('span', selectedPath, 'wf-chip wf-chip-path'));
      for (const p of projects) {
        const cls = p.is_self ? 'wf-chip wf-chip-self' : 'wf-chip wf-chip-ext';
        const permOk = p.resolved && (p.permissions?.run_submit !== false);
        const label = (p.is_self ? '本项目: ' : '跨项目: ') + (p.name || p.alias) + (p.resolved ? (permOk ? '' : ' (未授权运行)') : ' (未解析)');
        const chip = el('span', label, cls + (permOk ? '' : ' wf-chip-warn'));
        metaRow.append(chip);
      }
      if (currentRun) {
        metaRow.append(statusPill(currentRun.status));
        if (currentRun.attempt > 1) metaRow.append(el('span', '第 ' + currentRun.attempt + ' 次尝试', 'wf-chip'));
      }

      // Run history selector.
      runPicker.replaceChildren();
      const previewOpt = el('option', '结构预览（未选运行）'); previewOpt.value = '';
      runPicker.append(previewOpt);
      for (const r of currentRuns) {
        const m = statusMeta(r.status);
        const opt = el('option', `${r.wf_run_id.slice(0, 10)} · ${m.label} · ${fmtTime(r.created_at)}`);
        opt.value = r.wf_run_id;
        runPicker.append(opt);
      }
      runPicker.value = selectedRunId || '';

      const isActive = currentRun && (['starting', 'running', 'stopping'].includes(currentRun.status) || currentRun.orchestrator_alive);
      runBtn.disabled = busyAction || !currentInspect?.can_run;
      stopBtn.hidden = !isActive;
      stopBtn.disabled = busyAction || currentRun?.status === 'stopping';
      resumeBtn.hidden = !currentRun || isActive || currentRun.status === 'succeeded';
      resumeBtn.disabled = busyAction;
      reconcileBtn.hidden = !currentRun?.needs_reconcile;
      reconcileBtn.disabled = busyAction;
    }

    function renderGraph() {
      const model = currentRun?.graph || currentInspect?.graph;
      if (!model) {
        graphWrap.replaceChildren(el('p', '请选择左侧编排查看依赖图。', 'muted'));
        return;
      }
      const viewModel = {...model, selected: selectedNodeId};
      if (!graphRenderer) {
        graphRenderer = ForgeWorkbenchGraph.mount(graphWrap, {
          onSelect: sel => {
            const nid = sel?.node_id || sel?.key;
            if (!nid) return;
            selectedNodeId = nid;
            graphRenderer.select(nid);
            renderNodeDetail(nid);
            onDetailOpen?.();
          },
        });
      }
      graphRenderer.update(viewModel);
      if (selectedNodeId) graphRenderer.select(selectedNodeId);
    }

    function renderFooter() {
      if (!currentRun) {
        footerBar.hidden = true;
        return;
      }
      footerBar.hidden = false;
      footerBar.replaceChildren();
      const counts = currentRun.counts || {};
      const total = (currentRun.node_list || []).length;
      const done = (counts.succeeded || 0) + (counts.reused || 0);
      const summary = el('span', `进度：${done}/${total} 完成` +
        (counts.running ? ` · ${counts.running} 运行中` : '') +
        (counts.failed ? ` · ${counts.failed} 失败` : '') +
        (counts.skipped ? ` · ${counts.skipped} 跳过` : '') +
        (currentRun.started_at ? ` · 耗时 ${fmtDuration(currentRun.started_at, currentRun.finished_at)}` : ''),
        'wf-footer-summary');
      footerBar.append(summary);
      const lastEv = (currentRun.events || []).slice(-1)[0];
      if (lastEv) {
        footerBar.append(el('span', `最新事件 ${fmtTime(lastEv.ts)} · ${lastEv.kind}${lastEv.node_id ? ' (' + lastEv.node_id + ')' : ''}`, 'wf-footer-event'));
      }
    }

    function renderStage() {
      renderHeader();
      renderBanners();
      renderGraph();
      renderFooter();
    }

    function renderNodeDetail(nodeId) {
      const spec = currentRun?.spec || currentInspect?.spec;
      if (!spec) return;
      const nodeDef = (spec.nodes || []).find(n => n.id === nodeId);
      if (!nodeDef) return;
      const nodeRun = currentRun?.nodes?.[nodeId] || null;
      const projects = currentRun?.projects || currentInspect?.projects || [];
      const projMeta = projects.find(p => p.alias === nodeDef.project) || {name: nodeDef.project_ref || nodeDef.project, is_self: nodeDef.project === 'self'};

      detailRoot.replaceChildren();
      const head = el('div', undefined, 'wb-source-header wf-detail-header');
      const headInfo = el('div', undefined, 'wf-detail-title-wrap');
      headInfo.append(el('strong', nodeDef.title || nodeDef.id));
      headInfo.append(el('span', (projMeta.is_self ? '本项目 · ' : '跨项目 · ') + (projMeta.name || nodeDef.project),
        'wf-chip ' + (projMeta.is_self ? 'wf-chip-self' : 'wf-chip-ext')));
      const closeBtn = el('button', '×', 'wb-canvas-detail-close');
      closeBtn.type = 'button';
      closeBtn.setAttribute('aria-label', '关闭节点详情');
      closeBtn.onclick = () => {
        selectedNodeId = null;
        graphRenderer?.select(null);
        onDetailClose?.();
      };
      head.append(headInfo, closeBtn);
      detailRoot.append(head);

      const body = el('div', undefined, 'wf-detail-body');

      // Primary actions: drill-down to Level 2 + rerun from node.
      const actRow = el('div', undefined, 'wf-detail-actions');
      const drillBtn = el('button', nodeRun?.child_run_id ? '查看子管线运行详情 →' : '查看子管线详情 →', 'primary wf-btn-drill');
      drillBtn.type = 'button';
      drillBtn.disabled = !projMeta.workspace_id && !projMeta.is_self;
      drillBtn.onclick = async () => {
        const targetWs = projMeta.is_self ? workspaceId : projMeta.workspace_id;
        if (!targetWs) return;
        await onOpenChild?.({
          hostWorkspaceId: workspaceId,
          hostProjectName: context?.name || workspaceId,
          workflowPath: selectedPath,
          workflowName: spec.name || selectedPath,
          wfRunId: selectedRunId,
          nodeId: nodeDef.id,
          nodeTitle: nodeDef.title || nodeDef.id,
          targetWorkspaceId: targetWs,
          entry: nodeDef.entry,
          params: nodeDef.params || null,
          childRunId: nodeRun?.child_run_id || null,
          childAttempt: nodeRun?.child_attempt || null,
        });
      };
      actRow.append(drillBtn);

      const isWfLive = currentRun && (['starting', 'running', 'stopping'].includes(currentRun.status) || currentRun.orchestrator_alive);
      if (currentRun && !isWfLive && selectedRunId) {
        const rerunBtn = el('button', '从此节点重跑', 'wf-btn-rerun-node');
        rerunBtn.type = 'button';
        rerunBtn.disabled = busyAction;
        rerunBtn.onclick = () => doResume(nodeDef.id);
        actRow.append(rerunBtn);
      }
      body.append(actRow);

      // Summary info table.
      const infoList = el('dl', undefined, 'wf-kv');
      const addKv = (k, v) => {
        infoList.append(el('dt', k));
        const dd = el('dd');
        if (v instanceof Node) dd.append(v);
        else dd.textContent = v;
        infoList.append(dd);
      };
      addKv('节点状态', statusPill(nodeRun?.status || 'pending'));
      addKv('节点 ID', nodeDef.id);
      addKv('目标入口', nodeDef.entry);
      if (nodeDef.params) addKv('基础参数', nodeDef.params);
      if (nodeDef.concurrency_group) addKv('并发组', nodeDef.concurrency_group);
      if (nodeDef.deps?.length) addKv('上游依赖', nodeDef.deps.join('、'));
      if (nodeRun?.child_run_id) {
        addKv('子运行 ID', nodeRun.child_run_id + (nodeRun.child_attempt ? ' (attempt ' + nodeRun.child_attempt + ')' : ''));
      }
      if (nodeRun?.started_at) {
        addKv('耗时', fmtDuration(nodeRun.started_at, nodeRun.finished_at));
      }
      body.append(infoList);

      if (nodeRun?.error) {
        body.append(el('div', nodeRun.error, 'wf-banner wf-banner-error'));
      }

      // Parameter overrides (static & resolved).
      const hasStaticOv = nodeDef.overrides && Object.keys(nodeDef.overrides).length > 0;
      const hasResolvedOv = nodeRun?.resolved_overrides && Object.keys(nodeRun.resolved_overrides).length > 0;
      if (hasStaticOv || hasResolvedOv) {
        const sec = el('section', undefined, 'wf-detail-sec');
        sec.append(el('h3', hasResolvedOv ? '实际运行参数覆盖（已解析）' : '声明的参数覆盖'));
        const pre = el('pre', JSON.stringify(hasResolvedOv ? nodeRun.resolved_overrides : nodeDef.overrides, null, 2), 'wf-code');
        sec.append(pre);
        body.append(sec);
      }

      // Verified artifacts (Lance outputs & trained models).
      const art = nodeRun?.artifacts;
      if (art && (Object.keys(art.outputs || {}).length || (art.models_list || []).length)) {
        const sec = el('section', undefined, 'wf-detail-sec');
        sec.append(el('h3', '已核验交付产物'));
        for (const [name, out] of Object.entries(art.outputs || {})) {
          const card = el('div', undefined, 'wf-artifact-card');
          card.append(
            el('strong', `表 · ${name} (v${out.version})`),
            el('div', `${out.row_count} 行 · ${out.uri}`, 'muted small wf-break'),
          );
          sec.append(card);
        }
        for (const m of (art.models_list || [])) {
          const card = el('div', undefined, 'wf-artifact-card');
          const fileKeys = Object.keys(m.files || {}).join(', ');
          card.append(
            el('strong', `模型 · final_step=${m.final_step}`),
            el('div', `digest: ${(m.manifest_digest || '').slice(0, 16)}… · 文件: ${fileKeys || '无'}`, 'muted small wf-break'),
          );
          sec.append(card);
        }
        body.append(sec);
      }

      detailRoot.append(body);
    }

    async function openLaunchDialog() {
      if (!currentInspect?.spec) return;
      const spec = currentInspect.spec;
      const projects = currentInspect.projects || [];
      const dialog = el('dialog', undefined, 'wf-launch-dialog');
      dialog.setAttribute('aria-label', '运行多管线编排');
      const form = el('form', undefined, 'wf-launch-form');
      form.append(el('h2', '运行编排 · ' + spec.name));
      if (spec.description) form.append(el('p', spec.description, 'muted small'));

      // Involved projects permission check summary.
      const projBox = el('div', undefined, 'wf-dialog-section');
      projBox.append(el('strong', '涉及项目与权限核对'));
      for (const p of projects) {
        const ok = p.resolved && p.permissions?.run_submit;
        projBox.append(el('div',
          `${ok ? '✓' : '✕'} ${p.is_self ? '本项目' : '跨项目'} · ${p.name || p.alias} — ${ok ? '已授权运行' : (p.error || '未开启管线运行权限')}`,
          'wf-perm-line ' + (ok ? 'is-ok' : 'is-bad')));
      }
      form.append(projBox);

      // Top-level inputs.
      const inputEntries = Object.entries(spec.inputs || {});
      const inputFields = {};
      if (inputEntries.length) {
        const inBox = el('div', undefined, 'wf-dialog-section');
        inBox.append(el('strong', '编排输入参数'));
        for (const [key, info] of inputEntries) {
          const row = el('label', undefined, 'wf-input-row');
          row.append(el('span', `${key}${info.required ? ' *' : ''} (${info.type || 'any'})`, 'wf-input-label'));
          if (info.description) row.append(el('span', info.description, 'muted small'));
          let ctrl;
          if (info.type === 'boolean') {
            ctrl = el('input'); ctrl.type = 'checkbox'; ctrl.checked = Boolean(info.default);
          } else {
            ctrl = el('input');
            ctrl.type = (info.type === 'integer' || info.type === 'number') ? 'number' : 'text';
            if (info.type === 'number') ctrl.step = 'any';
            if (info.default !== undefined && info.default !== null) {
              ctrl.value = typeof info.default === 'object' ? JSON.stringify(info.default) : String(info.default);
            }
            ctrl.required = Boolean(info.required && info.default === undefined);
          }
          ctrl.dataset.inputKey = key;
          inputFields[key] = {ctrl, info};
          row.append(ctrl);
          inBox.append(row);
        }
        form.append(inBox);
      }

      // Optional reuse_run selectors per node (fetched lazily from each target project's succeeded runs).
      const reuseDetails = el('details', undefined, 'wf-dialog-section wf-reuse-details');
      reuseDetails.append(el('summary', '可选：复用已有成功运行（跳过已跑完的节点）'));
      const reuseHost = el('div', undefined, 'wf-reuse-list');
      reuseDetails.append(reuseHost);
      const reuseSelects = {};
      const runsByProject = new Map();
      await Promise.all(projects.filter(p => p.resolved && (p.workspace_id || p.is_self)).map(async p => {
        const wsId = p.is_self ? workspaceId : p.workspace_id;
        try {
          const res = await api('runs', {limit: 30}, undefined, wsId);
          runsByProject.set(p.alias, (res.runs || []).filter(r => r.status === 'succeeded'));
        } catch { runsByProject.set(p.alias, []); }
      }));
      for (const n of spec.nodes || []) {
        const row = el('label', undefined, 'wf-reuse-row');
        row.append(el('span', `${n.title || n.id} (${n.entry})`, 'wf-input-label'));
        const sel = el('select');
        sel.dataset.reuseNode = n.id;
        sel.append(new Option('重新执行此节点', ''));
        const candidates = (runsByProject.get(n.project) || []).filter(r => r.entry === n.entry);
        for (const r of candidates) {
          sel.append(new Option(`复用 ${r.run_id.slice(0, 10)} · ${fmtTime(r.created_at)}`, r.run_id));
        }
        reuseSelects[n.id] = sel;
        row.append(sel);
        reuseHost.append(row);
      }
      form.append(reuseDetails);

      const errBox = el('div', '', 'wf-banner wf-banner-error'); errBox.hidden = true;
      form.append(errBox);

      const btns = el('div', undefined, 'wf-dialog-actions');
      const cancelBtn = el('button', '取消'); cancelBtn.type = 'button'; cancelBtn.onclick = () => dialog.close();
      const submitBtn = el('button', '启动编排', 'primary'); submitBtn.type = 'submit';
      btns.append(cancelBtn, submitBtn);
      form.append(btns);

      form.onsubmit = async ev => {
        ev.preventDefault();
        errBox.hidden = true;
        submitBtn.disabled = true;
        try {
          const inputsPayload = {};
          for (const [key, {ctrl, info}] of Object.entries(inputFields)) {
            if (info.type === 'boolean') {
              inputsPayload[key] = Boolean(ctrl.checked);
            } else {
              const raw = ctrl.value.trim();
              if (!raw) continue;
              if (info.type === 'integer') {
                const n = Number.parseInt(raw, 10);
                if (!Number.isSafeInteger(n)) throw new Error(`参数 ${key} 需要整数`);
                inputsPayload[key] = n;
              } else if (info.type === 'number') {
                const n = Number(raw);
                if (!Number.isFinite(n)) throw new Error(`参数 ${key} 需要数字`);
                inputsPayload[key] = n;
              } else if (info.type === 'object' || info.type === 'array') {
                inputsPayload[key] = JSON.parse(raw);
              } else {
                inputsPayload[key] = raw;
              }
            }
          }
          const reusePayload = {};
          for (const [nid, sel] of Object.entries(reuseSelects)) {
            if (sel.value) reusePayload[nid] = sel.value;
          }
          const body = {path: selectedPath};
          if (Object.keys(inputsPayload).length) body.inputs = inputsPayload;
          if (Object.keys(reusePayload).length) body.reuse_runs = reusePayload;
          const started = await api('workflow-start', {}, body);
          dialog.close();
          selectedRunId = started.wf_run_id;
          currentRun = started;
          await refreshRunsFor(selectedPath);
          renderStage();
          if (selectedNodeId) renderNodeDetail(selectedNodeId);
          schedulePoll();
          refreshListQuiet();
        } catch (err) {
          errBox.textContent = err?.message || String(err);
          errBox.hidden = false;
        } finally {
          submitBtn.disabled = false;
        }
      };

      dialog.append(form);
      document.body.append(dialog);
      dialog.addEventListener('close', () => dialog.remove(), {once: true});
      dialog.showModal();
    }

    async function doStop() {
      if (!selectedRunId || busyAction) return;
      busyAction = true; renderHeader();
      try {
        currentRun = await api('workflow-cancel', {}, {wf_run_id: selectedRunId, expected_attempt: currentRun?.attempt});
        renderStage();
        if (selectedNodeId) renderNodeDetail(selectedNodeId);
        schedulePoll();
      } catch (err) { error?.(root, err); }
      finally { busyAction = false; renderHeader(); }
    }

    async function doReconcile() {
      if (!selectedRunId || busyAction) return;
      busyAction = true; renderHeader();
      try {
        currentRun = await api('workflow-reconcile', {}, {wf_run_id: selectedRunId});
        renderStage();
        if (selectedNodeId) renderNodeDetail(selectedNodeId);
        refreshListQuiet();
      } catch (err) { error?.(root, err); }
      finally { busyAction = false; renderHeader(); }
    }

    async function doResume(fromNode = null) {
      if (!selectedRunId || busyAction) return;
      busyAction = true; renderHeader();
      try {
        const body = {wf_run_id: selectedRunId, ...(fromNode ? {from_node: fromNode} : {})};
        currentRun = await api('workflow-resume', {}, body);
        renderStage();
        if (selectedNodeId) renderNodeDetail(selectedNodeId);
        schedulePoll();
        refreshListQuiet();
      } catch (err) { error?.(root, err); }
      finally { busyAction = false; renderHeader(); }
    }

    runBtn.onclick = () => openLaunchDialog();
    stopBtn.onclick = () => doStop();
    resumeBtn.onclick = () => doResume(null);
    reconcileBtn.onclick = () => doReconcile();
    sourceBtn.onclick = () => { if (selectedPath) onOpenSource?.(selectedPath); };
    runPicker.onchange = async () => {
      const nextId = runPicker.value || null;
      selectedRunId = nextId;
      clearPoll();
      if (!nextId) {
        currentRun = null;
        renderStage();
        if (selectedNodeId) renderNodeDetail(selectedNodeId);
        return;
      }
      try {
        currentRun = await api('workflow-run', {wf_run_id: nextId});
        renderStage();
        if (selectedNodeId) renderNodeDetail(selectedNodeId);
        schedulePoll();
      } catch (err) { error?.(root, err); }
    };

    async function refreshRunsFor(path) {
      try {
        const res = await api('workflow-runs', {workflow_path: path, limit: 20});
        if (disposed || selectedPath !== path) return;
        currentRuns = res.runs || [];
      } catch {
        currentRuns = [];
      }
    }

    async function openWorkflow(path, {wfRunId = null} = {}) {
      if (disposed || !path) return;
      clearPoll();
      active = true;
      selectedPath = path;
      selectedNodeId = null;
      onActivate?.();
      renderSidebar();
      try {
        const [insp, runsRes] = await Promise.all([
          api('workflow-inspect', {path}),
          api('workflow-runs', {workflow_path: path, limit: 20}).catch(() => ({runs: []})),
        ]);
        if (disposed || !active || selectedPath !== path) return;
        currentInspect = insp;
        currentRuns = runsRes.runs || [];
        const pickRunId = wfRunId || (currentRuns[0]?.wf_run_id || null);
        selectedRunId = pickRunId;
        if (pickRunId) {
          try { currentRun = await api('workflow-run', {wf_run_id: pickRunId}); }
          catch { currentRun = null; selectedRunId = null; }
        } else {
          currentRun = null;
        }
        if (disposed || !active || selectedPath !== path) return;
        renderStage();
        schedulePoll();
      } catch (err) {
        if (!disposed) error?.(root, err);
      }
    }

    async function loadList() {
      try {
        const res = await api('workflows');
        if (disposed) return workflows;
        workflows = res.workflows || [];
        renderSidebar();
        return workflows;
      } catch {
        workflows = [];
        renderSidebar();
        return [];
      }
    }

    function deactivate() {
      if (!active) return;
      active = false;
      clearPoll();
      renderSidebar();
    }

    return {
      loadList,
      openWorkflow,
      deactivate,
      isActive: () => active,
      hasWorkflows: () => workflows.length > 0,
      currentPath: () => selectedPath,
      setFilter: q => { filterQuery = q || ''; renderSidebar(); },
      closeDetail: () => {
        selectedNodeId = null;
        graphRenderer?.select(null);
      },
      dispose: () => {
        disposed = true;
        clearPoll();
        graphRenderer?.dispose?.();
        graphRenderer = null;
        navSection.remove();
      },
    };
  }

  globalThis.ForgeWorkflowPanel = Object.freeze({mount});
})();
