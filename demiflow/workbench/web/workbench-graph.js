/* Bounded graph presentation: one unified mount/update controller (C05/C06).
 * Consumes only GraphModel from ForgeWorkbenchPresentation; no format dispatch,
 * no second canvas. status/cpuChart keep their existing live consumers. */
(() => {
  'use strict';
  const ns = 'http://www.w3.org/2000/svg';
  const svg = (tag, attrs = {}, text) => {
    const node = document.createElementNS(ns, tag);
    for (const [k, v] of Object.entries(attrs)) node.setAttribute(k, String(v));
    if (text !== undefined) node.textContent = text;
    return node;
  };
  const MAX_NODES = 256, MAX_EDGES = 512;
  const kindLabel = {source_call: '源码调用', read_table: '数据输入', write_table: '输出', map: '自定义算子', join: '关联', filter: '筛选', query: '查询', reduce: '聚合'};
  const states = {pending: '已登记 · 未开始', running: '运行中', succeeded: '执行完成', failed: '执行失败', incomplete: '观测不完整', reused: '复用物化结果', cancelled: '已取消'};
  const EDGE_LABEL = {data: '数据流', value: '值依赖', action: '动作先后', reference: '源码引用 · 类型未核验'};
  const TONE_GLYPH = {neutral: '· ', success: '✓ ', failed: '✕ ', running: '▸ '};

  function status(node, snapshot) {
    if (!snapshot) return {label: node.kind === 'source_call' ? '尚未检查' : '未运行', tone: 'neutral'};
    const records = (snapshot.observations || []).filter(o => o.node_id === node.id);
    const output = (snapshot.outputs || []).find(o => o.action_id === node.id);
    if (output && output.status === 'committed') return {label: output.result ? '已提交并核验' : '已提交 · 登记待完成', tone: 'success'};
    if (snapshot.observation_availability === 'unsupported') return {label: '历史未记录节点观测', tone: 'neutral'};
    if (snapshot.observation_scope === 'paged_run_observations' && !Array.isArray(snapshot.observations)) return {label: '观测尚未载入', tone: 'neutral'};
    if (!records.length) return {label: snapshot.observations_truncated ? '摘要未载入 · 点击核对' : '未单独观测', tone: 'neutral'};
    const unique = [...new Set(records.map(r => r.state))];
    const label = records.length === 1 ? (states[unique[0]] || unique[0]) : records.length + ' 次观测 · ' + (unique.length === 1 ? (states[unique[0]] || unique[0]) : '多种状态');
    return {label, tone: unique.includes('failed') ? 'failed' : unique.includes('running') ? 'running' : unique.every(s => ['succeeded', 'reused'].includes(s)) ? 'success' : 'neutral'};
  }

  /* C05: bounded DAG layering + fork layout, stable in model order, O(V+E). */
  function layoutGraph(model, {width = 0, nodeWidth = 200, nodeHeight = 116, gapX = 36, gapY = 20, direction = 'horizontal'} = {}) {
    const nodes = model.nodes, edges = model.edges;
    if (nodes.length > MAX_NODES || edges.length > MAX_EDGES) throw new Error('图超过显示预算');
    const index = new Map(nodes.map((node, i) => [node.key, i]));
    const incoming = new Array(nodes.length).fill(0);
    const outgoing = Array.from({length: nodes.length}, () => []);
    for (const edge of edges) {
      const a = index.get(edge.from), b = index.get(edge.to);
      if (a === undefined || b === undefined || a === b) continue;
      outgoing[a].push(b); incoming[b]++;
    }
    const level = new Array(nodes.length).fill(0);
    const queue = [];
    for (let i = 0; i < nodes.length; i++) if (incoming[i] === 0) queue.push(i);
    let head = 0, visited = 0;
    while (head < queue.length) {
      const a = queue[head++]; visited++;
      for (const b of outgoing[a]) {
        level[b] = Math.max(level[b], level[a] + 1);
        if (--incoming[b] === 0) queue.push(b);
      }
    }
    if (visited < nodes.length) for (let i = 0; i < nodes.length; i++) if (incoming[i] > 0) level[i] = 0;
    const columns = new Map();
    for (let i = 0; i < nodes.length; i++) {
      if (!columns.has(level[i])) columns.set(level[i], []);
      columns.get(level[i]).push(i);
    }
    const levels = [...columns.keys()].sort((a, b) => a - b);
    const colIndex = new Map(levels.map((l, i) => [l, i]));
    const maxRows = Math.max(1, ...[...columns.values()].map(c => c.length));
    if(direction==='vertical'){
      const positions=new Array(nodes.length),contentWidth=maxRows*(nodeWidth+gapX)-gapX;
      for(const l of levels){const col=columns.get(l),offset=(contentWidth-(col.length*(nodeWidth+gapX)-gapX))/2;
        col.forEach((i,row)=>{positions[i]={key:nodes[i].key,x:24+offset+row*(nodeWidth+gapX),y:24+colIndex.get(l)*(nodeHeight+64),width:nodeWidth,height:nodeHeight};});}
      return {nodes:positions,width:Math.max(width,48+contentWidth),height:levels.length?48+levels.length*(nodeHeight+64)-64:0};
    }
    const contentHeight = maxRows * (nodeHeight + gapY) - gapY;
    const positions = new Array(nodes.length);
    for (const l of levels) {
      const col = columns.get(l);
      const colHeight = col.length * (nodeHeight + gapY) - gapY;
      const offsetY = (contentHeight - colHeight) / 2;
      col.forEach((i, row) => {
        positions[i] = {key: nodes[i].key, x: 24 + colIndex.get(l) * (nodeWidth + gapX), y: 24 + offsetY + row * (nodeHeight + gapY), width: nodeWidth, height: nodeHeight};
      });
    }
    const computedWidth = levels.length ? 48 + levels.length * (nodeWidth + gapX) - gapX : 0;
    const computedHeight = nodes.length ? 48 + contentHeight : 0;
    return {nodes: positions, width: Math.max(width || 0, computedWidth), height: computedHeight};
  }

  function cpuChart(host, points) {
    host.replaceChildren();
    if (!Array.isArray(points) || points.length > 600) throw new Error('性能窗口超过预算');
    const valid = points.filter(p => Number.isFinite(p.cpu_cores));
    if (!valid.length) {host.textContent = '此窗口没有 CPU 测量；缺测保持为空。'; return;}
    const first = points[0].time, last = points.at(-1).time, max = Math.max(1, ...valid.map(p => p.cpu_cores));
    const canvas = svg('svg', {viewBox: '0 0 600 135', role: 'img', 'aria-label': '整次运行的 CPU 核数趋势'}); let segment = [];
    const flush = () => {if (segment.length) canvas.append(svg('polyline', {points: segment.join(' '), fill: 'none', stroke: 'currentColor', 'stroke-width': 2})); segment = [];};
    for (const point of points) {if (!Number.isFinite(point.cpu_cores)) {flush(); continue;} segment.push((8 + 584 * (point.time - first) / Math.max(1, last - first)) + ',' + (124 - 112 * point.cpu_cores / max));} flush(); host.append(canvas);
  }

  /* C06: single mount/update controller; consumes only GraphModel. */
  function mount(host, {onSelect = () => {}} = {}) {
    const wrapper = document.createElement('div'); wrapper.className = 'wb-graph-view';
    const toolbar = document.createElement('div'); toolbar.className = 'wb-graph-bar';
    const notices = document.createElement('div'); notices.className = 'wb-graph-notices'; notices.setAttribute('role', 'status');
    const scroll = document.createElement('div'); scroll.className = 'wb-graph-scroll';
    const empty = document.createElement('div'); empty.className = 'wb-graph-empty';
    const canvas = svg('svg', {class: 'wb-graph-canvas', role: 'group', 'aria-label': '数据算子图'});
    scroll.append(canvas, empty);
    function toolButton(label, fn) {
      const button = document.createElement('button');
      button.type = 'button'; button.textContent = label; button.setAttribute('aria-label', label);
      button.addEventListener('click', fn); return button;
    }
    toolbar.append(
      toolButton('可读布局', () => readable()),
      toolButton('适应全图', () => {viewMode === 'fit' ? readable() : fit();}),
      toolButton('缩小', () => zoomBy(1 / 1.2)),
      toolButton('放大', () => zoomBy(1.2)));
    const directionButton=toolButton('改为纵向',()=>{directionChosen=true;direction=direction==='horizontal'?'vertical':'horizontal';if(currentModel){rebuild(currentModel);highlight(currentModel.selected);}});
    const zoomText=document.createElement('span');zoomText.className='wb-zoom-level';zoomText.setAttribute('aria-live','polite');
    toolbar.append(directionButton,zoomText);
    wrapper.append(toolbar, notices, scroll);
    host.replaceChildren(wrapper);

    let layout = null, fingerprint = null, zoomLevel = 1, viewMode = 'readable', direction='horizontal', directionChosen=false, currentModel=null;
    let width = 0, height = 0, resizeFrame = null, disposed = false;
    const elements = new Map();

    function size() {
      canvas.style.width = Math.round(width * zoomLevel) + 'px';
      canvas.style.height = Math.round(height * zoomLevel) + 'px';
      zoomText.textContent=Math.round(zoomLevel*100)+'%';
    }
    function readable() {viewMode = 'readable'; zoomLevel = 1; size();}
    function fit() {
      viewMode = 'fit';
      const w = scroll.clientWidth || host.clientWidth || 600, h = scroll.clientHeight || host.clientHeight || 300;
      zoomLevel = Math.max(.01, Math.min(1, (w - 16) / Math.max(1, width), (h - 16) / Math.max(1, height)));
      size();
    }
    function zoomBy(factor) {viewMode = 'manual'; zoomLevel = Math.max(.01, Math.min(2, zoomLevel * factor)); size();}
    const resize = typeof ResizeObserver === 'function' ? new ResizeObserver(() => {
      if (disposed || resizeFrame !== null) return;
      resizeFrame = requestAnimationFrame(() => {resizeFrame = null; if (viewMode === 'fit') fit();});
    }) : null;
    if (resize) resize.observe(scroll);

    function structFingerprint(model) {
      return JSON.stringify({
        b: model.binding, c: model.choice, e: model.execution,
        n: model.nodes.map(card => [card.key, card.selection.key, card.occurrences.map(m => m.key)]),
        ed: model.edges, cp: model.completeness,
      });
    }
    function charWidth(ch, fontSize) {return /[\u1100-\uFFFF]/.test(ch) ? fontSize : fontSize * .58;}
    function wrapText(text, maxWidth, fontSize, maxLines) {
      const lines = []; let line = '', lineWidth = 0;
      for (const ch of String(text)) {
        const w = charWidth(ch, fontSize);
        if (lineWidth + w > maxWidth && line) {
          lines.push(line); line = ch; lineWidth = w;
          if (lines.length === maxLines) {lines[maxLines - 1] = lines[maxLines - 1].slice(0, -1) + '…'; return lines;}
        } else {line += ch; lineWidth += w;}
      }
      if (line) lines.push(line);
      return lines.slice(0, maxLines);
    }
    function fitWidth(text, maxWidth, fontSize) {
      let line = '', lineWidth = 0;
      for (const ch of String(text)) {
        const w = charWidth(ch, fontSize);
        if (lineWidth + w > maxWidth) return line + '…';
        line += ch; lineWidth += w;
      }
      return line;
    }
    function summaryText(line) {
      if (!line) return '';
      if (line.value === null || line.value === undefined) return line.label + '：未知' + (line.reason ? '（' + line.reason + '）' : '');
      return line.label + '：' + line.value;
    }
    function drawEdges(model, pos) {
      const markerId = 'wb-arrow-' + Math.random().toString(16).slice(2);
      const defs = svg('defs');
      const marker = svg('marker', {id: markerId, viewBox: '0 0 10 10', refX: 9, refY: 5, markerWidth: 5, markerHeight: 5, orient: 'auto'});
      marker.append(svg('path', {d: 'M0 0 L10 5 L0 10z', fill: '#8e9eb8'}));
      defs.append(marker); canvas.append(defs);
      const groups = new Map();
      for (const edge of model.edges) {
        const k = edge.from + '>' + edge.to;
        if (!groups.has(k)) groups.set(k, []);
        groups.get(k).push(edge);
      }
      for (const list of groups.values()) {
        const a = pos.get(list[0].from), b = pos.get(list[0].to);
        if (!a || !b) continue;
        list.forEach((edge, idx) => {
          const fraction = (idx + 1) / (list.length + 1);
          let d, labelX, labelY;
          if (edge.from === edge.to) {
            const x = a.x + a.width, y = a.y + a.height * fraction;
            d = `M${x} ${y} C${x + 46} ${y - 30},${x + 46} ${y + 30},${x} ${Math.min(a.y + a.height - 8, y + 18)}`;
            labelX = x + 40; labelY = y;
          } else if(direction==='vertical'&&b.y>a.y){
            const x1=a.x+a.width*fraction,y1=a.y+a.height,x2=b.x+b.width*fraction,y2=b.y,my=(y1+y2)/2;
            d=`M${x1} ${y1} C${x1} ${my},${x2} ${my},${x2} ${y2}`;
            labelX=(x1+x2)/2+30;labelY=my-5;
          } else if (b.x > a.x) {
            const x1 = a.x + a.width, y1 = a.y + a.height * fraction, x2 = b.x, y2 = b.y + b.height * fraction, mx = (x1 + x2) / 2;
            d = `M${x1} ${y1} C${mx} ${y1},${mx} ${y2},${x2} ${y2}`;
            labelX = mx; labelY = (y1 + y2) / 2 - 6;
          } else {
            const x1 = a.x + a.width / 2, y1 = a.y + a.height, x2 = b.x + b.width / 2, y2 = b.y + b.height, dip = Math.max(y1, y2) + 34 + idx * 12;
            d = `M${x1} ${y1} C${x1} ${dip},${x2} ${dip},${x2} ${y2}`;
            labelX = (x1 + x2) / 2; labelY = dip - 4;
          }
          const path = svg('path', {d, class: 'wb-edge wb-edge-' + edge.kind, 'marker-end': 'url(#' + markerId + ')'});
          path.append(svg('title', {}, (EDGE_LABEL[edge.kind] || edge.kind) + ' · ' + (edge.fromPort || '—') + ' → ' + (edge.toPort || '—')));
          canvas.append(path);
          if ((edge.fromPort || edge.toPort) && !(edge.fromPort==='main'&&edge.toPort==='main'))
            canvas.append(svg('text', {x: labelX, y: labelY, class: 'wb-edge-port'}, (edge.fromPort || '—') + '→' + (edge.toPort || '—')));
        });
      }
    }
    function drawCards(model, pos) {
      for (const card of model.nodes) {
        const p = pos.get(card.key); if (!p) continue;
        const group = svg('g', {class: 'wb-node', transform: `translate(${p.x} ${p.y})`, tabindex: 0, role: 'button', 'aria-pressed': 'false'});
        const kindText = svg('text', {x: 14, y: 22, class: 'wb-node-kind'});
        const titleLines = [svg('text', {x: 14, y: 42, class: 'wb-node-title'}), svg('text', {x: 14, y: 60, class: 'wb-node-title'})];
        const summaryLines = [svg('text', {x: 14, y: 78, class: 'wb-node-summary'}), svg('text', {x: 14, y: 94, class: 'wb-node-summary'})];
        const stateText = svg('text', {x: 14, y: 110, class: 'wb-node-state'});
        const tip = svg('title', {});
        group.append(svg('rect', {width: p.width, height: p.height, rx: 10}), kindText, ...titleLines, ...summaryLines, stateText, tip);
        const activate = () => {if (!disposed) onSelect(card.selection);};
        group.addEventListener('click', activate);
        group.addEventListener('keydown', event => {if (event.key === 'Enter' || event.key === ' ') {event.preventDefault(); activate();}});
        elements.set(card.key, {group, kindText, titleLines, summaryLines, stateText, tip, card, width: p.width});
        canvas.append(group);
      }
    }
    function paintCard(card) {
      const el = elements.get(card.key); if (!el) return;
      el.card = card;
      const memberCount = card.occurrences.length;
      el.kindText.textContent = fitWidth(card.kindLabel + (memberCount > 1 ? ' · ×' + memberCount : ''), el.width - 28, 12);
      const titleLines = wrapText(card.title, el.width - 28, 14, 2);
      el.titleLines.forEach((node, i) => {node.textContent = titleLines[i] || '';});
      el.summaryLines.forEach((node, i) => {node.textContent = fitWidth(summaryText(card.summary[i]), el.width - 28, 12);});
      el.stateText.textContent = fitWidth((TONE_GLYPH[card.state.tone] || '') + card.state.label, el.width - 28, 12);
      el.group.dataset.state = card.state.tone;
      const facts = card.summary.map(summaryText).filter(Boolean);
      const sealed = card.selection.execution !== null;
      el.group.setAttribute('aria-label', card.title + ' · ' + card.kindLabel + (facts.length ? ' · ' + facts.join(' · ') : '') + ' · ' + card.state.label);
      el.tip.textContent = card.title + '\n' + card.kindLabel + '\n' + card.selection.key + (memberCount ? '\n成员 ' + memberCount + ' 次出现' : '') + (facts.length ? '\n' + facts.join('\n') : '') + (sealed ? '\n物理 stage/profile/exchange 与恢复限制见节点详情' : '');
    }
    function renderNotices(model) {
      notices.replaceChildren();
      const list = model.notices || [];
      notices.hidden = list.length === 0;
      for (const text of list.slice(0, 6)) {
        const line = document.createElement('div'); line.className = 'wb-graph-notice'; line.textContent = text; notices.append(line);
      }
    }
    function highlight(key) {
      for (const [k, el] of elements) {
        const on = key !== null && key !== undefined && (k === key || el.card.occurrences.some(m => m.key === key));
        el.group.classList.toggle('selected', on);
        el.group.setAttribute('aria-pressed', String(on));
      }
    }
    function scrollToKey(key) {
      if (!layout) return;
      const p = layout.nodes.find(n => n.key === key); if (!p) return;
      const left = p.x * zoomLevel, top = p.y * zoomLevel, right = left + p.width * zoomLevel, bottom = top + p.height * zoomLevel;
      if (left < scroll.scrollLeft) scroll.scrollLeft = Math.max(0, left - 16);
      else if (right > scroll.scrollLeft + scroll.clientWidth) scroll.scrollLeft = right - scroll.clientWidth + 16;
      if (top < scroll.scrollTop) scroll.scrollTop = Math.max(0, top - 16);
      else if (bottom > scroll.scrollTop + scroll.clientHeight) scroll.scrollTop = bottom - scroll.clientHeight + 16;
    }
    function rebuild(model) {
      canvas.replaceChildren(); elements.clear();
      if(!directionChosen)direction=model.nodes.length>12?'vertical':'horizontal';
      layout = layoutGraph(model, {width: scroll.clientWidth || host.clientWidth || 0,direction});
      directionButton.textContent=direction==='horizontal'?'改为纵向':'改为横向';
      directionButton.setAttribute('aria-label',directionButton.textContent);
      width = layout.width; height = layout.height;
      canvas.setAttribute('viewBox', `0 0 ${width} ${height}`);
      canvas.setAttribute('aria-label', model.choice.kind === 'sealed' ? '运行封存计划' : '数据算子图');
      const pos = new Map(layout.nodes.map(n => [n.key, n]));
      empty.hidden = model.nodes.length > 0;
      empty.textContent = model.nodes.length ? '' : model.choice.label;
      drawEdges(model, pos);
      drawCards(model, pos);
      for (const card of model.nodes) paintCard(card);
      readable();
    }
    function update(model) {
      if (disposed || !model) return;
      currentModel=model;
      if (!host.contains(wrapper)) host.replaceChildren(wrapper);
      const fp = structFingerprint(model);
      if (fingerprint === fp && elements.size) {
        for (const card of model.nodes) paintCard(card);
      } else {
        fingerprint = fp;
        rebuild(model);
      }
      renderNotices(model);
      highlight(model.selected);
    }
    function select(key) {highlight(key); scrollToKey(key);}
    function dispose() {
      if (disposed) return;
      disposed = true;
      if (resize) resize.disconnect();
      if (resizeFrame !== null) cancelAnimationFrame(resizeFrame);
      resizeFrame = null;
      elements.clear();
      wrapper.remove();
    }
    return Object.freeze({update, select, fit, zoom: zoomBy, dispose});
  }

  globalThis.ForgeWorkbenchGraph = Object.freeze({mount, layoutGraph, status, cpuChart});
})();
