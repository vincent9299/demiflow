/* Forge text reading. No project state, HTTP client, credentials or HTML insertion.
 * Also imported by the fixed markdown-worker.js so online and offline use one
 * parser/projection implementation. Byte/node limits are not an RSS limit. */
(() => {
  'use strict';
  const limits = Object.freeze({
    bytes: 65536, lines: 4096, nesting: 16, tokens: 6000, depth: 24,
    nodes: 8000, messageBytes: 524288, linkBytes: 2048,
    parseMs: 500, startupMs: 2000, sourceChunkBytes: 16384,
    sourceChunks: 4, sourceBytes: 65536
  });
  // Project Markdown files use the server's 1 MiB read bound. The worker,
  // deadline, nesting, DOM and message caps still apply to the full document.
  const documentBytes = 1024 * 1024, documentTokens = 8192;
  const tags = new Set(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'ul',
    'ol', 'li', 'blockquote', 'em', 'strong', 's', 'table', 'thead',
    'tbody', 'tr', 'th', 'td']);
  const leafTags = new Set(['br', 'hr', 'code', 'pre', 'image-note', 'table-wrap', 'link']);
  const reasons = Object.freeze({
    documentBytes: '文档超过 1 MiB 预览上限', documentTokens: '文档结构超过 8192 token 预览上限',
    bytes: '内容超过 64 KiB 预览预算', lines: '内容超过 4096 行预览预算',
    nesting: 'Markdown 达到解析嵌套边界', tokens: 'Markdown 超过 6000 token 预算',
    depth: '预览树超过 24 层深度预算', nodes: '预览超过 8000 个 DOM 节点预算',
    message: '预览结果超过 512 KiB 消息预算', link: '链接超过 2048 字节预算',
    structure: 'Markdown 结构无法完整预览', parser: '本地 Markdown 解析器不可用',
    timeout: '预览超时（解析预算 500 ms）', startup: '预览 worker 启动超时（预算 2000 ms）',
    worker: '预览 worker 不可用', superseded: '等待的预览已被更新的选择替代'
  });
  function fail(reason) { throw new Error(reason); }
  function width(text, i) {
    const c = text.charCodeAt(i);
    if (c < 0x80) return [1, 1];
    if (c < 0x800) return [2, 1];
    if (c >= 0xd800 && c <= 0xdbff && i + 1 < text.length) {
      const next = text.charCodeAt(i + 1);
      if (next >= 0xdc00 && next <= 0xdfff) return [4, 2];
    }
    return [3, 1]; // TextEncoder also replaces an unpaired surrogate with U+FFFD.
  }
  function byteLength(text, ceiling = Infinity) {
    let size = 0;
    for (let i = 0; i < text.length;) {
      const [bytes, units] = width(text, i);
      size += bytes;
      if (size > ceiling) return size;
      i += units;
    }
    return size;
  }
  function admit(text, documentView = false) {
    // No full-size encode/split allocation, including for a rejected large value.
    if (typeof text !== 'string') fail('structure');
    const cap = documentView ? documentBytes : limits.bytes;
    if (text.length > cap || byteLength(text, cap) > cap) fail(documentView ? 'documentBytes' : 'bytes');
    let lines = 1;
    for (let i = 0; i < text.length; i++) {
      if (text[i] === '\r') { if (text[i + 1] === '\n') i++; lines++; }
      else if (text[i] === '\n') lines++;
      if (lines > limits.lines) fail('lines');
    }
  }
  // Count JSON UTF-8 string bytes without allocating a JSON string. The final
  // bounded packet is encoded into a transferable buffer, not a cloned tree.
  function jsonStringBytes(text, ceiling = limits.messageBytes) {
    let size = 2;
    for (let i = 0; i < text.length;) {
      const c = text.charCodeAt(i), [bytes, units] = width(text, i);
      if (c === 34 || c === 92) size += 2;
      else if (c < 32) size += [8, 9, 10, 12, 13].includes(c) ? 2 : 6;
      else if (c >= 0xd800 && c <= 0xdfff && units === 1) size += 6;
      else size += bytes;
      if (size > ceiling) return size;
      i += units;
    }
    return size;
  }
  function budget() {
    // Envelope, id, metrics and all success/error key names fit in this reserve.
    let used = 256, nodes = 0;
    return {
      take(amount) { if (amount > limits.messageBytes - used) fail('message'); used += amount; },
      node(depth) {
        if (depth > limits.depth) fail('depth');
        if (++nodes > limits.nodes) fail('nodes');
      },
      get used() { return used; }, get nodes() { return nodes; }
    };
  }
  let parser;
  function parseTree(text, documentView = false) {
    admit(text, documentView);
    if (!parser) {
      if (typeof globalThis.markdownit !== 'function') fail('parser');
      parser = globalThis.markdownit('default', {
        html: false, linkify: false, typographer: false, maxNesting: limits.nesting
      });
    }
    const tokens = parser.parse(text, {});
    let tokenCount = 0;
    function inspect(items, depth) {
      if (depth > limits.depth) fail('depth');
      for (const token of items) {
        if (++tokenCount > (documentView ? documentTokens : limits.tokens)) fail(documentView ? 'documentTokens' : 'tokens');
        // markdown-it stops recognizing block syntax at maxNesting. Decline a
        // saturated result instead of presenting a partly literal deep preview.
        if (token.level + (token.nesting === 1 ? 1 : 0) >= limits.nesting) fail('nesting');
        if (token.children) inspect(token.children, depth + 1);
      }
    }
    inspect(tokens, 1);
    const meter = budget();
    function element(tag, attr, depth) {
      meter.node(depth);
      // [tag, attr, [children]]; charge before creating or attaching the item.
      meter.take(6 + jsonStringBytes(tag) + (attr === null ? 4 :
        typeof attr === 'number' ? 16 : jsonStringBytes(attr)));
      return [tag, attr, []];
    }
    const root = element('div', null, 1), stack = [root];
    function attach(item) {
      const children = stack[stack.length - 1][2];
      if (children.length) meter.take(1);
      children.push(item);
    }
    function open(tag, attr = null) {
      const item = element(tag, attr, stack.length + 1);
      attach(item); stack.push(item);
    }
    function close(tag) {
      if (stack.length <= 1 || stack[stack.length - 1][0] !== tag) fail('structure');
      stack.pop();
    }
    function textNode(value) {
      meter.node(stack.length + 1);
      meter.take(jsonStringBytes(value));
      attach(value);
    }
    function walk(items) {
      for (const token of items) {
        if (token.hidden) continue;
        if (token.type === 'inline') { walk(token.children || []); continue; }
        if (['text', 'html_inline', 'html_block'].includes(token.type)) { textNode(token.content); continue; }
        if (token.type === 'image') {
          open('image-note'); textNode('图片引用 · ' + (token.content || '未命名图片') + '（未加载）'); close('image-note');
          continue;
        }
        if (token.type === 'softbreak' || token.type === 'hardbreak') { open('br'); close('br'); continue; }
        if (token.type === 'hr') { open('hr'); close('hr'); continue; }
        if (token.type === 'code_inline') { open('code'); textNode(token.content); close('code'); continue; }
        if (token.type === 'fence' || token.type === 'code_block') {
          open('pre'); open('code'); textNode(token.content); close('code'); close('pre'); continue;
        }
        if (token.type === 'link_open') {
          const href = token.attrGet('href') || '';
          if (href.length > limits.linkBytes || byteLength(href, limits.linkBytes) > limits.linkBytes) fail('link');
          open('link', href); continue;
        }
        if (token.type === 'link_close') { close('link'); continue; }
        if (tags.has(token.tag) && token.nesting === 1) {
          if (token.tag === 'table') open('table-wrap');
          let start = null;
          if (token.tag === 'ol') {
            const value = Number(token.attrGet('start'));
            if (Number.isSafeInteger(value) && value > 0 && value <= 1000000) start = value;
          }
          open(token.tag, start); continue;
        }
        if (tags.has(token.tag) && token.nesting === -1) {
          close(token.tag);
          if (token.tag === 'table') close('table-wrap');
          continue;
        }
        fail('structure');
      }
    }
    walk(tokens);
    if (stack.length !== 1) fail('structure');
    return {tree: root, metrics: {tokens: tokenCount, nodes: meter.nodes, messageBytes: meter.used}};
  }
  function reasonOf(error) { return Object.hasOwn(reasons, error.message) ? error.message : 'structure'; }

  if (typeof document === 'undefined' && typeof importScripts === 'function') {
    // Only markdown-worker.js imports this branch. No state/identity is cloned.
    globalThis.onmessage = event => {
      const message = event.data;
      if (!message || !['parse','parse-document'].includes(message.type) || !Number.isSafeInteger(message.id)) return;
      try {
        const result = parseTree(message.text, message.type === 'parse-document');
        // parseTree has already charged every expanded string/node plus the
        // packet envelope. Only this bounded result may be stringified/encoded.
        const bytes = new TextEncoder().encode(JSON.stringify({type: 'result', id: message.id, ...result}));
        if (bytes.byteLength > limits.messageBytes) fail('message');
        globalThis.postMessage(bytes.buffer, [bytes.buffer]);
      } catch (error) {
        globalThis.postMessage({type: 'error', id: message.id, reason: reasonOf(error)});
      }
    };
    globalThis.postMessage({type: 'ready'});
    return;
  }

  // One worker for this module, one running job and at most one latest waiter.
  // Workers are terminated after every result/error/cancellation; no idle pool.
  let active = null, pending = null, serial = 0;
  function release(job) { job.text = null; job.deliver = null; job.owner = null; }
  function settle(job, error, result) {
    const deliver = job.deliver;
    release(job);
    if (deliver) deliver(error, result);
  }
  function drain() {
    if (!active && pending) { const next = pending; pending = null; start(next); }
  }
  function finish(run, error, result) {
    if (active !== run) return;
    active = null;
    clearTimeout(run.timer);
    if (run.worker) {
      run.worker.onmessage = null; run.worker.onerror = null; run.worker.onmessageerror = null;
      run.worker.terminate(); run.worker = null;
    }
    settle(run.job, error, result);
    drain();
  }
  function start(job) {
    const run = {job, worker: null, timer: null, phase: 'startup', began: performance.now()};
    active = run;
    try {
      // Fixed same-origin URL, no blob worker, dynamic source, or caller URL.
      run.worker = new Worker('/markdown-worker.js', {name: 'ForgeReadingMarkdown'});
      run.timer = setTimeout(() => finish(run, 'startup'), limits.startupMs);
      run.worker.onerror = event => { event.preventDefault(); finish(run, 'worker'); };
      run.worker.onmessageerror = () => finish(run, 'worker');
      run.worker.onmessage = event => {
        if (active !== run) return;
        let data = event.data;
        if (!data) { finish(run, 'worker'); return; }
        if (run.phase === 'startup') {
          if (performance.now() - run.began > limits.startupMs) { finish(run, 'startup'); return; }
          if (data.type !== 'ready') { finish(run, 'worker'); return; }
          clearTimeout(run.timer); run.phase = 'parse'; run.began = performance.now();
          run.timer = setTimeout(() => finish(run, 'timeout'), limits.parseMs);
          run.worker.postMessage({type: job.documentView ? 'parse-document' : 'parse', id: job.id, text: job.text});
          return;
        }
        // A delayed event loop cannot publish a result past the elapsed budget.
        if (performance.now() - run.began > limits.parseMs) { finish(run, 'timeout'); return; }
        if (data instanceof ArrayBuffer) {
          if (data.byteLength > limits.messageBytes) { finish(run, 'message'); return; }
          try { data = JSON.parse(new TextDecoder('utf-8', {fatal: true}).decode(data)); }
          catch { finish(run, 'structure'); return; }
          if (!data || typeof data !== 'object') { finish(run, 'structure'); return; }
        }
        if (data.id !== job.id) return;
        if (data.type === 'result') finish(run, null, data.tree);
        else finish(run, Object.hasOwn(reasons, data.reason) ? data.reason : 'worker');
      };
    } catch { finish(run, 'worker'); }
  }
  function schedule(text, owner, deliver, documentView = false) {
    const job = {text, owner, deliver, documentView, id: ++serial};
    if (!active) start(job);
    else {
      const obsolete = pending; pending = job;
      if (obsolete) settle(obsolete, 'superseded');
    }
    return () => {
      if (pending === job) { pending = null; release(job); }
      if (active && active.job === job) {
        job.deliver = null;
        finish(active, null);
      }
    };
  }

  const forbidden = /[\u0000-\u0020\u007f\\]/;
  function credentialKey(value) {
    for (let i = 0; i < 3; i++) {
      const decoded = decodeURIComponent(value);
      if (decoded === value) break;
      value = decoded;
    }
    value = value.replace(/([a-z])([A-Z])/g, '$1_$2');
    return /(?:^|[^a-z0-9])(?:token|key|secret|password|authorization|credential|signature|session|auth)(?:$|[^a-z0-9])/i.test(value) || /%/.test(value);
  }
  function externalURL(href) {
    try {
      if (!/^https?:\/\//i.test(href) || forbidden.test(href) || /%(?:0[0-9a-f]|1[0-9a-f]|20|7f|5c)/i.test(href)) return null;
      const url = new URL(href);
      if (!['https:', 'http:'].includes(url.protocol) || url.username || url.password) return null;
      for (const key of url.searchParams.keys()) if (credentialKey(key)) return null;
      for (const key of new URLSearchParams(url.hash.slice(1)).keys()) if (credentialKey(key)) return null;
      return url.href;
    } catch { return null; }
  }
  function relativeReference(href) {
    if (!href || forbidden.test(href) || /[:%?]/.test(href) || /^[\/#]/.test(href)) return false;
    const path = href.split('#', 1)[0];
    return path.split('/').every((part, i) => part && (!part.startsWith('.') || (part === '.' && i === 0)));
  }
  function slug(value) {
    return value.trim().toLowerCase().replace(/[^\p{L}\p{N}_\s-]/gu, '').replace(/\s+/g, '-');
  }
  function make(tag, value, className) {
    const node = document.createElement(tag);
    if (value !== undefined) node.append(document.createTextNode(value));
    if (className) node.className = className;
    return node;
  }
  function buildDOM(tree, resolveLink, listen, report) {
    const meter = budget(), anchors = new Map(), duplicateSlugs = new Map();
    // Reserve more than the fixed section/toolbar/status/content chrome. The
    // published component, including Text nodes and generated notes, stays
    // below 8000 even when the projected tree itself fits exactly at its cap.
    let domNodes = 32;
    function count(depth) {
      if (depth > limits.depth) fail('depth');
      if (++domNodes > limits.nodes) fail('nodes');
    }
    function text(value, depth) { count(depth); return document.createTextNode(value); }
    function elem(tag, depth) { count(depth); return document.createElement(tag); }
    function visit(item, depth, isRoot = false) {
      if (typeof item === 'string') {
        meter.node(depth); meter.take(jsonStringBytes(item));
        return text(item, depth);
      }
      if (!Array.isArray(item) || item.length !== 3) fail('structure');
      const [tag, attr, children] = item;
      if (!(isRoot ? tag === 'div' : tags.has(tag) || leafTags.has(tag)) || !Array.isArray(children)) fail('structure');
      if (tag === 'link') {
        if (typeof attr !== 'string' || attr.length > limits.linkBytes || byteLength(attr, limits.linkBytes) > limits.linkBytes) fail('link');
      } else if (tag === 'ol' && attr !== null) {
        if (!Number.isSafeInteger(attr) || attr < 1 || attr > 1000000) fail('structure');
      } else if (attr !== null) fail('structure');
      meter.node(depth);
      meter.take(6 + jsonStringBytes(tag) + (attr === null ? 4 : typeof attr === 'number' ? 16 : jsonStringBytes(attr)));
      let node, note = '', action = null;
      if (tag === 'link') {
        const safe = externalURL(attr);
        if (safe) {
          node = elem('a', depth); node.href = safe; node.target = '_blank';
          node.rel = 'noopener noreferrer'; node.referrerPolicy = 'no-referrer';
        } else if (attr.startsWith('#')) {
          node = elem('button', depth); node.type = 'button';
          action = () => {
            let target;
            try { target = anchors.get(decodeURIComponent(attr.slice(1))); } catch { /* readable missing target */ }
            if (target) { target.scrollIntoView({block: 'nearest'}); target.focus({preventScroll: true}); }
            else report('文内引用未找到：' + attr);
          };
        } else if (relativeReference(attr) && typeof resolveLink === 'function') {
          let resolved;
          try { resolved = resolveLink(attr); } catch { /* resolver must authorize every supported reference */ }
          if (resolved && typeof resolved.label === 'string' && resolved.label.length <= 200 && typeof resolved.open === 'function') {
            node = elem('button', depth); node.type = 'button'; node.title = resolved.label;
            action = async () => {
              try { await resolved.open(); } catch { report('引用暂时无法打开：' + attr); }
            };
          }
        }
        if (!node) {
          node = elem('span', depth);
          note = '〔引用未支持：' + attr + '〕';
          node.className = 'reading-unsupported';
        } else node.className = 'reading-link';
      } else {
        node = elem(tag === 'table-wrap' ? 'div' : tag === 'image-note' ? 'span' : tag, depth);
        if (tag === 'table-wrap') node.className = 'md-table-wrap';
        if (tag === 'image-note') node.className = 'reading-image-placeholder';
        if (tag === 'th') node.scope = 'col';
        if (tag === 'ol' && attr !== null) node.start = attr;
      }
      for (let i = 0; i < children.length; i++) {
        if (i) meter.take(1);
        node.append(visit(children[i], depth + 1));
      }
      if (note) {
        const small = elem('small', depth + 1); small.className = 'reading-link-reason';
        small.append(text(note, depth + 2)); node.append(small);
      }
      if (action) listen(node, 'click', action);
      if (/^h[1-6]$/.test(tag)) {
        const base = slug(node.textContent), index = duplicateSlugs.get(base) || 0;
        duplicateSlugs.set(base, index + 1);
        anchors.set(base + (index ? '-' + index : ''), node); node.tabIndex = -1;
      }
      return node;
    }
    const result = visit(tree, 3, true); // section -> content -> preview root
    result.className = 'reader reading-preview';
    // Detached construction includes all generated notes and Text nodes, before
    // a single published replacement. A failure discards the complete fragment.
    return result;
  }

  // Chat renders many messages together. It must not share the file reader's
  // "latest selection wins" queue or its persisted preview/source controls.
  // One reusable worker, at most 100 views/1 MiB of source, and one queued
  // reference per view keep history replay and streaming updates bounded.
  const markdownViews = new Set(), markdownQueue = new Set();
  let markdownBytes = 0, markdownWorker = null, markdownReady = false;
  let markdownRun = null, markdownTimer = null, markdownIdle = null, markdownSerial = 0;
  function stopMarkdownWorker() {
    clearTimeout(markdownTimer); clearTimeout(markdownIdle);
    if (markdownWorker) {
      markdownWorker.onmessage = markdownWorker.onerror = markdownWorker.onmessageerror = null;
      markdownWorker.terminate();
    }
    markdownWorker = null; markdownReady = false; markdownRun = null;
  }
  function markdownResult(reason, tree) {
    const run = markdownRun; markdownRun = null; clearTimeout(markdownTimer);
    if (run && !run.view.disposed && run.version === run.view.version) run.view.deliver(reason, tree);
    if (!markdownQueue.size) markdownIdle = setTimeout(stopMarkdownWorker, 1000);
    else pumpMarkdown();
  }
  function markdownFailure(reason) {
    const run = markdownRun;
    if (!run) {
      const batch = [...markdownQueue]; markdownQueue.clear(); stopMarkdownWorker();
      for (const view of batch) if (!view.disposed) view.deliver(reason);
      return;
    }
    stopMarkdownWorker(); markdownRun = run;
    markdownResult(reason);
  }
  function pumpMarkdown() {
    clearTimeout(markdownIdle);
    if (markdownRun || !markdownQueue.size) return;
    if (!markdownWorker) {
      try {
        markdownWorker = new Worker('/markdown-worker.js', {name: 'ForgeChatMarkdown'});
        const worker = markdownWorker;
        markdownTimer = setTimeout(() => {
          // Startup failed before a view was dispatched: fail the bounded
          // pending batch once, rather than restarting for every queued view.
          const batch = [...markdownQueue]; markdownQueue.clear(); stopMarkdownWorker();
          for (const view of batch) if (!view.disposed) view.deliver('startup');
        }, limits.startupMs);
        worker.onerror = event => {if (worker !== markdownWorker) return; event.preventDefault(); markdownFailure('worker');};
        worker.onmessageerror = () => {if (worker === markdownWorker) markdownFailure('worker');};
        worker.onmessage = event => {
          if (worker !== markdownWorker) return;
          let data = event.data;
          if (!markdownReady) {
            if (data?.type !== 'ready') return;
            clearTimeout(markdownTimer); markdownReady = true; pumpMarkdown(); return;
          }
          if (!markdownRun) return;
          if (performance.now() - markdownRun.began > limits.parseMs) {markdownFailure('timeout'); return;}
          if (data instanceof ArrayBuffer) {
            if (data.byteLength > limits.messageBytes) {markdownFailure('message'); return;}
            try {data = JSON.parse(new TextDecoder('utf-8', {fatal: true}).decode(data));}
            catch {markdownFailure('structure'); return;}
          }
          if (data?.id !== markdownRun.id) {markdownFailure('structure'); return;}
          markdownResult(data.type === 'result' ? null : Object.hasOwn(reasons, data.reason) ? data.reason : 'worker', data.tree);
        };
      } catch {
        const batch = [...markdownQueue]; markdownQueue.clear(); stopMarkdownWorker();
        for (const view of batch) if (!view.disposed) view.deliver('worker');
      }
      return;
    }
    if (!markdownReady) return;
    const view = markdownQueue.values().next().value; markdownQueue.delete(view);
    if (view.disposed) {pumpMarkdown(); return;}
    markdownRun = {view, version: view.version, id: ++markdownSerial, began: performance.now()};
    markdownTimer = setTimeout(() => markdownFailure('timeout'), limits.parseMs);
    try {markdownWorker.postMessage({type: 'parse', id: markdownRun.id, text: view.text});}
    catch {markdownFailure('worker');}
  }
  function createMarkdownView({element, text = '', onChange} = {}) {
    if (!(element instanceof Element)) throw new TypeError('A Markdown host element is required');
    if (markdownViews.size >= 100) throw new Error('Chat Markdown view budget exceeded');
    const view = {text: '', bytes: 0, version: 0, disposed: false, listeners: []};
    markdownViews.add(view); element.classList.add('development-markdown');
    function clearListeners() {for (const remove of view.listeners) remove(); view.listeners = [];}
    const report = reason => {if (typeof onChange === 'function') onChange({reason: reason || null});};
    const listen = (node, name, callback) => {node.addEventListener(name, callback); view.listeners.push(() => node.removeEventListener(name, callback));};
    view.deliver = (reason, tree) => {
      if (view.disposed) return;
      clearListeners();
      if (!reason) {
        try {
          const dom = buildDOM(tree, undefined, listen, value => report(value));
          dom.className = 'development-markdown-body'; element.replaceChildren(dom);
        } catch (error) {reason = reasonOf(error);}
      }
      if (reason) {
        clearListeners();
        const source = make('div', view.text, 'development-markdown-plain');
        source.style.whiteSpace = 'pre-wrap'; source.style.overflowWrap = 'anywhere';
        element.replaceChildren(source); element.dataset.markdownFallback = reason;
      } else delete element.dataset.markdownFallback;
      report(reason);
    };
    function update(next) {
      if (view.disposed || next === view.text && view.version) return;
      let fallback;
      try {admit(next);} catch (error) {
        fallback = reasonOf(error);
        if (fallback !== 'lines') {report(fallback); return;}
      }
      const size = byteLength(next);
      if (markdownBytes - view.bytes + size > 1024 * 1024) {report('bytes'); return;}
      markdownBytes += size - view.bytes; view.bytes = size; view.text = next; view.version++;
      if (fallback) {markdownQueue.delete(view); view.deliver(fallback); return;}
      // The first real text is readable while its safe Markdown preview builds.
      if (!element.childNodes.length) {
        const initial = make('div', next, 'development-markdown-plain');
        initial.style.whiteSpace = 'pre-wrap'; initial.style.overflowWrap = 'anywhere'; element.append(initial);
      }
      markdownQueue.add(view); pumpMarkdown();
    }
    function dispose() {
      if (view.disposed) return;
      view.disposed = true; markdownViews.delete(view); markdownQueue.delete(view);
      markdownBytes -= view.bytes; view.bytes = 0; view.text = ''; clearListeners(); element.replaceChildren();
      if (!markdownViews.size) stopMarkdownWorker();
    }
    update(text);
    return {element, update, dispose};
  }

  function createTextReader(options = {}) {
    if (typeof options.text !== 'string') throw new TypeError('ForgeReading text must be a string');
    if (options.defaultMode !== undefined && !['preview', 'source'].includes(options.defaultMode)) throw new TypeError('Invalid reading mode');
    let source = options.text, resolver = options.resolveLink;
    let owner = {identity: options.identity}, disposed = false, generation = 0;
    let completeInitial;const ready=new Promise(resolve=>{completeInitial=resolve;});
    const offline = options.offline === true;
    let mode, cancel = null, pageIndex = 0, renderListeners = [], uiListeners = [];
    const element = make('section', undefined, 'forge-reader');
    element.setAttribute('aria-label', '文本阅读');
    const toolbar = make('div', undefined, 'reading-toolbar'); toolbar.setAttribute('role', 'group');
    toolbar.setAttribute('aria-label', '文本显示方式');
    const documentView = typeof options.documentTitle === 'string';
    const preview = make('button', '预览'), raw = documentView ? preview : make('button', '原文');
    preview.type = raw.type = 'button';
    if (documentView) {
      element.classList.add('document-reader');
      toolbar.append(make('h2', options.documentTitle), preview);
    } else toolbar.append(preview, raw);
    const status = make('p', '', 'reading-status'); status.setAttribute('role', 'status');
    const content = make('div', undefined, 'reading-content');
    element.append(toolbar, status, content);
    function iconAction(button, label, path) {
      const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
      svg.setAttribute('viewBox', '0 0 24 24'); svg.setAttribute('aria-hidden', 'true');
      const mark = document.createElementNS(svg.namespaceURI, 'path');
      mark.setAttribute('d', path); svg.append(mark);
      button.replaceChildren(svg); button.title = label; button.setAttribute('aria-label', label);
    }
    function listen(node, event, callback, bucket = renderListeners) {
      node.addEventListener(event, callback);
      bucket.push(() => node.removeEventListener(event, callback));
    }
    function clearRender() {
      if (cancel) { const stop = cancel; cancel = null; stop(); }
      for (const remove of renderListeners) remove();
      renderListeners = []; content.replaceChildren();
      if (documentView) toolbar.querySelector('.reading-source-nav')?.remove();
    }
    function report(value) { if (!disposed) status.textContent = value; }
    // Deterministic UTF-16 boundaries allow O(1) previous/next page selection,
    // with no full-text split or unbounded index. 4096 units are at most 12288
    // UTF-8 bytes (+ one unit at an adjusted boundary), below 16 KiB per chunk.
    function boundary(value) {
      const n = Math.min(value, source.length);
      return n > 0 && n < source.length && source.charCodeAt(n - 1) >= 0xd800 &&
        source.charCodeAt(n - 1) <= 0xdbff && source.charCodeAt(n) >= 0xdc00 &&
        source.charCodeAt(n) <= 0xdfff ? n - 1 : n;
    }
    function showSource(prefix = '') {
      const chunks = Math.max(1, Math.ceil(source.length / 4096));
      const pages = Math.ceil(chunks / limits.sourceChunks);
      pageIndex = Math.max(0, Math.min(pageIndex, pages - 1));
      const from = boundary(pageIndex * limits.sourceChunks * 4096);
      const to = boundary(Math.min(chunks, (pageIndex + 1) * limits.sourceChunks) * 4096);
      const nav = make('div', undefined, 'reading-source-nav');
      const before = make('button', '上一页原文'), after = make('button', '下一页原文');
      const compact=options.compactSource===true,copy = make('button', compact?'复制代码':'复制本页原文');
      for (const button of [before, after, copy]) button.type = 'button';
      before.disabled = pageIndex === 0; after.disabled = pageIndex === pages - 1;
      const coordinates=`第 ${pageIndex + 1} / ${pages} 页 · UTF-16 字符位置 [${from}, ${to}) / ${source.length}`;
      const position = make('span', compact||documentView?`第 ${pageIndex + 1} / ${pages} 页`:coordinates);position.title=coordinates;
      if((compact||documentView)&&pages===1)before.hidden=after.hidden=position.hidden=true;
      nav.append(before, position, after, copy);
      if (documentView) {
        iconAction(before,'上一页原文','m14 6-6 6 6 6'); iconAction(after,'下一页原文','m10 6 6 6-6 6');
        iconAction(copy,pages===1?'复制原文':'复制本页原文','M9 8h11v13H9z M15 5V3H5v13h1');
        toolbar.insertBefore(nav, preview);
      } else content.append(nav);
      // Preserve the same per-page/per-chunk budgets and exact text boundaries,
      // but allow source-file readers to display the chunks as one code block.
      const continuous=compact&&options.continuousSource===true;
      const sourcePage=continuous?make('pre',undefined,'reading-source-page'):null;
      if(sourcePage)content.append(sourcePage);
      let mounted = 0;
      for (let i = pageIndex * limits.sourceChunks; i < Math.min(chunks, (pageIndex + 1) * limits.sourceChunks); i++) {
        const start = boundary(i * 4096), end = boundary((i + 1) * 4096);
        const part = source.slice(start, end), bytes = byteLength(part);
        if (bytes > limits.sourceChunkBytes || (mounted += bytes) > limits.sourceBytes) fail('bytes');
        const block = make(continuous?'span':'pre', undefined, 'reading-source-chunk');
        block.dataset.from = String(start); block.dataset.to = String(end);
        block.append(document.createTextNode(part)); (sourcePage||content).append(block);
      }
      if(documentView)report(prefix ? prefix+'，已显示原文。' : '');
      else if(compact){
        report((prefix?prefix+'；':'')+(pages===1?'源码完整显示。':'源码按页显示，共 '+pages+' 页。')+(offline?' 离线本地原文。':''));
      }else report((prefix ? prefix + '；已显示完整原文的当前页。' : '原文按页完整保留，可逐页查看与复制。') +
        ` 本页 ${mounted} UTF-8 字节。` + (offline ? ' 离线预览没有在线 worker 的 500 ms 超时保证。' : ''));
      function turn(delta) { pageIndex += delta; clearRender(); showSource(prefix); }
      listen(before, 'click', () => turn(-1)); listen(after, 'click', () => turn(1));
      listen(copy, 'click', async () => {
        if (!navigator.clipboard || typeof navigator.clipboard.writeText !== 'function') {
          report('剪贴板不可用，请直接选择本页原文复制。'); return;
        }
        const current = generation;
        try {
          await navigator.clipboard.writeText(source.slice(from, to));
          if (!disposed && generation === current) report(documentView?'已复制。':compact?'已复制本页代码。':`已准确复制原文字符 [${from}, ${to})。`);
        } catch { if (!disposed && generation === current) report('复制未完成，请直接选择本页原文复制。'); }
      });
    }
    function fallback(reason) {
      clearRender(); mode = 'source'; selectMode(); showSource(reasons[reason] || reasons.structure);completeInitial();
    }
    function selectMode() {
      if (documentView) {
        if (mode === 'preview') iconAction(preview,'查看源码','m8 6-6 6 6 6 M16 6l6 6-6 6 M14 3l-4 18');
        else iconAction(preview,'查看预览','M2 12s4-7 10-7 10 7 10 7-4 7-10 7S2 12 2 12z M15 12a3 3 0 1 1-6 0 3 3 0 0 1 6 0z');
      } else {
        preview.setAttribute('aria-pressed', String(mode === 'preview'));
        raw.setAttribute('aria-pressed', String(mode === 'source'));
      }
      element.dataset.mode = mode;
    }
    function choose(next) {
      if (disposed || mode === next) return;
      generation++; clearRender(); mode = next; selectMode();
      if (mode === 'source') { showSource();completeInitial(); return; }
      try { admit(source, documentView); } catch (error) { fallback(reasonOf(error)); return; }
      const current = generation, expectedOwner = owner;
      const deliver = (error, tree) => {
        if (disposed || generation !== current || owner !== expectedOwner || mode !== 'preview') return;
        cancel = null;
        if (error) { fallback(error); return; }
        try {
          const boundReport = value => {
            if (!disposed && generation === current && owner === expectedOwner && mode === 'preview') report(value);
          };
          const dom = buildDOM(tree, resolver, listen, boundReport);
          content.replaceChildren(dom);
          report(documentView ? '' : offline ? '离线本地预览；没有在线 worker 的 500 ms 超时保证，可切换原文核对。' : 'Markdown 预览；可随时切换原文核对。');completeInitial();
        } catch (failure) { fallback(reasonOf(failure)); }
      };
      report(offline ? '正在本地预览；离线解析没有在线超时保证。' : '正在预览…');
      if (offline) {
        try { deliver(null, parseTree(source, documentView).tree); } catch (error) { deliver(reasonOf(error)); }
      } else cancel = schedule(source, owner, deliver, documentView);
    }
    listen(preview, 'click', () => choose(documentView && mode === 'preview' ? 'source' : 'preview'), uiListeners);
    if (!documentView) listen(raw, 'click', () => choose('source'), uiListeners);
    choose(options.defaultMode || 'preview');
    function dispose() {
      if (disposed) return;
      disposed = true; generation++; clearRender();completeInitial();
      for (const remove of uiListeners) remove();
      uiListeners = []; source = null; resolver = null; owner = null;
      element.replaceChildren();
    }
    return {element, dispose,ready};
  }
  globalThis.ForgeReading = Object.freeze({createTextReader, createMarkdownView, limits,initialReady:true});
})();
