/* Isolated design spike. markdown-it parses Markdown; this file only builds
 * allowlisted DOM from its tokens. No HTML insertion, resource loads or API. */
(() => {
  'use strict';
  const limit = Object.freeze({bytes: 65536, tokens: 6000, nodes: 8000, depth: 24, lines: 4096});
  const parser = window.markdownit('default', {html: false, linkify: false, typographer: false, maxNesting: 16});
  const allowed = new Set(['p','h1','h2','h3','h4','h5','h6','ul','ol','li','blockquote','em','strong','s','table','thead','tbody','tr','th','td']);
  function el(tag, value, className) {
    const node = document.createElement(tag);
    if (value !== undefined) node.textContent = value;
    if (className) node.className = className;
    return node;
  }
  function linkTarget(href) {
    // Relative documents need a verified project-object resolver in production.
    // This mock intentionally has no resolver or actual project connection.
    try {
      if (!/^https?:\/\//i.test(href) || /[\u0000-\u0020\u007f\\]/.test(href)) return null;
      const url = new URL(href);
      if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password) return null;
      for (const key of url.searchParams.keys()) if (['token','x-forge-token','authorization'].includes(key.toLowerCase())) return null;
      const hash = new URLSearchParams(url.hash.slice(1));
      if ([...hash.keys()].some(k => ['token','x-forge-token','authorization'].includes(k.toLowerCase()))) return null;
      return url.href;
    } catch { return null; }
  }
  function modeFor(value, {kind='', literal=false}={}) {
    if (kind==='image'||kind==='evidence') return 'resource';
    if (kind==='json'||value!==null&&typeof value==='object') return 'structured';
    if (literal) return 'raw';
    if (typeof value!=='string') return 'compact';
    return kind==='text'||value.length>=240||value.includes('\n')||value.includes('\r')?'preview':'compact';
  }
  function build(source) {
    if (typeof source !== 'string' || source.length > limit.bytes || new TextEncoder().encode(source).length > limit.bytes || source.split('\n').length > limit.lines) throw new Error('内容超过预览预算；请使用原文视图。');
    const tokens = parser.parse(source, {});
    let count = 0;
    function inspect(items, depth) {
      if (depth > limit.depth) throw new Error('Markdown 嵌套超过预览预算；请使用原文视图。');
      for (const t of items) {
        if (++count > limit.tokens) throw new Error('Markdown 结构超过预览预算；请使用原文视图。');
        if (t.children) inspect(t.children, depth + 1);
      }
    }
    inspect(tokens, 0);
    const root = el('div', undefined, 'reader');
    let nodes = 1;
    const stack = [root];
    const linkNotes=new WeakMap();
    function append(n) {
      let amount=1;
      const scan=document.createTreeWalker(n,NodeFilter.SHOW_ALL);
      while(scan.nextNode())amount++;
      if (nodes+amount > limit.nodes) throw new Error('预览节点超过预算；请使用原文视图。');
      nodes+=amount;
      stack.at(-1).append(n);
    }
    function walk(items) {
      for (const t of items) {
        if (t.hidden) continue;
        if (t.type === 'inline') { walk(t.children || []); continue; }
        if (t.type === 'image') {
          append(el('span', '图片引用 · ' + (t.content || '未命名图片') + '（未加载）', 'image-placeholder'));
          continue;
        }
        if (t.type === 'text' || t.type === 'html_inline' || t.type === 'html_block') { append(document.createTextNode(t.content)); continue; }
        if (t.type === 'softbreak' || t.type === 'hardbreak') { append(el('br')); continue; }
        if (t.type === 'code_inline') { append(el('code', t.content)); continue; }
        if (t.type === 'fence' || t.type === 'code_block') {
          const pre = el('pre');
          if (t.info.trim()) pre.append(el('span', t.info.trim().slice(0,64), 'code-language'));
          pre.append(el('code', t.content)); append(pre);
          continue;
        }
        if (t.type === 'hr') { append(el('hr')); continue; }
        if (t.type === 'link_open') {
          const href = t.attrGet('href') || '', safe = linkTarget(href);
          const a = el(safe ? 'a' : 'span', undefined, safe ? 'external' : 'disabled-link');
          if (safe) { a.href = safe; a.target = '_blank'; a.rel = 'noopener noreferrer'; a.referrerPolicy = 'no-referrer'; }
          else {
            a.title = '样稿未连接项目；正式页面仅打开受支持的项目对象。';
            const relative=!href.includes(':')&&!href.includes('?')&&!href.startsWith('//')&&!/[\u0000-\u001f]/.test(href);
            linkNotes.set(a,relative?'〔引用未连接：'+href.slice(0,200)+'〕':'〔不支持此链接〕');
          }
          append(a); stack.push(a); continue;
        }
        if (t.type === 'link_close') {
          const link=stack.at(-1),message=linkNotes.get(link);
          if(message)append(el('small',message,'link-reason'));
          stack.pop();continue;
        }
        if (t.nesting === 1 && allowed.has(t.tag)) {
          if (stack.length >= limit.depth) throw new Error('Markdown 深度超过预览预算；请使用原文视图。');
          if (t.tag === 'table') { const wrap = el('div', undefined, 'md-table-wrap'); append(wrap); stack.push(wrap); }
          const n = el(t.tag);
          if (t.tag === 'ol') { const start = Number(t.attrGet('start')); if (Number.isSafeInteger(start) && start > 0) n.start = Math.min(start, 1000000); }
          if (t.tag === 'th') n.scope = 'col';
          append(n); stack.push(n); continue;
        }
        if (t.nesting === -1 && allowed.has(t.tag)) { stack.pop(); if (t.tag === 'table') stack.pop(); continue; }
        if (t.content) append(document.createTextNode(t.content));
      }
    }
    walk(tokens);
    if (stack.length !== 1) throw new Error('预览结构无法完整显示；请使用原文视图。');
    return root;
  }
  function render(parent, source) {
    try { const tree = build(source); parent.replaceChildren(tree); return {ok:true,nodes:tree.querySelectorAll('*').length}; }
    catch (err) { parent.replaceChildren(el('p', err.message, 'reader-limit')); return {ok:false,error:err.message}; }
  }
  // This is a sample-local helper, never imported by the current Forge platform.
  window.ForgeSampleReading = Object.freeze({render, linkTarget, modeFor, limit});
})();
