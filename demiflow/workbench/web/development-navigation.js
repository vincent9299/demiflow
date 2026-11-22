// The DSH components render the rail and tree. DemiFlow owns their data and lifetime.
(() => {
  const {createRoot, react:{createElement:h}} = DshRuntime;
  const {TurnNavigator, Level, files} = DshWorkbench;
  const node = (tag, cls, text) => {
    const value = document.createElement(tag);
    if (cls) value.className = cls;
    if (text) value.textContent = text;
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

  function filePanel({layout, toggle, api, iconButton}) {
    const pane = node('aside','development-files');
    pane.setAttribute('aria-label','项目文件'); pane.hidden = true;
    const title = node('div','development-files-title','文件');
    const close = iconButton('关闭项目目录','M6 6l12 12M18 6L6 18');
    const refresh = iconButton('刷新项目目录','M20 10a8 8 0 1 0-1 7M20 4v6h-6');
    title.append(close);
    const header = node('div',files.header), pathLabel = node('span',files.path,'项目根目录');
    header.append(pathLabel, refresh);
    const body = node('div',files.body), treeHost = node('ul',files.level);
    body.append(treeHost); pane.append(title, header, body); layout.append(pane);
    toggle.setAttribute('aria-expanded','false');
    const root = createRoot(treeHost);
    let levels = Object.create(null), expanded = [], disposed = false;
    const text = (key, args) => ({empty:'空目录',loading:'正在读取…',truncated:'加载更多文件',
      'entry.other':'文件','error.unavailable':args?.message || '读取失败'})[key] || '读取失败';
    function render() {
      root.render(h(Level, {path:'/',tree:{state:{levels,expanded}, t:text, onToggle, onMore:path=>list(path,true),
        onOpen:openFile}}));
    }
    async function list(path, more=false) {
      const cache = levels, previous = cache[path];
      if (more && (!previous?.level || previous.loading)) return;
      if (more) previous.loading = true;
      else cache[path] = {kind:'loading'};
      const pending = cache[path]; render();
      try {
        const page = await api('project-files', {...(path === '/' ? {} : {path:path.slice(1)}),offset:more ? previous.next : 0,limit:50});
        if (disposed || levels !== cache || cache[path] !== pending) return;
        const entries = more ? previous.level.entries.slice() : [];
        const count = Object.values(cache).reduce((n,l)=>n+(l.level?.entries.length||0),0);
        if (count + page.entries.length > 1024) throw new Error('已展开的文件较多，请收起部分目录后重试。');
        for (const row of page.entries) entries.push({name:row.name,type:row.kind === 'directory' ? 'directory' : 'file',preview:row.kind === 'markdown' || row.kind === 'text'});
        cache[path] = {kind:'ready',level:{entries,truncated:page.next_offset !== null}, next:page.next_offset};
        if (path === '/') pathLabel.textContent = page.root;
      } catch (e) {
        if (disposed || levels !== cache || cache[path] !== pending) return;
        const failure = {message:e.message};
        cache[path] = more ? {...previous,loading:false,failure} : {kind:'failed',failure};
      }
      render();
    }
    function onToggle(parent,path) {
      if (!expanded.includes(path)) {expanded.push(path);list(path);return;}
      const below = p => p===path || p.startsWith(path+'/');
      expanded = expanded.filter(p=>!below(p));
      for (const p of Object.keys(levels)) if (below(p)) delete levels[p];
      render();
    }
    let preview;
    async function openFile(path) {
      preview?.remove();
      const dialog = preview = node('dialog','development-file-preview');
      const closePreview = iconButton('关闭文件预览','M6 6l12 12M18 6L6 18');
      const content = node('pre',null,'正在读取…');
      dialog.append(node('strong',null,path),closePreview,content);pane.append(dialog);
      dialog.onclose = ()=>dialog.remove(); closePreview.onclick = ()=>dialog.close(); dialog.showModal();
      try {content.textContent = (await api('project-file-text',{path:path.slice(1)})).text;}
      catch(e) {content.textContent=e.message;}
    }
    function reset() {levels=Object.create(null);expanded=[];body.scrollTop=0;list('/');}
    function setOpen(value) {
      pane.hidden=!value;layout.classList.toggle('development-files-open',value);
      toggle.setAttribute('aria-expanded', String(value));
      if (value) {reset();close.focus();} else {levels=Object.create(null);toggle.focus();}
    }
    toggle.onclick=()=>setOpen(pane.hidden);close.onclick=()=>setOpen(false);refresh.onclick=reset;
    pane.onkeydown=e=>{if(e.key==='Escape'&&!preview?.open){e.preventDefault();setOpen(false);}};
    return {dispose(){disposed=true;preview?.remove();root.unmount();}};
  }
  globalThis.DevelopmentNavigation = {turnRail,filePanel};
})();
