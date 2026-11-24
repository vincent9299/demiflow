/* The native notebook stays in memory with its one kernel. Persist individual
   cells and keep the other edits as drafts belonging to this browser tab. */
const openPanels = new WeakMap();
const encodedPath = path => path.split('/').map(encodeURIComponent).join('/');

export async function cellStorage(panel, project, base) {
  if (!openPanels.has(panel)) {
    const pending = open(panel, project, base).catch(error => {openPanels.delete(panel);throw error;});
    openPanels.set(panel, pending);
  }
  return openPanels.get(panel);
}

async function open(panel, project, base) {
  const model = panel.model, shared = model.sharedModel;
  const cells = () => Array.from(model.cells);
  const key = () => 'demiflow.cell-drafts.' + JSON.stringify([project, panel.context.path]);
  const changes = new EventTarget(), pending = new Map(), errors = new Map();
  const url = () => base + 'api/demiflow/cards/' + encodedPath(panel.context.path);
  async function request(options) {
    const result = await fetch(url(), options);
    const data = await result.json();
    if (!result.ok) throw Error(data.message || '卡片保存失败。');
    return data;
  }
  const baseline = new Map(cells().map(c => [c.id, JSON.stringify(c.toJSON())]));
  const loaded = panel.context.contentsModel?.last_modified;
  const initial = await request();
  if (loaded && Date.parse(loaded)!==Date.parse(initial.modified))
    throw Error('分析在打开时已被修改，请刷新后再编辑。');
  let revisions = initial.cells;
  if (panel.isDisposed) throw Error('分析已关闭。');
  let timer, cacheKey = key(), previous = new Set(cells().map(c=>c.id));
  const notify = () => changes.dispatchEvent(new Event('change'));
  function isDraft(cell) {
    return !Object.hasOwn(revisions, cell.id) || baseline.get(cell.id) !== JSON.stringify(cell.toJSON());
  }
  function persistDrafts() {
    clearTimeout(timer);
    if (panel.isDisposed) return;
    const drafts = [];
    for (const [index, cell] of cells().entries()) {
      if (isDraft(cell)) drafts.push({index, revision:revisions[cell.id] ?? null, cell:cell.toJSON()});
    }
    try {
      if (drafts.length) sessionStorage.setItem(cacheKey, JSON.stringify(drafts));
      else sessionStorage.removeItem(cacheKey);
    } catch {
      for (const {cell} of drafts) errors.set(cell.id, '浏览器草稿空间不足，刷新前请保存这张卡片。');
      notify();
    }
  }
  let drafts = [];
  try { drafts = JSON.parse(sessionStorage.getItem(cacheKey) || '[]'); } catch {}
  for (const draft of drafts) {
    const id = draft.cell.id, index = cells().findIndex(cell => cell.id === id);
    if ((revisions[id] ?? null) !== draft.revision) {
      // Preserve the unsaved version as a separate draft when another editor
      // changed the saved cell. Saving it cannot overwrite that other edit.
      shared.insertCell(Math.min(draft.index + 1, shared.cells.length), {...draft.cell,id:crypto.randomUUID()});
    } else {
      if (index >= 0) shared.deleteCell(index);
      shared.insertCell(index >= 0 ? index : Math.min(draft.index, shared.cells.length), draft.cell);
    }
  }
  function commit(id, cell, before) {
    const next=(pending.get(id) || Promise.resolve()).catch(()=>{}).then(()=>write(id,cell,before));
    pending.set(id,next);notify();
    return next.finally(()=>{
      if(pending.get(id)===next)pending.delete(id);
      persistDrafts();notify();
    });
  }
  async function write(id, cell, before) {
    errors.delete(id);
    try {
      const result = await request({method:'POST',headers:{'Content-Type':'application/json'},
        body:JSON.stringify({id,expected:revisions[id] ?? null,cell,before})});
      if (cell) { revisions[id]=result.revision;baseline.set(id, JSON.stringify(cell)); }
      else { delete revisions[id];baseline.delete(id); }
      if (!panel.isDisposed && !cells().some(isDraft)) model.dirty=false;
    } catch(error) {
      errors.set(id, error.message);throw error;
    }
  }
  function changed() {
    const now = new Set(cells().map(c => c.id));
    for (const id of previous) {
      if (!now.has(id) && (Object.hasOwn(revisions,id)||pending.has(id))) {
        commit(id,null,null).catch(error => {
          // A failed delete must leave a visible, retryable original cell.
          if (!panel.isDisposed && !cells().some(c=>c.id===id) && baseline.has(id)) {
            shared.insertCell(shared.cells.length, JSON.parse(baseline.get(id)));
            errors.set(id,error.message);notify();
          }
        });
      }
    }
    previous=now;
    clearTimeout(timer);timer=setTimeout(persistDrafts,250);notify();
  }
  let documentSnapshot;
  async function savedDocument(_, state) {
    if (state === 'started') {
      documentSnapshot = new Map(cells().map(c=>[c.id, JSON.stringify(c.toJSON())]));
      return;
    }
    if (state !== 'completed' || !documentSnapshot) return;
    // A reload can follow the native save immediately, before the revision
    // read returns. Drop only drafts covered by that completed write now.
    baseline.clear();
    for (const [id,raw] of documentSnapshot) {
      baseline.set(id,raw);
      if (!Object.hasOwn(revisions,id)) revisions[id]=null;
    }
    documentSnapshot=null;
    persistDrafts();
    try {
      revisions=(await request()).cells;
      persistDrafts();notify();
    } catch { /* Individual writes still check the last observed revision. */ }
  }
  function renamed() {
    const old=cacheKey;cacheKey=key();persistDrafts();
    if(old!==cacheKey)sessionStorage.removeItem(old);
  }
  previous=new Set(cells().map(c=>c.id));
  model.contentChanged.connect(changed);
  panel.context.saveState.connect(savedDocument);
  panel.context.pathChanged.connect(renamed);
  window.addEventListener('pagehide',persistDrafts);
  panel.disposed.connect(()=>{
    clearTimeout(timer);window.removeEventListener('pagehide',persistDrafts);
    openPanels.delete(panel);
  });
  return {changes,isDraft,hasDrafts:()=>cells().some(isDraft),persistDrafts,
    forget(){clearTimeout(timer);sessionStorage.removeItem(cacheKey);},
    status(cell){return errors.get(cell.id) || (pending.has(cell.id)?'正在保存…':isDraft(cell)?'草稿':'已保存');},
    async save(cell){
      const peers=cells(), index=peers.findIndex(c=>c.id===cell.id);
      const before=peers.slice(index+1).find(c=>Object.hasOwn(revisions,c.id));
      if (index>=0) await commit(cell.id,cell.toJSON(),before?.id ?? null);
    }
  };
}
