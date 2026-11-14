/* One visible graph identity, with independent node-detail freshness. */
(() => {
  'use strict';
  const clone = value => JSON.parse(JSON.stringify(value));
  const cache = new Map();
  const bytes = value => new TextEncoder().encode(JSON.stringify(value)).byteLength;
  function remember(key, value) {
    if (bytes(value)>32768) throw new Error('配置草稿超过 32 KiB，请减少本次修改。');
    if (!cache.has(key) && cache.size>=64) throw new Error('配置草稿已达 64 份，请先处理已有草稿。');
    const proposed=new Map(cache);proposed.set(key,clone(value));
    if (bytes([...proposed])>512*1024) throw new Error('草稿总量超过 512 KiB，请先处理已有草稿。');
    cache.set(key,clone(value));
  }
  function create(project) {
    if (!/^[wl][0-9a-f]{32}$/.test(project)) throw new Error('项目身份不合法');
    let epoch=0, detail=0, disposed=false;
    let value={project,entry:null,params:null,report:null,run:null,attempt:null,mode:'config',node:null,logical_selection:null,graph:null};
    const capture=(node=false)=>Object.freeze({project,epoch,detail:node?detail:null});
    const valid=t=>!disposed&&t.project===project&&t.epoch===epoch&&(t.detail===null||t.detail===detail);
    function transition(patch) {
      if (disposed) throw new Error('工作台已关闭');
      epoch++;detail++;value={...value,...patch,graph:null,node:null,logical_selection:null};
      return capture();
    }
    function acceptGraph(ticket,graph) {
      if (!valid(ticket)) return false;
      const binding=graph?.binding;
      if (binding?.workspace_id!==project) throw new Error('图属于另一个项目');
      if (value.run) {
        if (binding.basis_kind!=='run'||binding.run_id!==value.run||binding.attempt!==value.attempt) throw new Error('运行图身份不一致');
      } else if (value.report) {
        if (binding.basis_kind!=='report'||binding.basis_digest!==value.report) throw new Error('报告图身份不一致');
      } else if (binding.basis_kind!=='source'||binding.entry!==value.entry||binding.params!==value.params) throw new Error('源码图身份不一致');
      value={...value,graph};return true;
    }
    return Object.freeze({
      capture,valid,transition,acceptGraph,
      current:()=>value,
      chooseNode:(id,selection=null)=>{
        if (id!==null&&!value.graph?.nodes.some(n=>n.id===id)) throw new Error('节点不属于当前图');
        if (selection!==null&&(typeof selection!=='object'||Array.isArray(selection)||typeof selection.logical_node_id!=='string'||!selection.logical_node_id||(selection.occurrence_id!==null&&typeof selection.occurrence_id!=='string'))) throw new Error('逻辑选择身份不合法');
        const logical=selection===null?null:{logical_node_id:selection.logical_node_id,occurrence_id:selection.occurrence_id};
        detail++;value={...value,node:id,logical_selection:logical};return capture(true);
      },
      setMode:mode=>{value={...value,mode};},
      dispose:()=>{disposed=true;epoch++;detail++;},
    });
  }
  globalThis.ForgeWorkbenchState=Object.freeze({create,
    drafts:Object.freeze({get:key=>cache.has(key)?clone(cache.get(key)):null,put:remember,delete:key=>cache.delete(key)})});
})();
