/* One visible graph identity, with independent node-detail freshness. */
(() => {
  'use strict';
  const clone = value => JSON.parse(JSON.stringify(value));
  const cache = new Map();
  const bytes = value => new TextEncoder().encode(JSON.stringify(value)).byteLength;
  const SHA256 = /^[0-9a-f]{64}$/;
  function remember(key, value) {
    if (bytes(value)>32768) throw new Error('配置草稿超过 32 KiB，请减少本次修改。');
    if (!cache.has(key) && cache.size>=64) throw new Error('配置草稿已达 64 份，请先处理已有草稿。');
    const proposed=new Map(cache);proposed.set(key,clone(value));
    if (bytes([...proposed])>512*1024) throw new Error('草稿总量超过 512 KiB，请先处理已有草稿。');
    cache.set(key,clone(value));
  }
  function validIdentity(execution,project) {
    if (execution===null) return null;
    if (typeof execution!=='object'||Array.isArray(execution)) throw new Error('执行身份不合法');
    const {project:p,basisDigest,runId,attempt,actionId,dagId,planId,planRevision,format}=execution;
    if ((project!==null&&p!==project)||!SHA256.test(basisDigest)||typeof runId!=='string'||!runId||
        !Number.isSafeInteger(attempt)||attempt<1||typeof actionId!=='string'||!actionId||actionId.length>128||
        !SHA256.test(dagId)||!SHA256.test(planId)||!Number.isSafeInteger(planRevision)||planRevision<0||
        (!['legacy','v3','v4'].includes(format))) throw new Error('执行身份不合法');
    return Object.freeze({project:p,basisDigest,runId,attempt,actionId,dagId,planId,planRevision,format});
  }
  const identityKey = execution => execution===null?null:JSON.stringify([execution.project,execution.basisDigest,
    execution.runId,execution.attempt,execution.actionId,execution.dagId,execution.planId,execution.planRevision,execution.format]);
  function validSelection(selection) {
    if (selection===null) return null;
    if (typeof selection!=='object'||Array.isArray(selection)) throw new Error('节点选择不合法');
    const {key,reportNodeId,actionId,logicalNodeId,occurrenceId,execution}=selection;
    if (typeof key!=='string'||!key||
        (reportNodeId!==null&&typeof reportNodeId!=='string')||
        (actionId!==null&&(typeof actionId!=='string'||!actionId))||
        (logicalNodeId!==null&&(typeof logicalNodeId!=='string'||!logicalNodeId))||
        (occurrenceId!==null&&(typeof occurrenceId!=='string'||!occurrenceId))) throw new Error('节点选择不合法');
    return Object.freeze({key,reportNodeId,actionId,logicalNodeId,occurrenceId,execution:validIdentity(execution,null)});
  }
  function create(project) {
    if (!/^[wl][0-9a-f]{32}$/.test(project)) throw new Error('项目身份不合法');
    let epoch=0, detail=0, view=0, disposed=false;
    let value={project,entry:null,params:null,report:null,run:null,attempt:null,mode:'config',node:null,logical_selection:null,graph:null,
      graphPreference:'auto',actionId:null,selection:null,execution:null};
    const capture=(node=false)=>Object.freeze({project,epoch,detail:node?detail:null,view:node?view:null});
    const valid=t=>!disposed&&t.project===project&&t.epoch===epoch&&(t.detail===null||(t.detail===detail&&t.view===view));
    function transition(patch) {
      if (disposed) throw new Error('工作台已关闭');
      epoch++;detail++;view++;
      value={...value,...patch,graph:null,node:null,logical_selection:null,selection:null,actionId:null,execution:null};
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
      setGraphSelection:selection=>{
        const checked=validSelection(selection);
        if (checked!==null&&checked.execution!==null&&checked.execution.project!==project) throw new Error('节点选择属于另一个项目');
        if (checked!==null&&checked.reportNodeId!==null&&!value.graph?.nodes.some(n=>n.id===checked.reportNodeId)) throw new Error('节点不属于当前图');
        detail++;value={...value,selection:checked};return capture(true);
      },
      setGraphView:({preference,actionId,execution=null})=>{
        if (disposed) throw new Error('工作台已关闭');
        if (preference!=='auto'&&preference!=='report') throw new Error('图视角不合法');
        if (actionId!==null&&(typeof actionId!=='string'||!actionId||actionId.length>128)) throw new Error('action 身份不合法');
        const checked=validIdentity(execution,project);
        const key=identityKey(checked);
        if (key!==identityKey(value.execution)) {
          view++;
          value={...value,selection:null,node:null,logical_selection:null};
        }
        value={...value,graphPreference:preference,actionId,execution:checked};
        return capture();
      },
      setMode:mode=>{value={...value,mode};},
      dispose:()=>{disposed=true;epoch++;detail++;view++;},
    });
  }
  globalThis.ForgeWorkbenchState=Object.freeze({create,
    drafts:Object.freeze({get:key=>cache.has(key)?clone(cache.get(key)):null,put:remember,delete:key=>cache.delete(key)})});
})();
