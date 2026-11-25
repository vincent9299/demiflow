/* Pure workbench fact projection (C01-C04, C09): no fetch, no DOM, no Store.
 * Inputs are the validated graph/model.py::get response and runs/view snapshot;
 * outputs are transient derived models, never a second persisted authority. */
(() => {
  'use strict';

  const MAX_NODES = 256, MAX_EDGES = 512, MAX_FACTS = 16;
  const LEGACY_SCHEMA = 'demiflow.logical_dag.v1';
  const V3_SCHEMA = 'demiflow.dataflow_graph.v3';
  const V4_SCHEMA = 'demiflow.dataflow_graph.v4';
  const hasOccurrenceIds = format => format === 'v3' || format === 'v4';
  const SHA = /^[0-9a-f]{64}$/;
  const ID = /^[A-Za-z0-9_-]{1,128}$/;
  const PROJECT = /^[wl][0-9a-f]{32}$/;
  const ROLES = new Set(['cpu', 'gpu', 'remote_api', 'io']);
  const EXCHANGE_MODES = new Set(['memory_incremental', 'durable_incremental', 'finite_sealed']);
  const FINITE_COMPLETE_KINDS = new Set([
    'SortOp', 'RepartitionOp', 'RandomShuffleOp', 'RandomizeBlockOrderOp',
    'OperatorLLMMapOp', 'AgentMapOp', 'EmbeddingMapOp', 'reduce', 'fit',
  ]);
  const isPreviewBarrier = (toNode, toPort) => Boolean(
    FINITE_COMPLETE_KINDS.has(toNode.op_kind)
    || (toNode.op_kind === 'join' && toPort === 'right')
    || (toNode.op_kind === 'StreamPrependOp' && toPort === 'prefix')
    || toNode.traits?.barrier === true
    || toNode.traits?.coordination === 'barrier'
  );
  const RECOVERY_MODES = new Set(['checkpoint', 'final_commit', 'delivery_only', 'attempt_only']);
  const ROLE_LABEL = {cpu: 'CPU', gpu: 'GPU', remote_api: '远程 API', io: 'I/O'};
  const KIND_LABEL = {
    source_call: '调用入口', source: '数据源', read_table: '读取', write_table: '写出',
    terminal: '写出', map: '自定义算子', join: '关联', filter: '过滤', query: '查询',
    reduce: '聚合', union: '合并',
    MapOp:'逐行处理',BoundMapOp:'逐行处理',AsyncMapOp:'异步处理',
    FlatMapOp:'展开',FilterOp:'筛选',prompt:'模型调用',OperatorLLMMapOp:'模型调用',
    MapBatchesOp:'批量处理',BatchMapOp:'批量处理',StreamGroupBatchesOp:'分组批处理',
    EmbeddingMapOp:'向量编码',DeduplicateOp:'去重',AgentMapOp:'智能体处理',
    LimitOp:'截取',SelectColumnsOp:'选择字段',DropColumnsOp:'删除字段',
    RenameColumnsOp:'重命名字段',AddColumnOp:'新增字段',RandomSampleOp:'抽样',
    SortOp:'排序',RepartitionOp:'重新分区',RandomShuffleOp:'打乱顺序',
    RandomizeBlockOrderOp:'打乱批次',VectorSearchOp:'向量检索',SearchWebOp:'网页检索',
    RegisterDocumentsOp:'登记文档',RegisterDocumentsBatchOp:'批量登记文档',
    ReadDocumentsOp:'读取文档',StreamPrependOp:'前置数据',
    lance:'固定表读取',write_lance:'写出结果',SaveLanceOp:'固定中间交付',
    fit:'训练',fixed_dataset:'固定训练输入',materialized:'内存输入',take_all:'读取结果',ReadObjectOp:'读取对象',
  };
  const STATES = {
    pending: '已登记 · 未开始', running: '运行中', succeeded: '执行完成', failed: '执行失败',
    incomplete: '观测不完整', reused: '复用物化结果', cancelled: '已取消',
  };
  const REASON_TEXT = {
    dataset_plan_unavailable: '尚未取得引擎执行计划。可选择已封存的运行，或点击“解析执行计划”读取当前入口和已保存配置；静态检查不能代替运行图。',
    not_sealed: '该 action 没有封存执行计划',
    unsupported_plan_schema: '封存计划版本不受支持',
    identity_mismatch: '封存计划身份不一致',
    v3_endpoint_identity_missing: '执行图投影缺少实例端点',
    invalid_topology: '执行图拓扑无效',
    invalid_physical_binding: '物理计划关联无效',
    projection_budget_exceeded: '执行图超出展示预算',
    action_not_found: '指定的 action 不在本图',
  };
  const NO_BRIDGE_TEXT = '该 action 尚无节点身份桥；执行节点与报告观测的对应不猜测。';

  const isRecord = value => value !== null && typeof value === 'object' && !Array.isArray(value);
  const isSha = value => typeof value === 'string' && SHA.test(value);
  const isId = value => typeof value === 'string' && ID.test(value);
  const isInt = value => Number.isSafeInteger(value) && value >= 0;
  const bounded = (value, max = 160) => {
    const text = String(value);
    return text.length > max ? text.slice(0, max - 1) + '…' : text;
  };
  const jsonKey = parts => JSON.stringify(parts);
  const fail = (reason, detail) => ({available: false, reason, detail: bounded(detail, 512)});
  const reasonText = reason => REASON_TEXT[reason] || String(reason || '未知原因');
  const kindLabel = kind => KIND_LABEL[kind] || '自定义算子';
  // Translate platform names only; preserve business-authored labels and UDF names.
  function displayTitle(name, kind) {
    if(name===kind||!name)return kindLabel(kind);
    if(name==='read_lance'&&['lance','read_table'].includes(kind))return '读取数据表';
    return bounded(name,80);
  }

  class ProjectionError extends Error {
    constructor(reason, detail) { super(detail); this.reason = reason; }
  }
  const raise = (reason, detail) => { throw new ProjectionError(reason, detail); };

  const reportKey = (digest, id) => jsonKey([digest, id]);
  const legacyGroupKey = (digest, actionId, logical) => jsonKey([digest, actionId, logical, null]);
  const legacyMemberKey = (digest, actionId, logical, occurrence) => jsonKey([digest, actionId, logical, occurrence]);
  const v3Key = (identity, occurrence) => jsonKey([
    identity.basisDigest, identity.actionId, identity.dagId, identity.planId, identity.planRevision, occurrence]);

  const sameIdentity = (a, b) => !!a && !!b
    && a.project === b.project && a.basisDigest === b.basisDigest && a.runId === b.runId
    && a.attempt === b.attempt && a.actionId === b.actionId && a.dagId === b.dagId
    && a.planId === b.planId && a.planRevision === b.planRevision && a.format === b.format;

  function findAction(graph, actionId) {
    const actions = graph && graph.logical && Array.isArray(graph.logical.actions) ? graph.logical.actions : [];
    return actions.find(entry => isRecord(entry) && entry.action_id === actionId) || null;
  }
  function findReportNode(graph, id) {
    return Array.isArray(graph && graph.nodes) ? graph.nodes.find(node => isRecord(node) && node.id === id) || null : null;
  }
  function indexLabel(graph, id) {
    const index = graph && graph.node_index;
    if (!index || !Array.isArray(index.nodes)) return null;
    const row = index.nodes.find(item => isRecord(item) && item.id === id);
    return row && typeof row.label === 'string' && row.label ? row.label : null;
  }
  function reportName(node) {
    if (!node) return null;
    if (typeof node.display_label === 'string' && node.display_label.trim()) return bounded(node.display_label, 80);
    if (node.operator && typeof node.operator.name === 'string' && node.operator.name) return bounded(node.operator.name, 80);
    if (typeof node.api === 'string' && node.api) return bounded(node.api.split(/[.:]/).at(-1) || node.api, 80);
    return null;
  }
  function snapshotFor(snapshot, binding) {
    if (!isRecord(snapshot) || snapshot.unchanged === true) return null;
    if (snapshot.format !== 'demiflow_forge_runtime_v1' || snapshot.availability !== 'available') return null;
    if (binding && binding.basis_kind === 'run'
        && (snapshot.run_id !== binding.run_id || snapshot.attempt !== binding.attempt)) return null;
    return snapshot;
  }

  /* ---- C09: the only version adapter ------------------------------------ */

  function portString(value) {
    return typeof value === 'string' && value.length >= 1 && value.length <= 64 ? value : null;
  }
  function readPorts(value) {
    if (!Array.isArray(value) || value.length > 32) return null;
    const ports = [], seen = new Set();
    for (const item of value) {
      const port = portString(item);
      if (port === null || seen.has(port)) return null;
      seen.add(port); ports.push(port);
    }
    return ports;
  }
  function valueEdgeFacts(edge, format) {
    if (format !== 'v4' || (edge.kind == null && edge.step == null)) return {};
    if (edge.kind === 'fixed_dataset' && edge.step == null) return {kind: edge.kind};
    if (edge.kind === 'model_manifest' && isInt(edge.step) && edge.step > 0) return {kind: edge.kind, step: edge.step};
    raise('invalid_topology', '训练依赖的类型或模型步数无效');
  }
  function readRecovery(physical, entry, format) {
    const raw = physical.recovery;
    let declared;
    if (hasOccurrenceIds(format)) {
      if (!RECOVERY_MODES.has(raw)) raise('invalid_physical_binding', 'v3 recovery 声明不在已知枚举内');
      declared = raw;
    } else if (typeof raw === 'string' && raw) {
      declared = bounded(raw, 120);
    } else if (isRecord(raw)) {
      declared = bounded(JSON.stringify(raw), 512);
    } else {
      raise('invalid_physical_binding', 'recovery 声明缺失');
    }
    const limitations = Array.isArray(physical.limitations)
      ? physical.limitations.filter(item => typeof item === 'string').slice(0, 16).map(item => bounded(item, 200))
      : [];
    const restriction = isRecord(entry.recovery_restriction) && typeof entry.recovery_restriction.reason === 'string'
      ? bounded(entry.recovery_restriction.reason, 300) : null;
    return {declared, limitations, restriction};
  }
  function readExchanges(physical, edgeIds, format) {
    const list = hasOccurrenceIds(format) ? physical.exchanges : physical.channels;
    if (list === undefined || list === null) return [];
    if (!Array.isArray(list)) raise('invalid_physical_binding', '物理通道列表无效');
    const out = [], seen = new Set();
    for (const item of list) {
      if (!isRecord(item) || typeof item.channel_id !== 'string' || !item.channel_id || item.channel_id.length > 128)
        raise('invalid_physical_binding', 'channel_id 无效');
      if (seen.has(item.channel_id)) raise('invalid_physical_binding', 'channel_id 重复');
      seen.add(item.channel_id);
      if (hasOccurrenceIds(format)) {
        if (!isSha(item.edge_id) || !edgeIds.has(item.edge_id))
          raise('invalid_physical_binding', 'exchange.edge_id 不在图边集合内');
        if (!EXCHANGE_MODES.has(item.mode)) raise('invalid_physical_binding', 'exchange.mode 不在已知枚举内');
        out.push({
          channelId: item.channel_id, edgeId: item.edge_id, mode: item.mode,
          routing: typeof item.routing === 'string' ? bounded(item.routing, 64) : null,
          reason: typeof item.reason === 'string' ? bounded(item.reason, 200) : null,
          creditRows: isInt(item.credit_rows) ? item.credit_rows : null,
          creditBytes: isInt(item.credit_bytes) ? item.credit_bytes : null,
          storageBytes: isInt(item.storage_bytes) ? item.storage_bytes : null,
          basis: 'sealed',
        });
      } else {
        if (typeof item.exchange_mode !== 'string' || !item.exchange_mode)
          raise('invalid_physical_binding', 'legacy exchange_mode 缺失');
        out.push({
          channelId: item.channel_id, edgeId: null, mode: bounded(item.exchange_mode, 64),
          routing: typeof item.routing === 'string' ? bounded(item.routing, 64) : null,
          reason: null, creditRows: null, creditBytes: null, storageBytes: null, basis: 'sealed',
        });
      }
    }
    return out;
  }

  function normalizeExecutionPlan(entry, binding) {
    try {
      if (!isRecord(entry) || !isRecord(binding)) return fail('identity_mismatch', 'action 或 graph binding 缺失');
      if (binding.basis_kind !== 'run' || typeof binding.workspace_id !== 'string' || !PROJECT.test(binding.workspace_id)
          || !isSha(binding.basis_digest) || typeof binding.run_id !== 'string' || !binding.run_id
          || !isInt(binding.attempt))
        return fail('identity_mismatch', 'run graph binding 不完整');
      const actionId = entry.action_id;
      if (!isId(actionId)) return fail('identity_mismatch', 'action_id 无效');
      if (entry.status !== 'sealed' || entry.binding_level !== 'sealed') {
        const detail = isRecord(entry.fallback) && typeof entry.fallback.detail === 'string' ? entry.fallback.detail
          : isRecord(entry.error) && typeof entry.error.reason === 'string' ? entry.error.reason
          : typeof entry.detail === 'string' ? entry.detail : '该 action 未封存执行计划';
        return fail('not_sealed', detail);
      }
      const dag = entry.dag;
      if (!isRecord(dag)) return fail('unsupported_plan_schema', '缺少 dag');
      const schema = dag.schema_version, version = entry.version;
      let format;
      if (schema === V3_SCHEMA || schema === V4_SCHEMA) {
        format = schema === V3_SCHEMA ? 'v3' : 'v4';
        if (version !== format) return fail('identity_mismatch', 'dag 与 version 标记不一致');
      } else if (schema === LEGACY_SCHEMA) {
        if (version === undefined || version === null || version === 'v1' || version === 'v2') format = 'legacy';
        else if (hasOccurrenceIds(version)) return fail('identity_mismatch', 'version 与 dag schema 冲突');
        else return fail('unsupported_plan_schema', '未知计划版本 ' + bounded(version, 40));
      } else {
        return fail('unsupported_plan_schema', '未知 dag schema ' + bounded(schema, 60));
      }
      if (dag.action_id !== actionId) return fail('identity_mismatch', 'dag.action_id 与 entry 不一致');
      if (!isSha(dag.dag_id)) return fail('identity_mismatch', 'dag_id 无效');
      if (!isSha(entry.plan_id)) return fail('identity_mismatch', 'plan_id 无效');
      const physical = entry.physical;
      if (!isRecord(physical)) return fail('invalid_physical_binding', '缺少 physical 计划');
      if (physical.plan_id !== entry.plan_id) return fail('identity_mismatch', 'physical.plan_id 与封存 plan_id 不一致');
      if (!isInt(physical.plan_revision)) return fail('identity_mismatch', 'plan_revision 无效');
      if (!Array.isArray(dag.nodes) || !Array.isArray(dag.edges)) return fail('invalid_topology', 'dag 节点或边缺失');
      if (dag.nodes.length > MAX_NODES || dag.edges.length > MAX_EDGES)
        return fail('projection_budget_exceeded', '执行图超过 ' + MAX_NODES + ' 节点 / ' + MAX_EDGES + ' 边预算');

      const nodes = [], byOccurrence = new Map(), logicalIds = new Set(), roleByOccurrence = new Map();
      for (const raw of dag.nodes) {
        if (!isRecord(raw)) return fail('invalid_topology', '节点格式无效');
        const logical = raw.logical_node_id, occurrence = raw.occurrence_id;
        const validLogical = hasOccurrenceIds(format) ? isSha(logical) : isId(logical);
        const validOccurrence = hasOccurrenceIds(format) ? isSha(occurrence) : isId(occurrence);
        if (!validLogical || !validOccurrence) return fail('invalid_topology', '节点身份无效');
        if (byOccurrence.has(occurrence)) return fail('invalid_topology', 'occurrence_id 重复');
        if (typeof raw.op_kind !== 'string' || !raw.op_kind || raw.op_kind.length > 64)
          return fail('invalid_topology', 'op_kind 无效');
        const inputPorts = readPorts(raw.input_ports), outputPorts = readPorts(raw.output_ports);
        if (!inputPorts || !outputPorts) return fail('invalid_topology', '端口列表无效');
        const traits = isRecord(raw.traits) ? raw.traits : {};
        const node = {logicalNodeId: logical, occurrenceId: occurrence, kind: raw.op_kind, inputPorts, outputPorts, placement: null};
        nodes.push(node); byOccurrence.set(occurrence, node); logicalIds.add(logical);
        roleByOccurrence.set(occurrence, ROLES.has(traits.resource_role) ? traits.resource_role : null);
      }

      const edges = [], edgeIds = new Set();
      for (const raw of dag.edges) {
        if (!isRecord(raw) || !isRecord(raw.from) || !isRecord(raw.to)) return fail('invalid_topology', '边格式无效');
        const fromPort = portString(raw.from.port), toPort = portString(raw.to.port);
        if (fromPort === null || toPort === null) return fail('invalid_topology', '边端口无效');
        if (hasOccurrenceIds(format)) {
          if (!isSha(raw.edge_id)) return fail('invalid_topology', 'edge_id 无效');
          if (edgeIds.has(raw.edge_id)) return fail('invalid_topology', 'edge_id 重复');
          for (const endpoint of [raw.from, raw.to]) {
            if (endpoint.occurrence_id === undefined || endpoint.occurrence_id === null)
              return fail('v3_endpoint_identity_missing', '执行图投影缺少实例端点');
          }
          const fromNode = byOccurrence.get(raw.from.occurrence_id), toNode = byOccurrence.get(raw.to.occurrence_id);
          if (!fromNode || fromNode.logicalNodeId !== raw.from.logical_node_id
              || !toNode || toNode.logicalNodeId !== raw.to.logical_node_id)
            return fail('invalid_topology', '边端点与节点身份不符');
          if (!fromNode.outputPorts.includes(fromPort) || !toNode.inputPorts.includes(toPort))
            return fail('invalid_topology', '边端口不在节点端口集合内');
          edgeIds.add(raw.edge_id);
          edges.push({
            edgeId: raw.edge_id,
            ...valueEdgeFacts(raw, format),
            from: {logicalNodeId: fromNode.logicalNodeId, occurrenceId: fromNode.occurrenceId, port: fromPort},
            to: {logicalNodeId: toNode.logicalNodeId, occurrenceId: toNode.occurrenceId, port: toPort},
          });
        } else {
          const fromId = raw.from.logical_node_id, toId = raw.to.logical_node_id;
          if (!logicalIds.has(fromId) || !logicalIds.has(toId)) return fail('invalid_topology', '边端点不在节点集合内');
          edges.push({
            edgeId: isSha(raw.edge_id) ? raw.edge_id : null,
            from: {logicalNodeId: fromId, occurrenceId: null, port: fromPort},
            to: {logicalNodeId: toId, occurrenceId: null, port: toPort},
          });
        }
      }

      const sources = Array.isArray(dag.sources) ? dag.sources : [];
      const terminals = Array.isArray(dag.terminals) ? dag.terminals : [];
      if (hasOccurrenceIds(format)) {
        for (const item of [...sources, ...terminals])
          if (!byOccurrence.has(item)) return fail('invalid_topology', 'source/terminal occurrence 不在节点集合内');
      } else {
        for (const item of sources) {
          // Native v1 sources identify the source descriptor, not its graph
          // node. compile_logical_dag intentionally gives those different IDs.
          // Read the sealed identity as-is; never rehash it to invent a node.
          if (typeof item === 'string' ? !logicalIds.has(item) : !isRecord(item) || !isId(item.source_id))
            return fail('invalid_topology', 'source 身份无效');
        }
        for (const item of terminals) {
          const id = typeof item === 'string' ? item : isRecord(item) ? item.logical_node_id : null;
          if (!logicalIds.has(id)) return fail('invalid_topology', 'terminal 不在节点集合内');
        }
      }

      const stages = isRecord(physical.stages) ? physical.stages : {};
      const stageByKey = new Map();
      for (const stageKey of Object.keys(stages)) {
        const known = hasOccurrenceIds(format) ? byOccurrence.has(stageKey) : logicalIds.has(stageKey);
        if (!known) raise('invalid_physical_binding', '物理 stage 索引指向未知节点');
        const stage = stages[stageKey];
        if (!isRecord(stage) || typeof stage.stage_id !== 'string' || !stage.stage_id || stage.stage_id.length > 128)
          raise('invalid_physical_binding', 'stage_id 无效');
        stageByKey.set(stageKey, stage);
      }
      for (const node of nodes) {
        const stageKey = hasOccurrenceIds(format) ? node.occurrenceId : node.logicalNodeId;
        const stage = stageByKey.get(stageKey);
        if (!stage) continue;
        node.placement = {
          stageId: stage.stage_id, role: roleByOccurrence.get(node.occurrenceId),
          profile: hasOccurrenceIds(format) ? (isRecord(stage.profile) ? stage.profile : null)
            : (isRecord(stage.budgets) ? stage.budgets : null),
          partitionCount: isInt(stage.partition_count) ? stage.partition_count : null,
          replicaMin: hasOccurrenceIds(format) && isInt(stage.replica_min) ? stage.replica_min : null,
          replicaTarget: hasOccurrenceIds(format) && isInt(stage.replica_target) ? stage.replica_target : null,
          replicaMax: hasOccurrenceIds(format) && isInt(stage.replica_max) ? stage.replica_max : null,
          fusedOccurrences: [], basis: 'sealed',
        };
      }
      if (hasOccurrenceIds(format)) {
        const byStage = new Map();
        for (const node of nodes) {
          if (!node.placement) continue;
          if (!byStage.has(node.placement.stageId)) byStage.set(node.placement.stageId, []);
          byStage.get(node.placement.stageId).push(node.occurrenceId);
        }
        for (const node of nodes) {
          if (!node.placement) continue;
          node.placement.fusedOccurrences = byStage.get(node.placement.stageId).filter(id => id !== node.occurrenceId);
        }
      }

      const exchanges = readExchanges(physical, edgeIds, format);
      const recovery = readRecovery(physical, entry, format);
      const identity = {
        project: binding.workspace_id, basisDigest: binding.basis_digest, runId: binding.run_id,
        attempt: binding.attempt, actionId, dagId: dag.dag_id, planId: entry.plan_id,
        planRevision: physical.plan_revision, format,
      };
      return {available: true, identity, nodes, edges, exchanges, recovery};
    } catch (error) {
      if (error instanceof ProjectionError) return fail(error.reason, error.message);
      return fail('invalid_topology', '执行计划投影失败：' + bounded(error && error.message ? error.message : error, 400));
    }
  }

  /* ---- sealed bridge / observation helpers (shared by C02/C04) ----------- */

  function bridgeEntriesFor(entry, identity, reportDigest) {
    const bridge = entry && entry.identity_bridge;
    if (!isRecord(bridge) || bridge.available !== true || !isRecord(bridge.bridge)) return null;
    const doc = bridge.bridge;
    if (doc.schema !== 'demiflow.identity_bridge.v1' || doc.action_id !== identity.actionId
        || doc.dag_id !== identity.dagId || !isSha(reportDigest) || doc.report_digest !== reportDigest
        || !Array.isArray(doc.entries)) return null;
    const seen = new Set();
    for (const item of doc.entries) {
      if (!isRecord(item) || typeof item.report_node_id !== 'string' || !item.report_node_id
          || item.report_node_id.length > 64 || !isId(item.logical_node_id) || !isId(item.occurrence_id)
          || !Number.isSafeInteger(item.report_occurrence) || item.report_occurrence < 0
          || (item.mode !== 'execute' && item.mode !== 'reuse')) return null;
      const logicalKey = jsonKey([item.logical_node_id, item.occurrence_id]);
      if (seen.has(logicalKey)) return null;
      seen.add(logicalKey);
    }
    return doc.entries;
  }
  function bridgeEntryFor(entries, logical, occurrence) {
    if (!entries) return null;
    const matches = entries.filter(item => item.logical_node_id === logical && item.occurrence_id === occurrence);
    return matches.length === 1 ? matches[0] : null;
  }
  function unambiguousReportId(entries, logical) {
    if (!entries) return null;
    const ids = new Set(entries.filter(item => item.logical_node_id === logical).map(item => item.report_node_id));
    return ids.size === 1 ? [...ids][0] : null;
  }
  function observationPageFor(entry, identity) {
    const page = entry && entry.node_observations;
    if (!isRecord(page) || page.available !== true || !Array.isArray(page.entries))
      return {ok: false, reason: isRecord(page) && typeof page.reason === 'string' ? page.reason : 'observation_page_unavailable'};
    if (page.attempt !== identity.attempt) return {ok: false, reason: 'observation_scope_mismatch'};
    if (hasOccurrenceIds(identity.format)
        && (page.action_id !== identity.actionId || page.dag_id !== identity.dagId
            || page.plan_id !== identity.planId || page.plan_revision !== identity.planRevision))
      return {ok: false, reason: 'observation_scope_mismatch'};
    const byKey = new Map();
    for (const item of page.entries) {
      if (!isRecord(item) || !isId(item.logical_node_id) || !isId(item.occurrence_id)) continue;
      byKey.set(jsonKey([item.logical_node_id, item.occurrence_id]), item);
    }
    return {ok: true, byKey, hasMore: page.has_more === true};
  }
  function terminalsFor(entry, format) {
    const dag = entry && isRecord(entry.dag) ? entry.dag : {};
    const list = Array.isArray(dag.terminals) ? dag.terminals : [];
    const set = new Set();
    for (const item of list) {
      if (typeof item === 'string') set.add(item);
      else if (!hasOccurrenceIds(format) && isRecord(item) && typeof item.logical_node_id === 'string') set.add(item.logical_node_id);
    }
    return set;
  }
  function nodeMapFor(entry) {
    return entry && isRecord(entry.node_map) ? entry.node_map : {};
  }
  function sealedContext(graph, entry, identity, snapshot) {
    const binding = graph.binding;
    return {
      entry, identity, binding,
      bridgeEntries: bridgeEntriesFor(entry, identity, graph.report_digest),
      page: observationPageFor(entry, identity),
      terminals: terminalsFor(entry, identity.format),
      nodeMap: nodeMapFor(entry),
      snapshot: snapshotFor(snapshot, binding),
    };
  }
  function matchesExecutionObservation(row, identity, occurrenceId) {
    // The snapshot/page wrapper binds run and attempt; an individual v3 row
    // must independently carry its original plan and execution occurrence.
    // A current wrapper cannot turn an old or unscoped row into current data.
    return isRecord(row) && !!identity && hasOccurrenceIds(identity.format)
      && row.action_id === identity.actionId && row.dag_id === identity.dagId
      && row.plan_id === identity.planId && row.plan_revision === identity.planRevision
      && row.occurrence_id === occurrenceId
      && (row.run_id === undefined || row.run_id === identity.runId)
      && (row.attempt === undefined || row.attempt === identity.attempt);
  }
  function memberState(node, context) {
    const {identity, binding, bridgeEntries, page, terminals, snapshot} = context;
    const neutral = label => ({label, tone: 'neutral'});
    const isTerminal = hasOccurrenceIds(identity.format) ? terminals.has(node.occurrenceId) : terminals.has(node.logicalNodeId);
    if (snapshot && isTerminal) {
      const output = (Array.isArray(snapshot.outputs) ? snapshot.outputs : [])
        .find(item => isRecord(item) && item.action_id === identity.actionId
          && (!hasOccurrenceIds(identity.format) || (matchesExecutionObservation(item, identity, node.occurrenceId)
            && (item.retained_from_attempt == null || item.retained_from_attempt === identity.attempt))));
      if (output && output.status === 'committed')
        return {label: output.result ? '已提交并核验' : '已提交 · 登记待完成', tone: 'success'};
    }
    const bridgeEntry = bridgeEntryFor(bridgeEntries, node.logicalNodeId, node.occurrenceId);
    if (!bridgeEntry) return neutral('未桥接 · 无观测关联');
    if (!page.ok) {
      if (page.reason === 'historical_observation_contract') return neutral('历史未记录节点观测');
      if (page.reason === 'observation_scope_mismatch')
        return neutral(hasOccurrenceIds(identity.format) ? '观测凭据不完整' : '观测页作用域不符');
      return neutral('观测页不可用');
    }
    const pageEntry = page.byKey.get(jsonKey([node.logicalNodeId, node.occurrenceId]));
    if (!pageEntry || pageEntry.availability !== 'observed') return neutral('本页未见观测');
    if (hasOccurrenceIds(identity.format) && (pageEntry.report_node_id !== bridgeEntry.report_node_id
        || pageEntry.report_occurrence !== bridgeEntry.report_occurrence)) return neutral('观测凭据不完整');
    if (!snapshot) return neutral('已有观测 · 摘要未载入');
    const rows = (Array.isArray(snapshot.observations) ? snapshot.observations : []).filter(item =>
      isRecord(item) && item.action_id === identity.actionId
      && item.node_id === bridgeEntry.report_node_id && item.occurrence === bridgeEntry.report_occurrence);
    const row = hasOccurrenceIds(identity.format)
      ? rows.find(item => matchesExecutionObservation(item, identity, node.occurrenceId)) : rows[0];
    if (!row && rows.length) return neutral('观测凭据不完整');
    if (!row) return neutral('已有观测 · 摘要未载入');
    const stateText = STATES[row.state] || bounded(row.state, 40);
    if (row.timing_scope === 'group') return neutral('阶段观测 · ' + stateText);
    const tone = row.state === 'failed' ? 'failed' : row.state === 'running' ? 'running'
      : (row.state === 'succeeded' || row.state === 'reused') ? 'success' : 'neutral';
    return {label: stateText, tone};
  }

  /* ---- report / source facts -------------------------------------------- */

  function reportStatus(node, snapshot) {
    if (!snapshot) return {label: node.kind === 'source_call' ? '尚未检查' : '未运行', tone: 'neutral'};
    const records = (Array.isArray(snapshot.observations) ? snapshot.observations : [])
      .filter(item => isRecord(item) && item.node_id === node.id);
    const output = (Array.isArray(snapshot.outputs) ? snapshot.outputs : [])
      .find(item => isRecord(item) && item.action_id === node.id);
    if (output && output.status === 'committed')
      return {label: output.result ? '已提交并核验' : '已提交 · 登记待完成', tone: 'success'};
    if (snapshot.observation_availability === 'unsupported') return {label: '历史未记录节点观测', tone: 'neutral'};
    if (snapshot.observation_scope === 'paged_run_observations' && !Array.isArray(snapshot.observations))
      return {label: '观测尚未载入', tone: 'neutral'};
    if (!records.length)
      return {label: snapshot.observations_truncated ? '摘要未载入 · 点击核对' : '未单独观测', tone: 'neutral'};
    const unique = [...new Set(records.map(item => item.state))];
    const label = records.length === 1 ? (STATES[unique[0]] || bounded(unique[0], 40))
      : records.length + ' 次观测 · ' + (unique.length === 1 ? (STATES[unique[0]] || bounded(unique[0], 40)) : '多种状态');
    const tone = unique.includes('failed') ? 'failed' : unique.includes('running') ? 'running'
      : unique.every(state => state === 'succeeded' || state === 'reused') ? 'success' : 'neutral';
    return {label, tone};
  }
  function fieldFacts(fields, basis) {
    if (!isRecord(fields)) return [{label: '字段', value: null, basis, reason: 'fields_unavailable'}];
    const names = Object.keys(fields);
    if (!names.length) return [{label: '字段', value: null, basis, reason: 'no_declared_fields'}];
    const facts = names.slice(0, MAX_FACTS).map(name => ({
      label: bounded(name, 80),
      value: typeof fields[name] === 'string' ? bounded(fields[name], 80) : bounded(JSON.stringify(fields[name]), 80),
      basis, reason: null,
    }));
    if (names.length > MAX_FACTS) facts.push({label: '字段总数', value: names.length + ' 个', basis, reason: null});
    return facts;
  }
  function parameterFacts(node, basis) {
    if (!isRecord(node.parameters)) return [];
    const origins = isRecord(node.parameter_origins) ? node.parameter_origins : {};
    return Object.keys(node.parameters).slice(0, MAX_FACTS).map(name => {
      let value = bounded(JSON.stringify(node.parameters[name]), 120);
      const origin = origins['/' + name];
      if (isRecord(origin) && typeof origin.argument === 'string') value = bounded(value + ' · 源参数 ' + origin.argument, 160);
      return {label: bounded(name, 80), value, basis, reason: null};
    });
  }
  function inputsFor(graph, node, basis) {
    const facts = [];
    const edges = Array.isArray(graph.edges) ? graph.edges : [];
    for (const edge of edges) {
      if (!isRecord(edge) || edge.type !== 'data' || edge.to !== node.id) continue;
      const upstream = findReportNode(graph, edge.from);
      const port = edge.port !== undefined && edge.port !== null ? String(edge.port) : null;
      const label = bounded((reportName(upstream) || String(edge.from)) + (port !== null ? ' · 端口 ' + port : ''), 120);
      if (!upstream) { facts.push({label, value: null, basis, reason: 'upstream_not_loaded'}); continue; }
      if (!isRecord(upstream.fields) || !Object.keys(upstream.fields).length) {
        facts.push({label, value: null, basis, reason: 'upstream_fields_unknown'}); continue;
      }
      const names = Object.keys(upstream.fields);
      facts.push({label, value: bounded(names.join(', '), 160), basis, reason: null});
      if (facts.length >= MAX_FACTS) break;
    }
    return facts;
  }

  /* ---- C02: one node card ------------------------------------------------ */

  function memberSelection(graph, node, identity, context) {
    const binding = graph.binding;
    const bridgeEntry = bridgeEntryFor(context.bridgeEntries, node.logicalNodeId, node.occurrenceId);
    const reportNodeId = bridgeEntry ? bridgeEntry.report_node_id
      : (typeof context.nodeMap[node.logicalNodeId] === 'string' ? context.nodeMap[node.logicalNodeId] : null);
    const key = hasOccurrenceIds(identity.format) ? v3Key(identity, node.occurrenceId)
      : legacyMemberKey(binding.basis_digest, identity.actionId, node.logicalNodeId, node.occurrenceId);
    return {
      key, reportNodeId, actionId: identity.actionId,
      logicalNodeId: node.logicalNodeId, occurrenceId: node.occurrenceId, execution: identity,
    };
  }
  function sealedTitle(graph, node, identity, context, ordinal, sameLogicalCount) {
    const bridgeEntry = bridgeEntryFor(context.bridgeEntries, node.logicalNodeId, node.occurrenceId);
    const reportId = bridgeEntry ? bridgeEntry.report_node_id
      : (typeof context.nodeMap[node.logicalNodeId] === 'string' ? context.nodeMap[node.logicalNodeId] : null);
    let name = reportName(findReportNode(graph, reportId)) || (reportId ? indexLabel(graph, reportId) : null);
    name = displayTitle(name, node.kind);
    if (hasOccurrenceIds(identity.format) && sameLogicalCount > 1) name = bounded(name + ' · 第' + ordinal + '次', 80);
    return name;
  }
  function placementSummary(node) {
    if (!node.placement) return {label: '阶段', value: null, basis: 'sealed', reason: 'stage_missing'};
    const parts = [node.placement.stageId];
    if (node.placement.role) parts.push(ROLE_LABEL[node.placement.role] || node.placement.role);
    if (node.placement.partitionCount !== null) parts.push(node.placement.partitionCount + ' 分区');
    return {label: '阶段', value: bounded(parts.join(' · ') + '（计划声明）', 160), basis: 'sealed', reason: null};
  }
  const HAS_CJK = /[\u4e00-\u9fff]/;
  function splitTopLevelArgs(argText) {
    const out = [];
    let cur = '', depth = 0, quote = '';
    for (let i = 0; i < argText.length; i++) {
      const ch = argText[i];
      if (quote) {
        cur += ch;
        if (ch === '\\' && i + 1 < argText.length) { cur += argText[++i]; continue; }
        if (ch === quote) quote = '';
        continue;
      }
      if (ch === '\'' || ch === '"') { quote = ch; cur += ch; continue; }
      if (ch === '(' || ch === '[' || ch === '{') { depth++; cur += ch; continue; }
      if (ch === ')' || ch === ']' || ch === '}') { depth = Math.max(0, depth - 1); cur += ch; continue; }
      if (ch === ',' && depth === 0) { if (cur.trim()) out.push(cur.trim()); cur = ''; continue; }
      cur += ch;
    }
    if (cur.trim()) out.push(cur.trim());
    return out;
  }
  function stripQuotes(s) {
    return typeof s === 'string' ? s.trim().replace(/^['"]|['"]$/g, '') : '';
  }
  const SOURCE_CREATORS = new Set(['read_lance', 'from_items', 'read_parquet', 'read_json', 'read_csv', 'read_records', 'range', 'from_arrow', 'from_pandas']);
  function compactApiExpr(call, fallbackLabel, dataSource) {
    const raw = typeof call?.summary === 'string' ? call.summary.trim() : '';
    if (!raw) return typeof fallbackLabel === 'string' && fallbackLabel.trim() && !HAS_CJK.test(fallbackLabel) ? fallbackLabel.trim() : '';
    const m = raw.match(/^(?:[A-Za-z_][\w.]*\.)?([A-Za-z_]\w*)\s*\(([\s\S]*)\)\s*$/);
    if (!m) {
      const clean = raw.replace(/^(?:rows|ds|dataset)\./, '.');
      return bounded(clean, 64);
    }
    const fn = m[1], args = splitTopLevelArgs(m[2]);
    const prefix = SOURCE_CREATORS.has(fn) ? 'data.' : '.';
    const kw = {};
    const pos = [];
    for (const a of args) {
      const eq = a.match(/^([A-Za-z_]\w*)\s*=\s*([\s\S]+)$/);
      if (eq) kw[eq[1]] = eq[2].trim();
      else pos.push(a);
    }
    if (['map', 'flat_map', 'filter', 'map_batches', 'map_async'].includes(fn)) {
      const target = pos[0] || kw.fn || fallbackLabel || '';
      return target ? `${prefix}${fn}(${target})` : `${prefix}${fn}()`;
    }
    if (fn === 'map_prompt_async' || fn === 'map_prompt') {
      const name = stripQuotes(pos[0] || kw.name || kw.prompt || '');
      return name ? `${prefix}${fn}(${name})` : `${prefix}${fn}()`;
    }
    if (fn === 'join') {
      const parts = [];
      if (kw.on) parts.push(`on=${kw.on}`);
      if (kw.how) parts.push(`how=${kw.how}`);
      return `${prefix}join(${parts.join(', ') || pos[0] || ''})`;
    }
    if (fn === 'save_lance') {
      if (kw.stage) return `${prefix}save_lance(stage=${kw.stage})`;
      if (pos[0]) return `${prefix}save_lance(${pos[0]})`;
      return `${prefix}save_lance()`;
    }
    if (fn === 'write_lance') {
      const target = pos[0] || kw.uri || '';
      const keyMatch = target.match(/\[['"]([^'"]+)['"]\]$/);
      return `${prefix}write_lance(${keyMatch ? keyMatch[1] : target})`;
    }
    if (fn === 'read_lance') {
      const uri = typeof dataSource?.uri === 'string' ? dataSource.uri : '';
      const base = uri ? uri.split('/').filter(Boolean).pop() : '';
      return `${prefix}read_lance(${base || pos[0] || kw.uri || ''})`;
    }
    if (fn === 'from_items') {
      return `${prefix}from_items(${pos[0] || ''})`;
    }
    const first = pos[0] || Object.entries(kw).slice(0, 1).map(([k, v]) => `${k}=${v}`).join('');
    return bounded(first ? `${prefix}${fn}(${first})` : `${prefix}${fn}()`, 64);
  }
  function semanticHeadline(rawTitle, desc) {
    const t = typeof rawTitle === 'string' ? rawTitle.trim() : '';
    const d = typeof desc === 'string' ? desc.trim().replace(/[。.]\s*$/, '') : '';
    if (t && HAS_CJK.test(t)) return t;
    if (d) return bounded(d, 48);
    return t;
  }
  function buildNodeCard(graph, selection, snapshot, execution = null) {
    const binding = graph.binding;
    if (!selection || selection.execution === null || !execution || !execution.available) {
      const node = findReportNode(graph, selection && selection.reportNodeId);
      const basis = binding.basis_kind === 'source' ? 'source' : 'report';
      const key = reportKey(binding.basis_digest, selection ? selection.reportNodeId : null);
      if (!node) {
        return {
          key, selection: selection || {key, reportNodeId: null, actionId: null, logicalNodeId: null, occurrenceId: null, execution: null},
          occurrences: [], kindLabel: '节点', title: '节点不在当前图',
          summary: [{label: '定位', value: null, basis, reason: 'node_not_loaded'}],
          state: {label: '未载入', tone: 'neutral'},
        };
      }
      const fields = isRecord(node.fields) ? Object.keys(node.fields) : [];
      return {
        key,
        selection: {key, reportNodeId: node.id, actionId: null, logicalNodeId: null, occurrenceId: null, execution: null},
        occurrences: [], kindLabel: kindLabel(node.kind), title: reportName(node) || kindLabel(node.kind),
        summary: [
          {label: '位置', value: bounded(String(node.source) + ':' + String(node.line), 160), basis, reason: null},
          fields.length ? {label: '字段', value: fields.length + ' 个', basis, reason: null}
            : {label: '字段', value: null, basis, reason: 'fields_unavailable'},
        ],
        state: reportStatus(node, snapshotFor(snapshot, binding)),
      };
    }
    const identity = execution.identity;
    const context = sealedContext(graph, findAction(graph, identity.actionId), identity, snapshot);
    if (hasOccurrenceIds(identity.format)) {
      const node = execution.nodes.find(item => item.occurrenceId === selection.occurrenceId
        && item.logicalNodeId === selection.logicalNodeId);
      if (!node) {
        return {
          key: selection.key, selection, occurrences: [], kindLabel: '执行实例', title: '实例不在当前计划',
          summary: [{label: '定位', value: null, basis: 'sealed', reason: 'occurrence_not_in_plan'}],
          state: {label: '定位失效', tone: 'neutral'},
        };
      }
      const sameLogical = execution.nodes.filter(item => item.logicalNodeId === node.logicalNodeId);
      const ordinal = sameLogical.findIndex(item => item.occurrenceId === node.occurrenceId) + 1;
      const cardSelection = memberSelection(graph, node, identity, context);
      const reportNode = findReportNode(graph, cardSelection.reportNodeId);
      const desc = typeof reportNode?.description === 'string' && reportNode.description.trim() ? reportNode.description.trim() : null;
      const call = reportNode?.api_call;
      const rawTitle = sealedTitle(graph, node, identity, context, ordinal, sameLogical.length);
      return {
        key: v3Key(identity, node.occurrenceId), selection: cardSelection, occurrences: [],
        kindLabel: kindLabel(node.kind), title: semanticHeadline(rawTitle, desc),
        apiExpr: compactApiExpr(call, rawTitle, reportNode?.data_source),
        summary: [
          ...(desc ? [{label: '说明', value: bounded(desc, 160), basis: 'sealed', reason: null}]
            : typeof call?.summary === 'string' ? [{label: 'API', value: bounded(call.summary, 256), basis: 'sealed', reason: null}] : []),
          {label: '端口', value: bounded((node.inputPorts.join(', ') || '—') + ' → ' + (node.outputPorts.join(', ') || '—'), 160), basis: 'sealed', reason: null},
          placementSummary(node),
        ],
        state: memberState(node, context),
      };
    }
    const members = execution.nodes.filter(item => item.logicalNodeId === selection.logicalNodeId);
    const first = members[0];
    const groupKey = legacyGroupKey(binding.basis_digest, identity.actionId, selection.logicalNodeId);
    const occurrences = members.map(node => memberSelection(graph, node, identity, context));
    const groupSelection = {
      key: groupKey,
      reportNodeId: typeof context.nodeMap[selection.logicalNodeId] === 'string' ? context.nodeMap[selection.logicalNodeId] : null,
      actionId: identity.actionId, logicalNodeId: selection.logicalNodeId, occurrenceId: null, execution: identity,
    };
    const single = members.length === 1;
    const state = single ? memberState(first, context)
      : (() => {
        const labels = new Set(members.map(node => memberState(node, context).label));
        return {label: members.length + ' 次出现' + (labels.size > 1 ? ' · 状态不同' : ''), tone: 'neutral'};
      })();
    const title = sealedTitle(graph, first, identity, context, 1, 1);
    return {
      key: groupKey, selection: single ? occurrences[0] : groupSelection, occurrences,
      kindLabel: kindLabel(first ? first.kind : 'map'), title,
      summary: [
        {label: '出现', value: single ? '1 次' : members.length + ' 次（需选择成员）', basis: 'sealed', reason: null},
        {label: '端口', value: bounded(((first && first.inputPorts.join(', ')) || '—') + ' → ' + ((first && first.outputPorts.join(', ')) || '—'), 160), basis: 'sealed', reason: null},
      ],
      state,
    };
  }

  /* ---- C01: choose the main view ---------------------------------------- */

  function nonSealedReason(entry) {
    if (isRecord(entry.fallback) && typeof entry.fallback.reason === 'string') return entry.fallback.reason;
    if (isRecord(entry.error) && typeof entry.error.code === 'string') return entry.error.code;
    if (typeof entry.reason === 'string') return entry.reason;
    return 'not_sealed';
  }
  function chooseGraphView(graph, {actionId = null, preference = 'auto'} = {}) {
    const binding = graph && graph.binding;
    const basis = binding ? binding.basis_kind : null;
    if (basis === 'source' || basis === 'report') return {kind: 'unavailable', actionId: null, label: '尚无运行执行图', reason: 'dataset_plan_unavailable'};
    if (basis !== 'run') return {kind: 'unavailable', actionId: null, label: '图基础未知', reason: 'invalid_binding'};
    const logical = graph.logical;
    const actions = logical && Array.isArray(logical.actions) ? logical.actions : [];
    if (actionId !== null && actionId !== undefined) {
      const entry = actions.find(item => isRecord(item) && item.action_id === actionId);
      if (!entry) return {kind: 'unavailable', actionId, label: reasonText('action_not_found'), reason: 'action_not_found'};
      if (entry.status === 'sealed') return {kind: 'sealed', actionId, label: '运行封存计划', reason: null};
      const reason = nonSealedReason(entry);
      return {kind: 'unavailable', actionId, label: '该 action 无封存执行计划 · ' + reason, reason};
    }
    if (!actions.length) {
      const reason = logical && typeof logical.reason === 'string' ? logical.reason : 'no_declared_actions';
      return {kind: 'unavailable', actionId: null, label: '没有可投影的 action · ' + reason, reason};
    }
    const first = actions.find(entry => isRecord(entry) && entry.status === 'sealed') || actions[0];
    const firstId = isRecord(first) && typeof first.action_id === 'string' ? first.action_id : null;
    if (isRecord(first) && first.status === 'sealed') return {kind: 'sealed', actionId: firstId, label: '运行封存计划', reason: null};
    const reason = nonSealedReason(first);
    return {kind: 'unavailable', actionId: firstId, label: '该 action 无封存执行计划 · ' + reason, reason};
  }

  /* ---- C03: the whole graph model --------------------------------------- */

  function emptyModel(binding, choice, execution, notices, completeness) {
    return {binding, choice, execution, nodes: [], edges: [], selected: null, completeness, notices};
  }
  function reportCompleteness(graph, nodes, edges) {
    const completeness = graph && isRecord(graph.completeness) ? graph.completeness : {};
    return {
      display: typeof completeness.display === 'string' ? completeness.display : 'complete',
      totalNodes: isInt(completeness.total_nodes) ? completeness.total_nodes : nodes.length,
      totalEdges: isInt(completeness.total_edges) ? completeness.total_edges : edges.length,
    };
  }
  function buildGraphModel(graph, {choice, snapshot = null, selection = null} = {}) {
    const binding = graph.binding;
    const notices = [];
    if (!choice || choice.kind === 'unavailable') {
      const reason = choice ? choice.reason : 'not_sealed';
      notices.push(reasonText(reason));
      return emptyModel(binding, choice || {kind: 'unavailable', actionId: null, label: '不可用', reason}, null, notices,
        reportCompleteness(graph, [], []));
    }
    if (choice.kind === 'source' || choice.kind === 'report') {
      const rawNodes = Array.isArray(graph.nodes) ? graph.nodes.slice(0, MAX_NODES) : [];
      const cards = rawNodes.map(node => buildNodeCard(graph, {
        key: reportKey(binding.basis_digest, node.id), reportNodeId: node.id,
        actionId: null, logicalNodeId: null, occurrenceId: null, execution: null,
      }, snapshot, null));
      const keys = new Set(cards.map(card => card.key));
      const edges = [];
      for (const edge of Array.isArray(graph.edges) ? graph.edges : []) {
        if (!isRecord(edge)) continue;
        const from = reportKey(binding.basis_digest, edge.from), to = reportKey(binding.basis_digest, edge.to);
        if (!keys.has(from) || !keys.has(to)) continue;
        const kind = ['data', 'value', 'action', 'reference'].includes(edge.type) ? edge.type : 'data';
        edges.push({
          edgeId: null, from, to, kind,
          fromPort: edge.port !== undefined && edge.port !== null ? bounded(edge.port, 64) : null, toPort: null,
        });
        if (edges.length >= MAX_EDGES) break;
      }
      let selected = null;
      if (selection && selection.execution === null && keys.has(selection.key)) selected = selection.key;
      const completeness = reportCompleteness(graph, cards, edges);
      if (completeness.display === 'clipped') notices.push('图已按预算裁剪；定位节点可加载其邻域。');
      if (choice.kind === 'source') notices.push('源码观察仅记录调用语法，语义尚未检查。');
      if (graph.freshness && graph.freshness.current === false) notices.push('报告与当前源码存在差异，保存后需重新检查。');
      return {binding, choice, execution: null, nodes: cards, edges, selected, completeness, notices};
    }
    const entry = findAction(graph, choice.actionId);
    const projection = normalizeExecutionPlan(entry, binding);
    if (!projection.available) {
      const unavailable = {kind: 'unavailable', actionId: choice.actionId, label: choice.label, reason: projection.reason};
      notices.push(reasonText(projection.reason) + '：' + projection.detail);
      notices.push('计划不可用时不回退到静态检查图。');
      return emptyModel(binding, unavailable, null, notices, reportCompleteness(graph, [], []));
    }
    const identity = projection.identity;
    const context = sealedContext(graph, entry, identity, snapshot);
    const cards = [], cardByKey = new Map();
    if (hasOccurrenceIds(identity.format)) {
      for (const node of projection.nodes) {
        const cardSelection = memberSelection(graph, node, identity, context);
        const card = buildNodeCard(graph, cardSelection, snapshot, projection);
        cards.push(card); cardByKey.set(card.key, card);
      }
    } else {
      const groups = new Map();
      for (const node of projection.nodes)
        if (!groups.has(node.logicalNodeId)) groups.set(node.logicalNodeId, node);
      for (const logical of groups.keys()) {
        const groupSelection = {
          key: legacyGroupKey(binding.basis_digest, identity.actionId, logical), reportNodeId: null,
          actionId: identity.actionId, logicalNodeId: logical, occurrenceId: null, execution: identity,
        };
        const card = buildNodeCard(graph, groupSelection, snapshot, projection);
        cards.push(card); cardByKey.set(card.key, card);
      }
    }
    const exchangeByEdge = new Map();
    for (const ex of projection.exchanges || []) if (ex.edgeId) exchangeByEdge.set(ex.edgeId, ex.mode);
    const edges = [];
    for (const edge of projection.edges) {
      const from = hasOccurrenceIds(identity.format) ? v3Key(identity, edge.from.occurrenceId)
        : legacyGroupKey(binding.basis_digest, identity.actionId, edge.from.logicalNodeId);
      const to = hasOccurrenceIds(identity.format) ? v3Key(identity, edge.to.occurrenceId)
        : legacyGroupKey(binding.basis_digest, identity.actionId, edge.to.logicalNodeId);
      if (!cardByKey.has(from) || !cardByKey.has(to)) continue;
      edges.push({
        edgeId: edge.edgeId, from, to, kind: edge.kind || 'data', step: edge.step,
        fromPort: edge.from.port, toPort: edge.to.port,
        exchangeMode: exchangeByEdge.get(edge.edgeId) || null,
      });
      if (edges.length >= MAX_EDGES) break;
    }
    let selected = null;
    if (selection && sameIdentity(selection.execution, identity)) {
      for (const card of cards) {
        if (card.key === selection.key || card.occurrences.some(member => member.key === selection.key)) {
          selected = selection.key; break;
        }
      }
      if (selected === null) notices.push('原选择不在当前执行图或已过期，已清除定位。');
    } else if (selection && selection.execution) {
      notices.push('原选择的执行身份与当前计划不一致，已清除定位。');
    }
    const completeness = {display: 'complete', totalNodes: cards.length, totalEdges: edges.length};
    if (graph.logical && graph.logical.truncated) notices.push('action 列表已按预算截断，仅展示已返回的 action。');
    if (isRecord(snapshot) && snapshot.unchanged !== true && snapshotFor(snapshot, binding) === null
        && binding.basis_kind === 'run')
      notices.push('运行摘要与当前 run/attempt 不一致，节点状态按缺测显示。');
    if (!context.bridgeEntries) notices.push(NO_BRIDGE_TEXT);
    return {binding, choice, execution: identity, nodes: cards, edges, selected, completeness, notices};
  }

  /* A compile preview is a logical plan, never a historical execution identity.
   * It uses the same renderer, but cannot acquire observations or physical facts. */
  function buildPlanPreviewModel(value, {selection = null} = {}) {
    const reject = message => {throw new Error('编译计划不可用：' + message);};
    const text = item => typeof item === 'string' && item.length > 0 && item.length <= 256;
    const ports = items => Array.isArray(items) && items.length <= 64
      && items.every(text) && new Set(items).size === items.length;
    if (!isRecord(value) || value.format !== 'demiflow.plan_preview.v1' || value.available !== true
        || value.scope !== 'first_action' || value.entry_completed !== false
        || !isRecord(value.basis) || !isSha(value.basis.digest) || !isRecord(value.dag))
      reject('响应缺少明确的编译依据或动作范围');
    const dag = value.dag;
    if ((dag.schema_version !== V3_SCHEMA && dag.schema_version !== V4_SCHEMA) || !text(dag.dag_id) || !text(dag.action_id)
        || !Array.isArray(dag.nodes) || !dag.nodes.length || dag.nodes.length > MAX_NODES
        || !Array.isArray(dag.edges) || dag.edges.length > MAX_EDGES)
      reject('计划格式或展示预算不受支持');
    const byId = new Map(), keys = new Map();
    for (const node of dag.nodes) {
      if (!isRecord(node) || !text(node.logical_node_id) || !text(node.occurrence_id)
          || !text(node.op_kind) || !ports(node.input_ports) || !ports(node.output_ports)
          || !(isRecord(node.traits) || (dag.schema_version === V4_SCHEMA
                && node.op_kind === 'fit' && node.traits == null)) || byId.has(node.occurrence_id))
        reject('节点身份、端口或声明无效');
      byId.set(node.occurrence_id, node);
      keys.set(node.occurrence_id, jsonKey(['preview', value.basis.digest, dag.dag_id, node.occurrence_id]));
    }
    for (const name of ['sources', 'terminals'])
      if (!Array.isArray(dag[name]) || !dag[name].length || dag[name].length > MAX_NODES
          || dag[name].some(id => !byId.has(id)) || new Set(dag[name]).size !== dag[name].length)
        reject('输入或终端不在计划内');
    const edgeIds = new Set(), incoming = new Map(dag.nodes.map(n => [n.occurrence_id, 0]));
    const outgoing = new Map(dag.nodes.map(n => [n.occurrence_id, []]));
    const edges = dag.edges.map(edge => {
      if (!isRecord(edge) || !text(edge.edge_id) || edgeIds.has(edge.edge_id)
          || !isRecord(edge.from) || !isRecord(edge.to)) reject('边身份无效');
      const from = byId.get(edge.from.occurrence_id), to = byId.get(edge.to.occurrence_id);
      if (!from || !to || from.logical_node_id !== edge.from.logical_node_id
          || to.logical_node_id !== edge.to.logical_node_id
          || !from.output_ports.includes(edge.from.port) || !to.input_ports.includes(edge.to.port))
        reject('数据边没有连接到准确的实例端口');
      edgeIds.add(edge.edge_id);incoming.set(to.occurrence_id, incoming.get(to.occurrence_id) + 1);
      outgoing.get(from.occurrence_id).push(to.occurrence_id);
      return {edgeId: edge.edge_id, from: keys.get(from.occurrence_id), to: keys.get(to.occurrence_id),
        kind: 'data', ...valueEdgeFacts(edge, dag.schema_version === V4_SCHEMA ? 'v4' : 'v3'),
        fromPort: edge.from.port, toPort: edge.to.port,
        exchangeMode: isPreviewBarrier(to, edge.to.port) ? 'finite_sealed' : 'memory_incremental'};
    });
    const queue = [...incoming].filter(([, count]) => count === 0).map(([id]) => id);
    for (let i = 0; i < queue.length; i++) for (const id of outgoing.get(queue[i])) {
      incoming.set(id, incoming.get(id) - 1);if (incoming.get(id) === 0) queue.push(id);
    }
    if (queue.length !== dag.nodes.length) reject('数据图含环');
    const nodes = dag.nodes.map(node => {
      const key = keys.get(node.occurrence_id);
      const rawTitle = typeof node.label === 'string' && node.label.trim()
        && node.label.length <= 256 ? displayTitle(node.label, node.op_kind) : kindLabel(node.op_kind);
      const desc = typeof node.description === 'string' && node.description.trim() ? node.description.trim() : null;
      return {key, selection: {key, reportNodeId: null, actionId: dag.action_id,
        logicalNodeId: node.logical_node_id, occurrenceId: node.occurrence_id,
        execution: null, previewDigest: value.basis.digest}, occurrences: [],
        // Optional labels are supplied by the compiler's declaration, never joined from AST nodes.
        kindLabel: kindLabel(node.op_kind), title: semanticHeadline(rawTitle, desc),
        apiExpr: compactApiExpr(node.api_call, node.label, node.data_source),
        summary: [...(desc
            ? [{label: '说明', value: bounded(desc, 160), basis: 'compile', reason: null}]
            : isRecord(node.api_call) && typeof node.api_call.summary === 'string'
              ? [{label: 'API', value: bounded(node.api_call.summary, 256), basis: 'compile', reason: null}] : []),
          {label: '端口', value: bounded((node.input_ports.join(', ') || '—') + ' → '
          + (node.output_ports.join(', ') || '—'), 160), basis: 'compile', reason: null},
          node.data_source ? {label: '版本', value: Number.isInteger(node.data_source.version)
            ? String(node.data_source.version) : '运行时固定', basis: 'compile', reason: null}
            : {label: '材料', value: node.source_views?.prompt?.available ? 'Prompt 与入参'
              : node.source_views?.implementation?.available ? '算子源码' : '算子入参', basis: 'compile', reason: null}],
        state: {label: '尚未运行', tone: 'neutral'}};
    });
    const notices = ['首个 Dataset 执行动作的完整数据流，尚未运行。依赖运行结果的后续动作不在本图。'];
    return {binding: value.basis, choice: {kind: 'preview', actionId: dag.action_id,
      label: '编译计划 · 尚未运行 · 仅首个执行动作', reason: null}, execution: null,
      nodes, edges, selected: selection?.previewDigest === value.basis.digest
        && nodes.some(n => n.key === selection.key) ? selection.key : null,
      completeness: {display: 'complete', totalNodes: nodes.length, totalEdges: edges.length}, notices};
  }

  /* ---- C04: node details on demand -------------------------------------- */

  function unavailableDetails(selection, title, reason) {
    return {
      selection, title, inputs: [], outputs: [], parameters: [],
      sourceAvailable: false, sourceReason: reason, recoveryRestriction: null,
      execution: null, placement: null, exchanges: [], recovery: null,
    };
  }
  function buildNodeDetails(graph, selection) {
    const binding = graph.binding;
    if (!selection) return unavailableDetails(selection, '未选择节点', 'no_selection');
    if (selection.execution === null) {
      const node = findReportNode(graph, selection.reportNodeId);
      if (!node) return unavailableDetails(selection, '节点不在当前图', 'node_not_loaded');
      const basis = binding.basis_kind === 'source' ? 'source' : 'report';
      return {
        selection, title: reportName(node) || kindLabel(node.kind),
        inputs: inputsFor(graph, node, basis), outputs: fieldFacts(node.fields, basis),
        parameters: parameterFacts(node, basis),
        sourceAvailable: true, sourceReason: null, recoveryRestriction: null,
        execution: null, placement: null, exchanges: [], recovery: null,
      };
    }
    const entry = findAction(graph, selection.actionId);
    const projection = normalizeExecutionPlan(entry, binding);
    if (!projection.available) return unavailableDetails(selection, '执行计划不可用', projection.reason);
    const identity = projection.identity;
    if (!sameIdentity(selection.execution, identity)) return unavailableDetails(selection, '执行身份已过期', 'identity_mismatch');
    const node = projection.nodes.find(item => item.occurrenceId === selection.occurrenceId
      && item.logicalNodeId === selection.logicalNodeId);
    const context = sealedContext(graph, entry, identity, null);
    if (!hasOccurrenceIds(identity.format) && selection.occurrenceId === null) {
      const members = projection.nodes.filter(item => item.logicalNodeId === selection.logicalNodeId);
      if (members.length > 1) {
        const name = reportName(findReportNode(graph, context.nodeMap[selection.logicalNodeId]))
          || indexLabel(graph, context.nodeMap[selection.logicalNodeId]) || kindLabel(members[0].kind);
        return {
          selection, title: bounded(name + '（逻辑组）', 80), inputs: [], outputs: [], parameters: [],
          sourceAvailable: false, sourceReason: 'multiple_occurrences_select_member',
          recoveryRestriction: projection.recovery.restriction, execution: identity,
          placement: members[0].placement, exchanges: [], recovery: projection.recovery,
        };
      }
    }
    if (!node) return unavailableDetails(selection, '实例不在当前计划', 'occurrence_not_in_plan');
    const bridgeEntry = bridgeEntryFor(context.bridgeEntries, node.logicalNodeId, node.occurrenceId);
    const reportId = bridgeEntry ? bridgeEntry.report_node_id : null;
    const reportNode = reportId ? findReportNode(graph, reportId) : null;
    const title = reportName(reportNode) || (reportId ? indexLabel(graph, reportId) : null) || kindLabel(node.kind);
    const inputs = [];
    for (const edge of projection.edges) {
      const adjacent = hasOccurrenceIds(identity.format) ? edge.to.occurrenceId === node.occurrenceId
        : edge.to.logicalNodeId === node.logicalNodeId;
      if (!adjacent) continue;
      const upstream = projection.nodes.find(item => hasOccurrenceIds(identity.format)
        ? item.occurrenceId === edge.from.occurrenceId : item.logicalNodeId === edge.from.logicalNodeId);
      const upstreamBridge = upstream ? bridgeEntryFor(context.bridgeEntries, upstream.logicalNodeId, upstream.occurrenceId) : null;
      const upstreamReportId = upstreamBridge ? upstreamBridge.report_node_id
        : (upstream && typeof context.nodeMap[upstream.logicalNodeId] === 'string' ? context.nodeMap[upstream.logicalNodeId] : null);
      const upstreamNode = upstreamReportId ? findReportNode(graph, upstreamReportId) : null;
      const label = bounded((reportName(upstreamNode) || (upstream ? kindLabel(upstream.kind) : '上游')) + ' · ' + edge.from.port + ' → ' + edge.to.port, 120);
      if (upstreamNode && isRecord(upstreamNode.fields) && Object.keys(upstreamNode.fields).length)
        inputs.push({label, value: bounded(Object.keys(upstreamNode.fields).join(', '), 160), basis: 'sealed', reason: null});
      else inputs.push({label, value: null, basis: 'sealed', reason: upstreamNode ? 'upstream_fields_unknown' : 'upstream_not_bridged'});
      if (inputs.length >= MAX_FACTS) break;
    }
    const outputs = reportNode ? fieldFacts(reportNode.fields, 'sealed')
      : [{label: '输出字段', value: null, basis: 'sealed', reason: 'report_node_unbridged'}];
    const parameters = reportNode ? parameterFacts(reportNode, 'sealed') : [];
    let exchanges = [];
    if (hasOccurrenceIds(identity.format)) {
      const adjacentEdges = new Set(projection.edges
        .filter(edge => edge.from.occurrenceId === node.occurrenceId || edge.to.occurrenceId === node.occurrenceId)
        .map(edge => edge.edgeId));
      exchanges = projection.exchanges.filter(item => adjacentEdges.has(item.edgeId));
    }
    const sourceReason = bridgeEntry ? null
      : (isRecord(entry.identity_bridge) && typeof entry.identity_bridge.reason === 'string'
        ? entry.identity_bridge.reason : 'identity_bridge_missing');
    return {
      selection, title: bounded(title, 80), inputs, outputs, parameters,
      sourceAvailable: !!bridgeEntry, sourceReason,
      recoveryRestriction: projection.recovery.restriction, execution: identity,
      placement: node.placement, exchanges, recovery: projection.recovery,
    };
  }

  globalThis.ForgeWorkbenchPresentation = Object.freeze({
    hasOccurrenceIds, chooseGraphView, buildNodeCard, buildGraphModel, buildNodeDetails, buildPlanPreviewModel, normalizeExecutionPlan, matchesExecutionObservation, compactApiExpr,
  });
})();
