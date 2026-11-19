/* Portable view references. No DOM, network, storage, or host dependency. */
(() => {
  'use strict';
  const text = (maximum, pattern) => ({type: 'string', minLength: 1, maxLength: maximum, ...(pattern ? {pattern} : {})});
  const fields = {
    workspace: text(2048, '^/'),
    alias: text(256, '^(?:[A-Za-z][A-Za-z0-9_-]{0,63}|run:r[0-9a-f]{24}:[A-Za-z][A-Za-z0-9_-]{0,63}(?::(?:[1-9]|[12][0-9]|3[0-2]))?)$'),
    binding_digest: text(64, '^[0-9a-f]{64}$'),
    report_digest: text(64, '^[0-9a-f]{64}$'),
    run_id: text(25, '^r[0-9a-f]{24}$'),
    session_id: text(33, '^d[0-9a-f]{32}$'),
    document: text(1024, '^.+\\.(?:[mM][dD]|[mM][aA][rR][kK][dD][oO][wW][nN])$'),
    attempt: {type: 'integer', minimum: 1, maximum: 32},
    row_id: text(20, '^(?:0|[1-9][0-9]*)$'),
    node_id: text(64),
    columns: {type: 'array', minItems: 1, maxItems: 16, uniqueItems: true, items: text(256)},
    offset: {type: 'integer', minimum: 0, maximum: Number.MAX_SAFE_INTEGER},
    entry: text(1024, '^.+\\.py:[^/:\\s]+$'),
    params: text(1024, '^.+\\.json$'),
  };
  fields.left = fields.right = fields.run_id;
  fields.expected_binding = fields.binding_digest;
  fields.version = {type: 'integer', minimum: 1, maximum: Number.MAX_SAFE_INTEGER};
  fields.alias_binding = fields.binding_digest;
  fields.action_id = text(128);
  fields.logical_node_id = fields.occurrence_id = text(64, '^[0-9a-f]{64}$');
  fields.dag_id = fields.plan_id = fields.binding_digest;
  fields.plan_revision = {type: 'integer', minimum: 0, maximum: Number.MAX_SAFE_INTEGER};
  fields.plan_format = {type: 'string', minLength: 2, maxLength: 6, pattern: '^(?:legacy|v3)$'};
  const kinds = {
    development: [[], []], development_new: [[], []], development_session: [['session_id'], []],
    overview: [[], ['document']], tables: [[], []], runs: [[], []], checks: [[], []],
    flows: [[], ['entry', 'params']],
    table: [['alias', 'binding_digest'], ['columns', 'offset']],
    case: [['alias', 'binding_digest', 'row_id'], ['columns']],
    inspection: [['report_digest'], ['node_id']],
    run: [['run_id'], ['node_id', 'report_digest', 'attempt']],
    run_compare: [['left', 'right'], []],
  };
  const schemas = Object.fromEntries(Object.entries(kinds).map(([kind, [required, optional]]) => [kind, {
    type: 'object', additionalProperties: false,
    properties: {v: {const: 1}, kind: {const: kind}, workspace: fields.workspace,
      ...Object.fromEntries([...required, ...optional].map(key => [key, fields[key]]))},
    required: ['v', 'workspace', 'kind', ...required],
  }]));
  schemas.run.dependentRequired = {node_id: ['report_digest'], report_digest: ['node_id']};
  const schemasV2 = Object.fromEntries(Object.entries(schemas).map(([kind, spec]) => [kind, {
    ...spec, properties: {...spec.properties, v:{const:2}, workspace:text(33, '^[wl][0-9a-f]{32}$')}
  }]));
  // Presentation only: all three project tabs retain the same plan/run locator.
  for (const kind of ['flows','inspection','checks','runs','run','run_compare']) {
    schemasV2[kind].properties.panel=text(9,'^(?:flows|operators|monitor)$');
  }
  schemasV2.table_request={
    type:'object',additionalProperties:false,
    properties:{v:{const:2},kind:{const:'table_request'},workspace:schemasV2.table.properties.workspace,
      alias:fields.alias,expected_binding:fields.expected_binding,columns:fields.columns,offset:fields.offset},
    required:['v','workspace','kind','alias']
  };
  // Saved Case configurations stay fixed to one immutable server revision.
  // This is a locator only: it grants no source or annotation permission.
  schemasV2.tables.properties.view_id=text(96,'^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,95}$');
  schemasV2.tables.properties.revision={type:'integer',minimum:1,maximum:2147483647};
  schemasV2.tables.dependentRequired={view_id:['revision'],revision:['view_id']};
  // C4.2: a fixed historical version is addressed by the paired (version, alias_binding).
  for (const kind of ['table', 'case', 'table_request']) {
    schemasV2[kind].properties.version = fields.version;
    schemasV2[kind].properties.alias_binding = fields.alias_binding;
    schemasV2[kind].dependentRequired = {...schemasV2[kind].dependentRequired, version: ['alias_binding'], alias_binding: ['version']};
  }
  // C8: run links may carry the executed-plan identity; the four plan fields travel together.
  Object.assign(schemasV2.run.properties, {
    action_id: fields.action_id, logical_node_id: fields.logical_node_id, occurrence_id: fields.occurrence_id,
    dag_id: fields.dag_id, plan_id: fields.plan_id, plan_revision: fields.plan_revision, plan_format: fields.plan_format,
  });
  function object(value) {
    return value !== null && typeof value === 'object' && !Array.isArray(value) &&
      (Object.getPrototypeOf(value) === Object.prototype || Object.getPrototypeOf(value) === null);
  }
  function check(value, spec) {
    if (spec.type === 'string') return typeof value === 'string' && value.length >= spec.minLength &&
      value.length <= spec.maxLength && !/[\u0000-\u001f]/.test(value) && (!spec.pattern || new RegExp(spec.pattern).test(value));
    if (spec.type === 'integer') return Number.isSafeInteger(value) && value >= spec.minimum && value <= spec.maximum;
    if (spec.type === 'array') return Array.isArray(value) && value.length >= spec.minItems && value.length <= spec.maxItems &&
      new Set(value).size === value.length && value.every(item => check(item, spec.items));
    return value === spec.const;
  }
  function boundedJSON(value, maximum) {
    const result = JSON.stringify(value);
    if (new TextEncoder().encode(result).byteLength > maximum) throw new Error('上下文超出字节上限，请缩小字段范围');
    return result;
  }
  function validateView(value) {
    if (!object(value)) throw new Error('不支持的视图类型');
    if(typeof value.document==='string'&&(value.document.includes('\\')||value.document.split('/').some(part=>!part||part.startsWith('.'))))throw new Error('文档路径必须位于项目内');
    const variants=value.v===2?schemasV2:schemas;
    if (!Object.hasOwn(variants,value.kind)) throw new Error('不支持的视图类型');
    const spec = variants[value.kind], keys = Object.keys(value);
    if (keys.some(key => !Object.hasOwn(spec.properties, key)) || spec.required.some(key => !Object.hasOwn(value, key)) ||
        keys.some(key => !check(value[key], spec.properties[key]))) throw new Error('视图字段或版本不合法');
    if (value.v===2&&value.kind==='tables'&&(Object.hasOwn(value,'view_id')!==Object.hasOwn(value,'revision'))) throw new Error('Case 视图链接必须绑定已保存的具体版本');
    if (value.v===2&&['table','case','table_request'].includes(value.kind)&&Object.hasOwn(value,'version')!==Object.hasOwn(value,'alias_binding')) throw new Error('历史版本链接必须同时提供 version 与 alias_binding');
    if (value.v===2&&value.kind==='table_request'&&Object.hasOwn(value,'version')&&Object.hasOwn(value,'expected_binding')&&value.expected_binding!==value.alias_binding) throw new Error('历史版本请求的 expected_binding 必须等于 alias_binding');
    if (value.v===2&&value.kind==='run') {
      const has=key=>Object.hasOwn(value,key);
      if (has('logical_node_id')&&!has('action_id')) throw new Error('逻辑节点定位必须绑定 action_id');
      if (has('occurrence_id')&&!has('logical_node_id')) throw new Error('实例定位必须绑定 logical_node_id');
      if ((has('action_id')||has('logical_node_id')||has('occurrence_id'))&&!has('attempt')) throw new Error('运行定位链接必须明确 attempt');
      const plan=['dag_id','plan_id','plan_revision','plan_format'].filter(has);
      if (plan.length&&plan.length<4) throw new Error('计划定位必须完整提供 dag_id/plan_id/plan_revision/plan_format');
      if (plan.length===4&&(!has('action_id')||!has('attempt'))) throw new Error('计划定位必须绑定 action_id 与明确 attempt');
      if (value.plan_format==='v3'&&has('logical_node_id')&&!has('occurrence_id')) throw new Error('v3 实例定位必须提供 occurrence_id');
    }
    if (value.row_id !== undefined && BigInt(value.row_id) > 18446744073709551615n) throw new Error('行定位值超出 uint64 范围');
    if (value.kind === 'run' && (Object.hasOwn(value, 'node_id') !== Object.hasOwn(value, 'report_digest'))) throw new Error('运行节点必须绑定报告');
    if (['entry', 'params'].some(key => value[key] && (value[key].includes('\\') || value[key].split(':')[0].split('/').some(part => ['', '.', '..'].includes(part))))) throw new Error('路径必须位于项目内');
    return JSON.parse(boundedJSON(value, 4096));
  }
  function encodeView(value) {
    const fragment = new URLSearchParams({view: JSON.stringify(validateView(value))}).toString();
    if (fragment.length > 16384) throw new Error('视图链接超出长度限制');
    return '#' + fragment;
  }
  function decodeView(hash) {
    if (typeof hash !== 'string' || hash.length > 16385) throw new Error('视图链接超出长度限制');
    const params = new URLSearchParams(hash.replace(/^#/, ''));
    if (params.getAll('view').length > 1 || params.getAll('token').length > 1 || [...params.keys()].some(k => !['view', 'token'].includes(k))) throw new Error('重复或未知链接参数');
    if (!params.has('view')) return null;
    const raw = params.get('view');
    if (new TextEncoder().encode(raw).byteLength > 4096) throw new Error('视图描述超出字节限制');
    return validateView(JSON.parse(raw));
  }
  globalThis.ForgeView = Object.freeze({validateView, encodeView, decodeView, boundedJSON, object,
    formatReference: value => JSON.stringify(validateView(value), null, 2),
    viewSchema: {anyOf: [...Object.values(schemas), ...Object.values(schemasV2)]}, caseSchema: schemas.case, columnsSchema: fields.columns,
    validateColumns: value => {if (!check(value, fields.columns)) throw new Error('请选择 1–16 个明确字段'); return [...value];}});
})();
