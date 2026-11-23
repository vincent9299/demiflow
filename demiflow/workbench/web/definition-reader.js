/* One definition reader for the project catalog and exact plan snapshots. */
(() => {
  'use strict';
  const el=(tag,text,cls)=>{const n=document.createElement(tag);if(text!=null)n.textContent=text;if(cls)n.className=cls;return n;};
  const json=value=>JSON.stringify(value,null,2);
  function table(host,headings,rows){
    const wrap=el('div',null,'definition-table'),t=el('table',null,'op-contract'),head=el('tr');
    headings.forEach(h=>head.append(el('th',h)));t.append(head);
    for(const row of rows){const tr=el('tr');row.forEach(v=>tr.append(el('td',v??'未声明')));t.append(tr);}wrap.append(t);host.append(wrap);
  }
  function contract(host,value){
    if(value.kind==='prompt'){
      host.append(el('h4','接口契约 · 输入变量'));
      if(value.input_variables?.length)table(host,['变量','类型','要求'],value.input_variables.map(v=>[v.name,v.type,v.required?'必填':'可选']));
      else host.append(el('p',value.input_diagnostic||'未声明模板输入变量。','definition-note'));
      host.append(el('h4','接口契约 · 输出 Schema'));
      if(value.output_schema)host.append(el('pre',json(value.output_schema),'definition-schema'));
      else host.append(el('p',value.response_format==='image'?'图片对象引用 ObjectRef；字段结构以该 API 的原生图片协议为准。':'未声明结构化响应 Schema。','definition-note'));
      const modelInfo=value.model&&typeof value.model==='object'?value.model:(value.model?{name:value.model,...(value.transport?{transport:value.transport}:{})}:null);
      const settings={...(modelInfo?{model:modelInfo.name||modelInfo.model||modelInfo.id||modelInfo}:{}),
        ...(modelInfo?.transport||value.transport?{transport:modelInfo?.transport||value.transport}:{}),
        ...(value.response_format?{response_format:value.response_format}:{}),...(value.defaults||{})};
      if(Object.keys(settings).length){host.append(el('h4','运行契约（定义默认）'));table(host,['运行参数','默认值'],Object.entries(settings).map(([k,v])=>[k,typeof v==='string'?v:json(v)]));}
    }else{
      host.append(el('h4','接口契约 · 输入参数与默认值'));
      const parameters=value.parameters||[];
      if(parameters.length)table(host,['参数','类型','默认值'],parameters.map(p=>[p.name,p.annotation||'未声明',p.required?'必填':p.default_expression??'未声明']));
      else host.append(el('p','未声明输入参数契约。','definition-note'));
      host.append(el('h4','接口契约 · 输出契约'),el('p',value.returns||'未声明返回类型；不推测返回字段。','definition-return'));
      host.append(el('p',value.schema_note||'类型注解属于定义契约；本次 Dataset 字段及绑定在“流程接线”中查看。','definition-note'));
    }
  }
  function modelSummary(value){
    if(typeof value.model==='string'&&value.model)return value.transport?value.model+' · '+value.transport:value.model;
    if(value.model&&typeof value.model==='object'){
      const name=value.model.name||value.model.model||value.model.id||'';
      const transport=value.model.transport||value.transport||'';
      return [name,transport].filter(Boolean).join(' · ');
    }
    return '';
  }
  function render(host,value,{mountText,mode='full'}={}){
    const root=el('section',null,'definition-reader');root.dataset.kind=value.kind;host.append(root);
    const modelLabel=value.kind==='prompt'?modelSummary(value):'';
    const retryLabel=value.kind==='prompt'&&value.defaults&&value.defaults.schema_retries!=null?'重试 '+value.defaults.schema_retries:'';
    const formatLabel=value.kind==='prompt'&&value.response_format?'输出 '+value.response_format:'';
    if(value.basis||value.declared_version||modelLabel||retryLabel||formatLabel||value.role_badge){
      const meta=el('div',null,'definition-meta');
      if(value.role_badge)meta.append(el('span',value.role_badge,'definition-role'));
      if(modelLabel)meta.append(el('span','模型 '+modelLabel,'definition-model'));
      if(formatLabel)meta.append(el('span',formatLabel,'definition-chip'));
      if(retryLabel)meta.append(el('span',retryLabel,'definition-chip'));
      if(value.basis)meta.append(el('span',value.basis));
      if(value.declared_version)meta.append(el('span','版本 '+value.declared_version));
      root.append(meta);
    }
    if(value.signature){const signature=el('pre',value.signature,'definition-signature');root.append(signature);}
    if(mode==='contract'){contract(root,value);return root;}
    if(mode==='full'){
      if(value.kind==='api'){if(value.description){const help=el('details',null,'definition-contract');help.append(el('summary','接口说明'),el('p',value.description,'definition-doc'));root.append(help);}contract(root,value);return root;}
      const fold=el('details',null,'definition-contract');fold.append(el('summary',value.kind==='prompt'?'契约声明（接口 · 运行默认）':'接口契约与默认值'));contract(fold,value);root.append(fold);
    }
    const source=value.kind==='prompt'?(value.template??(value.messages?json(value.messages):value.source)):value.source;
    if(typeof source==='string'){
      const body=el('div',null,'definition-source');root.append(body);mountText(body,source,{sourceOnly:true,compactSource:true,continuousSource:true,
        pythonLine:['code','data_tool'].includes(value.kind)?(value.line||1):undefined});
      const status=body.querySelector('.reading-status');if(status?.textContent==='源码完整显示。')status.hidden=true;
    }else root.append(el('p','这份绑定版本没有可读取的定义正文。','definition-note'));
    if(value.truncated)root.append(el('p','正文超过展示预算，当前为有界片段。','definition-note'));
    return root;
  }
  globalThis.DemiFlowDefinition=Object.freeze({render});
})();
