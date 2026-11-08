/* Discussion-only fixtures: no API, business file access, execution or credentials. */
(() => {
  'use strict';
  const app=document.getElementById('app'), picker=document.getElementById('scene');
  const base={scene:'ready',project:'native',flow:'archive',config:'small',panel:'configure',evidence:'checked',mode:'current',run:'none',permission:'full',node:'clean',fixed:true,dirty:false,input:'8',timeout:600,scope:'flow',filter:false,noMetrics:false,nav:false,sealedFlow:'archive',sealedConfig:'small',sealedInput:'8',sealedTimeout:600,detailScope:'node',rounding:2,savedRounding:2,savedInput:'8',output:'archive.lance',savedOutput:'archive.lance',configDirty:false,reportInput:'8',reportRounding:2,revision:1,sealedRounding:2,sealedOutput:'archive.lance',cause:'source'};
  Object.assign(base,{dataCatalogCollapsed:false,dataOutput:'cases',sourceOpen:true,sourceTab:'implementation',sourceContext:'node',sealedFixed:true,sealedRevision:1,sealedSourceAvailable:true,runId:'',runAttempt:1,runtimeUpdates:{},runHistoryOpen:false,runHistoryPage:0,runLogScope:'run',runLogLimit:6});
  const flows={
    archive:{name:'订单归档',entry:'archive',operator:'金额标准化',symbol:'normalize_order',path:'operators/normalize.py',kind:'自定义算子',purpose:'金额转数值并保留订单标识'},
    summary:{name:'订单汇总',entry:'summary',operator:'订单摘要',symbol:'summarize_order',path:'operators/summarize.py',kind:'自定义算子',purpose:'为每条订单添加金额摘要'},
    model:{name:'订单质检',entry:'review',operator:'模型质检',symbol:'review_orders',path:'operators/review.py',kind:'模型节点',purpose:'用声明的 Prompt 检查订单信息'}
  };
  const scenes={
    blocked:['原生流程 · 检查有问题',{evidence:'blocked',fixed:false}],
    ready:['原生流程 · 可准备',{evidence:'checked',fixed:true}],
    model:['模型节点 · 代码与 Prompt',{flow:'model',sourceTab:'prompt'}],
    missing_source:['历史运行 · 封存源码缺失',{run:'succeeded',mode:'run',panel:'results',sealedSourceAvailable:false}],
    legacy:['原项目 · 尚未接通',{project:'legacy',evidence:'missing',node:'',permission:'read'}],
    removed:['当前入口已移除 · 仍可停止原运行',{project:'legacy',evidence:'missing',run:'running',mode:'run',panel:'run',permission:'control'}],
    stale:['原生流程 · 依据已过期',{evidence:'stale',fixed:true}],
    running:['运行中 · 有指标',{evidence:'checked',fixed:true,run:'running',mode:'run',panel:'run'}],
    failed:['运行失败 · 部分提交',{evidence:'checked',fixed:true,run:'failed',mode:'run',panel:'results'}],
    stopped:['已停止 · 部分提交',{evidence:'checked',fixed:true,run:'cancelled',mode:'run',panel:'results'}],
    empty:['空项目 · 无数据和记录',{project:'empty',evidence:'missing',node:'',panel:'run',permission:'read'}],
    denied:['权限不足 · 只读',{evidence:'checked',fixed:true,permission:'read',panel:'run'}],
    control:['仅控制 · 仍能停止',{evidence:'checked',fixed:true,permission:'control',run:'running',mode:'run',panel:'run'}],
    no_metrics:['运行中 · 暂无采样',{evidence:'checked',fixed:true,run:'running',mode:'run',panel:'run',noMetrics:true}],
    unconfirmed:['停止中 · 尚未确认',{evidence:'checked',fixed:true,run:'unconfirmed',mode:'run',panel:'run'}],
    lost_prepare:['准备响应丢失 · 待核对',{evidence:'checked',fixed:true,run:'pending',panel:'run'}]
  };
  const titles={native:'订单归档 · 示例',legacy:'原项目 · 示例',empty:'空项目 · 示例'};
  const statuses={none:'尚未准备',prepared:'已准备 · 尚未执行',running:'运行中',stopping:'正在停止',unconfirmed:'正在停止 · 尚未确认',cancelled:'已停止',failed:'运行失败',succeeded:'运行成功',pending:'准备结果待核对'};
  const panels={configure:'读图与配置',check:'检查问题',run:'运行监控',results:'输出结果'};
  let state={...base},routeError='';
  const esc=s=>String(s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const dis=x=>x?' disabled':'';
  const btn=(id,label,kind='',off=false)=>`<button type="button" data-action="${id}" class="${kind}"${dis(off)}>${label}</button>`;
  const tag=(text,kind='')=>`<span class="tag ${kind}">${text}</span>`;
  const isRun=()=>state.mode==='run'&&Boolean(runtimeSelected());
  const selectedFlow=()=>isRun()?runtimeSelected().flow:state.flow;
  const selectedConfig=()=>isRun()?runtimeSelected().config:state.config;
  const selectedInput=()=>isRun()?runtimeSelected().input:state.input;
  const selectedTimeout=()=>isRun()?runtimeSelected().timeout:state.timeout;
  const configPath=()=>`settings/${selectedConfig()}.json`;
  const canSubmit=()=>state.project==='native'&&state.permission==='full';
  const canControl=()=>state.permission!=='read';
  const canPrepare=()=>canSubmit()&&state.evidence==='checked'&&!state.configDirty;
  const reportId=()=>isRun()?runtimeSeal():`check-demo-${16+state.revision}`;
  const evidenceTitle=()=>isRun()?'运行封存图 · attempt '+runtimeSelected().attempt:state.project!=='native'?'源码观察 · 关系未完整识别':state.evidence==='stale'?'旧检查图 · 当前依据已变化':state.evidence==='missing'?'源码观察图 · 尚未检查':state.evidence==='blocked'?'原生检查局部图 · 有阻塞':'已检查原生图';
  // Bounded discussion fixtures; runtime selection is shared by graph, evidence and controls.
  const runtimeFixtures=[
    {id:'run-demo-042',attempt:1,started:'11-05 19:42',status:'running',flow:'archive',config:'small',input:'8',timeout:600,rounding:2,output:'archive.lance',fixed:true,revision:1,sourceAvailable:true,rows:8240,archiveVersion:12,auditVersion:4,cpu:'1.24 核',rss:'392 MiB',groupSeconds:'18.2 秒'},
    {id:'run-demo-041',attempt:2,started:'11-05 19:35',status:'running',flow:'summary',config:'small',input:'9',timeout:540,rounding:3,output:'summary.lance',fixed:true,revision:2,sourceAvailable:true,rows:8240,archiveVersion:13,auditVersion:5,cpu:'0.82 核',rss:'286 MiB',groupSeconds:'9.6 秒'},
    {id:'run-demo-041',attempt:1,started:'11-05 19:28',status:'failed',flow:'summary',config:'small',input:'9',timeout:540,rounding:3,output:'summary.lance',fixed:false,revision:1,sourceAvailable:true,rows:8240,archiveVersion:13,auditVersion:5,cpu:null,rss:null,groupSeconds:'4.1 秒'},
    {id:'run-demo-040',attempt:1,started:'11-05 18:50',status:'succeeded',flow:'model',config:'small',input:'9',timeout:480,rounding:2,output:'review.lance',fixed:true,revision:3,sourceAvailable:true,rows:120,archiveVersion:11,auditVersion:3,cpu:'0.46 核',rss:'214 MiB',groupSeconds:'46.8 秒'},
    {id:'run-demo-039',attempt:1,started:'11-05 18:20',status:'cancelled',flow:'archive',config:'full',input:'8',timeout:600,rounding:2,output:'archive.lance',fixed:true,revision:1,sourceAvailable:false,rows:8240,archiveVersion:10,auditVersion:2,cpu:null,rss:null,groupSeconds:'17.8 秒'}
  ];
  const runtimeKeys=new Set(runtimeFixtures.map(r=>r.id+'/'+r.attempt));
  const runtimeUpdateKeys=new Set([...runtimeFixtures.map(r=>'native/'+r.id+'/'+r.attempt),'legacy/run-demo-042/1']);
  const runtimeKey=r=>state.project+'/'+r.id+'/'+r.attempt;
  function runtimeRecords(){
    if(state.project==='empty'||state.project==='legacy'&&['none','pending'].includes(state.run))return [];
    return runtimeFixtures.filter(r=>state.project==='native'||r.id==='run-demo-042').map(record=>{
      let r={...record};
      if(r.id==='run-demo-042'){
        Object.assign(r,{flow:state.sealedFlow,config:state.sealedConfig,input:state.sealedInput,timeout:state.sealedTimeout,rounding:state.sealedRounding,output:state.sealedOutput,fixed:state.sealedFixed,revision:state.sealedRevision,sourceAvailable:state.sealedSourceAvailable});
        if(!['none','pending'].includes(state.run))r.status=state.run;
      }
      return {...r,status:state.runtimeUpdates[runtimeKey(r)]||r.status};
    });
  }
  function runtimeSelected(){
    const id=state.runId||(!['none','pending'].includes(state.run)?'run-demo-042':'');
    const attempt=state.runId?state.runAttempt:1;
    return runtimeRecords().find(r=>r.id===id&&r.attempt===attempt)||null;
  }
  const runStatus=()=>runtimeSelected()?.status||state.run;
  const runtimeMissingMetrics=r=>!r.cpu||r.id==='run-demo-042'&&state.noMetrics;
  const runtimeCanControl=()=>{const r=runtimeSelected();return canControl()&&Boolean(r)&&r.attempt===Math.max(...runtimeRecords().filter(item=>item.id===r.id).map(item=>item.attempt));};
  const runtimeIdentity=(r=runtimeSelected())=>r?r.id+' / attempt '+r.attempt:'未选择运行';
  const runtimeSeal=(r=runtimeSelected())=>r?'seal-demo-'+r.id.slice(-3)+'-a'+r.attempt:'';
  const runDigits=()=>runtimeSelected()?.rounding??state.sealedRounding;
  const runOutput=()=>runtimeSelected()?.output??state.sealedOutput;
  const runFixed=()=>runtimeSelected()?.fixed??state.sealedFixed;
  const runRevision=()=>runtimeSelected()?.revision??state.sealedRevision;
  const runSourceAvailable=()=>runtimeSelected()?.sourceAvailable??state.sealedSourceAvailable;
  function runtimeOutputs(r=runtimeSelected()){
    if(!r)return [];
    const success=r.status==='succeeded',partial=r.status!=='prepared'&&['run-demo-042','run-demo-039'].includes(r.id);
    return [
      {name:'archive',title:'订单结果',committed:success||partial,version:r.archiveVersion,rows:r.rows},
      {name:'audit',title:'审计结果',committed:success,version:r.auditVersion,rows:r.rows}
    ];
  }
  function runtimeNodes(r=runtimeSelected()){
    if(!r)return {};
    const prepared=r.status==='prepared',active=['running','stopping','unconfirmed'].includes(r.status),outputs=runtimeOutputs(r);
    const inFirstAction=r.id==='run-demo-041'&&!['succeeded'].includes(r.status);
    const phase=prepared?'waiting':inFirstAction?r.status==='failed'?'failed':r.status==='cancelled'?'cancelled':'running':'succeeded';
    const labels={waiting:'等待执行',running:'执行中',succeeded:'已完成',failed:'执行失败',cancelled:'已停止'};
    const group={phase,label:'融合组 G1 · '+labels[phase],scope:'write_archive / G1 / occurrence 1',note:flows[r.flow].operator+'与客户关联共享同一次融合观察；耗时和行数归整组。',rows:prepared?'—':inFirstAction&&active?'3,100 / 8,240 行':r.status==='failed'&&inFirstAction?'异常前读取 320 行':r.rows.toLocaleString()+' 行',elapsed:prepared?'—':r.groupSeconds};
    const source={phase:prepared?'waiting':'unobserved',label:prepared?'等待执行':'未单独观测',scope:'未提供独立节点观察',note:'读取属于上游执行范围；不能由整次运行状态推断独立耗时或完成。',rows:'—',elapsed:'—'};
    const output=outputs[0],audit=outputs[1];
    return {
      orders:source,customers:{...source},clean:group,join:{...group},
      output:{phase:output.committed?'succeeded':'waiting',label:output.committed?'已提交 · v'+output.version:prepared?'等待执行':'尚无提交回执',scope:'write_archive / occurrence 1',note:output.committed?'原生提交回执已确认；提交事实与整次运行状态分别保留。':'尚未获得提交回执，不推断已成功或已写入。',rows:output.committed?output.rows.toLocaleString()+' 行':'—',elapsed:'未提供独立耗时'},
      audit:{phase:audit.committed?'succeeded':prepared||inFirstAction?'waiting':active?'running':r.status==='failed'?'failed':'cancelled',label:audit.committed?'已提交 · v'+audit.version:prepared?'等待执行':inFirstAction?(active?'等待执行':'本次未执行'):active?(r.status==='running'?'执行中 · 尚未提交':'停止待确认 · 未提交'):r.status==='failed'?'执行失败 · 未提交':'已停止 · 未提交',scope:'write_audit / occurrence 1',note:audit.committed?'审计提交回执已确认。':active&&!inFirstAction?'正在处理审计输出；未提交部分不能作为固定结果。':'未提供审计提交回执。',rows:audit.committed?audit.rows.toLocaleString()+' 行':active&&!inFirstAction?'4,900 / 8,240 行':'—',elapsed:audit.committed?'2.8 秒':active&&!inFirstAction?'3.2 秒':'—'}
    };
  }
  function runtimeNodeName(id=state.node){
    return {orders:'订单输入',customers:'客户输入',clean:flows[selectedFlow()].operator,join:'关联客户',output:'订单结果',audit:'审计结果'}[id]||'未选择节点';
  }
  function runtimeLogs(r=runtimeSelected()){
    if(!r)return [];
    const lines=[
      `${r.started}:00 INFO  ${runtimeIdentity(r)} · 合成运行日志`,
      `${r.started}:01 INFO  封存 ${runtimeSeal(r)} · ${flows[r.flow].entry} · settings/${r.config}.json`,
      `${r.started}:02 INFO  固定输入 orders v${r.input} / customers v3`
    ];
    if(r.status==='prepared')return [...lines,'尚未执行；当前没有执行端日志。'];
    lines.push('INFO  所属执行已启动；原始文本不保证逐节点归属。');
    if(r.id==='run-demo-041'&&r.status==='failed')lines.push('ERROR 金额字段转换失败：amount="invalid"','ERROR write_archive 动作失败；后续输出未提交。');
    else {
      lines.push(r.id==='run-demo-041'&&r.status!=='succeeded'?'INFO  首个动作仍在执行，已处理 3,100 行。':'INFO  首个动作结束，订单结果提交回执已保存。');
      lines.push(runtimeOutputs(r)[1].committed?'INFO  审计输出提交回执已保存。':'INFO  尚未取得审计输出提交回执。');
    }
    if(r.status==='stopping'||r.status==='unconfirmed')lines.push('CONTROL 停止请求已保存；仍在等待所属执行确认退出。');
    else if(r.status==='cancelled')lines.push('CONTROL 所属执行已确认停止；清理完成，已提交输出保留。');
    else if(r.status==='succeeded')lines.push('CONTROL 本次执行已完成；输出按独立提交回执确认。');
    else if(r.status==='failed')lines.push('CONTROL 本次执行失败；已停止，清理完成。');
    else lines.push('INFO  本条为固定演示快照；不会后台启动或轮询真实作业。');
    return lines;
  }
  function runtimeToolbar(){
    const records=runtimeRecords(),r=runtimeSelected(),latest=records.filter((item,index)=>records.findIndex(other=>other.id===item.id)===index);
    if(!records.length)return state.runHistoryOpen?'<div class="notice">当前项目暂无运行记录；运行条件仍可查看。</div>':'';
    const attempts=records.filter(item=>item.id===r?.id);
    return `<section class="runtime-toolbar" aria-label="运行记录选择"><label>运行记录<select id="runtime-run" aria-label="选择运行记录"><option value=""${r?' disabled':''}>选择一次运行</option>${latest.map(item=>`<option value="${item.id}"${r?.id===item.id?' selected':''}>${item.id} · ${r?.id===item.id?r.started:item.started} · ${statuses[r?.id===item.id?r.status:item.status]}</option>`).join('')}</select></label><label>尝试<select id="runtime-attempt" aria-label="选择运行尝试"${dis(!r)}>${attempts.length?attempts.map(item=>`<option value="${item.attempt}"${r.attempt===item.attempt?' selected':''}>attempt ${item.attempt} · ${statuses[item.status]}</option>`).join(''):'<option>—</option>'}</select></label><span class="runtime-fixture-note">合成记录 · ${latest.length} 次运行 / ${records.length} 次尝试</span>${isRun()?btn('current','返回当前配置'): ''}</section>${state.runHistoryOpen?runtimeHistory():''}`;
  }
  function runtimeHistory(){
    const records=runtimeRecords(),latest=records.filter((item,index)=>records.findIndex(other=>other.id===item.id)===index),page=Math.min(state.runHistoryPage,Math.max(0,Math.ceil(latest.length/3)-1)),items=latest.slice(page*3,page*3+3);
    return `<section class="runtime-history" aria-label="运行记录列表"><div class="runtime-history-heading"><strong>运行记录</strong><span>第 ${page+1} / ${Math.max(1,Math.ceil(latest.length/3))} 页 · 每页最多 3 条示例</span>${btn('close-run-history','收起记录')}</div>${items.map(r=>`<button type="button" class="runtime-history-row" data-run-record="${r.id}" aria-pressed="${runtimeSelected()?.id===r.id}"><span><strong>${r.id}</strong><small>${r.started} · ${flows[r.flow].name} · ${r.config}</small></span><span>${statuses[r.status]} · 最新 attempt ${r.attempt}</span><span>查看监控 →</span></button>`).join('')||'<p class="muted">暂无运行记录</p>'}<div class="runtime-history-pages">${btn('run-page-prev','上一页','',page===0)}${btn('run-page-next','下一页','',(page+1)*3>=latest.length)}</div></section>`;
  }
  function selectRuntime(id,attempt){
    const records=runtimeRecords(),r=records.find(item=>item.id===id&&(!attempt||item.attempt===attempt));
    if(!r)return;
    transition({runId:r.id,runAttempt:r.attempt,mode:'run',panel:'run',detailScope:'node',sourceOpen:false,sourceContext:'node',runLogScope:'run',runLogLimit:6,runHistoryOpen:false});
  }
  function enterMonitoring(extra={}){
    if(state.run==='pending'&&!state.runId){transition({panel:'run',...extra});return;}
    const r=runtimeSelected()||runtimeRecords()[0];
    if(r){transition({runId:r.id,runAttempt:r.attempt,mode:'run',panel:'run',detailScope:'node',sourceOpen:false,runLogScope:'run',runLogLimit:6,...extra});}
    else transition({panel:'run',...extra});
  }
  function changeRuntimeStatus(status){
    const r=runtimeSelected();if(!r)return;
    transition({runtimeUpdates:{...state.runtimeUpdates,[runtimeKey(r)]:status},runLogLimit:18},true);
  }
  function runtimeSummary(){
    const r=runtimeSelected();if(!r||!isRun())return '';
    const missing=runtimeMissingMetrics(r);
    return `<div class="runtime-summary" data-runtime-project="${state.project}" data-runtime-context="${runtimeIdentity(r)}"><div>${tag(statuses[r.status],r.status==='succeeded'?'good':r.status==='failed'?'bad':'')}<span>${runtimeIdentity(r)}</span><span>${r.started} 开始 · 固定演示快照</span></div><div class="runtime-samples"><span>CPU <b>${missing?'暂无采样':r.cpu}</b></span><span>RSS <b>${missing?'暂无采样':r.rss}</b></span><span>范围：所属进程树</span>${r.status==='running'?btn('finish','演示运行完成','',!runtimeCanControl()):''}</div></div>`;
  }
  function runtimeLogPanel(){
    const r=runtimeSelected();if(!r)return '';
    const observation=runtimeNodes(r)[state.node],nodeScope=state.runLogScope==='node',lines=runtimeLogs(r),visible=lines.slice(0,state.runLogLimit);
    const evidence=observation?{fixture:true,run_id:r.id,attempt:r.attempt,selected_node:state.node,observation_scope:observation.scope,state:observation.label,rows:observation.rows,elapsed:observation.elapsed,note:observation.note}:null;
    return `<section class="runtime-log-panel" aria-label="所选运行的日志与关联观察" data-runtime-project="${state.project}" data-runtime-context="${runtimeIdentity(r)}"><div class="runtime-log-heading"><div><h3>${nodeScope?'节点关联观察':'本次运行日志'}</h3><span>${runtimeIdentity(r)}</span></div><nav aria-label="日志查看范围">${btn('run-log-all','运行日志',!nodeScope?'active':'')}${btn('run-log-node','节点关联观察',nodeScope?'active':'')}</nav></div>${nodeScope?`<p class="runtime-log-scope">${runtimeNodeName()} · 仅展示可关联观察，不是完整节点日志。</p><pre class="runtime-log-text" tabindex="0">${esc(evidence?JSON.stringify(evidence,null,2):'没有选中节点。')}</pre>`:`<p class="runtime-log-scope">范围：本次运行 / attempt 原始文本 · 未提供可靠节点关联，选节点不会筛掉其他日志。</p><pre class="runtime-log-text" tabindex="0">${esc(visible.join('\n'))}</pre><div class="runtime-log-foot"><span>已显示 ${visible.length} / ${lines.length} 条合成日志</span>${btn('run-log-more','读取后续日志','',visible.length>=lines.length)}</div>`}</section>`;
  }
  function runtimeNodeDetail(){
    const r=runtimeSelected(),observation=runtimeNodes(r)[state.node];
    if(!observation)return '<p class="muted">选择图上节点查看本次运行的观察。</p>';
    return `<section class="runtime-node-detail" data-runtime-project="${state.project}" data-runtime-context="${runtimeIdentity(r)}"><p class="muted small">本次运行 · 选中节点</p><h2>${runtimeNodeName()}</h2><span class="runtime-state state-${observation.phase}">${observation.label}</span><p>${observation.note}</p><dl class="key-values"><div><dt>观察范围</dt><dd>${observation.scope}</dd></div><div><dt>处理行数</dt><dd>${observation.rows}</dd></div><div><dt>${['clean','join'].includes(state.node)?'融合组耗时':'节点耗时'}</dt><dd>${observation.elapsed}</dd></div></dl>${r.id==='run-demo-041'&&r.status==='failed'&&['clean','join'].includes(state.node)?'<div class="notice error">动作异常：amount=&quot;invalid&quot; 无法转换为数值。该异常属于融合组，不推断两个节点各自失败。</div>':''}<div class="actions">${btn('run-log-node','查看关联观察') }${btn('source','查看封存代码')}</div></section>`;
  }

  function validRuntimeUpdates(value){
    return Boolean(value)&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).length<=5&&Object.entries(value).every(([key,status])=>runtimeUpdateKeys.has(key)&&['prepared','running','stopping','unconfirmed','cancelled','failed','succeeded'].includes(status));
  }
  function readRoute(){
    if(!location.hash){const view=new URLSearchParams(location.search).get('view');return {...base,...(['data','cases'].includes(view)?{panel:'data',dataOutput:view==='data'?'query':'cases'}:{})};}
    try{
      if(location.hash.length>8192||!location.hash.startsWith('#draft='))throw Error('样稿定位链接无效');
      const v=JSON.parse(decodeURIComponent(location.hash.slice(7)));
      if(!v||typeof v!=='object'||Array.isArray(v))throw Error('样稿定位链接无效');
      const enums={scene:Object.keys(scenes),project:Object.keys(titles),flow:Object.keys(flows),sealedFlow:Object.keys(flows),config:['small','full'],sealedConfig:['small','full'],panel:[...Object.keys(panels),'history','development','overview','data'],evidence:['blocked','checked','stale','missing'],mode:['current','run'],run:Object.keys(statuses),permission:['full','read','control'],node:['','orders','customers','clean','join','output','audit'],input:['8','9'],sealedInput:['8','9'],scope:['flow','project'],detailScope:['node','graph'],cause:['source','config'],sourceTab:['implementation','call','prompt','configuration'],sourceContext:['node','entry'],dataOutput:['query','cases'],runId:['','run-demo-042','run-demo-041','run-demo-040','run-demo-039'],runLogScope:['run','node']};
      for(const [k,x]of Object.entries(v)){
        const limits={runAttempt:[1,2],runHistoryPage:[0,1],runLogLimit:[6,18],timeout:[1,600],sealedTimeout:[1,600],rounding:[0,6],savedRounding:[0,6],sealedRounding:[0,6],reportRounding:[0,6],revision:[1,999],sealedRevision:[1,999]};
        const valid=k==='runtimeUpdates'?validRuntimeUpdates(x):enums[k]?enums[k].includes(x):['savedInput','reportInput'].includes(k)?['8','9'].includes(x):['output','savedOutput','sealedOutput'].includes(k)?typeof x==='string'&&x.length<=128&&/^[a-z][a-z0-9_-]*\.lance$/.test(x):limits[k]?Number.isInteger(x)&&x>=limits[k][0]&&x<=limits[k][1]:typeof x==='boolean';
        if(!Object.hasOwn(base,k)||!valid)throw Error('样稿定位字段无效');
      }
      if(v.runtimeUpdates&&Object.keys(v.runtimeUpdates).some(key=>!key.startsWith((v.project||base.project)+'/')))throw Error('运行状态与项目不匹配');
      if(v.runId&&(!runtimeKeys.has(v.runId+'/'+(v.runAttempt||1))||v.project==='empty'||v.project==='legacy'&&v.runId!=='run-demo-042'))throw Error('运行记录与项目或尝试不匹配');
      if(v.panel==='history'){v.panel='run';v.runHistoryOpen=true;}
      routeError='';return {...base,...(!Object.hasOwn(v,'dataOutput')&&new URLSearchParams(location.search).get('view')==='data'?{dataOutput:'query'}:{}),...v};
    }catch(e){routeError=e.message+'；已回到默认讨论场景。';return {...base};}
  }
  function persist(replace=false){history[replace?'replaceState':'pushState'](null,'','#draft='+encodeURIComponent(JSON.stringify(state)));}
  function transition(update,replace=false){state={...state,...update};persist(replace);render();}
  function notice(){
    if(isRun()&&state.panel==='run')return '';
    if(isRun())return `<div class="notice"><div>查看 <strong>${runtimeIdentity()}</strong> 的封存图；后续源码、配置和入口变化不改变本次记录。</div>${btn('current','返回当前配置')}</div>`;
    if(state.project==='empty')return `<div class="notice">尚未识别流程；项目检查、运行能力和历史始终可达。</div>`;
    if(state.project==='legacy')return `<div class="notice warning"><div><strong>已识别原入口，完整图与原生绑定尚未形成。</strong> 动态关系保持未知。</div>${btn('show-run','查看运行接入条件')}</div>`;
    if(state.configDirty)return `<div class="notice warning"><div><strong>有未保存的配置草稿</strong> · 节点显示待保存值；旧检查不覆盖这些修改。</div>${btn('save-config','保存示例配置','primary',!canSubmit())}</div>`;
    if(state.evidence==='blocked')return `<div class="notice warning"><div><strong>1 个问题需要处理</strong> · amount 字段与契约不符，当前不能准备运行。</div>${btn('show-check','查看问题')}</div>`;
    if(state.evidence==='stale')return `<div class="notice warning"><div><strong>依据已过期</strong> · ${state.cause==='config'?'配置已保存，旧检查图尚未更新':'源码在检查后变化'}。旧图保留供对照。</div>${btn('recheck','重新检查')}</div>`;
    if(state.evidence==='missing')return `<div class="notice"><div>当前配置尚未检查，不沿用其他配置的通过结论。</div>${btn('recheck','检查当前配置')}</div>`;
    if(state.dirty)return `<div class="notice warning"><div><strong>运行预算已调整，待准备时核对。</strong> 代码和配置检查与运行准入分别保留。</div></div>`;
    return `<div class="notice success"><div><strong>接线检查通过。</strong> 行为与 Review 证据另列。</div></div>`;
  }
  function unavailableGraph(){
    return `<section class="section"><div class="section-title"><h2>数据算子图</h2>${tag('已知部分与未知区域','warn')}</div><p class="section-subtitle">${state.project==='empty'?'尚无可识别的公开入口':'只读来源：公开声明、README 与原入口'} · 不执行项目代码</p><div class="legacy-map"><div><span class="muted">入口</span><strong>${state.project==='empty'?'尚未识别':'dispatch.py:run_pipeline'}</strong><p>登记、识别和原生绑定分别判断</p></div><div><span class="muted">配置</span><strong>${state.project==='empty'?'暂无明确引用':'settings/default.json'}</strong><p>明确引用不等于执行配置绑定</p></div><div><span class="muted">未知区域</span><strong>数据 → 算子 → 输出</strong><p>缺少依据，不补造数据边</p></div></div><p class="section-subtitle">项目检查、运行管理与已有记录仍可访问。</p></section>`;
  }
  function graph(){
    if(state.project!=='native'&&!isRun())return unavailableGraph();
    const input=isRun()?runtimeSelected().input:state.configDirty?state.input:state.evidence==='stale'?state.reportInput:state.savedInput;
    const digits=isRun()?runDigits():state.configDirty?state.rounding:state.evidence==='stale'?state.reportRounding:state.savedRounding;
    const nodes=[
      ['orders','数据源','订单输入',`orders.lance · v${input}`,'读取固定版本订单'],
      ['customers','数据源','客户输入','customers.lance · v3','提供客户关联字段'],
      ['clean',flows[selectedFlow()].kind,flows[selectedFlow()].operator,selectedFlow()==='model'?'review_order · concurrency = 2':`rounding_digits = ${digits}`,flows[selectedFlow()].purpose],
      ['join','算子','关联客户','customer_id · left join','为订单补充客户信息'],
      ['output','输出','订单结果',isRun()?runOutput():state.configDirty?state.output:state.savedOutput,'保存加工后的订单'],
      ['audit','输出','审计结果',`共享 rounding_digits = ${digits}`,'保存本次处理摘要']
    ];
    const observing=state.evidence==='missing'&&!isRun();
    const nodeObservations=isRun()?runtimeNodes():{};
    const observation=id=>nodeObservations[id]?.label||'';
    return `<section class="section"><div class="section-title"><div><h2>数据算子图</h2><p class="section-subtitle">${evidenceTitle()} · ${observing?'测试源码观察':reportId()} · 6 节点 / 5 条边</p></div>${btn('graph-config','整图配置',state.detailScope==='graph'?'active':'')}</div><div data-runtime-project="${state.project}" data-runtime-context="${isRun()?runtimeIdentity():'current'}" class="graph-wrap${observing?' observing':''}"><svg class="graph-edges" aria-hidden="true"></svg><div class="graph-nodes">${nodes.map(([id,kind,title,detail,purpose])=>`<button type="button" data-node="${id}" data-runtime-state="${nodeObservations[id]?.phase||''}" class="node ${isRun()?'runtime-node state-'+nodeObservations[id].phase:''} ${state.node===id&&state.detailScope==='node'?'selected':''} ${id==='clean'&&state.evidence==='blocked'&&!isRun()?'problem':''}" aria-pressed="${state.node===id&&state.detailScope==='node'}"><span class="node-label">${kind}</span><strong>${title}</strong><span class="node-purpose">${purpose}</span><small>${detail}</small>${!isRun()&&state.configDirty&&['orders','clean','output','audit'].includes(id)?'<span class="node-draft">配置草稿 · 待保存</span>':''}${observation(id)?`<span class="node-observation">${observation(id)}</span>`:''}${id==='clean'&&state.evidence==='blocked'&&!isRun()?'<span class="node-problem">1 个字段契约问题</span>':''}</button>`).join('')}</div></div><div class="graph-legend"><span>${observing?'虚线：局部静态关系，语义未检查':'实线：报告中的数据关系'}</span><span>点节点：用途、配置、问题与来源</span></div>${isRun()?`<p class="fusion-note">G1 包含${flows[selectedFlow()].operator}与关联客户；观察范围以本次 action / occurrence 为准，不给每个节点分摊独立耗时。CPU / RSS 归整次运行。</p>`:''}${state.panel==='run'&&isRun()?runtimeLogPanel():''}${sourcePanel()}</section>`;
  }
  function graphConfig(){
    return `<p class="muted small">整张图</p><h2>流程与运行配置</h2><p class="detail-description">共享参数与累计预算挂在这里；图上各节点保留原配置来源。</p><label class="field">累计运行时限（秒）<input id="timeout" type="number" min="1" max="600" value="${selectedTimeout()}"${dis(isRun()||!canSubmit())}></label><p class="muted small">仅可收紧服务允许的预算，准备时核对。</p><dl class="key-values"><div><dt>原配置</dt><dd><code>${configPath()}</code></dd></div><div><dt>平台额度</dt><dd>沿用既有准入与资源分区</dd></div><div><dt>共享精度</dt><dd>${isRun()?runDigits():state.rounding} · 影响金额标准化、审计输出</dd></div></dl><p class="muted small">节点字段能映射到原参数时才可编辑；代码中的字面量或动态来源提供源码定位。</p><div class="actions">${btn('show-configure','返回节点配置')}${btn('source-entry','查看入口源码')}</div><details><summary>配置来源与保存语义</summary><p class="muted small">参数写回原配置，检查依据随保存失效；冲突时保留草稿。样稿的保存仅修改测试状态。</p></details>`;
  }
  function nodeDetail(){
    if(state.detailScope==='graph')return graphConfig();
    const names={orders:'订单输入',customers:'客户输入',clean:flows[selectedFlow()].operator,join:'关联客户',output:'订单结果',audit:'审计结果'};
    const summaries={orders:'从固定版本读取订单，提供后续加工的输入。',customers:'读取固定版本的客户表，为关联提供字段。',clean:'调用 normalize_order，将金额转为数值并按指定精度舍入。',join:'按 customer_id 左关联客户，保留订单输入。',output:'向已声明位置内的 Lance 表写入订单结果。',audit:'写入处理摘要，使用与金额标准化相同的精度参数。'};
    summaries.clean=selectedFlow()==='model'?'调用声明的模型算子；模板、模型引用和请求预算随节点绑定。':selectedFlow()==='summary'?'为每条订单生成金额摘要，保留原字段。':summaries.clean;
    const source=state.node==='clean'?flows[selectedFlow()].path+':7':'flows/orders.py:'+({orders:8,customers:9,join:14,output:17,audit:18}[state.node]||8);
    const off=isRun()||!canSubmit();let fields='';
    if(state.node==='orders')fields=`<label class="field">订单固定版本<select id="input-version"${dis(off)}><option value="8"${selectedInput()==='8'?' selected':''}>orders.lance · v8</option><option value="9"${selectedInput()==='9'?' selected':''}>orders.lance · v9</option></select></label><p class="origin">配置来源：<code>${configPath()}#/arguments/orders</code></p>`;
    if(['clean','audit'].includes(state.node)&&!(state.node==='clean'&&selectedFlow()==='model'))fields=`<label class="field">金额保留小数位<input id="rounding" type="number" min="0" max="6" value="${isRun()?runDigits():state.rounding}"${dis(off)}></label><p class="origin">配置来源：<code>${configPath()}#/arguments/rounding_digits</code></p><p class="shared-note">共享参数 · 同时影响“金额标准化”和“审计结果”。</p>`;
    if(state.node==='clean'&&selectedFlow()==='model')fields='<dl class="key-values"><div><dt>模型引用</dt><dd>demo-review-model</dd></div><div><dt>Prompt</dt><dd>review_order</dd></div><div><dt>并发 / 队列 / 请求上限</dt><dd>2 / 2 / 200</dd></div></dl><p class="origin">prompts/order_review.yaml · 合成示例。下方查看实现、调用点、模板与配置。</p>';
    if(state.node==='output')fields=`<label class="field">已声明位置内的输出文件<input id="output-path" pattern="[a-z]([a-z0-9_]|-)*[.]lance" maxlength="128" value="${isRun()?runOutput():state.output}"${dis(off)}></label><p class="origin">配置来源：<code>${configPath()}#/arguments/output/relative_uri</code></p><p class="muted small">位置与写入权限由平台声明，编辑不能扩大范围。</p>`;
    if(state.node==='join')fields='<dl class="key-values"><div><dt>连接字段</dt><dd>customer_id</dd></div><div><dt>连接方式</dt><dd>left</dd></div></dl><p class="origin">来自 Python 字面量，当前只读；通过源码定位修改。</p>';
    if(state.node==='customers')fields='<p class="origin">固定引用：customers.lance v3 · 当前示例中为代码字面量，只读。</p>';
    const problem=state.node==='clean'&&state.evidence==='blocked'&&!isRun();
    return `<p class="muted small">节点用途与配置</p><h2>${names[state.node]||'金额标准化'}</h2><p class="detail-description">${summaries[state.node]||summaries.clean}</p>${fields}<div class="actions">${state.configDirty?btn('save-config','保存示例配置','primary',off):''}${btn('source','查看实现与来源')}</div>${problem?`<div class="notice warning">amount 返回值与契约不符</div><div class="actions">${btn('related','查看关联问题','primary')}${btn('source','定位源码')}</div>`:''}<details><summary>实现、字段与配置依据</summary><p><code>${source}</code></p><pre><code>${esc(JSON.stringify({fixture:true,node:state.node,configuration:configPath(),fields:['order_id','amount'],basis:evidenceTitle()},null,2))}</code></pre></details>`;
  }
  function readiness(){
    const native=state.project==='native';
    return `<section><h2>运行管理</h2><div class="capabilities"><span>准备与提交</span><span>监控与停止</span><span>固定结果与证据</span></div><div class="readiness"><div><strong>流程与配置</strong><span>${native?'已绑定原生入口与参数':'尚未绑定原生 Flow 与参数'}</span></div><div><strong>检查依据</strong><span>${native&&state.evidence==='checked'?'接线已检查；有效覆盖值仍需准备检查':'未满足当前原生准备条件'}</span></div><div><strong>提交权限</strong><span>${canSubmit()?'服务和项目允许':'当前未开放；由部署者按已有授权配置'}</span></div><div><strong>控制权限</strong><span>${canControl()?'允许停止/核对已有运行':'当前未开放；可读不等于可停止'}</span></div><div><strong>执行范围</strong><span>原生 Local · 不托管任意脚本 / 原协议 / Ray / GPU</span></div></div><div class="primary-next"><p>${canPrepare()?'准备会检查有效配置并封存本次输入与代码。':'满足原生绑定、检查与授权后即可准备。'}</p>${btn('prepare','运行','primary',!canPrepare())}</div></section>`;
  }
  function configure(){
    return `<div class="workspace-grid graph-first">${graph()}<aside class="detail side-pane">${state.project==='native'||isRun()?nodeDetail():readiness()}</aside></div>`;
  }
  function workSurface(){
    if(state.panel==='configure')return configure();
    return `<div class="workspace-grid graph-first">${graph()}<div class="detail side-pane">${({check,run,results})[state.panel]()}</div></div>`;
  }
  function check(){
    const bad=state.evidence==='blocked'&&!isRun();let body='';
    if(state.project==='legacy')body=`<article class="issue"><div class="issue-category">${tag('平台支持缺口','warn')}<span>影响：原生准备 / 提交</span></div><h3>当前不支持原 config → run_pipeline 协议</h3><p>已识别原入口和配置。项目负责人改造后需明确绑定原生入口与参数。</p><code>dispatch.py:run_pipeline · unsupported_flow_protocol</code><div>${btn('show-run','查看接入条件','link')}</div></article><article class="issue"><div class="issue-category">${tag('读取范围')}<span>影响：部分数据引用不可查看</span></div><h3>2 个引用在注册目录之外</h3><p>保留引用和原因，不因只读发现而扩大权限。</p></article>`;
    else if(state.project==='empty')body=`<div class="empty"><h2>尚无可检查的明确入口</h2><p>先明确入口与配置；没有检查的部分不显示通过。</p></div>`;
    else if(state.evidence==='stale'&&!isRun())body=`<article class="issue"><div class="issue-category">${tag('依据已过期','warn')}<span>影响：当前配置不可沿用旧结论</span></div><h3>源码或配置在报告保存后变化</h3><p>重新检查生成新依据；旧报告和历史运行保留。</p>${btn('recheck','重新检查','primary')}</article>`;
    else if(bad)body=`<article class="issue"><div class="issue-category">${tag('代码 / 配置问题','bad')}<span>影响：准备运行</span></div><h3>金额字段返回了字符串，契约要求数值</h3><p>定位到“金额标准化”节点。修改输出或明确正确契约后重新检查。</p><code>operators/normalize.py:11 · 测试规则 OP-DEMO</code><div class="actions">${btn('source','定位源码','primary')}${btn('locate-node','在图中定位')}</div><details><summary>原始诊断与检查依据</summary><pre><code>OP-DEMO · invalid · fixture diagnostic
amount: expected number; inferred string
path: operators/normalize.py:11
report: check-demo-17</code></pre></details></article>`;
    else if(state.evidence==='missing')body=`<div class="empty"><h2>当前配置尚未检查</h2><p>检查只覆盖所选入口、配置、源码和固定输入。</p></div>${btn('recheck','检查当前配置','primary')}`;
    else body=`<article class="issue"><div class="issue-category">${tag('原生接线检查通过','good')}<span>当前流程 · 当前配置</span></div><h3>当前接线没有已知阻塞</h3><p>准备会检查有效覆盖值并封存；这不是业务质量认证。</p><div class="actions">${btn('show-run','核对并准备','primary',!canSubmit())}${btn('show-configure','返回配置与图')}</div></article>`;
    return `<div class="workspace-grid"><section><div class="scope-switch"><span>范围</span>${btn('scope-flow','当前流程',state.scope==='flow'?'active':'')}${btn('scope-project','整个项目',state.scope==='project'?'active':'')}${state.filter?`${tag('仅看当前节点')}${btn('all-issues','显示全部问题','link')}`:''}</div>${body}${!state.filter?`<article class="issue"><div class="issue-category">${tag('证据尚未完成')}<span>影响：项目整体认证</span></div><h3>行为验证与独立 Review 仍缺少当前证据</h3><p>与 Local 作业的实际准入分别判断；不以整体未完成掩盖当前能做什么。</p></article>`:''}</section><aside class="detail"><h2>这次检查了什么</h2><dl class="key-values"><div><dt>范围</dt><dd>${state.scope==='project'?'整个已声明项目':'当前入口 + 当前配置'}</dd></div><div><dt>源码</dt><dd>${state.project==='native'?'2 份实际读取':'公开声明与入口'}</dd></div><div><dt>Flow 语义</dt><dd>${state.project==='native'?'本配置原生接线':'未覆盖'}</dd></div><div><dt>算子行为</dt><dd>未执行</dd></div><div><dt>依据</dt><dd>${reportId()} · 测试</dd></div></dl><details><summary>规则、覆盖与原始状态</summary><pre><code>Fixture only
static_status: ${esc(isRun()?'checked':state.evidence)}
overall_status: incomplete
behavior: missing
review: missing</code></pre></details></aside></div>`;
  }
  function run(){
    if(runStatus()==='none')return readiness();
    if(runStatus()==='pending')return `<section><h2>准备响应丢失，先核对原请求</h2><p class="muted">请求 ID：request-demo-042 · 原请求载荷已保留</p><div class="notice warning">尚不能确定是否生成运行。查询原请求，不创建新的准备请求。</div><div class="actions">${btn('recover','演示查询到原运行','primary')}${btn('show-configure','查看配置')}</div></section>`;
    if(!isRun())return `<div class="notice"><div>当前是配置视角。选择已有运行后查看其封存依据。</div>${btn('open-run','打开已有运行')}</div>${readiness()}`;
    const r=runtimeSelected(),active=['running','stopping','unconfirmed'].includes(r.status),terminal=['failed','cancelled','succeeded'].includes(r.status),committed=runtimeOutputs(r).filter(item=>item.committed).length;
    return `${runtimeNodeDetail()}<section class="runtime-run-detail"><h2>${statuses[r.status]}</h2><p class="run-ref">${runtimeIdentity(r)} · 合成运行${r.attempt<Math.max(...runtimeRecords().filter(item=>item.id===r.id).map(item=>item.attempt))?' · 历史尝试，只读':''}</p><div class="run-facts"><span>输入 <b>orders v${selectedInput()} / customers v3</b></span><span>预算 <b>${selectedTimeout()} 秒累计</b></span></div>${r.status==='prepared'?'<div class="notice success">已封存，尚未执行。</div>':''}${['stopping','unconfirmed'].includes(r.status)?`<div class="notice warning"><div><strong>停止请求已保存，不等于已经停止。</strong>${r.status==='unconfirmed'?'尚未确认全部所属执行退出；cleanup=pending。':'等待所属执行退出；已提交输出保留。'}</div>${btn('confirm-stop','演示执行端确认停止','',!runtimeCanControl())}</div>`:''}${terminal?`<div class="notice ${r.status==='succeeded'?'success':'warning'}">${r.status==='failed'?'动作失败；所属执行已停止。':r.status==='cancelled'?'所属执行已确认停止，清理完成。':'所属执行已完成。'}${committed} / 2 个输出已提交，提交事实独立保留。</div>`:`<p class="muted small">${committed} / 2 个输出已有提交回执。固定演示快照，不执行真实作业。</p>`}<div class="actions">${btn('show-results','查看输出与证据')}${btn('show-configure','查看封存配置')}</div><details><summary>${active?'性能趋势与采样范围':'保留的采样与封存依据'}</summary><p class="muted small">${runtimeSeal(r)} · 所属进程树 CPU / RSS，当前 ${runtimeMissingMetrics(r)?'暂无采样':'为合成采样'}。</p>${runtimeMissingMetrics(r)?'<p class="muted small">没有采样，保留为空。</p>':`<svg class="trend" data-runtime-project="${state.project}" data-runtime-context="${runtimeIdentity(r)}" role="img" aria-label="所选运行的合成 CPU 趋势"></svg><p class="muted small">60–80 秒缺测；保留曲线断点，不生成 ETA。</p>`}</details></section>`;
  }
  function results(){
    if(!isRun()||runStatus()==='prepared')return `<div class="empty"><h2>${runStatus()==='prepared'?'这次尚无结果':'尚未选择运行'}</h2><p>准备不执行。选择一次运行后查看其固定结果与证据。</p></div>${btn('show-run','选择运行记录')}`;
    const r=runtimeSelected(),outputs=runtimeOutputs(r),active=['running','stopping','unconfirmed'].includes(r.status);
    return `<section data-runtime-project="${state.project}" data-runtime-context="${runtimeIdentity(r)}"><div class="section-title"><h2>输出与证据</h2>${tag(statuses[r.status],r.status==='succeeded'?'good':'warn')}</div><p class="section-subtitle">${runtimeIdentity(r)} · 提交事实与运行状态分别读取</p><table class="outputs"><thead><tr><th>输出</th><th>提交事实</th><th>固定版本</th><th>下一步</th></tr></thead><tbody>${outputs.map(item=>`<tr><td>${item.name}</td><td>${tag(item.committed?'已提交':active?'尚未确认':'未提交',item.committed?'good':'')}</td><td>${item.committed?'v'+item.version+' · '+item.rows.toLocaleString()+' 行':'—'}</td><td>${item.committed&&item.name==='archive'&&r.id==='run-demo-042'?btn('view-data','查看示例数据','link'):'查看下方固定引用'}</td></tr>`).join('')}</tbody></table><dl class="key-values"><div><dt>停止/完成</dt><dd>${active?'运行或停止仍待确认':'所属执行已停止'}</dd></div><div><dt>清理</dt><dd>${r.status==='unconfirmed'?'pending · 保留容量':active?'尚未结束':'complete'}</dd></div></dl><details><summary>固定引用与原始证据</summary><pre><code>${esc(JSON.stringify({fixture:true,run_id:r.id,attempt:r.attempt,seal:runtimeSeal(r),outputs:outputs.map(item=>({name:item.name,committed:item.committed,version:item.committed?item.version:null,rows:item.committed?item.rows:null}))},null,2))}</code></pre></details>${btn('show-run','返回本次运行')}</section>`;
  }
  function readme(){
    const name=titles[state.project].replace(' · 示例','');
    const introduction=state.project==='native'?'读取固定版本的订单与客户数据，完成金额标准化和客户关联，再写入订单与审计结果。':state.project==='legacy'?'保留原入口与原配置，通过公开声明查看已知关系。当前原生绑定与完整算子图尚未形成。':'项目尚无明确的公开入口；可以先在开发中补充项目说明与管道声明。';
    const raw=`# ${name}\n\n${introduction}\n\n## 使用方式\n\n- 开发：围绕项目代码继续开发。\n- 管道：读图、配置、检查、运行和查看历史记录。\n- 数据：查看固定版本数据与 Case。\n\n本文为样稿中的 README 示例。`;
    return `<article class="readme-preview"><p class="muted small">README.md · 测试项目说明</p><h2>${name}</h2><p>${introduction}</p><h3>使用方式</h3><ul><li><strong>开发</strong>：围绕项目代码继续开发。</li><li><strong>管道</strong>：读图、配置、检查、运行和查看历史记录。</li><li><strong>数据</strong>：查看固定版本数据与 Case。</li></ul><details><summary>查看 README 原文</summary><pre><code>${esc(raw).replaceAll(String.fromCharCode(92,110),String.fromCharCode(10))}</code></pre></details></article>`;
  }
  function other(){
    if(state.panel==='overview')return readme();
    if(state.panel==='data')return ForgeDataDemo.html(state.project,{catalogCollapsed:state.dataCatalogCollapsed});
    const text={development:['项目开发对话','沿用现有开发功能，返回管道时保留选择与问题定位。当前为样稿占位，不启动代码代理。'],data:['数据与 Case','测试结果 archive v12 · 8,240 行。固定版本与来源绑定保留。']}[state.panel];
    return `<section class="empty"><h2>${text[0]}</h2><p>${text[1]}</p><div class="actions source-preview">${btn(isRun()?'show-results':'show-configure','返回管道','primary')}</div></section>`;
  }
  function drawEdges(){
    const wrap=app.querySelector('.graph-wrap');if(!wrap)return;
    const svg=wrap.querySelector('svg'),box=wrap.getBoundingClientRect(),narrow=innerWidth<=760;
    svg.setAttribute('width',String(box.width));svg.setAttribute('height',String(box.height));
    svg.innerHTML='<defs><marker id="demo-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto"><path d="M 0 1 L 7 4 L 0 7"/></marker></defs>';
    for(const [from,to]of [['orders','clean'],['clean','join'],['customers','join'],['join','output'],['join','audit']]){
      const a=wrap.querySelector(`[data-node="${from}"]`).getBoundingClientRect(),b=wrap.querySelector(`[data-node="${to}"]`).getBoundingClientRect();
      const x1=(narrow?a.x+a.width/2:a.right)-box.x,y1=(narrow?a.bottom:a.y+a.height/2)-box.y;
      const x2=(narrow?b.x+b.width/2:b.left)-box.x,y2=(narrow?b.top:b.y+b.height/2)-box.y;
      const path=document.createElementNS('http://www.w3.org/2000/svg','path');
      if(narrow&&from==='clean')path.setAttribute('d',`M ${x1} ${y1} L ${x1} ${y1+10} L 5 ${y1+10} L 5 ${b.y+b.height/2-box.y} L ${b.left-box.x} ${b.y+b.height/2-box.y}`);
      else if(narrow&&from==='join'&&to==='audit')path.setAttribute('d',`M ${a.right-box.x} ${a.y+a.height/2-box.y} L ${box.width-5} ${a.y+a.height/2-box.y} L ${box.width-5} ${b.y+b.height/2-box.y} L ${b.right-box.x} ${b.y+b.height/2-box.y}`);
      else path.setAttribute('d',narrow?`M ${x1} ${y1} C ${x1} ${y1+12},${x2} ${y2-12},${x2} ${y2}`:`M ${x1} ${y1} C ${x1+18} ${y1},${x2-18} ${y2},${x2} ${y2}`);
      path.setAttribute('marker-end','url(#demo-arrow)');svg.append(path);
    }
  }
  function graphCommands(){
    const history=btn('run-history',state.runHistoryOpen?'收起记录':'运行记录');
    if(isRun()){
      if(runStatus()==='prepared')return btn('submit','运行','primary',!canSubmit())+history;
      if(runStatus()==='running')return btn('stop','停止运行','danger',!runtimeCanControl())+history;
      return btn('show-run','查看运行状态')+history;
    }
    if(state.project!=='native')return btn('show-check','项目检查')+history;
    return btn('recheck','检查','',state.configDirty)+btn('prepare','运行','primary',!canPrepare())+history;
  }
  function render(){
    ForgeCaseDemo.unmount();
    ForgeDataDemo.selectView(state.dataOutput);
    picker.value=state.scene;picker.parentElement.hidden=state.panel==='data';
    const work=Object.hasOwn(panels,state.panel),permissions=state.permission==='full'?'提交、控制已开放':state.permission==='control'?'仅控制已有运行':'只读 · 无提交/控制权限';
    app.innerHTML=`${routeError?`<div class="route-error" role="alert">${esc(routeError)}</div>`:''}<div class="shell ${state.nav?'nav-open':''}"><aside class="sidebar" aria-label="项目列表"><div class="brand">DemiFlow</div><div><div class="side-heading">项目</div><div class="projects">${Object.entries(titles).map(([id,name])=>`<button type="button" data-project="${id}" class="${id===state.project?'active':''}" aria-pressed="${id===state.project}"><span class="project-mark">◇</span>${name}</button>`).join('')}</div></div><div class="sidebar-footer">本地工作区<br>讨论草稿 · 测试数据</div></aside><div class="content"><header class="project-header"><div class="project-top">${btn('nav','项目','mobile-nav')}<strong>${titles[state.project]}</strong></div><nav class="project-tabs" aria-label="项目功能">${[['development','开发'],['configure','管道'],['data','数据'],['overview','Readme']].map(([id,name])=>`<button type="button" data-panel="${id}" class="${id===state.panel||id==='configure'&&(work||state.panel==='history')?'active':''}">${name}</button>`).join('')}</nav><span class="project-header-note">通用平台 · 交互样稿</span></header><main class="workspace ${state.panel==='data'?'data-workspace':''}"><h1 class="sr-only">${({development:'开发',overview:'Readme',data:'数据',history:'运行记录'})[state.panel]||'管道'}</h1>${work?`<div class="context-bar"><label class="field">流程<select id="flow"${dis(state.project!=='native'||isRun())}>${state.project==='native'||isRun()?`${Object.entries(flows).map(([id,flow])=>`<option value="${id}"${selectedFlow()===id?' selected':''}>${flow.name} · flows/orders.py:${flow.entry}</option>`).join('')}`:`<option>${state.project==='legacy'?'dispatch.py:run_pipeline':'尚未识别流程'}</option>`}</select></label><label class="field">配置<select id="config"${dis(state.project!=='native'||isRun())}>${state.project==='native'||isRun()?`<option value="small"${selectedConfig()==='small'?' selected':''}>settings/small.json · 小批次</option><option value="full"${selectedConfig()==='full'?' selected':''}>settings/full.json · 完整输入</option>`:`<option>${state.project==='legacy'?'settings/default.json · 原引用，未绑定':'尚无配置'}</option>`}</select></label><div class="evidence"><div class="evidence-heading">${tag(evidenceTitle(),state.evidence==='stale'?'warn':'')}<span class="permission">${state.project==='native'||isRun()?permissions:'运行尚未接通'}</span></div><div class="muted">${isRun()?runtimeIdentity()+' / '+runtimeSeal():state.evidence==='missing'?'测试源码观察':`测试依据 · ${reportId()}`} · 非在线证据</div></div></div><nav class="steps" aria-label="流程工作区">${Object.entries(panels).map(([id,name],i)=>`<button type="button" data-panel="${id}" class="${id===state.panel?'active':''}"${id===state.panel?' aria-current="page"':''}>${name}${id==='check'&&state.evidence==='blocked'&&!isRun()?'<span class="badge-count">1</span>':''}</button>`).join('')}<span class="graph-commands">${graphCommands()}</span></nav>${isRun()||state.panel==='run'?runtimeToolbar():''}<div class="workflow-body">${notice()}${state.panel==='run'?runtimeSummary():''}${workSurface()}</div>`:other()}<p class="footer-note">${state.panel==='data'?(ForgeDataDemo.getView()==='cases'?'Case 视图处理合成数据；配置与标注只保存在本浏览器。Python 内核未接入。':'SQL 仅查询浏览器内测试表；支持单表筛选、分组、聚合、排序与 LIMIT。未连接业务数据或 Python 内核。'):'管道按钮仅演示交互；真实作业提交在正式 API 中接入。'}</p><div class="sr-only" aria-live="polite">${state.project} · ${state.panel} · ${statuses[runStatus()]}</div></main></div></div>`;
    if(state.panel==='data')ForgeDataDemo.mount(state.project);
    requestAnimationFrame(()=>{drawEdges();drawTrend();});
  }
  function seal(){return {sealedFlow:state.flow,sealedConfig:state.config,sealedInput:state.input,sealedTimeout:state.timeout,sealedRounding:state.rounding,sealedOutput:state.output,sealedFixed:state.fixed,sealedRevision:state.revision,sealedSourceAvailable:true};}
  function sourceBinding(){
    const flow=flows[selectedFlow()],node=state.sourceContext==='entry'?'entry':state.node||'clean';
    const fixed=isRun()?runFixed():state.fixed,revision=isRun()?runRevision():state.revision;
    const digits=isRun()?runDigits():state.evidence==='stale'?state.reportRounding:state.savedRounding;
    const model=selectedFlow()==='model'&&node==='clean';
    const call=selectedFlow()==='model'?'reviewed = review_orders(orders)':`cleaned = orders.map(${flow.symbol}, fn_kwargs={"digits": rounding_digits})`;
    const inputName=selectedFlow()==='model'?'reviewed':'cleaned';
    const entry=`def ${flow.entry}(*, orders, customers, rounding_digits, output):
    ${call}
    joined = ${inputName}.join(customers, on="customer_id", how="left")
    joined.write_lance(**output)`;
    const implementations={
      archive:`def normalize_order(row, digits):
    return {**row, "amount": ${fixed?'round(float(row["amount"]), digits)':'str(row["amount"])'}}`,
      summary:'def summarize_order(row, digits):\n    amount = round(float(row["amount"]), digits)\n    return {**row, "amount": amount, "summary": "Order {}: {}".format(row["order_id"], amount)}',
      model:'def review_orders(orders):\n    return orders.map_prompt_async(\n        "review_order",\n        config="prompts/order_review.yaml",\n        inputs={"order": "order_payload"},\n        output="review",\n        concurrency=2, queue_depth=2, max_requests=200,\n    )'
    };
    const calls={entry,clean:call,orders:'orders = data.read_lance(**orders_reference)',customers:'customers = data.read_lance(uri="customers.lance", version=3)',join:`joined = ${inputName}.join(customers, on="customer_id", how="left")`,output:'joined.write_lance(**output)',audit:'summary.write_lance(**audit_output)'};
    const names={entry:flow.name+' · 流程入口',clean:flow.operator,orders:'订单输入',customers:'客户输入',join:'关联客户',output:'订单结果',audit:'审计结果'};
    const path=node==='clean'?flow.path+':7':'flows/orders.py:'+({entry:5,orders:8,customers:9,join:14,output:17,audit:18}[node]||8);
    const configuration=node==='entry'?{source:configPath()+'#/arguments',orders_version:Number(isRun()?runtimeSelected().input:state.savedInput),rounding_digits:digits,output:isRun()?runOutput():state.savedOutput}:node==='customers'?{source:'flows/orders.py:9',uri:'customers.lance',version:3}:node==='join'?{source:'flows/orders.py:14',on:'customer_id',how:'left'}:model?{source:'prompts/order_review.yaml#/prompts/review_order',prompt:'review_order',model:{name:'demo-review-model',api_key_env:'MODEL_API_KEY'},concurrency:2,queue_depth:2,max_requests:200}:node==='orders'?{source:configPath()+'#/arguments/orders',uri:'orders.lance',version:Number(isRun()?runtimeSelected().input:state.evidence==='stale'?state.reportInput:state.savedInput)}:node==='output'?{source:configPath()+'#/arguments/output',relative_uri:isRun()?runOutput():state.savedOutput}:{source:configPath()+'#/arguments/rounding_digits',rounding_digits:digits};
    const snippets={implementation:{path,code:node==='clean'?implementations[selectedFlow()]:calls[node],note:node==='clean'?'选中节点绑定的函数实现。':'内置算子显示已绑定调用片段；平台内部实现可另按声明定位，本例未展开。'},call:{path:`flows/orders.py:${node==='entry'?5:node==='clean'?12:({orders:8,customers:9,join:14,output:17,audit:18}[node]||8)}`,code:calls[node],note:'调用点保留该节点的参数和上下文；复用同一实现的节点仍各自定位。'},configuration:{path:configuration.source,code:JSON.stringify(configuration,null,2),note:'与图依据对应的参数摘要，非完整配置文件。未保存的配置草稿仍在右侧；凭据只显示引用。'}};
    if(model)snippets.prompt={path:'prompts/order_review.yaml#/prompts/review_order/template',code:'检查下列订单信息是否完整，给出结论与理由。\n订单：{{ order | json }}\n返回符合声明 schema 的 JSON。',note:'这是声明中的 Prompt 模板，尚未按某条 Case 渲染；不代表实际请求或模型回答。'};
    const tab=Object.hasOwn(snippets,state.sourceTab)?state.sourceTab:'implementation';
    return {node,model,name:names[node],symbol:node==='clean'?flow.symbol:node==='entry'?flow.entry:node,path,revision,snippets,tab};
  }
  function sourcePanel(){
    const binding=sourceBinding(),snippet=binding.snippets[binding.tab];
    const unavailable=isRun()&&!runSourceAvailable();
    return `<section id="node-source" class="node-source" aria-label="选中节点的源码"><div class="source-heading"><div><span class="source-eyebrow">只读源码 · 合成示例</span><h3>${esc(binding.name)}</h3></div><button type="button" data-action="toggle-source" aria-expanded="${state.sourceOpen}" aria-controls="source-content">${state.sourceOpen?'收起代码':'展开代码'}</button></div>${state.sourceOpen?`<div id="source-content"><div class="source-basis">${isRun()?'运行封存快照':'图绑定快照'} · fixture-r${binding.revision} · ${reportId()}${isRun()?' / '+runtimeIdentity():''}</div>${unavailable?'<p class="source-unavailable" role="status">本次封存记录未提供该源码片段，无法查看。不回退读取当前工作文件。</p>':`<nav class="source-tabs" aria-label="源码类型">${[['implementation','实现'],['call','调用点'],['prompt','Prompt 模板'],['configuration','配置']].filter(([id])=>Object.hasOwn(binding.snippets,id)).map(([id,label])=>`<button type="button" data-source-tab="${id}" aria-pressed="${binding.tab===id}" class="${binding.tab===id?'active':''}">${label}</button>`).join('')}</nav><div class="source-location"><code>${esc(snippet.path)}</code><span>节点 ${binding.node} · ${esc(binding.symbol)}</span></div><pre class="source-text" tabindex="0" aria-label="${esc(binding.name)} ${binding.tab}"><code>${esc(snippet.code)}</code></pre><p class="source-explanation">${esc(snippet.note)}</p>${binding.model?'<p class="source-evidence-note">实际请求 / 回答需要已有 run、attempt 与 Case 证据；本样稿未提供，也不会发起模型调用。</p>':''}${binding.node==='clean'&&!state.fixed&&!isRun()?`<div class="actions">${btn('fix-demo','模拟修正并复查','primary')}<span class="muted small">仅改变样稿状态</span></div>`:''}`}<details class="source-mapping"><summary>查看节点与来源绑定</summary><p>项目：${titles[state.project]} · 流程：${esc(flows[selectedFlow()].entry)} · 节点：${binding.node} · 图依据：${reportId()}<br>实现：${esc(binding.path)} · 符号：${esc(binding.symbol)} · 合成快照：fixture-r${binding.revision}</p><p>本页源码为布局示例。正式版按绑定清单与文件摘要读取；本样稿未接入已有源码读取接口。</p></details></div>`:''}</section>`;
  }
  function openSource(entry=false){
    transition({sourceOpen:true,sourceContext:entry?'entry':'node',sourceTab:entry?'call':'implementation'},true);
    document.getElementById('node-source')?.scrollIntoView({block:'nearest'});
  }
  function drawTrend(){
    const svg=app.querySelector('.trend');if(!svg||svg.getBoundingClientRect().width<20)return;
    const w=Math.floor(svg.getBoundingClientRect().width),h=120,left=40,right=w-12,top=22,bottom=92;
    svg.setAttribute('viewBox',`0 0 ${w} ${h}`);
    const factor=parseFloat(runtimeSelected()?.cpu||'1.24')/1.24;
    const samples=[[0,.3],[20,.7],[40,1.1],[60,1],[70,null],[80,1.2],[100,1.3],[120,1.24]].map(([t,v])=>[t,v===null?null:v*factor]);
    const max=Math.max(...samples.filter(p=>p[1]!==null).map(p=>p[1]))*1.15;
    const line=(x1,y1,x2,y2)=>`<line x1="${x1}" y1="${y1}" x2="${x2}" y2="${y2}"/>`;
    let markup=`<text x="0" y="13">CPU（核）</text>${line(left,bottom,right,bottom)}<text x="0" y="${top+5}">${max.toFixed(1)}</text><text x="18" y="${bottom}">0</text><text x="${left}" y="115">0 秒</text><text x="${right}" y="115" text-anchor="end">120 秒</text>`,segment=[];
    const flush=()=>{if(segment.length)markup+=`<polyline points="${segment.join(' ')}"/>`;segment=[];};
    for(const [t,v]of samples){if(v===null){flush();continue;}segment.push(`${left+(right-left)*t/120},${bottom-(bottom-top)*v/max}`);}flush();
    svg.innerHTML=markup;
  }
  function act(id){
    const handlers={
      'show-configure':()=>transition({panel:'configure',detailScope:'node'}),'show-check':()=>transition({panel:'check',filter:false}),'show-run':()=>enterMonitoring(),'show-results':()=>transition({panel:'results'}),
      related:()=>transition({panel:'check',filter:true}),'all-issues':()=>transition({filter:false}),'locate-node':()=>transition({panel:'configure',node:'clean'}),
      'scope-flow':()=>transition({scope:'flow'}),'scope-project':()=>transition({scope:'project'}),
      'toggle-data-catalog':()=>{transition({dataCatalogCollapsed:!state.dataCatalogCollapsed},true);app.querySelector('[data-action=toggle-data-catalog]')?.focus({preventScroll:true});},
      'demo-data-query':()=>transition({panel:'data',dataOutput:'query'}),
      'demo-data-cases':()=>transition({panel:'data',dataOutput:'cases'}),
      'run-history':()=>state.panel==='run'?transition({runHistoryOpen:!state.runHistoryOpen}):enterMonitoring({runHistoryOpen:true}),
      'close-run-history':()=>transition({runHistoryOpen:false},true),'run-page-prev':()=>transition({runHistoryPage:Math.max(0,state.runHistoryPage-1)},true),'run-page-next':()=>transition({runHistoryPage:Math.min(1,state.runHistoryPage+1)},true),
      'run-log-all':()=>transition({runLogScope:'run'},true),'run-log-node':()=>transition({runLogScope:'node'},true),'run-log-more':()=>transition({runLogLimit:Math.min(18,state.runLogLimit+6)},true),
      'graph-config':()=>transition({panel:'configure',detailScope:'graph'}),
      'source-entry':()=>openSource(true),
      'save-config':()=>{if(canSubmit()&&state.configDirty)transition({savedInput:state.input,savedRounding:state.rounding,savedOutput:state.output,configDirty:false,evidence:'stale',cause:'config'});},
      source:()=>openSource(),'toggle-source':()=>transition({sourceOpen:!state.sourceOpen},true),
      'fix-demo':()=>{transition({fixed:true,evidence:'checked',filter:false,panel:'check',reportInput:state.savedInput,reportRounding:state.savedRounding,revision:state.revision+1});},
      recheck:()=>{if(!state.configDirty)transition({evidence:state.evidence==='blocked'&&!state.fixed?'blocked':'checked',panel:'check',filter:false,reportInput:state.savedInput,reportRounding:state.savedRounding,revision:state.revision+1});},
      prepare:()=>{if(canPrepare()){const updates={...state.runtimeUpdates};delete updates[state.project+'/run-demo-042/1'];transition({...seal(),run:'running',runId:'run-demo-042',runAttempt:1,runtimeUpdates:updates,mode:'run',panel:'run',dirty:false,sourceOpen:false,runLogScope:'run',runLogLimit:6});}},
      submit:()=>{if(canSubmit()&&runStatus()==='prepared')changeRuntimeStatus('running');},
      stop:()=>{if(runtimeCanControl()&&runStatus()==='running')changeRuntimeStatus('stopping');},
      'confirm-stop':()=>{if(runtimeCanControl()&&['stopping','unconfirmed'].includes(runStatus()))changeRuntimeStatus('cancelled');},finish:()=>{if(runtimeCanControl()&&runStatus()==='running')changeRuntimeStatus('succeeded');},
      recover:()=>{const updates={...state.runtimeUpdates};delete updates[state.project+'/run-demo-042/1'];transition({...seal(),run:'prepared',runId:'run-demo-042',runAttempt:1,runtimeUpdates:updates,mode:'run'});},
      current:()=>transition({mode:'current',panel:'configure',detailScope:'node'}),'open-run':()=>enterMonitoring(),
      'view-data':()=>transition({panel:'data'}),nav:()=>transition({nav:!state.nav},true)
    };
    handlers[id]?.();
  }
  document.addEventListener('click',e=>{
    const b=e.target.closest('button');if(!b||b.disabled)return;
    if(b.dataset.action){act(b.dataset.action);return;}
    if(b.dataset.runRecord){selectRuntime(b.dataset.runRecord);return;}
    if(b.dataset.sourceTab){transition({sourceTab:b.dataset.sourceTab},true);return;}
    if(b.dataset.node){transition({node:b.dataset.node,detailScope:'node',sourceContext:'node',sourceOpen:state.panel==='run'?state.sourceOpen:true,sourceTab:'implementation',filter:state.panel==='check'},true);return;}
    if(b.dataset.panel){if(b.dataset.panel==='run')enterMonitoring();else transition({panel:b.dataset.panel});return;}
    if(b.dataset.project){const previousPanel=state.panel;const scene={native:'blocked',legacy:'legacy',empty:'empty'}[b.dataset.project];state={...base,...scenes[scene][1],scene};if(['data','overview','development'].includes(previousPanel))state.panel=previousPanel;persist();render();}
  });
  picker.addEventListener('change',()=>{state={...base,...scenes[picker.value][1],scene:picker.value};routeError='';persist();render();});
  document.addEventListener('change',e=>{
    if(e.target.id==='runtime-run'){selectRuntime(e.target.value);return;}
    if(e.target.id==='runtime-attempt'){selectRuntime(runtimeSelected()?.id,Number(e.target.value));return;}
    if(e.target.id==='flow')transition({flow:e.target.value,evidence:'missing',fixed:true,node:'clean',dirty:false,sourceContext:'node',sourceTab:'implementation',sourceOpen:true});
    if(e.target.id==='config')transition({config:e.target.value,evidence:'missing',fixed:true,dirty:false});
    if(e.target.id==='input-version')transition({input:e.target.value,configDirty:true});
    if(e.target.id==='rounding'){if(e.target.checkValidity())transition({rounding:Number(e.target.value),configDirty:true});else e.target.reportValidity();}
    if(e.target.id==='output-path'){if(e.target.checkValidity())transition({output:e.target.value,configDirty:true});else e.target.reportValidity();}
    if(e.target.id==='timeout'){if(e.target.checkValidity())transition({timeout:Number(e.target.value),dirty:true});else e.target.reportValidity();}
  });
  addEventListener('popstate',()=>{state=readRoute();render();});
  addEventListener('resize',()=>{drawEdges();drawTrend();});
  document.addEventListener('toggle',drawTrend,true);
  for(const [key,[name]]of Object.entries(scenes)){const o=document.createElement('option');o.value=key;o.textContent=name;picker.append(o);}
  state=readRoute();render();
})();
