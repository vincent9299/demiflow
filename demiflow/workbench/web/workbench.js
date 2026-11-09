/* Project pipeline workbench. All facts come from native graph/run APIs. */
(() => {
  'use strict';
  const $=(tag,text,cls)=>{const n=document.createElement(tag);if(text!==undefined)n.textContent=text;if(cls)n.className=cls;return n;};
  const labels={pending:'已登记 · 未开始',prepared:'已准备',starting:'正在启动',running:'运行中',stopping:'正在停止',succeeded:'成功',failed:'失败',cancelled:'已取消',interrupted:'中断'};
  const active=status=>['starting','running','stopping'].includes(status);
  const json=value=>JSON.stringify(value,null,2);
  const safe=value=>value==null?'未测量':String(value);
  const unit=value=>value==null?'未测量':(value/1024**2).toFixed(1)+' MiB';
  const timeText=value=>{const d=new Date(value*1000),p=n=>String(n).padStart(2,'0');return d.getFullYear()+'-'+p(d.getMonth()+1)+'-'+p(d.getDate())+' '+p(d.getHours())+':'+p(d.getMinutes());};
  const inputText=field=>field.value_type==='str'?String(field.value):json(field.value);
  async function mount(options) {
    const {main,api,workspaceId:project,view={},mountText,disposeReaders,onContext=()=>{},rememberView=()=>{},
      openResult=async()=>{},navigate=async()=>{},projectSettings={}}=options;
    let ctx=options.context;
    const state=ForgeWorkbenchState.create(project),commands=ForgeWorkbenchCommands.create({project,api});
    const root=$('section',undefined,'workbench');main.append(root);
    let controller=new AbortController(),disposed=false,run=null,snapshot=null,working=null,lastRun=null;
    let busy=false,historyOpen=false,sourceKind='implementation',detailKind='logs',sourceSequence=0,nodeSequence=0;
    let logOffset=0,logBody='',eventCursor=null,eventRows=[],lastMetricAt=0,lastEventAt=0;
    let records=null,cursors=[null],page=0,listSequence=0,sourceOpen=true,comparison=null,outputFingerprint=null,runFilter='';
    let observationCursor=null,observationPages=[],graphSequence=0,scopeNode=null,nodeOffset=0,inspectorWhole=false;
    let graphKind='declared',logicalAction=null,logicalSelected=null;
    let settingsBasis=null,issueScope='flow',scopedCheckButton=null;
    const aliasDrafts=new Map();
    const data=()=>state.current();
    const current=t=>!disposed&&root.isConnected&&state.valid(t);
    const request=(name,query={},body)=>api(name,query,body,project,body?{}:{signal:controller.signal});
    const can=key=>!!(ctx.run_permissions?.[key]??(key==='run_submit'&&ctx.runs_enabled));
    const configWrite=()=>!!ctx.capabilities?.config_write;
    const draftKey=()=>project+'|'+(working?.entry||'')+'|'+(working?.params||'');
    const getDraft=()=>ForgeWorkbenchState.drafts.get(draftKey())||{values:{},inputs:{},timeout:String(ctx.run_limits.timeout_s)};
    function storeDraft(value){ForgeWorkbenchState.drafts.put(draftKey(),value);updateActions();}
    function message(text,tone='notice',ticket=state.capture()) {if(!current(ticket))return;notice.textContent=text;notice.className='wb-notice '+tone;notice.hidden=!text;delete notice.dataset.observationError;}
    function button(label,action,parent,{disabled=false,cls=''}={}) {
      const b=$('button',label,cls);b.type='button';b.disabled=disabled;
      b.onclick=async()=>{const ticket=state.capture();if(disposed||!b.isConnected||b.disabled)return;b.disabled=true;b.dataset.pending='true';try{await action();}catch(error){if(error.name!=='AbortError'&&current(ticket))message(error.message,'error-box',ticket);}finally{delete b.dataset.pending;if(b.isConnected){b.disabled=disabled;updateActions();}}};
      parent?.append(b);return b;
    }
    function select(label,items,value) {
      const node=$('select');node.setAttribute('aria-label',label);
      for(const item of items){const option=$('option',item.label??item.value);option.value=item.value;node.append(option);}
      if(value!=null)node.value=String(value);return node;
    }
    function field(label,input,parent){const n=$('label',label);n.append(input);parent.append(n);return n;}
    function details(label,parent){const n=$('details');n.append($('summary',label));parent.append(n);return n;}
    function raw(label,value,parent){const n=details(label,parent);n.append($('pre',json(value),'wb-code'));return n;}
    const toolbar=$('div',undefined,'wb-toolbar'),selectors=$('div',undefined,'wb-selectors'),actions=$('div',undefined,'wb-actions');
    const entryChoice=select('流程',[],null),paramsChoice=select('配置',[],null);
    field('流程',entryChoice,selectors);field('配置',paramsChoice,selectors);toolbar.append(selectors,actions);root.append(toolbar);
    const checkButton=button('检查',()=>check(),actions),runButton=button('运行',()=>submit(),actions,{cls:'primary'});
    const historyButton=button('运行记录',async()=>{historyOpen=!historyOpen;historyPanel.hidden=!historyOpen;historyButton.setAttribute('aria-expanded',String(historyOpen));if(historyOpen)await listRuns();},actions);
    historyButton.setAttribute('aria-expanded','false');
    button('接入与权限',()=>{settings.hidden=!settings.hidden;paintSettings();},actions,{cls:'quiet'});
    const notice=$('div',undefined,'wb-notice');notice.hidden=true;notice.setAttribute('role','status');root.append(notice);
    const accessState=$('p',undefined,'wb-access wb-caption');accessState.hidden=true;root.append(accessState);
    const settings=$('div',undefined,'wb-settings');settings.hidden=true;root.append(settings);
    const historyPanel=$('div',undefined,'wb-history');historyPanel.hidden=true;root.append(historyPanel);
    const navigation=$('div',undefined,'wb-navigation'),modes=new Map();
    for(const[key,label]of [['config','读图与配置'],['checks','检查问题'],['monitor','运行监控'],['results','输出结果']])modes.set(key,button(label,()=>changeMode(key),navigation));
    const basisLabel=$('span',undefined,'wb-basis');toolbar.append(basisLabel);navigation.append(actions);root.insertBefore(navigation,notice);
    const runBar=$('div',undefined,'wb-run-bar');runBar.hidden=true;root.append(runBar);
    const runFacts=$('div',undefined,'wb-run-facts');runFacts.hidden=true;root.append(runFacts);
    const body=$('div',undefined,'wb-body'),graphSection=$('div',undefined,'wb-graph-section'),inspector=$('aside',undefined,'wb-inspector');
    const checks=$('section',undefined,'wb-checks'),checkScopes=$('div',undefined,'wb-check-scopes'),checkGrid=$('div',undefined,'wb-check-grid'),checkIssues=$('div',undefined,'wb-check-issues'),checkCoverage=$('aside',undefined,'wb-check-coverage');checks.hidden=true;checkGrid.append(checkIssues,checkCoverage);checks.append(checkScopes,checkGrid);root.append(checks);
    const graphTools=$('div',undefined,'wb-graph-tools'),graphTitle=$('span','数据算子图'),graphHost=$('div',undefined,'wb-graph');
    graphTools.append(graphTitle);const tools=$('div');graphTools.append(tools);
    const wholeButton=button('整图配置',()=>wholeConfiguration(),tools);
    const graphView=ForgeWorkbenchGraph.mount(graphHost,{onSelect:id=>chooseNode(id)});
    const logicalView=ForgeWorkbenchGraph.mountLogical(graphHost,{onSelect:(id,declared)=>chooseLogicalNode(id,declared)});
    const logicalToggle=button('执行逻辑图（E2）',()=>toggleGraphKind(),tools);
    logicalToggle.setAttribute('aria-pressed','false');
    const activeGraphView=()=>graphKind==='logical'?logicalView:graphView;
    button('−',()=>activeGraphView().zoom(.8),tools).setAttribute('aria-label','缩小算子图');
    button('+',()=>activeGraphView().zoom(1.25),tools).setAttribute('aria-label','放大算子图');
    button('适应',()=>activeGraphView().fit(),tools).setAttribute('aria-label','适应算子图');
    const graphLegend=$('p','实线：数据流　虚线：值依赖 / 动作先后　点线：未核验的源码引用','wb-caption');
    const logicalBar=$('div',undefined,'wb-logical-bar');logicalBar.hidden=true;
    const locator=details('定位节点 / 查看局部',graphSection),locatorBody=$('div',undefined,'wb-inline-actions');locator.append(locatorBody);locator.hidden=true;
    graphSection.prepend(graphTools,logicalBar,graphHost,graphLegend);body.append(graphSection,inspector);root.append(body);
    const runDetail=$('section',undefined,'wb-run-detail'),detailHeader=$('div',undefined,'wb-source-header'),detailTabs=$('div',undefined,'wb-source-tabs'),runDetailBody=$('div',undefined,'wb-run-detail-body');runDetail.hidden=true;
    const detailButtons=new Map();
    detailHeader.append($('strong','日志与观测'),detailTabs);runDetail.append(detailHeader,runDetailBody);graphSection.append(runDetail);
    for(const[key,label]of [['logs','运行日志'],['observations','节点证据'],['events','控制事件']])detailButtons.set(key,button(label,async()=>{detailKind=key;await loadRunDetail(true);updateActions();},detailTabs));
    const bottom=$('section',undefined,'wb-source'),sourceHeader=$('div',undefined,'wb-source-header'),sourceTitle=$('strong','节点源码'),sourceTabs=$('div',undefined,'wb-source-tabs'),sourceBody=$('div',undefined,'wb-source-body');
    const sourceButtons=new Map();
    for(const[key,label]of [['implementation','实现'],['callsite','调用点'],['prompt','Prompt'],['configuration','配置']])sourceButtons.set(key,button(label,async()=>{sourceKind=key;sourceOpen=true;sourceBody.hidden=false;sourceTabs.hidden=false;await loadSource();},sourceTabs));
    sourceHeader.append(sourceTitle);const sourceToggle=button('收起代码',async()=>{sourceOpen=!sourceOpen;sourceBody.hidden=sourceTabs.hidden=!sourceOpen;sourceSequence++;disposeReaders(sourceBody);sourceBody.replaceChildren();if(sourceOpen)await loadSource();updateActions();},sourceHeader);bottom.append(sourceHeader,sourceTabs,sourceBody);graphSection.append(bottom);
    const results=$('section',undefined,'wb-results');results.hidden=true;root.append(results);
    const documents=details('当前项目检查与来源',root);
    let projectInfoRendered=false,projectInfoPending=null,projectInfoDirty=false;
    const checkState=()=>options.projectCheckState?.()||{version:0,active:false};
    const projectInfo=$('div');documents.append(projectInfo);
    async function refreshProjectInfo(){
      projectInfoDirty=true;
      if(projectInfoPending||checkState().active)return projectInfoPending;
      projectInfoDirty=false;const version=checkState().version;
      const container=$('div');projectInfoRendered=true;container.hidden=true;projectInfo.append(container);
      projectInfoPending=Promise.resolve(options.renderProjectInfo?.(container)).then(()=>{
        if(!disposed&&root.isConnected&&version===checkState().version&&!checkState().active&&!projectInfoDirty){for(const old of projectInfo.children)if(old!==container)disposeReaders(old);projectInfo.replaceChildren(container);container.hidden=false;}
        else{disposeReaders(container);container.remove();projectInfoRendered=false;projectInfoDirty=true;}
      }).catch(error=>{
        disposeReaders(container);container.remove();projectInfoRendered=false;
        const superseded=version!==checkState().version||checkState().active;
        if(superseded)projectInfoDirty=true;
        else{projectInfoDirty=false;if(!disposed&&root.isConnected&&error.name!=='AbortError')message(error.message,'error-box');}
      }).finally(()=>{
        projectInfoPending=null;
        if(!disposed&&documents.open&&projectInfoDirty&&!checkState().active)return refreshProjectInfo();
      });
      return projectInfoPending;
    }
    documents.addEventListener('toggle',()=>{if(documents.open&&!projectInfoRendered)refreshProjectInfo();});
    button('在开发中查看与修正',()=>navigate('development'),documents);
    function graphLoading(loading){graphHost.inert=inspector.inert=checks.inert=loading;if(loading){graphHost.setAttribute('aria-busy','true');basisLabel.textContent='正在读取图…';disposeReaders(sourceBody);sourceBody.replaceChildren($('p','正在读取绑定来源…','wb-caption'));}else graphHost.removeAttribute('aria-busy');updateActions();}
    function transition(patch){notice.hidden=true;notice.textContent='';delete notice.dataset.observationError;observer.stop();controller.abort();controller=new AbortController();snapshot=null;sourceSequence++;nodeSequence++;graphSequence++;scopeNode=null;nodeOffset=0;logicalAction=null;logicalSelected=null;logOffset=0;logBody='';eventCursor=null;eventRows=[];lastMetricAt=lastEventAt=0;observationCursor=null;observationPages=[];lastObservationRevision=null;trend.hidden=true;trendPlot.replaceChildren();runFacts.hidden=results.hidden=runDetail.hidden=true;runFacts.replaceChildren();results.replaceChildren();runDetailBody.replaceChildren();outputFingerprint=null;graphLoading(true);const ticket=state.transition(patch);markHistorySelection();updateActions();return ticket;}
    function registered(){return !!working&&ctx.flows.some(f=>f.entry===working.entry&&(f.parameters||[]).includes(working.params));}
    function choices(){
      const entries=new Map();
      for(const e of ctx.project?.entries||[])entries.set(e.entry,{value:e.entry,label:e.entry+(e.protocol==='native_flow'?'':' · 原入口')});
      for(const e of ctx.flows)entries.set(e.entry,{value:e.entry,label:e.entry});
      if(run?.entry&&!entries.has(run.entry))entries.set(run.entry,{value:run.entry,label:run.entry+' · 封存入口'});
      return [...entries.values()];
    }
    function paintSelectors(){
      const selectedEntry=run?.entry||working?.entry,selectedParams=run?.parameter_path||working?.params;
      entryChoice.replaceChildren();for(const item of choices()){const option=$('option',item.label);option.value=item.value;entryChoice.append(option);}entryChoice.value=selectedEntry||'';
      entryChoice.disabled=!!run||!entryChoice.options.length;
      const configs=ctx.flows.find(f=>f.entry===selectedEntry)?.parameters||[];
      paramsChoice.replaceChildren();for(const path of [...new Set([...configs,...(selectedParams?[selectedParams]:[])])]){const option=$('option',path);option.value=path;paramsChoice.append(option);}
      if(!paramsChoice.options.length){const option=$('option',run?'封存运行使用内联参数':'尚无原生参数绑定');option.value='';paramsChoice.append(option);}
      paramsChoice.value=selectedParams||'';paramsChoice.disabled=!!run||!configs.length;
      updateActions();
    }
    function updateActions(){
      if(!root.isConnected)return;
      const resultsActive=data().mode==='results',checksActive=data().mode==='checks';
      body.hidden=resultsActive||checksActive;checks.hidden=!checksActive;runDetail.hidden=!run||resultsActive;runFacts.hidden=!run||resultsActive;trend.hidden=!run||resultsActive;
      runBar.hidden=!run||!!comparison;
      if(!checksActive&&documents.parentElement===checkIssues){documents.open=false;root.append(documents);}
      const dirty=working&&Object.keys(getDraft().values).length>0;
      checkButton.disabled=busy||!!run||!registered()||dirty;
      if(scopedCheckButton?.isConnected)scopedCheckButton.disabled=checkButton.disabled;
      runButton.disabled=busy||!!run||!registered()||!can('run_submit')||dirty||!data().graph?.parameters.available||data().graph?.freshness.current===false;
      runButton.title=run?'返回当前配置后提交新运行':dirty?'先保存当前配置草稿':!can('run_submit')?'项目或服务尚未开放提交权限':!registered()?'先登记原生流程与参数文件':!data().graph?'正在读取图与配置':data().graph.freshness.current===false?'检查依据已变化，请先读取当前配置或重新检查':!data().graph.parameters.available?'登记配置暂不可用':'提交新运行；检查、封存并启动';
      wholeButton.disabled=!data().graph;
      logicalToggle.disabled=!data().graph||graphHost.hasAttribute('aria-busy');
      accessState.hidden=!!run||registered()&&can('run_submit');
      accessState.textContent=!working?.entry?'尚未发现可用入口。可在开发中编写流程，再通过“接入与权限”登记。':!registered()?'已发现原项目入口，尚未绑定可执行的原生流程与参数；当前仅观察源码，接入不会自动授予运行权限。':'原生流程已登记；当前可阅读与检查，项目或服务尚未开放提交权限。';
      for(const[key,b]of modes){b.classList.toggle('active',data().mode===key);b.setAttribute('aria-pressed',String(data().mode===key));}
      for(const[key,b]of sourceButtons)b.classList.toggle('active',sourceKind===key);
      for(const[key,b]of detailButtons){b.classList.toggle('active',detailKind===key);b.disabled=!run;}
      sourceToggle.textContent=sourceOpen?'收起代码':'展开代码';sourceToggle.setAttribute('aria-expanded',String(sourceOpen));
      guardSettings();
    }
    entryChoice.onchange=async()=>{if(disposed||!entryChoice.isConnected)return;const entry=entryChoice.value,params=(ctx.flows.find(f=>f.entry===entry)?.parameters||[])[0]||null;await useWorking(entry,params);};
    paramsChoice.onchange=async()=>{if(!disposed&&paramsChoice.isConnected)await useWorking(entryChoice.value,paramsChoice.value);};
    function diagnosticTarget(d){return (d.related_nodes||[])[0]||data().graph?.nodes.find(n=>n.source===d.path&&n.line===d.line)?.id;}
    function renderDiagnostics(items,parent,onLocate=null){
      if(!items.length){parent.append($('p','此范围没有诊断；是否通过仍以检查状态和覆盖为准。','wb-caption'));return;}
      for(const d of items.slice(0,256)){
        const item=$('div',undefined,'wb-issue'),id=diagnosticTarget(d);
        item.append($('strong',(d.rule||d.code||'诊断')+' · '+({source:'源码或配置',resource:'资源限制',platform:'平台问题',unsupported:'能力未支持',unknown:'原因待核对'}[d.cause||'unknown']||'原因待核对')),
          $('p',d.detail||d.message||''),$('small',(d.path||'整图')+(d.line?':'+d.line:'')+(id?'':' · 未绑定到可见节点')));
        if(id)button('定位节点',()=>onLocate?onLocate(id):data().graph.nodes.some(n=>n.id===id)?chooseNode(id):locateNode(id),item);parent.append(item);
      }
    }
    async function fetchGraph(ticket,{preserveSelection=false,refresh=false}={}){
      const selected=data(),query=selected.run?{basis_kind:'run',run_id:selected.run,attempt:selected.attempt}:selected.report?{basis_kind:'report',report_digest:selected.report}:{basis_kind:'source',entry:selected.entry,...(selected.params?{params:selected.params}:{})};
      const sequence=++graphSequence;if(scopeNode)query.node_id=scopeNode;if(nodeOffset)query.node_offset=nodeOffset;
      const value=await request('graph',query);if(!current(ticket)||sequence!==graphSequence)return;
      if(refresh&&Object.keys(getDraft().values).length){message('项目内容已变化；配置草稿保留，请核对后再保存。');return;}
      const previous=preserveSelection?data().node:null,keepWhole=preserveSelection&&inspectorWhole;
      if(!state.acceptGraph(ticket,value))return;
      const selectedNode=value.nodes.some(n=>n.id===previous)?previous:value.nodes.some(n=>n.id===scopeNode)?scopeNode:value.nodes[0]?.id;
      if(value.binding.basis_kind==='report')working={entry:value.binding.entry,params:value.binding.params};
      graphTitle.textContent='数据算子图 · '+value.completeness.total_nodes+' 节点 / '+value.completeness.total_edges+' 条边';
      const basis=value.binding.basis_kind;
      basisLabel.textContent=(basis==='run'?'封存运行图':basis==='report'?'检查报告图 · '+value.status:'源码观察图 · 尚未检查')+(scopeNode?' · 节点邻域':value.completeness.display==='clipped'?' · 显示已裁剪':'');
      graphView.update(value,snapshot||run,selectedNode);paintSelectors();graphLoading(false);paintRunSeal();
      if(graphKind==='logical')renderLogical(value);
      paintLocator(value);
      if(!value.nodes.length){
        paintConfiguration(null);
        if(!['logs','events','observations'].includes(sourceKind)){
          disposeReaders(sourceBody);sourceBody.replaceChildren($('p','本图没有可展示的调用节点。请核对右侧诊断或“检查问题”；没有节点不代表检查通过。','wb-caption'));
        }
      }
      else chooseNode(selectedNode);
      if(keepWhole)paintConfiguration(null);
      if(data().mode==='checks')paintIssues();
      updateActions();
      if(value.freshness.current===false){message('检查依据已变化，请重新检查。 '+value.freshness.changes.join('；'),'notice');button('读取当前配置',()=>useWorking(value.binding.entry,value.binding.params),notice);}
      else if(value.completeness.display==='clipped')message('当前图只显示部分节点或边，可定位节点查看局部；完整检查以报告为准。');
    }
    async function locateNode(id,offset=nodeOffset){
      const ticket=state.capture();scopeNode=id;nodeOffset=offset;graphLoading(true);
      try{await fetchGraph(ticket);}finally{if(current(ticket))graphLoading(false);}
    }
    function paintLocator(graph){
      locator.hidden=graph.completeness.total_nodes<=128&&!scopeNode;locatorBody.replaceChildren();
      const index=graph.node_index;if(!index)return;
      const choice=select('定位图节点',index.nodes.map(n=>({value:n.id,label:n.label+' · '+n.source+':'+n.line})),scopeNode);
      choice.onchange=()=>{if(!disposed&&choice.isConnected)locateNode(choice.value).catch(error=>message(error.message,'error-box'));};locatorBody.append(choice);
      button('定位',()=>locateNode(choice.value),locatorBody,{disabled:!index.nodes.length});
      button('上一组节点',()=>locateNode(scopeNode,Math.max(0,index.offset-64)),locatorBody,{disabled:index.offset===0});
      button('下一组节点',()=>locateNode(scopeNode,index.next_offset),locatorBody,{disabled:index.next_offset==null});
      button('返回全图范围',()=>locateNode(null,0),locatorBody,{disabled:!scopeNode});
      locatorBody.append($('span','已索引 '+index.total+' 个节点 · 当前显示 '+graph.nodes.length+' 个；局部视图仍绑定同一图依据。','wb-caption'));
    }
    function logicalEntry(){
      const view=data().graph?.logical;
      if(!view?.available||!view.actions.length)return null;
      return view.actions.find(a=>a.action_id===logicalAction)||view.actions[0];
    }
    function toggleGraphKind(){
      graphKind=graphKind==='logical'?'declared':'logical';
      logicalToggle.setAttribute('aria-pressed',String(graphKind==='logical'));
      logicalToggle.classList.toggle('active',graphKind==='logical');
      logicalToggle.textContent=graphKind==='logical'?'返回数据算子图':'执行逻辑图（E2）';
      const g=data().graph;if(!g){updateActions();return;}
      if(graphKind==='logical')renderLogical(g);
      else{logicalBar.hidden=true;graphLegend.textContent='实线：数据流　虚线：值依赖 / 动作先后　点线：未核验的源码引用';
        graphTitle.textContent='数据算子图 · '+g.completeness.total_nodes+' 节点 / '+g.completeness.total_edges+' 条边';
        graphView.update(g,snapshot||run,data().node);}
      updateActions();
    }
    function logicalPanel(title,detail,extra){
      const box=$('div',undefined,'wb-logical-panel');
      box.append($('strong',title),$('p',detail));
      if(extra)box.append(extra);
      graphHost.replaceChildren(box);
    }
    function renderLogical(graph){
      const view=graph.logical;
      logicalBar.hidden=true;logicalBar.replaceChildren();
      graphLegend.textContent='实线：逻辑数据流（标注端口）　虚线框：共享祖先 / 复用　×N：同一逻辑节点多次出现';
      if(!view){graphTitle.textContent='执行逻辑图（E2）· 无投影';logicalPanel('执行逻辑图不可用','当前图响应没有逻辑投影字段。');return;}
      if(!view.available){
        graphTitle.textContent='执行逻辑图（E2）· '+(view.semantic_unchecked?'语义未检查':'不可用');
        logicalPanel(view.semantic_unchecked?'语义未检查':'执行逻辑图不可用',view.detail||view.reason||'',
          view.semantic_unchecked?$('p','对当前流程运行“检查”生成报告后，可在此查看按 action 投影的执行逻辑图。','wb-caption'):null);
        return;
      }
      const entry=logicalEntry();
      if(!entry){graphTitle.textContent='执行逻辑图（E2）· 无 action';logicalPanel('没有可投影的 action','报告声明了 action 列表，但当前没有可展示的条目。');return;}
      logicalAction=entry.action_id;
      logicalBar.hidden=false;
      if(view.actions.length>1||view.truncated){
        const choice=select('逻辑图 action',view.actions.map(a=>({value:a.action_id,label:(a.action_id||'无效 action').slice(-12)+' · '+(a.status==='projected'?'已投影':'投影失败')})),entry.action_id);
        choice.onchange=()=>{if(!disposed&&choice.isConnected){logicalAction=choice.value;logicalSelected=null;renderLogical(data().graph);}};
        field('action',choice,logicalBar);
      }
      logicalBar.append($('span','共 '+view.action_count+' 个 action'+(view.truncated?' · 显示已截断':''),'wb-caption'));
      if(entry.status!=='projected'){
        graphTitle.textContent='执行逻辑图（E2）· 投影失败';
        const error=entry.error||{};
        const box=$('div',undefined,'wb-logical-panel');
        box.append($('strong','此 action 的逻辑投影失败'),$('p','其它 action 的投影不受影响。','wb-caption'),
          $('p','code：'+(error.code||'unknown')),$('p','reason：'+(error.reason||'未提供')),$('p','retriable：'+(error.retriable?'true':'false')));
        graphHost.replaceChildren(box);return;
      }
      const dag=entry.dag;
      graphTitle.textContent='执行逻辑图（E2）· '+dag.nodes.length+' 逻辑节点 / '+dag.edges.length+' 条边';
      logicalBar.append($('span','契约校验通过 · '+dag.schema_version,'wb-tag'),$('span','dag_id '+dag.dag_id.slice(0,16),'wb-caption'));
      const labels={};for(const n of graph.nodes)labels[n.id]=n.operator?.name||n.api.split(/[.:]/).at(-1)||n.kind;
      logicalView.update(dag,entry.node_map,labels,logicalSelected);
    }
    function chooseLogicalNode(id,declared){
      const entry=logicalEntry();
      const group=entry?.status==='projected'?entry.dag.nodes.filter(n=>n.logical_node_id===id):[];
      if(declared&&!data().graph.nodes.some(n=>n.id===declared)){
        locateNode(declared).catch(error=>{if(error.name!=='AbortError')message(error.message,'error-box');});
        return;
      }
      if(declared)chooseNode(declared);
      logicalSelected=id;logicalView.select(id);
      if(!group.length)return;
      const node=group[0],box=details('逻辑节点（E2）· '+id.slice(0,12),inspector);box.open=true;
      const facts=$('dl',undefined,'details');
      for(const[label,value]of[['op_kind',node.op_kind],['输入端口',node.input_ports.join(', ')||'无'],['输出端口',node.output_ports.join(', ')||'无'],['出现次数',String(group.length)],['operator_spec_ref',node.operator_spec_ref||'未绑定（只读投影）'],['声明节点',declared||'未映射']])facts.append($('dt',label),$('dd',value));
      box.append(facts);
      if(entry.dag.shared_ancestors.some(s=>s.logical_node_id===id))box.append($('p','共享祖先 / 复用：该逻辑节点被多个消费方共享。','wb-caption'));
      raw('traits',node.traits,box);raw('occurrence_id',group.map(n=>n.occurrence_id),box);
      inspector.prepend(box);
    }
    async function useWorking(entry,params,report=null,mode='config'){
      if(disposed)throw new DOMException('工作台已关闭','AbortError');
      sourceOpen=true;sourceBody.hidden=sourceTabs.hidden=false;
      run=null;comparison=null;working={entry,params};const ticket=transition({entry,params,report,run:null,attempt:null,mode});
      runBar.hidden=runFacts.hidden=results.hidden=true;results.replaceChildren();paintSelectors();
      try{if(entry){await fetchGraph(ticket);if(current(ticket))rememberView(report?{kind:'inspection',report_digest:report}:{kind:'flows',entry,...(params?{params}:{})});}
        else{basisLabel.textContent='尚无已声明入口';graphHost.replaceChildren($('p','尚无已声明入口。可在开发中编写流程，再在“接入与权限”登记。'));inspector.replaceChildren($('p','可查看项目说明、数据和检查结果。'));sourceBody.replaceChildren($('p','发现或登记入口后，可在这里阅读选中节点的源码。','wb-caption'));settings.hidden=false;paintSettings();}}
      catch(error){if(current(ticket)&&error.name!=='AbortError')message(error.message,'error-box',ticket);}
      finally{if(current(ticket))graphLoading(false);}
      paintPending();
    }
    async function check(){
      if(busy)return;if(run||!registered())throw new Error('请返回已登记的当前流程后检查。');if(Object.keys(getDraft().values).length)throw new Error('请先保存当前配置修改，再检查。');
      const ticket=state.capture(),body={entry:working.entry,params:working.params};busy=true;updateActions();message('正在核对源码、参数、固定输入与平台契约…');
      try{const value=await request('inspect',{},body);if(!current(ticket))return;
        if(value.digest)await useWorking(body.entry,body.params,value.digest,'checks');
        else{const host=data().mode==='checks'?checkIssues:inspector;host.replaceChildren($('h3','检查未能生成报告'));renderDiagnostics(value.diagnostics||[],host);message('检查未能生成有效报告，请处理诊断。','error-box');}
      }finally{busy=false;updateActions();}
    }
    function paintIssues(){
      const g=data().graph;if(documents.parentElement===checkIssues)root.append(documents);
      scopedCheckButton=null;
      checkScopes.replaceChildren($('span','检查范围','wb-caption'));checkIssues.replaceChildren();checkCoverage.replaceChildren($('h3','本次检查依据'));
      for(const[key,label]of [['flow','当前流程'],['project','整个项目']]){const item=button(label,()=>{issueScope=key;paintIssues();},checkScopes);item.classList.toggle('active',issueScope===key);item.setAttribute('aria-pressed',String(issueScope===key));}
      if(issueScope==='project'){
        checkIssues.append(documents);documents.open=true;refreshProjectInfo();
        checkCoverage.append($('p','沿用项目已有的检查结果与覆盖；不会把单个流程的通过结论扩大为整个项目通过。','wb-caption'));return;
      }
      const checked=g?.binding.basis_kind==='report',passed=checked&&g.status==='checked',draftDirty=working&&Object.keys(getDraft().values).length>0;
      checkIssues.append($('h3',passed?(draftDirty?'已保存配置的接线检查通过':'当前配置的接线检查通过'):checked?'已保存配置的检查问题':'当前配置尚未检查'));
      checkIssues.append($('p',draftDirty?'当前有未保存修改，这里显示已保存配置的检查结果；保存修改后再检查。':passed?'已核对本次入口、配置及固定输入；行为验证和业务质量按各自已有证据判断。':checked?'按问题定位代码、配置或数据；修改后重新检查当前配置。':'读取算子图仅用于理解源码关系，检查后才能得到当前配置的结论。','wb-caption'));
      if(g?.diagnostics.length)renderDiagnostics(g.diagnostics,checkIssues,async id=>{state.setMode('config');updateActions();if(data().graph.nodes.some(n=>n.id===id))chooseNode(id);else await locateNode(id);});
      if(g?.diagnostics_truncated)checkIssues.append($('p','当前展示部分问题，完整报告共 '+g.diagnostic_count+' 项。','wb-caption'));
      const actions=$('div',undefined,'wb-inline-actions');checkIssues.append(actions);
      scopedCheckButton=button(checked?'重新检查当前配置':'检查当前配置',()=>check(),actions,{disabled:checkButton.disabled});button('返回配置与图',()=>wholeConfiguration(),actions);
      const facts=$('dl',undefined,'details');checkCoverage.append(facts);
      for(const[label,value]of [['范围','所选入口与配置'],['流程',g?.binding.entry||working?.entry||'尚未选择'],['配置',g?.binding.params||'未绑定参数文件'],['依据',checked?'固定检查报告':'源码观察，尚未检查'],['状态',checked?g.status:'尚未检查']])facts.append($('dt',label),$('dd',value));
      if(g)raw('规则、覆盖与原始依据',{binding:g.binding,coverage:g.coverage,freshness:g.freshness,completeness:g.completeness,status:g.status},checkCoverage);
    }
    function wholeConfiguration(){if(!run)state.setMode('config');paintConfiguration(null);updateActions();}
    function affects(field,node){
      if(field.affected_nodes.includes(node.id))return true;
      if(!field.affected_nodes_truncated)return false;
      return Object.values(node.parameter_origins||{}).some(origin=>{
        if(origin.argument!==field.parts[1]||!Array.isArray(origin.path))return false;
        const path=field.parts.slice(2),prefix=(a,b)=>a.length<=b.length&&a.every((part,index)=>part===b[index]);
        return ['table_alias','output_path'].includes(field.kind)||prefix(origin.path,path)||prefix(path,origin.path);
      });
    }
    function parse(field,raw){
      if(field.value_type==='str')return raw;
      if(field.value_type==='bool'){if(!['true','false'].includes(raw))throw new Error('布尔值必须为 true 或 false');return raw==='true';}
      const number=Number(raw);if(!raw.trim()||!Number.isFinite(number)||(field.value_type==='int'&&!Number.isSafeInteger(number)))throw new Error('请填写有限且可精确表示的数字');return number;
    }
    function paintConfiguration(node){
      const graph=data().graph;if(!graph)return;
      inspectorWhole=!node;
      inspector.replaceChildren($('h3',!graph.nodes.length?'暂无可展示节点':node?(node.operator?.name||node.api.split(/[.:]/).at(-1)):'整图配置'));
      if(!graph.nodes.length)renderDiagnostics(graph.diagnostics,inspector);
      if(node){inspector.append($('p',node.api+' · '+node.source+':'+node.line,'wb-caption'));const status=ForgeWorkbenchGraph.status(node,snapshot||run);inspector.append($('span',status.label,'wb-tag'));}
      const fields=graph.parameters.fields.filter(f=>!node||affects(f,node));
      const d=run?{values:{}}:getDraft(),key=draftKey();
      if(graph.parameters.reason)inspector.append($('p','配置不可编辑：'+graph.parameters.reason,'wb-caption'));
      if(graph.parameters.truncated)inspector.append($('p','配置投影已达显示上限，未显示项请查看登记文件。','wb-caption'));
      if(!fields.length)inspector.append($('p',node?'没有能证明来自登记参数文件的可写项。代码字面量与动态计算可在下方源码查看。':'没有公开的登记参数字段。','wb-caption'));
      for(const item of fields){
        const row=$('div',undefined,'wb-parameter'),label=$('label',item.pointer.replace('/arguments/',''));
        const input=item.value_type==='bool'?select(item.pointer,[{value:'true',label:'true'},{value:'false',label:'false'}],String(item.value)):$('input');
        input.setAttribute('aria-label','配置 '+item.pointer);input.value=d.values[item.pointer]??inputText(item);input.maxLength=16384;
        input.disabled=!!run||busy||!configWrite()||!item.writable||(item.value_type==='int'&&!Number.isSafeInteger(item.value));
        input.oninput=()=>{if(disposed||run||!input.isConnected||key!==draftKey())return;const next=getDraft(),old=next.values[item.pointer];if(input.value===inputText(item))delete next.values[item.pointer];else next.values[item.pointer]=input.value;
          try{if(Object.keys(next.values).length>32)throw new Error('一次最多修改 32 项');storeDraft(next);}
          catch(error){input.value=old??inputText(item);message(error.message+'；已保留之前的草稿。','error-box');}};
        label.append(input);row.append(label);if((item.affected_node_count??item.affected_nodes.length)>1)row.append($('small','本图范围共享影响 '+(item.affected_node_count??item.affected_nodes.length)+' 个节点'));inspector.append(row);
      }
      if(!run&&graph.parameters.available){
        const bar=$('div',undefined,'wb-inline-actions');inspector.append(bar);
        button('保存配置',()=>saveParameters(),bar,{disabled:!configWrite()});
        button('放弃本地修改',()=>{const next=getDraft();next.values={};storeDraft(next);paintConfiguration(node);},bar);
        if(!configWrite())inspector.append($('p','此项目配置只读；有运行权限时仍可调整本次运行的输入与预算。','wb-caption'));
        runtimeOptions(inspector);
      }
      if(node?.kind==='source_call')inspector.append($('p','当前仅观察到源码调用；输入输出类型、参数语义与执行可用性尚未核验。','wb-caption'));
      else if(node){const schema=details('字段与实际调用参数',inspector);raw('输出字段',node.fields,schema);raw('实际参数',node.parameters,schema);if(node.contract)raw('原生契约与能力',{contract:node.contract,capability:node.capability},schema);}
      paintParameterPending(inspector);
    }
    function runtimeOptions(parent){
      const box=details('本次运行设置',parent),d=getDraft(),key=draftKey();
      for(const item of data().graph?.parameters.fields||[]){if(item.kind!=='table_alias'||item.parts.length!==3)continue;
        const name=item.parts[1],choices=ctx.tables.map(t=>({value:t.alias,label:t.alias+' · v'+t.reference.ref.lance_version}));
        const chosen=d.inputs[name]||item.value;if(options.continueInput&&!choices.some(c=>c.value===options.continueInput))choices.push({value:options.continueInput,label:options.continueInput+' · 上次结果'});
        if(!choices.some(c=>c.value===chosen))choices.push({value:chosen,label:chosen+' · 需核对'});
        const input=select('本次输入 '+name,choices,chosen);input.disabled=!can('run_submit');input.onchange=()=>{if(disposed||run||!input.isConnected||key!==draftKey())return;const next=getDraft();next.inputs[name]=input.value;storeDraft(next);};field(name,input,box);
      }
      const timeout=$('input');timeout.type='number';timeout.min=1;timeout.max=ctx.run_limits.timeout_s;timeout.value=d.timeout;timeout.setAttribute('aria-label','本次运行秒数上限');
      timeout.oninput=()=>{if(disposed||run||!timeout.isConnected||key!==draftKey())return;const next=getDraft();next.timeout=timeout.value;storeDraft(next);};field('秒数上限',timeout,box);
      button('重置本次运行设置',()=>{const next=getDraft();next.inputs={};next.timeout=String(ctx.run_limits.timeout_s);storeDraft(next);paintConfiguration(inspectorWhole?null:data().graph.nodes.find(n=>n.id===data().node)||null);},box);
      box.append($('p','这些设置只作用本次运行。输出使用原生执行器的新运行目录。','wb-caption'));
      if(options.continueInput)box.append($('p','可在输入槽中选择上次结果：'+options.continueInput,'wb-caption'));
    }
    async function saveParameters(){
      if(busy)return;
      const ticket=state.capture(),g=data().graph,d=getDraft(),key=draftKey();
      const fields=new Map(g.parameters.fields.map(f=>[f.pointer,f]));
      const replace=Object.entries(d.values).map(([path,raw])=>{const f=fields.get(path);if(!f)throw new Error('配置字段已变化，草稿保留，请先核对');return {op:'replace',path,value:parse(f,raw)};});
      if(!replace.length){message('没有待保存的配置修改。');return;}
      busy=true;updateActions();
      try{const result=await commands.saveParameters({path:g.parameters.path,expected_sha256:g.parameters.sha256,graph_binding:g.binding,replace});
        const now=ForgeWorkbenchState.drafts.get(key);if(now){for(const[path,raw]of Object.entries(d.values))if(now.values[path]===raw)delete now.values[path];ForgeWorkbenchState.drafts.put(key,now);}
        if(current(ticket)){if(result.no_change)message('配置值未变化，未写入文件。');else{await useWorking(working.entry,working.params);message('配置已保存，图和检查需要重新核对。');}}
      }finally{busy=false;if(root.isConnected&&data().graph){if(data().mode==='checks')paintIssues();else paintConfiguration(inspectorWhole?null:data().graph.nodes.find(n=>n.id===data().node)||null);}updateActions();}
    }
    function chooseNode(id){
      if(!data().graph?.nodes.some(n=>n.id===id)||disposed)return;
      state.chooseNode(id);graphView.select(id);
      if(graphKind==='logical'){logicalSelected=null;logicalView.select(null);}
      observationCursor=null;observationPages=[];lastObservationRevision=null;
      const node=data().graph.nodes.find(n=>n.id===id),binding=data().graph.binding;
      sourceTitle.textContent=(binding.basis_kind==='run'?'封存源码 · ':'源码 · ')+(node.operator?.name||node.api.split(/[.:]/).at(-1)||node.kind);
      if(binding.basis_kind==='report')rememberView({kind:'inspection',report_digest:binding.basis_digest,node_id:id});
      else if(binding.basis_kind==='run')rememberView({kind:'run',run_id:run.run_id,attempt:run.attempt,report_digest:run.report_digest,node_id:id});
      if(data().mode==='checks')paintIssues();else paintConfiguration(node);
      const ticket=state.capture(true);
      if(sourceOpen)loadSource().catch(error=>{if(current(ticket)&&error.name!=='AbortError')message(error.message,'error-box',ticket);});
      if(run&&detailKind==='observations')loadRunDetail(true).catch(error=>{if(current(ticket)&&error.name!=='AbortError')message(error.message,'error-box',ticket);});
    }
    async function loadSource(){
      updateActions();disposeReaders(sourceBody);sourceBody.replaceChildren();
      if(!sourceOpen)return;
      const g=data().graph,node=data().node,ticket=state.capture(true),sequence=++sourceSequence;
      if(!g||!node){sourceBody.append($('p','选择节点后查看实现、调用点或公开配置。','wb-caption'));return;}
      sourceBody.append($('p','正在读取绑定来源…','wb-caption'));
      try{const value=await request('graph-source',{graph_binding:JSON.stringify(g.binding),node_id:node,kind:sourceKind});
        if(!current(ticket)||sequence!==sourceSequence)return;
        if(value.node_id!==node||json(value.graph_binding)!==json(g.binding))throw new Error('源码响应身份不一致');
        disposeReaders(sourceBody);sourceBody.replaceChildren();
        if(value.availability==='unavailable'){sourceBody.append($('p','该视角没有可用材料：'+value.reason,'wb-caption'));return;}
        sourceBody.append($('div',({sealed:'封存来源',report_bound:'报告绑定来源',current:'当前来源'}[value.source_basis]||'绑定来源')+' · '+(value.path||'公开配置')+(value.start_line?':'+value.start_line:''),'wb-caption'));
        if(value.text!==undefined)mountText(sourceBody,value.text,{sourceOnly:true,compactSource:true});
        else mountText(sourceBody,json(value.public_projection??value.fields),{sourceOnly:true,compactSource:true});
        if(value.note)sourceBody.append($('p',value.note,'wb-caption'));
        if(value.reason==='builtin_callsite_only')sourceBody.append($('p','此处为绑定调用点；平台算子的内部实现未展开。','wb-caption'));
        if(value.reason==='source_callsite_only')sourceBody.append($('p','此处为已观察的调用点；实现关系尚未证明。','wb-caption'));
        if(value.truncated)sourceBody.append($('p','这里只显示有界源码片段。','wb-caption'));
      }catch(error){if(current(ticket)&&sequence===sourceSequence&&error.name!=='AbortError'){sourceBody.replaceChildren($('p',error.message,'error-box'));}}
    }
    async function changeMode(mode){
      if(['monitor','results'].includes(mode)){
        if(mode==='monitor'&&comparison){comparison=null;outputFingerprint=null;}
        if(!run){if(lastRun)await openRun(lastRun.run_id,lastRun.attempt,mode);else{historyOpen=true;historyPanel.hidden=false;historyButton.setAttribute('aria-expanded','true');await listRuns();message('选择一次运行后查看它的封存图、状态、日志和结果。');return;}}
        state.setMode(mode);results.hidden=mode!=='results';paintResults();paintRunBar();updateActions();return;
      }
      if(run)await useWorking(working?.entry||ctx.flows[0]?.entry||choices()[0]?.value,working?.params||ctx.flows[0]?.parameters?.[0]||null,null,mode);
      else{state.setMode(mode);if(mode==='checks')paintIssues();else paintConfiguration(inspectorWhole?null:data().graph?.nodes.find(n=>n.id===data().node)||null);updateActions();}
    }
    function paintRunBar(){
      runBar.replaceChildren();runBar.hidden=!run||!!comparison;runFacts.hidden=!run||data().mode==='results';trend.hidden=!run||data().mode==='results';runDetail.hidden=!run||data().mode==='results';
      if(!run)return;
      const choices=(records?.runs||[]).map(item=>({value:item.run_id,label:item.run_id.slice(-10)+' · '+(labels[item.run_id===run.run_id?run.status:item.status]||item.status)+' · '+(item.parameter_path||item.entry)}));
      if(!choices.some(item=>item.value===run.run_id))choices.unshift({value:run.run_id,label:run.run_id.slice(-10)+' · '+(labels[run.status]||run.status)});
      const selectedRun=select('运行记录',choices,run.run_id);selectedRun.title=run.run_id;selectedRun.onchange=()=>openRun(selectedRun.value,null,data().mode).catch(error=>message(error.message,'error-box'));field('运行',selectedRun,runBar);runBar.append($('span',labels[run.status]||run.status,'wb-tag'));
      const attempts=run.attempts?.length?run.attempts:[{attempt:run.attempt,status:run.status}];
      const choice=select('运行尝试',attempts.map(a=>({value:String(a.attempt),label:'尝试 '+a.attempt+' · '+(labels[a.status]||a.status)})),String(run.attempt));
      choice.onchange=()=>openRun(run.run_id,Number(choice.value),data().mode).catch(error=>message(error.message,'error-box'));runBar.append(choice);
      const historical=run.current_attempt!==run.attempt;
      if(historical)runBar.append($('span','历史尝试 · 只读','wb-caption'));
      runBar.append($('span',undefined,'wb-caption wb-run-seal'));
      const target=Object.freeze({run_id:run.run_id,attempt:run.attempt,digest:run.digest});
      const control=async action=>{const ticket=state.capture();await action(target);if(current(ticket))await observer.refresh();};
      if(!historical&&run.status==='prepared')button('开始这次运行',async()=>{
        const ticket=state.capture();await request('run-start',{}, {run_id:target.run_id,expected:target.digest,expected_attempt:target.attempt});if(current(ticket))await observer.refresh();
      },runBar,{disabled:!can('run_submit')});
      if(!historical&&active(run.status))button('停止',()=>control(commands.cancel),runBar,{disabled:!can('run_control')||run.status==='stopping'});
      if(!historical&&!['prepared','succeeded'].includes(run.status))button('核对提交与清理',()=>control(commands.reconcileRun),runBar,{disabled:!can('run_control')||run.supervisor_alive===true});
      if(!historical&&(['cancelled','interrupted'].includes(run.status)||(run.status==='failed'&&run.state.recoverable)))button('恢复运行',async()=>{
        const ticket=state.capture();const value=await commands.resume(target);if(current(ticket)){
          message('已创建新的尝试；当前继续显示原尝试，可选择新尝试查看。');
          const fresh=await request('run',{run_id:target.run_id,attempt:target.attempt,report:'1'});if(current(ticket)){run=fresh;paintRunBar();}
        }
      },runBar,{disabled:!can('run_submit')||run.state.usage_complete!==true});
      button('刷新状态',()=>observer.refresh(),runBar);button('返回当前配置',()=>useWorking(working?.entry||run.entry,working?.params||run.parameter_path),runBar);
      if(run.state.cancel_requested_at)runBar.append($('span','停止：'+({requested:'等待执行端确认',terminating:'正在请求所属进程退出',killing:'正在终止未退出进程',confirmed:'所属执行已停止',unconfirmed:'尚未确认全部停止'}[run.state.stop_phase]||'等待确认'),'wb-caption'));
      if(run.state.cleanup==='pending')runBar.append($('span','临时资源清理待确认 · 容量保留','wb-caption'));
      paintRunSeal();
    }
    function paintRunSeal(){
      const node=runBar.querySelector('.wb-run-seal');if(!node||!run)return;
      const parts=[],graph=data().graph;
      const bound=graph?.binding?.basis_kind==='run'&&graph.binding.run_id===run.run_id&&graph.binding.attempt===run.attempt;
      if(bound&&graph.completeness.display==='complete'){
        const names=graph.nodes.filter(n=>n.kind==='read_table'&&typeof n.parameters?.uri==='string')
          .map(n=>{const name=n.parameters.uri.split('/').pop().replace(/\.lance$/,'');return typeof n.parameters.version==='number'?name+' v'+n.parameters.version:name;});
        if(names.length)parts.push('封存输入 '+names.slice(0,4).join(' / ')+(names.length>4?' 等'+names.length+'个':''));
      }else if(bound)parts.push('图已裁剪，输入摘要从略');
      if(typeof run.limits?.timeout_s==='number')parts.push('预算 '+run.limits.timeout_s+' 秒累计'+(typeof run.state?.usage?.elapsed_ms==='number'?'，已用 '+Math.round(run.state.usage.elapsed_ms/1000)+' 秒':''));
      node.textContent=parts.join(' · ');
    }
    function paintRunFacts(freshness=run?.freshness){
      if(!run)return;runFacts.replaceChildren();
      const sample=snapshot?.telemetry?.sample||{},age=freshness?.telemetry_age_s;
      for(const[label,value]of [['整次运行 CPU',sample.cpu_cores==null?'未测量':sample.cpu_cores.toFixed(2)+' 核'],['进程树 RSS',unit(sample.rss_bytes)],
        ['动作吞吐',sample.throughput?.rows_per_s==null?'未测量':sample.throughput.rows_per_s.toFixed(1)+' 行/秒'],['运行时间',run.state.started_at?Math.max(0,Math.round((run.state.finished_at||Date.now()/1000)-run.state.started_at))+' 秒':'尚未开始']]){
        const card=$('div');card.append($('small',label),$('strong',value));runFacts.append(card);
      }
      const note=$('div',age==null?'暂无可用遥测':(age>5?'观测已陈旧 · ':'最近采样 · ')+Math.round(age)+' 秒前','wb-caption');runFacts.append(note);
      if(run.state.error){
        const failure=run.state.error,box=$('div',undefined,'error-box wb-run-error');
        const summary=typeof failure==='string'?failure:typeof failure.message==='string'&&failure.message.trim()?failure.message:typeof failure.type==='string'?failure.type:'运行错误';
        box.append($('strong',summary));if(typeof failure!=='string')raw('错误详情',failure,box);runFacts.append(box);
      }
      if(run.supervisor_alive===false&&active(run.status))runFacts.append($('div','监督进程不可用，运行可能中断；停止与提交结果待核对。','error-box'));
      if(run.outputs.some(o=>o.status==='committed')&&run.status!=='succeeded')runFacts.append($('div','部分输出已提交；整次运行尚未成功。','wb-partial'));
    }
    async function openRun(id,attempt=null,mode='monitor'){
      historyOpen=false;historyPanel.hidden=true;historyButton.setAttribute('aria-expanded','false');
      sourceOpen=false;sourceBody.hidden=sourceTabs.hidden=true;
      const initial=transition({run:id,attempt,report:null,mode});run=null;comparison=null;
      runBar.hidden=false;runBar.replaceChildren($('span','正在读取运行记录…','wb-caption'));
      let value;
      try{value=await request('run',{run_id:id,compact:'1',...(attempt?{attempt}:{})});}
      catch(error){if(current(initial)&&error.name!=='AbortError'){graphHost.replaceChildren($('p','运行图暂未载入，请重新选择记录。','wb-caption'));inspector.replaceChildren();message(error.message,'error-box',initial);graphLoading(false);}return;}
      if(!current(initial))return;
      if(value.run_id!==id||(attempt&&value.attempt!==attempt))throw new Error('运行记录身份不一致');
      run=value;lastRun={run_id:id,attempt:value.attempt};
      const ticket=transition({run:id,attempt:value.attempt,report:null,mode});
      paintSelectors();paintRunBar();paintRunFacts();results.hidden=mode!=='results';paintResults();
      rememberView({kind:'run',run_id:id,attempt:value.attempt});
      const target=Object.freeze({run_id:id,attempt:value.attempt,digest:value.digest,ticket});
      await Promise.allSettled([fetchGraph(ticket),observer.set(target),records?Promise.resolve():listRuns()]).then(values=>{
        if(!current(ticket))return;
        for(const result of values)if(result.status==='rejected'&&result.reason.name!=='AbortError')message(result.reason.message,'error-box',ticket);
      });
      if(current(ticket))graphLoading(false);
      if(current(ticket))await loadRunDetail(true);
      paintPending();
    }
    async function submit(){
      if(busy||run)return;
      const d=getDraft(),body={entry:working.entry,params:working.params,inputs:{...d.inputs},limits:{timeout_s:Number(d.timeout)},new_outputs:true};
      if(!Number.isInteger(body.limits.timeout_s)||body.limits.timeout_s<1||body.limits.timeout_s>ctx.run_limits.timeout_s)throw new Error('运行秒数上限超出项目预算');
      const ticket=state.capture();busy=true;updateActions();let intent=null;
      try{
        intent=await commands.identity(body);
        if(!current(ticket))return;
        if(intent.phase==='start'){
          const known=await commands.inspect(intent);if(current(ticket)){await openRun(known.run.run_id,known.run.attempt);message('已定位原提交，请在这次运行上继续核对。');}return;
        }
        message('正在检查实际配置并封存本次运行…');
        intent=await commands.prepare(intent);
        if(!current(ticket))return;
        await openRun(intent.run_id,intent.attempt);
        if(disposed||!run||data().run!==intent.run_id||data().attempt!==intent.attempt)return;
        const startTicket=state.capture();
        try{await commands.start(intent);if(current(startTicket)){message('运行已提交：'+intent.run_id);await observer.refresh();}}
        catch(error){if(current(startTicket))message('已固定运行 '+intent.run_id+'；启动结果待核对。请查看这次运行，勿另建提交。 '+error.message,'error-box',startTicket);}
      }catch(error){if(current(ticket)){message('提交结果待核对，原请求身份已保留。 '+error.message,'error-box',ticket);paintPending();}throw error;}
      finally{busy=false;updateActions();if(root.isConnected&&run)paintRunBar();paintPending();}
    }
    async function listRuns(){
      const ticket=state.capture(),sequence=++listSequence;
      const value=await request('runs',{limit:20,...(cursors[page]?{cursor:cursors[page]}:{})});
      if(!current(ticket)||sequence!==listSequence)return;records=value;paintHistory();if(run)paintRunBar();
    }
    function markHistorySelection(){
      for(const row of historyPanel.querySelectorAll('.wb-run-record')){
        const chosen=row.dataset.run===data().run;row.classList.toggle('selected',chosen);row.setAttribute('aria-pressed',String(chosen));
      }
    }
    function paintHistory(){
      if(!records)return;historyPanel.replaceChildren();
      const top=$('div',undefined,'wb-history-heading');top.append($('strong','运行记录'),$('span','本页 '+records.runs.length+' 条 · 全项目共 '+records.total+' 条'+(records.index?.complete===false?' · 列表待同步':''),'wb-caption'));historyPanel.append(top);
      const filter=select('筛选本页运行状态',[{value:'',label:'全部状态'},...Object.entries(labels).map(([value,label])=>({value,label}))],runFilter);
      filter.onchange=()=>{if(!disposed&&filter.isConnected){runFilter=filter.value;paintHistory();}};top.append(filter);
      if(!records.runs.length)historyPanel.append($('p','当前页没有运行记录。运行会在检查和封存后产生独立记录。','wb-caption'));
      const visible=records.runs.filter(item=>!runFilter||item.status===runFilter);
      if(records.runs.length&&!visible.length)historyPanel.append($('p','本页没有符合筛选条件的运行。','wb-caption'));
      for(const item of visible){const row=button('',()=>openRun(item.run_id),historyPanel,{cls:'wb-run-record'});row.replaceChildren($('strong',item.run_id.slice(-10)),$('span',timeText(item.created_at),'wb-caption'),$('span',item.entry),$('span',item.parameter_path||'内联配置'),$('span',labels[item.status]||item.status,'wb-tag'));row.dataset.run=item.run_id;row.setAttribute('aria-label',item.run_id+' · '+item.entry+' · '+(labels[item.status]||item.status));row.classList.toggle('selected',item.run_id===data().run);}
      markHistorySelection();
      const pager=$('div',undefined,'wb-inline-actions');historyPanel.append(pager);
      button('最新记录',async()=>{cursors=[null];page=0;await listRuns();},pager,{disabled:page===0});
      button('较新记录',async()=>{page--;await listRuns();},pager,{disabled:page===0});
      button('较早记录',async()=>{if(cursors.length>=32){cursors=cursors.slice(1);page--;}cursors=cursors.slice(0,page+1);cursors.push(records.next_cursor);page++;await listRuns();},pager,{disabled:!records.next_cursor});
      if(records.runs.length>1){
        const choices=records.runs.map(r=>({value:r.run_id,label:r.run_id.slice(-10)+' · '+(r.parameter_path||r.entry)}));
        const left=select('左侧运行',choices,choices[0].value),right=select('右侧运行',choices,choices[1].value);pager.append(left,right);
        button('比较两次运行',()=>compare(left.value,right.value),pager);
      }
    }
    function paintResults(){
      const fingerprint=json([comparison,run?.run_id,run?.attempt,run?.current_attempt,run?.outputs,can('attach'),can('run_submit')]);
      if(fingerprint===outputFingerprint)return;outputFingerprint=fingerprint;
      results.replaceChildren($('h3',comparison?'两次运行对比':'本次输出'));
      if(comparison){
        results.append($('p',(comparison.same_source?'代码相同':'代码不同')+' · '+(comparison.same_inputs?'输入版本相同':'输入版本不同')+' · 未定义业务质量评分','wb-caption'));
        for(const side of ['left','right']){const item=comparison[side],box=$('div',undefined,'wb-output');box.append($('strong',(side==='left'?'左侧':'右侧')+' · '+item.run_id));button('查看这次运行',()=>openRun(item.run_id,item.attempt),box);for(const output of item.outputs)box.append($('p',output.name+' · '+(output.result?'固定 v'+output.result.ref.lance_version+' · '+output.result.ref.row_count+' 行':'尚无核验结果')));results.append(box);}return;
      }
      if(!run){results.append($('p','选择一次运行后查看固定版本结果。'));return;}
      for(const output of run.outputs){
        const box=$('div',undefined,'wb-output');box.append($('strong',output.name+' · '+({not_started:'尚未开始',writing:'计算或写入中',committed:output.result?'已提交并核验':'已提交 · 登记待完成',unknown:'提交待核对'}[output.status]||output.status)),
          $('span',output.uri,'wb-caption'));results.append(box);
        if(output.retained_from_attempt!=null)box.append($('small','保留自尝试 '+output.retained_from_attempt));
        if(output.result){
          box.append($('p','固定版本 v'+output.result.ref.lance_version+' · '+output.result.ref.row_count+' 行'));
          const captured=Object.freeze({run_id:run.run_id,attempt:run.attempt,output:output.name,alias:'',expected_result:output.result_digest});
          if(run.attempt===run.current_attempt){
            const snapshotRun=run,snapshotOutput=output;
            button('查看固定结果',()=>openResult(snapshotRun,snapshotOutput),box);
            button('作为输入继续加工',()=>options.continueWithResult?.(snapshotRun,snapshotOutput),box,{disabled:!can('run_submit')});
            const alias=$('input'),aliasKey=run.run_id+'|'+run.attempt+'|'+output.name;alias.maxLength=64;alias.setAttribute('aria-label','结果别名 '+output.name);alias.placeholder='保存为输入表别名';alias.value=aliasDrafts.get(aliasKey)||'';box.append(alias);
            alias.oninput=()=>{if(disposed||!alias.isConnected)return;const next=new Map(aliasDrafts);if(alias.value)next.set(aliasKey,alias.value);else next.delete(aliasKey);
              if(next.size>128||new TextEncoder().encode(JSON.stringify([...next])).length>32768){alias.value=aliasDrafts.get(aliasKey)||'';message('结果别名草稿已达上限，请先处理已有项。','error-box');return;}
              if(alias.value)aliasDrafts.set(aliasKey,alias.value);else aliasDrafts.delete(aliasKey);};
            button('保留为输入表',async()=>{const ticket=state.capture();await request('run-attach',{}, {...captured,alias:alias.value});if(current(ticket)){aliasDrafts.delete(aliasKey);message('已保留固定版本引用：'+alias.value);const fresh=await request('context');if(current(ticket)){ctx=fresh;onContext(fresh);}}},box,{disabled:!can('attach')});
          }else box.append($('p','历史尝试的固定结果引用在下方保留；当前表入口不会代替它。','wb-caption'));
          raw('固定结果引用',output.result,box);
        }
      }
    }
    async function compare(left,right){const ticket=state.capture(),value=await request('run-compare',{left,right});if(!current(ticket))return;comparison=value;state.setMode('results');results.hidden=false;paintResults();updateActions();rememberView({kind:'run_compare',left,right});}
    async function loadRunDetail(force=false,resetRetry=false){
      if(!run||runDetail.hidden)return;
      const ticket=state.capture(detailKind==='observations'),sequence=++nodeSequence,kind=detailKind;
      const target={run_id:run.run_id,attempt:run.attempt},g=data().graph;
      try{
      if(kind==='observations'){
        const node=data().node;if(!node)return;
        const value=await request('run-observations',{...target,node_id:node,limit:32,...(observationCursor?{cursor:observationCursor}:{})});
        if(!current(ticket)||sequence!==nodeSequence||detailKind!==kind)return;
        runDetailBody.replaceChildren($('p','节点关联观测 · '+target.run_id+' / 尝试 '+target.attempt+' · 观测 '+(value.observation_revision||'不可用').slice(0,10)+' · 不是完整节点日志','wb-caption'));
        if(value.run_id!==target.run_id||value.attempt!==target.attempt)throw new Error('节点观测身份不一致');
        if(value.availability==='unsupported'){runDetailBody.append($('p','历史运行未记录此类观测。'));return;}
        if(!value.observations.length)runDetailBody.append($('p','这个节点没有单独观测；不会从整次运行状态推断节点成功。'));
        const paint=rows=>{for(const row of rows){const line=$('div',undefined,'wb-observation');line.append($('strong','动作 '+row.action_id+' · 第 '+(row.occurrence+1)+' 次 · '+(labels[row.state]||row.state)),
          $('p','输入 '+safe(row.rows_input)+' 行 · 输出 '+safe(row.rows_output)+' 行 · 调用 '+safe(row.calls)+' · 错误 '+safe(row.errors)),
          $('p','累计处理时间 '+safe(row.elapsed_s)+' 秒 · '+(row.timing_scope==='group'?'融合组共享范围':'节点观测范围'),'wb-caption'));
          if(row.error)line.append($('pre',json(row.error),'error-box'));raw('关联证据与生命周期',row,line);runDetailBody.append(line);}};
        paint(value.observations);
        const pager=$('div',undefined,'wb-inline-actions');runDetailBody.append(pager);
        button('上一页观测',async()=>{if(!current(ticket))return;observationCursor=observationPages.pop()??null;await loadRunDetail(true);},pager,{disabled:!observationPages.length});
        button('后续节点观测',async()=>{if(!current(ticket))return;if(observationPages.length>=32)observationPages.shift();observationPages.push(observationCursor);observationCursor=value.cursor;await loadRunDetail(true);},pager,{disabled:!value.has_more});
      }else if(kind==='logs'){
        const value=await request('run-logs',{...target,offset:logOffset});
        if(!current(ticket)||sequence!==nodeSequence||detailKind!==kind)return;
        // HTTP v1 text logs have no identity echo. The captured request and
        // selection ticket bind their scope; optional future echoes must match.
        if((value.run_id!=null&&value.run_id!==target.run_id)||(value.attempt!=null&&value.attempt!==target.attempt))throw new Error('日志身份不一致');
        logOffset=value.next_offset;const encoded=new TextEncoder().encode((value.truncated?'[较早日志已轮转]\n':logBody)+value.text);logBody=new TextDecoder().decode(encoded.slice(-65536));
        runDetailBody.replaceChildren($('p','原始运行文本 · '+target.run_id+' / 尝试 '+target.attempt+' · 不按当前节点过滤','wb-caption'),$('pre',logBody||'此范围暂无日志文本。','wb-code'));
        button('读取后续日志',()=>loadRunDetail(true),runDetailBody);
      }else if(kind==='events'){
        if(!force&&Date.now()-lastEventAt<5000)return;lastEventAt=Date.now();
        const value=await request('run-events',{...target,...(eventCursor?{cursor:eventCursor}:{})});
        if(!current(ticket)||sequence!==nodeSequence||detailKind!==kind)return;
        if(value.availability==='unsupported'){runDetailBody.replaceChildren($('p','历史运行未记录当前控制事件契约。','wb-caption'));return;}
        if(value.scope!=='control_events_only')throw new Error('控制事件覆盖范围无法核对');
        eventCursor=value.cursor;eventRows=[...eventRows,...(value.events||[])].slice(-100);
        runDetailBody.replaceChildren($('p','控制状态变化 · '+target.run_id+' / 尝试 '+target.attempt+' · 不代表完整节点日志','wb-caption'),$('pre',eventRows.map(e=>e.status+(e.stop_phase?' · '+e.stop_phase:'')).join('\n')||'暂无控制事件。','wb-code'));
      }
      }catch(error){
        if(!current(ticket)||sequence!==nodeSequence||detailKind!==kind)return;
        if(error.resetRequired&&kind==='observations'){
          observationCursor=null;observationPages=[];
          runDetailBody.replaceChildren($('p','观测版本已变化，旧页已丢弃；正在重读当前版本。','wb-caption'));
          if(!resetRetry)return loadRunDetail(true,true);
        }
        throw error;
      }
    }
    function settingsIdentity(){return json([projectSettings.get?.()?.digest||null,ctx.flows.length,ctx.run_permissions,ctx.capabilities]);}
    function guardSettings(){
      if(settings.hidden||settingsBasis===null)return;
      const stale=settingsBasis!==settingsIdentity();
      const note=settings.querySelector('.wb-settings-stale');if(note)note.hidden=!stale;
      const register=settings.querySelector('[data-access=register]');
      if(register)register.disabled=!can('run_submit')||graphHost.getAttribute('aria-busy')==='true'||register.dataset.pending==='true';
      // Keep the captured registry CAS base and the user's fields. A sync must
      // not silently rebase an unfinished permission edit onto another grant.
      if(stale)for(const b of settings.querySelectorAll('[data-access=permissions]'))b.disabled=true;
    }
    function paintSettings(){
      settingsBasis=settingsIdentity();
      settings.replaceChildren($('h3','接入与权限'));
      const stale=$('p','接入或权限信息已变化，当前填写已保留；收起后再展开可读取最新信息并重置填写。','wb-settings-stale wb-caption');stale.hidden=true;settings.append(stale);
      settings.append($('p','已登记 '+ctx.flows.length+' 个原生流程；提交、停止和配置写入分别授权。','wb-caption'));
      const form=$('div',undefined,'wb-inline-form'),entry=$('input'),params=$('input');entry.maxLength=params.maxLength=1024;
      entry.value=working?.entry||'';params.value=working?.params||'';entry.placeholder='flows/process.py:process';params.placeholder='flows/configs/default.json';
      entry.setAttribute('aria-label','登记流程入口');params.setAttribute('aria-label','登记参数文件');field('入口',entry,form);field('参数',params,form);
      const register=button('登记原生流程',async()=>{const ticket=state.capture(),body={entry:entry.value.trim(),params:params.value.trim()};
        try{await request('flow-register',{},body);}
        catch(error){if(error.code==='busy'&&current(ticket)){message('服务正在读取或检查，登记尚未开始；填写已保留，请稍后重试。','error-box',ticket);return;}throw error;}
        if(!current(ticket))return;
        settings.hidden=true;message('已登记 '+body.entry+'；正在读取入口与配置。');
        try{const fresh=await request('context');if(!current(ticket))return;ctx=fresh;onContext(fresh);await useWorking(body.entry,body.params);}
        catch(error){if(current(ticket)&&error.name!=='AbortError')message('入口已登记，读取视图暂未完成；请刷新后选择 '+body.entry+'。 '+error.message,'error-box',ticket);}
      },form,{disabled:!can('run_submit')});register.dataset.access='register';settings.append(form);
      const meta=projectSettings.get?.();
      if(!meta?.manage||!meta.project){settings.append($('p','当前项目权限由服务部署者配置。','wb-caption'));return;}
      const permissions=$('div',undefined,'wb-inline-form'),inputs=new Map(),configured=meta.project.configured_permissions||meta.project.permissions;
      for(const[key,label]of [['run_submit','接入与提交'],['run_control','停止与核对'],['attach','保留结果为输入']]){
        const input=$('input');input.type='checkbox';input.checked=!!configured[key];input.disabled=!meta.service[key];input.setAttribute('aria-label',label);field(label,input,permissions);inputs.set(key,input);
      }
      const savePermissions=button('保存运行权限',async()=>{const ticket=state.capture(),body={workspace_id:project,expected:meta.digest,...Object.fromEntries([...inputs].map(([key,input])=>[key,input.checked]))};
        await request('project-permissions',{},body);if(!current(ticket))return;
        await projectSettings.refresh?.();if(!current(ticket))return;
        const fresh=await request('context');if(current(ticket)){ctx=fresh;onContext(fresh);paintSettings();paintSelectors();message('运行权限已更新，未启动作业。');}
      },permissions,{disabled:!meta.service.run_submit||!meta.service.run_control||!meta.service.attach});savePermissions.dataset.access='permissions';settings.append(permissions);
      const config=$('input');config.type='checkbox';config.checked=!!(meta.project.configured_capabilities||meta.project.capabilities)?.config_write;config.disabled=!meta.workbench?.config_write;config.setAttribute('aria-label','保存图上配置');
      const capabilities=$('div',undefined,'wb-inline-form');field('保存图上配置',config,capabilities);
      const configuredCapabilities={config_write:false,analysis_execute:false,analysis_write:false,case_annotate:false,...(meta.project.configured_capabilities||meta.project.capabilities||{})};
      const saveCapabilities=button('保存配置写入权限',async()=>{const ticket=state.capture();await request('project-capabilities',{}, {workspace_id:project,expected:meta.digest,capabilities:{...configuredCapabilities,config_write:config.checked}});
        if(!current(ticket))return;await projectSettings.refresh?.();if(!current(ticket))return;
        const fresh=await request('context');if(current(ticket)){ctx=fresh;onContext(fresh);paintSettings();paintConfiguration(data().graph?.nodes.find(n=>n.id===data().node)||null);message('配置写入权限已更新。');}
      },capabilities,{disabled:!meta.workbench?.config_write});saveCapabilities.dataset.access='permissions';settings.append(capabilities);
      const analysisPermissions=$('div',undefined,'wb-inline-form'),analysisInputs=new Map();
      for(const[key,label]of [['analysis_execute','运行 Notebook'],['analysis_write','保存 Notebook 与视图'],['case_annotate','保存 Case 标注']]){
        const input=$('input');input.type='checkbox';input.checked=!!configuredCapabilities[key];input.disabled=!meta.workbench?.[key]&&!input.checked;input.setAttribute('aria-label',label);field(label,input,analysisPermissions);analysisInputs.set(key,input);
      }
      const saveAnalysis=button('保存数据分析权限',async()=>{const ticket=state.capture();await request('project-capabilities',{}, {workspace_id:project,expected:meta.digest,capabilities:{...configuredCapabilities,...Object.fromEntries([...analysisInputs].map(([key,input])=>[key,input.checked]))}});
        if(!current(ticket))return;await projectSettings.refresh?.();if(!current(ticket))return;
        const fresh=await request('context');if(current(ticket)){ctx=fresh;onContext(fresh);paintSettings();message('数据分析权限已更新，未执行 Notebook。');}
      },analysisPermissions,{disabled:![...analysisInputs.keys()].some(key=>meta.workbench?.[key]||configuredCapabilities[key])});saveAnalysis.dataset.access='permissions';settings.append(analysisPermissions);
      settings.append($('p','配置写入、分析执行、保存和标注分别受项目授权与服务上限约束；旧登记格式需由部署者显式迁移。','wb-caption'));
    }
    const pendingBox=$('section',undefined,'wb-pending');root.insertBefore(pendingBox,settings);
    function paintPending(){
      pendingBox.replaceChildren();
      let items=[];
      try{items=commands.pending();}catch(error){message(error.message,'error-box');return;}
      pendingBox.hidden=!items.length;
      for(const intent of items){
        const line=$('div',undefined,'wb-pending-row');line.append($('strong','提交待核对'),$('span',intent.request_id,'wb-caption'));pendingBox.append(line);
        if(intent.run_id)button('查看原运行',()=>openRun(intent.run_id,intent.attempt),line);
        button('核对原请求',async()=>{
          const ticket=state.capture(),value=await commands.inspect(intent);if(!current(ticket))return;
          if(value.run){await openRun(value.run.run_id,value.run.attempt);message(value.run.status==='prepared'?'原运行已准备，可在同一次运行上开始。':'已定位原运行，未创建另一条记录。');paintPending();}
          else if(value.receipt?.status==='failed'){
            line.append($('p','准备已确认失败：'+(value.receipt.error||value.receipt.reason||''),'error-box'));
            button('结束此失败请求',()=>{commands.finish(intent);paintPending();},line);
          }else{
            line.append($('p','准备尚未完成或失联，原身份继续保留。','wb-caption'));
            button('核对并恢复准备',async()=>{const t=state.capture(),receipt=await commands.reconcile(intent);if(current(t)){message('原准备状态：'+receipt.status);paintPending();}},line,{disabled:!can('run_control')});
          }
        },line);
        if(!intent.run_id)button('以原请求重试准备',async()=>{const ticket=state.capture(),known=await commands.retryPrepare(intent);if(current(ticket))await openRun(known.run_id,known.attempt);},line,{disabled:!can('run_submit')});
      }
    }
    function clearSavedDraft(intent){
      const key=project+'|'+intent.body.graph_binding.entry+'|'+intent.path,d=ForgeWorkbenchState.drafts.get(key);if(!d)return;
      for(const operation of intent.body.replace){const raw=d.values[operation.path];if(raw===undefined)continue;
        const match=typeof operation.value==='string'?raw===operation.value:raw===String(operation.value)||Number(raw)===operation.value;
        if(match)delete d.values[operation.path];
      }
      ForgeWorkbenchState.drafts.put(key,d);
    }
    function paintParameterPending(parent){
      if(run||!working)return;
      let intents=[];try{intents=commands.files().filter(i=>i.path===working.params);}catch(error){parent.append($('p',error.message,'error-box'));return;}
      for(const intent of intents){
        const box=$('div',undefined,'wb-file-pending');box.append($('strong','配置保存待核对'),$('p',intent.error||'尚未收到确定的保存结果。','wb-caption'));parent.append(box);
        button('读回当前配置',async()=>{const ticket=state.capture(),result=await commands.inspectFile(intent);if(!current(ticket))return;
          if(result.state==='content_matches'){
            box.append($('p','当前内容符合本次草稿；写入归属未确认，先前异常也不能由读回证明持久性。','wb-caption'));
            button('采用当前内容',async()=>{clearSavedDraft(intent);commands.forgetFile(intent.path);await useWorking(working.entry,working.params);message('已采用当前内容；先前保存异常仍不构成持久性证明，请重新检查。');},box);
          }else if(result.state==='base_observed'){
            box.append($('p','尚未观察到变化；原请求可能仍在途。可按原条件重试。','wb-caption'));
            button('按原条件重试保存',async()=>{const t=state.capture(),value=await commands.retryFile(intent);if(!current(t))return;
              if(value.applied&&value.new_sha256===intent.body.proposed_sha256){clearSavedDraft(intent);commands.forgetFile(intent.path);await useWorking(working.entry,working.params);message('原条件保存已完成，请重新检查。');}
            },box,{disabled:!configWrite()});
          }else box.append($('p','当前配置已被其他内容改变；草稿保留。请显式核对新基线，不能自动覆盖。','error-box'));
        },box);
      }
    }
    const trend=details('性能趋势 · 整次运行',root),trendPlot=$('div',undefined,'wb-trend');trend.append(trendPlot);trend.hidden=true;
    root.insertBefore(trend,body);
    let metricSequence=0,lastObservationRevision=null;
    async function refreshTrend(force=false){
      if(!run||!trend.open||(!force&&Date.now()-lastMetricAt<5000))return;
      const ticket=state.capture(),sequence=++metricSequence,target={run_id:run.run_id,attempt:run.attempt};lastMetricAt=Date.now();
      const value=await request('run-metrics',{...target,points:300});
      if(current(ticket)&&sequence===metricSequence)ForgeWorkbenchGraph.cpuChart(trendPlot,value.points||[]);
    }
    trend.ontoggle=()=>{if(trend.open)refreshTrend(true).catch(error=>message(error.message,'error-box'));};
    const observer=ForgeWorkbenchObserver.create({project,api,
      isCurrent:target=>current(target.ticket)&&data().run===target.run_id&&data().attempt===target.attempt,
      onSnapshot:async(value,target)=>{
        if(!current(target.ticket))return;
        if(notice.dataset.observationError)message('');
        if(value.availability==='unsupported'){snapshot={...value,observations:[],outputs:run.outputs,observation_availability:'unsupported'};graphView.paint(snapshot);message('此历史运行没有当前观测契约；封存图、记录和原始日志仍按已有材料显示。');return;}
        if(value.unchanged){paintRunFacts(value.freshness);return;}
        snapshot=value;run={...run,...value,attempts:run.attempts?.map(a=>a.attempt===value.attempt?{...a,status:value.status}:a)};
        paintRunBar();paintRunFacts();paintResults();graphView.paint(value);
        if(data().graph&&data().node&&data().mode!=='checks'){
          const stateLabel=inspector.querySelector('.wb-tag');if(stateLabel){const node=data().graph.nodes.find(n=>n.id===data().node);if(node)stateLabel.textContent=ForgeWorkbenchGraph.status(node,value).label;}
        }
      },
      onFailure:(error,target)=>{message('连接暂不可用，保留上次观测并退避重试。 '+error.message,'error-box',target.ticket);if(current(target.ticket))notice.dataset.observationError='true';},
      onDetails:async()=>{
        await refreshTrend();
        if(detailKind==='logs'||detailKind==='events')await loadRunDetail();
        if(detailKind==='observations'&&snapshot?.observation_revision!==lastObservationRevision){lastObservationRevision=snapshot?.observation_revision;await loadRunDetail();}
      },
    });
    const handle=Object.freeze({
      sync:async(fresh,{graphChanged=false,catalogChanged=false,runsChanged=false,force=false}={})=>{
        const permissionsChanged=json(ctx.run_permissions)!==json(fresh.run_permissions)||json(ctx.capabilities)!==json(fresh.capabilities);
        ctx=fresh;paintSelectors();updateActions();
        if(permissionsChanged){if(run){paintRunBar();paintResults();}if(data().graph&&data().mode!=='checks')paintConfiguration(inspectorWhole?null:data().graph.nodes.find(n=>n.id===data().node)||null);}
        if(historyOpen&&(runsChanged||force))await listRuns();
        if(catalogChanged||force){projectInfoRendered=false;if(documents.open)await refreshProjectInfo();}
        if(data().run&&!run&&!busy){await openRun(data().run,data().attempt,data().mode);return;}
        if(force&&run)await observer.refresh();
        if(run&&!data().graph)await fetchGraph(state.capture(),{preserveSelection:true});
        else if((graphChanged||force||!data().graph)&&!run&&!busy){
          if(Object.keys(getDraft().values).length)message('项目内容已变化；配置草稿保留，请核对后再保存。');
          else if(working.entry)await fetchGraph(state.capture(),{preserveSelection:true,refresh:true});
        }
      },
      dirty:()=>!!working&&Object.keys(getDraft().values).length>0,
      projectCheckSettled:()=>{if(!disposed&&documents.open&&projectInfoDirty)refreshProjectInfo();},
      dispose:()=>{disposed=true;observer.dispose();graphView.dispose();logicalView.dispose();controller.abort();state.dispose();disposeReaders(sourceBody);disposeReaders(runDetailBody);},
    });
    options.register?.(handle);
    let firstEntry=view.entry||ctx.flows[0]?.entry||choices()[0]?.value||null;
    let firstParams=view.params||ctx.flows.find(f=>f.entry===firstEntry)?.parameters?.[0]||null;
    working={entry:firstEntry,params:firstParams};
    if(view.kind==='run')await openRun(view.run_id,view.attempt||null);
    else if(view.kind==='inspection')await useWorking(firstEntry,firstParams,view.report_digest,'checks');
    else if(view.kind==='run_compare'){await useWorking(firstEntry,firstParams);await compare(view.left,view.right);}
    else await useWorking(firstEntry,firstParams,null,view.kind==='checks'?'checks':'config');
    if(view.kind==='runs'){historyOpen=true;historyPanel.hidden=false;historyButton.setAttribute('aria-expanded','true');await listRuns();}
    if(view.node_id&&data().graph){
      if(view.kind==='run'&&view.report_digest!==run?.report_digest)throw new Error('运行节点不属于这份封存报告');
      if(!data().graph.nodes.some(n=>n.id===view.node_id)){
        state.chooseNode(null);graphView.select(null);sourceSequence++;disposeReaders(sourceBody);sourceBody.replaceChildren();inspector.replaceChildren();
        try{await locateNode(view.node_id);}
        catch(error){if(error.code==='not_found')throw new Error('定位节点不属于本图');throw error;}
        if(!data().graph?.nodes.some(n=>n.id===view.node_id))throw new Error('定位节点不属于本图');
      }
      chooseNode(view.node_id);
    }
    return handle;
  }
  globalThis.ForgeWorkbench=Object.freeze({mount});
})();
