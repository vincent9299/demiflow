/* Declarative three-region Case reading and separate persistent evaluations. */
(() => {
  'use strict';
  const $=(tag,text,kind)=>{const node=document.createElement(tag);if(text!==undefined)node.textContent=text;if(kind)node.className=kind;return node;};
  const uid=()=> 'v'+crypto.randomUUID().replaceAll('-','');
  const formats=['message','dialogue','text','md','code','json','image','audio','video','tag','html'];
  const roles={input:'输入',output:'输出',reference:'参考',context:'上下文',evaluation:'评测材料'};
  const canonical=value=>JSON.stringify(value,(_key,item)=>item&&typeof item==='object'&&!Array.isArray(item)?Object.fromEntries(Object.keys(item).sort().map(key=>[key,item[key]])):item);
  async function hash(value){return [...new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(canonical(value))))].map(item=>item.toString(16).padStart(2,'0')).join('');}
  const primitiveTypes=new Set(['bool','int64','float64','utf8']);
  const fail=message=>{throw Error(message);};
  function text(value,label,maximum=256,empty=false){
    if(typeof value!=='string'||ForgeAnalysisState.bytes(value)>maximum||value.includes('\0')||(!empty&&!value.trim()))fail(label+'为空或超过 '+maximum+' 字节');
    for(const point of value)if(point.codePointAt(0)>=0xd800&&point.codePointAt(0)<=0xdfff)fail(label+'包含无效字符');
  }
  function exact(value,required,optional=[]){
    if(!value||typeof value!=='object'||Array.isArray(value)||required.some(key=>!Object.hasOwn(value,key))||Object.keys(value).some(key=>!required.includes(key)&&!optional.includes(key)))fail('Case 配置结构无效');
  }
  function validateQuestion(value){
    exact(value,['id','title','type','required'],['options','minimum','maximum']);ForgeAnalysisState.identity(value.id);text(value.title,'评测题目');
    if(typeof value.required!=='boolean'||!['choice','multi','rating','text'].includes(value.type))fail('评测类型或必填设置无效');
    if(['choice','multi'].includes(value.type)){
      if('minimum' in value||'maximum' in value||!Array.isArray(value.options)||!value.options.length||value.options.length>32)fail('单选/多选需要 1—32 个选项');
      const seen=new Set();for(const option of value.options){exact(option,['id','label']);ForgeAnalysisState.identity(option.id);text(option.label,'选项标题');if(seen.has(option.id))fail('选项 ID 重复');seen.add(option.id);}
    }else if(value.type==='rating'){
      if('options' in value||!Number.isInteger(value.minimum)||!Number.isInteger(value.maximum)||value.minimum< -10000||value.maximum>10000||value.minimum>value.maximum)fail('评分上下限须为 -10000 至 10000 之间的整数，且最低分不高于最高分');
    }else if('options' in value||'minimum' in value||'maximum' in value)fail('文本题包含了其他类型的设置');
    ForgeAnalysisState.bounded(value,16384);return value;
  }
  function validateAnswer(definition,value){
    validateQuestion(definition);
    if(value===null){if(definition.required)fail('请填写必填项：'+definition.title);return;}
    if(definition.type==='text'){text(value,'答案 '+definition.title,8192,!definition.required);return;}
    if(definition.type==='rating'){if(!Number.isInteger(value)||value<definition.minimum||value>definition.maximum)fail('评分超出定义范围：'+definition.title);return;}
    const options=new Set(definition.options.map(item=>item.id));
    if(definition.type==='choice'){if(typeof value!=='string'||!options.has(value))fail('请选择有效选项：'+definition.title);}
    else if(!Array.isArray(value)||value.length>32||definition.required&&!value.length||new Set(value).size!==value.length||value.some(item=>!options.has(item)))fail('多选答案无效：'+definition.title);
  }
  function validateFilter(value,schema){
    let count=0;
    const scalar=(item,field)=>{
      const kind=schema.get(field)?.type;if(item===null)fail('空值筛选请使用“为空”');
      if(kind==='bool'&&typeof item!=='boolean'||kind==='int64'&&!Number.isSafeInteger(item)||kind==='float64'&&(typeof item!=='number'||!Number.isFinite(item)))fail('筛选值与字段类型不符：'+field);
      if(kind==='utf8')text(item,'筛选值',1024,true);if(!primitiveTypes.has(kind))fail('该字段不支持筛选');
    };
    function visit(item,depth){
      if(++count>32||depth>4)fail('筛选最多 32 个节点、嵌套深度 4');
      if(item?.op==='and'||item?.op==='or'){
        exact(item,['op','items']);if(!Array.isArray(item.items)||!item.items.length||item.items.length>16)fail('每组需要 1—16 个筛选条件');for(const child of item.items)visit(child,depth+1);return;
      }
      exact(item,['op','field','value']);if(!schema.has(item.field))fail('筛选字段不在固定来源中');
      if(item.op==='is_null'){if(typeof item.value!=='boolean')fail('空值筛选需要是/否');return;}
      if(item.op==='eq'||item.op==='contains'){scalar(item.value,item.field);if(item.op==='contains'&&schema.get(item.field).type!=='utf8')fail('“包含”只支持文本字段');}
      else if(item.op==='in'){if(!Array.isArray(item.value)||!item.value.length||item.value.length>32)fail('“属于”需要 1—32 个值');for(const part of item.value)scalar(part,item.field);}
      else if(item.op==='range'){exact(item.value,['lower','upper']);if(!['int64','float64'].includes(schema.get(item.field).type))fail('范围筛选只支持数值字段');scalar(item.value.lower,item.field);scalar(item.value.upper,item.field);if(item.value.lower>item.value.upper)fail('筛选下限不能高于上限');}
      else fail('未知筛选关系');
    }
    if(value!==null)visit(value,0);return count;
  }
  function validateGroups(descriptor,draft=false){
    if(descriptor.version===1){if(Object.hasOwn(descriptor,'output_groups'))fail('输出方案需要 v2 视图');return;}
    if(descriptor.version!==2||!Array.isArray(descriptor.output_groups)||!descriptor.output_groups.length||descriptor.output_groups.length>8)fail('v2 视图需要 1—8 个输出方案');
    const components=new Map(descriptor.fields.map(field=>[field.id,field])),ids=new Set(),members=new Set();let count=0;
    for(const group of descriptor.output_groups){
      exact(group,['id','label','fields']);if(draft){if(typeof group.id!=='string'||group.id.length>96)fail('方案 ID 草稿无效');text(group.label,'方案标题',256,true);}else{ForgeAnalysisState.identity(group.id);text(group.label,'方案标题');}
      if(ids.has(group.id)||!Array.isArray(group.fields)||group.fields.length>32||!draft&&!group.fields.length)fail('方案 ID 重复或材料成员无效');ids.add(group.id);let output=false;
      for(const id of group.fields){if(++count>32||members.has(id))fail('方案材料最多 32 项且每个展示组件只能属于一个方案');members.add(id);const field=components.get(id);if(!draft&&(!field||!['output','evaluation'].includes(field.role)))fail('方案成员须是已配置的输出或模型评测字段');if(field?.role==='output')output=true;}
      if(!draft&&!output)fail('每个输出方案至少需要一个输出材料');
    }
  }
  function outputItems(descriptor){
    const fields=new Map(descriptor.fields.map(field=>[field.id,field])),used=new Set(),result=[];
    for(const group of descriptor.output_groups||[]){const members=group.fields.map(id=>fields.get(id)).filter(field=>field&&['output','evaluation'].includes(field.role));for(const id of group.fields)used.add(id);result.push({key:'group:'+group.id,kind:'group',id:group.id,label:group.label,fields:members});}
    for(const field of descriptor.fields)if(field.role==='output'&&!used.has(field.id))result.push({key:'field:'+field.id,kind:'field',label:field.label,fields:[field],field:field.field});
    return result;
  }
  function viewName(descriptor){
    if(descriptor?.name)return descriptor.name;
    const input=descriptor?.fields?.find(field=>field.role==='input')?.label||'Case 视图';
    const count=descriptor?.output_groups?.length;
    return input+(count?' · '+count+' 个方案':'');
  }
  // Display-only title from the first context/text field; the fixed row key
  // remains the identity. Never used as a business key, dedup or save target.
  function caseTitle(descriptor,record,key){
    const fallback='Case '+key;
    const field=descriptor?.fields?.find(item=>item.role==='context'&&item.format==='text');
    const raw=field&&record?record[field.field]:null;
    if(typeof raw!=='string')return {title:fallback,key:fallback};
    const collapsed=raw.replace(/\s+/g,' ').trim();
    if(!collapsed)return {title:fallback,key:fallback};
    const points=[...collapsed];
    return {title:points.length>160?points.slice(0,159).join('')+'…':collapsed,key:fallback};
  }
  function validateDescriptor(value,snapshot,forQuery=false){
    exact(value,['version','source_snapshot','case_key','fields','grouping','filters','sort','layout','questions'],['variant_key','output_groups','name']);
    if('name' in value){text(value.name,'视图名称',256);if(/[\r\n]/.test(value.name))fail('视图名称须为单行');}
    if(![1,2].includes(value.version)||value.source_snapshot!==snapshot.digest||canonical(value.case_key)!==canonical(snapshot.case_key))fail('Case 配置与固定来源不一致');
    if('variant_key' in value)fail('此工作台尚未接入可验证的 variant 键；请使用独立固定行');
    const schema=new Map(snapshot.schema.map(item=>[item.name,item]));
    if(!Array.isArray(value.fields)||!value.fields.length||value.fields.length>32)fail('视图需要 1—32 个展示字段');
    const ids=new Set(),projection=new Set();
    for(const field of value.fields){
      exact(field,['id','field','label','role','format'],['language']);ForgeAnalysisState.identity(field.id);text(field.label,'字段标题');
      if(ids.has(field.id)||!schema.has(field.field)||!Object.hasOwn(roles,field.role)||!formats.includes(field.format))fail('字段绑定、角色或预览格式无效');ids.add(field.id);projection.add(field.field);
      if('language' in field){if(field.format!=='code')fail('代码语言只用于 code 字段');ForgeAnalysisState.identity(field.language);}
    }
    validateGroups(value);if(!['card','comparison','gallery'].includes(value.layout))fail('未知 Case 布局');
    if(!Array.isArray(value.grouping)||value.grouping.length>4||new Set(value.grouping).size!==value.grouping.length)fail('分组最多 4 个不同字段');
    for(const name of value.grouping){if(!primitiveTypes.has(schema.get(name)?.type))fail('分组需要基本类型字段');projection.add(name);}
    if(!Array.isArray(value.sort)||value.sort.length>4)fail('排序最多 4 个字段');const ordered=new Set();
    for(const order of value.sort){exact(order,['field','direction']);if(ordered.has(order.field)||!primitiveTypes.has(schema.get(order.field)?.type)||!['asc','desc'].includes(order.direction))fail('排序字段或方向无效');ordered.add(order.field);projection.add(order.field);}
    validateFilter(value.filters,schema);
    const collect=item=>{if(!item)return;if(item.items)for(const child of item.items)collect(child);else projection.add(item.field);};collect(value.filters);
    if(forQuery&&projection.size>16)fail('展示、分组、筛选和排序共用的读取字段最多 16 个，请减少字段');
    if(!Array.isArray(value.questions)||value.questions.length>16)fail('评测定义最多 16 项');const questions=new Set();
    for(const item of value.questions){validateQuestion(item);if(questions.has(item.id))fail('评测题目 ID 重复');questions.add(item.id);}
    ForgeAnalysisState.bounded(value,64*1024);return value;
  }
  function mount({parent,api,project,connected,permissions,mountText,disposeReaders=()=>{},media,fieldMedia,resolveMedia,authorizeURL,onContext,features={},toolbarHost=null}){
    const S=ForgeAnalysisState,store=new S.Store(),root=$('section',undefined,'analysis-case');root.hidden=true;parent.append(root);
    const baseTitle=document.title;
    const toolbar=$('div',undefined,'analysis-bar'),contextBar=$('div',undefined,'case-ui-context'),notice=$('div',undefined,'case-ui-notice'),status=$('p',undefined,'analysis-case-status muted small'),presentationPanel=$('div'),pending=$('div'),mediaPending=$('div',undefined,'case-media-pending'),settings=$('details',undefined,'analysis-case-settings');settings.hidden=true;
    const grid=$('div',undefined,'analysis-case-grid'),rail=$('aside',undefined,'analysis-case-rail'),content=$('article',undefined,'analysis-case-content'),review=$('aside',undefined,'analysis-case-review');
    const profile=$('details',undefined,'analysis-case-profile');profile.append($('summary','字段分布与覆盖范围'));contextBar.append(status);grid.append(rail,content,review);root.append(toolbar,contextBar,notice,presentationPanel,pending,mediaPending,settings,grid);notice.setAttribute('role','status');
    let disposed=false,active=false,ticket=0,selectionTicket=0,reviewTicket=0,reading=false,busy=false,pageTicket=0,sourceTicket=0;
    let candidate=null,locator=null,source=null,state=null,page=null,row=null,headsReady=false,activeQuestions=[],latestHeads=new Map(),configurationChanged=false,materialDraftPending=false;
    let captureBlocked=false,discarded=false,sourceTransition=false,pageBasis=null,viewConflict=null,presentation=null,unavailableReason=null,configTab='fields',queryControlsOpen=false;
    let sourceDisplay=null,publishedContext=null;
    let currentGroupBindings=[],verifiedReview=null,filterApplying=false;
    const rendererHandles=new Set(),permissionButtons=new Map(),evidenceViews=new Set(),groupChoices=new Map();let groupChoiceSource=null;
    const abort=new AbortController(),live=version=>!disposed&&active&&ticket===version&&root.isConnected;
    const current=(version,draft)=>live(version)&&state===draft;
    function publishContext(phase){
      if(!live(ticket)||typeof onContext!=='function')return;
      const snapshot=!sourceTransition&&['ready','unavailable'].includes(phase)?source:null;
      const label=snapshot?sourceDisplay:phase==='loading'||phase==='unavailable'?candidate?.label:null;
      const view=snapshot&&state?.view_id?{id:state.view_id,revision:state.revision}:null;
      const identity=canonical({phase,digest:snapshot?.digest||null,label:label||null,view});if(identity===publishedContext)return;
      publishedContext=identity;
      try{onContext({phase,source:snapshot?S.clone(snapshot):null,label:label||null,view});}
      catch(error){info('数据来源栏刷新失败；当前材料和草稿已保留：'+error.message,true);}
    }
    const permitted=operation=>connected&&!!permissions[operation==='view-save'?'analysis_write':'case_annotate'];
    function releaseContent(){selectionTicket++;globalThis.ForgeAnalysisRender?.disposeWithin?.(content);for(const handle of rendererHandles)try{handle.dispose();}catch{}rendererHandles.clear();disposeReaders(content);}
    function invalidate(){ticket++;pageTicket++;reviewTicket++;headsReady=false;verifiedReview=null;currentGroupBindings=[];materialDraftPending=false;releaseContent();}
    function basis(){const saved=state?.view_id&&canonical(state.base)===canonical(state.descriptor);return state?{id:saved?state.view_id:null,revision:saved?state.revision:0,descriptor:state.descriptor}:null;}
    const queryBasis=(view=basis(),submittedQuery=query(),filter=state?.review_filter)=>({view,query:submittedQuery,...(filter?{review:filter}:{})});
    const pageIsCurrent=()=>!!page&&pageBasis===canonical(queryBasis());
    function paintStatus(){
      if(!state||!source){status.textContent=connected?'请选择已登记的固定来源。':'分析服务未连接。';document.title=baseTitle;return;}
      const dirty=state.answers.filter(item=>item.dirty).length;
      let pendingCount=0;try{pendingCount=store.pending(project).filter(item=>item.scope.kind==='case'&&item.operation!=='media-capture'&&item.scope.source===source.digest).length;}catch{ /* unreadable pending store keeps the status line */ }
      const viewState=state.view_touched?'未保存视图草稿'+(state.revision?'（基于 v'+state.revision+'）':''):state.view_id?'已存视图 v'+state.revision:'本地视图';
      status.textContent=viewName(state.descriptor)+' · '+viewState+(dirty?' · '+dirty+' 份答案草稿':'')+(pendingCount?' · '+pendingCount+' 份待核对':'')+(sourceTransition?' · 正在切换来源':unavailableReason?' · 来源不可用':'')+(page&&!pageIsCurrent()?' · 配置已变化，需重新读取':'')+' · 来源 '+source.digest.slice(0,12);
      viewIdentity.textContent=(state.view_id?'视图 '+state.view_id+' · v'+state.revision:'本地视图草稿')+'\n固定来源 '+source.digest+'\n'+(sourceDisplay||'固定来源')+' · '+source.rows+' 行 · '+source.schema.length+' 字段';
      document.title=(active?viewName(state.descriptor)+' · ':'')+baseTitle;
    }
    function guardWrite(operation,fixedSource=source?.digest){if(!permitted(operation))throw Error(operation==='view-save'?'当前项目尚未开放保存分析视图':'当前项目尚未开放标注权限');if(fixedSource===source?.digest&&(sourceTransition||reading))throw Error('请先完成固定来源核对');if(fixedSource===source?.digest&&unavailableReason)throw Error('固定来源当前不可用，可查看历史；恢复原来源后再保存');}
    function writeButton(label,operation,callback,parent){const item=button(label,callback,parent,!permitted(operation));permissionButtons.set(item,operation);return item;}
    function paintPermissions(){for(const [item,operation]of permissionButtons){if(!item.isConnected)permissionButtons.delete(item);else item.disabled=!permitted(operation)||busy||!!unavailableReason&&(!item.dataset.sourceSnapshot||item.dataset.sourceSnapshot===source?.digest);}paintStatus();}
    const read=(name,query={})=>api('analysis-'+name,query,undefined,project,{signal:abort.signal});
    const readBody=(name,value)=>api('analysis-'+name,{},value,project,{signal:abort.signal});
    const info=(text,error=false)=>notice.replaceChildren($('p',text,error?'error-box':'muted small'));
    function button(label,callback,parent,disabled=false){const item=$('button',label);item.type='button';item.disabled=disabled;item.onclick=()=>{const version=ticket;return Promise.resolve().then(callback).catch(error=>{if(live(version)&&error.name!=='AbortError')info(error.message,true);});};parent.append(item);return item;}
    function select(options,value,label,parent){const item=$('select');item.setAttribute('aria-label',label);for(const [key,text]of options){const option=$('option',text);option.value=key;item.append(option);}item.value=value;parent.append(item);return item;}
    function input(value,label,parent,maximum=256,type='text'){const field=$('input');field.type=type;if(type==='checkbox')field.checked=!!value;else field.value=value;field.maxLength=maximum;field.setAttribute('aria-label',label);parent.append(field);return field;}
    const scopedDraftPrefix='case-fields-v2:',groupDraftPrefix='case-groups-v3:';
    const draftKey=id=>typeof id==='string'&&[scopedDraftPrefix,groupDraftPrefix].some(prefix=>id.startsWith(prefix))?id.slice(id.indexOf(':')+1):id;
    function draftIdentity(id){S.identity(draftKey(id));}
    function flush(){
      if(!state||!source||discarded)return true;
      // Old Case code rejects this private codec marker instead of treating a
      // scoped answer as a whole-Case answer. The logical view key is preserved.
      const grouped=state.id.startsWith(groupDraftPrefix)||state.descriptor.version===2||state.retained_view?.descriptor.version===2||state.review_group||state.review_variant||state.review_filter||state.answers.some(entry=>entry.target_variant);
      if(grouped)state.id=groupDraftPrefix+draftKey(state.id);
      else if(state.answers.some(entry=>entry.target_field!==undefined&&entry.target_field!==null)&&!state.id.startsWith(scopedDraftPrefix))state.id=scopedDraftPrefix+draftKey(state.id);
      try{const durable=store.saveCaseDraft(project,source.digest,state);captureBlocked=false;discard.hidden=!(state.view_touched||state.answers.some(item=>item.dirty));
        if(!durable)info('Case 草稿仅在本窗口暂存，重开浏览器前请导出。');return true;
      }catch(error){captureBlocked=true;discard.hidden=false;info('Case 草稿未能安全暂存，已保留当前内容并暂停离开：'+error.message+'。请先导出，或明确放弃本来源草稿。',true);return false;}
    }
    function changed(){discarded=false;flush();paintStatus();if(source)publishContext(sourceTransition?'loading':unavailableReason?'unavailable':'ready');}
    const beforeUnload=event=>{if(!flush()||store.hasVolatileSources()){event.preventDefault();event.returnValue='';}};
    globalThis.addEventListener('beforeunload',beforeUnload);
    const sourceLabel=$('strong','从数据目录选择固定版本表');toolbar.append(sourceLabel);
    function labelSource(label,snapshot){sourceDisplay=label;sourceLabel.textContent=label;sourceLabel.title='固定来源 '+snapshot.digest+' · '+snapshot.rows+' 行 · '+snapshot.schema.length+' 字段';}
    const toolbarActions=$('div',undefined,'case-ui-toolbar-actions'),actionsHost=toolbarHost instanceof HTMLElement?toolbarHost:toolbar;
    const picker=select([['','本地视图草稿']],'','选择 Case 视图',toolbarActions);
    button('读取 Case',()=>load(0),toolbarActions,!connected);
    const configure=button('字段与评测配置',()=>{settings.hidden=false;settings.open=!settings.open;},toolbarActions);configure.setAttribute('aria-expanded','false');
    settings.ontoggle=()=>{settings.hidden=!settings.open;configure.setAttribute('aria-expanded',String(settings.open));};
    writeButton('保存视图','view-save',saveView,toolbarActions);
    actionsHost.append(toolbarActions);
    const advanced=$('details',undefined,'case-ui-tools'),advancedBody=$('div',undefined,'case-ui-tools-body');advanced.append($('summary','历史与草稿'),advancedBody);contextBar.append(advanced,profile);
    const viewReference=$('details',undefined,'case-ui-view-reference'),viewIdentity=$('code');viewReference.append($('summary','视图标识与来源'),viewIdentity);advancedBody.append(viewReference);
    button('恢复已存视图（保留答案草稿）',()=>restoreView(),advancedBody,!connected);
    const revisionPicker=input('','视图历史版本',advancedBody,10,'number');revisionPicker.min=1;revisionPicker.placeholder='视图历史版本';
    button('读取视图版本',()=>{if(!state?.view_id)throw Error('请先选择已保存视图');const revision=Number(revisionPicker.value);if(!Number.isSafeInteger(revision)||revision<1)throw Error('请输入有效的视图版本');return openView(state.view_id,revision);},advancedBody);
    button('复制为新视图',()=>{if(!state)throw Error('请先选择来源');state.id=uid();state.view_id=null;state.revision=0;state.base=null;state.view_touched=true;clearViewConflict();changed();picker.value='';info('已复制为新的本地视图；点击保存视图后会创建新记录。');},advancedBody);
    button('导出 Case 草稿',()=>{
      if(!state||!source)throw Error('请先选择固定数据源');
      const raw=S.bounded({format:'demiflow_case_draft_v1',project,source,state},2*1024*1024);
      const url=URL.createObjectURL(new Blob([raw],{type:'application/json'})),anchor=$('a');anchor.href=url;anchor.download='case-draft.json';anchor.click();setTimeout(()=>URL.revokeObjectURL(url),1000);
    },advancedBody);
    const imported=$('input');imported.type='file';imported.accept='.json,application/json';imported.hidden=true;toolbar.append(imported);
    button('导入 Case 草稿',()=>imported.click(),advancedBody);
    imported.onchange=async()=>{try{
      const file=imported.files[0];if(!file)return;if(file.size>2*1024*1024)throw Error('Case 草稿文件超过 2 MiB');
      const value=JSON.parse(await file.text());S.bounded(value,2*1024*1024);
      if(value.format!=='demiflow_case_draft_v1'||value.project!==project||!source||value.source?.digest!==source.digest)throw Error('请先在原项目选择草稿对应的固定来源；导入不注册新数据源');
      if(state.view_touched||state.answers.some(item=>item.dirty))throw Error('当前来源有未保存修改，请先保存、导出或明确放弃，再导入另一份草稿');
      const next=checkedDraft(value.state);if(!flush())return;invalidate();state=S.clone(next);discarded=false;page=null;row=null;configurationChanged=false;changed();paintRetainedView();paintSettings();paintPage();await load(0);info('已导入 Case 配置和未保存答案；未执行代码，也未自动写入评测记录。');
    }catch(error){info(error.message,true);}finally{imported.value='';}};
    const discard=button('放弃本来源草稿',()=>{
      try{store.removeCaseDraft(project,source.digest);}catch{ /* an unknown old envelope is preserved */ }
      invalidate();discarded=true;captureBlocked=false;state=defaults(source);row=null;page=null;pageBasis=null;clearViewConflict();paintRetainedView();paintSettings();paintPage();discard.hidden=true;info('已按你的选择放弃本来源未保存的视图与标注草稿；服务器视图、标注历史和其他来源草稿保留。');
    },advancedBody);discard.hidden=true;
    const mediaController=fieldMedia&&globalThis.ForgeCaseMedia?ForgeCaseMedia.create({project,store,adapter:fieldMedia,permissions:()=>connected?permissions:{},
      renderField:options=>ForgeAnalysisRender.field({density:'compact',tools:'menu',...options}),resolveMedia,pendingRoot:mediaPending,isCurrent:()=>!disposed&&active&&root.isConnected,
      onNotice:(message,error,fixed)=>{if(!fixed||source?.digest===fixed.source_snapshot&&row?._rowid===fixed.case_key)info(message,error);},
      onEvidence:()=>{for(const view of evidenceViews){if(view.element.isConnected)view.paint();else evidenceViews.delete(view);}}}):null;
    function defaults(snapshot){
      return {id:uid(),view_id:null,revision:0,base:null,view_touched:false,current:null,offset:0,query_scope:'full',cursors:[null],page_index:0,answers:[],descriptor:{version:1,source_snapshot:snapshot.digest,case_key:snapshot.case_key,
        fields:snapshot.schema.slice(0,Math.min(4,snapshot.schema.length)).map((field,index)=>({id:'f'+index,field:field.name,label:field.name,role:index===0?'input':index===1?'output':'context',format:field.type==='utf8'?'text':'json'})),
        grouping:[],filters:null,sort:[],layout:'card',questions:[{id:'quality',title:'结果是否符合预期',type:'choice',required:true,options:[{id:'yes',label:'符合'},{id:'no',label:'不符合'},{id:'unsure',label:'待定'}]}]}};
    }
    function checkedPresentation(value,snapshot){
      // Browser metadata preserves the server-issued reference; it is never
      // evidence of registration by itself. Records._origin rechecks each save.
      if(!value||value.registered!==true||value.project!==project||value.source_snapshot!==snapshot.digest||value.origin?.kind!=='notebook_output'||!(/^no[a-f0-9]{64}$/).test(value.origin.reference))return null;
      try{exact(value.origin,['kind','reference']);validateDescriptor(value.descriptor,snapshot);}catch{return null;}
      return value;
    }
    function adoptedPresentation(incoming,descriptor,snapshot){
      if(incoming.annotation_origin_registered!==true)return null;
      exact(incoming.origin,['kind','reference']);
      if(incoming.origin.kind!=='notebook_output'||!(/^no[a-f0-9]{64}$/).test(incoming.origin.reference))throw Error('Notebook 评测来源必须是服务端登记的精确引用，不能使用标签或自行构造的来源');
      return {registered:true,project,origin:S.clone(incoming.origin),source_snapshot:snapshot.digest,descriptor:S.clone(descriptor)};
    }
    function annotationOrigin(entries){
      const definitions=new Set((state.base?.questions||[]).map(canonical));
      if(state.view_id&&state.base?.source_snapshot===source.digest&&groupInDescriptor(state.base)&&entries.every(item=>definitions.has(canonical(item.definition))))return {kind:'saved_view',reference:state.view_id+'@'+state.revision};
      const presentation=checkedPresentation(state.presentation_origin,source);
      if(presentation&&groupInDescriptor(presentation.descriptor)){const original=new Set(presentation.descriptor.questions.map(canonical));if(entries.every(item=>original.has(canonical(item.definition))))return S.clone(presentation.origin);}
      return {kind:'unsaved_view',reference:draftKey(state.id)};
    }
    function checkedDraft(value){
      S.bounded(value,256*1024);if(!value||!Array.isArray(value.answers)||value.answers.length>64)throw Error('Case 草稿结构无效');
      draftIdentity(value.id);if(value.view_id!==null)S.identity(value.view_id);if(!Number.isSafeInteger(value.revision)||value.revision<0)throw Error('Case 草稿视图版本无效');
      const draftText=(item,label,max=8192)=>{if(typeof item!=='string'||item.length>max)throw Error(label+'草稿无效');};
      const draftQuestion=question=>{
        exact(question,['id','title','type','required'],['options','minimum','maximum']);S.identity(question.id);draftText(question.title,'评测题目',256);
        if(typeof question.required!=='boolean'||!['choice','multi','rating','text'].includes(question.type))throw Error('Case 草稿评测定义无效');
        if(['choice','multi'].includes(question.type)){if(!Array.isArray(question.options)||!question.options.length||question.options.length>32)throw Error('Case 草稿选项无效');for(const option of question.options){exact(option,['id','label']);S.identity(option.id);draftText(option.label,'选项',8192);}}
        if(question.type==='rating'&&['minimum','maximum'].some(key=>typeof question[key]!=='number'&&question[key]!==''))throw Error('Case 评分草稿无效');
      };
      const draftDescriptor=descriptor=>{
        exact(descriptor,['version','source_snapshot','case_key','fields','grouping','filters','sort','layout','questions'],['variant_key','output_groups','name']);
        if('name' in descriptor)draftText(descriptor.name,'视图名称',256);
        if(![1,2].includes(descriptor.version)||descriptor.source_snapshot!==source.digest||canonical(descriptor.case_key)!==canonical(source.case_key)||!Array.isArray(descriptor.fields)||!descriptor.fields.length||descriptor.fields.length>32||!Array.isArray(descriptor.questions)||descriptor.questions.length>16)throw Error('Case 草稿结构或来源无效');
        for(const field of descriptor.fields){exact(field,['id','field','label','role','format'],['language']);S.identity(field.id);if(!source.schema.some(item=>item.name===field.field)||typeof field.label!=='string'||field.label.length>256||!Object.hasOwn(roles,field.role)||!formats.includes(field.format))throw Error('Case 草稿字段无效');}
        validateGroups(descriptor,true);for(const question of descriptor.questions)draftQuestion(question);
        if(!['card','comparison','gallery'].includes(descriptor.layout)||!Array.isArray(descriptor.grouping)||descriptor.grouping.length>4||!Array.isArray(descriptor.sort)||descriptor.sort.length>4)throw Error('Case 草稿布局/分组/排序无效');
        const known=name=>source.schema.some(field=>field.name===name&&primitiveTypes.has(field.type));
        if(descriptor.grouping.some(name=>!known(name)))throw Error('Case 草稿分组字段无效');
        for(const order of descriptor.sort){exact(order,['field','direction']);if(!known(order.field)||!['asc','desc'].includes(order.direction))throw Error('Case 草稿排序字段无效');}
        let count=0;const walk=(item,depth)=>{
          if(item===null)return;if(!item||++count>32||depth>4)throw Error('Case 筛选草稿超限');
          if(item.op==='and'||item.op==='or'){exact(item,['op','items']);if(!Array.isArray(item.items)||!item.items.length||item.items.length>16)throw Error('Case 筛选条件组无效');for(const child of item.items)walk(child,depth+1);return;}
          exact(item,['op','field','value']);if(!known(item.field)||!['eq','in','range','contains','is_null'].includes(item.op))throw Error('Case 筛选草稿字段或关系无效');
          const scalar=part=>{if(!['string','number','boolean'].includes(typeof part)||typeof part==='string'&&part.length>1024)throw Error('Case 筛选值草稿无效');};
          if(item.op==='in'){if(!Array.isArray(item.value)||!item.value.length||item.value.length>32)throw Error('Case 筛选集合草稿无效');for(const part of item.value)scalar(part);}
          else if(item.op==='range'){exact(item.value,['lower','upper']);scalar(item.value.lower);scalar(item.value.upper);}else scalar(item.value);
        };walk(descriptor.filters,0);
      };
      draftDescriptor(value.descriptor);if(value.base)draftDescriptor(value.base);value.presentation_origin=checkedPresentation(value.presentation_origin,source);if(value.retained_view){draftIdentity(value.retained_view.id);draftDescriptor(value.retained_view.descriptor);value.retained_view.presentation_origin=checkedPresentation(value.retained_view.presentation_origin,source);}
      if(value.query_scope&&!['full','sample'].includes(value.query_scope)||value.cursors&&(!Array.isArray(value.cursors)||!value.cursors.length||value.cursors.length>51)||value.page_index!==undefined&&(!Number.isInteger(value.page_index)||value.page_index<0||value.page_index>=(value.cursors?.length||1)))throw Error('Case 查询范围或页记录无效');
      if(value.cursor_client_basis!==undefined&&!(/^[a-f0-9]{64}$/).test(value.cursor_client_basis))throw Error('Case 本地选样指纹无效');
      for(const name of ['review_variant'])if(value[name]!==undefined&&value[name]!==null&&!(/^cg[a-f0-9]{64}$/).test(value[name]))throw Error('方案草稿目标无效');
      if(value.review_group!==undefined&&value.review_group!==null)draftText(value.review_group,'方案选择',96);if((value.review_variant||value.review_group)&&value.review_field)throw Error('方案与字段目标不能混用');
      for(const name of ['review_field','output_field'])if(value[name]!==undefined&&value[name]!==null&&!source.schema.some(field=>field.name===value[name]))throw Error('Case 输出字段选择不属于固定来源');
      for(const entry of value.answers){if(!entry||typeof entry.case_key!=='string'||!(/^(0|[1-9][0-9]{0,19})$/).test(entry.case_key)||BigInt(entry.case_key)>=2n**64n||!(/^[a-f0-9]{64}$/).test(entry.digest)||!Number.isInteger(entry.expected_revision)||entry.expected_revision<0||entry.expected_revision>100||typeof entry.dirty!=='boolean')throw Error('Case 标注草稿无效');draftQuestion(entry.definition);if(entry.target_label!==undefined&&entry.target_label!==null)text(entry.target_label,'方案草稿标题',256,true);if(entry.target_variant!==undefined&&entry.target_variant!==null&&(!(/^cg[a-f0-9]{64}$/).test(entry.target_variant)||entry.target_field!==undefined&&entry.target_field!==null))throw Error('答案草稿方案目标无效');if(entry.target_field!==undefined&&entry.target_field!==null&&!source.schema.some(field=>field.name===entry.target_field))throw Error('答案草稿的目标字段不属于固定来源');if(entry.media_basis!==undefined)checkedMediaBasis(entry.media_basis,entry.case_key);}
      if(value.review_filter)checkReviewFilter(value.review_filter);for(const key of ['review_filter_label','review_filter_description'])if(value[key]!==undefined&&value[key]!==null)text(value[key],'筛选说明',1024,true);if(value.review_filter_form)S.bounded(value.review_filter_form,4096);
      return value;
    }
    async function useSource(){
      if(!active||!candidate||!connected||reading)return;
      if(locator&&canonical(locator)===canonical(candidate.locator)&&!sourceTransition)return;
      if(!flush())return;const version=ticket,choice=sourceTicket,chosen=S.clone(candidate);reading=true;publishContext('loading');info('正在核对固定数据源…');
      try{
        let snapshot;try{snapshot=await readBody('source-register',{locator:chosen.locator});}catch(error){if(!live(version)||choice!==sourceTicket)return;throw error;}
        if(!live(version)||choice!==sourceTicket||canonical(chosen.locator)!==canonical(candidate?.locator))return;
        const next=checkedDraftFor(snapshot,store.caseDraft(project,snapshot.digest)||defaults(snapshot));
        source=snapshot;locator=chosen.locator;state=next;discarded=false;sourceTransition=false;clearViewConflict();unavailableReason=null;
        labelSource(chosen.label,snapshot);
        page=null;row=null;pageBasis=null;configurationChanged=false;changed();paintRetainedView();paintSettings();paintPage();paintPending();
        await refreshViews();if(live(version)&&!sourceTransition)await restorePage();
      }catch(error){if(live(version)&&choice===sourceTicket)publishContext('unavailable');throw error;}
      finally{reading=false;if(active&&!disposed&&candidate&&(choice!==sourceTicket||version!==ticket)){const next=ticket;useSource().catch(error=>{if(live(next))info(error.message,true);});}}
    }
    function checkedDraftFor(snapshot,value){const previous=source;source=snapshot;try{return checkedDraft(value);}finally{source=previous;}}
    async function refreshViews(){
      const version=ticket,draft=state,value=await read('documents',{kind:'case_view',limit:32});if(!current(version,draft))return;
      picker.replaceChildren();const empty=$('option','本地视图草稿');empty.value='';picker.append(empty);
      for(const [index,item]of value.items.entries()){const label=item.name||(item.id===state?.view_id?viewName(state.descriptor):'Case 视图 '+(index+1));const option=$('option',label+' · v'+item.revision);option.value=item.id;option.title=item.id+' · v'+item.revision;picker.append(option);}picker.value=state?.view_id||'';picker.title=state?.view_id?'视图 '+state.view_id+' · v'+state.revision:'本地视图草稿';
    }
    async function openView(identifier,revision){
      if(state?.view_touched)throw Error('当前视图有未保存配置，请先保存、导出或恢复已存视图，再选择其他视图');
      if(!flush())return false;invalidate();sourceTicket++;candidate=null;const version=ticket,previousState=state;
      const saved=await read('view',{view_id:identifier,...(revision?{revision}:{})});if(!live(version))return false;
      const snapshot=await read('source',{digest:saved.document.source_snapshot});if(!live(version))return false;
      validateDescriptor(saved.document,snapshot);
      const next=checkedDraftFor(snapshot,store.caseDraft(project,snapshot.digest)||defaults(snapshot));
      if(next.view_touched&&canonical(next.descriptor)!==canonical(saved.document))throw Error('目标来源有未保存视图草稿，已保留；请先回到该来源处理草稿');
      if(!flush()||state!==previousState)return false;
      if(source?.digest!==snapshot.digest)locator=null;
      source=snapshot;state=next;candidate=null;sourceTransition=false;clearViewConflict();unavailableReason=null;
      state.view_id=saved.id;state.revision=saved.revision;state.presentation_origin=null;state.base=S.clone(saved.document);state.descriptor=S.clone(saved.document);state.view_touched=false;state.current=null;
      labelSource(viewName(saved.document)+' · v'+saved.revision,source);page=null;row=null;pageBasis=null;configurationChanged=false;changed();paintRetainedView();paintSettings();paintPage();await refreshViews();await load(0);return true;
    }
    async function restoreView(){
      if(!state?.view_id||!state.base)throw Error('当前没有已保存的视图版本；可复制为新视图或导出草稿');
      // This explicit action discards view edits only. Answer drafts stay keyed by definition.
      invalidate();state.descriptor=S.clone(state.base);state.option_texts={};state.view_touched=false;configurationChanged=false;clearViewConflict();changed();paintSettings();paintPage();await load(0);info('已恢复视图 v'+state.revision+'；各定义下的答案草稿保留。');
    }
    picker.onchange=async()=>{const identifier=picker.value;try{if(identifier)await openView(identifier);}catch(error){picker.value=state?.view_id||'';if(!disposed)info(error.message,true);}};
    function paintMaterialDraft(){
      review.replaceChildren($('h3','评测与标注'),$('p','材料或方案配置已修改。请应用配置，原范围下的答案与草稿保留。','case-ui-stale'));
      button('应用材料与方案配置',()=>{validateDescriptor(state.descriptor,source,true);return load(0);},review,!connected);
    }
    function materialConfigurationChanged(){
      state.view_touched=true;materialDraftPending=true;pageTicket++;reviewTicket++;headsReady=false;verifiedReview=null;currentGroupBindings=[];if(state.review_group)state.review_variant=null;changed();paintOutputSelection();paintMaterialDraft();
    }
    function paintGroupSettings(parent){
      const descriptor=state.descriptor,panel=$('section',undefined,'case-ui-group-config');parent.append(panel);panel.append($('h3','输出方案'),$('p','将图片、回答和已有模型评测组合为方案。保存配置后才可按已登记方案评测；未分组输出仍单独显示。','muted small'));
      if(features.case_output_groups!==true)panel.append($('p','当前服务尚未开放方案登记；原字段视图与答案保持可用。','muted small'));
      const groups=descriptor.output_groups||[];
      groups.forEach((group,index)=>{
        const box=$('fieldset',undefined,'case-ui-group-config-item');box.append($('legend','方案 '+(index+1)));panel.append(box);
        const name=$('label','方案名称','case-ui-group-name'),advanced=$('details',undefined,'case-ui-group-identity');box.append(name,advanced);advanced.append($('summary','高级：方案标识'),$('p','修改标识会建立独立评测对象，原方案记录保留。','muted small'));
        const id=input(group.id,'方案 ID '+(index+1),advanced,96),label=input(group.label,'方案标题 '+(index+1),name,256);
        id.oninput=()=>{const previous=group.id;group.id=id.value;if(state.review_group===previous)state.review_group=group.id;materialConfigurationChanged();};label.oninput=()=>{group.label=label.value;materialConfigurationChanged();};
        const eligible=descriptor.fields.filter(field=>['output','evaluation'].includes(field.role)),known=new Set(eligible.map(field=>field.id));
        for(const field of eligible){const choice=$('label',field.label+' · '+field.field),check=input(group.fields.includes(field.id),'方案 '+(index+1)+' 材料 '+field.id,choice,1,'checkbox');choice.prepend(check);box.append(choice);check.disabled=groups.some(other=>other!==group&&other.fields.includes(field.id));check.onchange=()=>{if(check.checked){if(groups.reduce((n,item)=>n+item.fields.length,0)>=32)throw Error('方案材料引用已达 32 项');group.fields.push(field.id);}else group.fields=group.fields.filter(value=>value!==field.id);materialConfigurationChanged();paintSettings();};}
        for(const missing of group.fields.filter(id=>!known.has(id))){box.append($('p','原材料 '+missing+' 已移除或改变角色，请修正方案成员。','case-ui-stale'));button('移除失效方案材料 '+missing,()=>{group.fields=group.fields.filter(id=>id!==missing);materialConfigurationChanged();paintSettings();},box);}
        button('移除输出方案',()=>{if(state.review_group===group.id&&reviewVariant())state.review_group=null;groups.splice(index,1);if(!groups.length){descriptor.version=1;delete descriptor.output_groups;}materialConfigurationChanged();paintSettings();},box);
      });
      button('添加输出方案',()=>{if(features.case_output_groups!==true)throw Error('当前服务尚未开放方案登记');const used=new Set(groups.flatMap(group=>group.fields)),field=descriptor.fields.find(field=>field.role==='output'&&!used.has(field.id));if(!field)throw Error('请先添加未分组的输出材料');descriptor.version=2;descriptor.output_groups=groups;groups.push({id:uid(),label:'新输出方案',fields:[field.id]});materialConfigurationChanged();paintSettings();},panel,features.case_output_groups!==true||groups.length>=8);
      button('应用材料与方案配置',()=>{validateDescriptor(descriptor,source,true);return load(0);},panel,!connected);
    }
    function paintSettings(){
      settings.replaceChildren($('summary','材料与布局 · 评测题目 · 筛选与排序'));
      if(!source||!state){settings.append($('p','选择数据源后可配置输入、输出、预览格式及标注项。','muted'));return;}
      const descriptor=state.descriptor,tabs=$('nav',undefined,'case-ui-config-tabs'),panels={};tabs.setAttribute('aria-label','视图配置分类');settings.append(tabs);
      const naming=$('label','视图名称','case-ui-view-name'),name=input(descriptor.name||'','视图名称',naming,256);name.placeholder=viewName(descriptor);settings.insertBefore(naming,tabs);name.oninput=()=>{if(name.value.trim())descriptor.name=name.value.trim();else delete descriptor.name;state.view_touched=true;changed();};
      for(const [id,label]of [['fields','材料与布局'],['questions','评测题目'],['filters','筛选与排序']]){const tab=button(label,()=>{configTab=id;paintSettings();},tabs);tab.setAttribute('aria-pressed',String(configTab===id));const panel=$('div',undefined,'case-ui-config-panel');panel.hidden=configTab!==id;panels[id]=panel;settings.append(panel);}
      const fieldsPanel=panels.fields,questionsPanel=panels.questions;
      panels.filters.append($('p','分组、筛选、排序与选样在左栏「配置选样条件」中编辑；条件只作用于已固定来源，修改后点击应用读取。','muted small'));
      panels.filters.append($('p','当前分组：'+(descriptor.grouping.length?descriptor.grouping.map(columnLabel).join(' / '):'无')+' · 排序 '+descriptor.sort.length+' 项 · '+(descriptor.filters?'已设筛选':'无筛选'),'muted small'));
      button('打开左栏选样条件',()=>{const panel=rail.querySelector('.case-ui-query-controls');if(panel){panel.open=true;panel.scrollIntoView({block:'start'});}else rail.scrollIntoView({block:'start'});},panels.filters);

      fieldsPanel.append($('p','字段角色决定材料位置，预览格式决定显示方式，评测题目独立定义。视图和答案分别保存；更改定义后旧答案仍按原题保留。','muted small'));
      const fields=$('div',undefined,'analysis-field-config');fieldsPanel.append($('h3','展示字段'),fields);
      descriptor.fields.forEach((field,index)=>{
        const line=$('div',undefined,'analysis-config-row');fields.append(line);line.setAttribute('role','group');line.setAttribute('aria-label','材料字段 '+(index+1));
        const binding=select(source.schema.map(item=>[item.name,item.name]),field.field,'展示字段 '+(index+1),line);
        const label=input(field.label,'字段标题 '+(index+1),line);
        const role=select(Object.entries(roles),field.role,'字段角色 '+(index+1),line);
        const format=select(formats.map(item=>[item,item]),field.format,'预览格式 '+(index+1),line);
        const sampleRow=row||page?.rows?.[0],sampleValue=sampleRow?sampleRow[field.field]:undefined;
        if(sampleValue!==undefined&&sampleValue!==null)line.append($('small','样例：'+(typeof sampleValue==='string'?sampleValue:JSON.stringify(sampleValue)).replace(/\s+/g,' ').slice(0,80),'case-ui-field-sample muted'));
        for(const [element,key]of [[binding,'field'],[label,'label'],[role,'role'],[format,'format']])element.oninput=()=>{field[key]=element.value;if(key==='format'&&field.format!=='code')delete field.language;if(descriptor.version===2)materialConfigurationChanged();else{state.view_touched=true;pageTicket++;changed();paintContent();}if(key==='format'||descriptor.version===2&&['field','role'].includes(key))paintSettings();};
        if(field.format==='code'){const language=select([['python','Python'],['sql','SQL'],['json','JSON'],['javascript','JavaScript'],['text','原文']],field.language||'python','代码语言 '+(index+1),line);language.onchange=()=>{field.language=language.value;if(descriptor.version===2)materialConfigurationChanged();else{state.view_touched=true;changed();paintContent();}};}
        button('移除字段',()=>{descriptor.fields.splice(index,1);if(descriptor.version===2)materialConfigurationChanged();else{state.view_touched=true;changed();paintContent();}paintSettings();},line,descriptor.fields.length<=1);
      });
      button('添加字段',()=>{if(descriptor.fields.length>=32)throw Error('视图最多 32 个展示组件；一次查询最多读取 16 个不同字段');const item=source.schema.find(value=>!descriptor.fields.some(field=>field.field===value.name))||source.schema[0];if(!item)return;descriptor.fields.push({id:uid(),field:item.name,label:item.name,role:'context',format:'json'});if(descriptor.version===2)materialConfigurationChanged();else{state.view_touched=true;changed();}paintSettings();},fields,descriptor.fields.length>=32);
      const layout=select([['card','单条输出'],['comparison','输出方案与字段对照'],['gallery','当前页画廊']],descriptor.layout,'Case 布局',fieldsPanel);layout.onchange=()=>{descriptor.layout=layout.value;state.view_touched=true;changed();paintContent();};
      paintGroupSettings(fieldsPanel);
      questionsPanel.append($('h3','评测定义'));
      descriptor.questions.forEach((question,index)=>{
        const box=$('div',undefined,'analysis-question-config');questionsPanel.append(box);
        const title=input(question.title,'评测题目 '+(index+1),box);
        const type=select([['choice','单选'],['multi','多选'],['rating','评分'],['text','文本']],question.type,'评测类型 '+(index+1),box);
        const required=$('label','必填 ');box.append(required);const requiredInput=input(question.required,'评测必填 '+(index+1),required,1,'checkbox');
        const update=()=>{state.view_touched=true;configurationChanged=true;headsReady=false;reviewTicket++;pageTicket++;review.replaceChildren($('h3','评测与标注'),$('p','评测定义已修改。请应用当前定义，旧答案和草稿保留。','muted'));changed();};
        title.oninput=()=>{question.title=title.value;update();};requiredInput.onchange=()=>{question.required=requiredInput.checked;update();};
        type.onchange=()=>{if(state.option_texts)delete state.option_texts[question.id];question.type=type.value;delete question.options;delete question.minimum;delete question.maximum;
          if(['choice','multi'].includes(question.type))question.options=[{id:'yes',label:'是'},{id:'no',label:'否'}];
          if(question.type==='rating'){question.minimum=1;question.maximum=5;}update();paintSettings();};
        if(['choice','multi'].includes(question.type)){
          const options=$('textarea');options.rows=3;options.maxLength=8192;options.setAttribute('aria-label','评测选项 '+(index+1));options.value=state.option_texts?.[question.id]??question.options.map(item=>item.label).join('\n');box.append(options);
          options.oninput=()=>{const labels=options.value.split('\n');if(labels.length>32){state.option_texts??={};state.option_texts[question.id]=options.value;update();info('评测选项超过 32 个，原输入已保留；请减少选项后再应用或保存。',true);return;}if(state.option_texts)delete state.option_texts[question.id];const previous=question.options;question.options=labels.map((label,i)=>({id:previous[i]?.id||'o'+i,label}));update();};
        }
        if(question.type==='rating')for(const key of ['minimum','maximum']){const number=input(question[key],(key==='minimum'?'最低分 ':'最高分 ')+(index+1),box,6,'number');number.oninput=()=>{question[key]=number.value===''?'':Number(number.value);update();};}
        button('移除评测项',()=>{if(state.option_texts)delete state.option_texts[question.id];descriptor.questions.splice(index,1);update();paintSettings();},box);
      });
      button('添加评测项',()=>{if(descriptor.questions.length>=16)throw Error('当前界面最多配置 16 个评测项');descriptor.questions.push({id:uid(),title:'新评测项',type:'text',required:false});state.view_touched=true;configurationChanged=true;headsReady=false;changed();paintSettings();},questionsPanel,descriptor.questions.length>=16);
      button('应用评测定义',async()=>{if(Object.keys(state.option_texts||{}).length)throw Error('评测选项草稿超过上限，请先修正');for(const question of descriptor.questions)validateQuestion(question);configurationChanged=false;await loadReview();info('已切换到当前评测定义；其他定义的答案与草稿保留，未自动迁移。');},questionsPanel);
    }
    function query(){
      const descriptor=state.descriptor;
      return {kind:'cases',columns:[...new Set(descriptor.fields.map(item=>item.field))],grouping:descriptor.grouping,filters:descriptor.filters,sort:descriptor.sort,scope:state.query_scope||'full',limit:20};
    }
    function checkReviewFilter(value){
      exact(value,['variant','field','definitions','match'],['option']);if(value.variant!==null&&(!(/^cg[a-f0-9]{64}$/).test(value.variant)||value.field!==null)||value.field!==null&&!source.schema.some(field=>field.name===value.field))throw Error('标注状态筛选目标无效');
      if(!Array.isArray(value.definitions)||!value.definitions.length||value.definitions.length>16||new Set(value.definitions).size!==value.definitions.length||value.definitions.some(id=>!(/^[a-f0-9]{64}$/).test(id))||!['unanswered','answered_any','answered_all','option'].includes(value.match))throw Error('标注状态筛选的题目或关系无效');
      if(value.match==='option'){exact(value.option,['definition','value']);if(!value.definitions.includes(value.option.definition))throw Error('选项筛选题目不在当前题目范围');S.identity(value.option.value);}else if(Object.hasOwn(value,'option'))throw Error('此标注状态筛选不能携带选项');return value;
    }
    function paintReviewFilter(){
      const panel=rail.querySelector('.case-ui-review-filter');if(!panel||!state)return;panel.replaceChildren($('h4','标注状态'));
      const form=state.review_filter_form||{},questions=state.descriptor.questions,available=features.case_review_filter===true&&questions.length>0;
      const choices=[['case','整个 Case'],...state.descriptor.fields.filter(field=>field.role==='output').map(field=>['field:'+field.field,'字段 · '+field.label]),...(state.descriptor.output_groups||[]).map(group=>['group:'+group.id,'方案 · '+group.label])];
      const unique=[...new Map(choices.map(item=>[item[0],item])).values()],scope=select(unique,form.target||'case','标注筛选对象',panel);
      const match=select([['','全部（不筛标注）'],['unanswered','未标注'],['answered_any','任一题已标注'],['answered_all','全部题已标注'],['option','指定题目选项']],form.match||'','标注状态条件',panel);
      const optionQuestions=questions.filter(question=>['choice','multi'].includes(question.type));let questionChoice=null,optionChoice=null;
      const saveForm=()=>{state.review_filter_form={target:scope.value,match:match.value,...(questionChoice?{question:questionChoice.value,option:optionChoice?.value||''}:{})};changed();};
      scope.onchange=saveForm;match.onchange=()=>{saveForm();paintReviewFilter();};
      if(match.value==='option'){
        questionChoice=select(optionQuestions.map(question=>[question.id,question.title]),form.question||optionQuestions[0]?.id,'筛选评测题目',panel);
        const definition=optionQuestions.find(question=>question.id===questionChoice.value);optionChoice=select((definition?.options||[]).map(option=>[option.id,option.label]),form.option||definition?.options[0]?.id,'筛选评测选项',panel);
        questionChoice.onchange=()=>{saveForm();delete state.review_filter_form.option;paintReviewFilter();};optionChoice.onchange=saveForm;
      }
      for(const field of [scope,match,questionChoice,optionChoice].filter(Boolean))field.disabled=!available;
      panel.append($('p',available?'仅当前服务端用户 · 当前 '+questions.length+' 题；本地未保存草稿不计入已标注。':features.case_review_filter===true?'配置评测题目后可按标注状态筛选。':'当前服务尚未开放全来源标注状态筛选。','muted small'));
      if(state.review_filter){const known=new Set(activeQuestions.map(item=>canonical(item.definition))),current=questions.every(question=>known.has(canonical(question)));if(current&&(state.review_filter.definitions.length!==activeQuestions.length||state.review_filter.definitions.some(digest=>!activeQuestions.some(item=>item.digest===digest))))panel.append($('p','题目定义已改变；现有筛选仍按先前定义。请重新应用或清除筛选。','case-ui-stale'));}
      if(state.review_filter)panel.append($('p',(state.review_filter.variant&&!currentGroupBindings.some(item=>item.registered&&item.variant===state.review_filter.variant)?'已生效（原登记方案）：':'已生效：')+(state.review_filter_label||'明确评测对象')+' · '+(state.review_filter_description||state.review_filter.match)+'；切换右侧评测对象不改变此筛选。快照 '+(page?.annotation_watermark??'待读取')+'，保存后需明确重新读取选样。','case-ui-review-filter-active muted small'));
      button('应用标注状态筛选',async()=>{
        if(filterApplying||!panel.isConnected||disposed||!active)return;filterApplying=true;paintReviewFilter();info('正在核对并应用标注状态筛选…');
        try{saveForm();const original=S.clone(state.review_filter_form),context=ticket,draft=state,view=S.clone(basis()),questionDefinitions=S.clone(questions);const valid=()=>current(context,draft)&&canonical(view)===canonical(basis())&&canonical(original)===canonical(state.review_filter_form);
        if(!available)throw Error('当前标注状态筛选尚不可用');if(!match.value){state.review_filter=null;state.review_filter_label=null;state.review_filter_description=null;changed();return await load(0);}
        const definitions=await Promise.all(questionDefinitions.map(async definition=>{validateQuestion(definition);return hash(definition);}));if(!valid())return;
        let variant=null,field=null;const key=original.target;
        if(key.startsWith('field:')){field=key.slice(6);if(!source.schema.some(item=>item.name===field))throw Error('筛选字段不属于固定来源');}
        else if(key.startsWith('group:')){if(!row||features.case_output_groups!==true)throw Error('请先读取 Case 并核对方案登记');const caseKey=row._rowid,value=await readBody('case-group-bindings',{digest:source.digest,case_key:caseKey,view});if(!valid()||row?._rowid!==caseKey)return;const binding=checkBindings(value,caseKey,{view}).find(item=>item.id===key.slice(6));if(!binding?.registered)throw Error('此筛选方案尚未登记，请先明确保存配置');variant=binding.variant;}
        else if(key!=='case')throw Error('请明确选择筛选评测对象');
        const filter={variant,field,definitions,match:original.match};
        if(filter.match==='option'){const index=questionDefinitions.findIndex(question=>question.id===original.question),definition=questionDefinitions[index];if(!definition||!['choice','multi'].includes(definition.type)||!definition.options.some(option=>option.id===original.option))throw Error('请选择实际评测题目和选项');filter.option={definition:definitions[index],value:original.option};}
        checkReviewFilter(filter);if(!valid())return;state.review_filter=filter;state.review_filter_label=unique.find(([value])=>value===key)?.[1]||key;let description=match.selectedOptions[0]?.textContent||filter.match;if(filter.option){const question=questionDefinitions[definitions.indexOf(filter.option.definition)];description+=' · '+question.title+' = '+question.options.find(item=>item.id===filter.option.value).label;}state.review_filter_description=[...description].slice(0,160).join('');changed();await load(0);
        }finally{filterApplying=false;if(!disposed&&active)paintReviewFilter();}
      },panel,!available||filterApplying);
      if(state.review_filter)button('清除标注状态筛选',()=>{state.review_filter=null;state.review_filter_label=null;state.review_filter_description=null;state.review_filter_form={target:form.target||'case',match:''};changed();return load(0);},panel);
    }
    async function load(cursor=0,direction='reset'){
      const reportEpoch=ticket;
      try{
      if(!source||!state)throw Error('请先从数据目录选择固定版本表');if(sourceTransition)throw Error('请先完成新来源核对；原来源草稿已保留');
      if(['next','back'].includes(direction)&&!pageIsCurrent())throw Error('视图或查询已变化，请先重新读取 Case；当前答案草稿已保留');
      if(!flush())return false;if(Object.keys(state.filter_texts||{}).length)throw Error('筛选集合草稿超过上限，请先修正');if(Object.keys(state.option_texts||{}).length)throw Error('评测选项草稿超过上限，请先修正');validateDescriptor(state.descriptor,source,true);if(source.case_key.kind!=='row_id')throw Error('此来源需要可验证的稳定业务键读取契约；当前工作台只开放固定 row_id 来源');
      const version=ticket,request=++pageTicket,draft=state,sourceDigest=source.digest;
      const view=S.clone(basis()),submittedQuery=S.clone(query()),submittedReview=state.review_filter?S.clone(checkReviewFilter(state.review_filter)):null,identity=canonical(queryBasis(view,submittedQuery,submittedReview));
      if(submittedReview){if(features.case_review_filter!==true)throw Error('当前服务不支持已保留的标注状态筛选；请明确清除筛选后读取');const definitions=await Promise.all(state.descriptor.questions.map(hash));if(!current(version,draft)||request!==pageTicket)return false;if(definitions.length!==submittedReview.definitions.length||submittedReview.definitions.some(id=>!definitions.includes(id)))throw Error('标注状态筛选仍绑定原题目定义；请明确重新应用或清除筛选');}
      info('正在读取固定来源的 Case…');headsReady=false;reviewTicket++;
      let value;try{value=await readBody('case-page',{digest:sourceDigest,query:submittedQuery,view,...(submittedReview?{review:submittedReview}:{}),...(cursor?{cursor}:{})});}catch(error){if(!current(version,draft)||request!==pageTicket)return false;if(['not_found','conflict','operation_rejected','operation_failed'].includes(error.code)&&direction==='reset'){unavailableReason=error.message;publishContext('unavailable');if(!row&&state.current)row={_rowid:state.current};paintPage();if(row)await loadReview();}else if(error.code==='resource_limit'){paintPage();}throw error;}
      if(!current(version,draft)||request!==pageTicket)return false;
      if(identity!==canonical(queryBasis())){info('读取期间配置已变化，已保留当前草稿；请重新读取 Case。',true);return false;}
      if(value.source_snapshot!==sourceDigest||value.kind!=='cases'||!Array.isArray(value.rows)||value.rows.length>20||canonical(value.columns)!==canonical(submittedQuery.columns)||!value.coverage)throw Error('Case 响应与固定来源或字段投影不一致');
      // Service digests use its versioned canonical encoder. In particular,
      // Python and JSON.stringify spell exponents differently. Treat issued
      // digests as opaque cursor bindings; keep a separate local fingerprint.
      const clientBasis=await hash(queryBasis(view,submittedQuery)),expectedView=value.view_digest,expectedQuery=value.query_digest;
      if(!current(version,draft)||request!==pageTicket||identity!==canonical(queryBasis()))return false;
      if(!(/^[a-f0-9]{64}$/).test(expectedView)||!(/^[a-f0-9]{64}$/).test(expectedQuery)||!Number.isSafeInteger(value.annotation_watermark)||value.annotation_watermark<0||!(/^[a-f0-9]{64}$/).test(value.scope_digest))throw Error('Case 响应缺少匹配的视图、查询或标注快照依据');
      if(submittedReview){exact(value.review,['filter','principal_scope','annotation_watermark','selection_digest']);if(canonical(value.review.filter)!==canonical(submittedReview)||value.review.annotation_watermark!==value.annotation_watermark||!(/^[a-f0-9]{64}$/).test(value.review.principal_scope)||!(/^[a-f0-9]{64}$/).test(value.review.selection_digest))throw Error('标注状态筛选响应与目标、题目或快照不一致');}else if(value.review!==undefined)throw Error('服务端返回了未请求的标注状态筛选');
      if(submittedQuery.scope==='full'&&value.coverage.complete!==true)throw Error('全集查询未完成，已拒绝把部分结果当作全集；可明确选择样本后重新读取');
      if(value.coverage.requested_scope!==submittedQuery.scope)throw Error('Case 查询返回了不同的样本范围');
      if(cursor&&(cursor.source_snapshot!==sourceDigest||cursor.view_digest!==expectedView||cursor.query_digest!==expectedQuery||cursor.scope_digest!==value.scope_digest||cursor.annotation_watermark!==value.annotation_watermark||cursor.offset!==value.offset))throw Error('Case 翻页快照已变化，请显式重新读取选样');
      if(value.next_cursor!==null){const next=value.next_cursor;if(!next||next.format!=='demiflow_case_cursor_v1'||next.source_snapshot!==sourceDigest||next.view_digest!==expectedView||next.query_digest!==expectedQuery||next.scope_digest!==value.scope_digest||next.annotation_watermark!==value.annotation_watermark||!Number.isSafeInteger(next.offset)||next.offset<=value.offset)throw Error('Case 下一页游标依据无效');}
      const keys=new Set();for(const item of value.rows){if(typeof item?._rowid!=='string'||!(/^(0|[1-9][0-9]{0,19})$/).test(item._rowid)||BigInt(item._rowid)>=2n**64n||keys.has(item._rowid))throw Error('Case 稳定行键缺失或重复');keys.add(item._rowid);}
      if(direction==='next'){state.cursors=(state.cursors||[null]).slice(0,(state.page_index||0)+1);if(state.cursors.length>=51)throw Error('Case 翻页记录超出当前范围预算，请重新读取');state.cursors.push(cursor);state.page_index=(state.page_index||0)+1;}
      else if(direction==='back')state.page_index=Math.max(0,(state.page_index||0)-1);
      else if(direction==='restore'){/* Keep the saved cursor chain and page index. */}
      else{state.cursors=[null];state.page_index=0;}
      unavailableReason=null;page=value;pageBasis=identity;state.cursor_client_basis=clientBasis;state.offset=value.offset;row=value.rows.find(item=>item._rowid===state.current)||(direction==='reset'&&row?._rowid===state.current?row:null)||value.rows[0]||null;state.current=row?._rowid||null;
      materialDraftPending=false;changed();paintPage();if(row)await loadReview();
      if(current(version,draft)&&request===pageTicket)info('已读取'+(submittedQuery.scope==='full'?'预算内完整来源':'明确选择的样本')+'；分页与保存下一条保持本次选样顺序。');return true;
      }catch(error){if(live(reportEpoch))info(error.message,true);throw error;}
    }
    async function restorePage(){
      const context=ticket,draft=state,index=state.page_index||0,cursor=state.cursors?.[index];
      if(index>0&&cursor){
        const view=S.clone(basis()),submittedQuery=S.clone(query()),identity=canonical(queryBasis(view,submittedQuery)),clientBasis=await hash(JSON.parse(identity));
        if(!current(context,draft))return false;
        if(identity!==canonical(queryBasis())){info('恢复期间配置已变化，请重新读取 Case；原答案草稿保留。');return false;}
        if(cursor.source_snapshot===source.digest&&state.cursor_client_basis===clientBasis)return load(cursor,'restore');
        const loaded=await load(0);if(current(context,draft))info('上次页游标与当前视图或查询不一致，已重新读取首个选样页；原答案草稿保留。');return loaded;
      }
      return load(0);
    }
    function primitive(){return source.schema.filter(field=>primitiveTypes.has(field.type));}
    function columnLabel(name){const field=state?.descriptor.fields.find(item=>item.field===name);return field&&field.label!==name?field.label+' · '+name:name;}
    function defaultPredicate(){const field=primitive()[0];if(!field)throw Error('当前来源没有可筛选的基本类型字段');return {op:'eq',field:field.name,value:field.type==='bool'?true:['int64','float64'].includes(field.type)?0:''};}
    function filterCount(item){return item?1+(item.items||[]).reduce((count,child)=>count+filterCount(child),0):0;}
    function filterDepth(item){return item?.items?1+Math.max(...item.items.map(filterDepth)):0;}
    function draftScalar(raw,name){const kind=source.schema.find(field=>field.name===name)?.type;if(kind==='bool')return raw==='true'?true:raw==='false'?false:raw;if(['int64','float64'].includes(kind)&&raw.trim()&&Number.isFinite(Number(raw)))return Number(raw);return raw;}
    function editQuery(){state.view_touched=true;pageTicket++;changed();}
    function paintFilters(parent){
      const descriptor=state.descriptor,available=primitive();
      const label=(name,path)=>name+(path?' '+path:'');
      function paint(item,host,path,depth,remove){
        const box=$('fieldset',undefined,'case-ui-filter');box.append($('legend',item.items?'条件组':'条件'));host.append(box);
        const compound=!!item.items;
        if(compound){
          const relation=select([['and','全部满足（且）'],['or','任一满足（或）']],item.op,label('筛选组合',path),box);relation.onchange=()=>{item.op=relation.value;editQuery();};
          item.items.forEach((child,index)=>paint(child,box,path?path+'.'+(index+1):String(index+1),depth+1,()=>{item.items.splice(index,1);if(!item.items.length)remove();else{editQuery();paintPage();}}));
          button('添加组内条件',()=>{if(filterCount(descriptor.filters)>=32||item.items.length>=16)throw Error('筛选节点已达上限');item.items.push(defaultPredicate());editQuery();paintPage();},box,depth>=4||item.items.length>=16);
          button('添加嵌套条件组',()=>{if(filterCount(descriptor.filters)>30||item.items.length>=16)throw Error('筛选节点已达上限');item.items.push({op:'and',items:[defaultPredicate()]});editQuery();paintPage();},box,depth>=3||item.items.length>=16);
        }else{
          const binding=select(available.map(field=>[field.name,columnLabel(field.name)+' · '+field.type]),item.field,label('筛选字段',path),box);
          const kind=source.schema.find(field=>field.name===item.field)?.type;
          const ops=[['eq','等于'],['in','属于（多个值）'],...(kind==='utf8'?[['contains','包含']]:[]),...(['int64','float64'].includes(kind)?[['range','数值范围']]:[]),['is_null','为空/非空']];
          const op=select(ops,item.op,label('筛选关系',path),box);
          binding.onchange=()=>{item.field=binding.value;state.filter_texts={};item.op='eq';const type=source.schema.find(field=>field.name===item.field)?.type;item.value=type==='bool'?true:['int64','float64'].includes(type)?0:'';editQuery();paintPage();};
          op.onchange=()=>{item.op=op.value;state.filter_texts={};const initial=kind==='bool'?true:['int64','float64'].includes(kind)?0:'';item.value=item.op==='is_null'?true:item.op==='in'?[initial]:item.op==='range'?{lower:0,upper:1}:initial;editQuery();paintPage();};
          if(item.op==='is_null'){
            const value=select([['true','为空'],['false','非空']],String(item.value),label('筛选值',path),box);value.onchange=()=>{item.value=value.value==='true';editQuery();};
          }else if(item.op==='in'){
            const values=$('textarea');values.rows=3;values.maxLength=32768;values.setAttribute('aria-label',label('筛选值集合',path));values.placeholder='每行一个值，最多 32 个';values.value=state.filter_texts?.[path||'root']??(Array.isArray(item.value)?item.value.join('\n'):String(item.value));box.append(values);
            values.oninput=()=>{const parts=values.value.split('\n');if(parts.length>32){state.filter_texts??={};state.filter_texts[path||'root']=values.value;editQuery();info('筛选集合超过 32 个值，原输入已保留；请减少后再读取或保存。',true);return;}if(state.filter_texts)delete state.filter_texts[path||'root'];item.value=parts.map(raw=>draftScalar(raw,item.field));editQuery();};
          }else if(item.op==='range'){
            for(const key of ['lower','upper']){const value=input(item.value?.[key]??'',label(key==='lower'?'筛选下限':'筛选上限',path),box,1024);value.inputMode='decimal';value.oninput=()=>{item.value[key]=draftScalar(value.value,item.field);editQuery();};}
          }else if(kind==='bool'){
            const value=select([['true','true'],['false','false']],String(item.value),label('筛选值',path),box);value.onchange=()=>{item.value=value.value==='true';editQuery();};
          }else{const value=input(item.value??'',label('筛选值',path),box,1024);value.placeholder=kind==='utf8'?'文本（可为空字符串）':'有限数值';value.oninput=()=>{item.value=draftScalar(value.value,item.field);editQuery();};}
        }
        button('移除此条件',remove,box);
      }
      if(descriptor.filters)paint(descriptor.filters,parent,'',0,()=>{descriptor.filters=null;state.filter_texts={};editQuery();paintPage();});
      button('添加筛选条件',()=>{if(filterCount(descriptor.filters)>=31||descriptor.filters&&filterDepth(descriptor.filters)>=4)throw Error('筛选已达节点或深度上限，请在现有条件组内添加');const next=defaultPredicate();descriptor.filters=descriptor.filters?{op:'and',items:[descriptor.filters,next]}:next;editQuery();paintPage();},parent,!available.length);
    }
    function paintFilterControls(parent){
      const descriptor=state.descriptor,available=primitive();
      const quick=$('div',undefined,'case-ui-query-quick');parent.append(quick);
      const controls=$('details',undefined,'case-ui-query-controls');controls.open=queryControlsOpen;controls.ontoggle=()=>{queryControlsOpen=controls.open;};controls.append($('summary','配置选样条件'));parent.append(controls);
      parent.append($('section',undefined,'case-ui-review-filter'));
      const sampleLabel=$('label','样本模式（前 1000 行 / 8 MiB） ');quick.append(sampleLabel);
      const sample=input(state.query_scope==='sample','明确选择样本模式',sampleLabel,1,'checkbox');sample.onchange=()=>{state.query_scope=sample.checked?'sample':'full';pageTicket++;changed();};
      quick.append($('p','默认预算内查询全集；超限明确拒绝。样本需要主动选择，筛选后仍会显示实际覆盖。','muted small'));
      if(groupChoiceSource!==source.digest){groupChoiceSource=source.digest;groupChoices.clear();}for(const name of groupChoices.keys())if(!descriptor.grouping.includes(name))groupChoices.delete(name);
      for(const name of descriptor.grouping){
        const choices=groupChoices.get(name)||new Map();groupChoices.set(name,choices);
        for(const value of [...(page?.groups||[]).map(group=>group.values[name]),...(page?.rows||[]).map(item=>item[name])]){
          if(value!==null&&!['string','number','boolean'].includes(typeof value)||typeof value==='string'&&S.bytes(value)>1024||typeof value==='number'&&!Number.isFinite(value))continue;
          const key=canonical(value);if(choices.has(key)||choices.size<32)choices.set(key,value);
        }
        const predicate=item=>item&&item.field===name&&(item.op==='eq'||item.op==='is_null'&&item.value===true),candidates=(descriptor.filters?.op==='and'?descriptor.filters.items:[descriptor.filters]).filter(predicate),leaf=candidates.length===1?candidates[0]:null;
        const selected=leaf?(leaf.op==='is_null'?null:leaf.value):undefined,values=[...choices.values()];if(leaf&&!choices.has(canonical(selected)))values.unshift(selected);
        const label=$('label','按 '+columnLabel(name)+' 限定分组','case-ui-group-control');quick.append(label);
        const options=[['','所有分组'],...values.map((value,index)=>['g'+index,value===null?'空值':String(value)])],selectedIndex=leaf?values.findIndex(value=>canonical(value)===canonical(selected)):-1;
        const choose=select(options,selectedIndex<0?'':'g'+selectedIndex,'分组取值 '+name,label);choose.disabled=candidates.length>1;choose.title='最多保留当前固定来源发现的 32 个分组取值；其他条件仍在筛选区。';
        choose.onchange=async()=>{if(!choose.isConnected||sourceTransition)return;try{
          let next=descriptor.filters;
          if(leaf){if(next===leaf)next=null;else {const items=next.items.filter(item=>item!==leaf);next=items.length===1?items[0]:items.length?{op:'and',items}:null;}}
          if(choose.value){const value=values[Number(choose.value.slice(1))],item=value===null?{op:'is_null',field:name,value:true}:{op:'eq',field:name,value};next=next?{op:'and',items:[next,item]}:item;}
          validateFilter(next,new Map(source.schema.map(field=>[field.name,field])));descriptor.filters=S.clone(next);state.filter_texts={};editQuery();paintPage();await load(0);
        }catch(error){paintPage();info(error.message,true);}};
        if(candidates.length>1)quick.append($('p','此分组字段有多个筛选条件，请在「配置选样条件」中修改。','muted small'));
      }
      if(!descriptor.grouping.length)button('配置分组与筛选',()=>{controls.open=true;controls.scrollIntoView({block:'start'});},quick);
      if(descriptor.filters)quick.append($('p','已设筛选条件；展开「配置选样条件」编辑。','muted small'));
      if(page&&!pageIsCurrent())quick.append($('p','筛选或配置草稿尚未应用读取。','case-ui-stale'));
      button('应用筛选与分组',()=>{queryControlsOpen=false;return load(0);},quick,!connected||sourceTransition);
      if(descriptor.filters)button('清除筛选条件',()=>{descriptor.filters=null;state.filter_texts={};editQuery();paintPage();return load(0);},quick);
      controls.append($('h4','筛选'));paintFilters(controls);
      controls.append($('h4','分组字段'));
      descriptor.grouping.forEach((name,index)=>{const line=$('div',undefined,'case-ui-order-row');controls.append(line);const choice=select(primitive().map(field=>[field.name,columnLabel(field.name)]),name,index===0?'分组字段':'分组字段 '+(index+1),line);choice.onchange=()=>{descriptor.grouping[index]=choice.value;editQuery();paintSettings();paintPage();};button('移除分组字段',()=>{descriptor.grouping.splice(index,1);editQuery();paintSettings();paintPage();},line);});
      button('添加分组字段',()=>{const next=primitive().find(field=>!descriptor.grouping.includes(field.name));if(!next)throw Error('没有更多可分组字段');descriptor.grouping.push(next.name);editQuery();paintSettings();paintPage();},controls,descriptor.grouping.length>=4);
      controls.append($('h4','稳定排序'));
      descriptor.sort.forEach((item,index)=>{
        const line=$('div',undefined,'case-ui-order-row');controls.append(line);const binding=select(available.map(field=>[field.name,field.name]),item.field,index===0?'排序字段':'排序字段 '+(index+1),line),direction=select([['asc','升序'],['desc','降序']],item.direction,index===0?'排序方向':'排序方向 '+(index+1),line);
        binding.onchange=()=>{item.field=binding.value;editQuery();};direction.onchange=()=>{item.direction=direction.value;editQuery();};button('移除排序字段',()=>{descriptor.sort.splice(index,1);editQuery();paintPage();},line);
      });
      button('添加排序字段',()=>{const next=available.find(field=>!descriptor.sort.some(item=>item.field===field.name));if(!next)throw Error('没有更多可排序字段');descriptor.sort.push({field:next.name,direction:'asc'});editQuery();paintPage();},controls,descriptor.sort.length>=4||!available.length);
      controls.append($('p','同值按固定行键区分；保存下一条使用本次返回的顺序。','muted small'));
    }
    function paintList(parent){
      if(!page)return;const descriptor=state.descriptor,coverage=page.coverage;
      parent.append($('p',(coverage.requested_scope==='sample'?'显式样本':'全集')+' · '+coverage.rows_scanned+' / '+coverage.total_rows+' 行；范围内匹配 '+page.matching_in_scope+' 条 · 标注快照 '+page.annotation_watermark,'analysis-case-coverage'));
      if(!pageIsCurrent())parent.append($('p','下方仍是上次读取的结果。配置已更改，翻页前请重新读取。','case-ui-stale'));
      if(page.other_groups)parent.append($('p','另有 '+page.other_groups+' 个分组；导航按当前返回页排列。','muted small'));
      const list=$('div',undefined,'analysis-case-list');parent.append(list);const groups=new Map();
      const visibleGroups=descriptor.grouping.filter(name=>descriptor.fields.some(field=>field.field===name));
      for(const item of page.rows){const key=visibleGroups.length?visibleGroups.map(name=>columnLabel(name)+' · '+JSON.stringify(item[name])).join(' · '):'当前选样页';if(!groups.has(key))groups.set(key,[]);groups.get(key).push(item);}
      for(const [group,items]of groups){const section=$('section',undefined,'case-ui-list-group');section.append($('h4',group+' · 本页 '+items.length+' 条'));list.append(section);
        for(const item of items){const named=caseTitle(descriptor,item,item._rowid),entry=button(named.title,()=>selectCase(item),section,sourceTransition);entry.dataset.caseKey=item._rowid;entry.setAttribute('aria-label','Case '+item._rowid);entry.setAttribute('aria-pressed',String(item._rowid===row?._rowid));
          if(named.title!==named.key)entry.append($('small',named.key));
        }
      }
      const paging=$('div',undefined,'analysis-cell-tools');parent.append(paging);
      button('上一页',()=>load(state.cursors[state.page_index-1],'back'),paging,!state.page_index||!pageIsCurrent());button('下一页',()=>load(page.next_cursor,'next'),paging,!page.next_cursor||!pageIsCurrent());
    }
    function paintPage(){
      rail.replaceChildren($('h3','分组与筛选'));if(!state||!source){releaseContent();content.replaceChildren($('p','从数据目录选择一张固定版本表。','muted'));review.replaceChildren();paintStatus();return;}
      const railBody=$('div',undefined,'case-ui-rail-body');
      if(unavailableReason){rail.append($('p','固定来源不可用：'+unavailableReason,'error-box'));const key=input(state.current||'','历史 Case 行键',rail,20);button('读取固定来源标注历史',async()=>{if(!(/^(0|[1-9][0-9]{0,19})$/).test(key.value)||BigInt(key.value)>=2n**64n)throw Error('请输入规范的固定行键');row={_rowid:key.value};state.current=key.value;changed();paintContent();await loadReview();},rail,!connected);}
      paintFilterControls(railBody);paintList(railBody);
      rail.append(railBody);paintReviewFilter();paintContent();if(!row)review.replaceChildren($('h3','评测与标注'),$('p',sourceTransition?'正在切换来源，原来源草稿已保留。':'当前读取范围没有 Case。','muted'));
      paintProfile();paintStatus();paintPermissions();
    }
    async function selectCase(item){
      if(sourceTransition||!page?.rows.includes(item))throw Error('Case 来源已变化，请重新读取');if(!flush())return false;
      row=item;state.current=item._rowid;reviewTicket++;headsReady=false;changed();paintContent();
      for(const entry of rail.querySelectorAll('.analysis-case-list button'))entry.setAttribute('aria-pressed',String(entry.dataset.caseKey===item._rowid));await loadReview();return true;
    }
    function paintOutputSelection(){
      for(const card of content.querySelectorAll('.case-ui-output-card'))card.classList.toggle('is-selected',card.dataset.caseKey===row?._rowid&&(card.dataset.groupId?state.review_group===card.dataset.groupId:!reviewVariant()&&reviewField()===card.dataset.field));
      for(const field of content.querySelectorAll('.analysis-case-field[data-role=output]'))field.classList.toggle('case-ui-field-reviewed',field.dataset.caseKey===row?._rowid&&!reviewVariant()&&reviewField()===field.dataset.field);
    }
    function paintContent(){
      releaseContent();content.replaceChildren();if(!row){content.append($('p','当前范围没有可显示的 Case。','muted'));return;}
      const version=++selectionTicket,context=ticket,selectedSource=source.digest,selectedLocator=locator?S.clone(locator):null,selectedRow=row,descriptor=state.descriptor;
      const heading=$('div',undefined,'case-ui-current-heading'),named=caseTitle(descriptor,row,row._rowid),title=$('h3',named.title),navigation=$('div',undefined,'case-ui-current-navigation');heading.append(title,navigation);content.append(heading,$('p',named.key,'case-ui-current-key muted small'));
      const index=page?.rows.findIndex(item=>item._rowid===row._rowid)??-1;
      button('←',()=>selectCase(page.rows[index-1]),navigation,index<=0).setAttribute('aria-label','上一个 Case');
      button('→',()=>selectCase(page.rows[index+1]),navigation,index<0||index+1>=page.rows.length).setAttribute('aria-label','下一个 Case');
      if(index<0&&page)content.append($('p','当前 Case 不在本次选样页，固定材料与答案草稿仍保留。可从左侧选择结果或清除筛选。','case-ui-stale'));
      if(unavailableReason){content.append($('p','原固定来源暂时不可读取。平台答案及其定义仍可查看；未替换为最新表。','error-box'));return;}
      const groupedIds=new Set((descriptor.output_groups||[]).flatMap(group=>group.fields)),shared=descriptor.fields.filter(field=>!['output','evaluation'].includes(field.role)),outputs=outputItems(descriptor),evaluation=descriptor.fields.filter(field=>field.role==='evaluation'&&!groupedIds.has(field.id));
      const selectedOutput=outputs.find(item=>item.key===state.output_item)||outputs.find(item=>item.kind==='field'&&item.field===state.output_field)||outputs[0];
      const sharedHost=$('div',undefined,'case-ui-shared-materials'),inputsHost=$('div',undefined,'case-ui-input-materials'),referencesHost=$('div',undefined,'case-ui-reference-materials');sharedHost.classList.toggle('case-ui-shared-groups',descriptor.version===2);sharedHost.append(inputsHost,referencesHost);content.append(sharedHost);
      // The shared reader keeps one latest waiter. Serialize this Case's
      // bounded visible siblings, using the reader's additive ready contract.
      const textJobs=new Set(),textQueue=[];let textActive=false,textDisposed=false;
      const currentText=()=>!textDisposed&&live(context)&&selectionTicket===version;
      const drainText=()=>{
        if(textActive||!currentText())return;
        let job;while(textQueue.length){const next=textQueue.shift();if(!next.disposed&&next.parent.isConnected){job=next;break;}next.dispose();}
        if(!job)return;textActive=true;job.element.replaceChildren();
        try{job.reader=mountText(job.element,job.text,job.options);Promise.resolve(job.reader?.ready).then(()=>{if(!job.disposed&&currentText())job.element.classList.add('case-ui-reader-complete');}).catch(error=>{if(!job.disposed&&currentText())job.element.append($('p',error.message,'error-box'));}).finally(()=>{job.text=null;job.options=null;textActive=false;job.finish();drainText();});}
        catch(error){job.element.append($('p',error.message,'error-box'));textActive=false;job.finish();drainText();}
      };
      const caseText=(parent,text,options={})=>{
        if(!globalThis.ForgeReading?.initialReady)return mountText(parent,text,options);
        if(textJobs.size>=32)throw Error('当前 Case 的文本字段已达 32 项预算');
        const element=$('div',undefined,'case-ui-text-reader');element.append($('p','等待当前 Case 的文本预览…','muted small'));parent.append(element);let finish;
        const ready=new Promise(resolve=>{finish=resolve;}),job={parent,element,text,options,reader:null,disposed:false,finish,
          dispose(){if(job.disposed)return;job.disposed=true;disposeReaders(element);job.reader?.dispose?.();element.replaceChildren();job.text=null;textJobs.delete(job);finish();}};
        textJobs.add(job);textQueue.push(job);drainText();return {element,ready,dispose:job.dispose};
      };
      rendererHandles.add({dispose(){textDisposed=true;for(const job of [...textJobs])job.dispose();textQueue.length=0;}});
      function material(field,record,parent){
        const item=$('section',undefined,'analysis-case-field');item.dataset.role=field.role;item.dataset.caseKey=record._rowid;item.dataset.field=field.field;
        const caption=$('h4',(roles[field.role]||field.role)+' · '+field.label);caption.title='源字段：'+field.field+' · 格式：'+field.format;item.append(caption);const meta=$('details',undefined,'case-ui-field-meta');meta.append($('summary','字段信息'),$('p','源字段 '+field.field+' · 格式 '+field.format,'muted small'));item.append(meta);parent.append(item);
        if(!Object.hasOwn(record,field.field)){item.append($('p','该字段尚未读取，请重新读取 Case 或选择本次选样结果。','muted'));return item;}
        if(field.role==='output'&&record[field.field]===null)item.append($('p','此输出字段为 null，未提供结果内容或失败原因。','muted small'));
        const isCurrent=()=>live(context)&&selectionTicket===version&&item.isConnected;
        let rendered;
        if(fieldMedia&&['image','audio','video'].includes(field.format)){
          if(!mediaController){item.append($('p','字段媒体组件尚未加载，无法核对固定媒体；原字段值保留。','error-box'));rendered=ForgeAnalysisRender.field({parent:item,format:typeof record[field.field]==='string'?'text':'json',value:record[field.field],label:field.label,density:'compact',tools:'menu',mountText:caseText,disposeReaders,isCurrent});}
          else rendered=mediaController.mountField({parent:item,source_snapshot:selectedSource,case_key:record._rowid,field:field.field,format:field.format,value:record[field.field],label:field.label,compact:descriptor.version===2,mountText:caseText,disposeReaders,preview:(format,value)=>readBody('field-preview',{format,value}),isCurrent});
        }else rendered=ForgeAnalysisRender.field({parent:item,format:field.format,value:record[field.field],label:field.label,language:field.language,density:'compact',tools:'menu',mountText:caseText,disposeReaders,project,authorizeURL,
          resolveMedia:resolveMedia?request=>resolveMedia({...request,project,source_snapshot:selectedSource,case_key:record._rowid,field:field.field}):undefined,
          context:{project,source_snapshot:selectedSource,case_key:record._rowid,field:field.field},preview:(format,value)=>readBody('field-preview',{format,value}),
          media:selectedLocator?.kind==='workspace_table'&&media?host=>media(host,{alias:selectedLocator.alias,binding_digest:selectedLocator.binding,row_id:record._rowid,field:field.field},isCurrent):null,isCurrent});
        Promise.resolve(rendered).then(handle=>{if(handle&&typeof handle.dispose==='function'){if(isCurrent()){rendererHandles.add(handle);handle.ready?.catch?.(error=>{if(isCurrent())item.append($('p',error.message,'error-box'));});}else handle.dispose();}}).catch(error=>{if(isCurrent())item.append($('p',error.message,'error-box'));});
        return item;
      }
      for(const field of shared.filter(field=>field.role==='input'))material(field,selectedRow,inputsHost);
      for(const field of shared.filter(field=>field.role==='reference'))material(field,selectedRow,referencesHost);
      if(!inputsHost.children.length)inputsHost.hidden=true;if(!referencesHost.children.length)referencesHost.hidden=true;
      const contexts=shared.filter(field=>field.role==='context');
      if(contexts.length){const panel=$('details',undefined,'case-ui-context-materials'),summary=contexts.map(field=>field.label+(typeof selectedRow[field.field]==='string'&&selectedRow[field.field].length<=80?' · '+selectedRow[field.field]:'')).join(' / ');panel.append($('summary','上下文 · '+summary));content.insertBefore(panel,sharedHost);let mounted=false;panel.ontoggle=()=>{if(live(context)&&selectionTicket===version&&panel.isConnected&&panel.open&&!mounted){mounted=true;for(const field of contexts)material(field,selectedRow,panel);}};}
      if(!outputs.length){content.append($('p','尚未配置输出字段，可在“字段与评测配置”中指定输出角色。','muted small'));}
      else {
        const outputHeader=$('div',undefined,'case-ui-output-heading'),switches=$('div',undefined,'case-ui-layout-switch');outputHeader.append($('span',descriptor.version===2?'输出方案与独立字段':'输出字段'),switches);content.append(outputHeader);
        for(const [mode,label]of [['card','单条'],['comparison','对照'],['gallery','画廊']]){
          const change=button(label,()=>{state.descriptor.layout=mode;state.gallery_offset=0;state.view_touched=true;changed();paintSettings();paintContent();},switches);change.setAttribute('aria-pressed',String(descriptor.layout===mode));
        }
        if(descriptor.layout!=='comparison'){
          const pickerLabel=descriptor.version===2?'显示输出方案或字段':'显示输出字段',label=$('label',pickerLabel,'case-ui-output-picker');content.append(label);
          const choices=outputs.map(item=>[descriptor.version===2?item.key:item.field,item.label]),choice=select(choices,descriptor.version===2?selectedOutput.key:selectedOutput.field,pickerLabel,label);
          choice.onchange=()=>{if(descriptor.version===2)state.output_item=choice.value;else state.output_field=choice.value;state.gallery_offset=0;changed();paintContent();};
        }
        const outputHost=$('div',undefined,'case-ui-output-grid');outputHost.dataset.layout=descriptor.layout;content.append(outputHost);
        const output=(item,record,parent,gallery=false)=>{
          const card=$('article',undefined,'case-ui-output-card'+(item.kind==='group'?' case-ui-output-group':''));card.dataset.caseKey=record._rowid;
          if(item.kind==='group'){card.dataset.groupId=item.id;card.append($('h4',item.label,'case-ui-group-heading'));}else card.dataset.field=item.field;
          card.classList.toggle('is-selected',record._rowid===row._rowid&&(item.kind==='group'?state.review_group===item.id:!reviewVariant()&&reviewField()===item.field));parent.append(card);
          if(gallery)button(caseTitle(descriptor,record,record._rowid).title,()=>selectCase(record),card).setAttribute('aria-label','查看 Case '+record._rowid);
          if(!item.fields.length)card.append($('p','此方案尚未配置有效输出材料，请在配置中补全。','case-ui-stale'));
          for(const field of item.fields){const fieldHost=material(field,record,card);if(item.kind==='group'&&field.role==='output')button('评测此字段',()=>reviewOutput(record,field.field),fieldHost).setAttribute('aria-label','复核 Case '+record._rowid+' 输出字段 '+field.label);}
          const choose=button(item.kind==='group'?'复核此输出方案':'复核此输出字段',()=>item.kind==='group'?reviewGroup(record,item):reviewOutput(record,item.field),card);choose.setAttribute('aria-label','复核 Case '+record._rowid+' '+(item.kind==='group'?'输出方案 ':'输出字段 ')+item.label);
        };
        if(descriptor.layout==='gallery'){
          const maximum=Math.max(1,Math.min(12,Math.floor((32-shared.length-evaluation.length)/Math.max(1,selectedOutput.fields.length)))),rows=page?.rows||[],offset=Math.min(Math.max(0,Number.isInteger(state.gallery_offset)?state.gallery_offset:0),Math.max(0,rows.length-maximum));
          const shown=rows.slice(offset,offset+maximum);content.insertBefore($('p','当前选样页 · '+selectedOutput.label+' · 显示 '+shown.length+' / '+rows.length+' 个 Case；左侧选样范围不变。','case-ui-gallery-scope muted small'),outputHost);
          for(const record of shown)output(selectedOutput,record,outputHost,true);
          if(rows.length>maximum){const paging=$('div',undefined,'analysis-cell-tools');content.append(paging);button('上一组本页画廊',()=>{state.gallery_offset=Math.max(0,offset-maximum);changed();paintContent();},paging,offset===0);button('下一组本页画廊',()=>{state.gallery_offset=offset+maximum;changed();paintContent();},paging,offset+maximum>=rows.length);}
        }else if(descriptor.layout==='comparison')for(const item of outputs)output(item,selectedRow,outputHost);
        else output(selectedOutput,selectedRow,outputHost);
      }
      paintOutputSelection();if(evaluation.length){const panel=$('details',undefined,'case-ui-model-evaluation');panel.append($('summary','当前 Case · 已有模型评测字段'));content.append(panel);let mounted=false;panel.ontoggle=()=>{if(live(context)&&selectionTicket===version&&panel.isConnected&&panel.open&&!mounted){mounted=true;for(const field of evaluation)material(field,selectedRow,panel);}};}
    }
    function paintProfile(){
      profile.replaceChildren($('summary','字段分布与覆盖范围'));if(!source)return;
      const output=$('div');profile.append(output);button('读取字段统计',async()=>{
        const version=ticket,columns=[...new Set(state.descriptor.fields.map(field=>field.field))];
        const value=await read('source-profile',{digest:source.digest,columns:JSON.stringify(columns)});if(!live(version)||!output.isConnected)return;
        const coverage=value.coverage;output.replaceChildren($('p',(coverage.complete?'全表':'前部抽样')+' · '+coverage.rows_scanned+' / '+coverage.total_rows+' 行 · 投影字节 '+coverage.logical_bytes_scanned+'；长值和复杂字段单独计数。','muted small'));
        for(const field of value.fields){const item=$('details');item.append($('summary',field.name+' · 空值 '+field.null_count+' · 复杂值 '+field.complex_count+' · 长值 '+field.long_or_unrepresentable_count));
          item.append($('p','最小值 '+JSON.stringify(field.minimum)+' · 最大值 '+JSON.stringify(field.maximum)+' · 非有限值 '+field.nonfinite_count,'muted small'));
          for(const enumeration of field.enumeration)item.append($('p',JSON.stringify(enumeration.value)+' · '+enumeration.count));
          if(field.other_short_primitive_count)item.append($('p','其他短值 '+field.other_short_primitive_count,'muted small'));output.append(item);}
      },profile,!connected);
    }
    const reviewField=()=>state?.review_field??null;
    const reviewVariant=()=>state?.review_variant??null;
    const reviewScope=()=>canonical({group:state?.review_group??null,variant:reviewVariant(),field:reviewField()});
    const sameAnswer=(entry,caseKey,field=reviewField(),variant=reviewVariant())=>entry.case_key===caseKey&&(entry.target_field??null)===field&&(entry.target_variant??null)===variant;
    const target=(caseKey,field=reviewField(),variant=reviewVariant())=>({source:source.digest,case_key:caseKey,variant,field});
    function checkBindings(value,caseKey,{view,variant}={}){
      S.bounded(value,48*1024);exact(value,['format','source_snapshot','case_key','view_digest','groups']);
      if(value.format!=='demiflow_case_group_bindings_v1'||value.source_snapshot!==source.digest||value.case_key!==caseKey||!Array.isArray(value.groups)||value.groups.length>(variant?1:8))throw Error('输出方案绑定与固定来源或 Case 不一致');
      if(variant){if(value.view_digest!==null||value.groups.length!==1)throw Error('旧方案绑定响应无效');}
      else if(!(/^[a-f0-9]{64}$/).test(value.view_digest)||pageIsCurrent()&&value.view_digest!==page.view_digest)throw Error('输出方案绑定与当前视图快照不一致');
      const expected=new Map((view?.descriptor.output_groups||[]).map(group=>[group.id,group])),seen=new Set();
      if(!variant&&value.groups.length!==expected.size)throw Error('方案绑定响应缺少当前方案');
      for(const binding of value.groups){
        if(binding.registered===false){exact(binding,['id','label','registered','variant','target','reason']);if(binding.variant!==null||binding.target!==null||binding.reason!=='save_or_register_required'||variant)throw Error('未登记方案不能返回可写目标');}
        else{
          exact(binding,['id','label','registered','variant','target','definition','registration']);
          if(binding.registered!==true||!(/^cg[a-f0-9]{64}$/).test(binding.variant)||variant&&binding.variant!==variant||canonical(binding.target)!==canonical(target(caseKey,null,binding.variant)))throw Error('方案评测目标未经匹配验证');
          const semantic=binding.definition;exact(semantic,['format','source_snapshot','id','fields']);
          if(semantic.format!=='demiflow_case_group_semantics_v1'||semantic.source_snapshot!==source.digest||semantic.id!==binding.id||!Array.isArray(semantic.fields)||!semantic.fields.length||semantic.fields.length>16||new Set(semantic.fields).size!==semantic.fields.length||semantic.fields.some(name=>!source.schema.some(field=>field.name===name)))throw Error('方案成员定义不属于固定来源');
          const proof=binding.registration;exact(proof,['kind','reference','descriptor_sha256']);if(!['saved_view','notebook_output'].includes(proof.kind)||!(/^[a-f0-9]{64}$/).test(proof.descriptor_sha256))throw Error('方案缺少不可变登记引用');text(proof.reference,'方案登记引用',256);if(proof.kind==='notebook_output'){if(!(/^no[a-f0-9]{64}$/).test(proof.reference))throw Error('方案 Notebook 登记引用无效');}else{const parts=proof.reference.split('@');if(parts.length!==2||!(/^[1-9][0-9]{0,9}$/).test(parts[1]))throw Error('方案视图登记版本无效');S.identity(parts[0]);}
          if(!variant){const group=expected.get(binding.id),members=new Set((group?.fields||[]).map(id=>view.descriptor.fields.find(field=>field.id===id)?.field));if(semantic.fields.length!==members.size||semantic.fields.some(name=>!members.has(name)))throw Error('方案绑定成员与提交配置不同');}
        }
        S.identity(binding.id);text(binding.label,'方案标题');if(seen.has(binding.id)||!variant&&(!expected.has(binding.id)||expected.get(binding.id).label!==binding.label))throw Error('方案绑定的 ID 或标题不匹配');seen.add(binding.id);
      }
      return value.groups;
    }
    function groupInDescriptor(descriptor,proof=verifiedReview){
      if(!reviewVariant())return true;if(!proof?.definition||descriptor?.source_snapshot!==source.digest)return false;
      const group=descriptor.output_groups?.find(item=>item.id===proof.definition.id);if(!group)return false;
      const names=new Set(group.fields.map(id=>descriptor.fields.find(field=>field.id===id)?.field));return names.size===proof.definition.fields.length&&proof.definition.fields.every(name=>names.has(name));
    }
    function paintReviewSubject(parent,valid){
      const label=$('label','Case '+row._rowid+' · 标注对象','case-ui-target');parent.append(label);
      const fieldKey=name=>/^(group:|old:|field:)/.test(name)?'field:'+name:name;
      const fields=[...new Map(state.descriptor.fields.filter(field=>field.role==='output').map(field=>[field.field,field])).values()],choices=[['','整个 Case']];
      for(const group of state.descriptor.output_groups||[]){const registered=currentGroupBindings.find(item=>item.id===group.id)?.registered;choices.push(['group:'+group.id,'输出方案 · '+group.label+(registered===false?'（需保存配置）':'')]);}
      choices.push(...fields.map(field=>[fieldKey(field.field),'输出字段 · '+field.label]));if(reviewField()&&!fields.some(field=>field.field===reviewField()))choices.push([fieldKey(reviewField()),'原字段 · '+reviewField()]);
      const old=new Set(state.answers.map(entry=>entry.target_variant).filter(Boolean));if(reviewVariant()&&!state.review_group)old.add(reviewVariant());
      for(const variant of old){const binding=currentGroupBindings.find(item=>item.registered&&item.variant===variant)||(verifiedReview?.variant===variant?verifiedReview:null),hint=state.answers.find(entry=>entry.target_variant===variant&&entry.target_label)?.target_label;if(!currentGroupBindings.some(item=>item.registered&&item.variant===variant)||!state.review_group&&reviewVariant()===variant)choices.push(['old:'+variant,(binding?.registered?'已登记方案 · ':'保留方案 · ')+(binding?.label||hint||variant.slice(0,14))]);}
      const selected=state.review_group?'group:'+state.review_group:reviewVariant()?'old:'+reviewVariant():reviewField()?fieldKey(reviewField()):'';
      if(!choices.some(([key])=>key===selected))choices.push([selected,'当前配置中缺失的方案 · '+(state.review_group||reviewVariant()?.slice(0,14))]);
      const subject=select(choices,selected,'标注对象',label);
      subject.onchange=()=>{if(!valid()||!flush())return;const value=subject.value,previous=state.output_item;state.review_group=value.startsWith('group:')?value.slice(6):null;state.review_variant=value.startsWith('old:')?value.slice(4):null;state.review_field=value&&!state.review_group&&!state.review_variant?(value.startsWith('field:')?value.slice(6):value):null;const shown=outputItems(state.descriptor).find(item=>state.review_group?item.id===state.review_group:reviewField()&&item.fields.some(field=>field.field===reviewField()));if(shown&&state.descriptor.layout!=='comparison'){state.output_item=shown.key;if(shown.kind==='field')state.output_field=shown.field;}reviewTicket++;headsReady=false;verifiedReview=null;changed();if(state.output_item!==previous&&state.descriptor.layout!=='comparison')paintContent();else paintOutputSelection();loadReview().catch(error=>info(error.message,true));};
    }
    async function reviewGroup(item,group){
      if(!state.descriptor.output_groups?.some(value=>value.id===group.id))throw Error('方案不属于当前配置');if(!flush())return;
      state.review_group=group.id;state.review_variant=null;state.review_field=null;state.output_item=group.key;reviewTicket++;headsReady=false;verifiedReview=null;
      if(item!==row)return selectCase(item);changed();paintOutputSelection();await loadReview();
    }
    function selectedMedia(caseKey){
      if(!reviewVariant()&&!reviewField())return mediaController.evidence(source.digest,caseKey);
      const shared=state.descriptor.fields.filter(field=>['input','reference','context'].includes(field.role)).map(field=>field.field);
      const members=reviewVariant()?verifiedReview?.definition.fields:reviewField()?[reviewField()]:[];
      if(reviewVariant()&&!members)throw Error('请先核对方案登记，再选择固定媒体依据');
      const allowed=new Set([...shared,...members]);return mediaController.evidence(source.digest,caseKey,allowed).filter(ref=>allowed.has(ref.field));
    }
    async function reviewOutput(item,field){
      if(!source.schema.some(value=>value.name===field))throw Error('复核输出不属于固定来源');
      if(!flush())return;state.review_field=field;state.review_group=null;state.review_variant=null;state.output_field=field;reviewTicket++;headsReady=false;verifiedReview=null;
      if(item!==row)return selectCase(item);
      changed();paintOutputSelection();await loadReview();
    }
    function checkedMediaBasis(value,caseKey){
      if(!globalThis.ForgeCaseMedia)throw Error('媒体依据组件未加载；原答案草稿保留，请恢复组件后核对');
      ForgeCaseMedia.basis(value,target(caseKey));if(value.some(item=>!source.schema.some(field=>field.name===item.field)))throw Error('答案媒体依据引用了固定来源以外的字段');return value;
    }
    function copyHeadBasis(entry,head){
      delete entry.media_basis_error;
      if(head?.media_basis!==undefined)entry.media_basis=S.clone(checkedMediaBasis(head.media_basis,entry.case_key));else delete entry.media_basis;
    }
    function appendMediaBasis(parent,value){
      const details=$('details',undefined,'case-media-basis');details.append($('summary',value===undefined?'历史未记录媒体依据':'固定媒体依据 · '+value.length+' 个字段'));parent.append(details);
      if(value===undefined){details.append($('p','此记录未保存媒体依据；不据此推断曾播放或查看媒体。','muted small'));return;}
      if(!value.length)details.append($('p','此答案未绑定固定媒体。','muted small'));
      for(const ref of value){const line=$('div',undefined,'case-media-reference');line.append($('strong',ref.field+' · '+ref.mime),$('p','来源 '+ref.source_snapshot+' · Case '+ref.case_key+' · '+ref.artifact_id+' · 代次 '+ref.artifact_generation,'muted small'),$('p','内容 SHA '+ref.expected_sha256+' · 绑定 '+ref.expected_binding,'muted small'));details.append(line);}
    }
    function answerMedia(parent,entry,valid){
      if(!mediaController&&entry.media_basis===undefined)return;
      const element=$('div',undefined,'case-media-answer');parent.append(element);
      const paint=()=>{
        if(!valid()||!element.isConnected)return;const basisOpen=element.querySelector('.case-media-basis')?.open||false;element.replaceChildren();appendMediaBasis(element,entry.media_basis);element.querySelector('.case-media-basis').open=basisOpen;
        if(entry.media_basis_error)element.append($('p',entry.media_basis_error,'error-box'));
        if(!mediaController)return;
        let selected,error;try{selected=selectedMedia(entry.case_key);}catch(failure){error=failure.message;}
        if(error)element.append($('p',error,'error-box'));
        else if(entry.dirty&&canonical(selected)!==canonical(entry.media_basis||[]))element.append($('p','当前预览的固定媒体已变化；此答案仍保留编辑时的依据，可明确采用当前媒体。','case-ui-stale'));
        if(entry.media_basis?.length&&features.annotation_media_basis!==true)element.append($('p','平台尚未接通媒体依据的持久保存；此答案保留为草稿。','case-ui-stale'));
        let adoptHost=element;if(state.descriptor.version===2&&!entry.dirty&&entry.expected_revision===0){adoptHost=element.querySelector('.case-media-basis');if(entry.media_basis===undefined)adoptHost.querySelector('summary').textContent='媒体依据（编辑时固定）';}
        const adopt=button('采用当前固定媒体作为本题依据',()=>{if(!valid())return;entry.media_basis=S.clone(checkedMediaBasis(selectedMedia(entry.case_key),entry.case_key));delete entry.media_basis_error;entry.dirty=true;changed();paint();},adoptHost,!!error||!selected?.length&&!entry.media_basis?.length);adopt.hidden=!error&&canonical(selected||[])===canonical(entry.media_basis||[]);
        adopt.setAttribute('aria-label','采用当前固定媒体作为本题依据 '+entry.definition.title);
      };
      evidenceViews.add({element,paint});paint();
    }
    function answerEntry(caseKey,definition,digest,head){
      let entry=state.answers.find(item=>sameAnswer(item,caseKey)&&item.digest===digest);
      if(!entry){
        if(state.answers.length>=64){const index=state.answers.findIndex(item=>!item.dirty);if(index<0)throw Error('本来源的未保存答案已达 64 个，请先保存或导出');state.answers.splice(index,1);}
        entry={case_key:caseKey,target_field:reviewField(),target_variant:reviewVariant(),target_label:verifiedReview?.label||null,digest,definition:S.clone(definition),value:head?.state==='answered'?head.value:null,expected_revision:head?.revision||0,dirty:false};copyHeadBasis(entry,head);state.answers.push(entry);
      }else if(canonical(entry.definition)!==canonical(definition)){throw Error('答案草稿的定义与摘要不一致，已保留原草稿，请导出检查');}else if(!entry.dirty){entry.value=head?.state==='answered'?head.value:null;entry.expected_revision=head?.revision||0;copyHeadBasis(entry,head);}
      if(reviewVariant()&&verifiedReview)entry.target_label=verifiedReview.label;return entry;
    }
    async function loadReview(){
      headsReady=false;evidenceViews.clear();review.replaceChildren($('h3','评测与标注'));if(!row||!state||sourceTransition)return;
      if(materialDraftPending){paintMaterialDraft();return;}
      const version=++reviewTicket,context=ticket,draft=state,caseKey=row._rowid,sourceDigest=source.digest,questions=S.clone(state.descriptor.questions),initialScope=reviewScope(),view=S.clone(basis());
      const initialValid=()=>current(context,draft)&&reviewTicket===version&&source.digest===sourceDigest&&row?._rowid===caseKey&&reviewScope()===initialScope;
      verifiedReview=null;
      try{
        if(features.case_output_groups===true){
          if(state.descriptor.version===2){const value=await readBody('case-group-bindings',{digest:sourceDigest,case_key:caseKey,view});if(!initialValid())return;currentGroupBindings=checkBindings(value,caseKey,{view});}
          else currentGroupBindings=[];
          if(state.review_group){const binding=currentGroupBindings.find(item=>item.id===state.review_group);verifiedReview=binding?.registered?binding:null;state.review_variant=verifiedReview?.variant||null;}
          else if(reviewVariant()){const value=await readBody('case-group-bindings',{digest:sourceDigest,case_key:caseKey,variant:reviewVariant()});if(!initialValid())return;verifiedReview=checkBindings(value,caseKey,{variant:reviewVariant()})[0];}
        }else currentGroupBindings=[];
      }catch(error){if(!initialValid())return;paintReviewSubject(review,initialValid);review.append($('p','方案绑定暂不可用：'+error.message+'；原答案草稿保留。','error-box'));return;}
      const reference=target(caseKey),boundScope=reviewScope();
      const valid=()=>current(context,draft)&&reviewTicket===version&&source.digest===sourceDigest&&row?._rowid===caseKey&&reviewScope()===boundScope;
      if(state.review_group&&!state.descriptor.output_groups?.some(group=>group.id===state.review_group)){paintReviewSubject(review,valid);review.append($('p','此方案已从当前配置移除。材料和草稿保留，请明确选择评测对象或恢复原配置。','case-ui-stale'));return;}
      if(state.review_group||reviewVariant()){
        if(!verifiedReview){paintReviewSubject(review,valid);review.append($('p',features.case_output_groups===true?'此方案尚未登记。材料可查看；请明确保存视图配置后再填写方案评测。':'当前服务尚未开放已登记方案评测；材料和草稿保留，可选择整个 Case 或输出字段。','case-ui-stale'));if(features.case_output_groups===true)writeButton('保存配置并登记方案','view-save',saveView,review);changed();return;}
        changed();
      }
      if(configurationChanged){review.append($('p','请先应用当前评测定义；旧答案和草稿保留。','muted'));return;}
      for(const definition of questions)validateQuestion(definition);
      const computed=await Promise.all(questions.map(async definition=>({definition,digest:await hash(definition)})));if(!valid())return;
      const heads=new Map();let watermark=null;
      for(let offset=0;offset<computed.length;offset+=4){
        let value;try{value=await readBody('annotation-heads',{target:reference,definitions:computed.slice(offset,offset+4).map(item=>item.digest),...(watermark===null?{}:{watermark})});}catch(error){if(!valid())return;throw error;}if(!valid())return;
        if(canonical(value.target)!==canonical(reference))throw Error('标注响应与选中 Case 不一致');
        if(watermark!==null&&watermark!==value.annotation_watermark)throw Error('标注读取快照不一致，请重新读取');watermark=value.annotation_watermark;
        for(const item of value.items)heads.set(item.definition_digest,item.head);
      }
      if(!valid()||configurationChanged)return;activeQuestions=computed;latestHeads=heads;headsReady=true;paintReviewFilter();
      review.replaceChildren($('h3','评测与标注'));button('配置评测',()=>{configTab='questions';settings.hidden=false;settings.open=true;paintSettings();},review);if(!permissions.case_annotate)review.append($('p','当前没有标注写权限。可查看历史、保留和导出本地答案草稿。','muted small'));
      paintReviewSubject(review,valid);if(verifiedReview){const proof=$('p','已登记方案 · '+verifiedReview.label+' · '+verifiedReview.definition.fields.map(name=>state.descriptor.fields.find(field=>field.field===name)?.label||name).join(' / '),'case-ui-group-proof muted small');proof.title='源字段：'+verifiedReview.definition.fields.join(' / ');review.append(proof);if(!state.review_group)review.append($('p','当前按原登记成员评测；中间材料仍按当前视图展示。','case-ui-stale'));}
      const definitionOrigin=annotationOrigin(computed);
      const reviewContext=$('details',undefined,'case-ui-review-context');reviewContext.append($('summary',definitionOrigin.kind==='saved_view'?'题目：视图 v'+state.revision:definitionOrigin.kind==='notebook_output'?'题目：Notebook 固定输出':'题目：未保存视图'),$('p','保存到当前登录者的评审记录；原表保持固定版本。','muted small'),$('p',definitionOrigin.kind==='saved_view'?'题目依据：已存视图 v'+state.revision:definitionOrigin.kind==='notebook_output'?'题目依据：Notebook 固定输出登记引用 '+definitionOrigin.reference.slice(0,14)+'；保存时由服务端重新核对。':'题目依据：未保存视图定义；保存答案会单独保留题目定义。','case-ui-definition-origin muted small'));review.append(reviewContext);
      if(page&&watermark!==null&&watermark>page.annotation_watermark)review.append($('p','标注已有新版本；选样仍保持快照 '+page.annotation_watermark+'，需要时显式刷新。','case-ui-stale'));
      const historyLoaders=new Map();
      for(const item of activeQuestions){
        const {definition,digest}=item,head=heads.get(digest),entry=answerEntry(caseKey,definition,digest,head);
        const box=$('section',undefined,'analysis-answer');box.append($('h4',definition.title+(definition.required?' *':'')));review.append(box);
        const edit=value=>{if(!valid())return;if(!entry.dirty&&mediaController){try{entry.media_basis=selectedMedia(caseKey);delete entry.media_basis_error;}catch(error){entry.media_basis_error=error.message;}}entry.value=value;entry.dirty=true;changed();summary.textContent='当前答案 v'+entry.expected_revision+' · 有本地修改';for(const view of evidenceViews)view.paint();};
        if(definition.type==='choice'){
          const choice=select([['','未选择'],...definition.options.map(option=>[option.id,option.label])],entry.value??'','标注 '+definition.title,box);choice.onchange=()=>edit(choice.value||null);
        }else if(definition.type==='multi'){
          for(const option of definition.options){const label=$('label',option.label,'case-ui-answer-option'),check=input(Array.isArray(entry.value)&&entry.value.includes(option.id),'标注 '+definition.title+' '+option.label,label,1,'checkbox');label.prepend(check);label.classList.toggle('is-selected',check.checked);box.append(label);check.dataset.id=option.id;check.onchange=()=>{label.classList.toggle('is-selected',check.checked);edit([...box.querySelectorAll('input:checked')].map(element=>element.dataset.id));};}
        }else if(definition.type==='rating'){
          const number=input(entry.value??'','标注 '+definition.title,box,6,'number');number.min=definition.minimum;number.max=definition.maximum;number.step=1;number.oninput=()=>edit(number.value===''?null:Number(number.value));
        }else{const area=$('textarea');area.maxLength=8192;area.rows=4;area.setAttribute('aria-label','标注 '+definition.title);area.value=entry.value??'';area.oninput=()=>edit(area.value);box.append(area);}
        const summary=$('p','当前答案 v'+entry.expected_revision+(entry.dirty?' · 有本地修改':''),'muted small');box.append(summary);
        answerMedia(box,entry,valid);
        if(entry.dirty&&entry.expected_revision!==(head?.revision||0)){
          box.append($('p','保存冲突：草稿基于 v'+entry.expected_revision+'，服务器当前为 v'+(head?.revision||0)+'。请选择如何处理。','error-box'));
          button('保留草稿并采用服务器版本',async()=>{if(!valid())return;entry.expected_revision=head?.revision||0;changed();await loadReview();info('已保留答案草稿并采用刚读取的服务器版本；请核对后再次保存。');},box);
        }
        const actions=$('div',undefined,'case-ui-answer-actions');box.append(actions);
        button('使用服务器当前答案',async()=>{if(!valid())return;entry.value=head?.state==='answered'?S.clone(head.value):null;entry.expected_revision=head?.revision||0;copyHeadBasis(entry,head);entry.dirty=false;changed();await loadReview();},actions);
        writeButton('清除已保存答案','annotations-save',()=>saveEntries([{definition,expected_revision:entry.expected_revision,action:'clear'}],caseKey),actions);
        button('历史与恢复',()=>{const panel=review.querySelector('.case-ui-review-history');if(!panel)return;panel.open=true;const target=historyLoaders.get(digest);if(target){target.section.open=true;target.section.scrollIntoView({block:'nearest'});target.load();}},actions);
      }
      const historyPanel=$('details',undefined,'case-ui-review-history');historyPanel.append($('summary','标注历史与恢复'));review.append(historyPanel);
      for(const item of activeQuestions){const {definition,digest}=item,entry=state.answers.find(answer=>sameAnswer(answer,caseKey)&&answer.digest===digest);if(!entry)continue;const section=$('details',undefined,'case-ui-review-history-item');section.append($('summary',definition.title));historyPanel.append(section);historyLoaders.set(digest,{section,load:historyBody(section,definition,digest,entry,reference,valid)});}
      appendOldDefinitions(review,caseKey,reference,valid);
      const dirtyOther=state.answers.filter(item=>item.dirty&&!activeQuestions.some(question=>question.digest===item.digest&&sameAnswer(item,caseKey))).length;
      if(dirtyOther)review.append($('p','另有 '+dirtyOther+' 份其他 Case 或旧定义的标注草稿，仍保留在本来源草稿中。','muted small'));
      const saveBar=$('div',undefined,'case-ui-save-bar');review.append(saveBar);
      writeButton('保存本 Case 标注','annotations-save',()=>saveCurrent(false),saveBar);
      writeButton('保存并下一条','annotations-save',()=>saveCurrent(true),saveBar);
      button('撤销当前答案修改',async()=>{if(!valid())return;for(const {definition,digest}of computed){const entry=state.answers.find(item=>sameAnswer(item,caseKey,reference.field,reference.variant)&&item.digest===digest),head=heads.get(digest);if(entry){entry.value=head?.state==='answered'?S.clone(head.value):null;entry.expected_revision=head?.revision||0;copyHeadBasis(entry,head);entry.dirty=false;}}changed();await loadReview();},review);
      button('刷新当前标注',()=>loadReview(),review,!connected);changed();paintPermissions();
    }
    function historyBody(host,definition,digest,entry,reference,valid){
      let before=null,watermark=null;const histories=$('div');host.append(histories);
      const loadHistory=async()=>{
        let value;try{value=await read('annotations',{target:JSON.stringify(reference),definition:digest,limit:8,...(before?{before}:{}),...(watermark===null?{}:{watermark})});}
        catch(error){if(error.code==='not_found'&&valid()&&histories.isConnected){histories.replaceChildren($('p','该定义尚无服务器记录。','muted small'));return;}throw error;}
        if(!valid()||!histories.isConnected)return;watermark=value.annotation_watermark;before=value.next_before;histories.replaceChildren();
        for(const event of value.items){const line=$('div',undefined,'case-ui-history-entry');line.append($('p','v'+event.revision+' · '+event.state+' · '+(event.time||'')+' · '+JSON.stringify(event.value),'muted small'));histories.append(line);
          line.append($('p','定义来源 '+(event.origin?.kind||'未知')+' · '+(event.origin?.reference||''),'muted small'));
          appendMediaBasis(line,event.media_basis===undefined?undefined:checkedMediaBasis(event.media_basis,reference.case_key));
          writeButton('恢复到 v'+event.revision,'annotations-save',()=>saveEntries([{definition,expected_revision:entry.expected_revision,action:'revert',revision:event.revision}],reference.case_key),line);}
        if(before)button('更早标注',loadHistory,histories);if(!value.items.length)histories.append($('p','尚无已保存记录。','muted small'));paintPermissions();
      };
      button('读取历史',loadHistory,host,!connected);host.append($('p','题目定义 '+digest,'muted small'));return loadHistory;
    }
    function appendHistory(parent,definition,digest,entry,reference,valid){
      const history=$('details');history.append($('summary',parent.classList.contains('case-ui-old-answer')?'旧题标注历史与恢复':'标注历史与恢复'));parent.append(history);
      historyBody(history,definition,digest,entry,reference,valid);return history;
    }
    function appendOldDefinitions(parent,caseKey,reference,valid){
      const known=state.answers.filter(item=>sameAnswer(item,caseKey)&&!activeQuestions.some(question=>question.digest===item.digest));
      const panel=$('details',undefined,'case-ui-old-definitions');panel.append($('summary','旧定义与答案'));parent.append(panel);const list=$('div');panel.append(list);
      const render=(definition,digest,head,local)=>{
        if(activeQuestions.some(question=>question.digest===digest)||list.querySelector('[data-definition="'+digest+'"]'))return;
        const box=$('section',undefined,'case-ui-old-answer');box.dataset.definition=digest;box.append($('h4',definition.title),$('p','原定义 '+digest.slice(0,10)+' · '+definition.type,'muted small'));list.append(box);
        if(local?.dirty)box.append($('p','未保存的旧题草稿：'+JSON.stringify(local.value),'muted small'));
        else if(head)box.append($('p','已存答案 v'+head.revision+' · '+JSON.stringify(head.value),'muted small'));
        const entry=local||{definition,digest,expected_revision:head?.revision||0};
        appendHistory(box,definition,digest,entry,reference,valid);
        button('采用此题目定义',async()=>{if(!valid())return;const index=state.descriptor.questions.findIndex(item=>item.id===definition.id);if(index<0){if(state.descriptor.questions.length>=16)throw Error('评测项已达 16 个');state.descriptor.questions.push(S.clone(definition));}else state.descriptor.questions[index]=S.clone(definition);state.view_touched=true;configurationChanged=false;reviewTicket++;changed();paintSettings();await loadReview();},box);
      };
      let localOffset=0;const localTools=$('div');panel.append(localTools);
      const paintKnown=()=>{list.replaceChildren();localTools.replaceChildren();for(const local of known.slice(localOffset,localOffset+8))render(local.definition,local.digest,latestHeads.get(local.digest),local);if(known.length>8){localTools.append($('p','本来源已知旧定义 '+known.length+' 项；每页最多 8 项。','muted small'));button('上一页已知旧定义',()=>{localOffset=Math.max(0,localOffset-8);paintKnown();},localTools,localOffset===0);button('下一页已知旧定义',()=>{localOffset+=8;paintKnown();},localTools,localOffset+8>=known.length);}};paintKnown();
      if(features.annotation_definitions!==true){panel.append($('p','当前显示本来源已知定义；服务端尚未开放其他旧定义的发现入口。已保存答案可按原定义读取历史。','muted small'));return;}
      let before=null,watermark=null,discoverReading=false;
      const discover=async()=>{
        if(discoverReading)return;discoverReading=true;discoverButton.disabled=true;try{
        const value=await readBody('annotation-definitions',{target:reference,limit:8,...(before?{before}:{}),...(watermark===null?{}:{watermark})});if(!valid()||!list.isConnected)return;
        if(canonical(value.target)!==canonical(reference)||!Array.isArray(value.items)||value.items.length>8||!Number.isSafeInteger(value.annotation_watermark))throw Error('旧题历史与当前 Case 不一致或页超限');
        watermark=value.annotation_watermark;before=value.next_before;list.replaceChildren();localTools.replaceChildren();
        for(const item of value.items){validateQuestion(item.definition);if(await hash(item.definition)!==item.definition_digest)throw Error('旧题定义摘要不匹配');if(!valid())return;render(item.definition,item.definition_digest,item.head,state.answers.find(entry=>sameAnswer(entry,caseKey)&&entry.digest===item.definition_digest));}
        discoverButton.disabled=!before;discoverButton.textContent=before?'读取更早旧定义':'旧定义读取完成';
        if(known.length)button('查看本地旧题草稿',paintKnown,localTools);
        }finally{discoverReading=false;if(valid()&&discoverButton.isConnected)discoverButton.disabled=before===null&&discoverButton.textContent==='旧定义读取完成';}
      };
      const discoverButton=button('读取已保存旧定义',discover,panel,!connected);
    }
    async function saveCurrent(next){
      if(configurationChanged||!headsReady||!row)throw Error('请先应用并读取当前评测定义');
      if(busy)throw Error('当前操作尚未确认，请先核对原请求');
      info(next?'正在保存当前 Case 标注，确认后按本次选样顺序进入下一条…':'正在保存当前 Case 标注…');
      const context=ticket,draft=state,caseKey=row._rowid,selectedField=reviewField(),selectedVariant=reviewVariant(),originalPage=page,originalBasis=pageBasis,selected=activeQuestions.map(item=>item.digest);
      const entries=activeQuestions.map(item=>state.answers.find(answer=>sameAnswer(answer,caseKey)&&answer.digest===item.digest));
      for(const entry of entries)validateAnswer(entry.definition,entry.value);
      const changedEntries=entries.filter(entry=>entry.dirty).map(entry=>{
        if(entry.media_basis_error)throw Error(entry.media_basis_error+'；请核对媒体后明确采用本题依据');
        const value={definition:S.clone(entry.definition),expected_revision:entry.expected_revision,action:'set',value:S.clone(entry.value)};
        if(entry.media_basis!==undefined){checkedMediaBasis(entry.media_basis,caseKey);if(entry.media_basis.length&&features.annotation_media_basis!==true)throw Error('平台尚未接通媒体依据的持久保存；含媒体依据的答案草稿已保留');if(features.annotation_media_basis===true)value.media_basis=S.clone(entry.media_basis);}
        return value;
      });
      if(changedEntries.length)await saveEntries(changedEntries,caseKey);
      else if(!next){info('当前 Case 没有新的标注修改。');return;}
      if(!current(context,draft)||row?._rowid!==caseKey||(reviewField()!==selectedField||reviewVariant()!==selectedVariant))return;
      if(!next)return;
      if(state.answers.some(item=>sameAnswer(item,caseKey,selectedField,selectedVariant)&&selected.includes(item.digest)&&item.dirty)){info('已确认本次保存；保存期间有新答案修改，留在当前 Case。');return;}
      if(page!==originalPage||pageBasis!==originalBasis||!pageIsCurrent()){info('答案已保存；视图或查询已变化，重新读取后再继续下一条。');return;}
      const index=page.rows.findIndex(item=>item._rowid===caseKey);
      if(index<0){info('标注已保存；当前 Case 不在本次选样页，请从左侧选择后再继续。');return;}
      if(index+1<page.rows.length){await selectCase(page.rows[index+1]);info('已按本次选样顺序进入下一条。');}
      else if(page.next_cursor){await load(page.next_cursor,'next');}
      else info('当前 Case 已保存，已到本次选样末尾。');
    }
    function definitive(error){return error.operationStarted===false||['conflict','invalid_request','resource_limit','operation_rejected','not_found','unauthorized','forbidden'].includes(error.code);}
    async function mutation(operation,payload,scope){
      guardWrite(operation);if(busy)throw Error('当前操作尚未确认，请先核对原请求');
      if(store.pending(project).some(item=>item.scope.kind==='case'&&item.operation!=='media-capture'&&item.scope.source===source.digest))throw Error('本来源还有结果未确认的 Case 请求，请先核对原请求');
      if(!flush())throw Error('请先处理无法暂存的 Case 草稿');
      payload={request_id:uid(),...payload};const intent=store.prepare(project,operation,payload,{kind:'case',source:source.digest,draft_id:state.id,...(payload.target?{target_label:payload.target.variant?'输出方案 · '+(verifiedReview?.label||payload.target.variant.slice(0,14)):payload.target.field?'字段 · '+payload.target.field:'整个 Case'}:{}),...scope});busy=true;paintPending();paintPermissions();
      try{const value=await readBody(operation,payload);acknowledge(intent,value);store.finish(project,payload.request_id);return value;}
      catch(error){if(definitive(error)){store.finish(project,payload.request_id);throw error;}const unknown=Error(error.message+'；原请求 '+payload.request_id+' 已保留，请核对后再操作。');unknown.cause=error;throw unknown;}
      finally{busy=false;if(!disposed){paintPending();paintPermissions();}}
    }
    function acknowledge(intent,value){
      // Annotation identity is independent of the currently selected view.
      // View receipts may update a retained view, never a later copied view.
      const envelope=state&&source?.digest===intent.scope.source?state:store.caseDraft(project,intent.scope.source);
      if(!envelope)return;let draft=envelope;
      if(intent.operation==='view-save'&&intent.scope.draft_id&&draftKey(draft.id)!==draftKey(intent.scope.draft_id)){
        if(draftKey(envelope.retained_view?.id)!==draftKey(intent.scope.draft_id))return;draft=envelope.retained_view;
      }
      if(intent.operation==='view-save'){
        if(typeof value.id!=='string'||!Number.isSafeInteger(value.revision))throw Error('视图保存响应无法核对');
        draft.view_id=value.id;draft.revision=value.revision;draft.base=S.clone(intent.payload.descriptor);draft.view_touched=canonical(draft.descriptor)!==canonical(draft.base);
      }else if(intent.operation==='annotations-save'){
        if(canonical(value.target)!==canonical(intent.payload.target)||!Array.isArray(value.heads)||value.heads.length!==intent.payload.entries.length||new Set(value.heads.map(head=>head.definition)).size!==value.heads.length||value.heads.some(head=>!(/^[a-f0-9]{64}$/).test(head.definition)||!Number.isInteger(head.revision)||head.revision<1||head.revision>100)||intent.scope.definition_digests?.some(digest=>!value.heads.some(head=>head.definition===digest)))throw Error('标注保存响应无法核对');
        for(const head of value.heads)if(head.media_basis!==undefined){if(!globalThis.ForgeCaseMedia)throw Error('保存收据包含媒体依据，请恢复组件后核对原请求');ForgeCaseMedia.basis(head.media_basis,intent.payload.target);}
        for(const [index,sent]of intent.payload.entries.entries()){const digest=intent.scope.definition_digests?.[index],head=value.heads.find(item=>item.definition===digest)||value.heads.find(item=>draft.answers.some(answer=>answer.digest===item.definition&&canonical(answer.definition)===canonical(sent.definition)));
          if(sent.media_basis!==undefined&&(!head||canonical(head.media_basis)!==canonical(sent.media_basis)))throw Error('保存收据未回传本次精确媒体依据；原请求及草稿保留，请核对');}
        for(const head of value.heads){const entry=draft.answers.find(item=>sameAnswer(item,intent.payload.target.case_key,intent.payload.target.field,intent.payload.target.variant)&&item.digest===head.definition),sent=intent.payload.entries.find(item=>canonical(item.definition)===canonical(entry?.definition));
          if(entry&&sent){entry.expected_revision=head.revision;if(sent.action==='set'&&canonical(entry.value)===canonical(sent.value)&&canonical(entry.media_basis||[])===canonical(sent.media_basis||[])&&!entry.media_basis_error){entry.dirty=false;if(head.media_basis!==undefined)entry.media_basis=S.clone(head.media_basis);}
            else if(sent.action!=='set'&&canonical(entry.value)===intent.scope.previous?.[head.definition]&&(intent.scope.previous_media?.[head.definition]===undefined||canonical(entry.media_basis||[])===intent.scope.previous_media[head.definition]))draft.answers.splice(draft.answers.indexOf(entry),1);}}
      }
      if(!disposed&&envelope===state){picker.value=state.view_id||'';changed();}else store.saveCaseDraft(project,intent.scope.source,envelope);
    }
    async function saveView(){
      if(state?.descriptor.version===2&&features.case_output_groups!==true)throw Error('当前服务尚未开放输出方案配置保存');
      if(!state||!source)throw Error('请先选择数据源');guardWrite('view-save');if(Object.keys(state.filter_texts||{}).length)throw Error('筛选集合草稿超过上限，请先修正');if(Object.keys(state.option_texts||{}).length)throw Error('评测选项草稿超过上限，请先修正');validateDescriptor(state.descriptor,source);
      const context=ticket,draft=state;
      try{await mutation('view-save',{view_id:state.view_id,expected_revision:state.revision,descriptor:S.clone(state.descriptor)},{});}
      catch(error){if(current(context,draft)&&error.code==='conflict'&&state.view_id){viewConflict={id:state.view_id};paintViewConflict();}throw error;}
      if(!current(context,draft))return;clearViewConflict();await refreshViews();if(!current(context,draft))return;paintViewConflict();if((state.descriptor.version===2||materialDraftPending)&&page&&!pageIsCurrent())await load(0);else{paintContent();if(row)await loadReview();}if(!current(context,draft))return;if(!state.view_touched){labelSource(viewName(state.descriptor)+' · v'+state.revision,source);publishContext('ready');}paintStatus();info('视图已保存为 v'+state.revision+'；标注需在右侧另行保存。');
    }
    function clearViewConflict(){viewConflict=null;root.querySelector('.case-ui-view-conflict')?.remove();}
    function paintViewConflict(){
      let panel=root.querySelector('.case-ui-view-conflict');panel?.remove();if(!viewConflict||!state)return;
      panel=$('div',undefined,'case-ui-view-conflict error-box');notice.after(panel);panel.append($('p','视图保存冲突。当前配置草稿已保留，可复制为新视图，或读取服务器版本后明确采用新基线。'));
      button('读取服务器视图版本',async()=>{
        const context=ticket,draft=state,id=state.view_id,value=await read('view',{view_id:id});if(!current(context,draft)||state.view_id!==id||!panel.isConnected)return;
        if(value.document.source_snapshot!==source.digest)throw Error('服务器视图的来源已改变，请复制当前草稿或另行打开该视图');
        const summary=$('div');panel.append(summary);summary.append($('p','服务器当前 v'+value.revision+' · '+value.document.fields.length+' 字段 / '+value.document.questions.length+' 题；采用后仍需再次点击保存。'));
        button('保留配置并采用服务器版本',()=>{if(!current(context,draft))return;state.revision=value.revision;state.base=S.clone(value.document);state.view_touched=canonical(state.descriptor)!==canonical(state.base);clearViewConflict();changed();paintViewConflict();},summary);
      },panel);
    }
    async function saveEntries(entries,caseKey){
      if(!headsReady||configurationChanged||row?._rowid!==caseKey)throw Error('Case 或评测定义已变化，请先重新读取当前标注');guardWrite('annotations-save');
      const context=ticket,draft=state,sourceDigest=source.digest,selectedField=reviewField(),selectedVariant=reviewVariant();
      if(reviewVariant()&&(!verifiedReview||canonical(verifiedReview.target)!==canonical(target(caseKey))))throw Error('方案目标尚未核实；原草稿保留');
      entries=S.clone(entries);const mediaFields=new Set();for(const entry of entries)if(entry.media_basis!==undefined){if(entry.action!=='set')throw Error('清除或恢复答案不能由客户端更换媒体依据');checkedMediaBasis(entry.media_basis,caseKey);for(const ref of entry.media_basis)mediaFields.add(ref.field);}if(mediaFields.size>16)throw Error('一次保存最多绑定 16 个不同媒体字段；答案草稿已保留');
      const origin=annotationOrigin(entries);
      const previous=Object.fromEntries(state.answers.filter(item=>sameAnswer(item,caseKey)).map(item=>[item.digest,canonical(item.value)]));
      const previous_media=Object.fromEntries(state.answers.filter(item=>sameAnswer(item,caseKey)).map(item=>[item.digest,canonical(item.media_basis||[])]));
      const definition_digests=await Promise.all(entries.map(item=>hash(item.definition)));
      if(!current(context,draft)||source.digest!==sourceDigest||row?._rowid!==caseKey||(reviewField()!==selectedField||reviewVariant()!==selectedVariant)||!headsReady||configurationChanged)throw Error('Case 或评测定义已变化，答案草稿保留，请重新核对');
      try{await mutation('annotations-save',{target:target(caseKey,selectedField,selectedVariant),entries,origin},{case_key:caseKey,previous,previous_media,definition_digests});}
      catch(error){if(current(context,draft)&&source.digest===sourceDigest&&row?._rowid===caseKey&&reviewField()===selectedField&&reviewVariant()===selectedVariant&&error.code==='conflict'){await loadReview();info('保存冲突：已保留答案草稿，请比较服务器当前答案并明确选择。',true);}throw error;}
      if(!current(context,draft)||row?._rowid!==caseKey||(reviewField()!==selectedField||reviewVariant()!==selectedVariant))return;
      await loadReview();info('本次标注已保存；视图配置未随标注自动保存。');
    }
    function paintPending(){
      pending.replaceChildren();mediaController?.paintPending();let items=[];try{items=store.pending(project).filter(item=>item.scope.kind==='case'&&item.operation!=='media-capture');}catch(error){info(error.message,true);return;}
      for(const intent of items){const line=$('div',undefined,'analysis-pending-row');pending.append(line);line.append($('span','待核对 · '+intent.operation+' · 来源 '+intent.scope.source.slice(0,12)+(intent.payload.target?' · Case '+intent.payload.target.case_key+' · '+(intent.scope.target_label||(intent.payload.target.variant?'输出方案 · '+intent.payload.target.variant.slice(0,14):intent.payload.target.field?'字段 '+intent.payload.target.field:'整个 Case')):'')+' · '+intent.payload.request_id,'muted small'));
        button('核对 Case 原请求',async()=>{const value=await read('receipt',{request_id:intent.payload.request_id});acknowledge(intent,value);store.finish(project,intent.payload.request_id);if(disposed)return;paintPending();if(source?.digest===intent.scope.source&&row)await loadReview();info('Case 原操作已确认，保持当前选择。');},line,busy||!connected);
        const retry=writeButton('以原 Case 请求重试',intent.operation,async()=>{
          if(busy)return;guardWrite(intent.operation,intent.scope.source);store.durableIntents();busy=true;paintPermissions();
          try{const value=await readBody(intent.operation,intent.payload);acknowledge(intent,value);store.finish(project,intent.payload.request_id);if(!disposed&&source?.digest===intent.scope.source&&row)await loadReview();}
          catch(error){if(definitive(error))store.finish(project,intent.payload.request_id);throw error;}
          finally{busy=false;if(!disposed){paintPending();paintPermissions();}}
        },line);retry.dataset.sourceSnapshot=intent.scope.source;retry.disabled=busy||!permitted(intent.operation);
      }
    }
    function paintPresentation(){
      presentationPanel.replaceChildren();if(!presentation)return;
      presentationPanel.className='case-ui-presentation';presentationPanel.append($('p',(presentation.view_ref?'收到已保存的 Case 视图：':'收到 Notebook 固定输出视图：')+(presentation.label||presentation.descriptor?.source_snapshot?.slice(0,12)||presentation.view_ref?.id||'固定来源')+'。采用后读取已固定的来源；不会执行代码。'));
      if(state?.view_touched)presentationPanel.append($('p','当前视图有未保存修改。采用时先保留这份视图草稿，可随后恢复；答案仍按原定义保存。','muted small'));
      button(presentation.view_ref?'采用已保存视图':'采用 Notebook 输出视图',adoptPresentation,presentationPanel,!connected);
      button('保留当前配置',()=>{presentation=null;paintPresentation();info('已保留当前配置；原来源保持原值。');},presentationPanel);
    }
    async function adoptPresentation(){
      const incoming=presentation;if(!incoming)return false;if(!flush())return false;
      const context=ticket;let descriptor=incoming.descriptor,saved=null;
      if(incoming.view_ref){saved=await read('view',{view_id:incoming.view_ref.id,revision:incoming.view_ref.revision});if(!live(context)||presentation!==incoming)return false;descriptor=saved.document;}
      const snapshot=await read('source',{digest:descriptor.source_snapshot});if(!live(context)||presentation!==incoming)return false;validateDescriptor(descriptor,snapshot);
      const origin=saved?null:adoptedPresentation(incoming,descriptor,snapshot);
      let next=source?.digest===snapshot.digest?state:checkedDraftFor(snapshot,store.caseDraft(project,snapshot.digest)||defaults(snapshot));
      if(next.view_touched){
        if(next.retained_view)throw Error('该来源已有保留的视图草稿，请先恢复或导出处理后再采用另一份输出');
        next.retained_view=S.clone({id:next.id,view_id:next.view_id,revision:next.revision,base:next.base,descriptor:next.descriptor,view_touched:true,option_texts:next.option_texts||{},filter_texts:next.filter_texts||{},presentation_origin:next.presentation_origin||null});
      }
      if(!flush())return false;invalidate();sourceTicket++;candidate=null;source=snapshot;state=next;locator=null;sourceTransition=false;unavailableReason=null;page=null;row=null;pageBasis=null;
      state.id=uid();state.option_texts={};state.filter_texts={};state.descriptor=S.clone(descriptor);state.view_id=saved?.id||null;state.revision=saved?.revision||0;state.base=saved?S.clone(saved.document):null;state.view_touched=!saved;state.current=null;
      state.presentation_origin=origin;configurationChanged=false;presentation=null;clearViewConflict();
      labelSource(saved?viewName(descriptor)+' · v'+saved.revision:incoming.label||viewName(descriptor),source);changed();paintPresentation();paintRetainedView();paintSettings();paintPage();await refreshViews();await load(0);return true;
    }
    function paintRetainedView(){
      root.querySelector('.case-ui-retained-view')?.remove();if(sourceTransition||!state?.retained_view)return;
      const panel=$('div',undefined,'case-ui-retained-view');presentationPanel.after(panel);panel.append($('p','此前未保存的视图配置仍保留在本来源草稿中。答案草稿按各自定义保留。','muted small'));
      button('恢复先前视图草稿',async()=>{if(!flush())return;const previous=state.retained_view;if(!previous)return;invalidate();Object.assign(state,S.clone(previous),{presentation_origin:S.clone(previous.presentation_origin||null)});delete state.retained_view;configurationChanged=false;page=null;row=null;pageBasis=null;changed();paintRetainedView();paintSettings();paintPage();await load(0);},panel);
    }
    paintSettings();paintPage();paintPending();paintStatus();
    return {element:root,canLeave:flush,
      setSource(value){
        if(disposed)return false;if(candidate&&canonical(candidate)===canonical(value)&&!sourceTransition)return true;if(!flush())return false;
        invalidate();sourceTicket++;candidate=value?S.clone(value):null;clearViewConflict();root.querySelector('.case-ui-view-conflict')?.remove();
        if(!value){source=null;state=null;locator=null;sourceDisplay=null;sourceTransition=false;unavailableReason=null;sourceLabel.textContent='从数据目录选择固定版本表';sourceLabel.title='';page=null;row=null;pageBasis=null;paintRetainedView();paintSettings();paintPage();publishContext('empty');return true;}
        sourceTransition=true;unavailableReason=null;page=null;row=null;pageBasis=null;paintRetainedView();paintPage();publishContext('loading');if(active){const version=ticket;useSource().catch(error=>{if(live(version))info(error.message,true);});}return true;
      },
      setPresentation(value){
        if(disposed)return false;S.bounded(value,65536);
        if(!value||!value.descriptor&&!value.view_ref)throw Error('Notebook Case 输出需要统一视图描述或固定 view_ref');
        if(value.view_ref){S.identity(value.view_ref.id);if(!Number.isSafeInteger(value.view_ref.revision)||value.view_ref.revision<1)throw Error('Notebook view_ref 必须固定到具体版本');}
        else if(!(/^[a-f0-9]{64}$/).test(value.descriptor.source_snapshot))throw Error('Notebook Case 输出必须引用已登记的固定来源');
        presentation=S.clone(value);paintPresentation();return true;
      },
      activate(value){
        if(disposed)return false;if(!value&&!flush())return false;
        active=value;root.hidden=!value;
        if(value&&connected&&!state){const version=ticket;refreshViews().catch(error=>{if(live(version))info(error.message,true);});}
        if(!value){invalidate();document.title=baseTitle;return true;}
        publishContext(sourceTransition?'loading':unavailableReason?'unavailable':source?'ready':'empty');
        if(candidate&&(sourceTransition||!locator||canonical(candidate.locator)!==canonical(locator)))useSource().catch(error=>{if(!disposed)info(error.message,true);});
        else if(row){paintContent();const version=ticket;loadReview().catch(error=>{if(live(version))info(error.message,true);});}
        else if(state&&!page&&!unavailableReason&&!reading){const version=ticket;restorePage().catch(error=>{if(live(version))info(error.message,true);});}
        paintRetainedView();return true;
      },
      updatePermissions(value){permissions=value||{};paintPermissions();paintPending();mediaController?.updatePermissions();if(active&&row)loadReview().catch(error=>{if(!disposed)info(error.message,true);});},
      dispose(){if(disposed)return true;if(!flush())return false;disposed=true;invalidate();mediaController?.dispose();abort.abort();globalThis.removeEventListener('beforeunload',beforeUnload);disposeReaders(root);toolbarActions.remove();document.title=baseTitle;return true;}};
  }
  globalThis.ForgeCase=Object.freeze({mount,hash});
})();
