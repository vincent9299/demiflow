/* Source drafts and request recovery only. Server receipts own execution facts. */
(() => {
  'use strict';
  const encode = new TextEncoder();
  const ID = /^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,95}$/;
  const projectID = /^w[0-9a-f]{32}$/;
  const languages = new Set(['python','sql','markdown']);
  const pythonKeywords=new Set('False None True and as assert async await break class continue def del elif else except finally for from global if import in is lambda nonlocal not or pass raise return try while with yield'.split(' '));
  const storageStates=new Map(),kinds=new Set(['drafts','references','intents','case-drafts']);
  const unavailableStorage={getItem:()=>null,setItem:()=>{throw new Error('浏览器不允许持久保存');}};
  function volatileSources(){return [...storageStates.values()].some(state=>state.has('drafts')||state.has('references')||state.has('case-drafts'));}
  if(globalThis.addEventListener)globalThis.addEventListener('beforeunload',event=>{
    if(volatileSources()){event.preventDefault();event.returnValue='';}
  });
  const fail = message => { throw new Error(message); };
  const clone = value => JSON.parse(JSON.stringify(value));
  const bytes = value => encode.encode(value).byteLength;
  function text(value, maximum, label, empty=false) {
    if(typeof value!=='string'||value.length>maximum||bytes(value)>maximum||value.includes('\0')||(!empty&&!value.trim()))fail(label+'超出长度限制或为空');
    for(const point of value){const code=point.codePointAt(0);if(code>=0xd800&&code<=0xdfff)fail(label+'包含无效 Unicode');}
    return value;
  }
  function fields(value, required, optional=[]) {
    if(!value||typeof value!=='object'||Array.isArray(value)||required.some(key=>!Object.hasOwn(value,key))||Object.keys(value).some(key=>!required.includes(key)&&!optional.includes(key)))fail('Notebook 字段不符合约定');
  }
  function identity(value){if(typeof value!=='string'||!ID.test(value))fail('无效的记录标识');return value;}
  function bounded(value, maximum){const result=JSON.stringify(value);if(result.length>maximum||bytes(result)>maximum)fail('内容超过当前操作的字节上限');return result;}
  function cell(value) {
    fields(value,['id','language','source'],['result_name']);identity(value.id);
    if(!languages.has(value.language))fail('单元仅支持 Python、SQL 或 Markdown');
    text(value.source,16384,'单元代码',true);
    if(Object.hasOwn(value,'result_name')&&(value.language!=='sql'||typeof value.result_name!=='string'||!(/^[A-Za-z_][A-Za-z0-9_]{0,79}$/).test(value.result_name)||value.result_name.startsWith('__')||pythonKeywords.has(value.result_name)))fail('SQL 结果名需要普通 Python 变量名');
    bounded(value,48*1024);return clone(value);
  }
  function document(value) {
    fields(value,['title','cells']);text(value.title,256,'Notebook 名称');
    if(!Array.isArray(value.cells)||value.cells.length>64)fail('每本 Notebook 最多 64 个单元');
    const cells=value.cells.map(cell);
    if(new Set(cells.map(value=>value.id)).size!==cells.length)fail('单元标识重复');
    const result={title:value.title,cells};bounded(result,1024*1024);return result;
  }
  function draftDocument(value) {
    // Incomplete names and temporarily invalid SQL variables are edits, not
    // server operations. Preserve them across navigation within a separate
    // finite local draft bound; strict validation still precedes every save.
    fields(value,['title','cells']);text(value.title,1024,'草稿名称',true);
    if(!Array.isArray(value.cells)||value.cells.length>64)fail('草稿单元数量超限');
    for(const item of value.cells){fields(item,['id','language','source'],['result_name']);identity(item.id);
      if(!languages.has(item.language))fail('草稿单元语言无效');text(item.source,65536,'草稿代码',true);
      if(Object.hasOwn(item,'result_name'))text(item.result_name,256,'草稿变量名',true);
    }
    if(new Set(value.cells.map(item=>item.id)).size!==value.cells.length)fail('草稿单元标识重复');
    bounded(value,2*1024*1024);return clone(value);
  }
  function patch(base,draft,onlyCell=null) {
    base=document(base);draft=document(draft);
    const prior=new Map(base.cells.map(value=>[value.id,value])),next=new Map(draft.cells.map(value=>[value.id,value])),result={};
    if(!onlyCell&&base.title!==draft.title)result.title=draft.title;
    const puts=draft.cells.filter(value=>(!onlyCell||value.id===onlyCell)&&JSON.stringify(value)!==JSON.stringify(prior.get(value.id)));
    if(puts.length)result.put_cells=puts;
    if(!onlyCell){
      const removed=base.cells.filter(value=>!next.has(value.id)).map(value=>value.id);if(removed.length)result.delete_cells=removed;
    }
    const order=onlyCell?[...base.cells.map(value=>value.id),...puts.filter(value=>!prior.has(value.id)).map(value=>value.id)]:draft.cells.map(value=>value.id);
    if(JSON.stringify(order)!==JSON.stringify(base.cells.map(value=>value.id)))result.order=order;
    if(!Object.keys(result).length)return null;
    bounded(result,56*1024);return result;
  }
  function apply(base,change) {
    const cells=new Map(base.cells.map(value=>[value.id,clone(value)]));
    for(const id of change.delete_cells||[])cells.delete(id);
    for(const value of change.put_cells||[])cells.set(value.id,clone(value));
    return document({title:change.title??base.title,cells:(change.order||[...cells.keys()]).map(id=>cells.get(id))});
  }
  class Store {
    constructor(storage){
      if(storage===undefined){try{storage=globalThis.localStorage;}catch{storage=unavailableStorage;}}
      if(!storageStates.has(storage)){
        if(storageStates.size>=4)fail('本窗口草稿存储上下文已达上限');
        storageStates.set(storage,new Map());
      }
      this.storage=storage;this.volatile=storageStates.get(storage);
    }
    read(kind,maximum) {
      if(!kinds.has(kind))fail('未知的本地分析存储类型');
      const raw=this.volatile.get(kind)??this.storage.getItem('demiflow-analysis-'+kind+'-v1');if(raw===null)return [];
      if(raw.length>maximum||bytes(raw)>maximum)fail('本地分析草稿超限；请先导出当前内容');
      const value=JSON.parse(raw);fields(value,['v','items']);
      if(value.v!==1||!Array.isArray(value.items)||value.items.length>32)fail('无法识别本地分析草稿格式');
      return value.items;
    }
    write(kind,items,maximum){
      if(!kinds.has(kind))fail('未知的本地分析存储类型');
      const raw=bounded({v:1,items},maximum);
      // One replacement envelope per kind, with the SAME finite byte/item
      // limits as persistent storage. Never discard it after a quota/denial.
      this.volatile.set(kind,raw);
      try{this.storage.setItem('demiflow-analysis-'+kind+'-v1',raw);this.volatile.delete(kind);return true;}
      catch{return false;}
    }
    hasVolatileSources(){return this.volatile.has('drafts')||this.volatile.has('references')||this.volatile.has('case-drafts');}
    caseDraft(project,digest){
      if(!projectID.test(project)||!(/^[a-f0-9]{64}$/).test(digest))fail('无效的 Case 来源');
      const found=this.read('case-drafts',2*1024*1024).find(item=>item.project===project&&item.digest===digest);
      if(found)bounded(found.value,256*1024);return found?clone(found.value):null;
    }
    saveCaseDraft(project,digest,value){
      if(!projectID.test(project)||!(/^[a-f0-9]{64}$/).test(digest))fail('无效的 Case 来源');bounded(value,256*1024);
      const items=this.read('case-drafts',2*1024*1024).filter(item=>item.project!==project||item.digest!==digest);
      if(items.length>=32||items.filter(item=>item.project===project).length>=4)fail('Case 草稿已满；请先保存或导出已有草稿');
      return this.write('case-drafts',[...items,{project,digest,value:clone(value)}],2*1024*1024);
    }
    removeCaseDraft(project,digest){
      if(!projectID.test(project)||!(/^[a-f0-9]{64}$/).test(digest))fail('无效的 Case 来源');
      const items=this.read('case-drafts',2*1024*1024),next=items.filter(item=>item.project!==project||item.digest!==digest);
      if(next.length!==items.length)this.write('case-drafts',next,2*1024*1024);
    }
    durableIntents(){const items=this.read('intents',512*1024);if(!this.write('intents',items,512*1024))fail('无法持久保存原请求身份；操作尚未提交，请先恢复浏览器存储');}
    draft(project,id) {
      if(!projectID.test(project))fail('无效项目');identity(id);
      const found=this.read('drafts',4*1024*1024).find(item=>item.project===project&&item.id===id);
      if(found){draftDocument(found.document);if(!Number.isSafeInteger(found.revision)||found.revision<0)fail('本地草稿版本无效');}
      return found?clone(found):null;
    }
    drafts(project){if(!projectID.test(project))fail('无效项目');return this.read('drafts',4*1024*1024).filter(item=>item.project===project).map(item=>({...clone(item),document:draftDocument(item.document)}));}
    reference(project,id){if(!projectID.test(project))fail('无效项目');identity(id);return this.read('references',256*1024).find(item=>item.project===project&&item.id===id)||null;}
    saveReference(project,id,value){
      if(!projectID.test(project))fail('无效项目');identity(id);bounded(value,8192);
      const items=this.read('references',256*1024).filter(item=>item.project!==project||item.id!==id);
      if(items.length>=32)fail('本地 Notebook 引用已满；已保存的 Notebook 仍可从服务读取');
      this.write('references',[...items,{project,id,...clone(value)}],256*1024);
    }
    saveDraft(project,id,value) {
      if(!projectID.test(project))fail('无效项目');identity(id);draftDocument(value.document);
      const items=this.read('drafts',4*1024*1024).filter(item=>item.project!==project||item.id!==id);
      if(items.length>=32||items.filter(item=>item.project===project).length>=4)fail('本地草稿已满；请先保存或导出已有草稿');
      this.write('drafts',[...items,{...clone(value),project,id}],4*1024*1024);
    }
    removeDraft(project,id){const items=this.read('drafts',4*1024*1024),next=items.filter(item=>item.project!==project||item.id!==id);if(next.length!==items.length)this.write('drafts',next,4*1024*1024);}
    pending(project){if(!projectID.test(project))fail('无效项目');return this.read('intents',512*1024).filter(item=>item.project===project).map(clone);}
    prepare(project,operation,payload,scope) {
      if(!projectID.test(project))fail('无效项目');identity(operation);identity(payload.request_id);bounded(payload,65536);
      const items=this.read('intents',512*1024),old=items.find(item=>item.project===project&&item.payload.request_id===payload.request_id);
      const value={project,operation,payload:clone(payload),scope:clone(scope)};
      if(old){if(JSON.stringify(old)!==JSON.stringify(value))fail('原请求标识对应另一份内容');this.durableIntents();return clone(old);}
      if(items.length>=32||items.filter(item=>item.project===project).length>=8)fail('待核对操作已满，请先核对原请求');
      const prior=this.volatile.get('intents');
      if(!this.write('intents',[...items,value],512*1024)){
        // No request was dispatched, so this newly generated identity has no
        // uncertain server outcome. Preserve all PREVIOUS pending identities.
        if(prior===undefined)this.volatile.delete('intents');else this.volatile.set('intents',prior);
        fail('无法持久保存原请求身份；操作尚未提交，请先恢复浏览器存储');
      }
      return clone(value);
    }
    finish(project,requestID){this.write('intents',this.read('intents',512*1024).filter(item=>item.project!==project||item.payload.request_id!==requestID),512*1024);}
  }
  function imported(value,title) {
    bounded(value,2*1024*1024);
    if(!value||value.nbformat!==4||!Array.isArray(value.cells)||value.cells.length>64)fail('仅支持 nbformat 4 的源单元导入');
    return document({title,cells:value.cells.map((item,index)=>{
      if(!item||!['code','markdown'].includes(item.cell_type))fail('导入的单元类型不可用');
      const extension=item.metadata?.demiflow||{},language=extension.language||(item.cell_type==='markdown'?'markdown':'python');
      let source=language==='sql'?extension.sql_source:item.source??'';
      if(Array.isArray(source)){if(source.length>16384||source.some(value=>typeof value!=='string'))fail('导入的单元代码无效');source=source.join('');}
      return {id:item.id||'cell-'+(index+1),language,source,...(extension.result_name?{result_name:extension.result_name}:{})};
    })});
  }
  function exported(value) {
    value=document(value);
    return {nbformat:4,nbformat_minor:5,metadata:{kernelspec:{name:'python3',language:'python',display_name:'Python 3'},demiflow:{title:value.title,outputs_imported:false}},cells:value.cells.map(item=>{
      const extension={language:item.language};let source=item.source;
      if(item.language==='sql'){
        extension.sql_source=source;extension.requires='DemiFlow analysis helper and an authorized source binding';
        if(item.result_name)extension.result_name=item.result_name;
        const call='sql('+JSON.stringify(source).replaceAll('\u2028','\\u2028').replaceAll('\u2029','\\u2029')+')';
        source='from demiflow.workbench.analysis.client import sql\n'+(item.result_name?item.result_name+' = '+call+'\n'+item.result_name:call)+'\n';
      }
      return {id:item.id,cell_type:item.language==='markdown'?'markdown':'code',metadata:{demiflow:extension},source,...(item.language==='markdown'?{}:{execution_count:null,outputs:[]})};
    })};
  }
  globalThis.ForgeAnalysisState=Object.freeze({document,draftDocument,cell,patch,apply,Store,imported,exported,bounded,identity,clone,bytes});
})();
