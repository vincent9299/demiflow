/* Configurable Case discussion prototype: fixed synthetic records, no execution API. */
(() => {
  'use strict';
  const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const clone=value=>JSON.parse(JSON.stringify(value));
  const SNAPSHOT='case-template-fixture@1';
  const VIEW_STORE='demiflow-case-view-demo-v1',MARK_STORE='demiflow-case-configurable-marks-v2',LEGACY_STORE='demiflow-case-template-browser-drafts-v1';
  const variants=[{id:'base',label:'Base'},{id:'step1200',label:'Checkpoint 1200'},{id:'step2400',label:'Checkpoint 2400'}];
  const cases=[
    {id:'CASE-001',name:'红色陶瓷杯',group:'陶瓷杯',split:'验证集',task:'图文一致性',prompt:'一只红色陶瓷杯，带圆形把手，放在浅色木桌上。',reference:'red',scores:[64,83,92],notes:['颜色偏蓝，与输入要求不同。','颜色正确；杯身比例偏宽。','颜色、把手与材质较接近要求。']},
    {id:'CASE-002',name:'蓝色波点杯',group:'陶瓷杯',split:'测试集',task:'细节完整性',prompt:'一只带白色波点的蓝色马克杯，柔和自然光。',reference:'dots',scores:[57,78,90],notes:['蓝色正确，但缺少波点。','有波点；部分图案排列不均。','杯体与波点较完整。']},
    {id:'CASE-003',name:'黄色雨伞',group:'雨具',split:'测试集',task:'图文一致性',prompt:'一把展开的黄色雨伞，弯柄，浅灰色背景。',reference:'umbrella',scores:[69,null,89],notes:['伞面偏橙色。','此变体无结果：演示执行失败，保留缺项。','颜色与伞柄较完整。']}
  ];
  const FORMATS={text:'text · 原文',md:'md · Markdown',message:'message · 单条消息',dialogue:'dialogue · 多轮对话',code:'code · 代码',image:'image · 图片',audio:'audio · 音频',video:'video · 视频',tag:'tag · 标签',html:'html · 隔离预览',json:'json · 结构'};
  const ROLES={input:'输入',output:'输出',reference:'参考材料',context:'上下文',evaluation:'模型评测'};
  const QUESTION_TYPES={choice:'单选',multi:'多选标签',rating:'评分',text:'文本备注'};
  const FILTERS={group:'类别',split:'数据划分',task:'任务',best_score:'最高示例分 ≥',mark_status:'标注状态'};
  const GROUPS={none:'不分组',group:'类别',split:'数据划分',task:'任务'};
  const field=(id,source,role,format,title)=>({id,source,role,format,title,language:'python'});
  const question=(id,title,type,options=[],required=false,max=5)=>({id,title,type,options,required,max});
  const DEFAULT={title:'模型输入 / 输出复核',layout:'comparison',groupBy:'group',sortBy:'id',filterFields:['group','best_score','mark_status'],fields:[field('f1','prompt','input','text','任务说明'),field('f2','reference_image','reference','image','参考图'),field('f3','output_image','output','image','生成结果'),field('f4','answer','output','md','模型回答'),field('f5','evaluation','evaluation','json','模型评测')],questions:[question('tags','标签','multi',['收藏','待复核','效果较好','存在问题']),question('verdict','人工结论','choice',['通过','待复核','存在问题']),question('quality','人工评分','rating'),question('note','备注','text')]};
  function image(kind,variant='reference',small=false){
    const label=kind==='umbrella'?'黄色雨伞测试插画':kind==='dots'?'蓝色波点杯测试插画':'红色陶瓷杯测试插画';
    let body='';
    if(kind==='umbrella'){
      const color=variant==='base'?'#dd9353':'#e6bc5e';
      body=`<ellipse cx="150" cy="217" rx="70" ry="9" fill="#dddcd8"/><path d="M151 46V199q0 22 17 18q11-3 7-15" fill="none" stroke="#746d62" stroke-width="6" stroke-linecap="round"/><path d="M48 124q21-75 103-79q79 4 102 79q-24-15-46 0q-24-15-56 0q-28-15-54 0q-22-15-49 0" fill="${color}"/><path d="M151 45q-48 31-54 79M151 45q45 29 56 79M151 46v78" fill="none" stroke="#c99e46" stroke-width="2"/>`;
    }else{
      const color=kind==='dots'?'#6f9dc1':variant==='base'?'#7c9cb8':'#c86f67';
      const width=variant==='step1200'?118:99,left=(300-width)/2-7;
      const dots=kind==='dots'&&variant!=='base'?Array.from({length:8},(_,i)=>`<circle cx="${left+20+(i%3)*28}" cy="${117+Math.floor(i/3)*25}" r="5" fill="#f5f3ef"/>`).join(''):'';
      body=`<path d="M0 190H300V250H0Z" fill="#e5dace"/><path d="M0 218H300M35 190l25 60M226 190l-12 60" stroke="#d8ccbe"/><ellipse cx="149" cy="206" rx="75" ry="10" fill="#cabeb0" opacity=".6"/><path d="M${left+width-2} 110C${left+width+66} 97 ${left+width+62} 177 ${left+width-4} 172" fill="none" stroke="${color}" stroke-width="14"/><path d="M${left} 91q${width/2} 9 ${width} 0v89q0 25-${width/2} 25q-${width/2} 0-${width/2}-25Z" fill="${color}"/><ellipse cx="${left+width/2}" cy="91" rx="${width/2}" ry="12" fill="#edc7b9"/><ellipse cx="${left+width/2}" cy="92" rx="${width/2-6}" ry="7" fill="#70534b"/>${dots}<path d="M${left+13} 111v61" stroke="#fff" opacity=".18" stroke-width="7" stroke-linecap="round"/>`;
    }
    return `<svg class="ct-picture ${small?'ct-picture-small':''}" viewBox="0 0 300 250" role="img" aria-label="${label}"><rect width="300" height="250" fill="#eeefec"/><path d="M0 0H300V190H0Z" fill="#eeefec"/>${body}</svg>`;
  }
  const sourceDefinitions=[
    ['prompt','任务说明','text','Case'],['reference_image','参考图片','image','Case'],['messages','输入消息列表','dialogue','Case'],['context','上下文说明','md','Case'],
    ['output_image','生成图片','image','结果'],['answer','模型回答','md','结果'],['reason','评测理由','text','结果'],['score','示例分数','text','结果'],['evaluation','评测结构','json','结果'],
    ['message','单条模型消息','message','结果'],['conversation','完整对话','dialogue','结果'],['code','示例代码','code','Case'],['metadata','Case 元数据','json','Case'],['tags','数据标签','tag','Case'],
    ['html','HTML 示例','html','Case'],['audio','1 秒测试音','audio','Case'],['video','1 秒测试视频','video','Case']
  ];
  const sourceSpec=id=>sourceDefinitions.find(s=>s[0]===id);
  const getCase=id=>cases.find(c=>c.id===id)||cases[0];
  const getVariant=id=>variants.find(v=>v.id===id)||variants[2];
  const variantIndex=v=>variants.findIndex(item=>item.id===v.id);
  const media=(c,v)=>({kind:'image',subject:c.reference,variant:v?.id||'reference'});
  function fieldValue(source,c,v){
    const i=variantIndex(v),score=c.scores[i],reason=c.notes[i];
    const answer=score===null?null:`**${v.label} · 示例回答**

${reason}

- 示例评分：${score}
- 本条为合成展示材料。`;
    const messages=[{role:'system',content:'请根据输入材料检查生成结果，给出简短结论。'},{role:'user',content:[{type:'text',text:c.prompt},{type:'image',value:media(c)}]}];
    const outputMessage={role:'assistant',content:score===null?'没有可用回答。':reason};
    const values={prompt:c.prompt,reference_image:media(c),messages,context:`### 参考说明

当前 Case：**${c.name}**。保留输入材料，分别查看不同输出。`,output_image:score===null?null:media(c,v),answer,reason,score,evaluation:{score,verdict:score===null?'无结果':score>=80?'较符合':'需要复核',reason,source:'synthetic-model-evaluation@1'},message:outputMessage,conversation:[...messages,outputMessage],code:'def review_case(row):\n    score = row.get("score")\n    return {"needs_review": score is None or score < 80}',metadata:{case_id:c.id,category:c.group,split:c.split,task:c.task,snapshot:SNAPSHOT},tags:[c.group,c.split,c.task],html:`<article><h3>${esc(c.name)}</h3><p>这段内容在隔离框架中预览。</p><strong>合成 HTML 材料</strong><ul><li>${esc(c.group)}</li><li>${esc(c.prompt)}</li></ul></article>`,audio:{kind:'audio',src:'case-demo-tone.wav',label:'1 秒合成测试音'},video:{kind:'video',src:'case-demo-clip.webm',label:'1 秒合成测试视频'}};
    return values[source];
  }
  function options(entries,value){return entries.map(([id,label])=>`<option value="${esc(id)}"${id===value?' selected':''}>${esc(label)}</option>`).join('');}
  const maxBytes=(raw,limit)=>raw.length<=limit&&new TextEncoder().encode(raw).byteLength<=limit;
  function readRecord(key,limit){const raw=localStorage.getItem(key);if(!raw)return null;if(!maxBytes(raw,limit))throw Error('本浏览器记录超过演示读取预算。');return JSON.parse(raw);}
  const isPlain=value=>value&&typeof value==='object'&&!Array.isArray(value);
  function validQuestion(q){return isPlain(q)&&/^(tags|note|verdict|quality|q[1-9][0-9]{0,2})$/.test(q.id)&&typeof q.title==='string'&&q.title.trim().length>0&&q.title.length<=48&&Object.hasOwn(QUESTION_TYPES,q.type)&&typeof q.required==='boolean'&&[5,10].includes(q.max)&&Array.isArray(q.options)&&q.options.length<=8&&new Set(q.options).size===q.options.length&&q.options.every(v=>typeof v==='string'&&v.length>0&&v.length<=32)&&(!['choice','multi'].includes(q.type)||q.options.length>=2);}
  function validView(v){return isPlain(v)&&typeof v.title==='string'&&v.title.trim().length>0&&v.title.length<=60&&['card','comparison','gallery'].includes(v.layout)&&Object.hasOwn(GROUPS,v.groupBy)&&['id','name','best_score'].includes(v.sortBy)&&Array.isArray(v.filterFields)&&v.filterFields.length<=5&&new Set(v.filterFields).size===v.filterFields.length&&v.filterFields.every(f=>Object.hasOwn(FILTERS,f))&&Array.isArray(v.fields)&&v.fields.length<=8&&new Set(v.fields.map(f=>f.id)).size===v.fields.length&&v.fields.every(f=>isPlain(f)&&/^f[1-9][0-9]{0,2}$/.test(f.id)&&sourceSpec(f.source)&&Object.hasOwn(ROLES,f.role)&&Object.hasOwn(FORMATS,f.format)&&typeof f.title==='string'&&f.title.trim().length>0&&f.title.length<=40&&['python','sql','json','text'].includes(f.language))&&Array.isArray(v.questions)&&v.questions.length<=6&&new Set(v.questions.map(q=>q.id)).size===v.questions.length&&v.questions.every(validQuestion);}
  function validRevision(value){return Number.isSafeInteger(value)&&value>=0;}
  function validSelection(config,selection){
    if(!isPlain(selection)||typeof selection.search!=='string'||selection.search.length>80||typeof selection.group!=='string'||!isPlain(selection.filters)||Object.keys(selection.filters).length>5)return false;
    if(selection.group!=='all'&&(config.groupBy==='none'||!cases.some(c=>String(c[config.groupBy])===selection.group)))return false;
    return Object.entries(selection.filters).every(([field,value])=>config.filterFields.includes(field)&&typeof value==='string'&&value.length<=80&&(value===''||field==='best_score'?value===''||/^\d{1,3}(?:\.\d{1,3})?$/.test(value)&&Number(value)<=100:field==='mark_status'?['unmarked','marked','review','saved'].includes(value):cases.some(c=>c[field]===value)));
  }
  function readView(){const record=readRecord(VIEW_STORE,32768);if(record&&(record.format!=='case_view_demo_v1'||record.snapshot!==SNAPSHOT||!validRevision(record.revision)||!validView(record.config)||record.selection!==undefined&&!validSelection(record.config,record.selection)))throw Error('已保存视图的格式或数据快照不匹配，未自动应用。');return record;}
  function definition(q){return {id:q.id,title:q.title,type:q.type,options:[...q.options],required:q.required,max:q.max};}
  const binding=q=>JSON.stringify(definition(q));
  const emptyValue=q=>q.type==='multi'?[]:q.type==='rating'?null:'';
  function validAnswer(q,value){if(q.type==='text')return typeof value==='string'&&value.length<=400;if(q.type==='choice')return value===''||q.options.includes(value);if(q.type==='multi')return Array.isArray(value)&&value.length<=8&&new Set(value).size===value.length&&value.every(v=>q.options.includes(v));return value===null||Number.isInteger(value)&&value>=1&&value<=q.max;}
  function validMark(m){return isPlain(m)&&isPlain(m.entries)&&Object.keys(m.entries).length<=24&&Object.entries(m.entries).every(([key,entry])=>isPlain(entry)&&validQuestion(entry.question)&&binding(entry.question)===key&&validAnswer(entry.question,entry.value));}
  function readMarks(){const record=readRecord(MARK_STORE,131072);if(!record)return {format:'case_marks_demo_v2',snapshot:SNAPSHOT,revision:0,marks:{}};if(record.format!=='case_marks_demo_v2'||record.snapshot!==SNAPSHOT||!validRevision(record.revision)||!isPlain(record.marks)||Object.keys(record.marks).length>12||!Object.entries(record.marks).every(([key,m])=>/^CASE-00[1-3]\/(case|base|step1200|step2400)$/.test(key)&&validMark(m)))throw Error('已保存标注记录格式无效，未自动应用。');return record;}
  const ui={config:clone(DEFAULT),savedConfig:clone(DEFAULT),viewRevision:0,configOpen:false,configTab:'fields',textDrafts:{},configErrors:{},caseId:'CASE-001',target:'case',search:'',group:'all',filters:{},drafts:{},saved:{},revision:0,message:'',viewMessage:'',zoomed:new Set(),savedSelection:{group:'all',filters:{},search:''}};
  try{const record=readView();if(record){ui.config=clone(record.config);ui.savedConfig=clone(record.config);ui.viewRevision=record.revision;const selected=record.selection||{group:'all',filters:{},search:''};ui.group=selected.group;ui.filters=clone(selected.filters);ui.search=selected.search;ui.savedSelection=clone(selected);}}catch(error){ui.viewMessage=error.message;}
  try{const record=readMarks();ui.saved=clone(record.marks);ui.revision=record.revision;
    if(!localStorage.getItem(MARK_STORE)){
      const old=readRecord(LEGACY_STORE,32768),names={favorite:'收藏',review:'待复核',good:'效果较好',issue:'存在问题'};
      if(old&&old.snapshot===SNAPSHOT&&isPlain(old.marks)&&Object.keys(old.marks).length<=12){
        for(const [key,m]of Object.entries(old.marks)){
          if(!/^CASE-00[1-3]\/(case|base|step1200|step2400)$/.test(key)||!isPlain(m)||!Array.isArray(m.tags)||m.tags.length>4||!m.tags.every(v=>Object.hasOwn(names,v))||typeof m.note!=='string'||m.note.length>400)continue;
          const entries={};for(const [id,value]of [['tags',m.tags.map(v=>names[v])],['note',m.note]]){const q=DEFAULT.questions.find(q=>q.id===id);if(value.length)entries[binding(q)]={question:definition(q),value};}if(Object.keys(entries).length)ui.saved[key]={entries};
        }
        if(Object.keys(ui.saved).length)ui.message='已读取旧样稿的浏览器标签与备注；原记录保留，保存后使用新版浏览器草稿。';
      }
    }
  }catch(error){ui.message=error.message;}
  let fieldSequence=Math.max(0,...ui.config.fields.map(f=>Number(f.id.slice(1))))+1;
  let questionSequence=Math.max(0,...ui.config.questions.map(q=>Number(q.id.slice(1))||0),...Object.values(ui.saved).flatMap(m=>Object.values(m.entries).map(e=>Number(e.question.id.slice(1))||0)))+1;
  const markKey=()=>ui.caseId+'/'+ui.target;
  const mark=key=>ui.drafts[key]||ui.saved[key]||{entries:{}};
  const currentAnswer=(q,m=mark(markKey()))=>m.entries[binding(q)]?.value??emptyValue(q);
  const nonempty=value=>Array.isArray(value)?value.length>0:value!==null&&value!=='';
  function normalized(m){return JSON.stringify(Object.keys(m.entries).sort().map(k=>[k,m.entries[k]]));}
  const dirty=key=>!!ui.drafts[key]&&normalized(ui.drafts[key])!==normalized(ui.saved[key]||{entries:{}});
  const dirtyKeys=()=>Object.keys(ui.drafts).filter(dirty);
  const selection=()=>({group:ui.group,filters:clone(ui.filters),search:ui.search});
  const description=()=>({...ui.config,selection:selection()});
  const viewDirty=()=>JSON.stringify(ui.config)!==JSON.stringify(ui.savedConfig)||JSON.stringify(selection())!==JSON.stringify(ui.savedSelection);
  const marked=m=>Object.values(m.entries).some(entry=>nonempty(entry.value));
  const caseMarked=c=>['case',...variants.map(v=>v.id)].some(t=>ui.config.questions.some(q=>nonempty(currentAnswer(q,mark(c.id+'/'+t)))));
  const caseNeedsReview=c=>['case',...variants.map(v=>v.id)].some(t=>ui.config.questions.some(q=>{const value=currentAnswer(q,mark(c.id+'/'+t));return Array.isArray(value)?value.includes('待复核'):value==='待复核';}));
  const bestScore=c=>Math.max(...c.scores.filter(v=>v!==null));
  function selectedCases(){
    const result=cases.filter(c=>{
      const groupValue=ui.config.groupBy==='none'?'全部':c[ui.config.groupBy];
      if(ui.group!=='all'&&String(groupValue)!==ui.group)return false;
      if(ui.search&&!(c.id+' '+c.name+' '+c.prompt).toLowerCase().includes(ui.search.toLowerCase()))return false;
      return ui.config.filterFields.every(f=>{const value=ui.filters[f];if(value===undefined||value==='')return true;if(f==='best_score')return bestScore(c)>=Number(value);if(f==='mark_status')return value==='unmarked'?!caseMarked(c):value==='marked'?caseMarked(c):value==='review'?caseNeedsReview(c):['case',...variants.map(v=>v.id)].some(t=>marked(ui.saved[c.id+'/'+t]||{entries:{}}));return c[f]===value;});
    });
    return result.sort((a,b)=>ui.config.sortBy==='best_score'?bestScore(b)-bestScore(a):String(a[ui.config.sortBy]).localeCompare(String(b[ui.config.sortBy])));
  }
  function neighbor(delta){const ordered=[...cases].sort((a,b)=>ui.config.sortBy==='best_score'?bestScore(b)-bestScore(a):String(a[ui.config.sortBy]).localeCompare(String(b[ui.config.sortBy]))),visible=new Set(selectedCases().map(c=>c.id)),index=ordered.findIndex(c=>c.id===ui.caseId);for(let offset=1;offset<=ordered.length;offset++){const c=ordered[(index+delta*offset+ordered.length)%ordered.length];if(visible.has(c.id))return c.id;}return null;}
  function jsonTree(value,depth=0,budget={nodes:64}){
    if(--budget.nodes<0||depth>4)return '<span class="ct-quiet">结构预览达到上限</span>';
    if(value===null||typeof value!=='object')return `<span class="ct-json-scalar">${esc(JSON.stringify(value))}</span>`;
    const entries=Object.entries(value),shown=entries.slice(0,16);
    return `<details class="ct-json-node"${depth===0?' open':''}><summary>${Array.isArray(value)?'[':'{'} ${entries.length} 项 ${Array.isArray(value)?']':'}'}</summary><div>${shown.map(([key,item])=>`<div class="ct-json-entry"><b>${esc(key)}</b>${jsonTree(item,depth+1,budget)}</div>`).join('')}${entries.length>shown.length?'<span class="ct-quiet">其余字段未展开</span>':''}</div></details>`;
  }
  function markdown(text){
    const inline=line=>esc(line).replace(/\*\*([^*\n]+)\*\*/g,'<strong>$1</strong>').replace(/`([^`\n]+)`/g,'<code>$1</code>');
    return `<div class="ct-markdown">${text.split('\n').slice(0,80).map(line=>/^#{1,3} /.test(line)?`<h4>${inline(line.replace(/^#{1,3} /,''))}</h4>`:line.startsWith('- ')?`<div class="ct-md-item">• ${inline(line.slice(2))}</div>`:line?`<p>${inline(line)}</p>`:'').join('')}</div>`;
  }
  function imagePreview(value,key){
    const zoomed=ui.zoomed.has(key);
    return `<div class="ct-image-preview${zoomed?' is-zoomed':''}"><div class="ct-image-viewport"><button type="button" class="ct-image-button" data-ct-zoom="${esc(key)}" aria-label="${zoomed?'还原':'放大'}图片预览">${image(value.subject,value.variant)}</button></div><button type="button" class="ct-text-action" data-ct-zoom="${esc(key)}">${zoomed?'还原预览':'原位放大'}</button></div>`;
  }
  function messagePreview(message,key){
    const parts=typeof message.content==='string'?[{type:'text',text:message.content}]:message.content;
    return `<article class="ct-message ct-role-${esc(message.role)}"><div class="ct-message-role">${esc(message.role)}</div><div>${parts.slice(0,8).map((part,i)=>part.type==='text'?`<p>${esc(part.text)}</p>`:part.type==='image'&&part.value?.kind==='image'?imagePreview(part.value,key+'/part'+i):'<p class="ct-format-warning">此消息片段无法预览</p>').join('')}</div></article>`;
  }
  const validMessage=value=>isPlain(value)&&typeof value.role==='string'&&(typeof value.content==='string'||Array.isArray(value.content)&&value.content.length<=8&&value.content.every(p=>p&&typeof p.type==='string'));
  function renderValue(value,format,key,language){
    if(value===null||value===undefined)return '<div class="ct-missing">暂无结果<small>保留缺项，不补 0 或其他方案的内容</small></div>';
    const mismatch=expected=>`<div class="ct-format-warning" role="status">格式不匹配：${expected}。下方保留原值。</div><pre class="ct-raw">${esc(typeof value==='string'?value:JSON.stringify(value,null,2))}</pre>`;
    if(format==='json')return `<div class="ct-json-tree">${jsonTree(value)}</div>`;
    if(format==='image')return value?.kind==='image'?imagePreview(value,key):mismatch('请选择图片字段');
    if(format==='audio'||format==='video'){
      if(value?.kind!==format)return mismatch(`请选择${format==='audio'?'音频':'视频'}字段`);
      return `<div class="ct-media-preview"><${format} class="ct-player" controls preload="none"${format==='video'?' playsinline':''} src="${esc(value.src)}" aria-label="${esc(value.label)}"></${format}><p>${esc(value.label)} · 合成媒体</p></div>`;
    }
    if(format==='message')return validMessage(value)?messagePreview(value,key):mismatch('message 需要单条 role/content 消息');
    if(format==='dialogue')return Array.isArray(value)&&value.length<=12&&value.every(validMessage)?`<div class="ct-dialogue">${value.map((m,i)=>messagePreview(m,key+'/'+i)).join('')}</div>`:mismatch('dialogue 需要消息列表');
    if(format==='tag')return (typeof value==='string'||Array.isArray(value)&&value.length<=8&&value.every(v=>typeof v==='string'))?`<div class="ct-tag-values">${(Array.isArray(value)?value:[value]).map(v=>`<span>${esc(v)}</span>`).join('')}</div>`:mismatch('tag 需要文本或文本列表');
    if(format==='text')return ['string','number','boolean'].includes(typeof value)?`<div class="ct-text-value">${esc(value)}</div>`:mismatch('text 需要基础值，可切换 json 查看结构');
    if(typeof value!=='string')return mismatch(`${format} 需要文本字段`);
    if(format==='md')return markdown(value);
    if(format==='code')return `<div class="ct-code-language">${esc(language)}</div><ol class="ct-code-lines">${value.split('\n').slice(0,80).map(line=>`<li><code>${esc(line)||' '}</code></li>`).join('')}</ol>`;
    if(format==='html'){
      const srcdoc=`<!doctype html><meta charset="utf-8"><meta http-equiv="Content-Security-Policy" content="default-src 'none'; base-uri 'none'; form-action 'none'"><body>${value}</body>`;
      return `<iframe class="ct-html-preview" sandbox="" title="隔离 HTML 预览" srcdoc="${esc(srcdoc)}"></iframe><details class="ct-render-source"><summary>查看 HTML 源码</summary><pre class="ct-raw">${esc(value)}</pre></details>`;
    }
    return mismatch('此格式暂不可用');
  }
  function renderField(f,c,v){
    const value=fieldValue(f.source,c,v),key=c.id+'/'+v.id+'/'+f.id;
    return `<section class="ct-field-preview" data-preview-field="${f.id}" data-preview-source="${f.source}" data-preview-format="${f.format}"><div class="ct-field-label"><strong>${esc(f.title)}</strong><span>${f.format}</span></div>${renderValue(value,f.format,key,f.language)}</section>`;
  }
  const configText=(id,value)=>Object.hasOwn(ui.textDrafts,id)?ui.textDrafts[id]:value;
  const configStatus=()=>Object.keys(ui.configErrors).length?'配置待修正：'+Object.values(ui.configErrors)[0]+'；预览保留最近有效值。':ui.viewMessage||(viewDirty()?'视图有未保存修改；可直接预览，刷新会恢复最近保存版本。':ui.viewRevision?'已保存到本浏览器 · 视图版本 '+ui.viewRevision:'当前为默认示例视图。');
  function fieldEditor(){
    return `<div class="ct-config-description">选择原字段、放置区域和预览格式，下面的 Case 立即更新。最多 8 个展示字段。</div><div class="ct-field-editor">${ui.config.fields.map(f=>`<section class="ct-field-row" data-config-field="${f.id}"><label>原字段<select id="ct-source-${f.id}" data-ct-field="${f.id}" data-ct-prop="source">${sourceDefinitions.map(([id,label,,scope])=>`<option value="${id}"${f.source===id?' selected':''}>${esc(label)} · ${scope}</option>`).join('')}</select></label><label>展示区域<select id="ct-role-${f.id}" data-ct-field="${f.id}" data-ct-prop="role">${options(Object.entries(ROLES),f.role)}</select></label><label>预览格式<select id="ct-format-${f.id}" data-ct-field="${f.id}" data-ct-prop="format">${options(Object.entries(FORMATS),f.format)}</select></label><label>显示名称<input id="ct-label-${f.id}" data-ct-field="${f.id}" data-ct-prop="title" maxlength="40" value="${esc(configText('ct-label-'+f.id,f.title))}"></label>${f.format==='code'?`<label>代码语言<select id="ct-language-${f.id}" data-ct-field="${f.id}" data-ct-prop="language">${options([['python','Python'],['sql','SQL'],['json','JSON'],['text','原文']],f.language)}</select></label>`:''}<button type="button" data-ct-remove-field="${f.id}" class="ct-remove" aria-label="移除字段 ${esc(f.title)}">移除</button></section>`).join('')||'<p class="ct-empty">还没有展示字段，添加后即可预览。</p>'}</div><button type="button" data-ct="add-field"${ui.config.fields.length>=8||fieldSequence>999?' disabled':''}>＋ 添加字段</button>`;
  }
  function filterEditor(){
    return `<div class="ct-config-description">选择分组、排序和允许使用的筛选字段。左侧选样区按这里的配置生成。</div><div class="ct-config-grid"><label>分组字段<select id="ct-config-group">${options(Object.entries(GROUPS),ui.config.groupBy)}</select></label><label>排序方式<select id="ct-config-sort">${options([['id','Case ID'],['name','Case 名称'],['best_score','最高示例分 · 从高到低']],ui.config.sortBy)}</select></label></div><fieldset class="ct-filter-definitions"><legend>筛选字段</legend>${Object.entries(FILTERS).map(([id,label])=>`<label><input type="checkbox" data-ct-filter-def="${id}"${ui.config.filterFields.includes(id)?' checked':''}>${label}</label>`).join('')}</fieldset><p class="ct-config-note">界面筛选只作用于 3 条合成 Case；不执行 SQL、Python、模型或管道。</p>`;
  }
  function questionEditor(){
    return `<div class="ct-config-description">配置人工标注项，模型评测不会被这些记录覆盖。修改题目定义后，旧答案独立保留。最多 6 项。</div><div class="ct-question-editor">${ui.config.questions.map(q=>`<section class="ct-question-row" data-config-question="${q.id}"><label>标注项名称<input id="ct-question-title-${q.id}" data-ct-question="${q.id}" data-ct-qprop="title" maxlength="48" value="${esc(configText('ct-question-title-'+q.id,q.title))}"></label><label>控件类型<select id="ct-question-type-${q.id}" data-ct-question="${q.id}" data-ct-qprop="type">${options(Object.entries(QUESTION_TYPES),q.type)}</select></label>${['choice','multi'].includes(q.type)?`<label class="ct-question-options">选项（逗号分隔，2–8 项）<input id="ct-question-options-${q.id}" data-ct-question="${q.id}" data-ct-qprop="options" maxlength="264" value="${esc(configText('ct-question-options-'+q.id,q.options.join('，')))}"></label>`:q.type==='rating'?`<label>评分范围<select id="ct-question-max-${q.id}" data-ct-question="${q.id}" data-ct-qprop="max">${options([['5','1–5'],['10','1–10']],String(q.max))}</select></label>`:'<span class="ct-config-note">文本最多 400 字符</span>'}<label class="ct-required-check"><input type="checkbox" data-ct-question="${q.id}" data-ct-qprop="required"${q.required?' checked':''}>必填</label><button type="button" data-ct-remove-question="${q.id}" class="ct-remove" aria-label="移除标注项 ${esc(q.title)}">移除</button></section>`).join('')||'<p class="ct-empty">当前仅查看 Case。可添加人工标注项。</p>'}</div><button type="button" data-ct="add-question"${ui.config.questions.length>=6||questionSequence>999?' disabled':''}>＋ 添加标注项</button>`;
  }
  function configuration(){
    if(!ui.configOpen)return '';
    return `<section class="ct-configuration" aria-label="Case 视图配置"><div class="ct-config-heading"><label>视图名称<input id="ct-view-title" maxlength="60" value="${esc(configText('ct-view-title',ui.config.title))}"></label><label>快速试用预设<select id="ct-preset"><option value="">选择预设</option><option value="images">图像结果对照</option><option value="dialogue">对话内容复核</option><option value="media">音视频与结构字段</option></select></label><div class="ct-view-actions"><button type="button" class="primary" id="ct-save-view" data-ct="save-view">保存视图</button><button type="button" id="ct-restore-view" data-ct="restore-view">恢复已保存视图</button></div></div><nav class="ct-config-tabs" aria-label="视图配置分类">${[['fields','内容字段与格式'],['filters','分组与筛选'],['questions','标注项']].map(([id,label])=>`<button type="button" data-ct-config-tab="${id}" class="${ui.configTab===id?'is-active':''}" aria-pressed="${ui.configTab===id}">${label}</button>`).join('')}</nav>${({fields:fieldEditor,filters:filterEditor,questions:questionEditor})[ui.configTab]()}<div class="ct-view-status" role="status">${esc(configStatus())}</div><details class="ct-view-descriptor"><summary>查看当前视图描述与 Notebook 调用示意</summary><pre>${esc(JSON.stringify(description(),null,2))}</pre><code>display(case_view(records, view=当前视图描述))</code><p>接口为设计示意，本页未执行 Python。界面预览和描述共用当前配置。</p></details></section>`;
  }
  function filterControl(id){
    const value=ui.filters[id]??'';
    if(id==='best_score')return `<label class="ct-filter-control">${FILTERS[id]}<input id="ct-filter-${id}" data-ct-filter="${id}" type="number" min="0" max="100" placeholder="不限" value="${esc(value)}"></label>`;
    const values=id==='mark_status'?[['unmarked','未标记'],['marked','已标记 / 有草稿'],['review','待复核'],['saved','已保存过标注']]:[...new Set(cases.map(c=>c[id]))].map(v=>[v,v]);
    return `<label class="ct-filter-control">${FILTERS[id]}<select id="ct-filter-${id}" data-ct-filter="${id}"><option value="">不限</option>${options(values,value)}</select></label>`;
  }
  function navigation(visible){
    const groupValues=ui.config.groupBy==='none'?[]:[...new Set(cases.map(c=>String(c[ui.config.groupBy])))];
    const grouped=new Map();for(const c of visible){const key=ui.config.groupBy==='none'?'全部':String(c[ui.config.groupBy]);if(!grouped.has(key))grouped.set(key,[]);grouped.get(key).push(c);}
    return `<aside class="ct-case-index" aria-label="分组与筛选"><div class="ct-index-heading"><h3>分组与筛选</h3><span>${visible.length} / ${cases.length}</span></div><input id="ct-search" type="search" maxlength="80" value="${esc(ui.search)}" placeholder="搜索 ID / 名称" aria-label="搜索 Case">${ui.config.groupBy!=='none'?`<label class="ct-filter-control">按${GROUPS[ui.config.groupBy]}分组<select id="ct-group"><option value="all">所有分组</option>${options(groupValues.map(v=>[v,v]),ui.group)}</select></label>`:''}${ui.config.filterFields.map(filterControl).join('')}<button type="button" class="ct-text-action" data-ct="clear-filters">清除筛选</button><div class="ct-case-list">${[...grouped].map(([group,items])=>`<div class="ct-list-group"><h4>${esc(group)} <span>${items.length}</span></h4>${items.map(c=>`<button type="button" data-ct-case="${c.id}" data-ct-target="case" class="${c.id===ui.caseId?'is-active':''}"><small>${c.id}</small><strong>${esc(c.name)}</strong><span>${esc(c.split)} · 示例分 ${bestScore(c)}</span>${caseMarked(c)?'<em>已有标注 / 草稿</em>':''}</button>`).join('')}</div>`).join('')||'<p class="ct-empty">暂无匹配 Case</p>'}</div><p class="ct-index-note">当前记录不匹配筛选时，保留详情与草稿。布局和筛选不会执行模型。</p></aside>`;
  }
  function resultCard(c,v,compact=false){
    const i=variantIndex(v),missing=c.scores[i]===null,fields=ui.config.fields.filter(f=>f.role==='output'),evaluation=ui.config.fields.filter(f=>f.role==='evaluation');
    return `<article class="ct-variant ${ui.caseId===c.id&&ui.target===v.id?'is-selected':''}${compact?' is-compact':''}" data-variant="${v.id}"><div class="ct-variant-top"><strong>${esc(v.label)}</strong><span>${missing?'暂无结果':'示例分 '+c.scores[i]}</span></div>${fields.length?fields.map(f=>renderField(f,c,v)).join(''):'<p class="ct-empty">未配置输出字段</p>'}${evaluation.length?`<details class="ct-model-evaluation"><summary>模型评测 · ${missing?'无结果':c.scores[i]+' 分'}</summary>${evaluation.map(f=>renderField(f,c,v)).join('')}</details>`:''}<button type="button" class="ct-result-select" data-ct-case="${c.id}" data-ct-target="${v.id}">${ui.caseId===c.id&&ui.target===v.id?'正在复核此输出':'复核此输出'}</button></article>`;
  }
  function content(c,visible){
    const selected=getVariant(ui.target),shared=ui.config.fields.filter(f=>!['output','evaluation'].includes(f.role));
    let output;
    if(ui.config.layout==='gallery'){
      const groups=new Map();for(const item of visible){const key=ui.config.groupBy==='none'?'全部':String(item[ui.config.groupBy]);if(!groups.has(key))groups.set(key,[]);groups.get(key).push(item);}
      output=[...groups].map(([name,items])=>`<section class="ct-gallery-group"><h3>${esc(name)}</h3><div class="ct-gallery">${items.map(item=>`<div class="ct-gallery-item"><button type="button" class="ct-gallery-case" data-ct-case="${item.id}" data-ct-target="case">${esc(item.name)} · ${item.id}</button>${resultCard(item,selected,true)}</div>`).join('')}</div></section>`).join('')||'<p class="ct-empty">当前筛选没有结果，当前 Case 详情与标注仍保留。</p>';
    }else if(ui.config.layout==='card')output=`<div class="ct-single-output">${resultCard(c,selected)}</div>`;
    else output=`<div class="ct-comparison">${variants.map(v=>resultCard(c,v)).join('')}</div>`;
    return `<main class="ct-output" aria-label="Case 内容"><div class="ct-current-heading"><div><h2>${esc(c.name)}</h2><p>${c.id} · ${esc(c.group)} · ${esc(c.split)}</p></div><div class="ct-navigation"><button type="button" data-ct="previous" aria-label="上一个 Case">←</button><button type="button" data-ct="next" aria-label="下一个 Case">→</button></div></div>${!visible.some(item=>item.id===c.id)?'<p class="ct-inline-note">当前 Case 不在筛选结果中，详情和草稿仍保留。可从左侧选择或清除筛选。</p>':''}<div class="ct-shared-fields">${shared.map(f=>`<div class="ct-shared-field"><span class="ct-role-label">${ROLES[f.role]}${sourceSpec(f.source)[3]==='结果'?' · '+esc(selected.label):''}</span>${renderField(f,c,selected)}</div>`).join('')||'<p class="ct-empty">尚未配置输入或参考字段，可从“配置视图”添加。</p>'}</div><div class="ct-output-heading"><span>输出 / 模型评测 · 合成材料</span><div class="ct-layout-switch">${[['card','单条'],['comparison','对照'],['gallery','画廊']].map(([id,label])=>`<button type="button" data-ct-layout="${id}" class="${ui.config.layout===id?'is-active':''}" aria-pressed="${ui.config.layout===id}">${label}</button>`).join('')}</div></div>${ui.config.layout==='gallery'?`<p class="ct-gallery-scope">画廊展示当前方案 ${esc(selected.label)} · ${visible.length} 个 Case；可在右侧标注对象中切换方案。</p>`:''}${output}<details class="ct-evidence"><summary>原始记录与当前字段绑定</summary><pre>${esc(JSON.stringify({snapshot:SNAPSHOT,case_id:c.id,variants:variants.map((v,i)=>({id:v.id,score:c.scores[i],reason:c.notes[i]})),fields:ui.config.fields.map(f=>({source:f.source,role:f.role,format:f.format}))},null,2))}</pre><p>角色标签仅说明展示区域；这些合成材料不是实际模型请求/回答证据。</p></details></main>`;
  }
  function answerControl(q,m){
    const value=currentAnswer(q,m),id=q.id==='note'?'ct-note':'ct-answer-'+q.id;
    if(q.type==='multi')return `<div class="ct-tag-choices">${q.options.map((option,i)=>`<button type="button" data-ct-multi="${q.id}" data-ct-option="${i}" class="${value.includes(option)?'is-active':''}" aria-pressed="${value.includes(option)}">${esc(option)}</button>`).join('')}</div>`;
    if(q.type==='text')return `<textarea id="${id}" data-ct-answer="${q.id}" rows="3" maxlength="400" placeholder="记录观察或复核理由">${esc(value)}</textarea>`;
    const entries=q.type==='rating'?Array.from({length:q.max},(_,i)=>[String(i+1),String(i+1)+' 分']):q.options.map(option=>[option,option]);
    return `<select id="${id}" data-ct-answer="${q.id}"><option value="">未填写</option>${options(entries,value===null?'':String(value))}</select>`;
  }
  function annotation(c){
    const m=mark(markKey()),active=new Set(ui.config.questions.map(binding)),archived=Object.values(m.entries).filter(e=>!active.has(binding(e.question))&&nonempty(e.value));
    return `<aside class="ct-mark-panel" aria-label="评测与标注"><div class="ct-section-heading"><h3>评测与标注</h3><span class="ct-local-label">浏览器草稿</span></div><label class="ct-control-label"><span class="ct-mark-object">${c.id}</span> · 标注对象<select id="ct-target" aria-label="标记对象"><option value="case"${ui.target==='case'?' selected':''}>整个 Case</option>${options(variants.map(v=>[v.id,v.label]),ui.target)}</select></label><div class="ct-review-questions">${ui.config.questions.map(q=>{const prior=Object.values(m.entries).some(e=>e.question.id===q.id&&binding(e.question)!==binding(q)&&nonempty(e.value));return `<div class="ct-review-question" data-review-question="${q.id}"><label class="ct-question-label"${q.type==='multi'?'':` for="${q.id==='note'?'ct-note':'ct-answer-'+q.id}"`}>${esc(q.title)}${q.required?'<span class="ct-required">必填</span>':''}</label>${prior&&!m.entries[binding(q)]?'<p class="ct-definition-changed">题目定义已变化，旧答案保留；请按当前定义重新填写。</p>':''}${answerControl(q,m)}</div>`;}).join('')||'<p class="ct-empty">未配置人工标注项，当前可自由查看 Case。</p>'}</div><p class="ct-mark-status" id="ct-mark-status">${dirty(markKey())?'有未保存的标注修改':ui.saved[markKey()]?'已保存在本浏览器':'尚未标注'}</p><div class="ct-save-actions"><button type="button" class="primary" data-ct="save"${ui.config.questions.length?'':' disabled'}>保存标注</button><button type="button" data-ct="save-next"${ui.config.questions.length?'':' disabled'}>保存并下一条</button><button type="button" class="ct-text-action" data-ct="reset">撤销当前修改</button></div>${archived.length?`<details class="ct-previous-answers"><summary>其他定义下的标注 (${archived.length})</summary>${archived.map(e=>`<div><strong>${esc(e.question.title)} · ${QUESTION_TYPES[e.question.type]}</strong><p>${esc(Array.isArray(e.value)?e.value.join('、'):e.value)}</p></div>`).join('')}</details>`:''}<details class="ct-mark-binding"><summary>身份与保存位置</summary><p><code>${SNAPSHOT}<br>${esc(markKey())}</code></p><p>Case/输出分别标注，答案绑定题目定义。只写本浏览器，不改原数据、模型评测或 Notebook 输出。</p></details></aside>`;
  }
  function html(project){
    if(project!=='native')return '<section class="ad-empty-project"><h2>Case 分析</h2><p>选择“订单归档 · 示例”查看独立的合成数据。</p></section>';
    const c=getCase(ui.caseId),visible=selectedCases();
    return `<section id="case-template-demo" class="ct-demo"><div class="ct-header"><div><h2>${esc(ui.config.title)}</h2><p>固定合成快照 · 3 个 Case / 3 个输出方案</p></div><div class="ct-header-actions"><span>${ui.config.fields.length} 个字段 · ${ui.config.questions.length} 个标注项${viewDirty()?' · 视图待保存':''}</span><button type="button" id="ct-config-toggle" data-ct="toggle-config" aria-expanded="${ui.configOpen}" class="${ui.configOpen?'is-active':'primary'}">${ui.configOpen?'收起配置':'配置视图'}</button></div></div>${!ui.configOpen&&(ui.viewMessage||Object.keys(ui.configErrors).length)?`<p class="ct-inline-note">${esc(configStatus())}</p>`:''}${configuration()}<div class="ct-layout">${navigation(visible)}${content(c,visible)}${annotation(c)}</div><div class="ct-bottom"><span id="ct-global-draft-count">${dirtyKeys().length} 项标注未保存 · 浏览器标注版本 ${ui.revision}</span><p id="ct-message" aria-live="polite">${esc(ui.message||'输入/输出、媒体与分数均为合成示例；页面操作不发起模型或业务作业。')}</p></div></section>`;
  }
  function unmount(){const root=document.getElementById('case-template-demo');if(root)for(const media of root.querySelectorAll('audio,video')){media.pause();media.removeAttribute('src');media.load();}}
  function mount(project){
    const root=document.getElementById('case-template-demo');if(!root||root.dataset.mounted)return;root.dataset.mounted='true';
    function repaint(){
      const active=document.activeElement,focus=active&&root.contains(active)?active.id:'',start=active?.selectionStart,end=active?.selectionEnd;
      unmount();root.outerHTML=html(project);mount(project);
      if(focus){const next=document.getElementById(focus);if(next){next.focus({preventScroll:true});if(typeof start==='number'&&['text','search'].includes(next.type)||next?.tagName==='TEXTAREA')try{next.setSelectionRange(start,end);}catch{}}}
    }
    function changed(){ui.viewMessage='';ui.zoomed.clear();repaint();}
    function setAnswer(q,value){
      if(!validAnswer(q,value))throw Error('标注值不符合当前题目定义。');
      const key=markKey(),current=mark(key),id=binding(q);
      if(!Object.hasOwn(current.entries,id)&&Object.keys(current.entries).length>=24)throw Error('当前对象已达到 24 份题目定义的演示上限，已有记录保留。');
      if(!ui.drafts[key])ui.drafts[key]=clone(current);
      ui.drafts[key].entries[id]={question:definition(q),value:clone(value)};
      root.querySelector('#ct-mark-status').textContent=dirty(key)?'有未保存的标注修改':'与最近保存值一致';
      root.querySelector('#ct-global-draft-count').textContent=`${dirtyKeys().length} 项标注未保存 · 浏览器标注版本 ${ui.revision}`;
    }
    function saveMarks(advance){
      const nextId=neighbor(1);
      try{
        const missing=ui.config.questions.filter(q=>q.required&&!nonempty(currentAnswer(q)));if(missing.length)throw Error('请先填写必填项：'+missing.map(q=>q.title).join('、'));
        const current=readMarks();if(current.revision!==ui.revision)throw Error('另一个页面更新了浏览器标注，请刷新核对；当前草稿仍保留。');
        if(ui.revision>=Number.MAX_SAFE_INTEGER)throw Error('浏览器标注版本达到上限。');
        const next=clone(ui.saved),value=clone(mark(markKey()));if(marked(value))next[markKey()]=value;else delete next[markKey()];
        const record={format:'case_marks_demo_v2',snapshot:SNAPSHOT,revision:ui.revision+1,marks:next},raw=JSON.stringify(record);if(!maxBytes(raw,131072))throw Error('浏览器标注超过 128KiB 演示上限。');
        localStorage.setItem(MARK_STORE,raw);ui.saved=next;ui.revision=record.revision;delete ui.drafts[markKey()];ui.message='标注已保存在本浏览器；没有写入项目数据。';
        if(advance&&nextId){ui.caseId=nextId;ui.target='case';}
      }catch(error){ui.message='保存未完成，草稿仍保留：'+error.message;}
      repaint();
    }
    function saveView(){
      try{
        if(Object.keys(ui.configErrors).length)throw Error('请先修正配置：'+Object.values(ui.configErrors).join('、'));
        if(!validView(ui.config))throw Error('视图配置无效。');const current=readView();if((current?.revision||0)!==ui.viewRevision)throw Error('另一个页面保存了视图，当前配置保留；可恢复已保存视图后核对。');
        if(ui.viewRevision>=Number.MAX_SAFE_INTEGER)throw Error('视图版本达到上限。');
        const record={format:'case_view_demo_v1',snapshot:SNAPSHOT,revision:ui.viewRevision+1,config:clone(ui.config),selection:selection()},raw=JSON.stringify(record);if(!maxBytes(raw,32768))throw Error('视图超过32KiB演示上限。');
        localStorage.setItem(VIEW_STORE,raw);ui.savedConfig=clone(ui.config);ui.savedSelection=selection();ui.viewRevision=record.revision;ui.viewMessage='视图已保存在本浏览器，刷新后可继续使用。';
      }catch(error){ui.viewMessage='保存视图未完成，当前配置仍保留：'+error.message;}
      repaint();
    }
    function applyPreset(name){
      const next=clone(DEFAULT);
      if(name==='dialogue'){next.title='对话输入与输出复核';next.layout='card';next.groupBy='task';next.filterFields=['task','split','mark_status'];next.fields=[field('f1','messages','input','dialogue','输入消息'),field('f2','message','output','message','模型消息'),field('f3','answer','output','md','模型回答'),field('f4','evaluation','evaluation','json','模型评测')];}
      if(name==='media'){next.title='音视频与结构字段预览';next.layout='card';next.fields=[field('f1','audio','input','audio','测试音频'),field('f2','video','input','video','测试视频'),field('f3','code','output','code','示例代码'),field('f4','metadata','context','json','Case 元数据'),field('f5','html','output','html','HTML 预览')];}
      ui.config=next;ui.textDrafts={};ui.configErrors={};ui.group='all';ui.filters={};ui.search='';fieldSequence=Math.max(fieldSequence,...next.fields.map(f=>Number(f.id.slice(1))+1));ui.viewMessage='已载入预设，可继续调整；原标注与草稿保留。';ui.zoomed.clear();repaint();
    }
    root.addEventListener('play',e=>{if(e.target.matches('audio,video'))for(const player of root.querySelectorAll('audio,video'))if(player!==e.target)player.pause();},true);
    function previewOnly(){
      const layout=root.querySelector('.ct-layout'),c=getCase(ui.caseId),visible=selectedCases();
      unmount();layout.outerHTML=`<div class="ct-layout">${navigation(visible)}${content(c,visible)}${annotation(c)}</div>`;
      root.querySelector('.ct-header h2').textContent=ui.config.title;
      root.querySelector('.ct-header-actions>span').textContent=`${ui.config.fields.length} 个字段 · ${ui.config.questions.length} 个标注项${viewDirty()?' · 视图待保存':''}`;
      root.querySelector('.ct-view-status').textContent=configStatus();
      const descriptor=root.querySelector('.ct-view-descriptor pre');if(descriptor)descriptor.textContent=JSON.stringify(description(),null,2);
    }
    function editConfigText(el){
      const id=el.id;ui.textDrafts[id]=el.value;ui.viewMessage='';
      try{
        if(id==='ct-view-title'){if(!el.value.trim()||el.value.length>60)throw Error('视图名称需为1–60字符');ui.config.title=el.value.trim();}
        if(el.dataset.ctField){const f=ui.config.fields.find(f=>f.id===el.dataset.ctField);if(!el.value.trim()||el.value.length>40)throw Error('字段名称需为1–40字符');f.title=el.value.trim();}
        if(el.dataset.ctQuestion){const q=ui.config.questions.find(q=>q.id===el.dataset.ctQuestion),next=clone(q);if(el.dataset.ctQprop==='title')next.title=el.value.trim();else next.options=el.value.split(/[,，\n]/).map(v=>v.trim()).filter(Boolean);if(!validQuestion(next))throw Error('标注项需要名称；选项需为2–8个不重复值，每项最多32字符');Object.assign(q,next);}
        delete ui.configErrors[id];el.removeAttribute('aria-invalid');previewOnly();
      }catch(error){ui.configErrors[id]=error.message;el.setAttribute('aria-invalid','true');root.querySelector('.ct-view-status').textContent=configStatus();}
    }
    root.addEventListener('input',e=>{
      const el=e.target;if(el.tagName==='INPUT'&&(el.id==='ct-view-title'||el.dataset.ctField&&el.dataset.ctProp==='title'||el.dataset.ctQuestion&&['title','options'].includes(el.dataset.ctQprop))){editConfigText(el);return;}
      if(e.target.id==='ct-search'){ui.search=e.target.value;repaint();return;}
      const id=e.target.dataset.ctAnswer;if(id){const q=ui.config.questions.find(q=>q.id===id);if(q?.type==='text')try{setAnswer(q,e.target.value);}catch(error){ui.message=error.message;root.querySelector('#ct-message').textContent=ui.message;}}
    });
    root.addEventListener('change',e=>{
      const el=e.target;
      if(el.tagName==='INPUT'&&(el.id==='ct-view-title'||el.dataset.ctField&&el.dataset.ctProp==='title'||el.dataset.ctQuestion&&['title','options'].includes(el.dataset.ctQprop)))return;
      if(el.dataset.ctAnswer&&ui.config.questions.find(q=>q.id===el.dataset.ctAnswer)?.type==='text')return;
      try{
        if(el.dataset.ctField){
          const f=ui.config.fields.find(f=>f.id===el.dataset.ctField),prop=el.dataset.ctProp;
          if(prop==='source'){const old=sourceSpec(f.source),spec=sourceSpec(el.value);if(!spec)throw Error('未知字段。');f.source=el.value;f.format=spec[2];if(f.title===old[1])f.title=spec[1];}
          if(prop==='role'){if(!Object.hasOwn(ROLES,el.value))throw Error('未知展示区域。');f.role=el.value;}
          if(prop==='format'){if(!Object.hasOwn(FORMATS,el.value))throw Error('未知预览格式。');f.format=el.value;}
          if(prop==='language')f.language=el.value;
          changed();return;
        }
        if(el.dataset.ctQuestion){
          const q=ui.config.questions.find(q=>q.id===el.dataset.ctQuestion),prop=el.dataset.ctQprop,next=clone(q);
          if(prop==='type'){next.type=el.value;next.options=['choice','multi'].includes(next.type)?next.options.length>=2?next.options:['通过','待复核','存在问题']:[];}
          if(prop==='required')next.required=el.checked;
          if(prop==='max')next.max=Number(el.value);
          if(!validQuestion(next))throw Error('标注项需有名称；选择控件需要2–8个不重复选项，每项最多32字符。');
          if(prop==='type'){delete ui.textDrafts['ct-question-options-'+q.id];delete ui.configErrors['ct-question-options-'+q.id];}
          Object.assign(q,next);changed();return;
        }
        if(el.dataset.ctFilterDef){const id=el.dataset.ctFilterDef;ui.config.filterFields=Object.keys(FILTERS).filter(key=>key===id?el.checked:ui.config.filterFields.includes(key));delete ui.filters[id];changed();return;}
        if(el.id==='ct-preset'&&el.value){applyPreset(el.value);return;}
        if(el.id==='ct-config-group'){ui.config.groupBy=el.value;ui.group='all';changed();return;}
        if(el.id==='ct-config-sort'){ui.config.sortBy=el.value;changed();return;}
        if(el.id==='ct-group'){ui.group=el.value;repaint();return;}
        if(el.dataset.ctFilter){if(el.type==='number'&&!el.checkValidity())throw Error('筛选分数需为0–100。');ui.filters[el.dataset.ctFilter]=el.value;repaint();return;}
        if(el.id==='ct-target'){ui.target=el.value;ui.message='';repaint();return;}
        if(el.dataset.ctAnswer){const q=ui.config.questions.find(q=>q.id===el.dataset.ctAnswer);if(q){setAnswer(q,q.type==='rating'?(el.value===''?null:Number(el.value)):el.value);repaint();}return;}
      }catch(error){ui.viewMessage='本次配置未应用：'+error.message;repaint();}
    });
    root.addEventListener('click',e=>{
      const b=e.target.closest('button');if(!b||b.disabled)return;
      if(b.dataset.ctConfigTab){ui.configTab=b.dataset.ctConfigTab;repaint();return;}
      if(b.dataset.ctLayout){ui.config.layout=b.dataset.ctLayout;changed();return;}
      if(b.dataset.ctCase){ui.caseId=b.dataset.ctCase;ui.target=b.dataset.ctTarget;ui.message='';repaint();return;}
      if(b.dataset.ctZoom){const key=b.dataset.ctZoom;if(ui.zoomed.has(key))ui.zoomed.delete(key);else if(ui.zoomed.size<32)ui.zoomed.add(key);repaint();return;}
      if(b.dataset.ctRemoveField){delete ui.textDrafts['ct-label-'+b.dataset.ctRemoveField];delete ui.configErrors['ct-label-'+b.dataset.ctRemoveField];ui.config.fields=ui.config.fields.filter(f=>f.id!==b.dataset.ctRemoveField);changed();return;}
      if(b.dataset.ctRemoveQuestion){for(const prop of ['title','options']){delete ui.textDrafts['ct-question-'+prop+'-'+b.dataset.ctRemoveQuestion];delete ui.configErrors['ct-question-'+prop+'-'+b.dataset.ctRemoveQuestion];}ui.config.questions=ui.config.questions.filter(q=>q.id!==b.dataset.ctRemoveQuestion);changed();return;}
      if(b.dataset.ctMulti){const q=ui.config.questions.find(q=>q.id===b.dataset.ctMulti),option=q?.options[Number(b.dataset.ctOption)];if(q&&option!==undefined){const values=currentAnswer(q);try{setAnswer(q,values.includes(option)?values.filter(v=>v!==option):[...values,option]);ui.message='';}catch(error){ui.message=error.message;}repaint();}return;}
      const action=b.dataset.ct;
      if(action==='toggle-config'){ui.configOpen=!ui.configOpen;repaint();return;}
      if(action==='add-field'&&ui.config.fields.length<8&&fieldSequence<=999){const spec=sourceDefinitions.find(s=>!ui.config.fields.some(f=>f.source===s[0]))||sourceDefinitions[0];ui.config.fields.push(field('f'+fieldSequence++,spec[0],spec[3]==='结果'?'output':'input',spec[2],spec[1]));changed();return;}
      if(action==='add-question'&&ui.config.questions.length<6&&questionSequence<=999){ui.config.questions.push(question('q'+questionSequence++,'新标注项','choice',['通过','待复核','存在问题']));changed();return;}
      if(action==='clear-filters'){ui.filters={};ui.search='';ui.group='all';repaint();return;}
      if(action==='save-view'){saveView();return;}
      if(action==='restore-view'){try{const record=readView();ui.config=clone(record?.config||DEFAULT);ui.textDrafts={};ui.configErrors={};ui.savedConfig=clone(ui.config);ui.viewRevision=record?.revision||0;fieldSequence=Math.max(fieldSequence,...ui.config.fields.map(f=>Number(f.id.slice(1))+1));questionSequence=Math.max(questionSequence,...ui.config.questions.map(q=>(Number(q.id.slice(1))||0)+1));const selected=record?.selection||{group:'all',filters:{},search:''};ui.group=selected.group;ui.filters=clone(selected.filters);ui.search=selected.search;ui.savedSelection=clone(selected);ui.zoomed.clear();ui.viewMessage='已恢复最近保存的视图，标注与草稿保留。';}catch(error){ui.viewMessage=error.message;}repaint();return;}
      if(action==='save'||action==='save-next'){saveMarks(action==='save-next');return;}
      if(action==='reset'){delete ui.drafts[markKey()];ui.message='已恢复当前对象最近保存的标注。';repaint();return;}
      if(action==='previous'||action==='next'){const id=neighbor(action==='next'?1:-1);if(id){ui.caseId=id;ui.target='case';ui.message='已切换 Case，未保存标注仍保留。';repaint();}return;}
    });
  }
  window.ForgeCaseDemo=Object.freeze({html,mount,unmount});
})();
