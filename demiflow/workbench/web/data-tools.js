/* Reusable project tools run only when the user executes their Notebook cell. */
const el=(tag,text,cls)=>{
  const node=document.createElement(tag);
  if(text!==undefined)node.textContent=text;
  if(cls)node.className=cls;
  return node;
};

function pythonValue(value){
  if(value===null)return 'None';
  if(typeof value==='boolean')return value?'True':'False';
  if(typeof value==='number'){
    if(!Number.isFinite(value)||(Number.isInteger(value)&&!Number.isSafeInteger(value)))
      throw Error('数值超出可准确表示的范围。');
    return String(value);
  }
  if(typeof value==='string')return JSON.stringify(value);
  if(Array.isArray(value))return '['+value.map(pythonValue).join(', ')+']';
  return '{'+Object.entries(value).map(([key,item])=>pythonValue(key)+': '+pythonValue(item)).join(', ')+'}';
}

function launchForm(body,detail,{project,canInsert,insert,dialog}){
  const entry=detail.notebook_entry;
  if(!entry){body.append(el('p','该定义可在项目中导入。使用命名参数的公开入口支持从此处插入 Notebook。'));return;}
  if(entry.preset_error)body.append(el('p',entry.preset_error,'op-error'));
  const form=el('form',undefined,'tool-launch');
  const presets=el('select'),fields=el('div',undefined,'tool-fields'),note=el('p',undefined,'op-location');
  presets.setAttribute('aria-label','工具预设');note.setAttribute('role','status');
  const storageKey='demiflow.tool-presets.'+project+'.'+detail.id;
  let saved=[];
  try{const value=JSON.parse(localStorage.getItem(storageKey)||'[]');if(Array.isArray(value))saved=value;}catch{}
  const options=[['默认参数',{}],...Object.entries(entry.presets),...saved];
  function newOption(name,index){const option=el('option',name);option.value=String(index);presets.append(option);}
  options.forEach(([name],i)=>newOption(name,i));
  function values(){
    const result=[];
    for(const input of fields.querySelectorAll('input')){
      if(!input.value && !input.required && !input.dataset.includeEmpty)continue;
      try{
        const value=input.dataset.format==='json'?JSON.parse(input.value):input.value;
        result.push([input.name,value]);
      }catch{throw Error(input.name+' 需要有效的数值、布尔值或 JSON 参数。');}
    }
    return Object.fromEntries(result);
  }
  function fill(){
    fields.replaceChildren();note.textContent='';
    const preset=options[presets.selectedIndex][1];
    for(const p of entry.parameters){
      const label=el('label',p.name),input=el('input');
      const supplied=Object.hasOwn(preset,p.name);
      const value=supplied?preset[p.name]:p.default_known?p.default:undefined;
      const json=value==null?/^(int|float|bool|list|dict|tuple)\b/.test(p.annotation||''):typeof value!=='string'&&value!==null;
      input.dataset.format=json?'json':'text';input.name=p.name;
      if(typeof value==='number'||(value==null&&['int','float'].includes(p.annotation))){
        input.type='number';input.step=p.annotation==='int'?'1':'any';
      }
      input.required=p.required && value!=='';
      if(value==='' || (supplied && value===null))input.dataset.includeEmpty='true';
      if(supplied && value===null)input.dataset.format='json';
      input.value=value===undefined||(!supplied && value===null)?'':input.dataset.format==='json'?JSON.stringify(value):value;
      input.placeholder=p.default_expression??p.annotation??(p.required?'必填':'可选');
      label.append(input);fields.append(label);
    }
  }
  const launch=el('button','插入 Notebook');launch.type='submit';launch.disabled=!canInsert;
  launch.title=canInsert?'插入调用单元格，由你运行':'此工作区未启用可写 Notebook';
  form.onsubmit=async event=>{
    event.preventDefault();
    try{
      const args=Object.entries(values()).map(([key,value])=>'    '+key+'='+pythonValue(value)+',');
      const call=entry.symbol+'('+(args.length?'\n'+args.join('\n')+'\n':'')+')';
      const code='from '+entry.module+' import '+entry.symbol+'\n\n'+(entry.asynchronous?'await ':'')+call;
      await insert(code);dialog.close();
    }catch(error){note.textContent='无法插入。'+error.message;}
  };
  const saveName=el('input'),save=el('button','保存到此浏览器');
  saveName.placeholder='新预设名称';saveName.setAttribute('aria-label','新预设名称');saveName.maxLength=96;save.type='button';
  save.onclick=()=>{
    try{
      const name=saveName.value.trim();
      if(!name)throw Error('请填写预设名称。');
      if(options.some(([label])=>label===name))throw Error('已有同名预设。');
      if(!form.reportValidity())return;
      const pair=[name,values()],next=[...saved,pair];
      if(next.length>32||JSON.stringify(next).length>65536)throw Error('预设已达到保存上限。');
      localStorage.setItem(storageKey,JSON.stringify(next));saved=next;options.push(pair);
      newOption(name,options.length-1);presets.selectedIndex=options.length-1;saveName.value='';note.textContent='预设已保存。';
    }catch(error){note.textContent=error.message;}
  };
  presets.onchange=fill;fill();
  if(entry.parameters.length){
    const saveBox=el('details');saveBox.append(el('summary','另存参数预设'),saveName,save);
    const label=el('label','预设');label.append(presets);form.append(label,fields,saveBox);
  }
  form.append(launch,note);body.append(form);
}

export function showTools({root,kind,api,mountText,disposeReaders,project,canInsert,insert}){
  const label=kind==='annotation'?'标注工具':'查看工具';
  const dialog=el('dialog',undefined,'data-tools-dialog'),header=el('header');
  dialog.setAttribute('aria-label',label);
  const close=el('button','关闭'),select=el('select'),body=el('section'),issues=el('div');
  select.setAttribute('aria-label','选择'+label);select.hidden=true;close.onclick=()=>dialog.close();
  header.append(el('strong',label),select,close);dialog.append(header,issues,body);root.append(dialog);
  dialog.addEventListener('close',()=>{disposeReaders(dialog);dialog.remove();},{once:true});dialog.showModal();
  body.textContent='正在读取'+label+'…';
  let generation=0;
  async function read(){
    const ticket=++generation;
    disposeReaders(body);body.textContent='读取定义…';
    try{
      const detail=await api('data-tool-definition',{id:select.value});
      if(!dialog.open||generation!==ticket)return;
      body.replaceChildren(el('h3',detail.symbol),el('p',detail.path+' · L'+detail.line,'op-location'));
      if(detail.description||detail.module_summary)
        body.append(el('p',detail.description||'模块说明 · '+detail.module_summary,'op-description'));
      launchForm(body,detail,{project,canInsert,insert,dialog});
      const source=el('details'),content=el('div');source.append(el('summary','查看源码'),content);body.append(source);
      source.ontoggle=()=>{if(source.open && !content.childElementCount)DemiFlowDefinition.render(content,detail,{mountText,mode:'source'});};
    }catch(error){if(dialog.open&&generation===ticket)body.textContent=error.message;}
  }
  api('project-data-tools').then(async index=>{
    if(!dialog.open)return;
    for(const issue of index.diagnostics)issues.append(el('p',issue.path+' · '+issue.detail,'op-error'));
    for(const item of index.definitions.filter(d=>d.tool_kind===kind)){
      const option=el('option',item.symbol+' · '+item.path);option.value=item.id;select.append(option);
    }
    if(!select.length){body.textContent='此项目尚未在 data_tools/ 中声明'+label+'。';return;}
    select.hidden=false;select.onchange=read;await read();
  }).catch(error=>{if(dialog.open)body.textContent=error.message;});
  return dialog;
}
