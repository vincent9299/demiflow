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

export function showTool({root,item,api,mountText,disposeReaders,project,canInsert,insert,reference}){
  const importOnly=['tool','analysis','annotation_component'].includes(item.tool_kind);
  const label={tool:'工具',analysis:'分析工具',view:'查看工具',annotation:'标注工具',annotation_component:'标注组件'}[item.tool_kind];
  const dialog=el('dialog',undefined,'data-tools-dialog'),header=el('header');
  dialog.setAttribute('aria-label',label);
  const close=el('button','关闭'),body=el('section');close.onclick=()=>dialog.close();
  header.append(el('strong',label),close);dialog.append(header,body);root.append(dialog);
  dialog.addEventListener('close',()=>{disposeReaders(dialog);dialog.remove();},{once:true});
  dialog.showModal();body.textContent='读取定义…';
  api('data-tool-definition',{id:item.id}).then(detail=>{
    if(!dialog.open)return;
    body.replaceChildren(el('h3',detail.symbol),el('p',detail.path+' · L'+detail.line,'op-location'));
    if(detail.description||detail.module_summary)
      body.append(el('p',detail.description||'模块说明 · '+detail.module_summary,'op-description'));
    if(reference){
      const use=el('button','在当前卡片中引用');use.onclick=()=>{dialog.close();reference();};body.append(use);
    }
    if(!importOnly)launchForm(body,detail,{project,canInsert,insert,dialog});
    else if(detail.module){
      const code='from '+detail.module+' import '+detail.symbol;
      body.append(el('pre',code,'definition-signature'));
      const button=el('button','插入导入语句'),status=el('p');
      button.disabled=!canInsert;button.onclick=async()=>{
        try{await insert(code);dialog.close();}catch(error){status.textContent=error.message;}
      };
      body.append(button,status);
    }
    const source=el('details'),content=el('div');source.append(el('summary','查看源码'),content);body.append(source);
    source.ontoggle=()=>{if(source.open && !content.childElementCount)DemiFlowDefinition.render(content,detail,{mountText,mode:'source'});};
    source.open=importOnly;
  }).catch(error=>{if(dialog.open)body.textContent=error.message;});
  return dialog;
}
