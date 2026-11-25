/* Clean read-only semantic viewer for project and built-in data tools. */
const el=(tag,text,cls)=>{
  const node=document.createElement(tag);
  if(text!==undefined)node.textContent=text;
  if(cls)node.className=cls;
  return node;
};

export function showTool({root,item,api,mountText,disposeReaders,reference}){
  const label={tool:'工具',analysis:'分析工具',view:'查看工具',annotation:'标注工具',annotation_component:'标注组件'}[item.tool_kind]||'数据工具';
  const dialog=el('dialog',undefined,'data-tools-dialog'),header=el('header');
  dialog.setAttribute('aria-label',label);
  const close=el('button','关闭'),body=el('section');close.type='button';close.onclick=()=>dialog.close();
  header.append(el('strong',label),close);dialog.append(header,body);root.append(dialog);
  dialog.addEventListener('close',()=>{disposeReaders(dialog);dialog.remove();},{once:true});
  dialog.showModal();body.textContent='读取定义…';
  api('data-tool-definition',{id:item.id}).then(detail=>{
    if(!dialog.open)return;
    const heading=detail.title&&detail.title!==detail.symbol?detail.title+'（'+detail.symbol+'）':detail.symbol;
    body.replaceChildren(el('h3',heading),el('p',detail.path+' · L'+detail.line,'op-location'));
    const desc=detail.summary||detail.description||detail.module_summary;
    if(desc)body.append(el('p',desc,'op-description'));
    if(detail.when_to_use)body.append(el('p','适用场景：'+detail.when_to_use,'op-description'));
    if(detail.module)body.append(el('pre','from '+detail.module+' import '+detail.symbol,'definition-signature'));
    if(reference){
      const use=el('button','在当前卡片中引用');use.type='button';use.onclick=()=>{dialog.close();reference();};body.append(use);
    }
    const source=el('details'),content=el('div');source.append(el('summary','查看源码'),content);body.append(source);
    source.ontoggle=()=>{if(source.open && !content.childElementCount)DemiFlowDefinition.render(content,detail,{mountText,mode:'source'});};
  }).catch(error=>{if(dialog.open)body.textContent=error.message;});
  return dialog;
}
