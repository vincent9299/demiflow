/* Run-specific presentation. Identity, polling and commands stay in the workbench. */
(() => {
  'use strict';
  const node=(tag,text,cls)=>{const n=document.createElement(tag);if(text!==undefined)n.textContent=text;if(cls)n.className=cls;return n;};
  const labels={prepared:'待启动',starting:'正在启动',running:'运行中',stopping:'正在停止',succeeded:'已完成',failed:'失败',cancelled:'已停止',interrupted:'中断'};
  function duration(state,now=Date.now()/1000){
    if(!state?.started_at)return '尚未启动';
    const seconds=Math.max(0,Math.floor((state.finished_at||now)-state.started_at));
    return seconds>=3600?Math.floor(seconds/3600)+'时'+Math.floor(seconds%3600/60)+'分':seconds>=60?Math.floor(seconds/60)+'分'+seconds%60+'秒':seconds+'秒';
  }
  function flowName(context,entry){
    const flow=context?.flows?.find(f=>f.entry===entry)||context?.project?.entries?.find(f=>f.entry===entry);
    return flow?.title||flow?.name||flow?.description?.split('\n')[0]||entry?.split('/').at(-1)||'运行';
  }
  function outputCount(outputs){
    const ready=(outputs||[]).filter(o=>o.status==='committed'&&o.result);
    // Different outputs may be successive stages. Never sum them as one row count.
    return ready.length?ready.length+' 个输出已提交':'尚无已提交输出';
  }
  function summary(parent,run){
    const identity=node('span','#'+run.run_id.slice(-10),'wb-run-id');identity.title=run.run_id;
    const status=node('span',labels[run.status]||run.status,'wb-tag');
    status.dataset.state=run.status;
    const timing=node('span',duration(run.state),'wb-run-duration');timing.title='本次尝试耗时';
    parent.replaceChildren(status,timing,node('span',outputCount(run.outputs),'wb-run-output-count'),identity);
  }
  function snapshot(parent,value){
    parent.replaceChildren(node('h3','本次参数与版本'));
    const dl=node('dl',undefined,'wb-run-definition');
    for(const[label,text]of [['流程',value.entry],['业务参数文件',value.parameter_path||'本次内联参数'],
      ['封存标识',value.digest],['执行经历','第 '+value.attempt+' 次启动'+(value.attempt!==value.current_attempt?' · 历史只读':'')]]){
      dl.append(node('dt',label),node('dd',text));
    }
    parent.append(dl,node('p','参数及引用来自本次封存；修改当前流程不会改变它们。','wb-caption'));
    const parameters=node('pre',JSON.stringify(value.original_parameters?.arguments??{},null,2),'wb-code');
    parameters.setAttribute('aria-label','封存业务参数');parent.append(parameters);
    const bindings=value.report?.binding;
    if(bindings){
      const inputs=node('details');inputs.append(node('summary','固定输入与代码版本'));
      inputs.append(node('pre',JSON.stringify({tables:bindings.tables||{},sources:bindings.sources||{},resources:bindings.resources||{}},null,2),'wb-code'));
      parent.append(inputs);
    }
    const history=node('ol',undefined,'wb-attempt-history');
    for(const a of value.attempts||[])history.append(node('li','第 '+a.attempt+' 次启动 · '+(labels[a.status]||a.status)));
    if(history.children.length)parent.append(node('h4','执行经历'),history);
  }
  function confirmation(parent,{title,description,rows=[],confirmLabel='确认',confirm,cancel}){
    parent.replaceChildren(node('h3',title),node('p',description,'wb-caption'));
    const dl=node('dl',undefined,'wb-run-definition');
    for(const[label,value]of rows)dl.append(node('dt',label),node('dd',String(value)));
    parent.append(dl);
    const actions=node('div',undefined,'wb-inline-actions'),yes=node('button',confirmLabel,'primary'),no=node('button','返回');
    yes.type=no.type='button';
    yes.onclick=async()=>{yes.disabled=no.disabled=true;try{await confirm();}catch(error){parent.append(node('p',error.message,'error-box'));}finally{yes.disabled=no.disabled=false;}};
    no.onclick=cancel;actions.append(yes,no);parent.append(actions);parent.hidden=false;yes.focus();
  }
  globalThis.ForgeRunPresentation=Object.freeze({labels,duration,flowName,outputCount,summary,snapshot,confirmation});
})();
