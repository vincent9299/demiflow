/* Shared typed fields. Existing reading/HTML projections keep their owners. */
(() => {
  'use strict';
  const node=(tag,text,kind)=>{const item=document.createElement(tag);if(text!==undefined)item.textContent=text;if(kind)item.className=kind;return item;};
  const handles=new Set();let watching=false;
  function boundedJSON(value){
    let count=0,bytes=0;const seen=new Set();
    const walk=(item,depth)=>{
      if(++count>8192||depth>8)throw Error('原值超过 JSON 节点/深度预算');
      if(typeof item==='string'){if(item.length>256*1024)throw Error('字段超过 256KiB 预算');bytes+=new TextEncoder().encode(item).length+2;}
      else if(item===null||typeof item==='boolean'||typeof item==='number'&&Number.isFinite(item))bytes+=24;
      else if(Array.isArray(item)||item&&typeof item==='object'&&Object.getPrototypeOf(item)===Object.prototype){
        if(seen.has(item))throw Error('循环引用不可预览');seen.add(item);
        const keys=Object.keys(item);if(keys.length>8192)throw Error('字段节点过多');bytes+=keys.length*3+2;
        for(const key of keys){if(!Array.isArray(item))walk(key,depth+1);walk(item[key],depth+1);}seen.delete(item);
      }else throw Error(item===undefined?'字段不存在（undefined）':'此值不是受支持的 JSON 类型');
      if(bytes>256*1024)throw Error('字段超过 256KiB 预算');
    };
    walk(value,0);const raw=typeof value==='string'?value:JSON.stringify(value,null,2);
    if(new TextEncoder().encode(raw).length>256*1024)throw Error('格式化原值超过 256KiB 预算');return raw;
  }
  function raw(parent,value){
    try{parent.append(node('pre',boundedJSON(value),'analysis-raw'));}
    catch(error){parent.append(node('p',error.message+'；请使用受控原始 artifact 读取。','muted small'));}
  }
  function original(parent,value,label='原值'){const detail=node('details');detail.append(node('summary',label));detail.ontoggle=()=>{if(detail.open&&detail.children.length===1)raw(detail,value);};parent.append(detail);}
  function jsonTree(parent,value){
    boundedJSON(value);let remaining=256;
    const render=(item,depth,label)=>{
      if(remaining--<=0)return node('span','… 初始节点预算已用完，请查看原值','muted small');
      if(item===null||typeof item!=='object')return node('div',(label===undefined?'':label+': ')+JSON.stringify(item),'analysis-json-leaf');
      const detail=node('details'),keys=Object.keys(item);detail.append(node('summary',(label===undefined?'':label+': ')+(Array.isArray(item)?'数组':'对象')+' · '+keys.length));
      let start=0;const page=node('div'),bar=node('div',undefined,'analysis-output-controls');detail.append(page,bar);
      function paint(){
        page.replaceChildren();bar.replaceChildren();remaining=256;
        if(depth>=8){page.append(node('span','达到深度预算，请查看原值'));return;}
        for(const key of keys.slice(start,start+32)){if(remaining<=0){page.append(node('span','本页节点预算已用完'));break;}page.append(render(item[key],depth+1,key));}
        for(const [label,next]of [['上一页',start-32],['下一页',start+32]]){const button=node('button',label);button.type='button';button.disabled=next<0||next>=keys.length;button.onclick=()=>{start=next;paint();};bar.append(button);}
      }
      detail.ontoggle=()=>{if(detail.open&&!page.children.length)paint();};return detail;
    };
    const root=render(value,0);parent.append(root);if(root.tagName==='DETAILS')root.open=true;original(parent,value);
  }
  function watch(){if(watching)return;watching=true;new MutationObserver(()=>{for(const handle of handles)if(!handle.element.isConnected)handle.dispose();}).observe(document.documentElement,{childList:true,subtree:true});}
  function disposeWithin(parent){for(const handle of [...handles])if(!parent||parent===handle.element||parent.contains(handle.element))handle.dispose();}
  function field({parent,format,value,label='',originalLabel='原值',language='',mountText,preview,media,resolveMedia,disposeReaders,isCurrent=()=>parent.isConnected}){
    if(handles.size>=128){const element=node('p','当前字段组件达到 128 项预算，请收起其他输出。','muted small');parent.append(element);return {element,ready:Promise.resolve(),dispose(){element.remove();}};}
    const host=node('div',undefined,'analysis-field-value');parent.append(host);
    let dead=false;const abort=new AbortController(),owned=new Set();
    const current=()=>!dead&&!abort.signal.aborted&&host.isConnected&&isCurrent();
    function own(handle){if(handle&&typeof handle.dispose==='function'){if(dead)handle.dispose();else owned.add(handle);}}
    function clear(){for(const handle of owned)handle.dispose();owned.clear();disposeReaders?.(host);for(const frame of host.querySelectorAll('iframe'))frame.removeAttribute('srcdoc');host.replaceChildren();}
    const handle={element:host,ready:null,dispose(){if(dead)return;dead=true;abort.abort();clear();handles.delete(handle);}};
    handles.add(handle);watch();
    const fallback=reason=>{if(dead)return;clear();host.append(node('p',reason+' · 以下为原值','muted small'));raw(host,value);};
    async function render(){
      if(dead||abort.signal.aborted)return;
      if(value===null){host.append(node('span','null','analysis-null muted'));return;}
      if(value===undefined){host.append(node('span','字段不存在（undefined）','muted'));return;}
      if(['text','code','md'].includes(format)){
        if(typeof value!=='string'){fallback('字段类型与 '+format+' 不匹配');return;}
        boundedJSON(value);
        if(format==='md'){
          if(typeof mountText!=='function'){fallback('Markdown 阅读器不可用');return;}
          own(mountText(host,value));
        }else{
          const pre=node('pre',value,'analysis-raw');if(format==='code'){const lines=value.split('\n',2049),shown=lines.slice(0,2048);host.append(node('span',(language||'代码')+' · 只读'+(lines.length>2048?' · 行号预览前 2048 行':''),'muted small'));pre.classList.add('analysis-code');pre.setAttribute('aria-label','代码行号预览（只读）');pre.textContent=shown.map((line,index)=>String(index+1).padStart(4,' ')+'  '+line).join('\n');host.append(pre);original(host,value);}else host.append(pre);
        }
        const copy=node('button','复制原文');copy.type='button';copy.onclick=async()=>{try{await navigator.clipboard.writeText(value);if(current())copy.textContent='已复制';}catch{if(current())copy.textContent='复制不可用，请选择原文';}};host.append(copy);return;
      }
      if(format==='json'){try{jsonTree(host,value);}catch(error){fallback(error.message);}return;}
      if(format==='tag'){
        if(!Array.isArray(value)||value.length>64||value.some(item=>typeof item!=='string'||new TextEncoder().encode(item).length>256)){fallback('Tag 需要最多 64 个字符串标签');return;}
        for(const item of value)host.append(node('span',item,'analysis-tag'));return;
      }
      if(format==='html'){
        if(typeof value!=='string'){fallback('HTML 需要文本');return;}boundedJSON(value);
        if(typeof preview!=='function'){fallback('受限 HTML 预览服务不可用');return;}
        host.append(node('p','正在读取受限 HTML 预览…','muted small'));
        try{
          const rendered=await preview('html',value,{signal:abort.signal});if(!current())return;
          if(rendered?.format!=='html'||rendered.sandbox!==''||rendered.active_content!==false||typeof rendered.preview!=='string'||new TextEncoder().encode(rendered.preview).length>258*1024)throw Error('HTML 预览协议不匹配');
          const frame=node('iframe');frame.title=label+' HTML 预览';frame.setAttribute('sandbox','');frame.referrerPolicy='no-referrer';frame.srcdoc=rendered.preview;
          clear();host.append(frame);if(rendered.blocked_media)host.append(node('p','HTML 内嵌图片/音视频已隔离；请使用类型化的授权媒体引用。','muted small'));const source=node('details');source.append(node('summary','HTML 原文'));source.ontoggle=()=>{if(source.open&&source.children.length===1)raw(source,value);};host.append(source);
        }catch(error){if(current())fallback(error.message);}return;
      }
      if(format==='message'||format==='dialogue'){
        const turns=format==='message'?[value]:value;
        const validPart=part=>part&&typeof part==='object'&&Object.keys(part).length===2&&Object.hasOwn(part,'value')&&['text','image','audio','video'].includes(part.format)&&(part.format!=='text'||typeof part.value==='string'&&part.value.length<=16384);
        if(!Array.isArray(turns)||turns.length>32||turns.some(turn=>!turn||Object.keys(turn).length!==2||!['system','user','assistant','tool'].includes(turn.role)||!(typeof turn.content==='string'&&turn.content.length<=16384||Array.isArray(turn.content)&&turn.content.length<=8&&turn.content.every(validPart)))){fallback('消息需要 role/content，Dialogue 最多 32 轮，每轮最多 8 个类型片段');return;}
        boundedJSON(value);
        for(const turn of turns){
          const article=node('div',undefined,'analysis-message');article.append(node('strong',turn.role));host.append(article);
          if(typeof turn.content==='string')article.append(node('pre',turn.content));
          else for(const part of turn.content)own(field({parent:article,format:part.format,value:part.value,label,mountText,preview,resolveMedia,disposeReaders,isCurrent:current}));
        }original(host,value);return;
      }
      if(['image','audio','video'].includes(format)){
        if(resolveMedia&&globalThis.ForgeAnalysisMedia)own(ForgeAnalysisMedia.create({host,kind:format,reference:value,resolveMedia,signal:abort.signal,isCurrent:current}));
        else if(format==='image'&&typeof media==='function'){
          const button=node('button','预览图片');button.type='button';host.append(button);
          button.onclick=async()=>{if(!current())return;button.disabled=true;try{const result=await media(host);if(!current()){result?.dispose?.();return;}own(result);}catch(error){if(current())host.append(node('p',error.message,'error-box'));}finally{if(current())button.disabled=false;}};
        }else host.append(node('p','此来源尚未接入经核对的 '+format+' 媒体；可查看原始引用。','muted small'));
        original(host,value,originalLabel);return;
      }
      fallback('未识别的预览格式');
    }
    handle.ready=Promise.resolve().then(render).catch(error=>{if(current())fallback(error.message);});return handle;
  }
  globalThis.ForgeAnalysisRender=Object.freeze({field,disposeWithin,stats:()=>({fields:handles.size})});
})();
