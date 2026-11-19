/* Adapted from DeepSeek Harness 0.2.1-rc.2 InputBar, ModelSelect,
 * PermissionTrigger and ContextMeter. The host retains draft/send ownership.
 * MIT License
 * Copyright (c) 2026 DeepSeek
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
(() => {
  'use strict';
  function connectionStatus(caps={}) {
    // Older local services expose provider configuration without the two flags.
    // This is a display projection only; sending still requires enabled/start.
    const configured=caps.service_configured??(caps.enabled===true||!!caps.providers?.length);
    const authorized=caps.project_authorized??(caps.enabled===true);
    const label=!configured?'尚未配置开发代理':!authorized?'本项目未启用开发':caps.read_only?'开发历史只读':!caps.start?'开发代理暂不可用':'开发代理可用';
    return {configured,authorized,label};
  }
  let serial=0;
  const encoder=new TextEncoder(), bytes=value=>encoder.encode(value).length;
  const finite=value=>typeof value==='number'&&Number.isFinite(value)&&value>=0;
  const compact=value=>value<1000?String(value):value<1e6?scaled(value/1000)+'K':scaled(value/1e6)+'M';
  const scaled=value=>String(value>=100?Math.round(value):Math.round(value*10)/10);
  const el=(tag,className,text)=>{const node=document.createElement(tag);if(className)node.className=className;if(text!==undefined)node.textContent=text;return node;};
  const svg=(path,size=14,fill=false)=>{
    const node=document.createElementNS('http://www.w3.org/2000/svg','svg');
    for(const [key,value] of Object.entries({viewBox:`0 0 ${size} ${size}`,width:size,height:size,'aria-hidden':'true',fill:fill?'currentColor':'none'}))node.setAttribute(key,value);
    if(!fill)for(const [key,value] of Object.entries({stroke:'currentColor','stroke-width':'1.4','stroke-linecap':'round','stroke-linejoin':'round'}))node.setAttribute(key,value);
    const shape=document.createElementNS(node.namespaceURI,'path');shape.setAttribute('d',path);node.append(shape);return node;
  };
  const chevron=()=>svg('m3.5 5.5 3.5 3.5 3.5-3.5');
  const modelLabel=provider=>provider?.display_name||provider?.model_name||String(provider?.model||'Select model').replace(/^deepseek-/i,'DeepSeek-').replace(/(^|-)(v\d+(?:\.\d+)?|flash|pro)(?=-|$)/gi,(_,dash,part)=>dash+part[0].toUpperCase()+part.slice(1));
  const effortLabel=provider=>provider?.effort_display_name||({low:'Low',medium:'Medium',high:'High',xhigh:'Extra high',max:'Max'}[provider?.effort]||provider?.effort||'');

  function create(options={}) {
    const id='forge-composer-'+(++serial), state={capabilities:{},session:null,readonly:false,historyOnly:false,sending:false,creation:false,stats:null,disposed:false,attaching:false};
    const removers=[],dialogs=new Set();let menuOpen=null,providerFingerprint='';
    const listen=(target,name,handler,opts)=>{target.addEventListener(name,handler,opts);removers.push(()=>target.removeEventListener(name,handler,opts));};
    const form=el('form','development-composer development-composer-native'),notice=el('div','dc-notice'),card=el('div','dc-card');
    form.setAttribute('aria-label','项目开发输入');notice.hidden=true;
    const input=el('textarea','dc-input');input.rows=1;input.placeholder='发消息，讨论或修改这个项目…';input.setAttribute('aria-label','开发要求');
    const row=el('div','dc-row'),tools=el('div','dc-tools'),trailing=el('div','dc-trailing');
    const button=(label,className)=>{const node=el('button',className);node.type='button';node.setAttribute('aria-label',label);node.title=label;return node;};
    const add=button('添加内容','dc-add');add.append(svg('M7 2v10M2 7h10'));
    const access=button('查看当前开发权限','dc-access'),accessLabel=el('span','dc-access-label');
    access.setAttribute('aria-haspopup','dialog');
    access.append(svg('M7 1.4 12 3.3v3.5c0 2.7-1.8 4.5-5 5.8-3.2-1.3-5-3.1-5-5.8V3.3L7 1.4ZM7 4v3.5M7 9.7h.01'),accessLabel,chevron());
    const providers=el('select','dc-provider-binding');providers.hidden=true;providers.setAttribute('aria-hidden','true');providers.tabIndex=-1;
    const model=el('div','dc-model'),modelTrigger=button('开发代理','dc-model-trigger'),modelName=el('span','dc-model-name'),effort=el('span','dc-effort');
    modelTrigger.append(modelName,effort,chevron());model.append(modelTrigger);
    const send=button('发送','dc-primary dc-send'),stop=button('停止','dc-primary dc-stop');send.type='submit';
    send.append(svg('M8.3125.980183c.35517.072917.66652.223997.9502.452147.22454.18064.46759.4256.71679.67481L14.707 6.83468 13.293 8.24874 9 3.95577V15.0417H7V3.95577L2.70703 8.24874 1.29297 6.83468 6.02051 2.10714c.2492-.24921.49226-.49417.71679-.67481.23932-.19247.54677-.38831.9502-.452147.2098-.033177.4156-.025017.625.00000Z',16,true));
    stop.append(svg('M5 3h6a2 2 0 0 1 2 2v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2Z',16,true));
    const file=el('input','dc-text-file');file.type='file';file.hidden=true;file.accept='text/*,.md,.txt,.csv,.json,.jsonl,.yaml,.yml,.py,.js,.ts,.sql,.log,.toml,.ini';file.setAttribute('aria-label','选择文本附件');
    const dock=el('div','dc-dock');dock.setAttribute('aria-label','会话统计');dock.hidden=true;
    tools.append(add,access);trailing.append(model,send,stop);row.append(tools,trailing);card.append(input,row,providers,file);form.append(notice,card,dock);

    function resize(){if(state.disposed)return;input.style.height='auto';input.style.height=Math.min(Math.max(input.scrollHeight,parseFloat(getComputedStyle(input).minHeight)||36),220)+'px';}
    function inform(message,error=false){notice.textContent=message;notice.hidden=!message;notice.setAttribute('role',error?'alert':'status');notice.classList.toggle('dc-notice-error',error);}
    function place(menu,trigger){
      if(menu.hidden)return;
      const anchor=trigger.getBoundingClientRect(),rect=menu.getBoundingClientRect();
      const left=Math.min(Math.max(12,anchor.right-rect.width),Math.max(12,innerWidth-rect.width-12));
      const top=anchor.top>=rect.height+20?anchor.top-rect.height-8:Math.min(anchor.bottom+8,Math.max(12,innerHeight-rect.height-12));
      menu.style.left=left+'px';menu.style.top=Math.max(12,top)+'px';
    }
    function closeMenu(restore=false){if(!menuOpen)return;const previous=menuOpen;menuOpen=null;previous.menu.hidden=true;previous.trigger.setAttribute('aria-expanded','false');if(restore)previous.trigger.focus();}
    function disclosure(trigger,label){
      const menu=el('div','dc-menu');menu.id=id+'-'+label;menu.hidden=true;menu.setAttribute('role','menu');menu.setAttribute('aria-label',label);
      trigger.setAttribute('aria-controls',menu.id);trigger.setAttribute('aria-haspopup','menu');trigger.setAttribute('aria-expanded','false');form.append(menu);
      const open=(last=false)=>{
        if(trigger.disabled)return;closeMenu();menu.hidden=false;trigger.setAttribute('aria-expanded','true');menuOpen={menu,trigger};place(menu,trigger);
        const rows=[...menu.querySelectorAll('button:not(:disabled)')];(last?rows.at(-1):rows.find(node=>node.getAttribute('aria-checked')==='true')||rows[0])?.focus();
      };
      trigger.onclick=()=>menu.hidden?open():closeMenu(true);
      trigger.onkeydown=event=>{if(['ArrowDown','ArrowUp'].includes(event.key)){event.preventDefault();open(event.key==='ArrowUp');}};
      menu.onkeydown=event=>{
        if(event.key==='Escape'){event.preventDefault();event.stopPropagation();closeMenu(true);return;}
        if(['ArrowDown','ArrowUp','Home','End'].includes(event.key)){
          event.preventDefault();const rows=[...menu.querySelectorAll('button:not(:disabled)')],at=rows.indexOf(document.activeElement);
          const next=event.key==='Home'?0:event.key==='End'?rows.length-1:(at+(event.key==='ArrowUp'?-1:1)+rows.length)%rows.length;rows[next]?.focus();
        }
        if(event.key==='Tab')closeMenu();
      };
      return menu;
    }
    const addMenu=disclosure(add,'添加内容'),modelMenu=disclosure(modelTrigger,'选择模型');
    const reference=button('引用文件/目录','dc-menu-option');reference.textContent='引用文件/目录';reference.setAttribute('role','menuitem');
    reference.title='可选：在对话中引用任意文件或目录路径';reference.onclick=()=>{closeMenu();options.onReference?.();};
    const attachment=button('添加文本附件','dc-menu-option');attachment.textContent='添加文本附件';attachment.setAttribute('role','menuitem');
    attachment.title='将不超过 24 KiB 的 UTF-8 文本加入草稿';attachment.onclick=()=>{closeMenu();file.click();};addMenu.append(reference,attachment);
    listen(document,'pointerdown',event=>{if(menuOpen&&!menuOpen.menu.contains(event.target)&&!menuOpen.trigger.contains(event.target))closeMenu();});
    const reposition=()=>{if(menuOpen)place(menuOpen.menu,menuOpen.trigger);};listen(window,'resize',reposition);listen(document,'scroll',reposition,true);
    listen(form,'mousedown',event=>{
      if(input.disabled||event.button!==0||event.defaultPrevented||event.target.closest('button,a,input,select,textarea,[tabindex]')||!card.contains(event.target))return;
      event.preventDefault();input.focus();
    });
    for(const control of [send,stop])listen(control,'mousedown',event=>{if(event.button===0)event.preventDefault();});
    listen(input,'input',()=>{resize();renderAvailability();});
    listen(providers,'change',()=>{renderAvailability();options.onProviderChange?.(providers.value);});
    listen(file,'change',async()=>{
      const selected=file.files?.[0];if(!selected||state.disposed||input.disabled)return;
      state.attaching=true;renderAvailability();inform('');
      try{
        if(selected.size>24*1024)throw new Error('文本附件不能超过 24 KiB。');
        if(selected.type.startsWith('image/')||/\.(png|jpe?g|gif|webp|bmp|ico|svg|avif|heic|pdf)$/i.test(selected.name))throw new Error('此入口仅添加 UTF-8 文本，暂不支持图片或 PDF 附件。');
        let content;try{content=new TextDecoder('utf-8',{fatal:true}).decode(await selected.arrayBuffer());}catch{throw new Error('请选择有效的 UTF-8 文本文件。');}
        if(/[\x00-\x08\x0b\x0c\x0e-\x1f]/.test(content))throw new Error('该文件含有二进制内容，不能作为文本附件添加。');
        if(state.disposed||input.disabled)return;
        const name=selected.name.replace(/[\r\n\u2028\u2029]/g,' ');
        if(bytes(name)>512)throw new Error('附件文件名过长。');
        const inserted=(input.value?'\n\n':'')+'--- 文本附件：'+name+' ---\n'+content+'\n--- 文本附件结束 ---';
        if(bytes(input.value+inserted)>32768)throw new Error('加入附件后要求会超过 32 KiB，请缩短草稿或附件。');
        input.value+=inserted;input.selectionStart=input.selectionEnd=input.value.length;input.dispatchEvent(new Event('input',{bubbles:true}));input.focus();
        inform('已将 '+name+' 的文本加入草稿。');
      }catch(error){if(!state.disposed)inform(error.message,true);}
      finally{file.value='';state.attaching=false;if(!state.disposed)renderAvailability();}
    });
    access.onclick=()=>{
      closeMenu();const facts={capabilities:state.capabilities,session:state.session};
      if(options.onInspectAccess){options.onInspectAccess(facts);return;}
      const dialog=el('dialog','dc-access-dialog'),title=el('h2',null,'当前开发权限'),description=el('p',null,access.title),close=el('button',null,'关闭');close.type='button';close.onclick=()=>dialog.close();
      dialog.setAttribute('aria-label','当前开发权限');dialog.append(title,description,close);dialogs.add(dialog);document.body.append(dialog);
      dialog.addEventListener('close',()=>{dialogs.delete(dialog);dialog.remove();if(!state.disposed)access.focus();},{once:true});dialog.showModal();
    };

    function renderAvailability(){
      const caps=state.capabilities,session=state.session,turn=session?.turn;
      const readonly=state.readonly||caps.read_only===true,unavailable=!caps.enabled;
      const busy=!!(turn&&(turn.cleanup!=='complete'||turn.development?.busy===true));
      const provider=(caps.providers||[]).find(value=>value.id===(session?.provider||providers.value));
      input.disabled=readonly||state.historyOnly||unavailable||state.sending;
      send.disabled=input.disabled||!caps.start||!provider?.available||busy||!input.value.trim()||bytes(input.value)>32768;
      stop.disabled=readonly||state.historyOnly||unavailable||!busy;stop.hidden=!busy||state.historyOnly;send.hidden=busy&&!state.historyOnly;
      providers.disabled=readonly||state.historyOnly||unavailable||!!session||state.creation||state.sending;
      modelTrigger.disabled=providers.disabled||!(caps.providers||[]).length;
      const displayed=session?.model?{...provider,model:session.model,effort:session.effort}:provider;
      modelName.textContent=modelLabel(displayed);effort.textContent=effortLabel(displayed);effort.hidden=!effort.textContent;
      modelTrigger.title=provider?modelLabel(displayed)+(effort.textContent?' '+effort.textContent:'')+' · '+provider.id:'未提供可用开发代理';
      modelTrigger.hidden=!(caps.providers||[]).length;
      accessLabel.textContent=readonly||state.historyOnly?'Read only':caps.mode==='restricted'?'Managed access':caps.mode==='trusted_local'?'Trusted local':'Project access';
      access.title=readonly||state.historyOnly?'当前仅可查看历史记录。':caps.mode==='restricted'?'项目开发受 Forge 管理；输入可在对话中发现和引用，写入仍限项目授权范围。':caps.mode==='trusted_local'?'可信本地执行；当前未声明 OS 防篡改边界。':'按当前项目的已授权开发范围执行。';
      access.setAttribute('aria-label','查看当前开发权限：'+accessLabel.textContent);
      add.disabled=input.disabled||state.attaching;file.disabled=add.disabled;reference.disabled=!options.onReference||add.disabled;attachment.disabled=add.disabled;
      for(const item of modelMenu.querySelectorAll('[data-provider]'))item.setAttribute('aria-checked',String(item.dataset.provider===(session?.provider||providers.value)));
      if(menuOpen?.trigger.disabled)closeMenu();
    }
    function renderProviders(){
      const list=state.capabilities.providers||[],fingerprint=JSON.stringify(list);
      if(fingerprint===providerFingerprint)return;providerFingerprint=fingerprint;
      const selected=state.session?.provider||providers.value;providers.replaceChildren();modelMenu.replaceChildren();
      for(const provider of list){const option=el('option',null,provider.model);option.value=provider.id;option.disabled=!provider.available;providers.append(option);}
      if(list.some(provider=>provider.id===selected))providers.value=selected;
      else providers.value=list.find(provider=>provider.available)?.id||list[0]?.id||'';
      for(const provider of list){
        const option=button(modelLabel(provider),'dc-menu-option dc-model-option'),copy=el('span','dc-model-copy');option.setAttribute('role','menuitemradio');option.dataset.provider=provider.id;option.disabled=!provider.available;
        copy.append(el('span',null,modelLabel(provider)),el('small',null,provider.id+(effortLabel(provider)?' · '+effortLabel(provider):'')+(provider.available?'':' · 暂不可用')));
        option.append(copy,el('span','dc-model-check','✓'));option.title=provider.reason||modelLabel(provider);
        option.onclick=()=>{providers.value=provider.id;providers.dispatchEvent(new Event('change',{bubbles:true}));closeMenu();input.focus();};modelMenu.append(option);
      }
    }
    function renderStats(){
      dock.replaceChildren();const stats=state.stats;
      if(!stats||(stats.session_id&&stats.session_id!==state.session?.id)){dock.hidden=true;return;}
      if(stats.coverage?.state==='partial'){
        dock.hidden=false;dock.append(el('span','dc-stat-pending','正在读取会话统计…'));dock.title='统计尚未覆盖全部记录，不将未知值当作 0。';return;
      }
      const counters=[];
      const partial=stats.coverage?.metrics_state==='partial'?'历史统计仅包含有统计记录的轮次。':'';
      if(finite(stats.turns))counters.push(compact(stats.turns)+' turns');
      else if(finite(stats.conversation_turns))counters.push(compact(stats.conversation_turns)+' messages');
      if(finite(stats.steps))counters.push(compact(stats.steps)+' steps');
      const speed=finite(stats.decode_tokens_per_second)?scaled(stats.decode_tokens_per_second)+' tok/s':'';
      if(counters.length||speed){const group=el('span','dc-stat');group.title=(stats.turns_basis==='conversation_requests'?'工作台会话轮数：已保存的用户请求。':'已记录的会话活动；未提供的指标不显示。')+partial;group.append(svg('M2.4 11.6a6 6 0 1 1 9.2 0M7 7l3-3M7 7v.01'),el('span',null,counters.join(' ')+(counters.length&&speed?' · ':'')+speed));dock.append(group);}
      const usage=[];
      if(finite(stats.tokens?.total))usage.push(compact(stats.tokens.total)+' tok');
      if(finite(stats.cache_hit_ratio)&&stats.cache_hit_ratio<=1)usage.push('Cache hit '+scaled(stats.cache_hit_ratio*100)+'%');
      if(usage.length){const group=el('span','dc-stat');group.title='代理报告的 token 用量与缓存命中率。'+partial;group.append(svg('M2 3c0-2.3 10-2.3 10 0s-10 2.3-10 0v8c0 2.3 10 2.3 10 0V3M2 7c0 2.3 10 2.3 10 0'),el('span',null,usage.join(' · ')));dock.append(group);}
      const context=stats.context;
      if(finite(context?.used)&&finite(context?.size)&&context.size>0){
        const percent=Math.min(100,Math.round(context.used/context.size*100)),meter=el('span','dc-context');
        meter.title='最新代理报告：上下文已用 '+context.used.toLocaleString()+' / '+context.size.toLocaleString()+' tokens。'+partial;meter.setAttribute('aria-label','上下文已用 '+percent+'%');
        const ring=document.createElementNS('http://www.w3.org/2000/svg','svg');ring.setAttribute('viewBox','0 0 14 14');ring.setAttribute('width','14');ring.setAttribute('height','14');ring.setAttribute('aria-hidden','true');
        for(const kind of ['track','fill']){const circle=document.createElementNS(ring.namespaceURI,'circle');for(const [key,value] of Object.entries({cx:7,cy:7,r:5.5,fill:'none','stroke-width':2,class:'dc-ring-'+kind}))circle.setAttribute(key,value);if(kind==='fill'){circle.setAttribute('stroke-dasharray',`${Math.PI*11*percent/100} ${Math.PI*11}`);circle.setAttribute('transform','rotate(-90 7 7)');}ring.append(circle);}
        meter.append(ring,el('span',null,percent+'%'));dock.append(meter);
      }
      dock.hidden=!dock.childElementCount;dock.title=partial+(stats.unavailable?.length?'部分指标尚无记录。':'');
    }
    function update(values={}){
      if(state.disposed)return;Object.assign(state,values);renderProviders();renderAvailability();
      for(const item of modelMenu.querySelectorAll('[data-provider]'))item.setAttribute('aria-checked',String(item.dataset.provider===(state.session?.provider||providers.value)));
      renderStats();reposition();
    }
    function dispose(){
      if(state.disposed)return;closeMenu();state.disposed=true;
      for(const remove of removers)remove();for(const dialog of dialogs){dialog.close();dialog.remove();}dialogs.clear();
      file.value='';form.querySelectorAll('button').forEach(node=>{node.onclick=null;node.onkeydown=null;});addMenu.onkeydown=modelMenu.onkeydown=null;
    }
    update();return {form,input,send,stop,providers,update,dispose,resize};
  }
  window.ForgeDevelopmentComposer=Object.freeze({create,connectionStatus});
})();
