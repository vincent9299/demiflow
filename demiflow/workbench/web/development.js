/* Project development projection. No provider credentials, commands or raw HTML. */
(() => {
  'use strict';
  /* Adapted from DeepSeek Harness 0.2.1-rc.2:
     ui-chat/use-scroll-follow.js and chat/ReasoningRow.js;
     ui-conversation/InputBar.js (send/stop SVGs, focus and IME handling);
     ui-model-selection/ModelSelect.js (disclosure and keyboard behavior).
MIT License

Copyright (c) 2026 DeepSeek

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/
  function latestCompletedParagraphFirstLine(text) {
    let summary = '';
    let paragraphStart = 0;
    const separator = /\r?\n(?:[\t ]*\r?\n)+/g;
    while (true) {
      const nextParagraph = separator.exec(text);
      const paragraphEnd = nextParagraph === null ? text.length : nextParagraph.index + nextParagraph[0].indexOf('\n');
      const newline = text.indexOf('\n', paragraphStart);
      if (newline !== -1 && newline <= paragraphEnd) {
        const candidate = text.slice(paragraphStart, newline).trim();
        if (candidate !== '') summary = candidate;
      }
      if (nextParagraph === null) return summary;
      paragraphStart = nextParagraph.index + nextParagraph[0].length;
    }
  }
  function scrollMetrics(element) {
  	const height = element.clientHeight;
  	return {
  		top: element.scrollTop,
  		height,
  		floor: Math.max(0, element.scrollHeight - height)
  	};
  }
  /** One scrollport's follow intent; native animation progress does not count as reader movement. */
  var ScrollFollow = class ScrollFollow {
  	following;
  	threshold;
  	static owners = /* @__PURE__ */ new WeakMap();
  	target = null;
  	sampledTop;
  	/**
  	* @param following - initial follow intent.
  	* @param threshold - accepted distance from the floor, in pixels.
  	*/
  	constructor(following, threshold) {
  		this.following = following;
  		this.threshold = threshold;
  	}
  	/**
  	* Find the mounted controller for reading-position compensation.
  	* @param element - scrollport with an optional follow owner.
  	* @returns its controller, when bound.
  	*/
  	static forElement(element) {
  		return this.owners.get(element);
  	}
  	/**
  	* Share this controller with reading-position compensation for the same scrollport.
  	* @param element - owned scrollport.
  	* @returns release the association on unmount or close.
  	*/
  	bind(element) {
  		ScrollFollow.owners.set(element, this);
  		return () => {
  			if (ScrollFollow.owners.get(element) === this) ScrollFollow.owners.delete(element);
  		};
  	}
  	/**
  	* Expose follow intent independently of the current offset.
  	* @returns whether content growth should follow the floor.
  	*/
  	get active() {
  		return this.following;
  	}
  	/**
  	* Expose outstanding native motion before accepting reader input.
  	* @returns whether a native follow animation has an outstanding target.
  	*/
  	get animating() {
  		return this.target !== null;
  	}
  	/**
  	* Classify bottom arrivals using this scrollport's own tolerance.
  	* @param metrics - current scroll geometry.
  	* @returns whether the position is within the follow threshold.
  	*/
  	nearBottom(metrics) {
  		return Math.abs(metrics.floor - metrics.top) <= this.threshold;
  	}
  	/**
  	* Commit caller-owned follow decisions without moving the scrollport.
  	* @param active - externally committed follow intent.
  	*/
  	setFollowing(active) {
  		this.following = active;
  		if (!active) this.target = null;
  	}
  	/** Adopt the next visible layout as a fresh reader position. */
  	reset() {
  		this.setFollowing(false);
  		this.sampledTop = void 0;
  	}
  	/**
  	* Adopt delivered scrolling while retaining intent during native animation.
  	* @param metrics - current geometry.
  	* @param movedByReader - caller attribution; omitted callers compare the last sampled position.
  	* @returns updated follow intent.
  	*/
  	sample(metrics, movedByReader = this.sampledTop === void 0 || Math.abs(metrics.top - this.sampledTop) > .5) {
  		this.sampledTop = metrics.top;
  		if (!this.animating && movedByReader) this.following = this.nearBottom(metrics);
  		return this.active;
  	}
  	/**
  	* Settle native scrolling; an off-target stop releases follow intent.
  	* @param metrics - actual geometry delivered at scrollend.
  	* @returns follow intent after completing or interrupting native motion.
  	*/
  	settle(metrics) {
  		const target = this.target;
  		this.target = null;
  		return this.sample(metrics, target === null ? void 0 : Math.abs(metrics.top - Math.min(target, metrics.floor)) > this.threshold);
  	}
  	/**
  	* Position immediately and adopt the resulting follow intent.
  	* @param element - scrolling element.
  	* @param metrics - geometry before positioning.
  	* @param top - requested offset, clamped to the measured range.
  	* @returns geometry after positioning.
  	*/
  	jump(element, metrics, top) {
  		const animated = this.animating;
  		this.target = null;
  		const target = Math.max(0, Math.min(metrics.floor, top));
  		if (animated) element.scrollTo({
  			top: target,
  			behavior: "instant"
  		});
  		else if (target !== metrics.top) element.scrollTop = target;
  		const landed = {
  			...metrics,
  			top: element.scrollTop
  		};
  		this.sampledTop = landed.top;
  		this.following = this.nearBottom(landed);
  		return landed;
  	}
  	/**
  	* Follow the measured floor, respecting reduced motion for smooth requests.
  	* Smooth requests preserve an outstanding target; immediate follow updates
  	* retarget active motion when content changes the floor.
  	* Within-tolerance positioning is immediate while no animation is outstanding.
  	* @param element - scrolling element.
  	* @param metrics - current geometry.
  	* @param behavior - native animation for growth, or immediate positioning unless motion is already active.
  	* @returns current geometry; smooth requests retain their starting position until native scroll delivery.
  	*/
  	toBottom(element, metrics, behavior) {
  		this.following = true;
  		if (metrics.top === metrics.floor) return this.jump(element, metrics, metrics.floor);
  		if (behavior === "instant" && this.animating) {
  			if (this.target !== metrics.floor) {
  				this.target = metrics.floor;
  				element.scrollTo({
  					top: metrics.floor,
  					behavior: "smooth"
  				});
  			}
  			return metrics;
  		}
  		if (behavior === "instant" || !this.animating && this.nearBottom(metrics)) return this.jump(element, metrics, metrics.floor);
  		if (typeof matchMedia === "function" && matchMedia("(prefers-reduced-motion: reduce)").matches) return this.jump(element, metrics, metrics.floor);
  		if (this.target === null) {
  			this.target = metrics.floor;
  			element.scrollTo({
  				top: metrics.floor,
  				behavior: "smooth"
  			});
  		}
  		return metrics;
  	}
  	/**
  	* Cancel native motion before a reader gesture; only subsequent actual movement changes follow intent.
  	* @param element - scrolling element.
  	* @param metrics - position at interruption.
  	*/
  	interrupt(element, metrics) {
  		if (!this.animating) return;
  		this.target = null;
  		this.sampledTop = metrics.top;
  		element.scrollTo({
  			top: metrics.top,
  			behavior: "instant"
  		});
  	}
  };
  const el=(tag,text,cls)=>{const e=document.createElement(tag);if(text!==undefined)e.textContent=text;if(cls)e.className=cls;return e;};
  const bytes=s=>new TextEncoder().encode(s).byteLength;
  const drafts=new Map(), active=new Set();
  try { const saved=sessionStorage.getItem('forge-development-drafts'); if(saved&&bytes(saved)<=2*1024*1024){const entries=JSON.parse(saved);if(Array.isArray(entries)&&entries.length<=64)for(const pair of entries)if(Array.isArray(pair)&&pair.length===2&&typeof pair[0]==='string'&&typeof pair[1]==='string'&&bytes(pair[1])<=32768)drafts.set(pair[0],pair[1]);}}catch{}
  const saveDrafts=()=>{try{sessionStorage.setItem('forge-development-drafts',JSON.stringify([...drafts]));}catch{}};
  const labels={preparing:'准备开发依据',starting:'正在连接',running:'正在开发',waiting_input:'等待回答',waiting_approval:'等待许可',stopping:'已请求停止，正在清理',completed:'本轮已完成',failed:'本轮失败',interrupted:'连接中断',cancelled:'已停止'};
  const developmentLabels={needs_input:'等待补充需求，检查已暂停',preparing:'准备开发依据',developing:'正在开发',queued:'等待检查',checking:'正在检查',verifying:'正在验证',reviewing:'正在独立审阅',repair_pending:'发现问题，准备自动修复',repairing:'正在自动修复',stopping:'已请求停止，正在清理',failed:'开发检查未通过',stale:'内容已改变，需要重新检查',review_unavailable:'检查尚未完成 · 待独立审阅',not_applicable:'本轮没有开发变更',cancelled:'开发检查已停止',unknown:'开发检查结果待核对'};
  function developmentLabel(value){
    if(value.state==='accepted')return value.source_fingerprint&&value.accepted_fingerprint===value.source_fingerprint&&['check','verify','review'].every(k=>value.gates?.[k]==='passed')?(value.assurance==='enforced'?'本次开发完成 · 受限执行':'本次开发完成 · 可信本地'):'开发检查结果待核对';
    if(value.state==='review_unavailable'&&value.gates?.check==='passed'&&value.gates?.verify==='passed')return '检查与验证通过 · 待独立审阅';
    return developmentLabels[value.state]||'开发检查结果待核对';
  }
  const uid=()=>crypto.randomUUID();
  const iconButton=(label,path,viewBox='0 0 24 24',filled=false)=>{const b=el('button',undefined,'development-icon');b.type='button';b.title=label;b.setAttribute('aria-label',label);const svg=document.createElementNS('http://www.w3.org/2000/svg','svg');svg.setAttribute('viewBox',viewBox);svg.setAttribute('aria-hidden','true');if(filled)svg.classList.add('development-filled-icon');const p=document.createElementNS(svg.namespaceURI,typeof path==='string'?'path':'rect');for(const [key,value]of Object.entries(typeof path==='string'?{d:path}:path))p.setAttribute(key,value);svg.append(p);b.append(svg);return b;};
  async function mount({main,api,workspaceId,view,mountText,disposeReaders,openSession,error,register}) {
    const state={disposed:false,timer:null,session:null,sequence:0,items:new Map(),textItems:new Map(),lastText:null,pagedText:null,textBytes:0,pending:new Set(),submission:null,dialogs:new Set()};
    const layout=el('section',undefined,'development-layout'),body=el('div',undefined,'development-conversation');
    layout.append(body);main.append(layout);main.classList.add('development-main');active.add(state);
    const handle={get sessionId(){return state.session?.id;},dispose(){state.disposed=true;clearTimeout(state.timer);state.observer?.disconnect();state.closeModel?.();active.delete(state);main.classList.remove('development-main');disposeReaders(layout);for(const dialog of state.dialogs)dialog.remove();state.dialogs.clear();}};register(handle);
    const head=el('header',undefined,'development-head'),title=el('span','项目对话','development-title');
    const archive=el('button','归档历史','development-archive');archive.type='button';archive.disabled=true;
    archive.title='压缩较早的上下文，保留原始消息';head.append(title,archive);body.append(head);
    const status=el('p','正在读取开发能力…','development-status');status.setAttribute('role','status');status.setAttribute('aria-live','polite');
    let capabilities=await api('dev-capabilities');state.capabilitiesAt=Date.now();if(!layout.isConnected)return {dispose(){}};
    const info=el('details',undefined,'development-info'),infoToggle=el('summary','对话信息'),infoBody=el('div',undefined,'development-info-body');
    const reason=el('p',capabilities.reason);infoBody.append(reason);info.append(infoToggle,infoBody);head.append(info);
    // The native select keeps the existing provider binding; ModelSelect's
    // compact trigger/menu is the visible control, without a React dependency.
    const providers=el('select');
    for(const p of capabilities.providers||[]){const option=el('option',p.id+' · '+p.model+(p.available?'':' · 暂不可用'));option.value=p.id;option.disabled=!p.available;providers.append(option);}
    const messages=el('nav',undefined,'development-message-rail'),versions=el('nav',undefined,'development-version-rail');
    messages.setAttribute('aria-label','历史消息定位');versions.setAttribute('aria-label','版本时间线');layout.prepend(messages);layout.append(versions);
    let turnOffset=0,versionOffset=0,listing=false;
    const stamp=value=>new Date(value*1000).toLocaleString('zh-CN',{month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit'});
    async function sessions(reset=false,offset=turnOffset,versionsAt=versionOffset){
      if(!capabilities.enabled||listing)return;
      listing=true;
      try{
        const requested=reset?0:offset,requestedVersions=reset?0:versionsAt;
        const [result,versionResult]=await Promise.all([api('dev-project-turns',{offset:requested,limit:20}),api('dev-project-turns',{offset:requestedVersions,limit:20,kind:'timeline'})]);if(state.disposed)return;
        turnOffset=requested;versionOffset=requestedVersions;messages.replaceChildren(el('span','消息','development-rail-caption'));versions.replaceChildren(el('span','迭代记录','development-rail-caption'));
        const nav=(host,label,direction,next)=>{const b=el('button',direction,'development-rail-page');b.title=label;b.setAttribute('aria-label',label);b.onclick=()=>sessions(false,host===messages?next:turnOffset,host===versions?next:versionOffset).catch(e=>error(body,e));host.append(b);};
        if(requested>0)nav(messages,'较新的消息','↑',Math.max(0,requested-20));
        if(requestedVersions>0)nav(versions,'较新的迭代','↑',Math.max(0,requestedVersions-20));
        let count=0;
        for(const row of result.turns){
          if(row.operation==='message'){
            const b=el('button',undefined,'development-message-mark');b.setAttribute('aria-label','定位消息：'+row.title);b.title=row.title;
            b.append(el('span',row.title,'development-rail-preview'));b.onclick=()=>jump(row).catch(e=>error(body,e));messages.append(b);
          }
        }
        for(const row of versionResult.turns){
          count++;
          const compact=row.operation==='compact',done=row.compaction?.status==='compacted';
          const name=compact?(done?'历史已归档':row.compaction?.status==='noop'?'暂无需归档':row.cleanup!=='complete'?'正在归档…':'归档未完成'):
            row.changed_files?'代码变更 · '+(row.revision?.after?.slice(0,7)||'已记录'):'检查记录';
          const b=el('button',undefined,'development-version-node');b.dataset.kind=compact?'archive':row.changed_files?'change':'check';b.title=name+' · '+stamp(row.created);b.setAttribute('aria-label',b.title);
          b.append(el('span',undefined,'development-version-dot'),el('span',name,'development-version-label'),el('time',stamp(row.created),'development-version-time'));
          b.onclick=()=>{
            const d=el('dialog',undefined,'development-version-detail'),close=el('button','关闭'),go=el('button','查看对应消息');close.onclick=()=>d.close();
            d.append(el('h2',name),el('p',stamp(row.created),'muted'));
            if(compact)d.append(el('p',done?'较早上下文已由 CLI 压缩为摘要，原始消息仍可查看。':row.compaction?.status==='noop'?'当前没有可安全压缩的较早上下文。':'归档尚未确认完成，原始记录保留。'));
            else d.append(el('p',row.changed_files?row.changed_files+' 个文件发生变化；这是观察到的项目快照，检查结果单独记录。':'本轮产生了检查记录，具体结果见对应消息。'));
            if(row.revision)d.append(el('p','快照 '+row.revision.before.slice(0,12)+' → '+row.revision.after.slice(0,12)));
            if(row.compaction?.shadowed_items)d.append(el('p','压缩了 '+row.compaction.shadowed_items+' 条 CLI 上下文记录'));
            if(row.compaction?.history_through_sequence)d.append(el('p','归档前的应用消息记录：1–'+row.compaction.history_through_sequence));
            go.onclick=()=>{d.close();jump(row).catch(e=>error(body,e));};d.append(go,close);document.body.append(d);trackDialog(d);d.showModal();
          };versions.append(b);
        }
        if(!count)versions.append(el('span','变更和归档将在这里留下记录','development-rail-empty'));
        if(result.next_offset!==null)nav(messages,'更早的消息','↓',result.next_offset);
        if(versionResult.next_offset!==null)nav(versions,'更早的迭代','↓',versionResult.next_offset);
      }finally{listing=false;}
    }
    const tools=el('div',undefined,'toolbar'),basis=el('button','查看开发依据'),recover=el('button','核对连接状态');tools.append(basis,recover);infoBody.append(tools);
    const stage=el('div',undefined,'development-stage'),hero=el('div',undefined,'development-hero');hero.append(el('h2','今天想做些什么？'));body.append(stage);
    const earlier=el('button','加载更早的记录'),latest=el('button','回到最新记录');earlier.hidden=true;latest.hidden=true;stage.append(earlier,latest);
    const timeline=el('div',undefined,'development-timeline');timeline.setAttribute('aria-label','开发对话记录');stage.append(timeline,hero);const flow=el('div',undefined,'development-flow');timeline.append(flow);const follow=new ScrollFollow(true,60);timeline.onscroll=()=>{follow.sample(scrollMetrics(timeline));if(follow.active)newContent.hidden=true;};
    const newContent=el('button','有新内容，滚动到底部');newContent.hidden=true;stage.append(newContent);newContent.onclick=()=>{follow.toBottom(timeline,scrollMetrics(timeline),'instant');newContent.hidden=true;};
    state.observer=new ResizeObserver(()=>{if(!state.disposed&&follow.active)follow.toBottom(timeline,scrollMetrics(timeline),'instant');});state.observer.observe(flow);state.observer.observe(timeline);
    const pendingHost=el('div',undefined,'development-pending');stage.append(pendingHost);
    const form=el('form',undefined,'development-composer'),input=el('textarea');input.rows=1;input.placeholder='发消息，讨论或修改这个项目…';input.setAttribute('aria-label','开发要求');
    const controls=el('div',undefined,'development-controls'),send=iconButton('发送','M8.3125 0.980183C8.66767 1.0531 8.97902 1.20418 9.2627 1.43233C9.48724 1.61297 9.73029 1.85793 9.97949 2.10714L14.707 6.83468L13.293 8.24874L9 3.95577V15.0417H7V3.95577L2.70703 8.24874L1.29297 6.83468L6.02051 2.10714C6.26971 1.85793 6.51277 1.61297 6.7373 1.43233C6.97662 1.23986 7.28445 1.04402 7.6875 0.980183C7.8973 0.947006 8.1031 0.95516 8.3125 0.980183Z','0 0 16 16',true),stop=iconButton('停止',{x:3,y:3,width:10,height:10,rx:3},'0 0 16 16',true);send.type='submit';send.disabled=true;stop.disabled=true;
    send.classList.add('development-send');stop.classList.add('development-send');
    send.title='发送 · Enter';stop.title='停止 · Esc';
    const modelPicker=el('div',undefined,'development-model'),modelTrigger=iconButton('开发代理','M6 9l6 6 6-6'),modelName=el('span',undefined,'development-model-name'),modelMenu=el('div',undefined,'development-model-menu');
    modelTrigger.className='development-model-trigger';modelTrigger.prepend(modelName);modelTrigger.setAttribute('aria-haspopup','menu');modelTrigger.setAttribute('aria-expanded','false');
    modelMenu.id='development-model-'+uid();modelMenu.setAttribute('role','menu');modelMenu.setAttribute('aria-label','选择模型');modelMenu.hidden=true;modelTrigger.setAttribute('aria-controls',modelMenu.id);modelPicker.append(modelTrigger,modelMenu);
    const closeModel=(restore=false)=>{modelMenu.hidden=true;modelTrigger.setAttribute('aria-expanded','false');document.removeEventListener('pointerdown',outsideModel);if(restore)modelTrigger.focus();};
    const outsideModel=e=>{if(!modelPicker.contains(e.target))closeModel();};state.closeModel=closeModel;
    function openModel(last=false){
      if(modelTrigger.disabled)return;modelMenu.replaceChildren();
      for(const p of capabilities.providers||[]){
        const option=el('button',undefined,'development-model-option'),copy=el('span',undefined,'development-model-copy');option.type='button';option.setAttribute('role','menuitemradio');option.setAttribute('aria-checked',String(providers.value===p.id));option.disabled=!p.available;
        copy.append(el('span',p.model),el('small',p.id+(p.available?'':' · 暂不可用')));option.append(copy,el('span',providers.value===p.id?'✓':'','development-model-check'));
        option.onclick=()=>{providers.value=p.id;closeModel();availability();input.focus();};modelMenu.append(option);
      }
      modelMenu.hidden=false;modelTrigger.setAttribute('aria-expanded','true');document.addEventListener('pointerdown',outsideModel);
      const options=[...modelMenu.querySelectorAll('button:not(:disabled)')];(last?options.at(-1):options.find(o=>o.getAttribute('aria-checked')==='true')||options[0])?.focus();
    }
    modelTrigger.onclick=()=>modelMenu.hidden?openModel():closeModel(true);
    modelPicker.onkeydown=e=>{
      if(e.key==='Escape'&&!modelMenu.hidden){e.preventDefault();e.stopPropagation();closeModel(true);}
      else if(['ArrowDown','ArrowUp','Home','End'].includes(e.key)){
        e.preventDefault();if(modelMenu.hidden){openModel(e.key==='ArrowUp'||e.key==='End');return;}
        const options=[...modelMenu.querySelectorAll('button:not(:disabled)')],at=options.indexOf(document.activeElement);
        const next=e.key==='Home'?0:e.key==='End'?options.length-1:(at+(e.key==='ArrowUp'?-1:1)+options.length)%options.length;options[next]?.focus();
      }
    };
    modelPicker.onfocusout=e=>{if(!modelPicker.contains(e.relatedTarget))closeModel();};
    const disconnected=el('span',capabilities.service_configured?'本项目未授予开发能力':'未连接 CLI','development-provider-label');providers.hidden=!(capabilities.providers||[]).length;disconnected.hidden=!providers.hidden;
    const hint=el('span','Enter 发送 · Shift+Enter 换行','development-composer-hint'),trailing=el('div',undefined,'development-controls-trailing');trailing.append(modelPicker,disconnected,send,stop);controls.append(hint,trailing);form.append(input,controls);stage.append(form,status);
    form.onmousedown=e=>{if(input.disabled||e.button!==0||e.defaultPrevented||e.target.closest('button,a,input,select,textarea,[tabindex]'))return;e.preventDefault();input.focus();};
    send.onmousedown=stop.onmousedown=e=>{if(e.button===0)e.preventDefault();};
    let draftKey=workspaceId+':project';input.value=drafts.get(draftKey)||'';
    const resizeInput=()=>{input.style.height='auto';input.style.height=Math.min(input.scrollHeight,220)+'px';};
    input.oninput=()=>{resizeInput();if(bytes(input.value)>32768){input.setCustomValidity('要求不能超过 32 KiB');return;}input.setCustomValidity('');if(!drafts.has(draftKey)&&drafts.size>=64){input.setCustomValidity('已保存 64 份草稿，请先清空不再需要的草稿。');return;}if(input.value)drafts.set(draftKey,input.value);else drafts.delete(draftKey);saveDrafts();availability();};
    let composing=false,composingUntil=0;
    input.addEventListener('compositionstart',()=>{composing=true;});input.addEventListener('compositionend',()=>{composing=false;composingUntil=Date.now()+10;});
    input.onkeydown=e=>{if(composing||Date.now()<composingUntil||e.isComposing||e.keyCode===229)return;if(e.key==='Enter'&&!e.shiftKey&&!e.altKey){e.preventDefault();if(!e.repeat)form.requestSubmit();}if(e.key==='Escape'&&!stop.disabled)stop.click();};
    function availability(){
      const t=state.session?.turn,busy=t&&(t.cleanup!=='complete'||t.development?.busy===true);
      const provider=(capabilities.providers||[]).find(p=>p.id===(state.session?.provider||providers.value));
      const readonly=capabilities.read_only===true;
      const unapproved=capabilities.service_configured===true&&capabilities.project_authorized===false;
      send.disabled=readonly||!!state.historyOnly||!capabilities.start||!provider?.available||!!busy||!!state.sending||!input.value.trim();
      stop.disabled=readonly||!capabilities.enabled||!busy;stop.hidden=!busy;send.hidden=!!busy;
      input.disabled=readonly||!!state.historyOnly||!capabilities.enabled||!!state.sending;
      providers.disabled=readonly||!capabilities.enabled||!!state.session||!!state.creation||!!state.sending;
      providers.hidden=!(capabilities.providers||[]).length;disconnected.hidden=!providers.hidden;
      modelPicker.hidden=providers.hidden;modelTrigger.disabled=providers.disabled;modelName.textContent=provider?.model||'选择模型';modelTrigger.title=provider?provider.model+' · '+provider.id:'选择开发代理';if(modelTrigger.disabled)closeModel();
      archive.disabled=readonly||!capabilities.start||!provider?.available||!!busy||!!state.sending||!!state.historyOnly||!state.session?.revision;
      basis.disabled=!t;recover.disabled=readonly||!capabilities.enabled||!state.session;
      title.textContent=readonly||state.historyOnly?'历史消息 · 只读':'项目对话';
      hero.hidden=!!state.items.size;layout.classList.toggle('development-empty',!state.items.size);
      disconnected.textContent=unapproved?'本项目未授予开发能力':capabilities.service_configured?'已有开发服务连接不可用':'未连接 CLI';
      status.textContent=t?(t.operation==='compact'?(t.cleanup!=='complete'?'正在归档历史…':t.state==='completed'?'归档处理完成':'归档未完成'):(t.development?developmentLabel(t.development):(labels[t.state]||t.state)))+(t.dispatch==='unknown'?' · 发送结果未知，请先核对':''):
        readonly?'已有开发历史 · 只读':unapproved?'本项目未授予开发能力':provider?.available?'':capabilities.service_configured?'已有开发服务当前不可用':'尚未连接开发代理';
      reason.textContent=capabilities.reason+(t?.development?' 开发验收由 Forge 独立检查；'+(t.development.assurance==='enforced'?'开发进程受外部执行边界约束。':capabilities.mode==='restricted'?'本次受限开发的验收证据尚未齐备。':'当前为可信本地模式，未防篡改。')+(t.development.reason?' '+t.development.reason:''):'')+(provider?.reason?' '+provider.reason:'')+(provider?.input_requests==='unsupported'?' 此代理不支持轮内补充输入；停止后可继续。':'')+(provider?.history_replay==='unsupported'?' 恢复时仅展示本应用已保存的历史。':'');
    }
    providers.onchange=availability;
    function trackDialog(dialog){state.dialogs.add(dialog);dialog.addEventListener('close',()=>{state.dialogs.delete(dialog);dialog.remove();},{once:true});}
    function output(item){
      const d=el('dialog'),bar=el('div',undefined,'toolbar'),close=el('button','关闭'),next=el('button','下一页原文'),content=el('pre');let cursor=0;
      const reference={...item.sourceRef},sid=state.displaySession||state.session.id;cursor=reference.from_sequence-1;
      close.onclick=()=>d.close();bar.append(close,next);d.append(el('h2','已保存原文 · 当前消息片段'),bar,content);document.body.append(d);trackDialog(d);
      async function load(){next.disabled=true;try{
        let page;
        if(capabilities.backend==='existing_http'){
          // Older owners need no protocol upgrade: filter their existing bounded
          // event page by the exact persisted identities and visible segment.
          const saved=await api('dev-events',{session_id:sid,after_sequence:cursor,limit:50});
          const matches=event=>event.turn_id===reference.turn_id&&(!reference.provider_epoch||event.provider_epoch===reference.provider_epoch)&&event.sequence>=reference.from_sequence&&event.sequence<=reference.through_sequence&&(event.data.item_id===reference.item_id||event.data.id===reference.item_id||event.type==='message.user'&&reference.item_id===event.turn_id+':user'||event.type==='change.observed'&&reference.item_id==='change:'+event.sequence||event.type==='check.reference'&&reference.item_id==='check:'+event.sequence);
          page={chunks:saved.events.filter(matches).map(event=>({text:event.data.text??JSON.stringify(event.data,null,2)})),next_sequence:Math.min(saved.next_sequence,reference.through_sequence),last_sequence:Math.min(saved.last_sequence,reference.through_sequence)};
        }else page=await api('dev-output',{session_id:sid,...reference,after_sequence:cursor});
        cursor=page.next_sequence;content.textContent=page.chunks.map(c=>c.text).join('');next.hidden=cursor>=page.last_sequence;
      }catch(e){error(d,e);}finally{next.disabled=false;}}
      next.onclick=load;d.showModal();load();
    }
    const scoped=(event,kind,id)=>JSON.stringify([event.session_id,event.turn_id,event.provider_epoch||'',kind,id]);
    function card(id,label,event,rawId=id){
      let item=state.items.get(id);
      if(!item){
        while(state.items.size>=100){const first=state.items.keys().next().value;const old=state.items.get(first);disposeReaders(old.node);old.node.remove();state.textBytes-=bytes(old.text);state.items.delete(first);if(old.textKey&&state.textItems.get(old.textKey)===first)state.textItems.delete(old.textKey);earlier.hidden=false;}
        const chat=label==='你'||label==='助手',process=label==='模型过程'||label==='思考摘要',node=el(chat?'article':'details',undefined,'development-message'),heading=el(chat?'span':'summary',label),content=el('div'),source=iconButton('分页查看原文','M14 3H6a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9zM14 3v6h6M8 13h8M8 17h5');source.title='查看原文';source.classList.add('development-raw');source.onclick=()=>output(item);source.hidden=!event;node.classList.add(chat?(label==='你'?'development-user':'development-assistant'):process?'development-process':'development-tool');if(chat)heading.className='sr-only';node.append(heading,content,source);flow.append(node);hero.hidden=true;layout.classList.remove('development-empty');
        const preview=process?el('span',undefined,'development-process-preview'):null;
        if(process)heading.replaceChildren(el('span',label,'development-process-title'),preview);
        item={node,heading,content,text:'',source,role:label,preview};state.items.set(id,item);
      }
      if(event){
        if(!item.sourceRef)item.sourceRef={item_id:rawId,turn_id:event.turn_id,...(event.provider_epoch?{provider_epoch:event.provider_epoch}:{}),from_sequence:event.sequence,through_sequence:event.sequence};
        else item.sourceRef.through_sequence=event.sequence;
      }
      return item;
    }
    function finishText(){
      const item=state.lastText&&state.items.get(state.lastText.id);state.lastText=null;
      if(item){item.node.classList.remove('development-streaming');putText(item,item.text,false,true);}
    }
    function message(event){
      const d=event.data,key=scoped(event,'message:'+ (d.channel||'answer'),d.item_id),complete=event.type==='message.completed';
      let id=complete?state.textItems.get(key):state.lastText?.key===key?state.lastText.id:null;
      if(!id||!state.items.has(id)){
        finishText();id=key+':'+event.sequence;
      }
      const item=card(id,d.channel==='process'?'模型过程':d.channel==='summary'?'思考摘要':'助手',event,d.item_id);
      item.textKey=key;state.textItems.set(key,id);
      if(complete){
        if(d.interrupted||d.status==='incomplete'){
          if(!item.incomplete){item.incomplete=el('span','未完成','development-message-state');if(item.preview)item.heading.append(item.incomplete);else item.source.before(item.incomplete);}
        }else{item.incomplete?.remove();item.incomplete=null;}
        item.node.classList.remove('development-streaming');
        if(d.paged){
          // Older workers store large completion bodies as adjacent output
          // chunks. Replace preceding deltas without changing the prose role.
          putText(item,'');item.sourceRef.from_sequence=event.sequence;
          state.lastText={key,id};state.pagedText=key;
        }else{item.sourceRef.from_sequence=event.sequence;putText(item,d.text,false,true);if(state.lastText?.id===id)state.lastText=null;}
      }
      else{state.lastText={key,id};item.node.classList.add('development-streaming');putText(item,d.text,true);}
    }
    function putText(item,text,append=false,markdown=false){
      const previous=bytes(item.text),value=append?item.text+text:text,n=bytes(value);
      const overBudget=n>65536||state.textBytes-previous+n>1024*1024;
      if(item.preview){
        const running=item.node.classList.contains('development-streaming');
        const summary=running?latestCompletedParagraphFirstLine(overBudget?item.text:value).replaceAll('**',''):'';
        item.preview.textContent=running?(summary.slice(0,256)+(summary.length>256?'…':'')||'正在输出…'):'';
      }
      if(overBudget){if(!item.previewTruncated){item.source.classList.add('development-raw-overflow');item.source.append(el('span','超出预览预算 · 查看原文'));item.source.title='内容超过预览预算，分页查看该段原文';item.previewTruncated=true;}return;}
      state.textBytes+=n-previous;item.text=value;
      if(append&&item.content.firstElementChild?.classList.contains('development-stream')){const pre=item.content.firstElementChild;if(pre.firstChild)pre.firstChild.appendData(text);else pre.append(document.createTextNode(text));return;}
      disposeReaders(item.content);item.content.replaceChildren();
      if(markdown){const reader=mountText(item.content,item.text),options=el('details',undefined,'development-reader-options');options.append(el('summary','显示选项'));for(const control of reader.element.querySelectorAll('.reading-toolbar,.reading-status'))options.append(control);reader.element.append(options);}else item.content.append(el('pre',item.text,'development-stream'));
    }
    function project(event){
      const d=event.data;
      const completionBody=event.type==='output.delta'&&state.pagedText===scoped(event,'message:'+(d.channel||'answer'),d.item_id);
      if(!completionBody)state.pagedText=null;
      if(event.type==='message.user'||event.type==='output.delta'&&!completionBody||event.type.startsWith('item.'))finishText();
      if(event.type==='message.user')putText(card(event.turn_id+':user','你',event),d.text);
      else if(event.type==='message.delta'||event.type==='message.completed')message(event);
      else if(completionBody)message({...event,type:'message.delta'});
      else if(event.type==='output.delta')putText(card(scoped(event,'output',d.item_id),'工具输出',event,d.item_id),d.text,true);
      else if(event.type.startsWith('item.')){
        const item=card(scoped(event,'item',d.id),d.type==='fileChange'?'文件变更':'工具',event,d.id);putText(item,JSON.stringify(d,null,2));
      }else if(event.type==='change.observed'){
        const item=card('change:'+event.sequence,'观察到的变化 · 验证待更新',event);putText(item,d.files.map(f=>f.path+(f.within_scope?'':' · 超出范围')+'\n'+(f.diff||f.diff_unavailable||'')).join('\n'));
      }else if(event.type==='development.state'){
        if(!d.busy){const item=card('development:'+event.turn_id,'开发检查记录');item.source.hidden=true;putText(item,developmentLabel(d)+(d.reason?'\n'+d.reason:''));}
      }else if(event.type==='development.repair.started'){
        const item=card('repair:'+event.sequence,'自动修复 · 第 '+d.round+' 次');item.source.hidden=true;
        putText(item,'根据检查与审阅结果继续当前任务。'+(Array.isArray(d.feedback)?'\n'+d.feedback.map(f=>f.reason||'').filter(Boolean).join('\n'):''));
      }else if(event.type==='check.reference'){putText(card('check:'+event.sequence,'公开开发检查记录'+(d.review_kind==='self'?' · 自身审查':''),event),JSON.stringify(d.result,null,2));
      }else if(event.type==='provider.error'){putText(card('error:'+event.sequence,'代理报告错误'),JSON.stringify(d));}
      else if(event.type==='compaction.completed'){if(d.status==='compacted'&&!state.historyOnly){clearTimeline();earlier.hidden=false;}const item=card('archive:'+event.sequence,'历史归档');item.node.classList.add('development-archive-card');item.node.open=true;item.source.hidden=true;putText(item,d.status==='compacted'?'较早上下文已压缩为摘要。原始消息已保留，可从左侧定位查看。':'当前没有需要压缩的较早上下文。');}
      else if(event.type==='history.gap'){putText(card('history:'+event.sequence,'恢复说明'),'代理未回放历史；这里展示的是本应用先前保存的记录。');}
      else if(event.type==='turn.state'&&d.cleanup==='complete'){
        finishText();
        for(const item of state.items.values())if(['助手','模型过程','思考摘要'].includes(item.role)&&item.sourceRef?.turn_id===event.turn_id){item.node.classList.remove('development-streaming');putText(item,item.text,false,true);}
        if(d.result){try{const result=JSON.parse(d.result);if(result.reason)putText(card('result:'+event.turn_id,'本轮结果'),result.reason);}catch{}}
      }
      if(follow.active)follow.toBottom(timeline,scrollMetrics(timeline),'instant');else if(event.type==='message.delta'||event.type==='message.user')newContent.hidden=false;
    }
    function pending(request){
      if(state.pending.has(request.id))return;state.pending.add(request.id);
      const card=el('section',undefined,'development-input');card.dataset.request=request.id;card.append(el('h3',request.request.method.endsWith('requestUserInput')?'需要你的回答':'操作许可'));
      const fields=new Map(),r=request.request;
      if(r.method.endsWith('requestUserInput')){
        for(const q of r.params.questions){const label=el('label',q.question),answer=el('textarea');answer.rows=2;answer.setAttribute('aria-label',q.question);label.append(answer);card.append(label);fields.set(q.id,answer);if(q.options?.length)card.append(el('p',q.options.map(o=>o.label+': '+o.description).join('；'),'muted'));}
      }else {const tool=r.params.known_tool;card.append(el('pre',tool?JSON.stringify({tool:tool.title||tool.kind,arguments:tool.rawInput},null,2):r.params.command||r.params.reason||'请求增加操作权限；本部署不允许扩权。'));}
      const submit=el('button',r.method.endsWith('requestUserInput')?'提交回答':'允许一次','development-input-confirm'),deny=el('button','拒绝','development-input-deny');submit.disabled=!r.allow_once;
      async function respond(answer){submit.disabled=deny.disabled=true;try{await api('dev-input',{}, {session_id:state.session.id,turn_id:state.session.turn.id,pending_request_id:request.id,expected_digest:request.digest,answer});card.replaceChildren(el('p','回答已保存，等待代理接收。'));}catch(e){error(card,e);submit.disabled=!r.allow_once;deny.disabled=false;}}
      submit.onclick=()=>respond(r.method.endsWith('requestUserInput')?Object.fromEntries([...fields].map(([k,v])=>[k,v.value])):'allow_once');deny.onclick=()=>respond('deny');card.append(submit);if(!r.method.endsWith('requestUserInput'))card.append(deny);pendingHost.append(card);
    }
    function refresh(){
      if(state.disposed||!state.session)return Promise.resolve();
      if(state.refreshing)return state.refreshing;
      clearTimeout(state.timer);
      state.refreshing=(async()=>{
        let delay=1500;
        try{
          if(capabilities.backend==='existing_http'&&(!state.capabilitiesAt||Date.now()-state.capabilitiesAt>=5000)){
            const wasUnavailable=!capabilities.enabled||capabilities.read_only;
            capabilities=await api('dev-capabilities');state.capabilitiesAt=Date.now();
            if(wasUnavailable||!capabilities.enabled||capabilities.read_only){pendingHost.replaceChildren();state.pending.clear();}
            availability();
          }
          const current=await api('dev-session',{session_id:state.session.id});if(state.disposed)return;state.session=current;availability();const revisionKey=current.revision+':'+current.turn?.cleanup;if(state.railRevision!==revisionKey){state.railRevision=revisionKey;await sessions(true);}
          if(capabilities.enabled&&!capabilities.read_only)for(const request of current.pending||[])pending(request);
          for(const node of pendingHost.children)if(capabilities.read_only||!(current.pending||[]).some(p=>p.id===node.dataset.request))node.remove();
          if(!state.historyOnly){
            const busy=current.turn&&(current.turn.cleanup!=='complete'||current.turn.development?.busy===true);
            const epoch=state.feedEpoch;const page=await api('dev-events',{session_id:current.id,after_sequence:state.sequence,limit:50,wait_ms:busy?1500:0});if(state.disposed||epoch!==state.feedEpoch)return;
            for(const event of page.events)project(event);state.sequence=page.next_sequence;
            delay=state.sequence<page.last_sequence||busy?25:1500;
          }
          state.connectionError?.remove();state.connectionError=null;
        }catch(e){if(!state.disposed&&e.name!=='AbortError'){state.connectionError?.remove();state.connectionError=error(body,e);status.textContent='连接中断，正在重新连接…';delay=2500;}}
        finally{state.refreshing=null;if(!state.disposed)state.timer=setTimeout(refresh,delay);}
      })();
      return state.refreshing;
    }
    form.onsubmit=async e=>{e.preventDefault();if(send.disabled||!input.reportValidity()||!input.value.trim())return;send.disabled=true;
      const message=input.value,submission=state.submission||{text:message,id:uid()};if(submission.text!==message){error(body,new Error('上次发送结果未确认，请先核对记录后再编辑要求。'));return;}state.submission=submission;
      state.sending=true;availability();
      try{if(!state.session){state.creation||={request_id:uid(),provider_id:providers.value};state.session=await api('dev-session-create',{},state.creation);state.createdHere=true;}if(state.disposed)return;await api('dev-turn-start',{}, {session_id:state.session.id,submission_id:submission.id,expected_revision:state.session.revision,text:message});state.submission=null;input.value='';resizeInput();drafts.delete(draftKey);saveDrafts();if(state.createdHere){await openSession(state.session.id);return;}clearTimeout(state.timer);await refresh();await sessions(true);}
      catch(err){if(!state.disposed){error(body,err);await refresh().catch(()=>{});await sessions(true).catch(()=>{});}}
      finally{state.sending=false;if(!state.disposed)availability();}
    };
    archive.onclick=async()=>{
      if(archive.disabled)return;
      state.sending=true;availability();
      state.archiveSubmission||=uid();
      try{await api('dev-compact',{}, {session_id:state.session.id,submission_id:state.archiveSubmission,expected_revision:state.session.revision});state.archiveSubmission=null;await refresh();}
      catch(e){error(body,e);await refresh().catch(()=>{});}
      finally{state.sending=false;if(!state.disposed)availability();}
    };
    stop.onclick=async()=>{stop.disabled=true;try{await api('dev-cancel',{}, {session_id:state.session.id,turn_id:state.session.turn.id,request_id:uid()});status.textContent='已请求停止，等待所属工具清理。';}catch(e){error(body,e);stop.disabled=false;}};
    recover.onclick=async()=>{try{const value=await api('dev-recover',{}, {session_id:state.session.id,expected_revision:state.session.revision});status.textContent=value.recovery.can_continue?'连接状态已核对，可继续。':'清理归属尚未确认，保留写租约。';}catch(e){error(body,e);}};
    basis.onclick=async()=>{try{const value=await api('dev-context',{session_id:state.session.id,turn_id:state.session.turn.id});if(state.disposed)return;const d=el('dialog'),close=el('button','关闭');close.onclick=()=>d.close();d.append(el('h2','本轮开发依据'),close,el('pre',JSON.stringify(value.context,null,2)));document.body.append(d);trackDialog(d);d.showModal();}catch(e){error(body,e);}};
    function clearTimeline(){state.feedEpoch=(state.feedEpoch||0)+1;disposeReaders(flow);flow.replaceChildren();state.items.clear();state.textItems.clear();state.lastText=null;state.pagedText=null;state.textBytes=0;newContent.hidden=true;}
    async function loadWindow(sid,after){
      state.historyOnly=true;state.displaySession=sid;follow.setFollowing(false);clearTimeline();
      const epoch=state.feedEpoch;state.windowStart=after;earlier.hidden=after===0;latest.hidden=false;availability();
      const page=await api('dev-events',{session_id:sid,after_sequence:after,limit:50});
      if(state.disposed||state.feedEpoch!==epoch)return;
      for(const event of page.events)project(event);timeline.scrollTop=0;
    }
    async function jump(row){await loadWindow(row.session,Math.max(0,(row.sequence||1)-1));}
    earlier.onclick=()=>loadWindow(state.displaySession||state.session.id,Math.max(0,(state.windowStart||0)-50)).catch(e=>error(body,e));
    latest.onclick=async()=>{follow.setFollowing(true);clearTimeline();state.historyOnly=false;state.displaySession=null;state.sequence=Math.max(0,state.session.sequence-50,(state.session.archive_sequence||0)-1);state.windowStart=state.sequence;earlier.hidden=state.sequence===0;latest.hidden=true;clearTimeout(state.timer);await refresh();};
    await sessions();if(state.disposed)return handle;
    if(capabilities.enabled){
      state.session=(await api('dev-conversation')).session;
      if(state.disposed)return handle;
      if(state.session){providers.value=state.session.provider;input.value=drafts.get(draftKey)||drafts.get(workspaceId+':'+state.session.id)||'';state.sequence=Math.max(0,state.session.sequence-50,(state.session.archive_sequence||0)-1);state.windowStart=state.sequence;earlier.hidden=state.sequence===0;await refresh();}
    }
    availability();resizeInput();
    return handle;
  }
  globalThis.ForgeDevelopment=Object.freeze({mount,activeCount:()=>active.size});
})();
