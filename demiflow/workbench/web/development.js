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
  const drafts=new Map(),draftRevisions=new Map(),active=new Set();let draftSerial=0;
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
  async function mount({main,api,workspaceId,view,mountText,disposeReaders,openSession,openNew,error,register}) {
    const state={disposed:false,initializing:true,timer:null,session:null,sequence:0,items:new Map(),pending:new Set(),submission:null,dialogs:new Set(),stats:null,statsAt:0,newDraft:view?.kind==='development_new',listOffset:0,listAt:0,writerBusy:false,activeSession:null};
    const layout=el('section',undefined,'development-layout'),body=el('div',undefined,'development-conversation');
    layout.append(body);main.append(layout);main.classList.add('development-main');active.add(state);
    const handle={get sessionId(){return state.session?.id;},dispose(){if(state.disposed)return;state.disposed=true;clearTimeout(state.timer);state.observer?.disconnect();state.releaseViewport?.();state.navigationResize?.dispose();state.releaseMenus?.();state.composer?.dispose();state.chat?.dispose();active.delete(state);main.classList.remove('development-main');disposeReaders(layout);for(const dialog of state.dialogs)dialog.remove();state.dialogs.clear();}};register(handle);
    const sidebar=el('aside',undefined,'development-session-sidebar'),sideHeading=el('div',undefined,'development-session-heading');
    const createNew=el('button',undefined,'development-new-conversation');createNew.type='button';createNew.append(el('span','＋'),el('span','新会话'));createNew.disabled=true;
    const navigation=ForgeNavigation.header({title:'会话',searchLabel:'搜索会话',placeholder:'筛选当前页会话',onSearch:()=>{if(!state.initializing)renderConversations();}});
    sideHeading.append(createNew);
    const sessionList=el('nav',undefined,'development-session-list');sessionList.setAttribute('aria-label','项目会话');
    const sessionPages=el('div',undefined,'development-session-pages'),sessionNote=el('p','正在读取会话…','development-session-note');sidebar.append(navigation.element,sideHeading,sessionList,sessionPages,sessionNote);layout.prepend(sidebar);
    state.navigationResize=ForgeNavigation.resizable({layout,sidebar,label:'会话目录',active:false});
    const backdrop=el('button',undefined,'development-sidebar-backdrop');backdrop.type='button';backdrop.setAttribute('aria-label','关闭会话列表');layout.append(backdrop);
    const head=el('div',undefined,'development-controls'),sideToggle=iconButton('会话列表','M3 4h18v16H3zM9 4v16');
    function sidebarOpen(value){layout.dataset.sidebar=value?'open':'closed';sideToggle.setAttribute('aria-expanded',String(value));sidebar.hidden=!value;backdrop.hidden=!value;state.navigationResize.setActive(value);if(value)navigation.actions.append(sideToggle);else head.prepend(sideToggle);}
    sideToggle.onclick=()=>{sidebarOpen(layout.dataset.sidebar!=='open');sideToggle.focus();};
    const closeSidebar=()=>{sidebarOpen(false);sideToggle.focus();};backdrop.onclick=closeSidebar;
    sidebar.onkeydown=event=>{if(event.key==='Escape'&&!event.defaultPrevented){event.preventDefault();closeSidebar();}};
    let narrowViewport=matchMedia('(max-width:900px)').matches;sidebarOpen(!narrowViewport);
    createNew.onclick=()=>{if(!createNew.disabled&&typeof openNew==='function')openNew();};
    const archive=el('button','归档历史','development-archive');archive.type='button';archive.disabled=true;archive.title='压缩较早的上下文，保留原始消息';
    const more=el('details',undefined,'development-more'),moreToggle=el('summary','···'),moreMenu=el('div',undefined,'development-menu');
    moreToggle.setAttribute('aria-label','更多对话操作');more.append(moreToggle,moreMenu);head.append(more);body.append(head);
    const mode=el('details',undefined,'development-mode'),modeLabel=el('summary','显示模式 · Standard'),modeMenu=el('div',undefined,'development-mode-options');
    modeLabel.setAttribute('aria-label','对话显示模式');mode.append(modeLabel,modeMenu);
    for(const [value,label,description] of [['compact','Compact mode','只保留回复，过程可展开'],['standard','Standard mode','显示最近一轮的紧凑过程'],['detailed','Detailed mode','展开过程组及详细信息']]){
      const option=el('button',label);option.type='button';option.title=description;
      option.onclick=()=>{state.chat?.setMode(value);modeLabel.textContent='显示模式 · '+label.replace(' mode','');mode.open=false;more.open=false;};modeMenu.append(option);
    }
    const trajectoryToggle=el('button','查看轨迹'),history=el('details',undefined,'development-history-panel'),historyLabel=el('summary','历史消息定位');history.append(historyLabel);
    const info=el('details',undefined,'development-info'),infoToggle=el('summary','对话信息'),infoBody=el('div',undefined,'development-info-body');
    const reason=el('p','正在读取开发能力…');infoBody.append(reason);info.append(infoToggle,infoBody);moreMenu.append(mode,trajectoryToggle,history,info,archive);
    const status=el('p','正在读取开发能力…','development-status');status.setAttribute('role','status');status.setAttribute('aria-live','polite');
    let capabilities=await api('dev-capabilities');state.capabilitiesAt=Date.now();if(state.disposed||!layout.isConnected)return handle;
    const messages=el('nav',undefined,'development-message-rail'),versions=el('nav',undefined,'development-version-rail');
    messages.setAttribute('aria-label','历史消息定位');versions.setAttribute('aria-label','版本时间线');history.append(messages);
    const running=value=>!!(value?.turn&&(value.turn.cleanup!=='complete'||value.turn.development?.busy===true));
    const sessionReadonly=value=>!!value&&(value.writable===false||value.writable!==true&&(capabilities.multiple_conversations===true||!state.createdHere&&value.id!==state.primaryId));
    function renderConversations(){
      sessionList.replaceChildren();sessionPages.replaceChildren();
      createNew.disabled=capabilities.multiple_conversations!==true||!capabilities.enabled||capabilities.read_only===true||typeof openNew!=='function';
      createNew.title=capabilities.multiple_conversations===true?'新建一个空白会话':'当前开发服务仅支持一个项目会话';
      createNew.setAttribute('aria-current',state.newDraft&&!state.session?'page':'false');
      for(const saved of state.sessionRows||[]){
        const row=state.session?.id===saved.id?{...saved,...state.session}:saved;
        const button=el('button',undefined,'development-session');button.type='button';button.dataset.sessionId=row.id;
        button.setAttribute('aria-current',state.session?.id===row.id?'page':'false');
        const title=row.title||'未命名会话',readonly=sessionReadonly(row),turn=row.turn;
        const progress=running(row)&&turn.cleanup==='complete'?'处理中':turn?(labels[turn.state]||turn.state):row.revision?'已有记录':'尚无消息';
        const stateText=(readonly?'只读历史 · ':'')+progress;
        button.append(el('span',title,'development-session-title'),el('span',stateText,'development-session-state'));button.title=title+' · '+stateText;
        button.hidden=!(title+' '+stateText).toLocaleLowerCase().includes(navigation.search.value.trim().toLocaleLowerCase());
        button.onclick=()=>{if(row.id!==state.session?.id||state.historyOnly)openSession(row.id);};sessionList.append(button);
      }
      const pageButton=(label,offset)=>{const button=el('button',label);button.type='button';button.disabled=!!state.listing;button.onclick=()=>{state.listOffset=offset;loadConversations(true).catch(e=>error(body,e));};sessionPages.append(button);};
      if(state.listOffset>0)pageButton('较新的会话',Math.max(0,state.listOffset-20));
      if(state.nextListOffset!==null&&state.nextListOffset!==undefined)pageButton('更早的会话',state.nextListOffset);
      sessionNote.textContent=!capabilities.enabled?ForgeDevelopmentComposer.connectionStatus(capabilities).label:capabilities.multiple_conversations!==true?'当前服务仅支持一个项目会话':!(state.sessionRows||[]).length?'还没有会话，从新会话开始。':'';
      if(navigation.search.value.trim()&&!sessionList.querySelector('button:not([hidden])'))sessionNote.textContent='当前页没有匹配的会话';
    }
    async function loadConversations(force=false){
      if(state.disposed||!capabilities.enabled)return;
      if(state.listing)return state.listing;
      if(!force&&Date.now()-state.listAt<5000)return;
      const offset=state.listOffset;
      state.listing=(async()=>{
        const result=await api('dev-sessions',{offset,limit:20});if(state.disposed||offset!==state.listOffset)return;
        state.sessionRows=(result.sessions||[]).slice(0,20);state.nextListOffset=result.next_offset;
        state.writerBusy=result.writer_busy===true;state.activeSession=result.active_session_id||null;state.listAt=Date.now();
      })();
      try{await state.listing;}finally{state.listing=null;if(!state.disposed){renderConversations();availability();}}
    }
    let turnOffset=0,versionOffset=0,listing=false;
    const stamp=value=>new Date(value*1000).toLocaleString('zh-CN',{month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit'});
    async function sessions(reset=false,offset=turnOffset,versionsAt=versionOffset){
      if(!capabilities.enabled||!state.session||listing||(!history.open&&trajectory.hidden))return;
      listing=true;
      try{
        const requested=reset?0:offset,requestedVersions=reset?0:versionsAt;
        const scoped=capabilities.multiple_conversations?{session_id:state.session.id}:{};
        const [result,versionResult]=await Promise.all([api('dev-project-turns',{...scoped,offset:requested,limit:20}),api('dev-project-turns',{...scoped,offset:requestedVersions,limit:20,kind:'timeline'})]);if(state.disposed)return;
        turnOffset=requested;versionOffset=requestedVersions;messages.replaceChildren(el('span','消息','development-rail-caption'));versions.replaceChildren(el('span','迭代记录','development-rail-caption'));
        const nav=(host,label,direction,next)=>{const b=el('button',direction,'development-rail-page');b.title=label;b.setAttribute('aria-label',label);b.onclick=()=>sessions(false,host===messages?next:turnOffset,host===versions?next:versionOffset).catch(e=>error(body,e));host.append(b);};
        if(requested>0)nav(messages,'较新的消息','↑',Math.max(0,requested-20));
        if(requestedVersions>0)nav(versions,'较新的迭代','↑',Math.max(0,requestedVersions-20));
        let count=0;
        for(const row of result.turns.filter(row=>!state.session||row.session===state.session.id)){
          if(row.operation==='message'){
            const b=el('button',undefined,'development-message-mark');b.setAttribute('aria-label','定位消息：'+row.title);b.title=row.title;
            b.append(el('span',row.title,'development-rail-preview'));b.onclick=()=>jump(row).catch(e=>error(body,e));messages.append(b);
          }
        }
        for(const row of versionResult.turns.filter(row=>!state.session||row.session===state.session.id)){
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
    const historyNav=el('div',undefined,'development-history-nav'),earlier=el('button','加载更早的记录'),latest=el('button','回到最新记录');earlier.hidden=true;latest.hidden=true;historyNav.append(earlier,latest);stage.append(historyNav);
    const timeline=el('div',undefined,'development-timeline');timeline.setAttribute('aria-label','开发对话记录');stage.append(timeline,hero);const flow=el('div',undefined,'development-flow');timeline.append(flow);const follow=new ScrollFollow(true,60);timeline.onscroll=()=>{follow.sample(scrollMetrics(timeline));newContent.hidden=follow.active;};
    const trajectory=el('section',undefined,'development-trajectory'),trajectoryLog=el('div',undefined,'development-trajectory-log');trajectory.hidden=true;trajectory.setAttribute('aria-label','对话轨迹');trajectory.append(versions,trajectoryLog);stage.append(trajectory);
    function selectTab(showTrajectory){trajectory.hidden=!showTrajectory;timeline.hidden=showTrajectory;hero.hidden=showTrajectory||!!state.items.size;layout.classList.toggle('development-empty',!state.items.size&&!showTrajectory);newContent.hidden=showTrajectory||follow.active;historyNav.hidden=showTrajectory;trajectoryToggle.textContent=showTrajectory?'返回对话':'查看轨迹';if(showTrajectory)sessions(true).catch(e=>error(body,e));}
    trajectoryToggle.onclick=()=>{selectTab(trajectory.hidden);more.open=false;};history.ontoggle=()=>{if(history.open)sessions(true).catch(e=>error(body,e));};
    const newContent=iconButton('有新内容，滚动到底部','M6 9l6 6 6-6');newContent.classList.add('development-scroll-bottom');newContent.hidden=true;stage.append(newContent);newContent.onclick=()=>{follow.toBottom(timeline,scrollMetrics(timeline),'instant');newContent.hidden=true;};
    state.observer=new ResizeObserver(()=>{if(state.disposed)return;sizeViewport();if(state.composer)stage.style.setProperty('--development-composer-height',state.composer.form.getBoundingClientRect().height+'px');if(follow.active)follow.toBottom(timeline,scrollMetrics(timeline),'instant');});state.observer.observe(flow);state.observer.observe(timeline);
    const pendingHost=el('div',undefined,'development-pending');stage.append(pendingHost);
    function addReference(){
      const dialog=el('dialog',undefined,'development-reference-dialog'),field=el('input'),add=el('button','添加引用'),close=el('button','取消');
      field.type='text';field.placeholder='文件或目录路径';field.setAttribute('aria-label','文件或目录路径');field.maxLength=4096;add.disabled=true;
      field.oninput=()=>{add.disabled=!field.value.trim();};field.onkeydown=e=>{if(e.key==='Enter'){e.preventDefault();add.click();}};
      close.onclick=()=>dialog.close();add.onclick=()=>{if(!field.value.trim())return;input.value+=(input.value?'\n':'')+'引用文件或目录：'+field.value.trim();input.dispatchEvent(new Event('input',{bubbles:true}));dialog.close();input.focus();};
      dialog.append(el('h2','引用文件或目录'),el('p','可以引用项目外的数据，也可以直接在对话中描述；无需预先配置。'),field,add,close);document.body.append(dialog);trackDialog(dialog);dialog.showModal();field.focus();
    }
    const composer=globalThis.ForgeDevelopmentComposer.create({onProviderChange:()=>availability(),onReference:addReference});
    state.composer=composer;
    const {form,input,send,stop,providers}=composer;
    composer.update({capabilities,session:null,readonly:capabilities.read_only===true,historyOnly:false,sending:true,stats:null});
    const busyNotice=el('div',undefined,'development-writer-notice'),busyText=el('span'),busyJump=el('button','查看正在运行的会话');busyNotice.hidden=true;
    busyJump.type='button';busyJump.onclick=()=>{if(state.activeSession)openSession(state.activeSession);};busyNotice.append(busyText,busyJump);
    stage.append(busyNotice,form,status);state.observer.observe(form);
    const sizeViewport=()=>{layout.style.setProperty('--development-top',Math.max(0,main.getBoundingClientRect().top)+'px');const narrow=matchMedia('(max-width:900px)').matches;if(narrow&&!narrowViewport)sidebarOpen(false);narrowViewport=narrow;};
    window.addEventListener('resize',sizeViewport);sizeViewport();state.releaseViewport=()=>window.removeEventListener('resize',sizeViewport);
    const menus=[more];
    const closeMenus=e=>{for(const menu of menus)if(menu.open&&!menu.contains(e.target))menu.open=false;};
    const escapeMenus=e=>{if(e.key!=='Escape')return;for(const menu of menus)if(menu.open){menu.open=false;menu.querySelector('summary').focus();e.stopPropagation();}};
    document.addEventListener('pointerdown',closeMenus);head.addEventListener('keydown',escapeMenus);
    state.releaseMenus=()=>{document.removeEventListener('pointerdown',closeMenus);head.removeEventListener('keydown',escapeMenus);};
    const chat=globalThis.ForgeDevelopmentChat.create({flow,onRaw:output,formatDevelopment:developmentLabel,
      onChange:()=>{if(state.disposed||!state.chat)return;hero.hidden=!!state.chat.items.size||!trajectory.hidden;layout.classList.toggle('development-empty',!state.chat.items.size&&trajectory.hidden);},
      onTrim:()=>{earlier.hidden=false;}});
    state.chat=chat;state.items=chat.items;
    let draftKey=workspaceId+':new';input.value='';
    const resizeInput=()=>composer.resize();
    input.oninput=()=>{resizeInput();if(bytes(input.value)>32768){input.setCustomValidity('要求不能超过 32 KiB');return;}input.setCustomValidity('');if(!drafts.has(draftKey)&&drafts.size>=64){input.setCustomValidity('已保存 64 份草稿，请先清空不再需要的草稿。');return;}if(input.value){drafts.set(draftKey,input.value);draftRevisions.set(draftKey,++draftSerial);}else{drafts.delete(draftKey);draftRevisions.delete(draftKey);}saveDrafts();availability();};
    let composing=false,composingUntil=0;
    input.addEventListener('compositionstart',()=>{composing=true;});input.addEventListener('compositionend',()=>{composing=false;composingUntil=Date.now()+10;});
    input.onkeydown=e=>{if(composing||Date.now()<composingUntil||e.isComposing||e.keyCode===229)return;if(e.key==='Enter'&&!e.shiftKey&&!e.altKey){e.preventDefault();if(!e.repeat)form.requestSubmit();}if(e.key==='Escape'&&!stop.disabled)stop.click();};
    function availability(){
      const t=state.session?.turn,busy=t&&(t.cleanup!=='complete'||t.development?.busy===true);
      const provider=(capabilities.providers||[]).find(p=>p.id===(state.session?.provider||providers.value));
      const unsupportedNew=state.newDraft&&capabilities.multiple_conversations!==true;
      const readonly=capabilities.read_only===true||sessionReadonly(state.session)||unsupportedNew;
      const otherBusy=state.writerBusy&&(!state.activeSession||state.activeSession!==state.session?.id);
      composer.update({capabilities:otherBusy?{...capabilities,start:false}:capabilities,session:state.session,readonly,historyOnly:!!state.historyOnly,sending:!!state.sending||state.initializing,creation:!!state.creation,stats:state.displaySession&&state.displaySession!==state.session?.id?null:state.stats});
      archive.disabled=readonly||otherBusy||!capabilities.start||!provider?.available||!!busy||!!state.sending||!!state.historyOnly||!state.session?.revision;
      basis.disabled=!t;recover.disabled=readonly||!capabilities.enabled||!state.session;

      hero.hidden=!!state.items.size||!trajectory.hidden;layout.classList.toggle('development-empty',!state.items.size&&trajectory.hidden);
      busyNotice.hidden=!otherBusy||readonly||!!state.historyOnly;busyText.textContent='另一个会话正在使用此项目，可以继续编辑草稿，完成后再发送。';busyJump.hidden=!state.activeSession;
      status.textContent=unsupportedNew?'当前开发服务不支持新建独立会话，请从左侧打开已有会话。':readonly||state.historyOnly?'历史会话 · 只读，不能继续发送':t?(t.operation==='compact'?(t.cleanup!=='complete'?'正在归档历史…':t.state==='completed'?'归档处理完成':'归档未完成'):(t.development?developmentLabel(t.development):(labels[t.state]||t.state)))+(t.dispatch==='unknown'?' · 发送结果未知，请先核对':''):
        readonly?'已有开发历史 · 只读':provider?.available?'':ForgeDevelopmentComposer.connectionStatus(capabilities).label;
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
    let trajectoryBytes=0;
    function project(event){
      if(event.type==='compaction.completed'&&event.data.status==='compacted'&&!state.historyOnly){clearTimeline();earlier.hidden=false;}
      chat.project(event);
      const raw=JSON.stringify(event.data,null,2),size=bytes(raw);
      if(size<=65536){
        while(trajectoryLog.childElementCount>=100||trajectoryBytes+size>1024*1024){const first=trajectoryLog.firstElementChild;if(!first)break;trajectoryBytes-=Number(first.dataset.bytes||0);first.remove();}
        const row=el('details',undefined,'development-trajectory-record'),summary=el('summary');row.dataset.bytes=String(size);trajectoryBytes+=size;
        summary.append(el('span',event.type),el('time',stamp(event.observed_at)));row.append(summary);
        row.ontoggle=()=>{if(row.open&&!row.querySelector('pre'))row.append(el('pre',raw));};trajectoryLog.append(row);
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
      if(state.disposed)return Promise.resolve();
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
          await loadConversations();if(state.disposed)return;
          if(!state.session){availability();return;}
          const current=await api('dev-session',{session_id:state.session.id});if(state.disposed)return;state.session=current;chat.updateSession(current);renderConversations();availability();const revisionKey=current.revision+':'+current.turn?.cleanup;if(state.railRevision!==revisionKey){state.railRevision=revisionKey;await sessions(true);}
          if(capabilities.enabled&&!capabilities.read_only&&!sessionReadonly(current)&&!state.historyOnly)for(const request of current.pending||[])pending(request);
          for(const node of pendingHost.children)if(state.historyOnly||capabilities.read_only||sessionReadonly(current)||!(current.pending||[]).some(p=>p.id===node.dataset.request))node.remove();
          if(!state.historyOnly){
            const busy=current.turn&&(current.turn.cleanup!=='complete'||current.turn.development?.busy===true);
            const epoch=state.feedEpoch;const page=await api('dev-events',{session_id:current.id,after_sequence:state.sequence,limit:50,wait_ms:busy?1500:0});if(state.disposed||epoch!==state.feedEpoch)return;
            for(const event of page.events)project(event);state.sequence=page.next_sequence;chat.updateSession(current);
            delay=state.sequence<page.last_sequence||busy?25:1500;
          }
          if(capabilities.conversation_stats&&Date.now()-state.statsAt>=3000){state.statsAt=Date.now();try{const stats=await api('dev-stats',{session_id:current.id});if(!state.disposed&&state.session?.id===stats.session_id){state.stats=stats;availability();}}catch{}}
          state.connectionError?.remove();state.connectionError=null;
        }catch(e){if(!state.disposed&&e.name!=='AbortError'){state.connectionError?.remove();state.connectionError=error(body,e);status.textContent='连接中断，正在重新连接…';delay=2500;}}
        finally{state.refreshing=null;if(!state.disposed)state.timer=setTimeout(refresh,delay);}
      })();
      return state.refreshing;
    }
    form.onsubmit=async e=>{e.preventDefault();if(send.disabled||!input.reportValidity()||!input.value.trim())return;send.disabled=true;
      const message=input.value,sentDraftRevision=draftRevisions.get(draftKey)||0,submission=state.submission||{text:message,id:uid()};if(submission.text!==message){error(body,new Error('上次发送结果未确认，请先核对记录后再编辑要求。'));return;}state.submission=submission;
      state.sending=true;availability();
      try{if(!state.session){state.creation||={request_id:uid(),provider_id:providers.value,...(state.newDraft?{new_conversation:true}:{})};state.session=await api('dev-session-create',{},state.creation);state.createdHere=true;}if(state.disposed)return;await api('dev-turn-start',{}, {session_id:state.session.id,submission_id:submission.id,expected_revision:state.session.revision,text:message});state.submission=null;if(drafts.get(draftKey)===message&&(draftRevisions.get(draftKey)||0)===sentDraftRevision){drafts.delete(draftKey);draftRevisions.delete(draftKey);saveDrafts();}if(state.disposed)return;input.value='';resizeInput();if(state.createdHere){await openSession(state.session.id);return;}state.listAt=0;clearTimeout(state.timer);await refresh();await sessions(true);}
      catch(err){if(!state.disposed){error(body,err);await refresh().catch(()=>{});await sessions(true).catch(()=>{});}}
      finally{state.sending=false;if(!state.disposed)availability();}
    };
    archive.onclick=async()=>{
      if(archive.disabled)return;more.open=false;
      state.sending=true;availability();
      state.archiveSubmission||=uid();
      try{await api('dev-compact',{}, {session_id:state.session.id,submission_id:state.archiveSubmission,expected_revision:state.session.revision});state.archiveSubmission=null;await refresh();}
      catch(e){error(body,e);await refresh().catch(()=>{});}
      finally{state.sending=false;if(!state.disposed)availability();}
    };
    stop.onclick=async()=>{stop.disabled=true;try{await api('dev-cancel',{}, {session_id:state.session.id,turn_id:state.session.turn.id,request_id:uid()});status.textContent='已请求停止，等待所属工具清理。';}catch(e){error(body,e);stop.disabled=false;}};
    recover.onclick=async()=>{try{const value=await api('dev-recover',{}, {session_id:state.session.id,expected_revision:state.session.revision});status.textContent=value.recovery.can_continue?'连接状态已核对，可继续。':'清理归属尚未确认，保留写租约。';}catch(e){error(body,e);}};
    basis.onclick=async()=>{try{const value=await api('dev-context',{session_id:state.session.id,turn_id:state.session.turn.id});if(state.disposed)return;const d=el('dialog'),close=el('button','关闭');close.onclick=()=>d.close();d.append(el('h2','本轮开发依据'),close,el('pre',JSON.stringify(value.context,null,2)));document.body.append(d);trackDialog(d);d.showModal();}catch(e){error(body,e);}};
    function clearTimeline(){state.feedEpoch=(state.feedEpoch||0)+1;chat.reset();trajectoryLog.replaceChildren();trajectoryBytes=0;newContent.hidden=true;}
    async function loadWindow(sid,after){
      state.historyOnly=true;state.displaySession=sid;pendingHost.replaceChildren();state.pending.clear();follow.setFollowing(false);clearTimeline();
      const epoch=state.feedEpoch;state.windowStart=after;earlier.hidden=after===0;latest.hidden=false;availability();
      const page=await api('dev-events',{session_id:sid,after_sequence:after,limit:50});
      if(state.disposed||state.feedEpoch!==epoch)return;
      for(const event of page.events)project(event);selectTab(false);timeline.scrollTop=0;
    }
    async function jump(row){more.open=false;history.open=false;await loadWindow(row.session,Math.max(0,(row.sequence||1)-1));}
    earlier.onclick=()=>loadWindow(state.displaySession||state.session.id,Math.max(0,(state.windowStart||0)-50)).catch(e=>error(body,e));
    latest.onclick=async()=>{follow.setFollowing(true);clearTimeline();state.historyOnly=false;state.displaySession=null;state.sequence=Math.max(0,state.session.sequence-50,(state.session.archive_sequence||0)-1);state.windowStart=state.sequence;earlier.hidden=state.sequence===0;latest.hidden=true;clearTimeout(state.timer);await refresh();};
    if(capabilities.enabled){
      const primary=(await api('dev-conversation')).session;if(state.disposed)return handle;state.primaryId=primary?.id||null;
      if(view?.session_id)state.session=await api('dev-session',{session_id:view.session_id});
      else if(!state.newDraft)state.session=primary;
      if(state.disposed)return handle;
      if(state.session){
        draftKey=workspaceId+':'+state.session.id;
        providers.value=state.session.provider;state.sequence=Math.max(0,state.session.sequence-50,(state.session.archive_sequence||0)-1);state.windowStart=state.sequence;earlier.hidden=state.sequence===0;
      }
      const legacy=workspaceId+':project',defaultDraft=state.session?.id===state.primaryId&&!!state.session||!state.primaryId&&!state.newDraft&&!view?.session_id;
      if(defaultDraft&&drafts.has(legacy)){
        if(!drafts.has(draftKey))drafts.set(draftKey,drafts.get(legacy));
        if(drafts.get(draftKey)===drafts.get(legacy)){drafts.delete(legacy);draftRevisions.delete(legacy);}
        saveDrafts();
      }
      input.value=drafts.get(draftKey)||'';await refresh();
    }
    if(state.disposed)return handle;state.initializing=false;renderConversations();availability();resizeInput();
    return handle;
  }
  globalThis.ForgeDevelopment=Object.freeze({mount,activeCount:()=>active.size});
})();
