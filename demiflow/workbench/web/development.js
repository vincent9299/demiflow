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
  const drafts=new Map(),draftRevisions=new Map(),active=new Set();let draftSerial=0,draftTimer=null;
  try { const saved=sessionStorage.getItem('forge-development-drafts'); if(saved&&bytes(saved)<=2*1024*1024){const entries=JSON.parse(saved);if(Array.isArray(entries)&&entries.length<=64)for(const pair of entries)if(Array.isArray(pair)&&pair.length===2&&typeof pair[0]==='string'&&typeof pair[1]==='string'&&bytes(pair[1])<=32768)drafts.set(pair[0],pair[1]);}}catch{}
  const flushDrafts=()=>{if(draftTimer){clearTimeout(draftTimer);draftTimer=null;}try{sessionStorage.setItem('forge-development-drafts',JSON.stringify([...drafts]));}catch{}};
  const saveDrafts=(immediate=false)=>{if(immediate){flushDrafts();return;}if(draftTimer)clearTimeout(draftTimer);draftTimer=setTimeout(flushDrafts,250);};
  window.addEventListener('pagehide',flushDrafts);
  const labels={preparing:'准备开发依据',starting:'正在连接',running:'正在开发',waiting_input:'等待回答',waiting_approval:'等待许可',stopping:'已请求停止，正在清理',completed:'本轮已完成',failed:'本轮失败',interrupted:'连接中断',cancelled:'已停止'};
  const uid=()=>crypto.randomUUID();
  const iconButton=(label,path,viewBox='0 0 24 24',filled=false)=>{const b=el('button',undefined,'development-icon');b.type='button';b.title=label;b.setAttribute('aria-label',label);const svg=document.createElementNS('http://www.w3.org/2000/svg','svg');svg.setAttribute('viewBox',viewBox);svg.setAttribute('aria-hidden','true');if(filled)svg.classList.add('development-filled-icon');const p=document.createElementNS(svg.namespaceURI,typeof path==='string'?'path':'rect');for(const [key,value]of Object.entries(typeof path==='string'?{d:path}:path))p.setAttribute(key,value);svg.append(p);b.append(svg);return b;};
  async function mount({main,api,workspaceId,view,disposeReaders,openSession,openNew,error,register}) {
    const state={disposed:false,navigationEpoch:0,initializing:true,timer:null,session:null,sequence:0,items:new Map(),pending:new Set(),submission:null,dialogs:new Set(),stats:null,statsAt:0,newDraft:view?.kind==='development_new',listOffset:0,listAt:0,writerBusy:false,activeSession:null};
    const layout=el('section',undefined,'development-layout'),body=el('div',undefined,'development-conversation');
    layout.append(body);main.append(layout);main.classList.add('development-main');active.add(state);
    const handle={workspaceId,get sessionId(){return state.session?.id;},navigate:async nextView=>{if(state.disposed)return;await switchView(nextView);},dispose(){if(state.disposed)return;flushDrafts();state.disposed=true;clearTimeout(state.timer);state.observer?.disconnect();state.releaseViewport?.();state.navigationResize?.dispose();state.rail?.dispose();state.files?.dispose();state.composer?.dispose();state.chat?.dispose();active.delete(state);main.classList.remove('development-main');disposeReaders(layout);for(const dialog of state.dialogs)dialog.remove();state.dialogs.clear();}};register(handle);
    const sidebar=el('aside',undefined,'development-session-sidebar'),sideHeading=el('div',undefined,'development-session-heading');
    const createNew=el('button',undefined,'development-new-conversation');createNew.type='button';createNew.append(el('span','＋'),el('span','新会话'));createNew.disabled=true;
    let sidebarTab='sessions';
    const navigation=ForgeNavigation.header({title:'会话',searchLabel:'搜索会话',placeholder:'筛选会话或文件',onSearch:()=>{if(sidebarTab==='files')state.files?.setFilter(navigation.search.value);else if(!state.initializing)renderConversations();}});
    const sideTabs=el('div',undefined,'dev-sidebar-tabs');
    const sessionsTabBtn=el('button','会话','dev-sidebar-tab is-active');sessionsTabBtn.type='button';sessionsTabBtn.setAttribute('aria-pressed','true');
    const filesTabBtn=el('button',undefined,'dev-sidebar-tab');filesTabBtn.type='button';filesTabBtn.setAttribute('aria-pressed','false');
    const filesBadge=el('span','0','dev-change-badge');filesBadge.hidden=true;
    filesTabBtn.append(el('span','文件'),filesBadge);
    sideTabs.append(sessionsTabBtn,filesTabBtn);
    const sessionsPane=el('div',undefined,'dev-sidebar-pane'),filesPane=el('div',undefined,'dev-sidebar-pane');filesPane.hidden=true;
    sideHeading.append(createNew);
    const sessionList=el('nav',undefined,'development-session-list');sessionList.setAttribute('aria-label','项目会话');
    const sessionPages=el('div',undefined,'development-session-pages'),sessionNote=el('p','正在读取会话…','development-session-note');
    sessionsPane.append(sideHeading,sessionList,sessionPages,sessionNote);
    sidebar.append(navigation.element,sideTabs,sessionsPane,filesPane);layout.prepend(sidebar);
    state.navigationResize=ForgeNavigation.resizable({layout,sidebar,label:'会话目录',active:false});
    const backdrop=el('button',undefined,'development-sidebar-backdrop');backdrop.type='button';backdrop.setAttribute('aria-label','关闭会话列表');layout.append(backdrop);
    const head=el('div',undefined,'development-controls'),sideToggle=iconButton('会话列表','M3 4h18v16H3zM9 4v16');
    function sidebarOpen(value){layout.dataset.sidebar=value?'open':'closed';sideToggle.setAttribute('aria-expanded',String(value));sidebar.hidden=!value;backdrop.hidden=!value;state.navigationResize.setActive(value);if(value)navigation.actions.append(sideToggle);else head.prepend(sideToggle);}
    function setSidebarTab(next){
      sidebarTab=next==='files'?'files':'sessions';
      const isFiles=sidebarTab==='files';
      sessionsPane.hidden=isFiles;filesPane.hidden=!isFiles;
      sessionsTabBtn.classList.toggle('is-active',!isFiles);filesTabBtn.classList.toggle('is-active',isFiles);
      sessionsTabBtn.setAttribute('aria-pressed',String(!isFiles));filesTabBtn.setAttribute('aria-pressed',String(isFiles));
      if(isFiles){state.files?.setFilter(navigation.search.value);state.files?.ensureTreeLoaded();}
      else if(!state.initializing)renderConversations();
    }
    sessionsTabBtn.onclick=()=>setSidebarTab('sessions');
    filesTabBtn.onclick=()=>setSidebarTab('files');
    sideToggle.onclick=()=>{sidebarOpen(layout.dataset.sidebar!=='open');sideToggle.focus();};
    const closeSidebar=()=>{sidebarOpen(false);sideToggle.focus();};backdrop.onclick=closeSidebar;
    sidebar.onkeydown=event=>{if(event.key==='Escape'&&!event.defaultPrevented){event.preventDefault();closeSidebar();}};
    let narrowViewport=matchMedia('(max-width:900px)').matches;sidebarOpen(!narrowViewport);
    createNew.onclick=()=>{if(!createNew.disabled&&typeof openNew==='function')openNew();};
    const filesToggle=iconButton('打开代码面板','M3 4h18v16H3zM15 4v16');
    head.append(filesToggle);body.append(head);
    const insertReference=path=>{
      if(!path||!state.composer)return;
      const box=state.composer.input;
      const line='引用文件：'+path;
      if(!box.value.includes(line)){
        box.value+=(box.value?'\n':'')+line;
        box.dispatchEvent(new Event('input',{bubbles:true}));
      }
      box.focus();
    };
    state.files=DevelopmentNavigation.filePanel({layout,filesHost:filesPane,toggle:filesToggle,api,iconButton,
      onReference:insertReference,
      onShowFilesTab:()=>{sidebarOpen(true);setSidebarTab('files');},
      onBadgeChange:count=>{filesBadge.textContent=String(count);filesBadge.hidden=!count;}});
    const status=el('p','正在读取开发能力…','development-status');status.setAttribute('role','status');status.setAttribute('aria-live','polite');
    let capabilities=await api('dev-capabilities');state.capabilitiesAt=Date.now();if(state.disposed||!layout.isConnected)return handle;
    const running=value=>!!(value?.turn&&value.turn.cleanup!=='complete');
    const sessionReadonly=value=>!!value&&(value.writable===false||value.writable!==true&&(capabilities.multiple_conversations===true||!state.createdHere&&value.id!==state.primaryId));
    function renderConversations(){
      const term=navigation.search.value.trim().toLocaleLowerCase();
      const fingerprint=JSON.stringify([capabilities.multiple_conversations,capabilities.enabled,capabilities.read_only,state.newDraft,state.session?.id,state.session?.title,state.session?.revision,state.session?.turn?.id,state.session?.turn?.state,state.session?.turn?.cleanup,state.historyOnly,state.listOffset,state.nextListOffset,!!state.listing,term,(state.sessionRows||[]).map(s=>[s.id,s.title,s.revision,s.writable,s.turn?.id,s.turn?.state,s.turn?.cleanup])]);
      if(fingerprint===state.conversationsFingerprint)return;
      state.conversationsFingerprint=fingerprint;
      sessionList.replaceChildren();sessionPages.replaceChildren();
      createNew.disabled=capabilities.multiple_conversations!==true||!capabilities.enabled||capabilities.read_only===true||typeof openNew!=='function';
      createNew.title=capabilities.multiple_conversations===true?'新建一个空白会话':'当前开发服务仅支持一个项目会话';
      createNew.setAttribute('aria-current',state.newDraft&&!state.session?'page':'false');
      for(const saved of state.sessionRows||[]){
        const row=state.session?.id===saved.id?{...saved,...state.session}:saved;
        const button=el('button',undefined,'development-session');button.type='button';button.dataset.sessionId=row.id;
        button.setAttribute('aria-current',state.session?.id===row.id?'page':'false');
        const title=row.title||'未命名会话',turn=row.turn;
        button.title=title;button.append(el('span',title,'development-session-title'));
        const activeState=running(row)&&['preparing','starting','running','waiting_input','waiting_approval','stopping'].includes(turn.state)
          ?labels[turn.state]:'';
        if(activeState)button.append(el('span',activeState,'development-session-state'));
        button.hidden=!(title+' '+activeState).toLocaleLowerCase().includes(term);
        button.onclick=()=>{if(row.id!==state.session?.id||state.historyOnly)openSession(row.id);};sessionList.append(button);
      }
      const pageButton=(label,offset)=>{const button=el('button',label);button.type='button';button.disabled=!!state.listing;button.onclick=()=>{state.listOffset=offset;loadConversations(true).catch(e=>error(body,e));};sessionPages.append(button);};
      if(state.listOffset>0)pageButton('较新的会话',Math.max(0,state.listOffset-20));
      if(state.nextListOffset!==null&&state.nextListOffset!==undefined)pageButton('更早的会话',state.nextListOffset);
      sessionNote.textContent=!capabilities.enabled?ForgeDevelopmentComposer.connectionStatus(capabilities).label:capabilities.multiple_conversations!==true?'当前服务仅支持一个项目会话':!(state.sessionRows||[]).length?'还没有会话，从新会话开始。':'';
      if(term&&!sessionList.querySelector('button:not([hidden])'))sessionNote.textContent='当前页没有匹配的会话';
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
    const stage=el('div',undefined,'development-stage'),hero=el('div',undefined,'development-hero');hero.append(el('h2','今天想做些什么？'));body.append(stage);
    const historyNav=el('div',undefined,'development-history-nav'),earlier=el('button','加载更早的记录'),latest=el('button','回到最新记录');earlier.hidden=true;latest.hidden=true;historyNav.append(earlier,latest);stage.append(historyNav);
    const scrollArea=el('div',undefined,'development-scroll-area');
    const timeline=el('div',undefined,'development-timeline');
    timeline.setAttribute('aria-label','开发对话记录');timeline.tabIndex=0;
    const flow=el('div',undefined,'development-flow');
    timeline.append(flow);scrollArea.append(timeline);stage.append(scrollArea,hero);
    const follow=new ScrollFollow(true,60);
    const rail=state.rail=DevelopmentNavigation.turnRail({host:scrollArea,timeline,flow,navigate:row=>{
      const turn=[...flow.children].find(section=>section.dataset.turnId===row.id);
      if(turn && turn.querySelector('.dfc-user')) {
        state.navigationEpoch++;
        follow.jump(timeline,scrollMetrics(timeline),timeline.scrollTop+turn.getBoundingClientRect().top-timeline.getBoundingClientRect().top-8);
        rail.update();
      } else loadWindow(row.session,row.sequence-1).catch(e=>error(body,e));
    }});
    head.insertBefore(rail.picker,filesToggle);
    timeline.onscroll=()=>{follow.sample(scrollMetrics(timeline));newContent.hidden=follow.active;rail.update();};
    const newContent=iconButton('有新内容，滚动到底部','M6 9l6 6 6-6');newContent.classList.add('development-scroll-bottom');newContent.hidden=true;stage.append(newContent);newContent.onclick=()=>{follow.toBottom(timeline,scrollMetrics(timeline),'instant');newContent.hidden=true;};
    let lastTop=-1,lastComposerHeight=-1;
    const sizeViewport=()=>{const top=Math.round(Math.max(0,main.getBoundingClientRect().top));if(top!==lastTop){lastTop=top;layout.style.setProperty('--development-top',top+'px');}const narrow=matchMedia('(max-width:900px)').matches;if(narrow&&!narrowViewport)sidebarOpen(false);narrowViewport=narrow;};
    state.observer=new ResizeObserver(entries=>{if(state.disposed)return;let flowChanged=false,timelineChanged=false,composerChanged=false;for(const entry of entries){if(entry.target===flow)flowChanged=true;else if(entry.target===timeline)timelineChanged=true;else if(state.composer&&entry.target===state.composer.form)composerChanged=true;}if(composerChanged&&state.composer){const h=Math.round(state.composer.form.getBoundingClientRect().height);if(h!==lastComposerHeight){lastComposerHeight=h;stage.style.setProperty('--development-composer-height',h+'px');}}if(timelineChanged)sizeViewport();if(flowChanged||timelineChanged){rail.update();if(follow.active)follow.toBottom(timeline,scrollMetrics(timeline),'instant');}});state.observer.observe(flow);state.observer.observe(timeline);
    const pendingHost=el('div',undefined,'development-pending');stage.append(pendingHost);
    function addReference(){
      const dialog=el('dialog',undefined,'development-reference-dialog'),field=el('input'),add=el('button','添加引用'),close=el('button','取消');
      field.type='text';field.placeholder='文件或目录路径';field.setAttribute('aria-label','文件或目录路径');field.maxLength=4096;add.disabled=true;
      field.oninput=()=>{add.disabled=!field.value.trim();};field.onkeydown=e=>{if(e.key==='Enter'){e.preventDefault();add.click();}};
      close.onclick=()=>dialog.close();add.onclick=()=>{if(!field.value.trim())return;input.value+=(input.value?'\n':'')+'引用文件或目录：'+field.value.trim();input.dispatchEvent(new Event('input',{bubbles:true}));dialog.close();input.focus();};
      dialog.append(el('h2','引用文件或目录'),el('p','可以引用项目外的数据，也可以直接在对话中描述；无需预先配置。'),field,add,close);document.body.append(dialog);trackDialog(dialog);dialog.showModal();field.focus();
    }
    const composer=globalThis.ForgeDevelopmentComposer.create({onProviderChange:()=>availability(),onReference:addReference,
      async onQueueAction(submission_id,action){
        try {
          if(action==='steer'){
            const message=state.session.queued_messages.find(row=>row.submission===submission_id);
            if(message)await steerQueued([message]);
          }else await api('dev-queue-change',{}, {session_id:state.session.id,submission_id,action});
          await refresh();
        } catch(err){if(!state.disposed)error(body,err);}
      }});
    state.composer=composer;
    const {form,input,send,stop,providers}=composer;
    composer.update({capabilities,session:null,readonly:capabilities.read_only===true,historyOnly:false,sending:true,stats:null});
    const busyNotice=el('div',undefined,'development-writer-notice'),busyText=el('span'),busyJump=el('button','查看正在运行的会话');busyNotice.hidden=true;
    busyJump.type='button';busyJump.onclick=()=>{if(state.activeSession)openSession(state.activeSession);};busyNotice.append(busyText,busyJump);
    stage.append(busyNotice,form,status);state.observer.observe(form);
    window.addEventListener('resize',sizeViewport);sizeViewport();state.releaseViewport=()=>window.removeEventListener('resize',sizeViewport);
    const chat=globalThis.ForgeDevelopmentChat.create({flow,onRaw:output,
      onOpenFile:(path,mode)=>state.files?.openFile(path,mode),
      onChange:()=>{if(state.disposed||!state.chat)return;rail.update();hero.hidden=!!state.chat.items.size;layout.classList.toggle('development-empty',!state.chat.items.size);},
      onTrim:()=>{earlier.hidden=false;}});
    state.chat=chat;state.items=chat.items;
    let draftKey=workspaceId+':new';input.value='';
    const resizeInput=()=>composer.resize();
    input.oninput=()=>{resizeInput();if(bytes(input.value)>32768){input.setCustomValidity('要求不能超过 32 KiB');return;}input.setCustomValidity('');if(!drafts.has(draftKey)&&drafts.size>=64){input.setCustomValidity('已保存 64 份草稿，请先清空不再需要的草稿。');return;}if(input.value){drafts.set(draftKey,input.value);draftRevisions.set(draftKey,++draftSerial);}else{drafts.delete(draftKey);draftRevisions.delete(draftKey);}saveDrafts();availability();};
    async function steerQueued(messages){
      const session_id=state.session.id,turn_id=state.session.turn.id;
      for(const item of messages){
        await api('dev-steer',{}, {session_id,turn_id,submission_id:item.submission,text:item.text,from_queue:true});
      }
    }
    let composing=false,composingUntil=0,steerGesture=false;
    input.addEventListener('compositionstart',()=>{composing=true;});input.addEventListener('compositionend',()=>{composing=false;composingUntil=Date.now()+10;});
    input.onkeydown=e=>{if(composing||Date.now()<composingUntil||e.isComposing||e.keyCode===229)return;if(e.key==='Enter'&&!e.shiftKey&&!e.altKey){e.preventDefault();if(!e.repeat){
        steerGesture=(e.metaKey||e.ctrlKey)&&running(state.session)&&capabilities.message_steering===true;
        try{
          if(steerGesture&&!input.value.trim()&&state.session.queued_messages?.length&&!input.disabled){
            steerQueued(state.session.queued_messages).catch(err=>{if(!state.disposed)error(body,err);}).finally(refresh);
          }else form.requestSubmit();
        }finally{steerGesture=false;}
      }}if(e.key==='Escape'&&!stop.disabled)stop.click();};
    function availability(){
      const t=state.session?.turn;
      const provider=(capabilities.providers||[]).find(p=>p.id===(state.session?.provider||providers.value))||(capabilities.providers||[]).find(p=>p.available);
      const unsupportedNew=state.newDraft&&capabilities.multiple_conversations!==true;
      const readonly=capabilities.read_only===true||sessionReadonly(state.session)||unsupportedNew;
      const otherBusy=state.writerBusy&&(!state.activeSession||state.activeSession!==state.session?.id);
      composer.update({capabilities:otherBusy?{...capabilities,start:false}:capabilities,session:state.session,readonly,historyOnly:!!state.historyOnly,sending:!!state.sending||state.initializing,creation:!!state.creation,stats:state.displaySession&&state.displaySession!==state.session?.id?null:state.stats});
      hero.hidden=!!state.items.size;layout.classList.toggle('development-empty',!state.items.size);
      busyNotice.hidden=!otherBusy||readonly||!!state.historyOnly;busyText.textContent='另一个会话正在使用此项目，可以继续编辑草稿，完成后再发送。';busyJump.hidden=!state.activeSession;
      status.dataset.turnState=t?.state||'';status.dataset.turnCleanup=t?.cleanup||'';
      const terminalClean=t&&t.cleanup==='complete'&&t.state==='completed'&&t.operation!=='compact'&&t.dispatch!=='unknown';
      status.textContent=unsupportedNew?'当前开发服务不支持新建独立会话，请从左侧打开已有会话。':state.historyOnly?'正在查看更早的历史记录，回到最新记录后可继续提问':readonly?'当前服务为只读模式':t?(terminalClean?'':(t.operation==='compact'?(t.cleanup!=='complete'?'正在归档历史…':t.state==='completed'?'归档处理完成':'归档未完成'):(labels[t.state]||t.state))+(t.dispatch==='unknown'?' · 发送结果未知，请先核对':'')):
        provider?.available?'':ForgeDevelopmentComposer.connectionStatus(capabilities).label;
    }
    let prewarmKey='';
    function requestPrewarm(){
      if(state.disposed||!capabilities.enabled||capabilities.read_only||capabilities.prewarm_api!==true)return;
      const sid=state.session?.id||null;
      if(state.session&&sessionReadonly(state.session))return;
      if(state.session?.turn&&state.session.turn.cleanup!=='complete')return;
      const providerId=state.session?.provider||providers.value||'';
      if(!providerId)return;
      const key=(sid||'new')+':'+providerId;
      if(key===prewarmKey)return;
      prewarmKey=key;
      api('dev-prewarm',{},{...(sid?{session_id:sid}:{}),provider_id:providerId}).catch(()=>{if(prewarmKey===key)prewarmKey='';});
    }
    providers.onchange=()=>{availability();requestPrewarm();};
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
    function project(event, {replaying=false}={}){
      if(event.type==='compaction.completed'&&event.data.status==='compacted'&&!state.historyOnly){clearTimeline();earlier.hidden=false;}
      chat.project(event);
      if(event.type==='change.observed'){state.files?.recordObservedChanges(event.data?.files||[]);}
      else if(event.type==='turn.state'){
        try{
          const result=(typeof event.data?.result==='string'?JSON.parse(event.data.result):event.data?.result)||{};
          if(result.pipeline_checks||result.pipeline_review){
            state.files?.recordChecks({native_flow:result.pipeline_checks?.native_flow,project_layout:result.pipeline_checks?.project_layout,review:result.pipeline_review});
          }
        }catch{}
        if(!replaying&&event.data?.cleanup==='complete')state.files?.refreshChanges();
      }else if(event.type==='review.completed'){
        state.files?.recordChecks({review:event.data?.status});
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
    function applySession(current){
      state.session=current;chat.updateSession(current);renderConversations();availability();
      const revisionKey=current.revision+':'+current.turn?.cleanup;
      if(state.railRevision!==revisionKey){state.railRevision=revisionKey;rail.load(api,current.id,capabilities.multiple_conversations).catch(()=>{state.railRevision=null;});}
      if(capabilities.enabled&&!capabilities.read_only&&!sessionReadonly(current)&&!state.historyOnly)for(const request of current.pending||[])pending(request);
      for(const node of pendingHost.children)if(state.historyOnly||capabilities.read_only||sessionReadonly(current)||!(current.pending||[]).some(p=>p.id===node.dataset.request))node.remove();
    }
    function refresh(){
      if(state.disposed||(state.sending&&state.createdHere))return Promise.resolve();
      if(state.refreshing)return state.refreshing;
      clearTimeout(state.timer);
      const startEpoch=state.feedEpoch||0;
      state.refreshing=(async()=>{
        let delay=1500;
        try{
          if(capabilities.backend==='existing_http'&&(!state.capabilitiesAt||Date.now()-state.capabilitiesAt>=5000)){
            const wasUnavailable=!capabilities.enabled||capabilities.read_only;
            capabilities=await api('dev-capabilities');state.capabilitiesAt=Date.now();
            if(wasUnavailable||!capabilities.enabled||capabilities.read_only){pendingHost.replaceChildren();state.pending.clear();}
            availability();
          }
          await loadConversations();if(state.disposed||(state.feedEpoch||0)!==startEpoch)return;
          if(!state.session){availability();return;}
          let current=state.session;
          if(!state.historyOnly&&capabilities.events_include_session===true){
            const busy=current.turn&&current.turn.cleanup!=='complete';
            const epoch=state.feedEpoch;
            const page=await api('dev-events',{session_id:current.id,after_sequence:state.sequence,limit:50,wait_ms:busy?1500:0,include_session:1});
            if(state.disposed||epoch!==state.feedEpoch)return;
            if(page.session){current=page.session;applySession(current);}
            for(const event of page.events)project(event);
            state.sequence=page.next_sequence;chat.updateSession(current);
            const stillBusy=current.turn&&current.turn.cleanup!=='complete';
            delay=page.events.length||state.sequence<page.last_sequence?0:stillBusy?25:1500;
          }else{
            current=await api('dev-session',{session_id:state.session.id});if(state.disposed||(state.feedEpoch||0)!==startEpoch)return;
            applySession(current);
            if(!state.historyOnly){
              const busy=current.turn&&current.turn.cleanup!=='complete';
              const epoch=state.feedEpoch;const page=await api('dev-events',{session_id:current.id,after_sequence:state.sequence,limit:50,wait_ms:busy?1500:0});if(state.disposed||epoch!==state.feedEpoch)return;
              for(const event of page.events)project(event);state.sequence=page.next_sequence;chat.updateSession(current);
              delay=state.sequence<page.last_sequence||busy?25:1500;
            }
          }
          if(capabilities.conversation_stats&&Date.now()-state.statsAt>=3000){state.statsAt=Date.now();try{const stats=await api('dev-stats',{session_id:current.id});if(!state.disposed&&state.session?.id===stats.session_id){state.stats=stats;availability();}}catch{}}
          state.connectionError?.remove();state.connectionError=null;
        }catch(e){if(!state.disposed&&e.name!=='AbortError'){state.connectionError?.remove();state.connectionError=error(body,e);status.textContent='连接中断，正在重新连接…';delay=2500;}}
        finally{state.refreshing=null;if(!state.disposed)state.timer=setTimeout(refresh,(state.feedEpoch||0)!==startEpoch?0:delay);}
      })();
      return state.refreshing;
    }
    form.onsubmit=async event=>{
      event.preventDefault();
      if(send.disabled||!input.reportValidity()||!input.value.trim())return;
      const message=input.value, sentDraftRevision=draftRevisions.get(draftKey)||0;
      const queue=running(state.session)||!!state.session?.queued_messages?.length;
      const submission=state.submission||{text:message,id:uid(),after:queue?state.session.turn.id:null,steer:steerGesture};
      if(submission.text!==message){
        error(body,new Error('上次发送结果未确认，请先核对记录后再编辑要求。'));return;
      }
      const returnFocus=submission.after && (document.activeElement===input || document.activeElement===send);
      state.submission=submission;state.sending=true;availability();
      try{
        if(!state.session){
          state.creation||={request_id:uid(),provider_id:providers.value,...(state.newDraft?{new_conversation:true}:{})};
          state.session=await api('dev-session-create',{},state.creation);state.createdHere=true;
        }
        if(state.disposed)return;
        const payload={session_id:state.session.id,submission_id:submission.id,text:message};
        let started=null;
        if(submission.after)await api(submission.steer?'dev-steer':'dev-enqueue',{}, {...payload,turn_id:submission.after});
        else started=await api('dev-turn-start',{}, {...payload,expected_revision:state.session.revision});
        state.submission=null;
        if(drafts.get(draftKey)===message&&(draftRevisions.get(draftKey)||0)===sentDraftRevision){
          drafts.delete(draftKey);draftRevisions.delete(draftKey);saveDrafts(true);
        }
        if(state.disposed)return;
        input.value='';resizeInput();
        if(state.createdHere){await openSession(state.session.id);return;}
        if(started&&state.session){state.session={...state.session,turn:started};}
        state.listAt=0;clearTimeout(state.timer);
        // The message is already durable. Long polling must not keep the
        // composer locked while the user types or promotes the next message.
        if(submission.after)refresh();
        else{const inflight=!!state.refreshing;await refresh();if(inflight&&!state.disposed)await refresh();}
      }catch(err){
        if(err.operationStarted===false||['invalid_request','conflict','resource_limit','forbidden'].includes(err.code))state.submission=null;
        if(!state.disposed){error(body,err);await refresh().catch(()=>{});}
      }
      finally{
        state.sending=false;
        if(!state.disposed){
          availability();
          if(returnFocus&&document.activeElement===document.body&&!input.disabled)input.focus({preventScroll:true});
        }
      }
    };
    stop.onclick=async()=>{stop.disabled=true;try{await api('dev-cancel',{}, {session_id:state.session.id,turn_id:state.session.turn.id,request_id:uid()});status.textContent='已请求停止，等待所属工具清理。';}catch(e){error(body,e);stop.disabled=false;}};
    function clearTimeline(){state.feedEpoch=(state.feedEpoch||0)+1;chat.reset();newContent.hidden=true;}
    async function loadLatestReplay(){
      if(!state.session)return;
      if(capabilities.history_replay_api===true&&state.session.sequence>0){
        const epoch=state.feedEpoch||0;
        const replay=await api('dev-replay',{session_id:state.session.id});
        if(state.disposed||epoch!==(state.feedEpoch||0))return;
        for(const event of replay.events)project(event,{replaying:true});
        state.sequence=replay.next_sequence;
        state.windowStart=Math.max(0,(replay.first_sequence||1)-1);
        earlier.hidden=!replay.has_earlier;
      }else{
        state.sequence=Math.max(0,state.session.sequence-50,(state.session.archive_sequence||0)-1);
        state.windowStart=state.sequence;earlier.hidden=state.sequence===0;
      }
    }
    async function loadWindow(sid,after){
      const request=++state.navigationEpoch;
      const page=await api('dev-events',{session_id:sid,after_sequence:after,limit:50});
      if(state.disposed||state.navigationEpoch!==request)return;
      state.historyOnly=true;state.displaySession=sid;pendingHost.replaceChildren();state.pending.clear();
      follow.setFollowing(false);clearTimeline();
      state.windowStart=after;earlier.hidden=after===0;latest.hidden=false;availability();
      for(const event of page.events)project(event,{replaying:true});
      timeline.scrollTop=0;rail.update();
    }
    earlier.onclick=()=>loadWindow(state.displaySession||state.session.id,Math.max(0,(state.windowStart||0)-50)).catch(e=>error(body,e));
    latest.onclick=async()=>{state.navigationEpoch++;follow.setFollowing(true);clearTimeline();state.historyOnly=false;state.displaySession=null;latest.hidden=true;clearTimeout(state.timer);await loadLatestReplay();await refresh();};
    async function switchView(nextView){
      if(state.disposed||!capabilities.enabled)return;
      flushDrafts();
      const nav=++state.navigationEpoch;
      clearTimeout(state.timer);
      pendingHost.replaceChildren();state.pending.clear();
      state.historyOnly=false;state.displaySession=null;latest.hidden=true;earlier.hidden=true;
      state.submission=null;state.creation=null;state.createdHere=false;
      state.stats=null;state.statsAt=0;state.railRevision=null;state.sequence=0;
      state.newDraft=nextView?.kind==='development_new';
      follow.setFollowing(true);clearTimeline();state.files?.clearSessionChanges();
      if(nextView?.session_id){
        const cached=(state.sessionRows||[]).find(r=>r.id===nextView.session_id);
        if(cached){state.session=cached;renderConversations();}
        const chosen=await api('dev-session',{session_id:nextView.session_id});
        if(state.disposed||nav!==state.navigationEpoch)return;
        if(chosen.session_kind==='notebook'){
          const primary=(await api('dev-conversation')).session;
          if(state.disposed||nav!==state.navigationEpoch)return;
          state.primaryId=primary?.id||null;state.session=primary;
        }else{
          state.session=chosen;
        }
      }else if(state.newDraft){
        state.session=null;rail.clear();
      }else{
        const primary=(await api('dev-conversation')).session;
        if(state.disposed||nav!==state.navigationEpoch)return;
        state.primaryId=primary?.id||null;state.session=primary;
        if(!primary)rail.clear();
      }
      if(state.disposed||nav!==state.navigationEpoch)return;
      if(state.session){
        draftKey=workspaceId+':'+state.session.id;
        if((capabilities.providers||[]).some(p=>p.id===state.session.provider))providers.value=state.session.provider;
        input.value=drafts.get(draftKey)||'';
        applySession(state.session);resizeInput();
        await loadLatestReplay();
        if(state.disposed||nav!==state.navigationEpoch)return;
        chat.updateSession(state.session);
      }else{
        draftKey=workspaceId+':new';
        input.value=drafts.get(draftKey)||'';
        renderConversations();availability();resizeInput();
      }
      state.listAt=0;
      requestPrewarm();
      if(!state.refreshing)await refresh();
      else state.timer=setTimeout(refresh,0);
    }
    if(capabilities.enabled){
      const primary=(await api('dev-conversation')).session;if(state.disposed)return handle;state.primaryId=primary?.id||null;
      if(view?.session_id){
        const chosen=await api('dev-session',{session_id:view.session_id});
        state.session=chosen.session_kind==='notebook'?primary:chosen;
      }
      else if(!state.newDraft)state.session=primary;
      if(state.disposed)return handle;
      if(state.session){
        draftKey=workspaceId+':'+state.session.id;
        providers.value=state.session.provider;
        await loadLatestReplay();if(state.disposed)return handle;
      }
      const legacy=workspaceId+':project',defaultDraft=state.session?.id===state.primaryId&&!!state.session||!state.primaryId&&!state.newDraft&&!view?.session_id;
      if(defaultDraft&&drafts.has(legacy)){
        if(!drafts.has(draftKey))drafts.set(draftKey,drafts.get(legacy));
        if(drafts.get(draftKey)===drafts.get(legacy)){drafts.delete(legacy);draftRevisions.delete(legacy);}
        saveDrafts(true);
      }
      input.value=drafts.get(draftKey)||'';requestPrewarm();await refresh();
    }
    if(state.disposed)return handle;state.initializing=false;renderConversations();availability();resizeInput();
    return handle;
  }
  globalThis.ForgeDevelopment=Object.freeze({mount,activeCount:()=>active.size});
})();
