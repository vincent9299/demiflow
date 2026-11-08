/* Buildless, offline design prototype. No fetch, storage or business writes. */
(() => {
  'use strict';
  const D = window.ForgeSampleData, R = window.ForgeSampleReading;
  const app = document.getElementById('app');
  const $ = (tag, value, cls) => { const n=document.createElement(tag);if(value!==undefined)n.textContent=value;if(cls)n.className=cls;return n; };
  let scene = new URLSearchParams(location.search).get('scene') || 'overview';
  let projectMode=scene==='incomplete'?'partial':scene==='onboarded'?'onboarded':'ready';
  let field = 'question', currentCase = 'c02', currentRoot = D.root, created = false;
  const scenes = new Set(['empty','onboarded','incomplete','overview','case','table','flows','checks','runs']);
  const labels = {empty:'欢迎使用',onboarded:'项目总览',incomplete:'项目总览',overview:'项目总览',case:'数据与 Case',table:'数据与 Case',flows:'流程与配置',checks:'规范检查',runs:'运行记录'};
  const icons = {
    overview:'M3 4h12v12H3z M7 4v12 M7 9h8',
    table:'M3 4h12v12H3z M3 8h12 M3 12h12 M7 4v12',
    flows:'M3 5h4v4H3z M11 11h4v4h-4z M7 7h6v4',
    checks:'M9 2l6 3v5c0 3-6 6-6 6s-6-3-6-6V5z M6 9l2 2 4-4',
    runs:'M9 3a6 6 0 1 1-5 3 M2 2v5h5 M9 5v4l3 2'
  };
  function icon(kind) {
    const svg=document.createElementNS('http://www.w3.org/2000/svg','svg');svg.setAttribute('viewBox','0 0 18 18');svg.setAttribute('aria-hidden','true');
    const p=document.createElementNS(svg.namespaceURI,'path');p.setAttribute('d',icons[kind]);svg.append(p);return svg;
  }
  function link(label, to, cls='') {
    const a=$('a',label,cls);a.href='?scene='+to;
    a.onclick=e=>{if(!e.ctrlKey&&!e.metaKey&&!e.shiftKey){e.preventDefault();go(to);}};return a;
  }
  function button(label,fn,cls='link-button') { const b=$('button',label,cls);b.type='button';b.onclick=fn;return b; }
  function go(to) { if(!scenes.has(to))to='overview';scene=to;history.pushState({},'', '?scene='+to);render();window.scrollTo(0,0); }
  function notify(message) {
    document.querySelector('.toast')?.remove();const t=$('div',message,'toast');t.role='status';document.body.append(t);
    document.getElementById('announce').textContent=message;setTimeout(()=>t.remove(),4200);
  }
  function note(title,body,neutral=false) {
    const n=$('aside',undefined,'status-note'+(neutral?' neutral':''));n.append($('span',neutral?'✓':'!','status-symbol'));
    const text=$('div');text.append($('strong',title),$('p',body));n.append(text);return n;
  }
  function intro(main,title,subtitle) {const p=$('div',undefined,'page-intro');p.append($('div','项目 / '+labels[scene],'eyebrow'),$('h1',title),$('p',subtitle));main.append(p);}
  function next(main,title,text,to,label) {
    const n=$('section',undefined,'next-step'),copy=$('div');copy.append($('div','下一步','step-label'),$('h2',title),$('p',text));n.append(copy,link(label+' →',to,'btn primary'));main.append(n);
  }
  function reader(parent,text,label='说明正文',defaultMode='preview') {
    const holder=$('div'),toolbar=$('div',undefined,'field-toolbar'),modes=$('div',undefined,'segmented'),body=$('div');
    toolbar.append($('span',label),modes);holder.append(toolbar,body);parent.append(holder);
    let mode=defaultMode;
    const choices=defaultMode==='plain'?[['plain','阅读'],['preview','Markdown 预览'],['raw','原文']]:[['preview','预览'],['raw','原文']];
    const buttons=new Map();
    for(const [key,title] of choices){const b=button(title,()=>{mode=key;paint();},'');b.setAttribute('aria-label',label+' · '+title);buttons.set(key,b);modes.append(b);}
    function paint() {
      for(const [key,b] of buttons)b.setAttribute('aria-pressed',String(mode===key));
      if(mode==='preview')R.render(body,text);
      else if(mode==='plain')body.replaceChildren($('div',text,'reader plain-text'));
      else body.replaceChildren($('pre',text,'raw-source'));
    }
    paint();return holder;
  }
  function sidebar(shell) {
    const side=$('aside',undefined,'sidebar');side.id='project-sidebar';
    const brand=$('a',undefined,'brand');brand.href='?scene=overview';brand.append($('span','Forge'),$('small','数据与流程\n工作台'));side.append(brand,$('div','项目','nav-caption'));
    if(scene==='empty') {
      side.append($('p','还没有登记项目','sidebar-empty'),button('+ 添加项目',()=>projectDialog(false),'btn'));
    } else {
      const picker=$('details',undefined,'project-picker'),summary=$('summary');summary.append($('span',created&&scene==='onboarded'?'新项目':D.name),$('span','⌄'));
      const menu=$('div',undefined,'project-menu');menu.append(link('✓ '+D.name,'overview'),$('p','示例目录：'+currentRoot),button('+ 新建项目',()=>projectDialog(true)),button('+ 注册已有项目',()=>projectDialog(false)));picker.append(summary,menu);side.append(picker);
      const nav=$('nav',undefined,'nav-items');nav.setAttribute('aria-label','当前项目任务');
      const current=['case','table'].includes(scene)?'table':['onboarded','incomplete'].includes(scene)?'overview':scene;
      for(const [to,label] of [['overview','项目总览'],['table','数据与 Case'],['flows','流程与配置'],['checks','规范检查'],['runs','运行记录']]){
        const a=link('',to,'nav-item');a.append(icon(to),$('span',label));if(to===current)a.setAttribute('aria-current','page');nav.append(a);
      }
      side.append(nav);
    }
    const bottom=$('div',undefined,'sidebar-bottom'),sync=$('p');sync.append($('i',undefined,'sync-dot'),document.createTextNode(scene==='empty'?'等待添加项目':'同步正常 · 示意'));bottom.append(sync,$('p','示例内容 · 不连接实际服务'));
    if(scene!=='empty')bottom.append(button('项目设置',()=>settings()));side.append(bottom);shell.append(side);
  }
  function render() {
    if(!scenes.has(scene))scene='overview';document.title='Forge · '+labels[scene]+' · 设计样稿';
    const originalCase=scene==='case'&&new URLSearchParams(location.search).get('origin')==='original';
    for(const a of document.querySelectorAll('.review-bar a')){if(a.search==='?scene='+scene+(originalCase?'&origin=original':''))a.setAttribute('aria-current','page');else a.removeAttribute('aria-current');}
    const shell=$('div',undefined,'shell');sidebar(shell);
    const workspace=$('div',undefined,'workspace'),bar=$('header',undefined,'topbar');
    const toggle=button('☰',()=>{const open=shell.classList.toggle('menu-open');toggle.setAttribute('aria-expanded',String(open));},'mobile-toggle');toggle.setAttribute('aria-label','展开项目导航');toggle.setAttribute('aria-controls','project-sidebar');toggle.setAttribute('aria-expanded','false');bar.append(toggle);
    const bread=$('div',undefined,'breadcrumbs');bread.append(link('我的项目','overview','bread-root'),$('span','/','bread-root'),$('strong',scene==='empty'?'Forge 数据工作台':D.name));
    if(scene!=='empty')bread.append($('span','/'),$('span',labels[scene]));bar.append(bread);
    const right=$('div',undefined,'topbar-right');right.append($('span',scene==='empty'?'等待添加项目':'可查看、检查 · 运行未开启','mode-label'),$('span',scene==='empty'?'未选择项目':'运行未开启','mobile-mode'),button('帮助',()=>help()));bar.append(right);
    const main=$('main',undefined,'content'+(scene==='empty'?' empty-content':''));main.id='main';main.tabIndex=-1;
    workspace.append(bar,main);shell.append(workspace);app.replaceChildren(shell);
    ({empty,overview,onboarded,incomplete,case:casePage,table:tablePage,flows,checks,runs}[scene])(main);
    main.append($('footer','Forge · 隔离设计样稿，所有路径、记录、版本与状态均为虚构示例。','page-footer'));
  }
  function empty(main) {
    const hero=$('section',undefined,'empty-hero');hero.append($('div','F','hero-mark'),$('div','FORGE · 数据与流程工作台','eyebrow'));
    const title=$('h1');title.append(document.createTextNode('让项目里的数据、流程和结果'),$('br'),document.createTextNode('看得清楚。'));hero.append(title);
    hero.append($('p','在原项目目录中阅读说明、查看数据与 Case、核对流程和检查结果。先添加一个项目，就能从已有内容开始。'));
    const actions=$('div',undefined,'actions');actions.append(button('注册已有项目 →',()=>projectDialog(false),'btn primary'),button('新建项目',()=>projectDialog(true),'btn'));hero.append(actions);main.append(hero);
    const steps=$('section',undefined,'empty-how');steps.setAttribute('aria-label','首次使用的三个步骤');
    for(const [n,title,text] of [['01','选择原项目目录','已有 pipeline 直接原地注册；新项目选择空目录。'],['02','从说明与一条 Case 开始','查看已识别内容，缺少的声明和证据会明确提示。'],['03','核对检查，再查看结果','检查不会执行 pipeline；运行还需授权与准入。']]){const item=$('div');item.append($('span',n,'step-number'),$('strong',title),$('p',text));steps.append(item);}
    main.append(steps);const p=$('p',undefined,'empty-note');p.append($('strong','原目录就是项目。'),document.createTextNode(' 登记只添加必要的管理信息；移除登记保留项目文件与历史数据。'));main.append(p);
  }
  function readme(main,side=true) {
    const grid=$('div',undefined,side?'overview-grid':''),section=$('section',undefined,'read-section');
    const head=$('div',undefined,'section-header');head.append($('h2','项目说明'),$('span','README.md','tag'));section.append(head);
    if(created&&scene==='onboarded')section.append($('p','这个新目录还没有 README。保存项目说明后，页面会显示在这里。','muted small'));
    else reader(section,D.readme,'README.md');
    const src=$('div',undefined,'document-source');src.append($('span','来自项目已保存的 README.md'),button('刷新',()=>notify('样稿只展示本地示例，不向实际服务发起刷新。')));section.append(src);grid.append(section);
    if(side){
      const aside=$('aside',undefined,'support-side'),contents=$('section',undefined,'support-group');contents.append($('h2','项目内容'));
      for(const [label,hint,to] of [['数据与 Case','3 张固定版本表 · 按需读取','table'],['流程与配置','2 个入口 · 2 份配置','flows'],['运行记录','仅展示实际保存的记录','runs']]){const a=link('',to,'content-link'),t=$('span',label);t.append($('small',hint));a.append(t,$('span','›','arrow'));contents.append(a);}
      const state=$('section',undefined,'support-group');state.append($('h2','检查与证据'),$('div','整体仍缺少证据','status-text'),$('p','静态检查通过。行为验证与语义审查尚未完成，不表示已获准运行。'),link('查看检查结果 →','checks','small'));
      const boundary=$('div',undefined,'fine-print'),details=$('details');details.append($('summary','项目与平台信息'),$('p','目录：'+currentRoot),$('p','运行权限：未开启'),$('p','修改范围：未启用；不表示已验证范围内'),$('p','规范与完整摘要在正式页面复用当前绑定事实。'));boundary.append(details,button('复制项目定位链接',()=>copyLink('overview')));aside.append(contents,state,boundary);grid.append(aside);
    }
    main.append(grid);
  }
  function overview(main) {
    if(projectMode==='partial'){incomplete(main);return;}
    if(projectMode==='onboarded'||projectMode==='new'){onboarded(main);return;}
    intro(main,D.name,'阅读原项目，查看固定版本的数据，并核对流程、检查和结果。');
    next(main,'先看一条题目，再核对筛选配置','从候选数据开始，逐条理解问题、答案和来源。','table','查看数据与 Case');
    const status=$('div',undefined,'evidence-line');status.append($('span','整体仍缺少证据'),$('span','静态通过 · 行为 / 审查尚未完成'),link('核对检查 →','checks'));main.append(status);
    readme(main);
  }
  function onboarded(main) {
    intro(main,created?'项目已新建，可以开始了':'项目已登记，从已有内容开始','原目录就是项目。你可以先阅读，再决定是否检查。');
    const progress=$('div',undefined,'onboarding-progress');for(const [label,cls] of [['✓ 目录已登记','complete'],['2 阅读项目内容','current'],['3 核对检查与证据','']])progress.append($('span',label,cls));main.append(progress);
    main.append(note(created?'已新建 · 尚无业务内容':'已登记 · 尚未检查',created?'只添加 workspace.json 与 .forge/ 管理信息。入口、数据和输出位置由项目自行定义。':'示例已识别 2 个入口、2 份配置与 3 个数据引用。识别结果不代表规范通过。',true));
    const n=$('section',undefined,'next-step'),copy=$('div');copy.append($('div','建议先做','step-label'),$('h2',created?'在原目录中编写说明与代码':'读项目说明，确认这里是不是你的项目'),$('p',created?'不要求固定业务目录。未保存或不支持的内容不会被猜测成可执行入口。':'可以直接查看已识别内容；不会自动执行声明中的命令。'));n.append(copy,button(created?'查看使用说明':'先读项目说明 ↓',()=>{if(created)help();else document.querySelector('.read-section').scrollIntoView({behavior:'smooth'});},'btn primary'));main.append(n);readme(main,false);
  }
  function incomplete(main) {
    intro(main,'项目已登记，部分内容尚未识别','可以继续读说明和源码；暂不能把未识别内容当作可检查、可运行的流程。');
    main.append(note('识别不完整 · 不代表项目没有内容','示例找到 1 个原入口与 2 份配置；缺少原生参数绑定。数据引用位于项目外，未获读取授权。'));
    next(main,'先核对入口与已有配置','这里只展示缺口和原依据，不替项目生成声明或改写代码。','flows','查看入口与配置');
    const list=$('div');
    for(const [title,text,path] of [['入口已识别，尚不能完成 Flow 检查','原协议为 config → run_pipeline；配置引用尚未明确绑定为原生参数。','dispatch.py:run_pipeline · 依据：公开项目声明'],['数据引用在项目外，当前不能预览','保留原引用与原因。不能根据文档链接扩大读取权限。','../shared/candidates.lance · 依据：README.md:18']]){const d=$('section',undefined,'diagnostic');d.append($('h3',title),$('p',text),$('p',path,'path'));list.append(d);}main.append(list);
    const actions=$('div',undefined,'actions');actions.append(link('查看规范检查','checks','btn'),button('查看原源码',()=>codeDialog('dispatch.py')));main.append(actions);
    const d=$('details',undefined,'record-meta');d.append($('summary','识别范围与完整依据'),$('p','识别按现有文件数、字节、深度预算读取公开声明。未读取、未知协议和超限范围均不计为通过。'));main.append(d);readme(main,false);
  }
  function tablePage(main) {
    intro(main,'数据与 Case','选择一张表，再打开一条记录。列表只显示摘要，详情按字段阅读。');
    if(projectMode==='partial'){main.append(note('已识别引用，当前未获读取授权','../shared/candidates.lance 位于项目外。文档中的引用不授予读取权限；未打开数据，不会显示虚构行数或 Case。'),link('查看识别与检查缺口','checks','btn'));return;}
    if(projectMode==='new'){main.append($('p','新目录尚未声明数据引用；这不代表平台已扫描或授权其他位置。','muted'));return;}
    const bar=$('div',undefined,'section-header');bar.append($('h2','candidates'),$('span','固定版本 v3 · 示例 3 行','tag'));main.append(bar);
    main.append($('p','来源：已绑定输入 · data/candidates.lance。版本不会随表头更新自动切换。','muted small'));
    const wrap=$('div',undefined,'table-wrap'),t=$('table',undefined,'data-table'),head=$('thead'),tr=$('tr');for(const title of ['业务键','题目摘要','质量分数',''])tr.append($('th',title));head.append(tr);t.append(head);const body=$('tbody');
    for(const row of D.rows){const tr=$('tr');tr.append($('td',row.id));const c=$('td');c.append($('div',row.title,'cell-summary'));tr.append(c,$('td',row.id==='c02'?'0.91':'0.85'));const go=$('td');go.append(button('阅读 Case →',()=>{currentCase=row.id;field='question';window.scrollTo(0,0);window.history.replaceState({},'','?scene=table');scene='case';history.pushState({},'','?scene=case');render();}));tr.append(go);body.append(tr);}t.append(body);wrap.append(t);main.append(wrap);
    const pager=$('div',undefined,'toolbar-small');pager.append($('span','1–3 / 3 行 · 摘要可缩略，详情不静默截断'),$('span','上一页　下一页'));main.append(pager);
    main.append(note('数据来源与执行证据分别查看','固定版本输入可阅读，不表示某次流程已经执行。独立准备的对照数据不标为运行产物。',true));
  }
  function casePage(main) {
    const originalCase=new URLSearchParams(location.search).get('origin')==='original';
    main.append(link('← 返回 candidates 数据表','table','view-back'));
    const heading=$('div',undefined,'case-title');heading.append($('h1',originalCase?'Case · '+D.rows.find(r=>r.id===currentCase).rowId:'题目 '+currentCase));
    heading.append(originalCase?$('span','原项目 Case 暂无独立定位协议','muted small'):button('复制 Case 定位链接',()=>copyLink('case')));main.append(heading);
    const identity=$('div',undefined,'identity-strip');identity.append($('span','candidates · v3','tag'),$('span',originalCase?'原项目引用 · 打开时固定版本':'已绑定输入'),$('span','·'),$('span',originalCase?'case_id = '+currentCase:'业务键 case_id = '+currentCase));main.append(identity);
    const layout=$('div',undefined,'case-layout'),list=$('aside',undefined,'case-list');list.setAttribute('aria-label','当前固定版本的记录');list.append($('div','当前表 · 3 条记录','list-label'));
    for(const row of D.rows){const choice=button('',()=>{currentCase=row.id;caseRender();},'case-choice');choice.append($('strong',originalCase?'row_id '+row.rowId:'题目 '+row.id),$('span',row.title));if(row.id===currentCase)choice.setAttribute('aria-current','true');list.append(choice);}list.append($('div','v3 · 切换只读取所选记录','list-footer'));layout.append(list);
    const article=$('article',undefined,'reading-pane'),tabs=$('div',undefined,'field-tabs');tabs.setAttribute('role','tablist');tabs.setAttribute('aria-label','Case 字段');
    for(const [key,title] of [['question',originalCase?'question':'题目正文'],['answer',originalCase?'answer':'参考答案'],['other','其他字段']]){const b=button(title,()=>{field=key;caseRender();},'');b.setAttribute('role','tab');b.setAttribute('aria-selected',String(field===key));b.tabIndex=field===key?0:-1;b.onkeydown=e=>{const keys=['question','answer','other'];if(['ArrowLeft','ArrowRight','Home','End'].includes(e.key)){e.preventDefault();const i=keys.indexOf(field);field=e.key==='Home'?'question':e.key==='End'?'other':keys[(i+(e.key==='ArrowRight'?1:2))%3];caseRender();document.querySelector('.field-tabs [aria-selected=true]').focus();}};tabs.append(b);}article.append(tabs);
    if(field==='other') {
      const dl=$('dl',undefined,'meta-grid');for(const [k,v] of [['case_id',currentCase],['score','0.91'],['category','条件完整性'],['metadata','结构化 JSON · 原文保留']])dl.append($('dt',k),$('dd',v));article.append(dl,$('pre','{\n  "review_status": "pending",\n  "source": "sample_fixture"\n}','code-view'));
    } else {
      const q=currentCase==='c02'?D.question:'## '+D.rows.find(r=>r.id===currentCase).title+'\n\n这是一条用于审阅切换行为的虚构题目。请选择 **c02** 查看完整的长文、表格、引用和代码块示例。';
      const content=field==='question'?q:D.answer;
      if(R.modeFor(content,originalCase?{}:{kind:'text'})==='preview')reader(article,content,originalCase?field+' · 长文本（按长度 / 多行识别）':field==='question'?'question · 文本字段':'answer · 文本字段');
      else article.append($('div',content,'reader plain-text'));
    }
    const meta=$('details',undefined,'record-meta');meta.append($('summary','固定版本、来源与原始记录'),$('p','数据引用：data/candidates.lance · v3 · 3 行'),$('p','row_id = '+D.rows.find(r=>r.id===currentCase).rowId+'（只用于固定行定位，不是跨表业务键）'),$('p','绑定摘要：'+D.binding),$('p','外部链接需用户点击；相对文档引用未扩展为任意文件读取。'));
    const raw=$('details');raw.append($('summary','原始 JSON · 按需查看'),$('pre',JSON.stringify({case_id:currentCase,question:currentCase==='c02'?D.question:D.rows.find(r=>r.id===currentCase).title,answer:D.answer,score:0.91},null,2),'raw-source'));meta.append(raw);article.append(meta);
    const pair=$('details',undefined,'pair-box');pair.append($('summary','按业务键对照另一张表'),$('p','按 case_id 匹配 scores · v2；不按 row_id 推定一一对应。'));
    pair.append(button('查找对应 Case（示例）',()=>{if(!pair.querySelector('.status-note'))pair.append(note('找到 2 条匹配记录 · 业务键不唯一','这是独立准备的对照数据，不是当前 Flow 的执行结果。'));},'btn'));if(!originalCase)article.append(pair);
    layout.append(article);main.append(layout);
    function caseRender(){render();}
  }
  function flows(main) {
    intro(main,'流程与配置','查看已有入口和配置，核对实际绑定；检查与运行分别进行。');
    if(projectMode==='new'){main.append($('p','这个新目录尚未声明入口或配置。平台不会按文件名猜测可执行流程。','muted'));return;}
    if(projectMode==='partial'){
      main.append(note('原入口已识别，尚不能完成 Flow 检查','协议为 config → run_pipeline；已有配置引用未绑定为受支持的原生参数。不授予检查通过或运行权限。'));
      const list=$('ul',undefined,'plain-list'),item=$('li'),text=$('div');text.append($('code','dispatch.py:run_pipeline'),$('div','原协议 · 缺少原生参数绑定','row-detail'));item.append(text,button('查看源码',()=>codeDialog('dispatch.py')));list.append(item);main.append(list);
      main.append($('h2','已引用的配置'),$('p','settings/select_a.json · settings/select_b.json（引用存在，不代表已绑定为原生 Flow 参数）','muted small'),$('pre','{\n  "threshold": 0.80\n}','code-view'),link('核对检查缺口 →','checks','btn'));return;
    }
    main.append(note('检查与运行未授权的范围不会被补齐','已识别入口不等于支持原生 Flow。下面是用于设计 Review 的已绑定原生入口示例。',true));
    const l=$('ul',undefined,'plain-list');for(const [a,b]of [['recipe.py:select','原生 Flow · settings/select_a.json'],['recipe.py:compare','原生 Flow · settings/select_b.json']]){const li=$('li'),text=$('div');text.append($('code',a),$('div',b,'row-detail'));li.append(text,button('查看源码',()=>codeDialog()));l.append(li);}main.append(l);
    const section=$('section',undefined,'section-space');section.append($('h2','settings/select_a.json'),$('pre','{\n  "threshold": 0.80,\n  "input": "candidates",\n  "output": "selected_a"\n}','code-view'));
    const actions=$('div',undefined,'actions');actions.append(link('查看检查结果','checks','btn primary'));const run=button('准备运行',()=>{},'btn');run.disabled=true;actions.append(run,$('span','本会话未开启运行；查看和检查仍可用。','muted small'));section.append(actions);main.append(section);
  }
  function checks(main) {
    intro(main,'规范检查','查看当前检查结果、实际覆盖范围和仍缺少的证据。');
    if(projectMode!=='ready'){
      main.append(note(projectMode==='partial'?'整体尚未完成 · 静态检查不完整':'尚未检查 · 仍缺少证据',projectMode==='partial'?'已识别 1 个原入口，但未绑定原生参数；源码前置检查不能代替 Flow 语义检查。行为验证与语义审查也尚未完成。':'登记不会自动执行检查。请先阅读项目内容，检查会保存派生报告，不执行 pipeline。'));
      main.append(link(projectMode==='new'?'返回项目说明':'查看入口与配置',projectMode==='new'?'overview':'flows','btn'),$('p','样稿保持这份缺口，不会把导航点击变成检查通过。','muted small'));return;
    }
    main.append(note('整体尚缺证据','静态检查已通过；行为验证与语义审查尚未完成。检查报告不等于执行许可。'));
    const summary=$('section',undefined,'check-summary');for(const [k,v]of [['静态检查','已通过'],['行为验证','尚无证据'],['语义审查','尚无证据']]){const n=$('div');n.append($('small',k),$('strong',v,k==='静态检查'?'':'warning-text'));summary.append(n);}main.append(summary);
    main.append($('p','已覆盖示例中 2 / 2 组声明入口与配置。未声明内容不计为通过。','muted small'));
    const actions=$('div',undefined,'actions');actions.append(button('检查当前项目（样稿）',()=>notify('样稿不会保存或提交检查。正式页面复用 project-check，并显示真实结果。'),'btn'),button('模拟依据变化',()=>{main.querySelector('.status-note').replaceWith(note('依据已变化 · 旧报告保留','示例源码已变更；旧静态结果不能继续当作当前结果，也不能放行运行。'));}));main.append(actions);
    const details=$('details',undefined,'record-meta');details.append($('summary','完整规则、诊断与绑定摘要'),$('p','正式实现复用现有规范版本、诊断路径、行号、检查来源和摘要；不会在客户端重新判定通过。'));main.append(details);
  }
  function runs(main) {
    intro(main,'运行记录','只有真实保存的运行与结果显示为实验记录。');
    const box=$('section',undefined,'quiet-box');box.append($('h2','还没有本示例的实际运行记录'),$('p','你仍可以查看固定版本数据。README 中的日志路径或独立对照表，不会被解释成运行成功。'),link('返回数据与 Case','table','btn'));main.append(box);
  }
  function modal(title) {const dialog=$('dialog',undefined,'dialog');dialog.setAttribute('aria-label',title);dialog.append($('h2',title));document.body.append(dialog);dialog.addEventListener('close',()=>dialog.remove(),{once:true});return dialog;}
  function projectDialog(create) {
    const d=modal(create?'新建项目':'注册已有项目');d.append($('p',create?'选择服务所在机器上的空目录，或尚不存在的目录。':'填写原 pipeline 所在目录。Forge 会保留代码、数据与原目录结构。'));
    const form=$('form'),root=$('input');root.required=true;root.value=create?'/example/new-project':D.root;root.maxLength=4096;root.autocomplete='off';root.id='sample-project-root';
    const label=$('label','服务器上的绝对目录');label.htmlFor=root.id;label.append(root);form.append(label);
    const name=$('input');name.value=create?'新项目':D.name;name.id='sample-project-name';name.maxLength=128;name.required=create;const nl=$('label','显示名称'+(create?'':'（可选）'));nl.htmlFor=name.id;nl.append(name);form.append(nl);
    form.append($('p','隔离样稿：此表单只切换示例场景，不创建目录、不登记项目、不连接服务。','demo-note'));
    const actions=$('div',undefined,'actions');actions.append(button('取消',()=>d.close(),'btn'));const submit=$('button',create?'预览新建结果':'预览登记结果','btn primary');submit.type='submit';actions.append(submit);form.append(actions);
    form.onsubmit=e=>{e.preventDefault();if(!root.value.startsWith('/')){root.setCustomValidity('请填写绝对目录。');root.reportValidity();root.setCustomValidity('');return;}currentRoot=root.value;created=create;projectMode=create?'new':'onboarded';d.close();go('onboarded');};d.append(form);d.showModal();root.focus();
  }
  function help(){const d=modal('从已有项目开始');d.append($('p','Forge 是数据与流程工作台：原地登记项目，阅读说明和 Case，再核对流程、检查和运行事实。'),$('p','对话与代码修改仍在原开发工具中完成。此样稿不增加聊天或模型选择。'),button('明白了',()=>d.close(),'btn primary'));d.showModal();}
  function settings(){const d=modal('项目设置');d.append($('p','当前示例：'+D.name),$('p',currentRoot),$('p','正式页面的“移除项目”只取消控制台登记，保留目录、历史与已启动任务，并继续使用现有摘要确认。'),button('预览移除确认',()=>{d.close();const confirm=modal('移除项目');confirm.append($('p','当前示例：'+D.name),$('p',currentRoot),$('p','仅取消登记；文件、管理信息、历史数据保留。已启动任务继续运行，未提交草稿会清除。'),button('取消',()=>confirm.close(),'btn'),button('确认移除（仅样稿）',()=>{confirm.close();go('empty');},'btn'));confirm.showModal();}),button('关闭',()=>d.close(),'btn'));d.showModal();}
  function codeDialog(path='recipe.py'){const d=modal('原源码 · 只读');d.append($('p','示例路径：'+path),$('pre',path==='dispatch.py'?'# 示例原协议；平台只读，不执行\ndef run_pipeline(config):\n    pass':'# 示例只用于展示原文样式\ndef select(config):\n    return candidates.filter(score_at_least(config.threshold))','code-view'),button('关闭',()=>d.close(),'btn'));d.showModal();}
  async function copyLink(to){const url=new URL(location.href);url.search='?scene='+to;url.hash='';try{await navigator.clipboard.writeText(url.href);notify('已复制样稿链接。正式 Case 链接仍绑定项目、表摘要与字符串 row_id。');}catch{notify('样稿链接：'+url.href);}}
  window.addEventListener('popstate',()=>{scene=new URLSearchParams(location.search).get('scene')||'overview';render();});
  document.addEventListener('keydown',e=>{if(e.key==='Escape'){const shell=document.querySelector('.shell');shell?.classList.remove('menu-open');document.querySelector('.mobile-toggle')?.setAttribute('aria-expanded','false');}});
  render();
})();
