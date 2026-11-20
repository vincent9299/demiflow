/* Project definitions, independent of the selected flow or run. */
(() => {
  'use strict';
  const el=(tag,text,cls)=>{const n=document.createElement(tag);if(text!=null)n.textContent=text;if(cls)n.className=cls;return n;};
  function mount({host,request,mountText,disposeReaders,project}){
    let index=null,selected=null,revision='current',sequence=0,disposed=false,detail=null,activeKind='code';
    const root=el('div',null,'op-browser'),tree=el('nav',null,'op-tree'),viewer=el('section',null,'op-viewer');
    tree.setAttribute('aria-label','项目算子目录');viewer.setAttribute('aria-label','算子定义');
    const navigation=ForgeNavigation.header({title:'算子',searchLabel:'查找算子',onSearch:()=>paintTree()});
    const categories=el('div',null,'op-category-tabs');categories.setAttribute('role','tablist');categories.setAttribute('aria-label','算子分类');
    for(const [kind,label] of [['code','代码算子'],['prompt','Prompt 算子']]){
      const tab=el('button',label);tab.type='button';tab.setAttribute('role','tab');tab.dataset.kind=kind;
      tab.onclick=()=>{activeKind=kind;paintTree();};
      tab.onkeydown=event=>{if(['ArrowLeft','ArrowRight','Home','End'].includes(event.key)){event.preventDefault();const index=event.key==='Home'?0:event.key==='End'?1:kind==='code'?1:0;categories.children[index].click();categories.children[index].focus();}};
      categories.append(tab);
    }
    const {search}=navigation,list=el('div',null,'op-tree-items');
    list.setAttribute('role','tabpanel');list.setAttribute('aria-label','代码算子');
    tree.append(navigation.element,categories,list);root.append(tree,viewer);host.replaceChildren(root);
    const treeNavigation=ForgeNavigation.collapsible({layout:root,sidebar:tree,header:navigation,label:'算子目录'});
    const key='demiflow.operator-selection.'+project;
    try{selected=sessionStorage.getItem(key);}catch{}
    function button(text,action,parent,cls='op-text-button'){
      const b=el('button',text,cls);b.type='button';b.onclick=()=>Promise.resolve(action()).catch(error=>showError(error));parent.append(b);return b;
    }
    function showError(error){if(!disposed){const n=el('p',error.message,'op-error');n.setAttribute('role','alert');viewer.replaceChildren(n);}}
    function paintTree(){
      for(const tab of categories.children){const active=tab.dataset.kind===activeKind;tab.setAttribute('aria-selected',String(active));tab.tabIndex=active?0:-1;}
      list.setAttribute('aria-label',activeKind==='code'?'代码算子':'Prompt 算子');
      list.replaceChildren();const term=search.value.trim().toLocaleLowerCase();
      for(const kind of [activeKind]){
        const rows=(index?.definitions||[]).filter(d=>d.kind===kind&&(d.symbol+' '+d.path+' '+d.description).toLocaleLowerCase().includes(term));
        if(!rows.length)continue;
        const group=el('div',null,'op-tree-group');
        const paths=new Map();for(const row of rows){if(!paths.has(row.path))paths.set(row.path,[]);paths.get(row.path).push(row);}
        for(const [path,items]of paths){
          const folder=el('details',null,'op-tree-file');folder.open=!!term||items.some(i=>i.id===selected)||paths.size<4;
          const title=el('summary');title.append(el('span',path));title.title=path;folder.append(title);
          for(const item of items){const b=button('',()=>open(item.id),folder,'op-tree-leaf');
            b.append(el('span',kind==='code'?'ƒ':'P','op-glyph'),el('span',item.symbol));
            b.dataset.definition=item.id;b.setAttribute('aria-pressed',String(selected===item.id));b.title=item.path+' · '+item.symbol;
          }group.append(folder);
        }list.append(group);
      }
      if(!list.childNodes.length)list.append(el('p',index?.definitions.length?'没有匹配的算子':'项目尚未声明可复用算子或 Prompt。','op-muted'));
      for(const issue of index?.diagnostics||[])list.append(el('p',issue.path+'：'+issue.detail,'op-error'));
      list.querySelector('[aria-pressed=true]')?.scrollIntoView({block:'nearest'});
    }
    async function open(id,version='current',{selectKind=true}={}){
      selected=id;revision=version;
      if(selectKind)activeKind=index.definitions.find(item=>item.id===id)?.kind||activeKind;
      const ticket=++sequence;paintTree();
      try{sessionStorage.setItem(key,id);}catch{}
      disposeReaders(viewer);viewer.replaceChildren(el('p','读取定义…','op-muted'));
      const value=await request('operator-definition',{id,revision:version});
      if(disposed||ticket!==sequence)return;detail=value;paintDetail();
    }
    function paintDetail(){
      disposeReaders(viewer);viewer.replaceChildren();
      const header=el('header',null,'op-heading'),identity=el('div');
      identity.append(el('span',detail.kind==='code'?'代码算子':'Prompt','op-eyebrow'),el('h2',detail.symbol),el('div',detail.path,'op-location'));
      const actions=el('div',null,'op-heading-actions');
      button(revision==='current'?'版本历史':revision.slice(0,8)+' · 历史版本',()=>versions(),actions);
      header.append(identity,actions);viewer.append(header);
      if(detail.description)viewer.append(el('p',detail.description.split('\n')[0],'op-description'));
      if(detail.historical){const n=el('div',null,'op-history-notice');n.append(el('span','正在阅读已保存版本；不会修改当前代码。'));button('返回当前',()=>open(selected),n);viewer.append(n);}
      const tabs=el('nav',null,'op-tabs'),content=el('div',null,'op-content');tabs.setAttribute('aria-label','定义内容');viewer.append(tabs,content);
      const render=kind=>{
        for(const b of tabs.children)b.setAttribute('aria-pressed',String(b.dataset.tab===kind));
        disposeReaders(content);content.replaceChildren();
        DemiFlowDefinition.render(content,detail,{mountText,mode:kind==='source'?'source':'contract'});
      };
      for(const [kind,label]of [['source',detail.kind==='prompt'?'Prompt':'代码'],['contract','接口与默认值']]){const b=button(label,()=>render(kind),tabs);b.dataset.tab=kind;}
      render('source');
    }
    async function versions(){
      const ticket=sequence,id=selected;const value=await request('operator-history',{id});
      if(disposed||ticket!==sequence||id!==selected)return;
      viewer.querySelector('.op-history')?.remove();const panel=el('aside',null,'op-history'),heading=el('div',null,'op-history-heading');heading.append(el('strong','版本历史'));button('关闭',()=>panel.remove(),heading);panel.append(heading);
      button('当前工作区',()=>open(id),panel,'op-version');
      for(const v of value.versions){const b=button('',()=>open(id,v.revision),panel,'op-version');b.append(el('span',new Date(v.timestamp*1000).toLocaleString()),el('code',v.revision.slice(0,8)));}
      if(!value.versions.length)panel.append(el('p',value.reason||'尚无已保存提交。','op-muted'));
      else panel.append(el('p',value.scope,'op-muted'));
      viewer.append(panel);
    }
    search.oninput=paintTree;
    return {
      async load({force=false}={}){
        if(index&&!force)return;
        const ticket=++sequence;const next=await request('project-operators');if(disposed||ticket!==sequence)return;
        index=next;paintTree();const item=index.definitions.find(d=>d.id===selected)||index.definitions[0];
        if(item)await open(item.id,item.id===selected?revision:'current',{selectKind:!force});
        else viewer.replaceChildren(el('p','在开发中声明项目的 operators / prompts，即可在这里阅读定义。','op-empty'));
      },
      async selectSource(path,line,sha256){
        await this.load();const normalized=path?.replaceAll('\\','/');
        const matches=index.definitions.filter(d=>normalized===d.path
          &&d.file_sha256===sha256&&(!line||d.line<=line&&d.end_line>=line));
        if(matches.length===1){await open(matches[0].id);return true;}return false;
      },
      async selectPrompt(target){
        await this.load();const matches=index.definitions.filter(d=>d.kind==='prompt'&&d.symbol===target.name&&d.declared_version===target.version&&d.template_sha256===target.template_sha256);
        if(matches.length===1){await open(matches[0].id);return true;}return false;
      },
      dispose(){disposed=true;treeNavigation.dispose();sequence++;disposeReaders(viewer);root.remove();}
    };
  }
  globalThis.DemiFlowOperators=Object.freeze({mount});
})();
