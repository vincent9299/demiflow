/* Bounded graph presentation shared by the live workbench and fixed exports. */
(() => {
  'use strict';
  const ns='http://www.w3.org/2000/svg';
  const svg=(tag,attrs={},text)=>{const node=document.createElementNS(ns,tag);for(const[k,v]of Object.entries(attrs))node.setAttribute(k,String(v));if(text!==undefined)node.textContent=text;return node;};
  const kindLabel={source_call:'源码调用',read_table:'数据输入',write_table:'输出',map:'自定义算子',join:'关联',filter:'筛选',query:'查询',reduce:'聚合'};
  const states={pending:'已登记 · 未开始',running:'运行中',succeeded:'执行完成',failed:'执行失败',incomplete:'观测不完整',reused:'复用物化结果',cancelled:'已取消'};
  function status(node,snapshot) {
    if(!snapshot)return {label:node.kind==='source_call'?'尚未检查':'未运行',tone:'neutral'};
    const records=(snapshot.observations||[]).filter(o=>o.node_id===node.id);
    const output=(snapshot.outputs||[]).find(o=>o.action_id===node.id);
    if(output?.status==='committed')return {label:output.result?'已提交并核验':'已提交 · 登记待完成',tone:'success'};
    if(snapshot.observation_availability==='unsupported')return {label:'历史未记录节点观测',tone:'neutral'};
    if(snapshot.observation_scope==='paged_run_observations'&&!Array.isArray(snapshot.observations))return {label:'观测尚未载入',tone:'neutral'};
    if(!records.length)return {label:snapshot.observations_truncated?'摘要未载入 · 点击核对':'未单独观测',tone:'neutral'};
    const unique=[...new Set(records.map(r=>r.state))];
    const label=records.length===1?(states[unique[0]]||unique[0]):records.length+' 次观测 · '+(unique.length===1?(states[unique[0]]||unique[0]):'多种状态');
    return {label,tone:unique.includes('failed')?'failed':unique.includes('running')?'running':unique.every(s=>['succeeded','reused'].includes(s))?'success':'neutral'};
  }
  function mount(host,{onSelect=()=>{}}={}) {
    const canvas=svg('svg',{'aria-label':'数据算子图',role:'group'});host.replaceChildren(canvas);
    let current=null,selected=null,zoom=1,width=600,height=280,viewMode='readable',resizeFrame=null;
    const elements=new Map();
    function size(){canvas.style.width=Math.round(width*zoom)+'px';canvas.style.height=Math.round(height*zoom)+'px';}
    function fit(){viewMode='fit';zoom=Math.max(.25,Math.min(1,(host.clientWidth-16)/width,(host.clientHeight-16)/height));size();}
    function readable(){viewMode='readable';zoom=Math.max(host.clientWidth>=640?.5:.85,Math.min(1,(host.clientWidth-16)/width));size();host.style.setProperty('--wb-content-height',Math.round(height*zoom+24)+'px');}
    const resize=typeof ResizeObserver==='function'?new ResizeObserver(()=>{if(resizeFrame!==null||viewMode==='manual')return;resizeFrame=requestAnimationFrame(()=>{resizeFrame=null;if(viewMode==='fit')fit();else if(viewMode==='readable')readable();});}):null;resize?.observe(host);
    function select(id){selected=id;for(const[key,value]of elements){value.group.classList.toggle('selected',key===id);value.group.setAttribute('aria-pressed',String(key===id));}}
    function paint(snapshot){for(const node of current?.nodes||[]){const row=elements.get(node.id),value=status(node,snapshot);row.state.textContent=value.label.slice(0,26);row.group.dataset.state=value.tone;row.group.setAttribute('aria-label',(node.operator?.name||node.api)+' · '+value.label);}}
    function update(graph,snapshot,nodeId) {
      if(!host.contains(canvas))host.replaceChildren(canvas);
      if(current&&JSON.stringify(current.binding)===JSON.stringify(graph.binding)&&JSON.stringify(current.nodes)===JSON.stringify(graph.nodes)&&JSON.stringify(current.edges)===JSON.stringify(graph.edges)){current=graph;paint(snapshot);select(nodeId);return;}
      if(graph.nodes.length>256||graph.edges.length>512)throw new Error('图超过显示预算');
      current=graph;canvas.replaceChildren();elements.clear();
      const nodes=graph.nodes,edges=graph.edges,ids=new Set(nodes.map(n=>n.id));
      const levels=new Map(nodes.map(n=>[n.id,0])),incoming=new Map(nodes.map(n=>[n.id,0])),outgoing=new Map();
      for(const edge of edges){if(!ids.has(edge.from)||!ids.has(edge.to))continue;incoming.set(edge.to,incoming.get(edge.to)+1);if(!outgoing.has(edge.from))outgoing.set(edge.from,[]);outgoing.get(edge.from).push(edge.to);}
      const queue=nodes.filter(n=>incoming.get(n.id)===0).map(n=>n.id);let visited=0;
      while(queue.length){const id=queue.shift();visited++;for(const to of outgoing.get(id)||[]){levels.set(to,Math.max(levels.get(to),levels.get(id)+1));incoming.set(to,incoming.get(to)-1);if(incoming.get(to)===0)queue.push(to);}}
      if(visited<nodes.length)for(const node of nodes)if(incoming.get(node.id)>0)levels.set(node.id,0);
      const counts=new Map(),positions=new Map();
      for(const node of nodes){const level=levels.get(node.id),row=counts.get(level)||0;counts.set(level,row+1);positions.set(node.id,{x:24+level*224,y:24+row*122});}
      width=Math.max(440,(Math.max(0,...levels.values())+1)*224+8);height=Math.max(150,Math.max(1,...counts.values())*122+20);
      canvas.setAttribute('viewBox',`0 0 ${width} ${height}`);
      const markerId='wb-arrow-'+Math.random().toString(16).slice(2),defs=svg('defs'),marker=svg('marker',{id:markerId,viewBox:'0 0 10 10',refX:9,refY:5,markerWidth:5,markerHeight:5,orient:'auto'});marker.append(svg('path',{d:'M0 0 L10 5 L0 10z',fill:'#8e9eb8'}));defs.append(marker);canvas.append(defs);
      for(const edge of edges){const a=positions.get(edge.from),b=positions.get(edge.to);if(!a||!b)continue;const path=svg('path',{d:`M${a.x+192} ${a.y+46} C${a.x+212} ${a.y+46},${b.x-20} ${b.y+46},${b.x} ${b.y+46}`,class:'wb-edge wb-edge-'+edge.type,'marker-end':'url(#'+markerId+')'});path.append(svg('title',{},({data:'数据流',value:'值依赖',action:'动作先后',reference:'源码引用 · 类型未核验'})[edge.type]||edge.type));canvas.append(path);}
      for(const node of nodes){const pos=positions.get(node.id),group=svg('g',{class:'wb-node','data-node':node.id,transform:`translate(${pos.x} ${pos.y})`,tabindex:0,role:'button'});
        const title=(node.operator?.name||node.api.split(/[.:]/).at(-1)||node.kind).slice(0,24);
        const state=svg('text',{x:13,y:76,class:'wb-node-state'});
        group.append(svg('rect',{width:192,height:94,rx:9}),svg('text',{x:13,y:22,class:'wb-node-kind'},kindLabel[node.kind]||node.kind),svg('text',{x:13,y:47,class:'wb-node-title'},title),state);
        const activate=()=>{select(node.id);onSelect(node.id);};group.onclick=activate;group.onkeydown=event=>{if(event.key==='Enter'||event.key===' '){event.preventDefault();activate();}};
        elements.set(node.id,{group,state});canvas.append(group);
      }
      paint(snapshot);select(nodeId);readable();
    }
    return Object.freeze({update,paint,select,fit,zoom:factor=>{viewMode='manual';zoom=Math.max(.25,Math.min(2,zoom*factor));size();},dispose(){resize?.disconnect();if(resizeFrame!==null)cancelAnimationFrame(resizeFrame);resizeFrame=null;}});
  }
  function mountLogical(host,{onSelect=()=>{}}={}) {
    const canvas=svg('svg',{'aria-label':'声明图投影（E2）',role:'group'});
    let current=null,selected=null,zoom=1,width=600,height=280,viewMode='readable',resizeFrame=null;
    const elements=new Map();
    function size(){canvas.style.width=Math.round(width*zoom)+'px';canvas.style.height=Math.round(height*zoom)+'px';}
    function fit(){viewMode='fit';zoom=Math.max(.25,Math.min(1,(host.clientWidth-16)/width,(host.clientHeight-16)/height));size();}
    function readable(){viewMode='readable';zoom=Math.max(host.clientWidth>=640?.5:.85,Math.min(1,(host.clientWidth-16)/width));size();host.style.setProperty('--wb-content-height',Math.round(height*zoom+24)+'px');}
    const resize=typeof ResizeObserver==='function'?new ResizeObserver(()=>{if(resizeFrame!==null||viewMode==='manual')return;resizeFrame=requestAnimationFrame(()=>{resizeFrame=null;if(viewMode==='fit')fit();else if(viewMode==='readable')readable();});}):null;resize?.observe(host);
    function select(id){selected=id;for(const[key,value]of elements){value.group.classList.toggle('selected',key===id);value.group.setAttribute('aria-pressed',String(key===id));}}
    function update(dag,nodeMap,labels,selectedId,basis) {
      if(!host.contains(canvas))host.replaceChildren(canvas);
      canvas.setAttribute('aria-label',(basis==='sealed'||basis==='executed')?'运行封存计划（E2）':'声明图投影（E2）');
      current=dag;canvas.replaceChildren();elements.clear();
      const occurrences=new Map();
      for(const node of dag.nodes){if(!occurrences.has(node.logical_node_id))occurrences.set(node.logical_node_id,[]);occurrences.get(node.logical_node_id).push(node);}
      const ids=[...occurrences.keys()],shared=new Set(dag.shared_ancestors.map(s=>s.logical_node_id));
      const levels=new Map(ids.map(id=>[id,0])),incoming=new Map(ids.map(id=>[id,0])),outgoing=new Map();
      for(const edge of dag.edges){const a=edge.from.logical_node_id,b=edge.to.logical_node_id;if(!occurrences.has(a)||!occurrences.has(b))continue;incoming.set(b,incoming.get(b)+1);if(!outgoing.has(a))outgoing.set(a,[]);outgoing.get(a).push(b);}
      const queue=ids.filter(id=>incoming.get(id)===0);let visited=0;
      while(queue.length){const id=queue.shift();visited++;for(const to of outgoing.get(id)||[]){levels.set(to,Math.max(levels.get(to),levels.get(id)+1));incoming.set(to,incoming.get(to)-1);if(incoming.get(to)===0)queue.push(to);}}
      if(visited<ids.length)for(const id of ids)if(incoming.get(id)>0)levels.set(id,0);
      const counts=new Map(),positions=new Map();
      for(const id of ids){const level=levels.get(id),row=counts.get(level)||0;counts.set(level,row+1);positions.set(id,{x:24+level*224,y:24+row*122});}
      width=Math.max(440,(Math.max(0,...levels.values())+1)*224+8);height=Math.max(150,Math.max(1,...counts.values())*122+20);
      canvas.setAttribute('viewBox',`0 0 ${width} ${height}`);
      const markerId='wb-logical-arrow-'+Math.random().toString(16).slice(2),defs=svg('defs'),marker=svg('marker',{id:markerId,viewBox:'0 0 10 10',refX:9,refY:5,markerWidth:5,markerHeight:5,orient:'auto'});marker.append(svg('path',{d:'M0 0 L10 5 L0 10z',fill:'#8e9eb8'}));defs.append(marker);canvas.append(defs);
      for(const edge of dag.edges){const a=positions.get(edge.from.logical_node_id),b=positions.get(edge.to.logical_node_id);if(!a||!b)continue;
        const path=svg('path',{d:`M${a.x+192} ${a.y+46} C${a.x+212} ${a.y+46},${b.x-20} ${b.y+46},${b.x} ${b.y+46}`,class:'wb-edge wb-edge-data','marker-end':'url(#'+markerId+')'});
        path.append(svg('title',{},'逻辑数据流 · 端口 '+edge.from.port+' → '+edge.to.port));canvas.append(path);
        canvas.append(svg('text',{x:(a.x+192+b.x)/2,y:(a.y+b.y)/2+38,class:'wb-edge-port'},edge.from.port+'→'+edge.to.port));}
      for(const id of ids){const pos=positions.get(id),group=occurrences.get(id),node=group[0];
        const declared=nodeMap[id],label=(declared&&labels[declared])||node.op_kind;
        const ports=(node.input_ports.join(',')||'—')+' → '+(node.output_ports.join(',')||'—');
        const badge=(shared.has(id)?'共享':'')+(group.length>1?(shared.has(id)?' · ':'')+'×'+group.length+' 次出现':'');
        const element=svg('g',{class:'wb-node'+(shared.has(id)?' wb-node-shared':''),'data-node':id,transform:`translate(${pos.x} ${pos.y})`,tabindex:0,role:'button','aria-label':label+' · '+node.op_kind+' · 端口 '+ports});
        element.append(svg('rect',{width:192,height:94,rx:9}),svg('text',{x:13,y:22,class:'wb-node-kind'},node.op_kind),svg('text',{x:13,y:47,class:'wb-node-title'},String(label).slice(0,24)),svg('text',{x:13,y:65,class:'wb-node-ports'},ports),svg('text',{x:13,y:82,class:'wb-node-state'},badge||id.slice(0,12)));
        element.append(svg('title',{},'logical_node_id '+id+'\noccurrence '+group.map(n=>n.occurrence_id).join(', ')));
        const activate=()=>{select(id);onSelect(id,declared);};element.onclick=activate;element.onkeydown=event=>{if(event.key==='Enter'||event.key===' '){event.preventDefault();activate();}};
        elements.set(id,{group:element});canvas.append(element);}
      select(selectedId);readable();
    }
    return Object.freeze({update,select,fit,zoom:factor=>{viewMode='manual';zoom=Math.max(.25,Math.min(2,zoom*factor));size();},dispose(){resize?.disconnect();if(resizeFrame!==null)cancelAnimationFrame(resizeFrame);resizeFrame=null;}});
  }
  function cpuChart(host,points) {
    host.replaceChildren();
    if(!Array.isArray(points)||points.length>600)throw new Error('性能窗口超过预算');
    const valid=points.filter(p=>Number.isFinite(p.cpu_cores));
    if(!valid.length){host.textContent='此窗口没有 CPU 测量；缺测保持为空。';return;}
    const first=points[0].time,last=points.at(-1).time,max=Math.max(1,...valid.map(p=>p.cpu_cores));
    const canvas=svg('svg',{viewBox:'0 0 600 135',role:'img','aria-label':'整次运行的 CPU 核数趋势'});let segment=[];
    const flush=()=>{if(segment.length)canvas.append(svg('polyline',{points:segment.join(' '),fill:'none',stroke:'currentColor','stroke-width':2}));segment=[];};
    for(const point of points){if(!Number.isFinite(point.cpu_cores)){flush();continue;}segment.push((8+584*(point.time-first)/Math.max(1,last-first))+','+(124-112*point.cpu_cores/max));}flush();host.append(canvas);
  }
  globalThis.ForgeWorkbenchGraph=Object.freeze({mount,mountLogical,status,cpuChart});
})();
