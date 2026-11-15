/* Bounded, read-only presentation of an already-fetched TableSlice. No fetch,
   no aggregation, no cross-result join; bar draws from a zero axis and line
   keeps original row order. */
(() => {
  'use strict';
  const MAX_ROWS=200,MAX_COLS=16,MAX_BYTES=256*1024;
  const SVG='http://www.w3.org/2000/svg';
  const $=(tag,text,kind)=>{const node=document.createElement(tag);if(text!==undefined)node.textContent=text;if(kind)node.className=kind;return node;};
  const svg=(tag,attrs={})=>{const node=document.createElementNS(SVG,tag);for(const [key,value]of Object.entries(attrs))node.setAttribute(key,String(value));return node;};
  const bytes=value=>new TextEncoder().encode(JSON.stringify(value)).length;
  const numericType=type=>type==='int64'||type==='float64';
  const isFiniteSafe=value=>typeof value==='number'&&Number.isFinite(value)&&(!Number.isInteger(value)||Number.isSafeInteger(value));
  const columnNames=slice=>slice.columns.map(column=>column.name);
  const fmt=value=>{
    if(value===null||value===undefined)return '';
    if(typeof value!=='number')return String(value);
    const abs=Math.abs(value);
    if(abs!==0&&(abs>=1e6||abs<1e-3))return value.toExponential(2);
    return String(Math.round(value*1000)/1000);
  };
  function boundsReason(slice){
    if(!slice||!Array.isArray(slice.columns)||!Array.isArray(slice.rows))return '表格切片结构无效。';
    if(slice.columns.length>MAX_COLS)return '列数超过 '+MAX_COLS+' 列上限，不绘制图表。';
    if(slice.rows.length>MAX_ROWS)return '行数超过 '+MAX_ROWS+' 行上限，不绘制图表。';
    if(bytes({columns:slice.columns,rows:slice.rows})>MAX_BYTES)return '表格切片超过 256KiB 内存预算，不绘制图表。';
    return null;
  }
  function validateChart(slice,spec){
    const bound=boundsReason(slice);
    if(bound)return {allowed:false,reason:bound};
    const kind=spec?.kind;
    if(kind==='table'||kind==='json')return {allowed:true,reason:null};
    if(kind!=='bar'&&kind!=='line')return {allowed:false,reason:'未知图表类型。'};
    const names=columnNames(slice);
    if(!spec.x||!names.includes(spec.x))return {allowed:false,reason:'请选择有效的 X 列。'};
    if(!spec.y||!names.includes(spec.y))return {allowed:false,reason:'请选择有效的 Y 列。'};
    const column=slice.columns.find(item=>item.name===spec.y);
    if(!numericType(column?.type))return {allowed:false,reason:'Y 列必须是数值列（int64/float64）；当前为 '+(column?.type||'未知')+'，保留表格与原值。'};
    let plotted=0;
    for(const row of slice.rows){
      const value=row[spec.y];
      if(value===null||value===undefined){continue;}
      if(!isFiniteSafe(value))return {allowed:false,reason:'Y 列含非有限或不安全整数值，拒绝绘制图表；表格与原值仍可用。'};
      plotted++;
    }
    if(!plotted)return {allowed:false,reason:'Y 列在当前页没有可绘制的数值。'};
    return {allowed:true,reason:null};
  }
  function mount({parent,slice,spec,isCurrent=()=>true}){
    const element=$('div',undefined,'analysis-charts');parent.append(element);
    let disposed=false;
    const names=columnNames(slice);
    const numericColumns=slice.columns.filter(column=>numericType(column.type)).map(column=>column.name);
    const state={
      kind:['table','bar','line','json'].includes(spec?.kind)?spec.kind:'table',
      x:names.includes(spec?.x)?spec.x:(names[0]||null),
      y:numericColumns.includes(spec?.y)?spec.y:(numericColumns[0]||null)
    };
    const toolbar=$('div',undefined,'analysis-charts-toolbar');
    const coverage=$('p',undefined,'analysis-charts-coverage muted small');
    const reason=$('p',undefined,'analysis-charts-reason error-box');reason.hidden=true;
    const content=$('div',undefined,'analysis-charts-content');
    const kinds=[['table','表格'],['bar','柱图'],['line','折线'],['json','JSON']];
    const kindButtons=new Map();
    for(const [kind,label]of kinds){
      const button=$('button',label);button.type='button';button.dataset.kind=kind;
      button.onclick=()=>{if(disposed||!isCurrent())return;state.kind=kind;render();};
      kindButtons.set(kind,button);toolbar.append(button);
    }
    const xLabel=$('label','X'),xSelect=$('select');xSelect.setAttribute('aria-label','X 列');
    for(const name of names){const option=$('option',name);option.value=name;xSelect.append(option);}
    if(state.x)xSelect.value=state.x;
    xSelect.onchange=()=>{if(disposed||!isCurrent())return;state.x=xSelect.value;render();};
    xLabel.append(xSelect);
    const yLabel=$('label','Y'),ySelect=$('select');ySelect.setAttribute('aria-label','Y 列（数值）');
    for(const name of numericColumns){const option=$('option',name);option.value=name;ySelect.append(option);}
    if(state.y)ySelect.value=state.y;
    ySelect.onchange=()=>{if(disposed||!isCurrent())return;state.y=ySelect.value;render();};
    yLabel.append(ySelect);
    toolbar.append(xLabel,yLabel);
    element.append(toolbar,coverage,reason,content);
    function paintCoverage(){
      const info=slice.coverage||{label:'',complete:false,totalRows:null};
      let text=info.label||'当前结果页';
      if(!info.complete)text+=' · 当前结果页';
      if(Number.isInteger(info.totalRows))text+=' · 总行数 '+info.totalRows;
      coverage.textContent=text;
    }
    function renderTable(){
      const wrap=$('div',undefined,'analysis-charts-table');
      const table=$('table'),head=$('thead'),headRow=$('tr');
      for(const column of slice.columns){const th=$('th');th.append($('span',column.name),$('span',column.type,'muted small'));headRow.append(th);}
      head.append(headRow);table.append(head);
      const bodyEl=$('tbody');
      for(const row of slice.rows){
        const tr=$('tr');
        for(const column of slice.columns){const value=row[column.name];tr.append($('td',value===null||value===undefined?'':String(value)));}
        bodyEl.append(tr);
      }
      table.append(bodyEl);wrap.append(table);content.append(wrap);
    }
    function renderJSON(){
      const pre=$('pre',JSON.stringify({columns:slice.columns,coverage:slice.coverage,rows:slice.rows},null,2),'analysis-charts-json');
      content.append(pre);
    }
    function renderChart(width){
      const kind=state.kind;
      const values=slice.rows.map(row=>{const value=row[state.y];return isFiniteSafe(value)?value:null;});
      const finite=values.filter(value=>value!==null);
      if(!finite.length){content.append($('p','当前页无可绘制数值。','muted small'));return;}
      const height=260,left=52,right=14,top=14,bottom=46;
      const plotW=Math.max(40,width-left-right),plotH=height-top-bottom;
      let yMin,yMax;
      if(kind==='bar'){yMin=Math.min(0,...finite);yMax=Math.max(0,...finite);}
      else{yMin=Math.min(...finite);yMax=Math.max(...finite);}
      if(yMin===yMax){yMin-=1;yMax+=1;}
      const yOf=value=>top+plotH*(1-(value-yMin)/(yMax-yMin));
      const root=svg('svg',{width,height,viewBox:`0 0 ${width} ${height}`,role:'img','aria-label':(kind==='bar'?'柱图':'折线')+'（当前结果页）'});
      root.classList.add('analysis-charts-svg');
      const grid=svg('g');root.append(grid);
      const ticks=4;
      for(let index=0;index<=ticks;index++){
        const value=yMin+(yMax-yMin)*index/ticks,y=yOf(value);
        grid.append(svg('line',{x1:left,y1:y,x2:left+plotW,y2:y,class:'analysis-charts-grid'}));
        const label=svg('text',{x:left-6,y:y+4,'text-anchor':'end',class:'analysis-charts-tick'});label.textContent=fmt(value);grid.append(label);
      }
      const n=slice.rows.length,step=plotW/Math.max(1,n);
      const labelEvery=Math.max(1,Math.ceil(n/8));
      const xLabels=svg('g');root.append(xLabels);
      for(let index=0;index<n;index++){
        if(index%labelEvery!==0&&index!==n-1)continue;
        const raw=slice.rows[index][state.x];
        const text=raw===null||raw===undefined?'':String(raw);
        const label=svg('text',{x:left+step*(index+0.5),y:height-bottom+18,'text-anchor':'middle',class:'analysis-charts-tick'});
        label.textContent=text.length>10?text.slice(0,9)+'…':text;
        xLabels.append(label);
      }
      if(kind==='bar'){
        const zero=yOf(0);
        root.append(svg('line',{x1:left,y1:zero,x2:left+plotW,y2:zero,class:'analysis-charts-axis'}));
        const barW=Math.max(1,step*0.7);
        const bars=svg('g');root.append(bars);
        values.forEach((value,index)=>{
          if(value===null)return;
          const y=yOf(value),x=left+step*index+(step-barW)/2;
          bars.append(svg('rect',{x,y:Math.min(y,zero),width:barW,height:Math.max(1,Math.abs(zero-y)),class:'analysis-charts-bar'}));
        });
      }else{
        const points=svg('g');root.append(points);
        let path='',pen=false;
        values.forEach((value,index)=>{
          if(value===null){pen=false;return;}
          const x=left+step*(index+0.5),y=yOf(value);
          path+=(pen?'L':'M')+x.toFixed(1)+' '+y.toFixed(1)+' ';pen=true;
          points.append(svg('circle',{cx:x,cy:y,r:2.5,class:'analysis-charts-point'}));
        });
        if(path)root.append(svg('path',{d:path.trim(),class:'analysis-charts-line',fill:'none'}));
      }
      content.append(root);
    }
    function render(){
      if(disposed||!isCurrent())return;
      content.replaceChildren();reason.hidden=true;
      paintCoverage();
      for(const [kind,button]of kindButtons){
        button.classList.toggle('active',state.kind===kind);
        if(kind==='bar'||kind==='line'){
          const check=validateChart(slice,{kind,x:state.x,y:state.y});
          button.disabled=!check.allowed;
          button.title=check.allowed?'':check.reason;
        }else button.disabled=false;
      }
      xSelect.disabled=!names.length;
      ySelect.disabled=!numericColumns.length;
      if(state.kind==='bar'||state.kind==='line'){
        const check=validateChart(slice,{kind:state.kind,x:state.x,y:state.y});
        if(!check.allowed){
          reason.textContent=check.reason;reason.hidden=false;
          state.kind='table';
          for(const [kind,button]of kindButtons)button.classList.toggle('active',kind==='table');
          renderTable();return;
        }
        renderChart(Math.max(240,element.clientWidth||content.clientWidth||320));
        return;
      }
      if(state.kind==='json')renderJSON();else renderTable();
    }
    let resizeFrame=null;
    const observer=typeof ResizeObserver==='function'?new ResizeObserver(()=>{
      if(disposed||!isCurrent()||(state.kind!=='bar'&&state.kind!=='line'))return;
      if(resizeFrame!==null)cancelAnimationFrame(resizeFrame);
      resizeFrame=requestAnimationFrame(()=>{resizeFrame=null;if(!disposed&&isCurrent())render();});
    }):null;
    observer?.observe(element);
    render();
    return {element,ready:null,dispose(){
      if(disposed)return;disposed=true;
      observer?.disconnect();
      if(resizeFrame!==null)cancelAnimationFrame(resizeFrame);
      element.remove();
    }};
  }
  globalThis.ForgeAnalysisCharts=Object.freeze({validateChart,mount});
})();