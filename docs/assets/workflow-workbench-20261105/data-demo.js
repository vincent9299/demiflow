/* Local discussion fixture. SELECT queries run only on these small in-memory tables.
 * No eval, Python kernel, business-data API, credentials or remote code execution. */
(() => {
  'use strict';
  const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const text=v=>v===null?'空值':typeof v==='boolean'?(v?'true':'false'):String(v);
  const catalog=[
    {id:'orders',name:'订单输入',role:'输入',versions:[8,9],origin:'订单归档 / 订单输入',fields:[['order_id','string','订单标识'],['status','string?','订单状态'],['channel','string','下单渠道'],['amount','number','订单金额'],['customer_id','string','客户标识'],['created_at','date','下单日期'],['note','string?','备注']]},
    {id:'customers',name:'客户输入',role:'输入',versions:[3,4],origin:'订单归档 / 客户输入',fields:[['customer_id','string','客户标识'],['tier','string','会员等级'],['region','string','地区'],['active','boolean','是否活跃']]},
    {id:'orders_clean',name:'标准化订单',role:'中间表',versions:[4,5],origin:'订单归档 / 金额标准化',fields:[['order_id','string','订单标识'],['status','string?','订单状态'],['channel','string','下单渠道'],['amount','number','标准化金额'],['customer_id','string','客户标识'],['created_at','date','下单日期'],['tier','string','客户等级']]},
    {id:'archive',name:'归档结果',role:'输出',versions:[12,13],origin:'订单归档 / 订单结果',fields:[['order_id','string','订单标识'],['channel','string','下单渠道'],['amount','number','订单金额'],['tier','string','客户等级'],['created_at','date','下单日期']]}
  ];
  const spec=id=>catalog.find(t=>t.id===id)||catalog[0];
  function source(id,version){
    const latest=version===spec(id).versions[1],orders=Array.from({length:latest?132:120},(_,i)=>({order_id:`ORD-${String(i+1).padStart(4,'0')}`,status:i%20<10?'paid':i%20<14?'pending':i%20<17?'refunded':i%20<19?'cancelled':null,channel:i%10<5?'app':i%10<8?'web':'store',amount:Math.round((19+(i*173%5000)/10)*100)/100,customer_id:`C${String(i%24+1).padStart(3,'0')}`,created_at:`2026-11-0${i%5+1}`,note:i%7===0?null:'常规订单'}));
    if(id==='orders')return orders;
    if(id==='customers')return Array.from({length:latest?28:24},(_,i)=>({customer_id:`C${String(i+1).padStart(3,'0')}`,tier:['standard','standard','silver','gold'][i%4],region:['华东','华南','华北','西南'][i%4],active:i%5!==0}));
    const clean=orders.map(({note,...r})=>({...r,tier:['standard','standard','silver','gold'][(Number(r.customer_id.slice(1))-1)%4]}));
    return id==='orders_clean'?clean:clean.filter(r=>r.status==='paid').map(({order_id,channel,amount,tier,created_at})=>({order_id,channel,amount,tier,created_at}));
  }
  function tokenize(code){
    if(new TextEncoder().encode(code).length>4096)throw Error('演示单元最多支持 4 KiB SQL。');
    const tokens=[];let i=0;
    while(i<code.length){
      if(/\s/.test(code[i])){i++;continue;}
      if(code.slice(i,i+2)==='--'){while(i<code.length&&code[i]!=='\n')i++;continue;}
      if(code[i]==="'"){
        let value='',closed=false;i++;
        while(i<code.length){if(code[i]==="'"){if(code[i+1]==="'"){value+="'";i+=2;}else{i++;closed=true;break;}}else value+=code[i++];}
        if(!closed)throw Error('字符串缺少结束引号。');
        tokens.push({kind:'literal',value});
      }else{
        const rest=code.slice(i),m=rest.match(/^(?:[A-Za-z_][A-Za-z0-9_]*|-?\d+(?:\.\d+)?|>=|<=|<>|!=|[(),;*=<>])/);
        if(!m)throw Error(`第 ${i+1} 个字符不在演示 SQL 支持范围内。`);
        const value=m[0];i+=value.length;
        tokens.push(/^-?\d/.test(value)?{kind:'literal',value:Number(value)}:{kind:'word',value});
      }
      if(tokens.length>512)throw Error('SQL 超出演示解析预算。');
    }
    return tokens;
  }
  function query(code,rows,table){
    const tokens=tokenize(code);let p=0;
    const peek=()=>String(tokens[p]?.value??'').toUpperCase();
    const take=()=>{if(p>=tokens.length)throw Error('SQL 尚未写完。');return tokens[p++];};
    const need=x=>{if(peek()!==x)throw Error(`这里需要 ${x}，当前是 ${tokens[p]?.value??'语句末尾'}。`);p++;};
    const identifier=()=>{const t=take();if(t.kind!=='word'||!/^[A-Za-z_]\w*$/.test(t.value))throw Error('这里需要字段名或表名。');return t.value.toLowerCase();};
    const fields=new Set(spec(table).fields.map(f=>f[0]));
    const field=()=>{const id=identifier();if(!fields.has(id))throw Error(`当前表没有字段 ${id}，可在左侧选字段并插入模板。`);return id;};
    need('SELECT');const select=[];
    do{
      if(select.length>=16)throw Error('演示最多选择 16 列。');
      let expression;
      if(peek()==='*'){p++;expression={kind:'star'};}
      else if(['COUNT','SUM','AVG','MIN','MAX'].includes(peek())&&tokens[p+1]?.value==='('){
        const op=take().value.toUpperCase();need('(');const name=peek()==='*'?(p++,'*'):field();need(')');
        if(name==='*'&&op!=='COUNT')throw Error('只有 COUNT 支持 *。');expression={kind:'aggregate',op,field:name};
      }else expression={kind:'field',field:field()};
      if(peek()==='AS'){p++;expression.alias=identifier();}
      expression.alias||=expression.field||expression.op?.toLowerCase();select.push(expression);
      if(peek()!==',')break;p++;
    }while(true);
    need('FROM');const from=identifier();if(!['data',table].includes(from))throw Error(`此演示单元只绑定当前 ${table}，请使用 FROM data 或 FROM ${table}。`);
    const predicates=[];
    const literal=()=>{if(['NULL','TRUE','FALSE'].includes(peek()))return ({NULL:null,TRUE:true,FALSE:false})[take().value.toUpperCase()];const t=take();if(t.kind!=='literal')throw Error('筛选值请使用数字、布尔值或单引号字符串。');return t.value;};
    if(peek()==='WHERE'){
      p++;
      do{
        const key=field();let predicate;
        if(peek()==='IS'){p++;const not=peek()==='NOT';if(not)p++;need('NULL');predicate=r=>not?r[key]!==null:r[key]===null;}
        else{const op=take().value;if(!['=','!=','<>','>','<','>=','<='].includes(op))throw Error('演示筛选支持 =、!=、>、<、>=、<= 和 IS NULL。');const v=literal();predicate=r=>{const x=r[key];if(x===null||v===null)return false;return op==='='?x===v:['!=','<>'].includes(op)?x!==v:op==='>'?x>v:op==='<'?x<v:op==='>='?x>=v:x<=v;};}
        predicates.push(predicate);if(predicates.length>8)throw Error('筛选条件最多 8 项。');if(peek()!=='AND')break;p++;
      }while(true);
    }
    const groups=[];
    if(peek()==='GROUP'){p++;need('BY');do{groups.push(field());if(groups.length>4)throw Error('演示最多按 4 个字段分组。');if(peek()!==',')break;p++;}while(true);}
    let order=null;
    if(peek()==='ORDER'){p++;need('BY');order={key:identifier(),desc:false};if(['ASC','DESC'].includes(peek()))order.desc=take().value.toUpperCase()==='DESC';}
    let limit=100;
    if(peek()==='LIMIT'){p++;const t=take();if(!Number.isInteger(t.value)||t.value<1||t.value>100)throw Error('演示 LIMIT 需为 1–100 的整数。');limit=t.value;}
    if(peek()===';')p++;
    if(p!==tokens.length)throw Error(`暂不支持 ${tokens[p].value}；此演示支持单表 SELECT、筛选、分组、聚合、排序与 LIMIT。`);
    const aggregate=select.some(x=>x.kind==='aggregate');
    if(select.some(x=>x.kind==='star')&&(select.length!==1||groups.length))throw Error('* 不能与其他投影或分组混用。');
    if(groups.length||aggregate)for(const e of select)if(e.kind==='field'&&!groups.includes(e.field))throw Error(`${e.field} 需要出现在 GROUP BY 中。`);
    const filtered=rows.filter(r=>predicates.every(fn=>fn(r))),columns=select[0].kind==='star'?[...fields]:select.map(e=>e.alias);
    if(new Set(columns).size!==columns.length)throw Error('结果列名重复，请使用 AS 分别命名。');
    function summarize(group,e){
      const values=e.field==='*'?group:group.map(r=>r[e.field]).filter(v=>v!==null);
      if(e.op==='COUNT')return values.length;
      if(!values.length)return null;
      if(['SUM','AVG'].includes(e.op)){if(values.some(v=>typeof v!=='number'))throw Error(`${e.op} 需要数值字段。`);const sum=values.reduce((a,b)=>a+b,0);return Math.round((e.op==='AVG'?sum/values.length:sum)*100)/100;}
      return values.reduce((a,b)=>e.op==='MIN'?(a<b?a:b):(a>b?a:b));
    }
    let result=[];
    if(groups.length||aggregate){
      const buckets=new Map();for(const row of filtered){const key=JSON.stringify(groups.map(k=>row[k]));if(!buckets.has(key))buckets.set(key,[]);buckets.get(key).push(row);}
      if(!groups.length&&!buckets.size)buckets.set('[]',[]);
      for(const group of buckets.values()){const row=Object.create(null);for(const e of select)row[e.alias]=e.kind==='aggregate'?summarize(group,e):group[0][e.field];result.push(row);}
    }else result=filtered.map(r=>{const row=Object.create(null);for(const e of select){if(e.kind==='star')for(const k of fields)row[k]=r[k];else row[e.alias]=r[e.field];}return row;});
    if(order){if(!columns.includes(order.key))throw Error(`排序列 ${order.key} 不在查询结果中。`);result.sort((a,b)=>{const x=a[order.key],y=b[order.key];const d=x===y?0:x===null?-1:y===null?1:x<y?-1:1;return order.desc?-d:d;});}
    return {columns,rows:result.slice(0,limit),total:result.length,limit,scanned:rows.length};
  }
  let activeView='cases',catalogCollapsed=false;
  const memories=new Map();let sequence=1;
  const makeCell=code=>({id:sequence++,code,view:'bar',output:null,error:'',page:0});
  const defaultCode='SELECT status, COUNT(*) AS count\nFROM data\nGROUP BY status\nORDER BY count DESC';
  function initial(){
    const cell=makeCell(defaultCode),ui={table:'orders',version:8,field:'status',filter:null,previewPage:0,caseRow:null,title:'订单状态分析',cells:[cell],message:'',saved:false};
    cell.output={...query(cell.code,source('orders',8),'orders'),table:'orders',version:8,code:cell.code,ms:0};return ui;
  }
  function validSaved(x){
    return x&&catalog.some(t=>t.id===x.table)&&spec(x.table).versions.includes(x.version)&&spec(x.table).fields.some(f=>f[0]===x.field)&&typeof x.title==='string'&&x.title.length<=80&&Array.isArray(x.cells)&&x.cells.length>0&&x.cells.length<=3&&x.cells.every(c=>Number.isInteger(c.id)&&typeof c.code==='string'&&c.code.length<=4096&&['table','bar','line','json'].includes(c.view)&&(!c.output||Array.isArray(c.output.columns)&&c.output.columns.length<=16&&c.output.columns.every(k=>typeof k==='string'&&k.length<=64)&&Array.isArray(c.output.rows)&&c.output.rows.length<=100&&c.output.rows.every(r=>r&&typeof r==='object'&&c.output.columns.every(k=>r[k]===null||['string','number','boolean'].includes(typeof r[k])))&&typeof c.output.code==='string'&&c.output.code.length<=4096&&catalog.some(t=>t.id===c.output.table)&&spec(c.output.table).versions.includes(c.output.version)));
  }
  function state(project){
    if(memories.has(project))return memories.get(project);
    let ui=initial();
    try{const raw=localStorage.getItem('demiflow-analysis-demo-'+project);if(raw&&raw.length<=65536){const saved=JSON.parse(raw);if(validSaved(saved)){ui={...saved,filter:null,caseRow:null,previewPage:0,message:'已恢复本浏览器保存的分析。',saved:true};sequence=Math.max(sequence,...ui.cells.map(c=>c.id+1));}}}catch{/* Optional local storage never blocks the fixture. */}
    memories.set(project,ui);return ui;
  }
  const missing=r=>r===null||r===undefined;
  function distribution(rows,field){
    const values=rows.map(r=>r[field]),nonNull=values.filter(v=>!missing(v)),unique=new Set(values.map(v=>JSON.stringify(v))).size;
    if(nonNull.length&&nonNull.every(v=>typeof v==='number')){
      const min=Math.min(...nonNull),max=Math.max(...nonNull),width=(max-min||1)/5;
      const bins=Array.from({length:5},(_,i)=>{const low=min+i*width,high=i===4?max:min+(i+1)*width;return {label:`${low.toFixed(0)}–${high.toFixed(0)}`,count:nonNull.filter(v=>v>=low&&(i===4?v<=high:v<high)).length,range:{low,high,last:i===4}};});
      return {unique,nulls:values.length-nonNull.length,bins,numeric:true,min,max};
    }
    const counts=new Map();for(const v of values){const key=JSON.stringify(v);if(!counts.has(key))counts.set(key,{value:v,label:text(v),count:0});counts.get(key).count++;}
    const all=[...counts.values()].sort((a,b)=>b.count-a.count),bins=all.slice(0,6);
    if(all.length>6)bins.push({label:`其他 ${all.length-6} 个值`,count:all.slice(6).reduce((n,b)=>n+b.count,0),other:true});
    return {unique,nulls:values.length-nonNull.length,bins,numeric:false};
  }
  function filterRows(rows,ui){
    const f=ui.filter;if(!f)return rows;
    return rows.filter(r=>f.range?typeof r[f.field]==='number'&&r[f.field]>=f.range.low&&(f.range.last?r[f.field]<=f.range.high:r[f.field]<f.range.high):r[f.field]===f.value);
  }
  function whereSQL(ui){
    const f=ui.filter;if(!f)return '';
    if(f.range)return `\nWHERE ${f.field} >= ${f.range.low} AND ${f.field} ${f.range.last?'<=':'<'} ${f.range.high}`;
    if(f.value===null)return `\nWHERE ${f.field} IS NULL`;
    const v=typeof f.value==='string'?"'"+f.value.replaceAll("'","''")+"'":String(f.value).toUpperCase();return `\nWHERE ${f.field} = ${v}`;
  }
  function template(kind,ui){
    if(kind==='preview')return 'SELECT *\nFROM data'+whereSQL(ui)+'\nLIMIT 8';
    if(kind==='count')return `SELECT ${ui.field}, COUNT(*) AS count\nFROM data\nGROUP BY ${ui.field}\nORDER BY count DESC`;
    if(kind==='time')return 'SELECT created_at, COUNT(*) AS count\nFROM data\nGROUP BY created_at\nORDER BY created_at ASC';
    return "SELECT channel, COUNT(*) AS orders, SUM(amount) AS total_amount\nFROM data\nWHERE status = 'paid'\nGROUP BY channel\nORDER BY total_amount DESC";
  }
  function rowTable(columns,rows,{className='',clickable=false,start=0}={}){
    return `<div class="ad-table-wrap"><table class="ad-table ${className}"><thead><tr>${columns.map(k=>`<th>${esc(k)}</th>`).join('')}</tr></thead><tbody>${rows.map((r,i)=>`<tr>${columns.map((k,j)=>`<td>${clickable&&j===0?`<button type="button" class="ad-row-link" data-ad-row="${start+i}">${esc(text(r[k]))}</button>`:`<span class="${r[k]===null?'ad-null':''}">${esc(text(r[k]))}</span>`}</td>`).join('')}</tr>`).join('')}</tbody></table>${rows.length?'':'<p class="ad-empty">当前条件下没有记录。</p>'}</div>`;
  }
  function profile(ui,rows){
    if(ui.caseRow!==null){const r=rows[ui.caseRow];if(r)return `<div class="ad-block-title"><h3>单条记录</h3><button type="button" data-ad="close-case" class="link">返回字段分布</button></div><dl class="ad-case">${spec(ui.table).fields.map(([k])=>`<div><dt>${esc(k)}</dt><dd>${esc(text(r[k]))}</dd></div>`).join('')}</dl>`;}
    const d=distribution(rows,ui.field),max=Math.max(1,...d.bins.map(b=>b.count));
    return `<div class="ad-block-title"><h3>字段分布 <code>${ui.field}</code></h3><span class="ad-quiet">全量演示数据</span></div><p class="ad-profile-caption">${d.unique} 个不同值（含空） · ${d.nulls} 个空值${d.numeric?` · ${d.min.toFixed(1)}–${d.max.toFixed(1)}`:''}</p><div class="ad-profile-bars">${d.bins.map((b,i)=>`<button type="button" class="ad-profile-row" data-ad-bin="${i}" aria-label="筛选 ${esc(ui.field)} ${esc(b.label)}"${b.other?' disabled':''}><span class="ad-bar-label">${esc(b.label)}</span><span class="ad-bar-track"><span class="ad-bar-fill" data-ad-width="${100*b.count/max}"></span></span><span class="ad-bar-count">${b.count}<small>${Math.round(100*b.count/Math.max(1,rows.length))}%</small></span></button>`).join('')}</div><p class="ad-hint">点分布项筛选左侧样例，分析单元保持独立。</p>`;
  }
  function canPlot(out){return out&&out.rows.length>0&&out.columns.length>1&&out.columns.some(k=>out.rows.some(r=>typeof r[k]==='number'));}
  function plotKeys(out){const y=out.columns.includes('total_amount')?'total_amount':out.columns.includes('count')?'count':out.columns.find(k=>out.rows.some(r=>typeof r[k]==='number'));return {y,x:out.columns.find(k=>k!==y)||out.columns[0]};}
  function canLine(out){if(!canPlot(out))return false;const {x}=plotKeys(out);return out.rows.length>1&&out.rows.every(r=>typeof r[x]==='number'||typeof r[x]==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(r[x]));}
  function result(cell,ui){
    if(!cell.output)return `<p class="ad-empty">${cell.error?'修正 SQL 后重新运行。':'运行单元后，结果会显示在这里。'}</p>`;
    const o=cell.output,stale=o.code!==cell.code||o.table!==ui.table||o.version!==ui.version;
    const controls=['table','bar','line','json'].map(v=>`<button type="button" data-ad-view="${v}" data-ad-cell="${cell.id}" aria-pressed="${cell.view===v}" class="${cell.view===v?'is-active':''}"${v==='bar'&&!canPlot(o)||v==='line'&&!canLine(o)?' disabled':''}>${({table:'表格',bar:'柱状图',line:'折线图',json:'JSON'})[v]}</button>`).join('');
    let content;
    if(cell.view==='bar'&&canPlot(o)){
      const {x,y}=plotKeys(o),shown=o.rows.slice(0,10),max=Math.max(1,...shown.map(r=>Number(r[y])||0));
      content=`<p class="ad-output-origin">数值列：${esc(y)} · 横条从 0 开始</p><div class="ad-output-bars" aria-label="查询结果柱状图">${shown.map(r=>`<div class="ad-output-bar"><span>${esc(text(r[x]))}</span><span class="ad-output-track"><span data-ad-width="${100*Math.max(0,Number(r[y])||0)/max}"></span></span><b>${esc(text(r[y]))}</b></div>`).join('')}</div>${o.rows.length>10?`<p class="ad-hint">图中展示前 10 / ${o.rows.length} 条结果；完整结果可切换表格。</p>`:''}`;
    }else if(cell.view==='line'&&canLine(o))content=`<svg class="ad-line-chart" data-ad-line="${cell.id}" role="img" aria-label="查询结果折线图"></svg>`;
    else if(cell.view==='json')content=`<pre class="ad-json">${esc(JSON.stringify(o.rows.slice(0,8),null,2))}</pre>${o.rows.length>8?'<p class="ad-hint">JSON 预览前8行；完整结果可切换表格分页。</p>':''}`;
    else{const start=cell.page*8;content=rowTable(o.columns,o.rows.slice(start,start+8));if(o.rows.length>8)content+=`<div class="ad-result-pager"><button type="button" data-ad-prev="${cell.id}"${cell.page?'':' disabled'}>上一页</button><span>${start+1}–${Math.min(start+8,o.rows.length)} / ${o.rows.length}</span><button type="button" data-ad-next="${cell.id}"${start+8<o.rows.length?'':' disabled'}>下一页</button></div>`;}
    return `<div class="ad-output-top"><span>Out[${cell.id}] <span class="${stale?'ad-stale':'ad-success'}">${stale?'结果待重算':'已计算'}</span></span><div class="ad-view-switch">${controls}</div></div><p class="ad-output-origin">${esc(o.table)} @v${o.version} · 扫描 ${o.scanned} 行测试数据 · 返回 ${o.rows.length} 行${o.total>o.rows.length?` / 共 ${o.total} 行，演示 LIMIT ${o.limit}`:''}</p>${content}`;
  }
  function caseNotebook(project){
    return `<section id="notebook-case-output" class="ad-case-notebook"><div class="ad-case-rich-output">${ForgeCaseDemo.html(project)}</div><details class="ad-case-origin"><summary>合成数据来源与 Notebook 调用示意</summary><p>case-template-fixture@1 · 独立合成 Case，未连接上方项目表或 Python 内核。</p><code>display(case_view(cases, view=当前视图配置))</code><p>界面字段、筛选和标注配置共用当前视图描述。</p></details></section>`;
  }
  function notebook(ui,project){
    const hasDate=spec(ui.table).fields.some(f=>f[0]==='created_at'),hasAmount=spec(ui.table).fields.some(f=>f[0]==='amount');
    return `<section class="ad-notebook"><div class="ad-analysis-toolbar"><nav class="ad-notebook-modes" aria-label="数据分析方式"><button type="button" data-action="demo-data-query" aria-pressed="${activeView==='query'}" class="${activeView==='query'?'is-active':''}">Notebook</button><button type="button" data-action="demo-data-cases" aria-pressed="${activeView==='cases'}" class="${activeView==='cases'?'is-active':''}">Case 分析</button></nav><span class="ad-kernel">${activeView==='cases'?'合成 Case · 本浏览器保存':'演示 SQL · Python 内核未接入'}</span><div class="ad-notebook-actions"${activeView==='cases'?' hidden':''}><button type="button" data-ad="add-cell"${ui.cells.length>=3?' disabled':''}>＋ 单元</button><button type="button" data-ad="save">保存分析</button></div></div><div class="ad-message" aria-live="polite"${ui.message?'':' hidden'}>${esc(ui.message)}</div><div class="ad-note-title"${activeView==='cases'?' hidden':''}><input id="ad-title" aria-label="分析名称" value="${esc(ui.title)}" maxlength="80"><span id="ad-save-status">${ui.saved?'已保存在本浏览器':'未保存'}</span></div>${activeView==='cases'?caseNotebook(project):ui.cells.map(cell=>`<section class="ad-cell" data-cell="${cell.id}"><div class="ad-cell-top"><span>In[${cell.id}] <span class="ad-binding">data = ${ui.table} @v${ui.version}</span></span><div class="ad-cell-actions"><label class="sr-only" for="ad-template-${cell.id}">插入查询模板</label><select id="ad-template-${cell.id}" data-ad-template="${cell.id}"><option value="">插入模板</option><option value="count">所选字段频数</option><option value="preview">预览 / 当前筛选</option>${hasDate?'<option value="time">按日期统计</option>':''}${hasAmount?'<option value="sales">已付款渠道金额</option>':''}</select><button type="button" class="primary" data-ad-run="${cell.id}">运行</button></div></div><div class="ad-code-wrap"><span class="ad-code-language">SQL</span><textarea spellcheck="false" maxlength="4096" rows="4" class="ad-code" data-ad-code="${cell.id}" aria-label="SQL 分析单元 ${cell.id}">${esc(cell.code)}</textarea></div><p class="ad-cell-hint">Ctrl / ⌘ + Enter 运行 · 可改字段、筛选、分组与排序<span data-ad-dirty="${cell.id}">${cell.output&&cell.output.code!==cell.code?'代码已修改，待运行':''}</span></p>${cell.error?`<div class="ad-error" role="alert">${esc(cell.error)}</div>`:''}<div class="ad-cell-output">${result(cell,ui)}</div></section>`).join('')}</section>`;
  }
  function html(project,options={}){catalogCollapsed=options.catalogCollapsed===true;return queryHtml(project);}
  function queryHtml(project){
    if(project!=='native')return `<section class="ad-empty-project"><h2>这个示例项目暂无可读取的演示表</h2><p>选择左侧“订单归档 · 示例”体验数据目录与分析笔记本。这里不连接真实项目或扫描磁盘。</p></section>`;
    const ui=state(project),t=spec(ui.table),rows=source(ui.table,ui.version),filtered=filterRows(rows,ui),start=ui.previewPage*5;
    const columns=t.fields.slice(0,5).map(f=>f[0]),overviewOpen=ui.overviewOpen??activeView==='query';
    return `<div id="data-analysis-demo" class="ad-workbench${catalogCollapsed?' is-catalog-collapsed':''}" data-project="${project}"><div class="ad-toolbar"><button type="button" data-action="toggle-data-catalog" class="ad-catalog-toggle" aria-expanded="${!catalogCollapsed}" aria-controls="ad-catalog" aria-label="${catalogCollapsed?'展开':'收起'}数据目录" title="${catalogCollapsed?'展开':'收起'}数据目录"><span aria-hidden="true">☷</span><span>数据目录</span></button><div class="ad-table-heading"><h2>${t.id}<span>${t.name}</span></h2></div><label class="ad-version"><span class="sr-only">固定版本</span><select id="ad-version" aria-label="固定版本">${t.versions.map(v=>`<option value="${v}"${v===ui.version?' selected':''}>v${v}</option>`).join('')}</select></label><span class="ad-table-count">${rows.length} 行 · ${t.fields.length} 字段</span><button type="button" id="ad-overview-toggle" data-ad="toggle-overview" class="ad-overview-toggle${overviewOpen?' is-active':''}" aria-controls="ad-data-overview" aria-expanded="${overviewOpen}">样例与字段 <span aria-hidden="true">${overviewOpen?'▴':'▾'}</span></button><span class="ad-table-origin" title="来源：${esc(t.origin)}">来源：${esc(t.origin)}</span><span class="ad-demo-badge" title="内存测试表，统计覆盖当前版本全部 ${rows.length} 行；只读演示">全量测试数据</span></div><aside id="ad-catalog" class="ad-catalog" aria-label="项目数据目录"${catalogCollapsed?' hidden':''}><div class="ad-catalog-title"><h2>数据目录</h2></div><div id="ad-catalog-content"><span class="ad-catalog-count">4 张演示表</span>${['输入','中间表','输出'].map(role=>`<div class="ad-table-group"><h3>${role}</h3>${catalog.filter(x=>x.role===role).map(x=>`<button type="button" data-ad-table="${x.id}" class="ad-table-item ${x.id===ui.table?'is-active':''}" aria-pressed="${x.id===ui.table}"><strong>${x.id}</strong><span>${x.name} · ${source(x.id,x.versions[0]).length} 行</span></button>`).join('')}</div>`).join('')}<div class="ad-fields-title"><h3>字段</h3><span>${t.fields.length}</span></div><div class="ad-fields">${t.fields.map(([name,type,label])=>`<button type="button" data-ad-field="${name}" class="${name===ui.field?'is-active':''}" aria-pressed="${name===ui.field}"><span>${name}<small>${label}</small></span><code>${type}</code></button>`).join('')}</div></div></aside><div class="ad-main"><section id="ad-data-overview" class="ad-data-overview" aria-label="数据样例与字段分布"${overviewOpen?'':' hidden'}><div class="ad-understanding"><section class="ad-preview"><div class="ad-block-title"><h3>数据样例</h3>${ui.filter?'<button type="button" data-ad="clear-filter" class="link">清除筛选</button>':'<span class="ad-quiet">点首列原位看记录</span>'}</div>${ui.filter?`<p class="ad-filter-label">${ui.filter.field}：${esc(ui.filter.label)} · ${filtered.length} 行匹配</p>`:''}${rowTable(columns,filtered.slice(start,start+5),{clickable:true,start})}<div class="ad-preview-footer"><span>预览 ${filtered.length?start+1:0}–${Math.min(start+5,filtered.length)} / ${filtered.length} 行</span><div><button type="button" data-ad="prev-preview" aria-label="上一页样例"${ui.previewPage?'':' disabled'}>‹</button><button type="button" data-ad="next-preview" aria-label="下一页样例"${start+5<filtered.length?'':' disabled'}>›</button></div></div></section><section class="ad-profile">${profile(ui,rows)}</section></div></section>${notebook(ui,project)}</div></div>`;
  }
  function draw(root,ui){
    root.querySelectorAll('[data-ad-width]').forEach(el=>el.style.width=Math.max(0,Math.min(100,Number(el.dataset.adWidth)))+'%');
    for(const svg of root.querySelectorAll('[data-ad-line]')){
      const cell=ui.cells.find(c=>c.id===Number(svg.dataset.adLine));if(!cell?.output||!canLine(cell.output))continue;
      const o=cell.output,{x,y}=plotKeys(o),values=o.rows.slice(0,100).map(r=>({x:typeof r[x]==='number'?r[x]:Date.parse(r[x]),y:r[y],label:text(r[x])})).sort((a,b)=>a.x-b.x);
      const w=Math.max(260,Math.floor(svg.getBoundingClientRect().width)),h=150,left=48,right=w-18,top=16,bottom=117,minX=Math.min(...values.map(r=>r.x)),maxX=Math.max(...values.map(r=>r.x)),minY=Math.min(0,...values.map(r=>Number(r.y)||0)),maxY=Math.max(1,...values.map(r=>Number(r.y)||0));
      svg.setAttribute('viewBox',`0 0 ${w} ${h}`);
      const points=values.map(r=>`${left+(right-left)*(r.x-minX)/Math.max(1,maxX-minX)},${bottom-(bottom-top)*((Number(r.y)||0)-minY)/(maxY-minY)}`).join(' ');
      svg.innerHTML=`<line x1="${left}" y1="${bottom}" x2="${right}" y2="${bottom}"/><text x="0" y="18">${esc(y)}</text><text x="9" y="${top+18}">${maxY}</text><text x="24" y="${bottom}">${minY}</text><polyline points="${points}"/><text x="${left}" y="142">${esc(values[0].label)}</text><text x="${right}" y="142" text-anchor="end">${esc(values.at(-1).label)}</text>`;
    }
  }
  function mount(project){
    const root=document.getElementById('data-analysis-demo');if(!root||root.dataset.mounted)return;
    root.dataset.mounted='true';const ui=state(project);
    const repaint=()=>{ForgeCaseDemo.unmount();root.outerHTML=queryHtml(project);mount(project);};
    const changed=()=>{ui.saved=false;};
    const run=cell=>{try{const start=performance.now();cell.output={...query(cell.code,source(ui.table,ui.version),ui.table),table:ui.table,version:ui.version,code:cell.code,ms:performance.now()-start};cell.error='';cell.page=0;if(!canPlot(cell.output))cell.view='table';else if(cell.view==='line'&&!canLine(cell.output))cell.view='bar';changed();ui.message='';}catch(e){cell.error=e.message;}repaint();};
    root.addEventListener('input',e=>{
      if(e.target.id==='ad-title'){ui.title=e.target.value;changed();root.querySelector('#ad-save-status').textContent='未保存';}
      if(e.target.dataset.adCode){const c=ui.cells.find(c=>c.id===Number(e.target.dataset.adCode));c.code=e.target.value;changed();root.querySelector(`[data-ad-dirty="${c.id}"]`).textContent=c.output?.code===c.code?'':'代码已修改，待运行';root.querySelector('#ad-save-status').textContent='未保存';const badge=root.querySelector(`[data-cell="${c.id}"] .ad-success, [data-cell="${c.id}"] .ad-stale`);if(badge){const stale=c.output.code!==c.code||c.output.table!==ui.table||c.output.version!==ui.version;badge.className=stale?'ad-stale':'ad-success';badge.textContent=stale?'结果待重算':'已计算';}}
    });
    root.addEventListener('keydown',e=>{if(e.target.dataset.adCode&&(e.ctrlKey||e.metaKey)&&e.key==='Enter'){e.preventDefault();run(ui.cells.find(c=>c.id===Number(e.target.dataset.adCode)));}});
    root.addEventListener('change',e=>{
      if(e.target.id==='ad-version'){ui.version=Number(e.target.value);ui.filter=null;ui.caseRow=null;ui.previewPage=0;changed();ui.message='数据版本已改变，已有单元输出保留原版本，运行后更新。';repaint();}
      if(e.target.dataset.adTemplate){const cell=ui.cells.find(c=>c.id===Number(e.target.dataset.adTemplate));if(e.target.value){cell.code=template(e.target.value,ui);cell.error='';changed();repaint();}}
    });
    root.addEventListener('click',e=>{
      const b=e.target.closest('button');if(!b||b.disabled)return;
      if(b.dataset.ad==='toggle-overview'){ui.overviewOpen=root.querySelector('#ad-data-overview').hidden;repaint();document.getElementById('ad-overview-toggle')?.focus({preventScroll:true});return;}
      if(b.dataset.adTable){ui.table=b.dataset.adTable;ui.version=spec(ui.table).versions[0];ui.field=spec(ui.table).fields.some(f=>f[0]==='status')?'status':spec(ui.table).fields[1][0];ui.filter=null;ui.previewPage=0;ui.caseRow=null;changed();ui.message='已切换表。旧分析结果保留来源；可插入当前字段查询模板。';repaint();return;}
      if(b.dataset.adField){ui.field=b.dataset.adField;ui.caseRow=null;ui.overviewOpen=true;repaint();return;}
      if(b.dataset.adBin!==undefined){const bin=distribution(source(ui.table,ui.version),ui.field).bins[Number(b.dataset.adBin)];if(bin&&!bin.other){ui.filter={field:ui.field,value:bin.value,range:bin.range,label:bin.label};ui.previewPage=0;ui.caseRow=null;repaint();}return;}
      if(b.dataset.adRow!==undefined){const all=source(ui.table,ui.version),row=filterRows(all,ui)[Number(b.dataset.adRow)];ui.caseRow=all.findIndex(r=>JSON.stringify(r)===JSON.stringify(row));repaint();return;}
      if(b.dataset.adRun){run(ui.cells.find(c=>c.id===Number(b.dataset.adRun)));return;}
      if(b.dataset.adView){const c=ui.cells.find(c=>c.id===Number(b.dataset.adCell));c.view=b.dataset.adView;repaint();return;}
      if(b.dataset.adPrev){ui.cells.find(c=>c.id===Number(b.dataset.adPrev)).page--;repaint();return;}
      if(b.dataset.adNext){ui.cells.find(c=>c.id===Number(b.dataset.adNext)).page++;repaint();return;}
      if(b.dataset.ad==='close-case'){ui.caseRow=null;repaint();}
      if(b.dataset.ad==='clear-filter'){ui.filter=null;ui.previewPage=0;repaint();}
      if(b.dataset.ad==='prev-preview'){ui.previewPage--;repaint();}
      if(b.dataset.ad==='next-preview'){ui.previewPage++;repaint();}
      if(b.dataset.ad==='add-cell'&&ui.cells.length<3){const c=makeCell(template('preview',ui));c.view='table';ui.cells.push(c);changed();repaint();}
      if(b.dataset.ad==='save'){
        try{const raw=JSON.stringify({...ui,filter:null,caseRow:null,message:'',saved:true});if(raw.length>65536)throw Error('保存内容超过演示的 64 KiB 上限，请减少输出。');localStorage.setItem('demiflow-analysis-demo-'+project,raw);ui.saved=true;ui.message='已保存分析、SQL 和当前输出到本浏览器。';}catch(error){ui.message='保存未完成：'+error.message;}repaint();
      }
    });
    if(activeView==='cases')ForgeCaseDemo.mount(project);
    requestAnimationFrame(()=>draw(root,ui));
    if(!window.__demiflowDataResize){window.__demiflowDataResize=true;addEventListener('resize',()=>{const current=document.getElementById('data-analysis-demo');if(current)draw(current,state(current.dataset.project));});}
  }
  window.ForgeDataDemo=Object.freeze({html,mount,selectView:value=>{activeView=value==='query'?'query':'cases';},getView:()=>activeView});
})();
