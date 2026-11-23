/* Conversation turns use the existing project provider, while Python runs in Jupyter. */
const utf8=new TextEncoder();
const limited=(value,length)=>value.length<=length?value:value.slice(0,length)+'…';

function promptFor(request,context,tools,cli=false){
  if(cli){
    if(utf8.encode(request).length>32768)throw Error('当前要求较长，请缩短要求再试。');
    return request;
  }
  const parts=[request,'当前 Notebook：'+context.path,'本轮运行许可：'+(context.allowRun?'允许':'仅编辑'),
    '要调整的单元格：\n'+limited(context.source,3000),
    '此前相邻代码：\n'+limited(context.before,2000),
    '当前卡片可见结果：'+limited(context.output,550),
    '引用的模块：'+JSON.stringify(context.selected.map(i=>[i.path,i.symbol])),
    '数据字典（固定版本）：'+limited(JSON.stringify(context.tables.slice(0,30)),3000)];
  let text=parts.join('\n\n');
  for(const item of tools){
    const line='\n'+JSON.stringify([item.path,item.symbol,item.tool_kind,limited(item.signature||'',280),limited(item.description||'',140)]);
    if(utf8.encode(text+line).length>30000)break;
    text+=line;
  }
  if(utf8.encode(text).length>32768)throw Error('当前要求与上下文较长，请缩短要求再试。');
  return text;
}

async function answer(api,sid,turn,after,active,onEvents,cli,handleCommand){
  let item=null,epoch=null,content='',paged=false,incomplete=false,oversize=false,terminal=null,state;
  while(active()){
    const response=await api('dev-events',{session_id:sid,after_sequence:after,limit:50,wait_ms:1200});
    if(!active())throw Error('已离开此工作区，可稍后取回回复。');
    let otherInput=false;
    onEvents(sid,response.events);
    for(const event of response.events){
      if(event.turn_id!==turn||event.session_id!==sid)continue;
      const data=event.data;
      if(event.type==='input.requested'&&data.request?.method==='notebook/command'&&handleCommand){
        if(data.expires*1000<=Date.now())continue;
        const binding={session_id:sid,turn_id:turn,pending_request_id:data.id,expected_digest:data.digest};
        const {claimed}=await api('dev-notebook-claim',{},binding);
        if(claimed){
          let reply;
          try{
            if(!active())throw Error('Notebook 已关闭。');
            const value=await handleCommand(data.request.command,data.request.arguments);
            if(utf8.encode(JSON.stringify(value)).length>15000)throw Error('结果超出回执大小，请缩小范围。');
            reply={value};
          }catch(error){reply={error:error.message||String(error)};}
          await api('dev-input',{}, {...binding,answer:reply});
        }
        continue;
      }
      if(event.type==='input.requested')otherInput=true;
      if(event.type==='turn.state'){
        if(data.state)state=data.state;
        if(data.cleanup==='complete'||data.cleanup==='unknown')terminal=data;
      }
      if(['process','summary'].includes(data.channel))continue;
      const complete=event.type==='message.completed';
      if(!(complete||event.type==='message.delta'||(event.type==='output.delta'&&paged&&data.item_id===item&&event.provider_epoch===epoch)))continue;
      if(complete||item!==data.item_id||epoch!==event.provider_epoch){content='';paged=false;incomplete=false;oversize=false;}
      item=data.item_id;epoch=event.provider_epoch;
      if(complete){paged=!!data.paged;incomplete=!!data.interrupted||data.status==='incomplete';}
      // CLI edits arrive as cell commands. Only the older text adapter needs
      // to collect a code suggestion; the timeline already renders both.
      if(!cli&&!oversize){
        content+=data.text||'';
        if(utf8.encode(content).length>65536){oversize=true;content='';}
      }
    }
    after=response.next_sequence;
    if(terminal){
      if(terminal.state!=='completed'||incomplete||terminal.cleanup!=='complete'){
        const result=typeof terminal.result==='string'?JSON.parse(terminal.result):terminal.result;
        const reason=result?.reason||terminal.reason||'';
        const interrupted=terminal.state==='interrupted';
        const detail=/BrokenPipeError|Broken pipe/i.test(reason)?'模型连接已关闭，未收到完整回复。':reason;
        const message=detail||(interrupted?'助手连接已中断。':'本轮未完成。');
        const finished=terminal.cleanup==='complete';
        throw Object.assign(Error(message),{finished,turn,retryable:finished&&interrupted});
      }
      if(cli||oversize)return {message:'',code:null};
      const tokens=globalThis.markdownit('commonmark').parse(content,{});
      const blocks=tokens.filter(t=>t.type==='fence'&&/^(python|py|python3)$/i.test(t.info.trim()));
      if(blocks.length!==1||!blocks[0].content.trim())return {message:content,code:null};
      const [start,end]=blocks[0].map,lines=content.split('\n');
      const closer=lines[end-1]?.trim()||'',marker=blocks[0].markup;
      if(closer.length<marker.length||[...closer].some(ch=>ch!==marker[0]))return {message:content,code:null};
      return {message:[...lines.slice(0,start),...lines.slice(end)].join('\n').trim(),code:blocks[0].content};
    }
    if(after>=response.last_sequence&&(state==='waiting_approval'||(otherInput&&state==='waiting_input')))
      throw Error('助手正在等待确认，可稍后取回回复。');
  }
  throw Error('稍后可取回这次回复。');
}

export function notebookConversation({api,project,path,cell,active,command,onSession,onEvents}){
  const key='demiflow.notebook-assistant.cell.'+JSON.stringify([project,path,cell]);
  let record={};
  try{
    const saved=sessionStorage.getItem(key);
    if(saved)record=JSON.parse(saved)||{};
    else if(localStorage.getItem(key))record=JSON.parse(localStorage.getItem(key))||{};
    else{
      // A previous version kept one pending turn per notebook. Finish it on
      // its original cell, then start a dedicated session for future prompts.
      const oldKey='demiflow.notebook-assistant.'+project+'.'+path;
      const old=JSON.parse(sessionStorage.getItem(oldKey)||'{}');
      if(old?.pending?.cell===cell){
        record={...old,legacyPending:true};
        sessionStorage.setItem(key,JSON.stringify(record));sessionStorage.removeItem(oldKey);
      }
    }
  }catch{}
  // Persist just the session binding across browser restarts. Outstanding
  // commands and execution consent belong to the tab that submitted them.
  function rememberSession(){
    if(record.sid&&!record.legacyPending)try{
      localStorage.setItem(key,JSON.stringify({sid:record.sid,provider:record.provider}));
    }catch{}
  }
  rememberSession();
  const save=()=>{try{sessionStorage.setItem(key,JSON.stringify(record));}catch{}rememberSession();};
  function finishTurn(){
    if(record.legacyPending)record={};
    else delete record.pending;
    save();
  }
  async function generate(request,context){
    if(context.cell!==cell)throw Error('此对话属于另一张卡片。');
    let pending=record.pending;
    if(pending&&pending.cell!==cell)throw Error('该请求属于另一张卡片。');
    if(!pending?.turn){
      const caps=await api('dev-capabilities');
      const available=(caps.providers||[]).filter(p=>p.available);
      const provider=record.provider?available.find(p=>p.id===record.provider):available.find(p=>p.id==='deepseek')||available[0];
      if(!caps.enabled||!caps.start||!caps.notebook_cells||!provider)throw Error('本项目当前无法生成单元格。'+(caps.reason||''));
      record.provider||=provider.id;record.cli=caps.notebook_cli===true;
    }
    if(!pending){
      const definitions=record.cli?[]:(await api('project-data-tools')).definitions;
      pending={cell:context.cell,source:context.source,allowRun:context.allowRun===true,request,prompt:promptFor(request,context,definitions,record.cli),id:crypto.randomUUID()};
      record.pending=pending;save();
    }
    if(!record.sid){
      if(!record.createId){
        const digest=await crypto.subtle.digest('SHA-256',utf8.encode(key));
        record.createId='cell-'+Array.from(new Uint8Array(digest),b=>b.toString(16).padStart(2,'0')).join('');
        save();
      }
      const session=await api('dev-session-create',{}, {request_id:record.createId,provider_id:record.provider,new_conversation:true,session_kind:'notebook'});
      record.sid=session.id;save();onSession(record.sid);
    }
    if(!pending.turn){
      if(pending.revision===undefined){
        const session=await api('dev-session',{session_id:record.sid});
        pending.revision=session.revision;pending.after=session.sequence||0;save();
      }
      try{
        const turn=await api('dev-turn-start',{}, {session_id:record.sid,submission_id:pending.id,expected_revision:pending.revision,text:pending.prompt,operation:record.cli?'notebook_cell_cli':'notebook_cell'});
        pending.turn=turn.id;save();
      }catch(error){
        if(['conflict','invalid_request','operation_rejected','resource_limit','busy','not_found','permission_denied'].includes(error.code)){finishTurn();}
        throw error;
      }
    }
    try{
      const result=await answer(api,record.sid,pending.turn,pending.after,active,onEvents,record.cli,async(name,args)=>{
        const value=await command(name,args,pending);
        if(name==='cell.edit'){pending.edited=true;pending.current=args.source;save();}
        return value;
      });
      if(pending.edited)result.code=null;
      finishTurn();
      return {...result,source:pending.source,request:pending.request,turn:pending.turn};
    }catch(error){if(error.finished){finishTurn();}throw error;}
  }
  return {generate,get pending(){return record.pending;},get sessionId(){return record.sid;},
    get provider(){return record.provider;},
    chooseProvider(id){if(!record.sid&&!record.pending){record.provider=id;save();}}};
}
